//! ClickHouse datasource provider using the native `clickhouse` crate with
//! Arrow IPC streaming.
//!
//! This is an alternative to the HTTP-based [`super::clickhouse::ClickHouseProvider`]
//! and serves as Phase 1 PoC for the DataFusion adapter pattern (KYO-69).
//!
//! Uses the `clickhouse` crate's `ArrowStream` output format for efficient
//! columnar data transfer, and DataFusion's SQL parser for dry-run validation.
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `host` | string | `"localhost"` | ClickHouse server hostname |
//! | `port` | int | `8123` | HTTP port |
//! | `database` | string | `"default"` | Default database |
//! | `secure` | bool | `false` | Use HTTPS instead of HTTP |
//! | `ssh_enabled` | bool | `false` | Whether to use SSH tunnel |
//! | `ssh_host` | string | — | Bastion host for SSH tunnel |
//! | `ssh_port` | int | `22` | SSH port |
//! | `ssh_username` | string | — | SSH username |
//! | `ssh_private_key` | string | — | PEM-encoded SSH private key |
//!
//! ## Credentials
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `username` | string | `"default"` | ClickHouse username |
//! | `password` | string | `""` | ClickHouse password |

use std::io::Cursor;
use std::time::Instant;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow_ipc::reader::StreamReader;
use clickhouse::Client;
use serde_json::Value;

use kyomi_connect_protocol::{ColumnInfo, SimpleType};

use crate::provider::{
    DatasourceProvider, DiscoveryResult, DryRunResult, QueryResult, QueryStatus,
};
use crate::{DATASOURCE_TIMEOUT_CONNECT, DATASOURCE_TIMEOUT_QUERY};

use kyomi_connect_protocol::Error;
#[cfg(feature = "ssh")]
use crate::ssh_tunnel::{SshTunnel, SshTunnelConfig};

/// Default ClickHouse HTTP port.
const DEFAULT_PORT: u16 = 8123;
/// Default ClickHouse database.
const DEFAULT_DATABASE: &str = "default";
/// Default ClickHouse username.
const DEFAULT_USERNAME: &str = "default";

/// ClickHouse datasource provider using the native `clickhouse` crate.
///
/// Executes queries via the `clickhouse` crate's native Arrow stream support
/// rather than the HTTP JSON API used by [`super::clickhouse::ClickHouseProvider`].
pub struct DataFusionClickHouseProvider {
    /// ClickHouse client from the `clickhouse` crate.
    client: Client,
    /// SSH tunnel, if configured. Held to keep the tunnel alive.
    #[cfg(feature = "ssh")]
    _ssh_tunnel: Option<SshTunnel>,
}

impl DataFusionClickHouseProvider {
    /// Create a new DataFusion ClickHouse provider from connection config and credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection URL cannot be constructed or the
    /// connectivity check fails.
    pub async fn new(
        connection_config: &Value,
        credentials: &Value,
    ) -> kyomi_connect_protocol::Result<Self> {
        // When the `ssh` feature is enabled, these are reassigned to the tunnel endpoint.
        #[cfg_attr(not(feature = "ssh"), allow(unused_mut))]
        let mut host = connection_config
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("localhost")
            .to_string();

        #[cfg_attr(not(feature = "ssh"), allow(unused_mut))]
        let mut port = connection_config
            .get("port")
            .and_then(|v| v.as_u64())
            .map(|p| p as u16)
            .unwrap_or(DEFAULT_PORT);

        let database = connection_config
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_DATABASE)
            .to_string();

        let secure = connection_config
            .get("secure")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let username = credentials
            .get("username")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_USERNAME);

        let password = credentials
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // SSH tunnel setup
        #[cfg(feature = "ssh")]
        let ssh_tunnel = match SshTunnelConfig::from_connection_config(connection_config) {
            Some(Ok(ssh_config)) => {
                let tunnel = SshTunnel::connect(
                    &ssh_config.host,
                    ssh_config.port,
                    &ssh_config.username,
                    &ssh_config.private_key,
                    &host,
                    port,
                )
                .await?;

                let (tunnel_host, tunnel_port) = tunnel.local_addr();
                host = tunnel_host.to_string();
                port = tunnel_port;

                Some(tunnel)
            }
            Some(Err(e)) => return Err(e),
            None => None,
        };

        // When using SSH tunnel, disable SSL (tunnel provides encryption)
        #[cfg(feature = "ssh")]
        let effective_secure = if ssh_tunnel.is_some() { false } else { secure };
        #[cfg(not(feature = "ssh"))]
        let effective_secure = secure;
        let scheme = if effective_secure { "https" } else { "http" };
        let url = format!("{scheme}://{host}:{port}");

        let client = Client::default()
            .with_url(&url)
            .with_user(username)
            .with_password(password)
            .with_database(&database);

        // Verify connectivity during construction with timeout.
        tokio::time::timeout(
            DATASOURCE_TIMEOUT_CONNECT,
            client.query("SELECT 1").execute(),
        )
        .await
        .map_err(|_| {
            Error::Provider(format!(
                "ClickHouse connection timed out after {}s",
                DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| Error::Provider(format!("ClickHouse connection failed: {e}")))?;

        Ok(Self {
            client,
            #[cfg(feature = "ssh")]
            _ssh_tunnel: ssh_tunnel,
        })
    }

    /// Execute a raw SQL query and return the result as Arrow RecordBatches.
    ///
    /// Uses ClickHouse's native `ArrowStream` output format for efficient
    /// columnar data transfer. Respects [`DATASOURCE_TIMEOUT_QUERY`].
    async fn query_arrow(&self, sql: &str) -> kyomi_connect_protocol::Result<Vec<RecordBatch>> {
        let mut cursor = self
            .client
            .query(sql)
            .fetch_bytes("ArrowStream")
            .map_err(|e| Error::Provider(format!("ClickHouse query failed: {e}")))?;

        let collected = tokio::time::timeout(DATASOURCE_TIMEOUT_QUERY, cursor.collect())
            .await
            .map_err(|_| {
                Error::Provider(format!(
                    "ClickHouse query timed out after {}s",
                    DATASOURCE_TIMEOUT_QUERY.as_secs()
                ))
            })?
            .map_err(|e| Error::Provider(format!("Failed to collect Arrow stream: {e}")))?;

        if collected.is_empty() {
            return Ok(Vec::new());
        }

        let reader = StreamReader::try_new(Cursor::new(collected.as_ref()), None)
            .map_err(|e| Error::Provider(format!("Failed to parse Arrow IPC: {e}")))?;

        let mut batches = Vec::new();
        for batch in reader {
            let batch =
                batch.map_err(|e| Error::Provider(format!("Failed to read Arrow batch: {e}")))?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Convert an Arrow schema to column metadata.
    fn schema_to_columns(schema: &SchemaRef) -> Vec<ColumnInfo> {
        schema
            .fields()
            .iter()
            .map(|field| ColumnInfo {
                name: field.name().clone(),
                col_type: Self::arrow_type_to_simple_type(field),
            })
            .collect()
    }

    /// Map an Arrow field's data type to the canonical [`SimpleType`].
    fn arrow_type_to_simple_type(field: &Field) -> SimpleType {
        match field.data_type() {
            DataType::Boolean => SimpleType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => SimpleType::Number,
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Dictionary(_, _) => SimpleType::String,
            DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _) => SimpleType::Date,
            // Binary and all other types map to String for wire compatibility.
            _ => SimpleType::String,
        }
    }

    /// Combine multiple RecordBatches into a single batch.
    fn combine_batches(
        batches: Vec<RecordBatch>,
    ) -> kyomi_connect_protocol::Result<Option<RecordBatch>> {
        if batches.is_empty() {
            return Ok(None);
        }
        if batches.len() == 1 {
            return Ok(Some(batches.into_iter().next().unwrap()));
        }

        let schema = batches[0].schema();
        let combined = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Provider(format!("Failed to combine RecordBatches: {e}")))?;
        Ok(Some(combined))
    }

    /// Extract string values from a single column of an Arrow RecordBatch.
    fn extract_string_column(batch: &RecordBatch, col_idx: usize) -> Vec<String> {
        if col_idx >= batch.num_columns() {
            return Vec::new();
        }
        batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .map(|arr| {
                (0..arr.len())
                    .filter_map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i).to_string())
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for DataFusionClickHouseProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        self.query_arrow("SELECT 1").await?;
        Ok(true)
    }

    async fn execute_query(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
        _job_id: Option<&str>,
    ) -> kyomi_connect_protocol::Result<QueryResult> {
        let start = Instant::now();

        // Apply LIMIT/OFFSET if provided. ClickHouse supports LIMIT N OFFSET M.
        let sql_with_pagination = match (limit, offset) {
            (Some(l), Some(o)) => format!("{sql} LIMIT {l} OFFSET {o}"),
            (Some(l), None) => format!("{sql} LIMIT {l}"),
            (None, Some(o)) => format!("{sql} OFFSET {o}"),
            (None, None) => sql.to_string(),
        };

        let batches = self.query_arrow(&sql_with_pagination).await?;
        let batch = Self::combine_batches(batches)?;

        let columns = batch.as_ref().map(|b| Self::schema_to_columns(&b.schema()));

        let row_count = batch.as_ref().map(|b| b.num_rows() as u64).unwrap_or(0);

        let total_rows = if include_total {
            let count_sql = format!("SELECT count(*) FROM ({sql})");
            match self.query_arrow(&count_sql).await {
                Ok(count_batches) => Self::combine_batches(count_batches)
                    .ok()
                    .flatten()
                    .and_then(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .map(|arr| arr.value(0))
                    }),
                Err(_) => None,
            }
        } else {
            None
        };

        // Determine if there are more rows beyond this page.
        let has_more = match limit {
            Some(l) => row_count == l as u64,
            None => false,
        };

        let execution_time_ms = start.elapsed().as_millis() as i64;

        Ok(QueryResult {
            status: QueryStatus::Success,
            columns,
            total_rows,
            has_more,
            bytes_processed: None,
            execution_time_ms: Some(execution_time_ms),
            error: None,
            record_batch: batch,
            job_id: None,
        })
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        // Use DataFusion's SQL parser (sqlparser-rs) with ClickHouse dialect
        // for syntax validation without executing the query.
        use datafusion::sql::parser::DFParser;
        use datafusion::sql::sqlparser::dialect::ClickHouseDialect;

        let dialect = ClickHouseDialect {};
        match DFParser::parse_sql_with_dialect(sql, &dialect) {
            Ok(_) => Ok(DryRunResult::success("Query syntax is valid")),
            Err(e) => Ok(DryRunResult::failure(e.to_string(), None, None)),
        }
    }

    async fn list_databases(&self) -> DiscoveryResult {
        match self
            .query_arrow(
                "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')",
            )
            .await
        {
            Ok(batches) => {
                let items = Self::combine_batches(batches)
                    .ok()
                    .flatten()
                    .map(|b| Self::extract_string_column(&b, 0))
                    .unwrap_or_default();

                DiscoveryResult { items, error: None }
            }
            Err(e) => DiscoveryResult {
                items: vec![],
                error: Some(e.to_string()),
            },
        }
    }

    async fn close(&self) {
        // The clickhouse::Client is dropped when the provider is dropped.
        // No explicit cleanup needed.
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn schema_to_columns_maps_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, true),
            Field::new(
                "created",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        let columns = DataFusionClickHouseProvider::schema_to_columns(&schema);
        assert_eq!(columns.len(), 5);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].col_type, SimpleType::Number);
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].col_type, SimpleType::String);
        assert_eq!(columns[2].name, "active");
        assert_eq!(columns[2].col_type, SimpleType::Boolean);
        assert_eq!(columns[3].name, "score");
        assert_eq!(columns[3].col_type, SimpleType::Number);
        assert_eq!(columns[4].name, "created");
        assert_eq!(columns[4].col_type, SimpleType::Date);
    }

    #[test]
    fn combine_batches_empty() {
        let result = DataFusionClickHouseProvider::combine_batches(vec![]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn combine_batches_single() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let result = DataFusionClickHouseProvider::combine_batches(vec![batch]).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn combine_batches_multiple() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let b1 = RecordBatch::new_empty(schema.clone());
        let b2 = RecordBatch::new_empty(schema);
        let result = DataFusionClickHouseProvider::combine_batches(vec![b1, b2]).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn dry_run_valid_sql() {
        use datafusion::sql::parser::DFParser;
        use datafusion::sql::sqlparser::dialect::ClickHouseDialect;

        let dialect = ClickHouseDialect {};
        assert!(DFParser::parse_sql_with_dialect("SELECT 1", &dialect).is_ok());
        assert!(
            DFParser::parse_sql_with_dialect("SELECT * FROM users WHERE id = 1", &dialect).is_ok()
        );
    }

    #[test]
    fn dry_run_invalid_sql() {
        use datafusion::sql::parser::DFParser;
        use datafusion::sql::sqlparser::dialect::ClickHouseDialect;

        let dialect = ClickHouseDialect {};
        assert!(DFParser::parse_sql_with_dialect("SELCT * FORM users", &dialect).is_err());
    }

    #[test]
    fn arrow_type_mapping_coverage() {
        let test_cases = vec![
            (DataType::Boolean, SimpleType::Boolean),
            (DataType::Int8, SimpleType::Number),
            (DataType::Int16, SimpleType::Number),
            (DataType::Int32, SimpleType::Number),
            (DataType::Int64, SimpleType::Number),
            (DataType::UInt8, SimpleType::Number),
            (DataType::UInt16, SimpleType::Number),
            (DataType::UInt32, SimpleType::Number),
            (DataType::UInt64, SimpleType::Number),
            (DataType::Float32, SimpleType::Number),
            (DataType::Float64, SimpleType::Number),
            (DataType::Utf8, SimpleType::String),
            (DataType::LargeUtf8, SimpleType::String),
            (DataType::Date32, SimpleType::Date),
            (DataType::Date64, SimpleType::Date),
            (
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                SimpleType::Date,
            ),
        ];

        for (dt, expected) in test_cases {
            let field = Field::new("test", dt, true);
            let actual = DataFusionClickHouseProvider::arrow_type_to_simple_type(&field);
            assert_eq!(actual, expected, "Type mapping failed for field: {field:?}");
        }
    }

    #[test]
    fn extract_string_column_out_of_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let result = DataFusionClickHouseProvider::extract_string_column(&batch, 99);
        assert!(result.is_empty());
    }

    #[test]
    fn has_more_logic() {
        // When limit is set and row_count matches, has_more should be true.
        let limit: Option<u32> = Some(10);
        let row_count: u64 = 10;
        assert!(limit.map_or(false, |l| row_count == l as u64));

        // When limit is set but row_count is less, has_more should be false.
        let row_count: u64 = 5;
        assert!(!limit.map_or(false, |l| row_count == l as u64));

        // When limit is None, has_more should be false.
        let limit: Option<u32> = None;
        assert!(!limit.map_or(false, |l| row_count == l as u64));
    }
}
