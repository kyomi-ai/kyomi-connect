//! PostgreSQL datasource provider using `sqlx`.
//!
//! Implements query execution for PostgreSQL databases with optional SSH tunnel
//! support. Connects via `sqlx::PgPool` and maps column types using
//! [`crate::type_mapping::map_postgres_type_oid`].
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `host` | string | `"localhost"` | PostgreSQL server hostname |
//! | `port` | int | `5432` | PostgreSQL port |
//! | `database` | string | `"postgres"` | Database name |
//! | `ssl_mode` | string | `"require"` | `disable`, `require`, `verify-ca`, `verify-full` |
//! | `ssh_enabled` | bool | `false` | Whether to use SSH tunnel |
//! | `ssh_host` | string | — | Bastion host for SSH tunnel |
//! | `ssh_port` | int | `22` | SSH port |
//! | `ssh_username` | string | — | SSH username |
//! | `ssh_private_key` | string | — | PEM-encoded SSH private key |
//!
//! ## Credentials
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `username` | string | PostgreSQL username |
//! | `password` | string | PostgreSQL password |

use std::time::Instant;

use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{Column, PgPool, Row, TypeInfo};

use crate::provider::{
    ColumnInfo, DatasourceProvider, DryRunResult, QueryResult, QueryStatus, SimpleType,
};
#[cfg(feature = "ssh")]
use crate::ssh_tunnel::{SshTunnel, SshTunnelConfig};
use crate::type_mapping::map_postgres_type_oid;

use kyomi_connect_protocol::Error;

/// Default PostgreSQL port.
const DEFAULT_PORT: u16 = 5432;
/// Default SSL mode.
const DEFAULT_SSL_MODE: &str = "require";
/// Default database name.
const DEFAULT_DATABASE: &str = "postgres";

/// PostgreSQL datasource provider.
///
/// Manages a connection pool (`PgPool`) and an optional SSH tunnel.
/// Implements the full [`DatasourceProvider`] trait including query
/// execution with pagination, dry-run validation via `EXPLAIN`, and
/// graceful resource cleanup.
pub struct PostgresProvider {
    /// Connection pool.
    pool: PgPool,
    /// SSH tunnel, if configured. Held to keep the tunnel alive.
    #[cfg(feature = "ssh")]
    _ssh_tunnel: Option<SshTunnel>,
}

impl PostgresProvider {
    /// Create a new PostgreSQL provider from connection config and credentials.
    ///
    /// Parses connection parameters, optionally sets up an SSH tunnel, configures
    /// SSL, and creates a connection pool.
    ///
    /// # Arguments
    ///
    /// * `connection_config` - Datasource-level configuration JSON.
    /// * `credentials` - Decrypted user-level credentials JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials are missing, SSH tunnel setup fails,
    /// or the connection pool cannot be created.
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
            .unwrap_or(DEFAULT_DATABASE);

        let ssl_mode_str = connection_config
            .get("ssl_mode")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_SSL_MODE);

        let username = credentials
            .get("username")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider("PostgreSQL requires a username".into()))?;

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

        // When using SSH tunnel, SSL is unnecessary (tunnel provides encryption)
        #[cfg(feature = "ssh")]
        let effective_ssl_mode = if ssh_tunnel.is_some() {
            "disable"
        } else {
            ssl_mode_str
        };
        #[cfg(not(feature = "ssh"))]
        let effective_ssl_mode = ssl_mode_str;

        let ssl_mode = parse_pg_ssl_mode(effective_ssl_mode);

        tracing::info!(
            host = host,
            port = port,
            database = database,
            ssl_mode = effective_ssl_mode,
            "Connecting to PostgreSQL"
        );

        let mut connect_options = PgConnectOptions::new()
            .host(&host)
            .port(port)
            .database(database)
            .username(username)
            .password(password)
            .ssl_mode(ssl_mode);

        // If a CA certificate path is specified, tell sqlx to use it for
        // server certificate verification (verify-ca / verify-full modes).
        if let Some(ssl_ca) = connection_config.get("ssl_ca").and_then(|v| v.as_str()) {
            connect_options = connect_options.ssl_root_cert(ssl_ca);
        }

        let pool = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            PgPool::connect_with(connect_options),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "PostgreSQL connection timed out after {}s",
                crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("PostgreSQL connection failed: {e}")))?;

        Ok(Self {
            pool,
            #[cfg(feature = "ssh")]
            _ssh_tunnel: ssh_tunnel,
        })
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for PostgresProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        let result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            sqlx::query("SELECT 1").execute(&self.pool),
        )
        .await
        .map_err(|_| Error::Internal("PostgreSQL test connection timed out".into()))?
        .map_err(|e| Error::Internal(format!("PostgreSQL test connection failed: {e}")))?;

        // If execute succeeded, connection is working
        let _ = result;
        Ok(true)
    }

    async fn execute_query(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
    ) -> kyomi_connect_protocol::Result<QueryResult> {
        let start = Instant::now();

        let prepared = super::sqlx_common::prepare_query(sql, limit, offset);

        // Get total count if requested (only for SELECT/WITH queries)
        let total_rows = if prepared.is_select && include_total {
            get_total_count(&self.pool, &prepared.sql_stripped).await
        } else {
            None
        };

        let paginated_sql = &prepared.sql;
        let effective_limit = limit.unwrap_or(1000);

        tracing::debug!(sql = %paginated_sql.chars().take(200).collect::<String>(), "Executing PostgreSQL query");

        // Execute the query with timeout
        let query_result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_QUERY,
            sqlx::query(paginated_sql).fetch_all(&self.pool),
        )
        .await;

        let rows_result = match query_result {
            Ok(Ok(rows)) => rows,
            Ok(Err(e)) => {
                tracing::error!(error = %e, "PostgreSQL query error");
                return Ok(QueryResult {
                    status: QueryStatus::Error,
                    columns: None,
                    rows: None,
                    total_rows: None,
                    has_more: false,
                    bytes_processed: None,
                    execution_time_ms: Some(start.elapsed().as_millis() as i64),
                    error: Some(e.to_string()),
                    record_batch: None,
                });
            }
            Err(_) => {
                return Ok(QueryResult {
                    status: QueryStatus::Error,
                    columns: None,
                    rows: None,
                    total_rows: None,
                    has_more: false,
                    bytes_processed: None,
                    execution_time_ms: Some(start.elapsed().as_millis() as i64),
                    error: Some(format!(
                        "Query timed out after {}s",
                        crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                    )),
                    record_batch: None,
                });
            }
        };

        // Extract column info from the first row (or the query metadata)
        let columns = if let Some(first_row) = rows_result.first() {
            first_row
                .columns()
                .iter()
                .map(|col| {
                    let oid = col_type_oid(col);
                    ColumnInfo {
                        name: col.name().to_string(),
                        col_type: map_postgres_type_oid(oid),
                    }
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Build Arrow RecordBatch alongside JSON rows (only when there are rows).
        let mut arrow_builder = if !columns.is_empty() {
            Some(crate::arrow_builder::ArrowResultBuilder::new(&columns))
        } else {
            None
        };

        // Convert rows to JSON values and populate the Arrow builder in one pass.
        let mut json_rows = Vec::with_capacity(rows_result.len());
        for row in &rows_result {
            let mut row_values = Vec::with_capacity(columns.len());
            for (i, col_info) in columns.iter().enumerate() {
                let value = pg_row_value_to_json(row, i, col_info.col_type);
                row_values.push(value);
            }
            json_rows.push(row_values);

            if let Some(ref mut builder) = arrow_builder {
                pg_row_to_arrow(row, &columns, builder);
            }
        }

        let record_batch = arrow_builder.and_then(|builder| {
            builder.finish().map_err(|e| {
                tracing::warn!(error = %e, "PostgreSQL Arrow batch construction failed; falling back to JSON-only");
                e
            }).ok()
        });

        let has_more = json_rows.len() == effective_limit as usize;
        let execution_time_ms = start.elapsed().as_millis() as i64;

        Ok(QueryResult {
            status: QueryStatus::Success,
            columns: Some(columns),
            rows: Some(json_rows),
            total_rows,
            has_more,
            bytes_processed: None,
            execution_time_ms: Some(execution_time_ms),
            error: None,
            record_batch,
        })
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        let explain_sql = format!("EXPLAIN {sql}");

        let result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_DRY_RUN,
            sqlx::query(&explain_sql).fetch_all(&self.pool),
        )
        .await;

        match result {
            Ok(Ok(_)) => Ok(DryRunResult::success("Query valid")),
            Ok(Err(e)) => {
                let (line, column) = parse_pg_error_position(&e, sql);
                Ok(DryRunResult::failure(e.to_string(), line, column))
            }
            Err(_) => Ok(DryRunResult::failure(
                format!(
                    "Dry run timed out after {}s",
                    crate::DATASOURCE_TIMEOUT_DRY_RUN.as_secs()
                ),
                None,
                None,
            )),
        }
    }

    async fn execute_query_stream(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
        chunk_size: Option<u32>,
    ) -> kyomi_connect_protocol::Result<kyomi_connect_protocol::QueryStream> {
        let start = Instant::now();
        let chunk_size = chunk_size.unwrap_or(100) as usize;

        let prepared = super::sqlx_common::prepare_query(sql, limit, offset);

        // Get total count if requested (only for SELECT/WITH queries)
        let total_rows = if prepared.is_select && include_total {
            get_total_count(&self.pool, &prepared.sql_stripped).await
        } else {
            None
        };

        tracing::debug!(
            sql = %prepared.sql.chars().take(200).collect::<String>(),
            "Streaming PostgreSQL query"
        );

        let paginated_sql = prepared.sql;
        let pool = self.pool.clone();

        let (tx, stream) = super::sqlx_common::make_stream_channel();

        tokio::spawn(async move {
            let row_stream = sqlx::query(&paginated_sql).fetch(&pool);
            super::sqlx_common::drive_sqlx_stream(
                tx,
                row_stream,
                total_rows,
                chunk_size,
                start,
                |row: &sqlx::postgres::PgRow| {
                    row.columns()
                        .iter()
                        .map(|col| {
                            let oid = col_type_oid(col);
                            ColumnInfo {
                                name: col.name().to_string(),
                                col_type: map_postgres_type_oid(oid),
                            }
                        })
                        .collect()
                },
                |row: &sqlx::postgres::PgRow, columns: &[ColumnInfo]| {
                    let mut row_values = Vec::with_capacity(columns.len());
                    for (i, col_info) in columns.iter().enumerate() {
                        row_values.push(pg_row_value_to_json(row, i, col_info.col_type));
                    }
                    row_values
                },
            )
            .await;
        });

        Ok(stream)
    }

    async fn list_databases(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query(
                "SELECT datname FROM pg_database \
                 WHERE datistemplate = false \
                 ORDER BY datname",
                None,
                None,
                false,
            )
            .await
        {
            Ok(result) => {
                let items: Vec<String> = result
                    .rows
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .filter_map(|row| row.first().and_then(|v| v.as_str()).map(String::from))
                    .collect();
                crate::provider::DiscoveryResult { items, error: None }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list PostgreSQL databases: {e}")),
            },
        }
    }

    async fn list_schemas(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query(
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                 ORDER BY schema_name",
                None,
                None,
                false,
            )
            .await
        {
            Ok(result) => {
                let items: Vec<String> = result
                    .rows
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .filter_map(|row| row.first().and_then(|v| v.as_str()).map(String::from))
                    .collect();
                crate::provider::DiscoveryResult { items, error: None }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list PostgreSQL schemas: {e}")),
            },
        }
    }

    async fn close(&self) {
        self.pool.close().await;
        tracing::debug!("PostgreSQL connection pool closed");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a PostgreSQL SSL mode string to the sqlx enum.
fn parse_pg_ssl_mode(mode: &str) -> PgSslMode {
    match mode {
        "disable" => PgSslMode::Disable,
        "prefer" => PgSslMode::Prefer,
        "require" => PgSslMode::Require,
        "verify-ca" => PgSslMode::VerifyCa,
        "verify-full" => PgSslMode::VerifyFull,
        _ => {
            tracing::warn!(
                mode = mode,
                "Unknown PostgreSQL ssl_mode, defaulting to Require"
            );
            PgSslMode::Require
        }
    }
}

/// Get total row count for a SELECT query.
///
/// Wraps the query in `SELECT COUNT(*) FROM (...) sub`. If the count query
/// fails (e.g., because of query complexity), returns `None` silently.
async fn get_total_count(pool: &PgPool, sql: &str) -> Option<i64> {
    let count_sql = format!("SELECT COUNT(*) FROM ({sql}) AS _count_subquery");

    match tokio::time::timeout(
        crate::DATASOURCE_TIMEOUT_QUERY,
        sqlx::query_scalar::<_, i64>(&count_sql).fetch_one(pool),
    )
    .await
    {
        Ok(Ok(count)) => Some(count),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "Failed to get total count, continuing without it");
            None
        }
        Err(_) => {
            tracing::warn!("Total count query timed out, continuing without it");
            None
        }
    }
}

/// Extract the type OID from a PostgreSQL column.
///
/// Uses `PgTypeInfo` to get the OID. Falls back to 0 (unknown) if the
/// type info is not available.
fn col_type_oid(col: &sqlx::postgres::PgColumn) -> u32 {
    // sqlx PgTypeInfo exposes the OID via its name matching.
    // We map using the type name string since sqlx doesn't expose raw OID directly.
    let type_info = col.type_info();
    pg_type_name_to_oid(type_info.name())
}

/// Map a PostgreSQL type name (as returned by sqlx) to its OID for use with
/// our `map_postgres_type_oid` function.
///
/// sqlx returns type names like "BOOL", "INT4", "TEXT", etc.
///
/// Also used by the Redshift provider (which shares the PostgreSQL wire protocol).
pub(crate) fn pg_type_name_to_oid(name: &str) -> u32 {
    match name.to_uppercase().as_str() {
        "BOOL" => 16,
        "INT2" | "SMALLINT" | "SMALLSERIAL" => 21,
        "INT4" | "INT" | "INTEGER" | "SERIAL" => 23,
        "INT8" | "BIGINT" | "BIGSERIAL" => 20,
        "OID" => 26,
        "FLOAT4" | "REAL" => 700,
        "FLOAT8" | "DOUBLE PRECISION" => 701,
        "NUMERIC" | "DECIMAL" => 1700,
        "CHAR" | "\"CHAR\"" => 18,
        "NAME" => 19,
        "TEXT" => 25,
        "BPCHAR" => 1042,
        "VARCHAR" | "CHARACTER VARYING" => 1043,
        "DATE" => 1082,
        "TIME" | "TIME WITHOUT TIME ZONE" => 1083,
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => 1114,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => 1184,
        "JSON" => 114,
        "JSONB" => 3802,
        "UUID" => 2950,
        "TEXT[]" | "_TEXT" => 1009,
        "VARCHAR[]" | "_VARCHAR" => 1015,
        "INT8[]" | "_INT8" => 1016,
        "INT4[]" | "_INT4" => 1007,
        "INTERVAL" => 1186,
        "BYTEA" => 17,
        "MONEY" => 790,
        _ => 0, // Unknown — will map to SimpleType::Unknown
    }
}

/// Extract a value from a PostgreSQL row at the given index and convert to JSON.
///
/// Uses the mapped [`SimpleType`] to determine the correct Rust type to
/// `try_get` from the row. Falls back to `Value::Null` for any extraction error.
///
/// Format a PostgreSQL INTERVAL as a human-readable string.
///
/// PgInterval stores months, days, and microseconds separately. We produce
/// the same format PostgreSQL uses in text output (e.g. "1 year 2 mons 3 days 04:05:06").
fn format_pg_interval(iv: &sqlx::postgres::types::PgInterval) -> String {
    let mut parts = Vec::new();

    if iv.months != 0 {
        let years = iv.months / 12;
        let mons = iv.months % 12;
        if years != 0 {
            parts.push(format!(
                "{years} year{}",
                if years.abs() != 1 { "s" } else { "" }
            ));
        }
        if mons != 0 {
            parts.push(format!(
                "{mons} mon{}",
                if mons.abs() != 1 { "s" } else { "" }
            ));
        }
    }
    if iv.days != 0 {
        parts.push(format!(
            "{} day{}",
            iv.days,
            if iv.days.abs() != 1 { "s" } else { "" }
        ));
    }
    if iv.microseconds != 0 {
        let total_secs = iv.microseconds / 1_000_000;
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        parts.push(format!("{hours:02}:{mins:02}:{secs:02}"));
    }

    if parts.is_empty() {
        "00:00:00".to_string()
    } else {
        parts.join(" ")
    }
}

/// Also used by the Redshift provider (which shares the PostgreSQL wire protocol).
pub(crate) fn pg_row_value_to_json(
    row: &sqlx::postgres::PgRow,
    idx: usize,
    col_type: SimpleType,
) -> Value {
    // First try to detect NULL regardless of type
    // sqlx returns an error for NULL values when try_get expects a non-Option type
    match col_type {
        SimpleType::Boolean => row
            .try_get::<Option<bool>, _>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),

        SimpleType::Number => {
            // sqlx decodes int2→i16, int4→i32, int8→i64. Try all three.
            if let Ok(Some(v)) = row.try_get::<Option<i32>, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<Option<i16>, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<Option<f32>, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<Option<rust_decimal::Decimal>, _>(idx) {
                // NUMERIC/DECIMAL — lossless decode, convert to f64 for JSON
                match v.to_string().parse::<f64>() {
                    Ok(f) => serde_json::json!(f),
                    Err(_) => Value::Null,
                }
            } else if let Ok(Some(v)) =
                row.try_get::<Option<sqlx::postgres::types::PgMoney>, _>(idx)
            {
                // MONEY — stored as i64 cents, convert to decimal
                serde_json::json!(v.0 as f64 / 100.0)
            } else {
                Value::Null
            }
        }

        SimpleType::String => {
            if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
                Value::String(v)
            } else if let Ok(Some(v)) =
                row.try_get::<Option<sqlx::postgres::types::PgInterval>, _>(idx)
            {
                // INTERVAL — format as human-readable string
                Value::String(format_pg_interval(&v))
            } else if let Ok(Some(v)) = row.try_get::<Option<Vec<u8>>, _>(idx) {
                // BYTEA — hex-encode for display
                Value::String(format!("\\x{}", hex::encode(&v)))
            } else {
                Value::Null
            }
        }

        SimpleType::Date => {
            if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveDate>, _>(idx) {
                Value::String(v.format("%Y-%m-%d").to_string())
            } else {
                Value::Null
            }
        }

        SimpleType::Time => {
            if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveTime>, _>(idx) {
                Value::String(v.format("%H:%M:%S").to_string())
            } else {
                Value::Null
            }
        }

        SimpleType::Timestamp => {
            if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveDateTime>, _>(idx) {
                Value::String(v.format("%Y-%m-%dT%H:%M:%S").to_string())
            } else {
                Value::Null
            }
        }

        SimpleType::TimestampTz => {
            if let Ok(Some(v)) = row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(idx) {
                Value::String(v.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            } else {
                Value::Null
            }
        }

        SimpleType::Unknown => {
            // Try string as a safe fallback
            row.try_get::<Option<String>, _>(idx)
                .ok()
                .flatten()
                .map(Value::String)
                .unwrap_or(Value::Null)
        }
    }
}

/// Append a PostgreSQL row's values directly to an [`ArrowResultBuilder`].
///
/// This is the Arrow counterpart of [`pg_row_value_to_json`]. Instead of
/// creating `serde_json::Value` intermediaries, native Rust types go directly
/// into Arrow column builders, preserving date/time/timestamp precision.
///
/// Also used by the Redshift provider (which shares the PostgreSQL wire protocol).
pub(crate) fn pg_row_to_arrow(
    row: &sqlx::postgres::PgRow,
    columns: &[ColumnInfo],
    builder: &mut crate::arrow_builder::ArrowResultBuilder,
) {
    use crate::provider::SimpleType;

    for (idx, col) in columns.iter().enumerate() {
        match col.col_type {
            SimpleType::Boolean => match row.try_get::<Option<bool>, _>(idx) {
                Ok(Some(v)) => builder.append_bool(idx, v),
                _ => builder.append_null(idx),
            },
            SimpleType::Number => {
                // Try i32 → i64 → i16 → f32 → f64 → Decimal → PgMoney
                if let Ok(Some(v)) = row.try_get::<Option<i32>, _>(idx) {
                    builder.append_i64(idx, v as i64);
                } else if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) {
                    builder.append_i64(idx, v);
                } else if let Ok(Some(v)) = row.try_get::<Option<i16>, _>(idx) {
                    builder.append_i64(idx, v as i64);
                } else if let Ok(Some(v)) = row.try_get::<Option<f32>, _>(idx) {
                    builder.append_f64(idx, v as f64);
                } else if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
                    builder.append_f64(idx, v);
                } else if let Ok(Some(v)) =
                    row.try_get::<Option<rust_decimal::Decimal>, _>(idx)
                {
                    if let Ok(f) = v.to_string().parse::<f64>() {
                        builder.append_f64(idx, f);
                    } else {
                        builder.append_null(idx);
                    }
                } else if let Ok(Some(v)) =
                    row.try_get::<Option<sqlx::postgres::types::PgMoney>, _>(idx)
                {
                    builder.append_f64(idx, v.0 as f64 / 100.0);
                } else {
                    builder.append_null(idx);
                }
            }
            SimpleType::String => {
                if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
                    builder.append_string(idx, &v);
                } else if let Ok(Some(v)) =
                    row.try_get::<Option<sqlx::postgres::types::PgInterval>, _>(idx)
                {
                    builder.append_string(idx, &format_pg_interval(&v));
                } else if let Ok(Some(v)) = row.try_get::<Option<Vec<u8>>, _>(idx) {
                    builder.append_string(idx, &format!("\\x{}", hex::encode(&v)));
                } else {
                    builder.append_null(idx);
                }
            }
            SimpleType::Date => {
                if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveDate>, _>(idx) {
                    builder.append_naive_date(idx, v);
                } else {
                    builder.append_null(idx);
                }
            }
            SimpleType::Time => {
                if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveTime>, _>(idx) {
                    builder.append_naive_time(idx, v);
                } else {
                    builder.append_null(idx);
                }
            }
            SimpleType::Timestamp => {
                if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveDateTime>, _>(idx) {
                    builder.append_naive_datetime(idx, v);
                } else {
                    builder.append_null(idx);
                }
            }
            SimpleType::TimestampTz => {
                if let Ok(Some(v)) =
                    row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(idx)
                {
                    builder.append_datetime_utc(idx, v);
                } else {
                    builder.append_null(idx);
                }
            }
            SimpleType::Unknown => {
                if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
                    builder.append_string(idx, &v);
                } else {
                    builder.append_null(idx);
                }
            }
        }
    }
    builder.finish_row();
}

/// Parse PostgreSQL error for line/column position.
///
/// PostgreSQL errors include `statement_position` as a character offset.
/// We convert that to a (line, column) pair by counting newlines in the SQL.
fn parse_pg_error_position(error: &sqlx::Error, sql: &str) -> (Option<u32>, Option<u32>) {
    if let sqlx::Error::Database(db_err) = error {
        // sqlx DatabaseError doesn't directly expose statement_position,
        // but the error message often contains "at character N" or similar.
        let msg = db_err.message();

        // Try to find position info in the error message
        // PostgreSQL format: "... at character 42"
        if let Some(pos) = extract_character_position(msg) {
            return char_position_to_line_col(sql, pos);
        }

        // Also check the full error string representation
        let full_msg = error.to_string();
        if let Some(pos) = extract_character_position(&full_msg) {
            return char_position_to_line_col(sql, pos);
        }
    }

    (None, None)
}

/// Extract a character position from a PostgreSQL error message.
///
/// Looks for patterns like "at character 42" in the message.
fn extract_character_position(msg: &str) -> Option<usize> {
    // Pattern: "at character N" (PostgreSQL standard format)
    let pattern = "at character ";
    if let Some(idx) = msg.find(pattern) {
        let start = idx + pattern.len();
        let num_str: String = msg[start..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(pos) = num_str.parse::<usize>() {
            // PostgreSQL positions are 1-indexed
            return Some(pos);
        }
    }
    None
}

/// Convert a character position (1-indexed) to a (line, column) pair.
///
/// Also used by the Redshift provider for error position parsing.
pub(crate) fn char_position_to_line_col(sql: &str, char_pos: usize) -> (Option<u32>, Option<u32>) {
    if char_pos == 0 || char_pos > sql.len() {
        return (None, None);
    }

    let prefix = &sql[..char_pos.saturating_sub(1)];
    let line = prefix.chars().filter(|&c| c == '\n').count() as u32 + 1;

    // Column is position within the current line
    let last_newline = prefix.rfind('\n').map(|p| p + 1).unwrap_or(0);
    let column = (char_pos - last_newline) as u32;

    (Some(line), Some(column))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ssl_mode() {
        assert!(matches!(parse_pg_ssl_mode("disable"), PgSslMode::Disable));
        assert!(matches!(parse_pg_ssl_mode("prefer"), PgSslMode::Prefer));
        assert!(matches!(parse_pg_ssl_mode("require"), PgSslMode::Require));
        assert!(matches!(
            parse_pg_ssl_mode("verify-ca"),
            PgSslMode::VerifyCa
        ));
        assert!(matches!(
            parse_pg_ssl_mode("verify-full"),
            PgSslMode::VerifyFull
        ));
        // Unknown defaults to Require
        assert!(matches!(parse_pg_ssl_mode("unknown"), PgSslMode::Require));
    }

    #[test]
    fn pg_type_name_to_oid_common_types() {
        assert_eq!(pg_type_name_to_oid("BOOL"), 16);
        assert_eq!(pg_type_name_to_oid("INT4"), 23);
        assert_eq!(pg_type_name_to_oid("INT8"), 20);
        assert_eq!(pg_type_name_to_oid("TEXT"), 25);
        assert_eq!(pg_type_name_to_oid("VARCHAR"), 1043);
        assert_eq!(pg_type_name_to_oid("TIMESTAMP"), 1114);
        assert_eq!(pg_type_name_to_oid("TIMESTAMPTZ"), 1184);
        assert_eq!(pg_type_name_to_oid("UUID"), 2950);
        assert_eq!(pg_type_name_to_oid("JSONB"), 3802);
    }

    #[test]
    fn pg_type_name_unknown() {
        assert_eq!(pg_type_name_to_oid("SOME_CUSTOM_TYPE"), 0);
    }

    #[test]
    fn char_position_to_line_col_single_line() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let (line, col) = char_position_to_line_col(sql, 8);
        assert_eq!(line, Some(1));
        assert_eq!(col, Some(8));
    }

    #[test]
    fn char_position_to_line_col_multi_line() {
        let sql = "SELECT *\nFROM users\nWHERE id = 1";
        // Position 15 should be on line 2 (F=10, R=11, O=12, M=13, ' '=14, u=15)
        let (line, col) = char_position_to_line_col(sql, 15);
        assert_eq!(line, Some(2));
        assert_eq!(col, Some(6));
    }

    #[test]
    fn char_position_to_line_col_out_of_range() {
        let sql = "SELECT 1";
        let (line, col) = char_position_to_line_col(sql, 0);
        assert_eq!(line, None);
        assert_eq!(col, None);

        let (line, col) = char_position_to_line_col(sql, 100);
        assert_eq!(line, None);
        assert_eq!(col, None);
    }

    #[test]
    fn extract_character_position_found() {
        let msg = "ERROR: syntax error at character 42";
        assert_eq!(extract_character_position(msg), Some(42));
    }

    #[test]
    fn extract_character_position_not_found() {
        let msg = "ERROR: syntax error near 'FORM'";
        assert_eq!(extract_character_position(msg), None);
    }
}
