//! MySQL datasource provider using `sqlx`.
//!
//! Implements query execution for MySQL databases with optional SSH tunnel
//! support. Connects via `sqlx::MySqlPool` and maps column types using
//! [`crate::type_mapping::map_mysql_type_name`].
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `host` | string | `"localhost"` | MySQL server hostname |
//! | `port` | int | `3306` | MySQL port |
//! | `database` | string | `""` | Database name (optional, needed for queries) |
//! | `ssl_mode` | string | `"require"` | `disable`, `preferred`, `require`, `verify-ca`, `verify-full` |
//! | `ssl_ca` | string | — | PEM-encoded CA certificate (required for `verify-ca` / `verify-full`) |
//! | `ssh_enabled` | bool | `false` | Whether to use SSH tunnel |
//!
//! ## Credentials
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `username` | string | MySQL username |
//! | `password` | string | MySQL password |

use std::sync::LazyLock;
use std::time::Instant;

use regex::Regex;
use serde_json::Value;
use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use sqlx::{Column, MySqlPool, Row, TypeInfo};

use crate::provider::{
    ColumnInfo, DatasourceProvider, DryRunResult, QueryResult, QueryStatus, SimpleType,
};
#[cfg(feature = "ssh")]
use crate::ssh_tunnel::{SshTunnel, SshTunnelConfig};
use crate::type_mapping::map_mysql_type_name;

use kyomi_connect_protocol::Error;

/// Default MySQL port.
const DEFAULT_PORT: u16 = 3306;
/// Default SSL mode.
const DEFAULT_SSL_MODE: &str = "require";

/// MySQL datasource provider.
///
/// Manages a connection pool (`MySqlPool`) and an optional SSH tunnel.
/// MySQL uses autocommit by default, so no explicit transaction management
/// is needed for query execution.
pub struct MySqlProvider {
    /// Connection pool.
    pool: MySqlPool,
    /// SSH tunnel, if configured. Held to keep the tunnel alive.
    #[cfg(feature = "ssh")]
    _ssh_tunnel: Option<SshTunnel>,
}

impl MySqlProvider {
    /// Create a new MySQL provider from connection config and credentials.
    ///
    /// Parses connection parameters, optionally sets up an SSH tunnel,
    /// configures SSL, and creates a connection pool.
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
            .unwrap_or("");

        let ssl_mode_str = connection_config
            .get("ssl_mode")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_SSL_MODE);

        let username = credentials
            .get("username")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider("MySQL requires a username".into()))?;

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
        let effective_ssl_mode_str = if ssh_tunnel.is_some() {
            "disable"
        } else {
            ssl_mode_str
        };
        #[cfg(not(feature = "ssh"))]
        let effective_ssl_mode_str = ssl_mode_str;

        let ssl_mode = parse_mysql_ssl_mode(effective_ssl_mode_str);

        tracing::info!(
            host = host,
            port = port,
            database = database,
            ssl_mode = effective_ssl_mode_str,
            "Connecting to MySQL"
        );

        let mut connect_options = MySqlConnectOptions::new()
            .host(&host)
            .port(port)
            .database(database)
            .username(username)
            .password(password)
            .charset("utf8mb4")
            .ssl_mode(ssl_mode);

        // For verify-ca / verify-full, attach the CA certificate if provided
        if matches!(
            ssl_mode,
            MySqlSslMode::VerifyCa | MySqlSslMode::VerifyIdentity
        ) && let Some(ssl_ca_pem) = connection_config.get("ssl_ca").and_then(|v| v.as_str())
        {
            connect_options = connect_options.ssl_ca_from_pem(ssl_ca_pem.as_bytes().to_vec());
        }

        let pool = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            MySqlPool::connect_with(connect_options),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "MySQL connection timed out after {}s",
                crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("MySQL connection failed: {e}")))?;

        Ok(Self {
            pool,
            #[cfg(feature = "ssh")]
            _ssh_tunnel: ssh_tunnel,
        })
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for MySqlProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        let result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            sqlx::query("SELECT 1").execute(&self.pool),
        )
        .await
        .map_err(|_| Error::Internal("MySQL test connection timed out".into()))?
        .map_err(|e| Error::Internal(format!("MySQL test connection failed: {e}")))?;

        let _ = result;
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

        let prepared = super::sqlx_common::prepare_query(sql, limit, offset);

        // Get total count if requested
        let total_rows = if prepared.is_select && include_total {
            get_total_count(&self.pool, &prepared.sql_stripped).await
        } else {
            None
        };

        let paginated_sql = &prepared.sql;
        let effective_limit = limit.unwrap_or(1000);

        tracing::debug!(sql = %paginated_sql.chars().take(200).collect::<String>(), "Executing MySQL query");

        // Execute with timeout
        let query_result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_QUERY,
            sqlx::query(paginated_sql).fetch_all(&self.pool),
        )
        .await;

        let rows_result = match query_result {
            Ok(Ok(rows)) => rows,
            Ok(Err(e)) => {
                tracing::error!(error = %e, "MySQL query error");
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
                    job_id: None,
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
                    job_id: None,
                });
            }
        };

        // Extract column info
        let columns = if let Some(first_row) = rows_result.first() {
            first_row
                .columns()
                .iter()
                .map(|col| ColumnInfo {
                    name: col.name().to_string(),
                    col_type: map_mysql_type_name(col.type_info().name()),
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Build Arrow RecordBatch (the sole data path — JSON rows are not populated).
        let mut arrow_builder = if !columns.is_empty() {
            Some(crate::arrow_builder::ArrowResultBuilder::new(&columns))
        } else {
            None
        };

        for row in &rows_result {
            if let Some(ref mut builder) = arrow_builder {
                mysql_row_to_arrow(row, &columns, builder);
            }
        }

        let record_batch = arrow_builder.and_then(|builder| {
            builder
                .finish()
                .map_err(|e| {
                    tracing::warn!(error = %e, "MySQL Arrow batch construction failed");
                    e
                })
                .ok()
        });

        let row_count = record_batch.as_ref().map_or(0, |b| b.num_rows());
        let has_more = row_count == effective_limit as usize;
        let execution_time_ms = start.elapsed().as_millis() as i64;

        Ok(QueryResult {
            status: QueryStatus::Success,
            columns: Some(columns),
            rows: None,
            total_rows,
            has_more,
            bytes_processed: None,
            execution_time_ms: Some(execution_time_ms),
            error: None,
            record_batch,
            job_id: None,
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
                let line = parse_mysql_error_line(&e);
                Ok(DryRunResult::failure(e.to_string(), line, None))
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

    async fn execute_query_stream_arrow(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
        chunk_size: Option<u32>,
    ) -> kyomi_connect_protocol::Result<kyomi_connect_protocol::ArrowStream> {
        let start = Instant::now();
        let chunk_size = chunk_size.unwrap_or(100) as usize;

        let prepared = super::sqlx_common::prepare_query(sql, limit, offset);

        // Get total count if requested
        let total_rows = if prepared.is_select && include_total {
            get_total_count(&self.pool, &prepared.sql_stripped).await
        } else {
            None
        };

        tracing::debug!(
            sql = %prepared.sql.chars().take(200).collect::<String>(),
            "Arrow-streaming MySQL query"
        );

        let paginated_sql = prepared.sql;
        let pool = self.pool.clone();

        let (tx, stream) = super::sqlx_common::make_arrow_stream_channel();

        tokio::spawn(async move {
            let row_stream = sqlx::query(&paginated_sql).fetch(&pool);
            super::sqlx_common::drive_sqlx_stream_arrow(
                tx,
                row_stream,
                total_rows,
                chunk_size,
                start,
                |row: &sqlx::mysql::MySqlRow| {
                    row.columns()
                        .iter()
                        .map(|col| ColumnInfo {
                            name: col.name().to_string(),
                            col_type: map_mysql_type_name(col.type_info().name()),
                        })
                        .collect()
                },
                |row: &sqlx::mysql::MySqlRow,
                 columns: &[ColumnInfo],
                 builder: &mut crate::arrow_builder::ArrowResultBuilder| {
                    mysql_row_to_arrow(row, columns, builder);
                },
            )
            .await;
        });

        Ok(stream)
    }

    async fn list_databases(&self) -> crate::provider::DiscoveryResult {
        // MySQL list_databases is an alias for list_schemas (MySQL uses "database" terminology)
        self.list_schemas().await
    }

    async fn list_schemas(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA \
                 WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
                 ORDER BY SCHEMA_NAME",
                None,
                None,
                false,
                None,
            )
            .await
        {
            Ok(result) => {
                let items = crate::provider::extract_string_col_from_batch(
                    result.record_batch.as_ref(),
                    0,
                );
                crate::provider::DiscoveryResult {
                    items,
                    error: None,
                }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list MySQL databases: {e}")),
            },
        }
    }

    async fn close(&self) {
        self.pool.close().await;
        tracing::debug!("MySQL connection pool closed");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a MySQL SSL mode string to the sqlx enum.
///
/// Supports the standard MySQL connection parameter values:
/// - `disable` — no SSL
/// - `preferred` — SSL if available, fallback to unencrypted
/// - `require` — SSL required, no certificate verification
/// - `verify-ca` — SSL with CA certificate verification
/// - `verify-full` — SSL with CA cert + hostname verification
fn parse_mysql_ssl_mode(mode: &str) -> MySqlSslMode {
    match mode {
        "disable" => MySqlSslMode::Disabled,
        "preferred" => MySqlSslMode::Preferred,
        "require" => MySqlSslMode::Required,
        "verify-ca" => MySqlSslMode::VerifyCa,
        "verify-full" => MySqlSslMode::VerifyIdentity,
        _ => {
            tracing::warn!(
                mode = mode,
                "Unknown MySQL ssl_mode, defaulting to Required"
            );
            MySqlSslMode::Required
        }
    }
}

/// Get total row count for a SELECT query.
async fn get_total_count(pool: &MySqlPool, sql: &str) -> Option<i64> {
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

/// Try to read a MySQL column value as raw bytes and interpret as a UTF-8 string.
///
/// MySQL 8.0's `information_schema` reports many VARCHAR columns as `VARBINARY`,
/// which sqlx cannot decode as `String` directly. The wire bytes are valid UTF-8
/// though, so this provides a reliable fallback.
fn mysql_try_get_bytes_as_string(row: &sqlx::mysql::MySqlRow, idx: usize) -> Option<String> {
    row.try_get::<Option<Vec<u8>>, _>(idx)
        .ok()
        .flatten()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// Convert a MySQL row directly to Arrow column builders.
///
/// Native Rust types go directly into Arrow column builders, preserving
/// date/time/timestamp precision.
pub(crate) fn mysql_row_to_arrow(
    row: &sqlx::mysql::MySqlRow,
    columns: &[ColumnInfo],
    builder: &mut crate::arrow_builder::ArrowResultBuilder,
) {
    for (idx, col) in columns.iter().enumerate() {
        match col.col_type {
            SimpleType::Boolean => match row.try_get::<Option<bool>, _>(idx) {
                Ok(Some(v)) => builder.append_bool(idx, v),
                _ => builder.append_null(idx),
            },
            SimpleType::Number => {
                // Try i64 → u64 (unsigned) → f64 → Decimal
                if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(idx) {
                    builder.append_i64(idx, v);
                } else if let Ok(Some(v)) = row.try_get::<Option<u64>, _>(idx) {
                    // MySQL UNSIGNED integer types (BIGINT UNSIGNED, etc.)
                    builder.append_f64(idx, v as f64);
                } else if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(idx) {
                    builder.append_f64(idx, v);
                } else if let Ok(Some(v)) = row.try_get::<Option<rust_decimal::Decimal>, _>(idx) {
                    if let Ok(f) = v.to_string().parse::<f64>() {
                        builder.append_f64(idx, f);
                    } else {
                        builder.append_null(idx);
                    }
                } else {
                    builder.append_null(idx);
                }
            }
            SimpleType::String => {
                if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
                    builder.append_string(idx, &v);
                } else if let Some(s) = mysql_try_get_bytes_as_string(row, idx) {
                    builder.append_string(idx, &s);
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
                // MySQL TIMESTAMP is stored as UTC; sqlx decodes as NaiveDateTime
                if let Ok(Some(v)) = row.try_get::<Option<chrono::NaiveDateTime>, _>(idx) {
                    builder.append_datetime_utc(idx, v.and_utc());
                } else {
                    builder.append_null(idx);
                }
            }
            SimpleType::Unknown => {
                if let Ok(Some(v)) = row.try_get::<Option<String>, _>(idx) {
                    builder.append_string(idx, &v);
                } else if let Some(s) = mysql_try_get_bytes_as_string(row, idx) {
                    builder.append_string(idx, &s);
                } else {
                    builder.append_null(idx);
                }
            }
        }
    }
    builder.finish_row();
}

/// Regex for MySQL "at line N" error pattern, compiled once.
static MYSQL_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)at line (\d+)").expect("MySQL line regex"));

/// Parse MySQL error for line number.
///
/// MySQL format: "... at line N"
fn parse_mysql_error_line(error: &sqlx::Error) -> Option<u32> {
    let msg = error.to_string();
    MYSQL_LINE_RE
        .captures(&msg)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ssl_mode_all_variants() {
        assert!(matches!(
            parse_mysql_ssl_mode("disable"),
            MySqlSslMode::Disabled
        ));
        assert!(matches!(
            parse_mysql_ssl_mode("preferred"),
            MySqlSslMode::Preferred
        ));
        assert!(matches!(
            parse_mysql_ssl_mode("require"),
            MySqlSslMode::Required
        ));
        assert!(matches!(
            parse_mysql_ssl_mode("verify-ca"),
            MySqlSslMode::VerifyCa
        ));
        assert!(matches!(
            parse_mysql_ssl_mode("verify-full"),
            MySqlSslMode::VerifyIdentity
        ));
    }

    #[test]
    fn parse_ssl_mode_unknown_defaults_to_required() {
        assert!(matches!(
            parse_mysql_ssl_mode("unknown"),
            MySqlSslMode::Required
        ));
        assert!(matches!(parse_mysql_ssl_mode(""), MySqlSslMode::Required));
    }

    #[test]
    fn parse_mysql_error_line_found() {
        // Simulate an error message that contains "at line N"
        let msg = "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'FORM users' at line 1";
        let re = Regex::new(r"(?i)at line (\d+)").expect("regex");
        let line = re
            .captures(msg)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<u32>().ok());
        assert_eq!(line, Some(1));
    }

    #[test]
    fn parse_mysql_error_line_multi() {
        let msg = "Some error at line 3";
        let re = Regex::new(r"(?i)at line (\d+)").expect("regex");
        let line = re
            .captures(msg)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<u32>().ok());
        assert_eq!(line, Some(3));
    }

    #[test]
    fn parse_mysql_error_line_not_found() {
        let msg = "Unknown column 'foo' in 'field list'";
        let re = Regex::new(r"(?i)at line (\d+)").expect("regex");
        let line = re
            .captures(msg)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<u32>().ok());
        assert_eq!(line, None);
    }
}
