//! ClickHouse datasource provider using the HTTP REST API.
//!
//! Implements query execution for ClickHouse databases using plain HTTP via
//! `reqwest`. ClickHouse exposes an HTTP interface on port 8123 (default)
//! that accepts SQL queries as POST body and returns results in various
//! formats. Buffered queries use `JSONCompact`; streaming queries use
//! `JSONCompactEachRowWithNamesAndTypes` for incremental line-by-line parsing.
//!
//! Supports optional SSH tunnel for databases behind firewalls.
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

use std::sync::LazyLock;
use std::time::Instant;

use futures_util::StreamExt;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use regex::Regex;
use serde_json::Value;

use crate::provider::{
    ColumnInfo, DatasourceProvider, DryRunResult, QueryResult, QueryStatus, SimpleType,
};
#[cfg(feature = "ssh")]
use crate::ssh_tunnel::{SshTunnel, SshTunnelConfig};
use crate::type_mapping::map_clickhouse_type;

use kyomi_connect_protocol::Error;
use kyomi_connect_protocol::QueryStreamEvent;

/// Default ClickHouse HTTP port.
const DEFAULT_PORT: u16 = 8123;
/// Default ClickHouse database.
const DEFAULT_DATABASE: &str = "default";
/// Default ClickHouse username.
const DEFAULT_USERNAME: &str = "default";

/// ClickHouse datasource provider.
///
/// Uses the ClickHouse HTTP interface with `reqwest` for stateless query
/// execution. Each query is a separate HTTP request — there is no persistent
/// connection to manage.
pub struct ClickHouseProvider {
    /// HTTP client for making requests.
    client: reqwest::Client,
    /// Base URL for ClickHouse HTTP API (e.g., `http://localhost:8123`).
    base_url: String,
    /// Database name.
    database: String,
    /// ClickHouse username.
    username: String,
    /// ClickHouse password.
    password: String,
    /// Whether the server timezone is UTC. Used to annotate DateTime strings
    /// with "Z" so JavaScript correctly interprets them as UTC.
    server_tz_is_utc: bool,
    /// SSH tunnel, if configured. Held to keep the tunnel alive.
    #[cfg(feature = "ssh")]
    _ssh_tunnel: Option<SshTunnel>,
}

impl ClickHouseProvider {
    /// Create a new ClickHouse provider from connection config and credentials.
    ///
    /// Parses connection parameters, optionally sets up an SSH tunnel,
    /// and constructs the HTTP base URL.
    ///
    /// # Errors
    ///
    /// Returns an error if SSH tunnel setup fails.
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
            .unwrap_or(DEFAULT_USERNAME)
            .to_string();

        let password = credentials
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

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
        let base_url = format!("{scheme}://{host}:{port}");

        tracing::info!(
            host = host,
            port = port,
            database = database,
            secure = effective_secure,
            "Connecting to ClickHouse"
        );

        let client = crate::http_client()?;

        // Query the server timezone to determine if DateTime strings are UTC.
        // ClickHouse HTTP API returns DateTime as bare strings with no timezone
        // indicator. We need to know the server timezone to annotate them correctly.
        let server_tz_is_utc = {
            let tz_url = format!(
                "{}/?database={}&user={}{}",
                base_url,
                urlencoded(&database),
                urlencoded(&username),
                if password.is_empty() {
                    String::new()
                } else {
                    format!("&password={}", urlencoded(&password))
                },
            );
            match tokio::time::timeout(
                crate::DATASOURCE_TIMEOUT_CONNECT,
                client.post(&tz_url).body("SELECT timezone()").send(),
            )
            .await
            {
                Ok(Ok(resp)) if resp.status().is_success() => {
                    let body = resp.text().await.unwrap_or_default();
                    body.trim() == "UTC"
                }
                _ => {
                    // If we can't determine the timezone, assume UTC (the most
                    // common ClickHouse configuration).
                    true
                }
            }
        };

        tracing::info!(server_tz_is_utc, "ClickHouse server timezone detected");

        Ok(Self {
            client,
            base_url,
            database,
            username,
            password,
            server_tz_is_utc,
            #[cfg(feature = "ssh")]
            _ssh_tunnel: ssh_tunnel,
        })
    }

    /// Execute a raw SQL query via the ClickHouse HTTP API.
    ///
    /// Returns the raw response body as a string. Used internally for
    /// both data queries and metadata queries.
    async fn execute_http(
        &self,
        sql: &str,
        format: Option<&str>,
    ) -> Result<reqwest::Response, Error> {
        let mut url = format!("{}/?database={}", self.base_url, urlencoded(&self.database));

        // Add auth via query params
        url.push_str(&format!("&user={}", urlencoded(&self.username)));
        if !self.password.is_empty() {
            url.push_str(&format!("&password={}", urlencoded(&self.password)));
        }

        // Add format if specified
        if let Some(fmt) = format {
            url.push_str(&format!("&default_format={fmt}"));
        }

        let response = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_QUERY,
            self.client.post(&url).body(sql.to_string()).send(),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "ClickHouse query timed out after {}s",
                crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("ClickHouse HTTP request failed: {e}")))?;

        Ok(response)
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for ClickHouseProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        let response = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            self.client
                .post(format!(
                    "{}/?database={}&user={}{}",
                    self.base_url,
                    urlencoded(&self.database),
                    urlencoded(&self.username),
                    if self.password.is_empty() {
                        String::new()
                    } else {
                        format!("&password={}", urlencoded(&self.password))
                    },
                ))
                .body("SELECT 1")
                .send(),
        )
        .await
        .map_err(|_| Error::Internal("ClickHouse test connection timed out".into()))?
        .map_err(|e| Error::Internal(format!("ClickHouse test connection failed: {e}")))?;

        if response.status().is_success() {
            Ok(true)
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(Error::Internal(format!(
                "ClickHouse test connection failed: {body}"
            )))
        }
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

        let paginated_sql = &prepared.sql;
        let effective_limit = limit.unwrap_or(1000);

        tracing::debug!(
            sql = %paginated_sql.chars().take(200).collect::<String>(),
            "Executing ClickHouse query"
        );

        // Execute the query with JSONCompact format
        let response = match self.execute_http(paginated_sql, Some("JSONCompact")).await {
            Ok(r) => r,
            Err(e) => {
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
        };

        // Extract bytes_processed from X-ClickHouse-Summary header
        let bytes_processed = response
            .headers()
            .get("x-clickhouse-summary")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|v| {
                v.get("read_bytes")
                    .and_then(|b| b.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| v.get("read_bytes").and_then(|b| b.as_i64()))
            });

        let status_code = response.status();
        let body = response.text().await.unwrap_or_default();

        if !status_code.is_success() {
            tracing::error!(status = %status_code, error = %body, "ClickHouse query error");
            return Ok(QueryResult {
                status: QueryStatus::Error,
                columns: None,
                rows: None,
                total_rows: None,
                has_more: false,
                bytes_processed: None,
                execution_time_ms: Some(start.elapsed().as_millis() as i64),
                error: Some(body),
                record_batch: None,
            });
        }

        // Parse JSONCompact response
        let parsed: Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                return Ok(QueryResult {
                    status: QueryStatus::Error,
                    columns: None,
                    rows: None,
                    total_rows: None,
                    has_more: false,
                    bytes_processed: None,
                    execution_time_ms: Some(start.elapsed().as_millis() as i64),
                    error: Some(format!("Failed to parse ClickHouse response: {e}")),
                    record_batch: None,
                });
            }
        };

        // Extract column metadata from "meta" array
        let columns: Vec<ColumnInfo> = parsed
            .get("meta")
            .and_then(|m| m.as_array())
            .map(|meta| {
                meta.iter()
                    .map(|col| {
                        let name = col
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("?")
                            .to_string();
                        let type_str = col.get("type").and_then(|t| t.as_str()).unwrap_or("");
                        ColumnInfo {
                            name,
                            col_type: map_clickhouse_type(type_str),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Extract row data from "data" array, sanitize null bytes, and coerce
        // string-encoded numerics to proper JSON types.
        //
        // ClickHouse JSONCompact serializes UInt64/Int64 values as JSON strings
        // (to avoid JavaScript precision loss for large integers). We use the
        // column type metadata to convert them back to JSON numbers so that
        // downstream consumers (D3 charts, etc.) get proper numeric values.
        let rows: Vec<Vec<Value>> = parsed
            .get("data")
            .and_then(|d| d.as_array())
            .map(|data| {
                data.iter()
                    .map(|row| {
                        row.as_array()
                            .map(|arr| {
                                arr.iter()
                                    .enumerate()
                                    .map(|(i, v)| {
                                        let sanitized = sanitize_null_bytes(v.clone());
                                        coerce_value_type(
                                            &sanitized,
                                            columns.get(i),
                                            self.server_tz_is_utc,
                                        )
                                    })
                                    .collect()
                            })
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Build Arrow RecordBatch from the coerced JSON rows (sole data path).
        let mut arrow_builder = if !columns.is_empty() {
            Some(crate::arrow_builder::ArrowResultBuilder::new(&columns))
        } else {
            None
        };

        if let Some(ref mut builder) = arrow_builder {
            for row in &rows {
                clickhouse_row_to_arrow(row, &columns, builder, self.server_tz_is_utc);
            }
        }

        let record_batch = arrow_builder.and_then(|builder| {
            builder
                .finish()
                .map_err(|e| {
                    tracing::warn!(error = %e, "ClickHouse Arrow batch construction failed");
                    e
                })
                .ok()
        });

        let row_count = record_batch.as_ref().map_or(0, |b| b.num_rows());
        let has_more = row_count == effective_limit as usize;
        let execution_time_ms = start.elapsed().as_millis() as i64;

        // ClickHouse includes `rows_before_limit_at_least` in the response
        // metadata — the pre-LIMIT total row count at zero extra cost.
        // No separate COUNT(*) query needed.
        let total_rows = if include_total {
            parsed
                .get("rows_before_limit_at_least")
                .and_then(|v| v.as_i64())
        } else {
            None
        };

        Ok(QueryResult {
            status: QueryStatus::Success,
            columns: Some(columns),
            rows: None,
            total_rows,
            has_more,
            bytes_processed,
            execution_time_ms: Some(execution_time_ms),
            error: None,
            record_batch,
        })
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        let explain_sql = format!("EXPLAIN {sql}");

        let result = self.execute_http(&explain_sql, None).await;

        match result {
            Ok(response) => {
                if response.status().is_success() {
                    Ok(DryRunResult::success("Query valid"))
                } else {
                    let body = response.text().await.unwrap_or_default();
                    let (line, column) = parse_clickhouse_error_location(&body, sql);
                    Ok(DryRunResult::failure(body, line, column))
                }
            }
            Err(e) => Ok(DryRunResult::failure(e.to_string(), None, None)),
        }
    }

    async fn list_databases(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query(
                "SELECT name FROM system.databases \
                 WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') \
                 ORDER BY name",
                None,
                None,
                false,
            )
            .await
        {
            Ok(result) => {
                let items =
                    crate::provider::extract_string_col_from_batch(result.record_batch.as_ref(), 0);
                crate::provider::DiscoveryResult { items, error: None }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list ClickHouse databases: {e}")),
            },
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
        let prepared = super::sqlx_common::prepare_query(sql, limit, offset);

        // JSONCompactEachRowWithNamesAndTypes is SELECT-oriented; for non-SELECT
        // queries (DDL, INSERT, etc.) fall back to the default buffered path.
        if !prepared.is_select {
            let result = self
                .execute_query(sql, limit, offset, include_total)
                .await?;
            return crate::stream::query_result_to_stream(result);
        }

        let start = Instant::now();
        let chunk_size = chunk_size.unwrap_or(100) as usize;

        // Get total count if requested (only for SELECT/WITH queries)
        let total_rows = if include_total {
            get_total_count(self, &prepared.sql_stripped).await
        } else {
            None
        };

        let paginated_sql = prepared.sql;

        tracing::debug!(
            sql = %paginated_sql.chars().take(200).collect::<String>(),
            "Streaming ClickHouse query"
        );

        // Execute the query with JSONCompactEachRowWithNamesAndTypes format.
        // This returns NDJSON where:
        //   Line 1: column names as JSON array
        //   Line 2: column types as JSON array
        //   Lines 3+: row data as JSON arrays
        let response = self
            .execute_http(&paginated_sql, Some("JSONCompactEachRowWithNamesAndTypes"))
            .await?;

        // Extract bytes_processed from X-ClickHouse-Summary header before
        // consuming the response body.
        let bytes_processed = response
            .headers()
            .get("x-clickhouse-summary")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|v| {
                v.get("read_bytes")
                    .and_then(|b| b.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| v.get("read_bytes").and_then(|b| b.as_i64()))
            });

        let status_code = response.status();
        if !status_code.is_success() {
            let body = response.text().await.unwrap_or_default();
            tracing::error!(status = %status_code, error = %body, "ClickHouse streaming query error");
            return Err(Error::Internal(body));
        }

        // Stream the response body incrementally.
        let byte_stream = response.bytes_stream();
        let server_tz_is_utc = self.server_tz_is_utc;

        let (tx, stream) = super::sqlx_common::make_stream_channel();

        tokio::spawn(async move {
            // State for line-based parsing across byte stream chunks.
            // The HTTP response may split lines across multiple byte chunks,
            // so we buffer incomplete lines.
            let mut line_buf = String::new();
            let mut line_number: u64 = 0; // 0 = names, 1 = types, 2+ = data rows
            let mut columns: Vec<ColumnInfo> = Vec::new();
            let mut col_names: Vec<String> = Vec::new();
            let mut columns_ready = false;
            let mut chunk_buffer: Vec<Vec<Value>> = Vec::with_capacity(chunk_size);
            let mut chunk_index: u32 = 0;
            let mut total_rows_returned: u64 = 0;

            // Pin the byte stream so we can poll it.
            let mut byte_stream = std::pin::pin!(byte_stream);

            loop {
                let next =
                    tokio::time::timeout(crate::DATASOURCE_TIMEOUT_QUERY, byte_stream.next()).await;

                let bytes_item = match next {
                    Ok(Some(Ok(bytes))) => Some(bytes),
                    Ok(Some(Err(e))) => {
                        let _ = tx
                            .send(Err(Error::Internal(format!(
                                "ClickHouse streaming error: {e}"
                            ))))
                            .await;
                        return;
                    }
                    Ok(None) => None, // Stream exhausted
                    Err(_) => {
                        let _ = tx
                            .send(Err(Error::Internal(format!(
                                "ClickHouse query timed out after {}s",
                                crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                            ))))
                            .await;
                        return;
                    }
                };

                match bytes_item {
                    Some(bytes) => {
                        // Append bytes to the line buffer and process complete lines.
                        let chunk_str = match std::str::from_utf8(&bytes) {
                            Ok(s) => s,
                            Err(e) => {
                                let _ = tx
                                    .send(Err(Error::Internal(format!(
                                        "ClickHouse response contains invalid UTF-8: {e}"
                                    ))))
                                    .await;
                                return;
                            }
                        };

                        line_buf.push_str(chunk_str);

                        // Process all complete lines in the buffer.
                        while let Some(newline_pos) = line_buf.find('\n') {
                            let line = line_buf[..newline_pos].to_string();
                            line_buf = line_buf[newline_pos + 1..].to_string();

                            let line = line.trim();
                            if line.is_empty() {
                                continue;
                            }

                            if line_number == 0 {
                                // First line: column names
                                match serde_json::from_str::<Vec<String>>(line) {
                                    Ok(names) => col_names = names,
                                    Err(e) => {
                                        let _ = tx
                                            .send(Err(Error::Internal(format!(
                                                "Failed to parse ClickHouse column names: {e}"
                                            ))))
                                            .await;
                                        return;
                                    }
                                }
                                line_number += 1;
                            } else if line_number == 1 {
                                // Second line: column types
                                match serde_json::from_str::<Vec<String>>(line) {
                                    Ok(types) => {
                                        columns = col_names
                                            .iter()
                                            .zip(types.iter())
                                            .map(|(name, type_str)| ColumnInfo {
                                                name: name.clone(),
                                                col_type: map_clickhouse_type(type_str),
                                            })
                                            .collect();

                                        // Emit the header event.
                                        if tx
                                            .send(Ok(QueryStreamEvent::Header {
                                                columns: columns.clone(),
                                                total_rows,
                                            }))
                                            .await
                                            .is_err()
                                        {
                                            return; // Consumer dropped
                                        }
                                        columns_ready = true;
                                    }
                                    Err(e) => {
                                        let _ = tx
                                            .send(Err(Error::Internal(format!(
                                                "Failed to parse ClickHouse column types: {e}"
                                            ))))
                                            .await;
                                        return;
                                    }
                                }
                                line_number += 1;
                            } else {
                                // Data row: parse JSON array and apply coercion.
                                match serde_json::from_str::<Vec<Value>>(line) {
                                    Ok(raw_row) => {
                                        let row: Vec<Value> = raw_row
                                            .into_iter()
                                            .enumerate()
                                            .map(|(i, v)| {
                                                let sanitized = sanitize_null_bytes(v);
                                                coerce_value_type(
                                                    &sanitized,
                                                    columns.get(i),
                                                    server_tz_is_utc,
                                                )
                                            })
                                            .collect();

                                        chunk_buffer.push(row);
                                        total_rows_returned += 1;

                                        // Flush chunk when full
                                        if chunk_buffer.len() >= chunk_size {
                                            let rows = std::mem::replace(
                                                &mut chunk_buffer,
                                                Vec::with_capacity(chunk_size),
                                            );
                                            if tx
                                                .send(Ok(QueryStreamEvent::Chunk {
                                                    rows,
                                                    chunk_index,
                                                }))
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                            chunk_index += 1;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = tx
                                            .send(Err(Error::Internal(format!(
                                                "Failed to parse ClickHouse data row: {e}"
                                            ))))
                                            .await;
                                        return;
                                    }
                                }
                                line_number += 1;
                            }
                        }
                    }
                    None => {
                        // Stream exhausted. Process any remaining data in the buffer.
                        let remaining = line_buf.trim().to_string();
                        if !remaining.is_empty() && line_number >= 2 {
                            // Last line may not end with newline
                            match serde_json::from_str::<Vec<Value>>(&remaining) {
                                Ok(raw_row) => {
                                    let row: Vec<Value> = raw_row
                                        .into_iter()
                                        .enumerate()
                                        .map(|(i, v)| {
                                            let sanitized = sanitize_null_bytes(v);
                                            coerce_value_type(
                                                &sanitized,
                                                columns.get(i),
                                                server_tz_is_utc,
                                            )
                                        })
                                        .collect();
                                    chunk_buffer.push(row);
                                    total_rows_returned += 1;
                                }
                                Err(e) => {
                                    let _ = tx
                                        .send(Err(Error::Internal(format!(
                                            "Failed to parse ClickHouse data row: {e}"
                                        ))))
                                        .await;
                                    return;
                                }
                            }
                        }

                        // Emit header if we never got any rows (empty result set).
                        if !columns_ready
                            && tx
                                .send(Ok(QueryStreamEvent::Header {
                                    columns: Vec::new(),
                                    total_rows,
                                }))
                                .await
                                .is_err()
                        {
                            return;
                        }

                        // Flush remaining rows.
                        if !chunk_buffer.is_empty() {
                            let rows = std::mem::take(&mut chunk_buffer);
                            if tx
                                .send(Ok(QueryStreamEvent::Chunk { rows, chunk_index }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            chunk_index += 1;
                        }

                        let execution_time_ms = start.elapsed().as_millis() as i64;
                        let _ = tx
                            .send(Ok(QueryStreamEvent::Complete {
                                execution_time_ms: Some(execution_time_ms),
                                bytes_processed,
                                total_chunks: chunk_index,
                                total_rows_returned,
                            }))
                            .await;
                        return;
                    }
                }
            }
        });

        Ok(stream)
    }

    async fn close(&self) {
        // Stateless HTTP — nothing to close.
        tracing::debug!("ClickHouse provider closed (stateless HTTP)");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Percent-encode a string for use as a URL query parameter value.
///
/// Uses `NON_ALPHANUMERIC` from the `percent-encoding` crate, which encodes
/// every character that is not an ASCII alphanumeric. This covers all
/// characters unsafe in query parameters including spaces, `&`, `=`, `+`,
/// `#`, `%`, `?`, `/`, `@`, quotes, backslashes, and control characters.
fn urlencoded(s: &str) -> String {
    utf8_percent_encode(s, NON_ALPHANUMERIC).to_string()
}

/// Get total row count for a SELECT query.
///
/// Wraps the query in `SELECT COUNT(*) FROM (...) AS _count_subquery`.
/// Returns `None` silently on failure.
/// Get the total row count for a query via a separate COUNT(*) query.
///
/// Only used by the streaming path (`execute_query_stream`) which uses
/// `JSONCompactEachRowWithNamesAndTypes` format that doesn't include
/// `rows_before_limit_at_least`. The non-streaming `execute_query` path
/// gets the count for free from the JSONCompact response metadata.
async fn get_total_count(provider: &ClickHouseProvider, sql: &str) -> Option<i64> {
    let count_sql = format!("SELECT COUNT(*) FROM ({sql}) AS _count_subquery");

    let response = provider
        .execute_http(&count_sql, Some("JSONCompact"))
        .await
        .ok()?;

    if !response.status().is_success() {
        tracing::warn!("Failed to get ClickHouse total count, continuing without it");
        return None;
    }

    let body = response.text().await.ok()?;
    let parsed: Value = serde_json::from_str(&body).ok()?;

    parsed
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|cols| cols.first())
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
}

/// Coerce a JSON value to the correct type based on column metadata.
///
/// ClickHouse JSONCompact returns UInt64/Int64 as JSON strings to avoid
/// precision loss. This function converts string-encoded numbers and booleans
/// back to their proper JSON types using the column type information.
///
/// When `server_tz_is_utc` is true, DateTime strings are annotated with "Z"
/// so JavaScript correctly interprets them as UTC rather than local time.
fn coerce_value_type(value: &Value, col: Option<&ColumnInfo>, server_tz_is_utc: bool) -> Value {
    let Some(col) = col else {
        return value.clone();
    };

    match (&col.col_type, value) {
        // String-encoded number → JSON number
        (SimpleType::Number, Value::String(s)) => {
            if let Ok(n) = s.parse::<i64>() {
                Value::Number(n.into())
            } else if let Ok(f) = s.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or_else(|| value.clone())
            } else {
                value.clone()
            }
        }
        // String-encoded boolean → JSON boolean
        (SimpleType::Boolean, Value::String(s)) => match s.as_str() {
            "1" | "true" | "True" => Value::Bool(true),
            "0" | "false" | "False" => Value::Bool(false),
            _ => value.clone(),
        },
        // DateTime string → ISO 8601 format
        // ClickHouse HTTP API returns DateTime as bare strings like "2026-02-15 11:02:36"
        // in the server's timezone with no timezone indicator. JavaScript's new Date()
        // parses these as local time, causing incorrect timezone offsets.
        // Replace the space with T for ISO 8601 compliance, and append Z if the server
        // timezone is UTC (determined at connection time via SELECT timezone()).
        (SimpleType::Timestamp, Value::String(s)) => {
            // Match "YYYY-MM-DD HH:MM:SS" pattern (19+ chars with space at position 10)
            if s.len() >= 19 && s.as_bytes().get(10) == Some(&b' ') {
                let mut iso = String::with_capacity(s.len() + 1);
                iso.push_str(&s[..10]);
                iso.push('T');
                iso.push_str(&s[11..]);
                if server_tz_is_utc {
                    iso.push('Z');
                }
                Value::String(iso)
            } else {
                value.clone()
            }
        }
        // Already the right type, or no coercion needed
        _ => value.clone(),
    }
}

/// Convert a ClickHouse JSON row directly to Arrow column builders.
///
/// ClickHouse data arrives as JSON arrays from the HTTP API (JSONCompact format).
/// Each element in `row` corresponds to a column. Uses [`SimpleType`] from
/// `columns` to guide type-aware conversion via the shared
/// [`crate::arrow_builder::json_value_to_arrow`], with ClickHouse-specific
/// handling for server timezone annotation on timestamps.
pub(crate) fn clickhouse_row_to_arrow(
    row: &[Value],
    columns: &[ColumnInfo],
    builder: &mut crate::arrow_builder::ArrowResultBuilder,
    server_tz_is_utc: bool,
) {
    for (idx, col) in columns.iter().enumerate() {
        let value = row.get(idx).unwrap_or(&Value::Null);

        // ClickHouse-specific: for Timestamp columns, if the server timezone
        // is UTC, treat bare datetime strings as TimestampTz (append Z).
        if server_tz_is_utc && col.col_type == SimpleType::Timestamp {
            if let Some(s) = value.as_str() {
                // Convert "YYYY-MM-DD HH:MM:SS" → ISO 8601 with Z suffix
                if s.len() >= 19 && s.as_bytes().get(10) == Some(&b' ') {
                    let mut iso = String::with_capacity(s.len() + 1);
                    iso.push_str(&s[..10]);
                    iso.push('T');
                    iso.push_str(&s[11..]);
                    iso.push('Z');
                    let coerced = Value::String(iso);
                    crate::arrow_builder::json_value_to_arrow(
                        &coerced,
                        SimpleType::TimestampTz,
                        builder,
                        idx,
                    );
                    continue;
                }
            }
        }

        crate::arrow_builder::json_value_to_arrow(value, col.col_type, builder, idx);
    }
    builder.finish_row();
}

/// Sanitize null bytes from a JSON value.
///
/// ClickHouse can return null bytes (`\x00`) in string fields (e.g., country
/// codes like `"\x00\x00"`). These break downstream systems (SVG rendering,
/// JSON encoding, etc.). We strip them from string values.
fn sanitize_null_bytes(value: Value) -> Value {
    match value {
        Value::String(s) if s.contains('\x00') => Value::String(s.replace('\x00', "")),
        other => other,
    }
}

/// Regex for ClickHouse "(line X, col Y)" error pattern, compiled once.
static CH_LINE_COL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\(line\s+(\d+),\s*col\s+(\d+)\)").expect("ClickHouse line/col regex")
});

/// Regex for ClickHouse "(line X)" error pattern, compiled once.
static CH_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\(line\s+(\d+)\)").expect("ClickHouse line regex"));

/// Regex for ClickHouse "at position X" error pattern, compiled once.
static CH_POSITION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)at position\s+(\d+)").expect("ClickHouse position regex"));

/// Parse ClickHouse error for line/column position.
///
/// ClickHouse error format examples:
/// - `"Syntax error: failed at position 38 (line 4, col 1): FROM my_table"`
/// - `"failed at position 12 ('databases'): Expected one of..."`
/// - `"Syntax error: ... (line 19, col 25)"`
fn parse_clickhouse_error_location(error_msg: &str, sql: &str) -> (Option<u32>, Option<u32>) {
    // Try "(line X, col Y)" pattern first
    if let Some(caps) = CH_LINE_COL_RE.captures(error_msg) {
        let line = caps.get(1).and_then(|m| m.as_str().parse::<u32>().ok());
        let col = caps.get(2).and_then(|m| m.as_str().parse::<u32>().ok());
        if line.is_some() {
            return (line, col);
        }
    }

    // Try "(line X)" pattern
    if let Some(caps) = CH_LINE_RE.captures(error_msg) {
        let line = caps.get(1).and_then(|m| m.as_str().parse::<u32>().ok());
        if line.is_some() {
            return (line, None);
        }
    }

    // Fallback: "at position X" — convert character position to line/col
    if let Some(caps) = CH_POSITION_RE.captures(error_msg)
        && let Some(pos) = caps.get(1).and_then(|m| m.as_str().parse::<usize>().ok())
    {
        // ClickHouse positions are 1-indexed
        if pos > 0 && pos <= sql.len() {
            let prefix = &sql[..pos.saturating_sub(1)];
            let line = prefix.chars().filter(|&c| c == '\n').count() as u32 + 1;
            let last_newline = prefix.rfind('\n').map(|p| p + 1).unwrap_or(0);
            let column = (pos - last_newline) as u32;
            return (Some(line), Some(column));
        }
    }

    (None, None)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Null byte sanitization ---

    #[test]
    fn sanitize_null_bytes_removes_from_strings() {
        let input = Value::String("hello\x00world".into());
        let result = sanitize_null_bytes(input);
        assert_eq!(result, Value::String("helloworld".into()));
    }

    #[test]
    fn sanitize_null_bytes_handles_only_null_bytes() {
        let input = Value::String("\x00\x00".into());
        let result = sanitize_null_bytes(input);
        assert_eq!(result, Value::String(String::new()));
    }

    #[test]
    fn sanitize_null_bytes_passes_through_clean_strings() {
        let input = Value::String("clean".into());
        let result = sanitize_null_bytes(input);
        assert_eq!(result, Value::String("clean".into()));
    }

    #[test]
    fn sanitize_null_bytes_passes_through_non_strings() {
        assert_eq!(sanitize_null_bytes(Value::Null), Value::Null);
        assert_eq!(
            sanitize_null_bytes(serde_json::json!(42)),
            serde_json::json!(42)
        );
        assert_eq!(sanitize_null_bytes(Value::Bool(true)), Value::Bool(true));
    }

    // --- Type coercion ---

    fn num_col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type: SimpleType::Number,
        }
    }

    fn str_col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type: SimpleType::String,
        }
    }

    fn bool_col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type: SimpleType::Boolean,
        }
    }

    #[test]
    fn coerce_string_integer_to_number() {
        let col = num_col("count");
        let result = coerce_value_type(&Value::String("12".into()), Some(&col), true);
        assert_eq!(result, serde_json::json!(12));
    }

    #[test]
    fn coerce_string_float_to_number() {
        let col = num_col("rate");
        let result = coerce_value_type(&Value::String("3.14".into()), Some(&col), true);
        assert_eq!(result, serde_json::json!(3.14));
    }

    #[test]
    fn coerce_string_negative_to_number() {
        let col = num_col("delta");
        let result = coerce_value_type(&Value::String("-42".into()), Some(&col), true);
        assert_eq!(result, serde_json::json!(-42));
    }

    #[test]
    fn coerce_string_large_uint64_to_number() {
        let col = num_col("big");
        // Value within i64 range
        let result = coerce_value_type(
            &Value::String("9223372036854775807".into()),
            Some(&col),
            true,
        );
        assert_eq!(result, serde_json::json!(9_223_372_036_854_775_807_i64));
    }

    #[test]
    fn coerce_already_numeric_unchanged() {
        let col = num_col("count");
        let result = coerce_value_type(&serde_json::json!(42), Some(&col), true);
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn coerce_string_bool_true() {
        let col = bool_col("flag");
        assert_eq!(
            coerce_value_type(&Value::String("1".into()), Some(&col), true),
            Value::Bool(true)
        );
        assert_eq!(
            coerce_value_type(&Value::String("true".into()), Some(&col), true),
            Value::Bool(true)
        );
    }

    #[test]
    fn coerce_string_bool_false() {
        let col = bool_col("flag");
        assert_eq!(
            coerce_value_type(&Value::String("0".into()), Some(&col), true),
            Value::Bool(false)
        );
        assert_eq!(
            coerce_value_type(&Value::String("false".into()), Some(&col), true),
            Value::Bool(false)
        );
    }

    #[test]
    fn coerce_string_col_unchanged() {
        let col = str_col("name");
        let result = coerce_value_type(&Value::String("hello".into()), Some(&col), true);
        assert_eq!(result, Value::String("hello".into()));
    }

    #[test]
    fn coerce_null_unchanged() {
        let col = num_col("count");
        let result = coerce_value_type(&Value::Null, Some(&col), true);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn coerce_no_column_info_unchanged() {
        let result = coerce_value_type(&Value::String("12".into()), None, true);
        assert_eq!(result, Value::String("12".into()));
    }

    #[test]
    fn coerce_unparseable_number_unchanged() {
        let col = num_col("count");
        let result = coerce_value_type(&Value::String("not_a_number".into()), Some(&col), true);
        assert_eq!(result, Value::String("not_a_number".into()));
    }

    fn ts_col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type: SimpleType::Timestamp,
        }
    }

    #[test]
    fn coerce_datetime_to_iso8601_utc() {
        let col = ts_col("timestamp");
        let result = coerce_value_type(
            &Value::String("2026-02-15 11:02:36".into()),
            Some(&col),
            true,
        );
        assert_eq!(result, Value::String("2026-02-15T11:02:36Z".into()));
    }

    #[test]
    fn coerce_datetime64_to_iso8601_utc() {
        let col = ts_col("created_at");
        // DateTime64(3) returns subsecond precision
        let result = coerce_value_type(
            &Value::String("2026-02-15 11:02:36.123".into()),
            Some(&col),
            true,
        );
        assert_eq!(result, Value::String("2026-02-15T11:02:36.123Z".into()));
    }

    #[test]
    fn coerce_datetime_non_utc_no_z_suffix() {
        let col = ts_col("timestamp");
        // Non-UTC server: space→T conversion but no Z appended
        let result = coerce_value_type(
            &Value::String("2026-02-15 11:02:36".into()),
            Some(&col),
            false,
        );
        assert_eq!(result, Value::String("2026-02-15T11:02:36".into()));
    }

    #[test]
    fn coerce_datetime_short_string_unchanged() {
        let col = ts_col("timestamp");
        // Too short to be a valid datetime string
        let result = coerce_value_type(&Value::String("2026-02-15".into()), Some(&col), true);
        assert_eq!(result, Value::String("2026-02-15".into()));
    }

    #[test]
    fn coerce_datetime_null_unchanged() {
        let col = ts_col("timestamp");
        let result = coerce_value_type(&Value::Null, Some(&col), true);
        assert_eq!(result, Value::Null);
    }

    // --- Error location parsing ---

    #[test]
    fn parse_error_line_col_pattern() {
        let msg = "Syntax error: failed at position 38 (line 4, col 1): FROM my_table";
        let (line, col) = parse_clickhouse_error_location(msg, "");
        assert_eq!(line, Some(4));
        assert_eq!(col, Some(1));
    }

    #[test]
    fn parse_error_line_only_pattern() {
        let msg = "Some error (line 7)";
        let (line, col) = parse_clickhouse_error_location(msg, "");
        assert_eq!(line, Some(7));
        assert_eq!(col, None);
    }

    #[test]
    fn parse_error_position_fallback() {
        let sql = "SELECT *\nFROM users\nWHERE bad";
        let msg = "Syntax error at position 15";
        let (line, col) = parse_clickhouse_error_location(msg, sql);
        assert_eq!(line, Some(2));
        assert!(col.is_some());
    }

    #[test]
    fn parse_error_no_match() {
        let msg = "Unknown error occurred";
        let (line, col) = parse_clickhouse_error_location(msg, "SELECT 1");
        assert_eq!(line, None);
        assert_eq!(col, None);
    }

    #[test]
    fn parse_error_complex_message() {
        let msg = "Syntax error (Multi-statements are not allowed): failed at position 774 (end of query) (line 19, col 25)";
        let (line, col) = parse_clickhouse_error_location(msg, "");
        assert_eq!(line, Some(19));
        assert_eq!(col, Some(25));
    }

    // --- clickhouse_row_to_arrow ---

    use crate::arrow_builder::ArrowResultBuilder;
    use arrow::array::{Array, Date32Array, Float64Array, TimestampMicrosecondArray};

    fn make_col(name: &str, col_type: SimpleType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type,
        }
    }

    /// Convenience: build a one-row Arrow batch from a single-column ClickHouse row.
    fn ch_row_to_batch(
        value: Value,
        col_type: SimpleType,
        server_tz_is_utc: bool,
    ) -> arrow::record_batch::RecordBatch {
        let columns = vec![make_col("col", col_type)];
        let mut builder = ArrowResultBuilder::new(&columns);
        clickhouse_row_to_arrow(&[value], &columns, &mut builder, server_tz_is_utc);
        builder.finish().unwrap()
    }

    #[test]
    fn ch_datetime_utc_server_not_null() {
        // "2026-01-15 14:30:00" with server_tz_is_utc=true → must not be null
        let batch = ch_row_to_batch(
            Value::String("2026-01-15 14:30:00".into()),
            SimpleType::Timestamp,
            true,
        );
        assert!(
            !batch.column(0).is_null(0),
            "DateTime with UTC server must produce a non-null TimestampTz value"
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        // 2026-01-15 14:30:00 UTC in microseconds
        let expected = chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(14, 30, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn ch_datetime_non_utc_server_not_null() {
        // "2026-01-15 14:30:00" with server_tz_is_utc=false → Timestamp (no Z), not null
        let batch = ch_row_to_batch(
            Value::String("2026-01-15 14:30:00".into()),
            SimpleType::Timestamp,
            false,
        );
        assert!(
            !batch.column(0).is_null(0),
            "DateTime with non-UTC server must also produce a non-null Timestamp value"
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected = chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(14, 30, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn ch_date_value_not_null() {
        let batch = ch_row_to_batch(Value::String("2026-01-15".into()), SimpleType::Date, true);
        assert!(!batch.column(0).is_null(0), "Date value must not be null");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let expected = chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .signed_duration_since(epoch)
            .num_days() as i32;
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn ch_number_as_string_not_null() {
        let batch = ch_row_to_batch(Value::String("42".into()), SimpleType::Number, true);
        assert!(
            !batch.column(0).is_null(0),
            "Number-as-string must not be null"
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ch_null_value_is_null() {
        let batch = ch_row_to_batch(Value::Null, SimpleType::Number, true);
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn ch_datetime_with_subseconds_utc_not_null() {
        // DateTime64(3) returns subsecond precision: "2026-01-15 14:30:00.123"
        let batch = ch_row_to_batch(
            Value::String("2026-01-15 14:30:00.123".into()),
            SimpleType::Timestamp,
            true,
        );
        assert!(
            !batch.column(0).is_null(0),
            "DateTime64 with subseconds must not be null"
        );
    }

    // --- URL encoding ---

    #[test]
    fn urlencoded_simple() {
        assert_eq!(urlencoded("hello"), "hello");
    }

    #[test]
    fn urlencoded_special_chars() {
        assert_eq!(urlencoded("user name"), "user%20name");
        assert_eq!(urlencoded("a&b=c"), "a%26b%3Dc");
        assert_eq!(urlencoded("p@ss%"), "p%40ss%25");
    }

    #[test]
    fn urlencoded_empty() {
        assert_eq!(urlencoded(""), "");
    }

    #[test]
    fn urlencoded_quotes_and_backslashes() {
        assert_eq!(urlencoded("it's"), "it%27s");
        assert_eq!(urlencoded(r#"say "hi""#), "say%20%22hi%22");
        assert_eq!(urlencoded(r"path\to"), "path%5Cto");
    }

    #[test]
    fn urlencoded_control_chars() {
        assert_eq!(urlencoded("line\nbreak"), "line%0Abreak");
        assert_eq!(urlencoded("tab\there"), "tab%09here");
    }
}
