//! Databricks datasource provider using the SQL Statement Execution REST API.
//!
//! Implements query execution for Databricks SQL Warehouses using the
//! `/api/2.0/sql/statements` REST API. Supports PAT (Personal Access Token),
//! OAuth U2M (User-to-Machine), and OAuth M2M (Machine-to-Machine)
//! authentication.
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `server_hostname` | string | — | Databricks workspace hostname (required) |
//! | `http_path` | string | — | SQL warehouse HTTP path (required) |
//! | `catalog` | string | — | Unity Catalog name (optional) |
//! | `schema` | string | — | Default schema (optional) |
//!
//! ## Credentials
//!
//! **PAT auth:**
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `access_token` | string | Personal Access Token |
//!
//! **OAuth U2M auth:**
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `oauth_access_token` | string | OAuth access token from OAuth flow |
//!
//! **OAuth M2M auth (service principal):**
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `client_id` | string | OAuth client ID for service principal |
//! | `client_secret` | string | OAuth client secret for service principal |

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use regex::Regex;
use serde_json::Value;

use crate::provider::{ColumnInfo, DatasourceProvider, DryRunResult, QueryResult, QueryStatus};
use crate::type_mapping::map_databricks_type;

use kyomi_connect_protocol::Error;

/// Maximum time to wait for a statement to complete before giving up.
const STATEMENT_POLL_TIMEOUT: Duration = Duration::from_secs(120);
/// Interval between polling requests for statement status.
const STATEMENT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Databricks datasource provider.
///
/// Uses the Databricks SQL Statement Execution API for stateless query
/// execution. Each provider instance holds a bearer token for authentication.
pub struct DatabricksProvider {
    /// HTTP client for making requests.
    client: reqwest::Client,
    /// Databricks workspace hostname.
    server_hostname: String,
    /// Bearer token for API authentication (PAT or OAuth token).
    token: String,
    /// SQL warehouse ID extracted from the HTTP path.
    warehouse_id: String,
    /// Unity Catalog name (optional).
    catalog: Option<String>,
    /// Default schema (optional).
    schema: Option<String>,
}

impl DatabricksProvider {
    /// Create a new Databricks provider from connection config and credentials.
    ///
    /// Extracts the warehouse ID from the HTTP path and determines the
    /// authentication token from credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing or credentials are invalid.
    pub async fn new(
        connection_config: &Value,
        credentials: &Value,
    ) -> kyomi_connect_protocol::Result<Self> {
        let server_hostname = connection_config
            .get("server_hostname")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider("Databricks server_hostname is required".into()))?
            .to_string();

        let http_path = connection_config
            .get("http_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider("Databricks http_path is required".into()))?;

        // Extract warehouse ID from http_path
        // Format: "/sql/1.0/warehouses/{warehouse_id}"
        let warehouse_id = extract_warehouse_id(http_path)
            .ok_or_else(|| {
                Error::Provider(format!(
                    "Could not extract warehouse ID from http_path: {http_path}"
                ))
            })?
            .to_string();

        let catalog = connection_config
            .get("catalog")
            .and_then(|v| v.as_str())
            .map(String::from);

        let schema = connection_config
            .get("schema")
            .and_then(|v| v.as_str())
            .map(String::from);

        // Determine auth token: PAT > OAuth U2M > OAuth M2M (client_credentials)
        let pat = credentials
            .get("access_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let oauth_token = credentials
            .get("oauth_access_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let m2m_client_id = credentials
            .get("client_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let m2m_client_secret = credentials
            .get("client_secret")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let token = if let Some(t) = pat {
            t.to_string()
        } else if let Some(t) = oauth_token {
            // Covers both U2M tokens and M2M tokens cached by
            // ensure_valid_oauth_credentials
            t.to_string()
        } else if let (Some(client_id), Some(client_secret)) = (m2m_client_id, m2m_client_secret) {
            // OAuth M2M fallback: exchange client_id + client_secret directly.
            // This path is hit when DatabricksProvider::new() is called without
            // a prior ensure_valid_oauth_credentials pass (e.g., tests).
            let client = crate::http_client()?;
            let token_url = format!("https://{server_hostname}/oidc/v1/token");
            exchange_m2m_token(&client, &token_url, client_id, client_secret)
                .await?
                .access_token
        } else {
            return Err(Error::Provider(
                "Databricks requires access_token, oauth_access_token, or client_id + client_secret".into(),
            ));
        };

        tracing::info!(
            server_hostname = server_hostname,
            warehouse_id = warehouse_id,
            catalog = catalog.as_deref().unwrap_or("(none)"),
            "Connecting to Databricks"
        );

        let client = crate::http_client()?;

        Ok(Self {
            client,
            server_hostname,
            token,
            warehouse_id,
            catalog,
            schema,
        })
    }

    /// Build the SQL statements API URL.
    fn statements_url(&self) -> String {
        format!("https://{}/api/2.0/sql/statements", self.server_hostname)
    }

    /// Build the URL for getting a specific statement's status.
    fn statement_url(&self, statement_id: &str) -> String {
        format!(
            "https://{}/api/2.0/sql/statements/{statement_id}",
            self.server_hostname
        )
    }

    /// Build the URL for fetching a result chunk.
    fn chunk_url(&self, statement_id: &str, chunk_index: u64) -> String {
        format!(
            "https://{}/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}",
            self.server_hostname
        )
    }

    /// Submit a SQL statement and wait for results.
    ///
    /// Handles both inline results and async polling.
    async fn submit_statement(&self, sql: &str) -> Result<Value, Error> {
        let mut body = serde_json::json!({
            "statement": sql,
            "warehouse_id": self.warehouse_id,
            "wait_timeout": "120s",
            "disposition": "INLINE",
        });

        if let Some(body_obj) = body.as_object_mut() {
            if let Some(ref cat) = self.catalog {
                body_obj.insert("catalog".into(), Value::String(cat.clone()));
            }
            if let Some(ref s) = self.schema {
                body_obj.insert("schema".into(), Value::String(s.clone()));
            }
        }

        let response = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_QUERY,
            self.client
                .post(self.statements_url())
                .bearer_auth(&self.token)
                .header("Content-Type", "application/json")
                .json(&body)
                .send(),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "Databricks statement timed out after {}s",
                crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("Databricks HTTP request failed: {e}")))?;

        let response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::Internal(format!("Failed to parse Databricks response: {e}")))?;

        // Check statement status
        let state = response_body
            .get("status")
            .and_then(|s| s.get("state"))
            .and_then(|s| s.as_str())
            .unwrap_or("");

        match state {
            "SUCCEEDED" => Ok(response_body),
            "FAILED" | "CLOSED" | "CANCELED" => {
                let error_msg = response_body
                    .get("status")
                    .and_then(|s| s.get("error"))
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("Databricks statement failed");
                Err(Error::Internal(error_msg.to_string()))
            }
            "PENDING" | "RUNNING" => {
                // Need to poll for completion
                let statement_id = response_body
                    .get("statement_id")
                    .and_then(|id| id.as_str())
                    .ok_or_else(|| {
                        Error::Internal(
                            "Databricks response missing statement_id for polling".into(),
                        )
                    })?;
                self.poll_statement(statement_id).await
            }
            _ => {
                // Unknown state — try to extract any error message
                let msg = response_body
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown Databricks error");
                Err(Error::Internal(msg.to_string()))
            }
        }
    }

    /// Poll a Databricks statement until it completes.
    async fn poll_statement(&self, statement_id: &str) -> Result<Value, Error> {
        let deadline = Instant::now() + STATEMENT_POLL_TIMEOUT;
        let url = self.statement_url(statement_id);

        loop {
            if Instant::now() > deadline {
                return Err(Error::Internal(
                    "Databricks statement polling timed out".into(),
                ));
            }

            tokio::time::sleep(STATEMENT_POLL_INTERVAL).await;

            let response = self
                .client
                .get(&url)
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|e| Error::Internal(format!("Databricks poll failed: {e}")))?;

            let body: Value = response.json().await.map_err(|e| {
                Error::Internal(format!("Failed to parse Databricks poll response: {e}"))
            })?;

            let state = body
                .get("status")
                .and_then(|s| s.get("state"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            match state {
                "SUCCEEDED" => return Ok(body),
                "FAILED" | "CLOSED" | "CANCELED" => {
                    let error_msg = body
                        .get("status")
                        .and_then(|s| s.get("error"))
                        .and_then(|e| e.get("message"))
                        .and_then(|m| m.as_str())
                        .unwrap_or("Databricks statement failed");
                    return Err(Error::Internal(error_msg.to_string()));
                }
                "PENDING" | "RUNNING" => continue,
                _ => {
                    return Err(Error::Internal(format!(
                        "Unexpected Databricks state: {state}"
                    )));
                }
            }
        }
    }

    /// Fetch additional result chunks if the response indicates more data.
    ///
    /// Databricks may split large results into chunks. If `result.next_chunk_index`
    /// exists, we fetch additional chunks and merge them.
    async fn fetch_all_chunks(
        &self,
        initial_result: &Value,
        statement_id: &str,
    ) -> Vec<Vec<Value>> {
        let mut all_rows: Vec<Vec<Value>> = initial_result
            .get("result")
            .and_then(|r| r.get("data_array"))
            .and_then(|d| d.as_array())
            .map(|data| {
                data.iter()
                    .map(|row| row.as_array().cloned().unwrap_or_default())
                    .collect()
            })
            .unwrap_or_default();

        // Check for additional chunks
        let mut next_chunk = initial_result
            .get("result")
            .and_then(|r| r.get("next_chunk_index"))
            .and_then(|idx| idx.as_u64());

        while let Some(chunk_index) = next_chunk {
            let url = self.chunk_url(statement_id, chunk_index);

            let fetch_result = tokio::time::timeout(crate::DATASOURCE_TIMEOUT_QUERY, async {
                let response = self
                    .client
                    .get(&url)
                    .bearer_auth(&self.token)
                    .send()
                    .await
                    .map_err(|e| format!("Failed to fetch Databricks chunk {chunk_index}: {e}"))?;
                let body: Value = response
                    .json()
                    .await
                    .map_err(|e| format!("Failed to parse Databricks chunk {chunk_index}: {e}"))?;
                Ok::<Value, String>(body)
            })
            .await;

            let chunk_body: Value = match fetch_result {
                Ok(Ok(body)) => body,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Databricks chunk fetch failed");
                    break;
                }
                Err(_) => {
                    tracing::warn!(
                        "Databricks chunk {chunk_index} fetch timed out after {}s",
                        crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                    );
                    break;
                }
            };

            // Extract rows from this chunk
            if let Some(data) = chunk_body.get("data_array").and_then(|d| d.as_array()) {
                for row in data {
                    all_rows.push(row.as_array().cloned().unwrap_or_default());
                }
            }

            // Check for next chunk
            next_chunk = chunk_body
                .get("next_chunk_index")
                .and_then(|idx| idx.as_u64());
        }

        all_rows
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for DatabricksProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        match self.submit_statement("SELECT 1").await {
            Ok(_) => Ok(true),
            Err(e) => Err(Error::Internal(format!(
                "Databricks test connection failed: {e}"
            ))),
        }
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

        let prepared = super::sqlx_common::prepare_query_databricks(sql, limit, offset);

        // Get total count if requested
        let total_rows = if prepared.is_select && include_total && !prepared.is_metadata_command {
            get_total_count(self, &prepared.sql_stripped).await
        } else {
            None
        };

        tracing::debug!(
            sql = %prepared.sql.chars().take(200).collect::<String>(),
            "Executing Databricks query"
        );

        let result = match self.submit_statement(&prepared.sql).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "Databricks query error");
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
        };

        // Extract column metadata from manifest.schema.columns
        let columns: Vec<ColumnInfo> = result
            .get("manifest")
            .and_then(|m| m.get("schema"))
            .and_then(|s| s.get("columns"))
            .and_then(|c| c.as_array())
            .map(|cols| {
                cols.iter()
                    .map(|col| {
                        let name = col
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("?")
                            .to_string();
                        let type_text = col.get("type_text").and_then(|t| t.as_str()).unwrap_or("");
                        ColumnInfo {
                            name,
                            col_type: map_databricks_type(type_text),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Extract row data, fetching additional chunks if needed
        let statement_id = result
            .get("statement_id")
            .and_then(|id| id.as_str())
            .unwrap_or("");

        let rows = self.fetch_all_chunks(&result, statement_id).await;

        // Build Arrow RecordBatch from the JSON rows (sole data path).
        let mut arrow_builder = if !columns.is_empty() {
            Some(crate::arrow_builder::ArrowResultBuilder::new(&columns))
        } else {
            None
        };

        if let Some(ref mut builder) = arrow_builder {
            for row in &rows {
                databricks_row_to_arrow(row, &columns, builder);
            }
        }

        let record_batch = arrow_builder.and_then(|builder| {
            builder
                .finish()
                .map_err(|e| {
                    tracing::warn!(error = %e, "Databricks Arrow batch construction failed");
                    e
                })
                .ok()
        });

        let row_count = record_batch.as_ref().map_or(0, |b| b.num_rows());
        let has_more = limit.is_some_and(|lim| row_count == lim as usize);
        let execution_time_ms = start.elapsed().as_millis() as i64;

        Ok(QueryResult {
            status: QueryStatus::Success,
            columns: Some(columns),
            rows: None,
            total_rows,
            has_more,
            bytes_processed: None, // Databricks doesn't expose this easily
            execution_time_ms: Some(execution_time_ms),
            error: None,
            record_batch,
            job_id: None,
        })
    }

    async fn execute_query_stream_arrow(
        &self,
        sql: &str,
        _limit: Option<u32>,
        _offset: Option<u32>,
        _include_total: bool,
        _chunk_size: Option<u32>,
    ) -> kyomi_connect_protocol::Result<kyomi_connect_protocol::ArrowStream> {
        use std::time::Instant;

        use crate::arrow_builder::ArrowResultBuilder;
        use kyomi_connect_protocol::ArrowStreamEvent;

        let start = Instant::now();

        tracing::debug!(
            sql = %sql.chars().take(200).collect::<String>(),
            "Databricks: starting Arrow stream"
        );

        // Clone fields needed by the spawned task — &self cannot cross spawn.
        let client = self.client.clone();
        let statements_url = self.statements_url();
        let token = self.token.clone();
        let warehouse_id = self.warehouse_id.clone();
        let catalog = self.catalog.clone();
        let schema = self.schema.clone();
        let server_hostname = self.server_hostname.clone();
        let sql = sql.to_string();

        let (tx, stream) = super::sqlx_common::make_arrow_stream_channel();

        tokio::spawn(async move {
            // Submit statement.
            let mut body = serde_json::json!({
                "statement": sql,
                "warehouse_id": warehouse_id,
                "wait_timeout": "120s",
                "disposition": "INLINE",
            });

            if let Some(body_obj) = body.as_object_mut() {
                if let Some(ref cat) = catalog {
                    body_obj.insert("catalog".into(), serde_json::Value::String(cat.clone()));
                }
                if let Some(ref s) = schema {
                    body_obj.insert("schema".into(), serde_json::Value::String(s.clone()));
                }
            }

            let response = match tokio::time::timeout(
                crate::DATASOURCE_TIMEOUT_QUERY,
                client
                    .post(&statements_url)
                    .bearer_auth(&token)
                    .header("Content-Type", "application/json")
                    .json(&body)
                    .send(),
            )
            .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    let _ = tx
                        .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                            "Databricks HTTP request failed: {e}"
                        ))))
                        .await;
                    return;
                }
                Err(_) => {
                    let _ = tx
                        .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                            "Databricks statement timed out after {}s",
                            crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                        ))))
                        .await;
                    return;
                }
            };

            let initial_body: serde_json::Value = match response.json().await {
                Ok(v) => v,
                Err(e) => {
                    let _ = tx
                        .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                            "Failed to parse Databricks response: {e}"
                        ))))
                        .await;
                    return;
                }
            };

            // Poll until SUCCEEDED if needed.
            let result_body = {
                let state = initial_body
                    .get("status")
                    .and_then(|s| s.get("state"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");

                match state {
                    "SUCCEEDED" => initial_body,
                    "FAILED" | "CLOSED" | "CANCELED" => {
                        let msg = initial_body
                            .get("status")
                            .and_then(|s| s.get("error"))
                            .and_then(|e| e.get("message"))
                            .and_then(|m| m.as_str())
                            .unwrap_or("Databricks statement failed")
                            .to_string();
                        let _ = tx
                            .send(Err(kyomi_connect_protocol::Error::Internal(msg)))
                            .await;
                        return;
                    }
                    _ => {
                        // PENDING or RUNNING — poll.
                        let statement_id = match initial_body
                            .get("statement_id")
                            .and_then(|id| id.as_str())
                        {
                            Some(id) => id.to_string(),
                            None => {
                                let _ = tx
                                    .send(Err(kyomi_connect_protocol::Error::Internal(
                                        "Databricks response missing statement_id".into(),
                                    )))
                                    .await;
                                return;
                            }
                        };

                        let poll_url = format!(
                            "https://{server_hostname}/api/2.0/sql/statements/{statement_id}"
                        );
                        let deadline = Instant::now() + STATEMENT_POLL_TIMEOUT;

                        loop {
                            if Instant::now() > deadline {
                                let _ = tx
                                    .send(Err(kyomi_connect_protocol::Error::Internal(
                                        "Databricks statement polling timed out".into(),
                                    )))
                                    .await;
                                return;
                            }

                            tokio::time::sleep(STATEMENT_POLL_INTERVAL).await;

                            let poll_resp = match client
                                .get(&poll_url)
                                .bearer_auth(&token)
                                .send()
                                .await
                            {
                                Ok(r) => r,
                                Err(e) => {
                                    let _ = tx
                                        .send(Err(kyomi_connect_protocol::Error::Internal(
                                            format!("Databricks poll failed: {e}"),
                                        )))
                                        .await;
                                    return;
                                }
                            };

                            let poll_body: serde_json::Value = match poll_resp.json().await {
                                Ok(v) => v,
                                Err(e) => {
                                    let _ = tx
                                        .send(Err(kyomi_connect_protocol::Error::Internal(
                                            format!(
                                                "Failed to parse Databricks poll response: {e}"
                                            ),
                                        )))
                                        .await;
                                    return;
                                }
                            };

                            let poll_state = poll_body
                                .get("status")
                                .and_then(|s| s.get("state"))
                                .and_then(|s| s.as_str())
                                .unwrap_or("");

                            match poll_state {
                                "SUCCEEDED" => {
                                    break poll_body;
                                }
                                "FAILED" | "CLOSED" | "CANCELED" => {
                                    let msg = poll_body
                                        .get("status")
                                        .and_then(|s| s.get("error"))
                                        .and_then(|e| e.get("message"))
                                        .and_then(|m| m.as_str())
                                        .unwrap_or("Databricks statement failed")
                                        .to_string();
                                    let _ = tx
                                        .send(Err(kyomi_connect_protocol::Error::Internal(msg)))
                                        .await;
                                    return;
                                }
                                "PENDING" | "RUNNING" => continue,
                                _ => {
                                    let _ = tx
                                        .send(Err(kyomi_connect_protocol::Error::Internal(
                                            format!(
                                                "Unexpected Databricks state: {poll_state}"
                                            ),
                                        )))
                                        .await;
                                    return;
                                }
                            }
                        }
                    }
                }
            };

            // Extract column metadata from manifest.schema.columns.
            let columns: Vec<crate::provider::ColumnInfo> = result_body
                .get("manifest")
                .and_then(|m| m.get("schema"))
                .and_then(|s| s.get("columns"))
                .and_then(|c| c.as_array())
                .map(|cols| {
                    cols.iter()
                        .map(|col| {
                            let name = col
                                .get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("?")
                                .to_string();
                            let type_text =
                                col.get("type_text").and_then(|t| t.as_str()).unwrap_or("");
                            crate::provider::ColumnInfo {
                                name,
                                col_type: crate::type_mapping::map_databricks_type(type_text),
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Send the schema event.
            let builder = ArrowResultBuilder::new(&columns);
            let schema_ipc = match super::sqlx_common::schema_to_ipc_bytes(builder.schema()) {
                Ok(bytes) => bytes,
                Err(e) => {
                    let _ = tx
                        .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                            "Arrow schema serialization error: {e}"
                        ))))
                        .await;
                    return;
                }
            };

            if tx
                .send(Ok(ArrowStreamEvent::Schema {
                    schema_ipc,
                    columns: columns.clone(),
                    total_rows: None,
                }))
                .await
                .is_err()
            {
                return;
            }

            let statement_id = result_body
                .get("statement_id")
                .and_then(|id| id.as_str())
                .unwrap_or("")
                .to_string();

            let mut chunk_index: u32 = 0;
            let mut total_rows_returned: u64 = 0;

            // Collect inline data and any additional chunks.
            let mut next_chunk: Option<u64> = result_body
                .get("result")
                .and_then(|r| r.get("next_chunk_index"))
                .and_then(|idx| idx.as_u64());

            // Inline data_array from the initial result.
            let inline_rows: Vec<Vec<serde_json::Value>> = result_body
                .get("result")
                .and_then(|r| r.get("data_array"))
                .and_then(|d| d.as_array())
                .map(|data| {
                    data.iter()
                        .map(|row| row.as_array().cloned().unwrap_or_default())
                        .collect()
                })
                .unwrap_or_default();

            if !inline_rows.is_empty() {
                let mut builder = ArrowResultBuilder::new(&columns);
                for row in &inline_rows {
                    databricks_row_to_arrow(row, &columns, &mut builder);
                }
                match builder.finish_to_ipc() {
                    Ok(ipc_bytes) => {
                        if tx
                            .send(Ok(ArrowStreamEvent::Batch {
                                ipc_bytes,
                                chunk_index,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        chunk_index += 1;
                        total_rows_returned += inline_rows.len() as u64;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                "Arrow IPC serialization error: {e}"
                            ))))
                            .await;
                        return;
                    }
                }
            }

            // Fetch additional chunks.
            while let Some(ci) = next_chunk {
                let chunk_url = format!(
                    "https://{server_hostname}/api/2.0/sql/statements/{statement_id}/result/chunks/{ci}"
                );

                let chunk_resp = match tokio::time::timeout(
                    crate::DATASOURCE_TIMEOUT_QUERY,
                    client.get(&chunk_url).bearer_auth(&token).send(),
                )
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        let _ = tx
                            .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                "Databricks chunk {ci} fetch failed: {e}"
                            ))))
                            .await;
                        return;
                    }
                    Err(_) => {
                        let _ = tx
                            .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                "Databricks chunk {ci} fetch timed out after {}s",
                                crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                            ))))
                            .await;
                        return;
                    }
                };

                let chunk_status = chunk_resp.status();
                let chunk_body: serde_json::Value = match chunk_resp.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx
                            .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                "Failed to parse Databricks chunk {ci}: {e}"
                            ))))
                            .await;
                        return;
                    }
                };

                if chunk_status.is_client_error() || chunk_status.is_server_error() {
                    let msg = chunk_body
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Databricks chunk fetch failed")
                        .to_string();
                    let _ = tx
                        .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                            "Databricks chunk {ci}: {msg}"
                        ))))
                        .await;
                    return;
                }

                let chunk_rows: Vec<Vec<serde_json::Value>> = chunk_body
                    .get("data_array")
                    .and_then(|d| d.as_array())
                    .map(|data| {
                        data.iter()
                            .map(|row| row.as_array().cloned().unwrap_or_default())
                            .collect()
                    })
                    .unwrap_or_default();

                if !chunk_rows.is_empty() {
                    let mut builder = ArrowResultBuilder::new(&columns);
                    for row in &chunk_rows {
                        databricks_row_to_arrow(row, &columns, &mut builder);
                    }
                    match builder.finish_to_ipc() {
                        Ok(ipc_bytes) => {
                            if tx
                                .send(Ok(ArrowStreamEvent::Batch {
                                    ipc_bytes,
                                    chunk_index,
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            chunk_index += 1;
                            total_rows_returned += chunk_rows.len() as u64;
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                    "Arrow IPC serialization error: {e}"
                                ))))
                                .await;
                            return;
                        }
                    }
                }

                next_chunk = chunk_body
                    .get("next_chunk_index")
                    .and_then(|idx| idx.as_u64());
            }

            let execution_time_ms = start.elapsed().as_millis() as i64;
            let _ = tx
                .send(Ok(ArrowStreamEvent::Complete {
                    execution_time_ms: Some(execution_time_ms),
                    bytes_processed: None,
                    total_chunks: chunk_index,
                    total_rows_returned,
                }))
                .await;
        });

        Ok(stream)
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        let explain_sql = format!("EXPLAIN {sql}");

        match self.submit_statement(&explain_sql).await {
            Ok(_) => Ok(DryRunResult::success("Query valid")),
            Err(e) => {
                let error_msg = e.to_string();
                let line = parse_databricks_error_line(&error_msg);
                Ok(DryRunResult::failure(error_msg, line, None))
            }
        }
    }

    async fn list_catalogs(&self) -> crate::provider::DiscoveryResult {
        match self.execute_query("SHOW CATALOGS", None, None, false, None).await {
            Ok(result) => {
                let mut items: Vec<String> =
                    crate::provider::extract_string_col_from_batch(result.record_batch.as_ref(), 0)
                        .into_iter()
                        .filter(|name| {
                            let lower = name.to_lowercase();
                            lower != "system" && lower != "hive_metastore"
                        })
                        .collect();
                items.sort();
                crate::provider::DiscoveryResult { items, error: None }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list Databricks catalogs: {e}")),
            },
        }
    }

    async fn close(&self) {
        // Stateless REST API — no persistent connection to close.
        tracing::debug!("Databricks provider closed (stateless REST)");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the warehouse ID from a Databricks HTTP path.
///
/// The HTTP path format is `/sql/1.0/warehouses/{warehouse_id}`.
fn extract_warehouse_id(http_path: &str) -> Option<&str> {
    http_path
        .strip_prefix("/sql/1.0/warehouses/")
        .or_else(|| {
            // Also handle without leading slash
            http_path.strip_prefix("sql/1.0/warehouses/")
        })
        .map(|s| s.trim_end_matches('/'))
        .filter(|s| !s.is_empty())
}

/// Result of a Databricks M2M token exchange.
#[derive(Debug)]
pub(crate) struct M2mTokenResult {
    /// The access token issued by Databricks.
    pub access_token: String,
    /// Token lifetime in seconds (from the `expires_in` field in the response).
    pub expires_in: Option<i64>,
}

/// Exchange M2M (Machine-to-Machine) OAuth credentials for an access token.
///
/// Performs a `client_credentials` grant against the Databricks OIDC token
/// endpoint to obtain an access token from a service principal's
/// `client_id` + `client_secret`.
///
/// This matches the Python provider's M2M OAuth flow where credentials
/// contain `client_id` and `client_secret` instead of a PAT or user OAuth token.
///
/// # Arguments
///
/// * `client` - HTTP client for making the request.
/// * `token_url` - Full token endpoint URL (e.g., `https://{hostname}/oidc/v1/token`).
/// * `client_id` - Service principal client ID.
/// * `client_secret` - Service principal client secret.
pub(crate) async fn exchange_m2m_token(
    client: &reqwest::Client,
    token_url: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<M2mTokenResult, Error> {
    let params = [
        ("grant_type", "client_credentials"),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("scope", "all-apis"),
    ];

    let response = tokio::time::timeout(
        crate::OAUTH_REFRESH_TIMEOUT,
        client.post(token_url).form(&params).send(),
    )
    .await
    .map_err(|_| {
        Error::Internal(format!(
            "Databricks M2M token exchange timed out after {}s",
            crate::OAUTH_REFRESH_TIMEOUT.as_secs()
        ))
    })?
    .map_err(|e| {
        Error::Internal(format!(
            "Databricks M2M token exchange HTTP request failed: {e}"
        ))
    })?;

    let status = response.status();
    let body: Value = response.json().await.map_err(|e| {
        Error::Internal(format!(
            "Failed to parse Databricks M2M token response: {e}"
        ))
    })?;

    if !status.is_success() {
        let error = body
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown_error");
        let description = body
            .get("error_description")
            .and_then(|d| d.as_str())
            .unwrap_or("No description");
        return Err(Error::Internal(format!(
            "Databricks M2M token exchange failed ({error}): {description}"
        )));
    }

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or_else(|| {
            Error::Internal("Databricks M2M token response missing access_token".into())
        })?;

    let expires_in = body.get("expires_in").and_then(|v| v.as_i64());

    Ok(M2mTokenResult {
        access_token,
        expires_in,
    })
}

/// Get total row count for a SELECT query.
async fn get_total_count(provider: &DatabricksProvider, sql: &str) -> Option<i64> {
    let count_sql = format!("SELECT COUNT(*) FROM ({sql}) AS _count_subquery");

    let result = provider.submit_statement(&count_sql).await.ok()?;

    result
        .get("result")
        .and_then(|r| r.get("data_array"))
        .and_then(|d| d.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|cols| cols.first())
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
}

/// Regex for Databricks "line N" error pattern, compiled once.
static DATABRICKS_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)line\s+(\d+)").expect("Databricks line regex"));

/// Parse Databricks error for line number.
///
/// Databricks errors may contain `"line N"` patterns.
fn parse_databricks_error_line(error_msg: &str) -> Option<u32> {
    DATABRICKS_LINE_RE
        .captures(error_msg)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

// ---------------------------------------------------------------------------
// Arrow conversion
// ---------------------------------------------------------------------------

/// Convert a Databricks JSON row directly to Arrow column builders.
///
/// Databricks returns rows as JSON arrays in the `"data_array"` field of
/// each chunk. Each element in the array corresponds to a column value.
/// Uses [`SimpleType`] from `columns` to guide type-aware conversion via
/// the shared [`crate::arrow_builder::json_value_to_arrow`].
pub(crate) fn databricks_row_to_arrow(
    row: &[Value],
    columns: &[crate::provider::ColumnInfo],
    builder: &mut crate::arrow_builder::ArrowResultBuilder,
) {
    for (idx, col) in columns.iter().enumerate() {
        let value = row.get(idx).unwrap_or(&Value::Null);
        crate::arrow_builder::json_value_to_arrow(value, col.col_type, builder, idx);
    }
    builder.finish_row();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Warehouse ID extraction ---

    #[test]
    fn extract_warehouse_id_standard_path() {
        let path = "/sql/1.0/warehouses/abc123def456";
        assert_eq!(extract_warehouse_id(path), Some("abc123def456"));
    }

    #[test]
    fn extract_warehouse_id_with_trailing_slash() {
        let path = "/sql/1.0/warehouses/abc123def456/";
        assert_eq!(extract_warehouse_id(path), Some("abc123def456"));
    }

    #[test]
    fn extract_warehouse_id_without_leading_slash() {
        let path = "sql/1.0/warehouses/abc123def456";
        assert_eq!(extract_warehouse_id(path), Some("abc123def456"));
    }

    #[test]
    fn extract_warehouse_id_invalid_path() {
        assert_eq!(extract_warehouse_id("/some/other/path"), None);
        assert_eq!(extract_warehouse_id(""), None);
        assert_eq!(extract_warehouse_id("/sql/1.0/warehouses/"), None);
    }

    // --- Error line parsing ---

    #[test]
    fn parse_error_line_found() {
        let msg = "PARSE_SYNTAX_ERROR: syntax error line 3 at position 15";
        assert_eq!(parse_databricks_error_line(msg), Some(3));
    }

    #[test]
    fn parse_error_line_case_insensitive() {
        let msg = "Error at LINE 5";
        assert_eq!(parse_databricks_error_line(msg), Some(5));
    }

    #[test]
    fn parse_error_line_not_found() {
        let msg = "Column 'foo' does not exist";
        assert_eq!(parse_databricks_error_line(msg), None);
    }

    // --- M2M OAuth token exchange ---

    // --- databricks_row_to_arrow ---

    use crate::arrow_builder::ArrowResultBuilder;
    use crate::provider::{ColumnInfo, SimpleType};
    use arrow::array::{
        Array, BooleanArray, Date32Array, Float64Array, StringArray, TimestampMicrosecondArray,
    };

    fn make_col(name: &str, col_type: SimpleType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            col_type,
        }
    }

    fn db_row_to_batch(
        values: Vec<serde_json::Value>,
        col_type: SimpleType,
    ) -> arrow::record_batch::RecordBatch {
        let columns = vec![make_col("col", col_type)];
        let mut builder = ArrowResultBuilder::new(&columns);
        databricks_row_to_arrow(&values, &columns, &mut builder);
        builder.finish().unwrap()
    }

    #[test]
    fn db_number_as_string_not_null() {
        let batch = db_row_to_batch(vec![serde_json::json!("42")], SimpleType::Number);
        assert!(
            !batch.column(0).is_null(0),
            "Databricks number-as-string must not be null"
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn db_timestamp_string_not_null() {
        // Databricks TIMESTAMP comes as "YYYY-MM-DD HH:MM:SS" string
        let batch = db_row_to_batch(
            vec![serde_json::json!("2026-01-15 14:30:00")],
            SimpleType::Timestamp,
        );
        assert!(
            !batch.column(0).is_null(0),
            "Databricks timestamp must not be null"
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
    fn db_date_string_not_null() {
        let batch = db_row_to_batch(vec![serde_json::json!("2026-01-15")], SimpleType::Date);
        assert!(
            !batch.column(0).is_null(0),
            "Databricks date must not be null"
        );
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
    fn db_string_value_not_null() {
        let batch = db_row_to_batch(
            vec![serde_json::json!("hello databricks")],
            SimpleType::String,
        );
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "hello databricks");
    }

    #[test]
    fn db_boolean_string_not_null() {
        let batch = db_row_to_batch(vec![serde_json::json!("true")], SimpleType::Boolean);
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
    }

    #[test]
    fn db_null_value_is_null() {
        let batch = db_row_to_batch(vec![serde_json::Value::Null], SimpleType::Number);
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn db_multi_column_row() {
        let columns = vec![
            make_col("ts", SimpleType::Timestamp),
            make_col("n", SimpleType::Number),
            make_col("s", SimpleType::String),
        ];
        let row = vec![
            serde_json::json!("2026-01-15 14:30:00"),
            serde_json::json!("77"),
            serde_json::Value::Null,
        ];
        let mut builder = ArrowResultBuilder::new(&columns);
        databricks_row_to_arrow(&row, &columns, &mut builder);
        let batch = builder.finish().unwrap();

        assert!(!batch.column(0).is_null(0), "ts must not be null");
        assert!(!batch.column(1).is_null(0), "n must not be null");
        assert!(batch.column(2).is_null(0), "null s must be null");

        let arr_n = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr_n.value(0) - 77.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn m2m_token_exchange_success() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("client_id=test-client-id"))
            .and(body_string_contains("client_secret=test-client-secret"))
            .and(body_string_contains("scope=all-apis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "m2m-access-token-123",
                "token_type": "Bearer",
                "expires_in": 3600
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let token_url = format!("{}/oidc/v1/token", mock_server.uri());

        let result =
            exchange_m2m_token(&client, &token_url, "test-client-id", "test-client-secret").await;

        let token_result = result.expect("exchange should succeed");
        assert_eq!(token_result.access_token, "m2m-access-token-123");
        assert_eq!(token_result.expires_in, Some(3600));
    }

    #[tokio::test]
    async fn m2m_token_exchange_error_response() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": "invalid_client",
                "error_description": "Client authentication failed"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let token_url = format!("{}/oidc/v1/token", mock_server.uri());

        let result = exchange_m2m_token(&client, &token_url, "bad-client-id", "bad-secret").await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("invalid_client"),
            "Error should contain 'invalid_client', got: {err_msg}"
        );
        assert!(
            err_msg.contains("Client authentication failed"),
            "Error should contain description, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn m2m_token_exchange_missing_access_token_in_response() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;

        // Server returns 200 but without access_token
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token_type": "Bearer",
                "expires_in": 3600
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let token_url = format!("{}/oidc/v1/token", mock_server.uri());

        let result = exchange_m2m_token(&client, &token_url, "test-client-id", "test-secret").await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("missing access_token"),
            "Error should mention missing access_token, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn m2m_token_exchange_empty_access_token_in_response() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;

        // Server returns 200 with an empty access_token
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "",
                "token_type": "Bearer",
                "expires_in": 3600
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let token_url = format!("{}/oidc/v1/token", mock_server.uri());

        let result = exchange_m2m_token(&client, &token_url, "test-client-id", "test-secret").await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("missing access_token"),
            "Error should mention missing access_token for empty token, got: {err_msg}"
        );
    }
}
