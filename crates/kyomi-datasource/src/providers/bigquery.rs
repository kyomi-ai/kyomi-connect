//! BigQuery datasource provider using the REST API.
//!
//! Implements query execution for Google BigQuery using the BigQuery REST API
//! v2 (`bigquery.googleapis.com/bigquery/v2`). Supports three authentication
//! modes.
//!
//! ## Auth Modes
//!
//! | Mode | Description |
//! |------|-------------|
//! | `kyomi_oauth` | Kyomi's own Google OAuth — access token from `UserContext.oauth_data` |
//! | `enterprise_oauth` | Per-datasource enterprise OAuth — token from `credentials["oauth_access_token"]` |
//! | `service_account` | GCP service account key — JWT signed and exchanged for access token |
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `auth_mode` | string | `"kyomi_oauth"` | Authentication method |
//! | `default_billing_project` | string | — | Billing project for queries |
//! | `service_account_json` | string | — | Service account key JSON (for `service_account` mode) |
//! | `maximum_bytes_billed` | string | — | Optional byte limit for queries |
//! | `oauth_client_id` | string | — | OAuth client ID (for `enterprise_oauth` mode) |
//! | `oauth_client_secret` | string | — | OAuth client secret (for `enterprise_oauth` mode) |
//!
//! ## Credentials
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `billing_project` | string | Per-user billing project override |
//! | `oauth_access_token` | string | OAuth access token (for `enterprise_oauth` mode) |

use std::sync::LazyLock;
use std::time::Instant;

use regex::Regex;
use serde_json::Value;

use crate::factory::UserContext;
use crate::provider::{ColumnInfo, DatasourceProvider, DryRunResult, QueryResult, QueryStatus};
use crate::type_mapping::map_bigquery_type;
use kyomi_connect_protocol::QueryStreamEvent;

use kyomi_connect_protocol::Error;

/// Base URL for the BigQuery REST API v2.
const BIGQUERY_API_BASE: &str = "https://bigquery.googleapis.com/bigquery/v2";

/// Base URL for the Google Cloud Resource Manager API v1.
const GCP_RESOURCE_MANAGER_URL: &str = "https://cloudresourcemanager.googleapis.com/v1";

/// Google OAuth2 token endpoint for service account JWT exchange.
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Scopes requested for BigQuery service account access.
const SERVICE_ACCOUNT_SCOPES: &str = "https://www.googleapis.com/auth/bigquery.readonly https://www.googleapis.com/auth/cloudplatformprojects.readonly";

/// Resolve billing project with consistent precedence:
/// 1. `connection_config["billing_project"]` (workspace-level)
/// 2. `connection_config["default_billing_project"]` (legacy workspace-level)
/// 3. `credentials["billing_project"]` (per-user fallback)
/// 4. `sa_fallback` (service account project_id)
pub fn resolve_billing_project(
    connection_config: &Value,
    credentials: &Value,
    sa_fallback: Option<&str>,
) -> Option<String> {
    connection_config
        .get("billing_project")
        .or_else(|| connection_config.get("default_billing_project"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| {
            credentials
                .get("billing_project")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from)
        })
        .or_else(|| sa_fallback.filter(|s| !s.is_empty()).map(String::from))
}

/// BigQuery datasource provider.
///
/// Uses the BigQuery REST API for stateless query execution. Each provider
/// instance holds a resolved access token and billing project.
pub struct BigQueryProvider {
    /// HTTP client for making requests.
    client: reqwest::Client,
    /// Resolved OAuth2 access token for API calls.
    access_token: String,
    /// GCP project to bill queries against.
    billing_project: String,
    /// Optional maximum bytes billed per query.
    maximum_bytes_billed: Option<String>,
}

impl std::fmt::Debug for BigQueryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryProvider")
            .field("billing_project", &self.billing_project)
            .field("maximum_bytes_billed", &self.maximum_bytes_billed)
            .field("access_token", &"[REDACTED]")
            .finish()
    }
}

impl BigQueryProvider {
    /// Create a new BigQuery provider from connection config and credentials.
    ///
    /// Resolves the access token based on the configured auth mode and
    /// determines the billing project.
    ///
    /// # Arguments
    /// * `connection_config` - Datasource-level configuration.
    /// * `credentials` - Decrypted user-level credentials.
    /// * `user_context` - Optional user context (required for `kyomi_oauth` mode).
    ///
    /// # Errors
    ///
    /// Returns an error if the auth mode is invalid, credentials are missing,
    /// or service account JWT exchange fails.
    pub async fn new(
        connection_config: &Value,
        credentials: &Value,
        user_context: Option<&UserContext>,
    ) -> kyomi_connect_protocol::Result<Self> {
        let auth_mode = connection_config
            .get("auth_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("kyomi_oauth");

        let maximum_bytes_billed = connection_config
            .get("maximum_bytes_billed")
            .and_then(|v| v.as_str())
            .map(String::from);

        let client = crate::http_client()?;

        let (access_token, sa_project_id) = match auth_mode {
            "kyomi_oauth" => {
                let token = resolve_kyomi_oauth_token(user_context)?;
                tracing::info!("BigQuery: Using Kyomi OAuth token");
                (token, None)
            }
            "enterprise_oauth" => {
                let token = credentials
                    .get("oauth_access_token")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        Error::Provider(
                            "BigQuery enterprise_oauth requires oauth_access_token in credentials"
                                .into(),
                        )
                    })?
                    .to_string();
                tracing::info!("BigQuery: Using enterprise OAuth token");
                (token, None)
            }
            "service_account" => {
                let (token, project_id) =
                    exchange_service_account_jwt(&client, connection_config).await?;
                tracing::info!("BigQuery: Using service account JWT");
                (token, Some(project_id))
            }
            other => {
                return Err(Error::Provider(format!(
                    "Unknown BigQuery auth_mode: {other}"
                )));
            }
        };

        let billing_project =
            resolve_billing_project(connection_config, credentials, sa_project_id.as_deref())
                .ok_or_else(|| {
                    Error::Provider(
            "BigQuery requires a billing project. Set billing_project in datasource settings."
                .into(),
        )
                })?;

        tracing::info!(
            auth_mode = auth_mode,
            billing_project = billing_project,
            "BigQuery provider created"
        );

        Ok(Self {
            client,
            access_token,
            billing_project,
            maximum_bytes_billed,
        })
    }

    /// List all active GCP projects accessible by the authenticated user.
    ///
    /// Calls the Google Cloud Resource Manager API v1 to enumerate projects,
    /// filtering for `lifecycleState:ACTIVE`. Handles pagination via
    /// `nextPageToken`.
    ///
    /// Returns a sorted, deduplicated list of project IDs.
    async fn list_active_projects(&self) -> Result<Vec<String>, Error> {
        let mut project_ids = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let mut url =
                format!("{GCP_RESOURCE_MANAGER_URL}/projects?filter=lifecycleState:ACTIVE");
            if let Some(ref token) = page_token {
                url.push_str(&format!("&pageToken={token}"));
            }

            let response = tokio::time::timeout(
                crate::DATASOURCE_TIMEOUT_CONNECT,
                self.client.get(&url).bearer_auth(&self.access_token).send(),
            )
            .await
            .map_err(|_| Error::Internal("GCP Resource Manager API request timed out".into()))?
            .map_err(|e| {
                Error::Internal(format!("GCP Resource Manager API request failed: {e}"))
            })?;

            let status_code = response.status();
            let body: Value = response.json().await.map_err(|e| {
                Error::Internal(format!("Failed to parse Resource Manager response: {e}"))
            })?;

            if status_code.is_client_error() || status_code.is_server_error() {
                let msg = body
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("Failed to list GCP projects");
                return Err(Error::Internal(format!(
                    "GCP Resource Manager API error: {msg}"
                )));
            }

            // Extract project IDs from the response
            if let Some(projects) = body.get("projects").and_then(|p| p.as_array()) {
                for project in projects {
                    if let Some(project_id) = project.get("projectId").and_then(|v| v.as_str()) {
                        project_ids.push(project_id.to_string());
                    }
                }
            }

            // Check for next page
            match body.get("nextPageToken").and_then(|t| t.as_str()) {
                Some(token) if !token.is_empty() => {
                    page_token = Some(token.to_string());
                }
                _ => break,
            }
        }

        // Sort and deduplicate
        project_ids.sort();
        project_ids.dedup();

        tracing::info!(
            count = project_ids.len(),
            "BigQuery: Listed active GCP projects"
        );

        Ok(project_ids)
    }

    /// Submit a BigQuery job and wait for it to complete.
    ///
    /// Returns a tuple of `(job_id, location, job_body)` where `job_body` is the
    /// completed job response containing statistics. For dry-run requests, returns
    /// the immediate response (no polling).
    ///
    /// This is the shared foundation for both `run_query` (buffered) and
    /// `execute_query_stream` (streaming).
    async fn submit_query_job(
        &self,
        sql: &str,
        dry_run: bool,
    ) -> Result<(String, String, Value), Error> {
        let mut query_config = serde_json::json!({
            "query": sql,
            "useLegacySql": false,
        });

        if let Some(max_bytes) = &self.maximum_bytes_billed {
            query_config["maximumBytesBilled"] = Value::String(max_bytes.clone());
        }

        if dry_run {
            query_config["dryRun"] = Value::Bool(true);
        }

        let body = serde_json::json!({
            "configuration": {
                "query": query_config,
            }
        });

        let url = format!("{BIGQUERY_API_BASE}/projects/{}/jobs", self.billing_project);

        let response = tokio::time::timeout(
            if dry_run {
                crate::DATASOURCE_TIMEOUT_DRY_RUN
            } else {
                crate::DATASOURCE_TIMEOUT_QUERY
            },
            self.client
                .post(&url)
                .bearer_auth(&self.access_token)
                .header("Content-Type", "application/json")
                .json(&body)
                .send(),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "BigQuery job submission timed out after {}s",
                crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("BigQuery HTTP request failed: {e}")))?;

        let status_code = response.status();
        let response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::Internal(format!("Failed to parse BigQuery response: {e}")))?;

        if status_code.is_client_error() || status_code.is_server_error() {
            let msg = extract_bigquery_error(&response_body);
            return Err(Error::Internal(msg));
        }

        // Extract job ID and location for polling (owned values to avoid
        // borrowing response_body beyond the move below).
        let job_id = response_body
            .get("jobReference")
            .and_then(|r| r.get("jobId"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let location = response_body
            .get("jobReference")
            .and_then(|r| r.get("location"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // For dry run, return immediately with the job statistics
        if dry_run {
            return Ok((job_id, location, response_body));
        }

        // Non-dry-run responses must have a job ID for polling/results
        if job_id.is_empty() {
            return Err(Error::Internal("BigQuery response missing jobId".into()));
        }

        // Check if the job is already complete
        let job_status = response_body
            .get("status")
            .and_then(|s| s.get("state"))
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();

        let job_body = if job_status == "DONE" {
            // Check for errors in the completed job
            if let Some(err) = response_body
                .get("status")
                .and_then(|s| s.get("errorResult"))
            {
                let msg = err
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("BigQuery job failed");
                return Err(Error::Internal(msg.to_string()));
            }
            response_body
        } else {
            // Poll until complete
            self.poll_job(&job_id, &location).await?
        };

        Ok((job_id, location, job_body))
    }

    /// Submit a query job to BigQuery and wait for results.
    ///
    /// Uses the Jobs API to submit, poll, and retrieve results.
    async fn run_query(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        dry_run: bool,
    ) -> Result<Value, Error> {
        let (job_id, location, job_body) = self.submit_query_job(sql, dry_run).await?;

        // For dry run, return the job body directly (contains statistics)
        if dry_run {
            return Ok(job_body);
        }

        // Fetch results using the query results endpoint
        self.get_query_results(&job_id, &location, limit, offset, &job_body)
            .await
    }

    /// Poll a BigQuery job until it reaches DONE state.
    async fn poll_job(&self, job_id: &str, location: &str) -> Result<Value, Error> {
        let deadline = Instant::now() + crate::DATASOURCE_TIMEOUT_QUERY;

        loop {
            if Instant::now() > deadline {
                return Err(Error::Internal("BigQuery job polling timed out".into()));
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            let mut url = format!(
                "{BIGQUERY_API_BASE}/projects/{}/jobs/{job_id}",
                self.billing_project
            );
            if !location.is_empty() {
                url.push_str(&format!("?location={location}"));
            }

            let response = self
                .client
                .get(&url)
                .bearer_auth(&self.access_token)
                .send()
                .await
                .map_err(|e| Error::Internal(format!("BigQuery poll failed: {e}")))?;

            let body: Value = response.json().await.map_err(|e| {
                Error::Internal(format!("Failed to parse BigQuery poll response: {e}"))
            })?;

            let state = body
                .get("status")
                .and_then(|s| s.get("state"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            if state == "DONE" {
                // Check for errors
                if let Some(err) = body.get("status").and_then(|s| s.get("errorResult")) {
                    let msg = err
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("BigQuery job failed");
                    return Err(Error::Internal(msg.to_string()));
                }
                return Ok(body);
            }
        }
    }

    /// Fetch query results from a completed job.
    async fn get_query_results(
        &self,
        job_id: &str,
        location: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        job_body: &Value,
    ) -> Result<Value, Error> {
        let effective_limit = limit.unwrap_or(1000);
        let effective_offset = offset.unwrap_or(0);

        let mut url = format!(
            "{BIGQUERY_API_BASE}/projects/{}/queries/{job_id}?maxResults={effective_limit}&startIndex={effective_offset}",
            self.billing_project
        );
        if !location.is_empty() {
            url.push_str(&format!("&location={location}"));
        }

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.access_token)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("BigQuery get results failed: {e}")))?;

        let status_code = response.status();
        let mut results_body: Value = response.json().await.map_err(|e| {
            Error::Internal(format!("Failed to parse BigQuery results response: {e}"))
        })?;

        if status_code.is_client_error() || status_code.is_server_error() {
            let msg = extract_bigquery_error(&results_body);
            return Err(Error::Internal(msg));
        }

        // Merge statistics from the job body into the results
        if let Some(stats) = job_body.get("statistics") {
            results_body["statistics"] = stats.clone();
        }

        Ok(results_body)
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for BigQueryProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        let url = format!(
            "{BIGQUERY_API_BASE}/projects/{}/queries",
            self.billing_project
        );

        let body = serde_json::json!({
            "query": "SELECT 1",
            "useLegacySql": false,
            "maxResults": 1,
        });

        let response = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            self.client
                .post(&url)
                .bearer_auth(&self.access_token)
                .header("Content-Type", "application/json")
                .json(&body)
                .send(),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "BigQuery test connection timed out after {}s",
                crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("BigQuery test connection failed: {e}")))?;

        let status_code = response.status();
        let response_body: Value = response.json().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to parse BigQuery test connection response: {e}"
            ))
        })?;

        if status_code.is_client_error() || status_code.is_server_error() {
            let msg = extract_bigquery_error(&response_body);
            return Err(Error::Internal(format!(
                "BigQuery test connection failed: {msg}"
            )));
        }

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

        tracing::debug!(
            sql = %sql.chars().take(200).collect::<String>(),
            "Executing BigQuery query"
        );

        let result = match self.run_query(sql, limit, offset, false).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "BigQuery query error");
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

        // Extract column metadata from schema.fields
        let columns: Vec<ColumnInfo> = result
            .get("schema")
            .and_then(|s| s.get("fields"))
            .and_then(|f| f.as_array())
            .map(|fields| {
                fields
                    .iter()
                    .map(|field| {
                        let name = field
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("?")
                            .to_string();
                        let type_name = field.get("type").and_then(|t| t.as_str()).unwrap_or("");
                        ColumnInfo {
                            name,
                            col_type: map_bigquery_type(type_name),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Build Arrow RecordBatch (the sole data path — JSON rows are not populated).
        // We iterate result["rows"] directly so bigquery_row_to_arrow can access
        // the native f[].v cell structure it expects.
        let mut arrow_builder = if !columns.is_empty() {
            Some(crate::arrow_builder::ArrowResultBuilder::new(&columns))
        } else {
            None
        };

        if let Some(ref mut builder) = arrow_builder {
            if let Some(raw_rows) = result.get("rows").and_then(|r| r.as_array()) {
                for bq_row in raw_rows {
                    bigquery_row_to_arrow(bq_row, &columns, builder);
                }
            }
        }

        let record_batch = arrow_builder.and_then(|builder| {
            builder.finish().map_err(|e| {
                tracing::warn!(error = %e, "BigQuery Arrow batch construction failed");
                e
            }).ok()
        });

        // Extract total rows from the query response (available at zero cost
        // from the BigQuery API — only conditionally populated based on caller).
        let total_rows = if include_total {
            result.get("totalRows").and_then(|v| {
                v.as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| v.as_i64())
            })
        } else {
            None
        };

        // Extract bytes processed from job statistics
        let bytes_processed = result
            .get("statistics")
            .and_then(|s| s.get("query"))
            .and_then(|q| q.get("totalBytesProcessed"))
            .and_then(|v| {
                v.as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| v.as_i64())
            })
            .or_else(|| {
                // Also check top-level totalBytesProcessed
                result.get("totalBytesProcessed").and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<i64>().ok())
                        .or_else(|| v.as_i64())
                })
            });

        let effective_limit = limit.unwrap_or(1000);
        let row_count = record_batch.as_ref().map_or(0, |b| b.num_rows());
        let has_more = row_count == effective_limit as usize;
        let execution_time_ms = start.elapsed().as_millis() as i64;

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
        match self.run_query(sql, None, None, true).await {
            Ok(result) => {
                // Extract bytes processed from dry-run result
                let bytes_processed = result
                    .get("statistics")
                    .and_then(|s| s.get("query"))
                    .and_then(|q| q.get("totalBytesProcessed"))
                    .and_then(|v| {
                        v.as_str()
                            .and_then(|s| s.parse::<i64>().ok())
                            .or_else(|| v.as_i64())
                    })
                    .unwrap_or(0);

                let message = format!(
                    "Query valid. Estimated {} bytes processed.",
                    format_bytes(bytes_processed)
                );
                Ok(DryRunResult::success(message))
            }
            Err(e) => {
                let error_msg = e.to_string();
                let (line, column) = parse_bigquery_error_location(&error_msg);
                Ok(DryRunResult::failure(error_msg, line, column))
            }
        }
    }

    async fn list_projects(&self) -> kyomi_connect_protocol::Result<Vec<String>> {
        self.list_active_projects().await
    }

    async fn execute_query_stream(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
        _chunk_size: Option<u32>,
    ) -> kyomi_connect_protocol::Result<kyomi_connect_protocol::QueryStream> {
        let start = Instant::now();

        tracing::debug!(
            sql = %sql.chars().take(200).collect::<String>(),
            "Streaming BigQuery query"
        );

        // Submit the job and wait for completion before spawning the page
        // iteration task. This surfaces job-level errors (syntax, permissions)
        // directly to the caller.
        let (job_id, location, job_body) = self.submit_query_job(sql, false).await?;

        // Extract bytes processed from job statistics (available only once the
        // job completes, not on individual getQueryResults pages).
        let bytes_processed = job_body
            .get("statistics")
            .and_then(|s| s.get("query"))
            .and_then(|q| q.get("totalBytesProcessed"))
            .and_then(|v| {
                v.as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| v.as_i64())
            });

        // Determine pagination: the first page request mirrors get_query_results
        // but subsequent pages are fetched via pageToken.
        let effective_limit = limit.unwrap_or(1000);
        let effective_offset = offset.unwrap_or(0);

        // Clone fields needed by the spawned task.
        let client = self.client.clone();
        let access_token = self.access_token.clone();
        let billing_project = self.billing_project.clone();

        let (tx, stream) = super::sqlx_common::make_stream_channel();

        tokio::spawn(async move {
            // -- Fetch first page (with timeout) --
            let mut url = format!(
                "{BIGQUERY_API_BASE}/projects/{billing_project}/queries/{job_id}?maxResults={effective_limit}&startIndex={effective_offset}"
            );
            if !location.is_empty() {
                url.push_str(&format!("&location={location}"));
            }

            let first_page = match tokio::time::timeout(
                crate::DATASOURCE_TIMEOUT_QUERY,
                fetch_query_results_page(&client, &access_token, &url),
            )
            .await
            {
                Ok(Ok(page)) => page,
                Ok(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                Err(_) => {
                    let _ = tx
                        .send(Err(Error::Internal(format!(
                            "BigQuery first page fetch timed out after {}s",
                            crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                        ))))
                        .await;
                    return;
                }
            };

            // Extract column metadata from schema.fields and emit Header.
            let columns: Vec<ColumnInfo> = first_page
                .get("schema")
                .and_then(|s| s.get("fields"))
                .and_then(|f| f.as_array())
                .map(|fields| {
                    fields
                        .iter()
                        .map(|field| {
                            let name = field
                                .get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("?")
                                .to_string();
                            let type_name =
                                field.get("type").and_then(|t| t.as_str()).unwrap_or("");
                            ColumnInfo {
                                name,
                                col_type: map_bigquery_type(type_name),
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Conditionally extract total_rows based on include_total flag.
            let total_rows = if include_total {
                first_page.get("totalRows").and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<i64>().ok())
                        .or_else(|| v.as_i64())
                })
            } else {
                None
            };

            if tx
                .send(Ok(QueryStreamEvent::Header {
                    columns,
                    total_rows,
                }))
                .await
                .is_err()
            {
                return; // Consumer dropped
            }

            let mut chunk_index: u32 = 0;
            let mut total_rows_returned: u64 = 0;

            // Extract and emit the first page's rows.
            let first_rows = extract_bigquery_rows(&first_page);
            if !first_rows.is_empty() {
                total_rows_returned += first_rows.len() as u64;
                if tx
                    .send(Ok(QueryStreamEvent::Chunk {
                        rows: first_rows,
                        chunk_index,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                chunk_index += 1;
            }

            // Follow pageToken for subsequent pages.
            let mut page_token = first_page
                .get("pageToken")
                .and_then(|t| t.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from);

            while let Some(token) = page_token.take() {
                let mut next_url = format!(
                    "{BIGQUERY_API_BASE}/projects/{billing_project}/queries/{job_id}?maxResults={effective_limit}&pageToken={token}"
                );
                if !location.is_empty() {
                    next_url.push_str(&format!("&location={location}"));
                }

                let next_page = match tokio::time::timeout(
                    crate::DATASOURCE_TIMEOUT_QUERY,
                    fetch_query_results_page(&client, &access_token, &next_url),
                )
                .await
                {
                    Ok(Ok(page)) => page,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    Err(_) => {
                        let _ = tx
                            .send(Err(Error::Internal(format!(
                                "BigQuery page fetch timed out after {}s",
                                crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                            ))))
                            .await;
                        return;
                    }
                };

                let page_rows = extract_bigquery_rows(&next_page);
                if !page_rows.is_empty() {
                    total_rows_returned += page_rows.len() as u64;
                    if tx
                        .send(Ok(QueryStreamEvent::Chunk {
                            rows: page_rows,
                            chunk_index,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    chunk_index += 1;
                }

                page_token = next_page
                    .get("pageToken")
                    .and_then(|t| t.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from);
            }

            // Emit Complete.
            let execution_time_ms = start.elapsed().as_millis() as i64;
            let _ = tx
                .send(Ok(QueryStreamEvent::Complete {
                    execution_time_ms: Some(execution_time_ms),
                    bytes_processed,
                    total_chunks: chunk_index,
                    total_rows_returned,
                }))
                .await;
        });

        Ok(stream)
    }

    async fn close(&self) {
        // Stateless REST API -- no persistent connection to close.
        tracing::debug!("BigQuery provider closed (stateless REST)");
    }
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

/// Extract the Google OAuth access token from the user context.
///
/// The token lives at `oauth_data.google_oauth_tokens.access_token`.
fn resolve_kyomi_oauth_token(user_context: Option<&UserContext>) -> Result<String, Error> {
    let ctx = user_context.ok_or_else(|| {
        Error::Provider("BigQuery kyomi_oauth mode requires user context with OAuth data".into())
    })?;

    let oauth_data = ctx.oauth_data.as_ref().ok_or_else(|| {
        Error::Provider(
            "BigQuery kyomi_oauth mode requires Google OAuth data in user context".into(),
        )
    })?;

    oauth_data
        .get("google_oauth_tokens")
        .and_then(|t| t.get("access_token"))
        .and_then(|t| t.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or_else(|| {
            Error::Provider(
                "BigQuery kyomi_oauth: missing access_token in google_oauth_tokens".into(),
            )
        })
}

/// Resolve a BigQuery access token based on the configured auth mode.
///
/// This is the single entry point for any code that needs a GCP access token
/// (catalog refresh, query execution, project discovery, etc.).
/// It handles all three auth modes: kyomi_oauth, enterprise_oauth, service_account.
pub async fn resolve_access_token(
    connection_config: &Value,
    credentials: &Value,
    user_context: Option<&UserContext>,
) -> Result<String, Error> {
    let auth_mode = connection_config
        .get("auth_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("kyomi_oauth");

    let client = crate::http_client()?;

    match auth_mode {
        "kyomi_oauth" => resolve_kyomi_oauth_token(user_context),
        "enterprise_oauth" => credentials
            .get("oauth_access_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .ok_or_else(|| {
                Error::Provider(
                    "BigQuery enterprise_oauth requires oauth_access_token in credentials".into(),
                )
            }),
        "service_account" => {
            let (token, _project_id) =
                exchange_service_account_jwt(&client, connection_config).await?;
            Ok(token)
        }
        other => Err(Error::Provider(format!(
            "Unknown BigQuery auth_mode: {other}"
        ))),
    }
}

/// Parse a GCP service account JSON and exchange a signed JWT for an access
/// token.
///
/// Returns `(access_token, project_id)`.
///
/// This is `pub` so the BigQuery access token endpoint can reuse it.
pub async fn exchange_service_account_jwt(
    client: &reqwest::Client,
    connection_config: &Value,
) -> Result<(String, String), Error> {
    let sa_json_str = connection_config
        .get("service_account_json")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::Provider(
                "BigQuery service_account mode requires service_account_json in connection config"
                    .into(),
            )
        })?;

    let sa_json: Value = serde_json::from_str(sa_json_str)
        .map_err(|e| Error::Provider(format!("Invalid service_account_json: {e}")))?;

    let client_email = sa_json
        .get("client_email")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Provider("Service account JSON missing client_email".into()))?;

    let private_key_pem = sa_json
        .get("private_key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Provider("Service account JSON missing private_key".into()))?;

    let project_id = sa_json
        .get("project_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Build JWT claims
    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({
        "iss": client_email,
        "scope": SERVICE_ACCOUNT_SCOPES,
        "aud": GOOGLE_TOKEN_URL,
        "iat": now,
        "exp": now + 3600,
    });

    // Sign the JWT with RS256
    let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
        .map_err(|e| {
            Error::Internal(format!("Failed to parse service account private key: {e}"))
        })?;

    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    let jwt = jsonwebtoken::encode(&header, &claims, &encoding_key)
        .map_err(|e| Error::Internal(format!("Failed to sign service account JWT: {e}")))?;

    // Exchange the JWT for an access token
    let params = [
        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
        ("assertion", &jwt),
    ];

    let response = tokio::time::timeout(
        crate::OAUTH_REFRESH_TIMEOUT,
        client.post(GOOGLE_TOKEN_URL).form(&params).send(),
    )
    .await
    .map_err(|_| Error::Internal("Service account token exchange timed out".into()))?
    .map_err(|e| Error::Internal(format!("Service account token exchange HTTP failed: {e}")))?;

    let status = response.status();
    let body: Value = response
        .json()
        .await
        .map_err(|e| Error::Internal(format!("Failed to parse token exchange response: {e}")))?;

    if !status.is_success() {
        let error = body
            .get("error_description")
            .and_then(|d| d.as_str())
            .unwrap_or("Token exchange failed");
        return Err(Error::Internal(format!(
            "Service account token exchange failed: {error}"
        )));
    }

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Internal("Token exchange response missing access_token".into()))?
        .to_string();

    Ok((access_token, project_id))
}

// ---------------------------------------------------------------------------
// Streaming helpers
// ---------------------------------------------------------------------------

/// Fetch a single page of BigQuery query results via the REST API.
///
/// Used by `execute_query_stream` to iterate pages using `pageToken`.
async fn fetch_query_results_page(
    client: &reqwest::Client,
    access_token: &str,
    url: &str,
) -> Result<Value, Error> {
    let response = client
        .get(url)
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(|e| Error::Internal(format!("BigQuery get results failed: {e}")))?;

    let status_code = response.status();
    let body: Value = response
        .json()
        .await
        .map_err(|e| Error::Internal(format!("Failed to parse BigQuery results response: {e}")))?;

    if status_code.is_client_error() || status_code.is_server_error() {
        let msg = extract_bigquery_error(&body);
        return Err(Error::Internal(msg));
    }

    Ok(body)
}

/// Extract row data from a BigQuery getQueryResults response page.
///
/// BigQuery returns rows as `rows[].f[].v` (fields array per row, value per
/// field). This flattens that into `Vec<Vec<Value>>`.
fn extract_bigquery_rows(page: &Value) -> Vec<Vec<Value>> {
    page.get("rows")
        .and_then(|r| r.as_array())
        .map(|bq_rows| {
            bq_rows
                .iter()
                .map(|row| {
                    row.get("f")
                        .and_then(|f| f.as_array())
                        .map(|cells| {
                            cells
                                .iter()
                                .map(|cell| cell.get("v").cloned().unwrap_or(Value::Null))
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Convert a BigQuery JSON row (from the `rows[].f[].v` structure) directly
/// to Arrow column builders.
///
/// Each `bq_row` is expected to be a JSON object with an `"f"` array whose
/// elements have a `"v"` field containing the cell value. Uses [`SimpleType`]
/// from `columns` to guide type-aware conversion via the shared
/// [`crate::arrow_builder::json_value_to_arrow`].
pub(crate) fn bigquery_row_to_arrow(
    bq_row: &Value,
    columns: &[crate::provider::ColumnInfo],
    builder: &mut crate::arrow_builder::ArrowResultBuilder,
) {
    let cells = bq_row
        .get("f")
        .and_then(|f| f.as_array());

    for (idx, col) in columns.iter().enumerate() {
        let value = cells
            .and_then(|c| c.get(idx))
            .and_then(|cell| cell.get("v"))
            .unwrap_or(&Value::Null);

        crate::arrow_builder::json_value_to_arrow(value, col.col_type, builder, idx);
    }
    builder.finish_row();
}

// ---------------------------------------------------------------------------
// Error parsing helpers
// ---------------------------------------------------------------------------

/// Extract a human-readable error message from a BigQuery API error response.
fn extract_bigquery_error(response: &Value) -> String {
    // Try error.message first (standard Jobs API error format)
    if let Some(msg) = response
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
    {
        return msg.to_string();
    }

    // Try error.errors[0].message (alternative format)
    if let Some(msg) = response
        .get("error")
        .and_then(|e| e.get("errors"))
        .and_then(|e| e.as_array())
        .and_then(|arr| arr.first())
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
    {
        return msg.to_string();
    }

    // Fall back to status.errorResult.message (job-level error)
    if let Some(msg) = response
        .get("status")
        .and_then(|s| s.get("errorResult"))
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
    {
        return msg.to_string();
    }

    "Unknown BigQuery error".to_string()
}

/// Regex for BigQuery error location pattern `[line:column]`, compiled once.
static BIGQUERY_LOCATION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[(\d+):(\d+)\]").expect("BigQuery location regex"));

/// Parse BigQuery error message for line and column location.
///
/// BigQuery format: `"Syntax error at [3:15]"`
fn parse_bigquery_error_location(error_msg: &str) -> (Option<u32>, Option<u32>) {
    BIGQUERY_LOCATION_RE
        .captures(error_msg)
        .map(|caps| {
            let line = caps.get(1).and_then(|m| m.as_str().parse::<u32>().ok());
            let col = caps.get(2).and_then(|m| m.as_str().parse::<u32>().ok());
            (line, col)
        })
        .unwrap_or((None, None))
}

/// Format a byte count for display in dry-run messages.
fn format_bytes(bytes: i64) -> String {
    if bytes < 1_000 {
        format!("{bytes}")
    } else if bytes < 1_000_000 {
        format!("{:.1} KB", bytes as f64 / 1_000.0)
    } else if bytes < 1_000_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_000_000.0)
    } else {
        format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Error location parsing ---

    #[test]
    fn parse_error_location_standard() {
        let msg = "Syntax error in SQL query: Unexpected \"FORM\" at [3:15]";
        let (line, col) = parse_bigquery_error_location(msg);
        assert_eq!(line, Some(3));
        assert_eq!(col, Some(15));
    }

    #[test]
    fn parse_error_location_at_beginning() {
        let msg = "[1:1] Syntax error: Expected end of input";
        let (line, col) = parse_bigquery_error_location(msg);
        assert_eq!(line, Some(1));
        assert_eq!(col, Some(1));
    }

    #[test]
    fn parse_error_location_no_match() {
        let msg = "Table not found: project.dataset.table";
        let (line, col) = parse_bigquery_error_location(msg);
        assert_eq!(line, None);
        assert_eq!(col, None);
    }

    #[test]
    fn parse_error_location_large_numbers() {
        let msg = "Error at [100:250]";
        let (line, col) = parse_bigquery_error_location(msg);
        assert_eq!(line, Some(100));
        assert_eq!(col, Some(250));
    }

    // --- extract_bigquery_error ---

    #[test]
    fn extract_error_standard_format() {
        let response = serde_json::json!({
            "error": {
                "code": 400,
                "message": "Syntax error at [1:5]",
                "errors": [{
                    "message": "Syntax error at [1:5]",
                    "domain": "global",
                    "reason": "invalidQuery"
                }]
            }
        });
        assert_eq!(extract_bigquery_error(&response), "Syntax error at [1:5]");
    }

    #[test]
    fn extract_error_job_level_format() {
        let response = serde_json::json!({
            "status": {
                "state": "DONE",
                "errorResult": {
                    "message": "Job failed: table not found",
                    "reason": "notFound"
                }
            }
        });
        assert_eq!(
            extract_bigquery_error(&response),
            "Job failed: table not found"
        );
    }

    #[test]
    fn extract_error_unknown_format() {
        let response = serde_json::json!({"someField": "someValue"});
        assert_eq!(extract_bigquery_error(&response), "Unknown BigQuery error");
    }

    // --- format_bytes ---

    #[test]
    fn format_bytes_small() {
        assert_eq!(format_bytes(500), "500");
    }

    #[test]
    fn format_bytes_kilobytes() {
        assert_eq!(format_bytes(1_500), "1.5 KB");
    }

    #[test]
    fn format_bytes_megabytes() {
        assert_eq!(format_bytes(5_000_000), "5.0 MB");
    }

    #[test]
    fn format_bytes_gigabytes() {
        assert_eq!(format_bytes(2_500_000_000), "2.50 GB");
    }

    #[test]
    fn format_bytes_zero() {
        assert_eq!(format_bytes(0), "0");
    }

    // --- bigquery_row_to_arrow ---

    use crate::arrow_builder::ArrowResultBuilder;
    use arrow::array::{Array, Float64Array, StringArray, TimestampMicrosecondArray, Date32Array};
    use crate::provider::{ColumnInfo, SimpleType};

    fn make_col(name: &str, col_type: SimpleType) -> ColumnInfo {
        ColumnInfo { name: name.to_string(), col_type }
    }

    /// Build a BigQuery-format row value: `{"f": [{"v": ...}, ...]}`
    fn bq_row(values: &[serde_json::Value]) -> serde_json::Value {
        let cells: Vec<serde_json::Value> = values
            .iter()
            .map(|v| serde_json::json!({"v": v}))
            .collect();
        serde_json::json!({"f": cells})
    }

    #[test]
    fn bq_timestamp_as_string_not_null() {
        // BigQuery DATETIME columns arrive as "YYYY-MM-DD HH:MM:SS" strings
        let columns = vec![make_col("ts", SimpleType::Timestamp)];
        let mut builder = ArrowResultBuilder::new(&columns);
        let row = bq_row(&[serde_json::json!("2026-01-15 14:30:00")]);
        bigquery_row_to_arrow(&row, &columns, &mut builder);
        let batch = builder.finish().unwrap();
        assert!(
            !batch.column(0).is_null(0),
            "BigQuery DATETIME string must not be null"
        );
        let arr = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let expected = chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(14, 30, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn bq_number_as_string_not_null() {
        let columns = vec![make_col("n", SimpleType::Number)];
        let mut builder = ArrowResultBuilder::new(&columns);
        let row = bq_row(&[serde_json::json!("42")]);
        bigquery_row_to_arrow(&row, &columns, &mut builder);
        let batch = builder.finish().unwrap();
        assert!(!batch.column(0).is_null(0), "BigQuery number-as-string must not be null");
        let arr = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn bq_null_value_is_null() {
        let columns = vec![make_col("s", SimpleType::String)];
        let mut builder = ArrowResultBuilder::new(&columns);
        let row = bq_row(&[serde_json::Value::Null]);
        bigquery_row_to_arrow(&row, &columns, &mut builder);
        let batch = builder.finish().unwrap();
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn bq_string_value_not_null() {
        let columns = vec![make_col("s", SimpleType::String)];
        let mut builder = ArrowResultBuilder::new(&columns);
        let row = bq_row(&[serde_json::json!("hello")]);
        bigquery_row_to_arrow(&row, &columns, &mut builder);
        let batch = builder.finish().unwrap();
        assert!(!batch.column(0).is_null(0));
        let arr = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "hello");
    }

    #[test]
    fn bq_multi_column_row() {
        // BigQuery DATETIME (Timestamp), INT64 (Number), STRING (String)
        let columns = vec![
            make_col("ts", SimpleType::Timestamp),
            make_col("n", SimpleType::Number),
            make_col("s", SimpleType::String),
        ];
        let mut builder = ArrowResultBuilder::new(&columns);
        let row = bq_row(&[
            serde_json::json!("2026-01-15 14:30:00"),
            serde_json::json!("42"),
            serde_json::Value::Null,
        ]);
        bigquery_row_to_arrow(&row, &columns, &mut builder);
        let batch = builder.finish().unwrap();

        assert!(!batch.column(0).is_null(0), "ts must not be null");
        assert!(!batch.column(1).is_null(0), "n must not be null");
        assert!(batch.column(2).is_null(0), "s (null) must be null");

        let arr_n = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr_n.value(0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn bq_date_value_not_null() {
        let columns = vec![make_col("d", SimpleType::Date)];
        let mut builder = ArrowResultBuilder::new(&columns);
        let row = bq_row(&[serde_json::json!("2026-01-15")]);
        bigquery_row_to_arrow(&row, &columns, &mut builder);
        let batch = builder.finish().unwrap();
        assert!(!batch.column(0).is_null(0), "BigQuery Date must not be null");
        let arr = batch.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let expected = chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .signed_duration_since(epoch)
            .num_days() as i32;
        assert_eq!(arr.value(0), expected);
    }

    // --- resolve_kyomi_oauth_token ---

    #[test]
    fn kyomi_oauth_missing_user_context() {
        let result = resolve_kyomi_oauth_token(None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("user context"), "Error: {err}");
    }

    #[test]
    fn kyomi_oauth_missing_oauth_data() {
        let ctx = UserContext {
            oauth_data: None,
            user_email: "test@example.com".into(),
            workspace_id: "ws-1".into(),
        };
        let result = resolve_kyomi_oauth_token(Some(&ctx));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Google OAuth data"), "Error: {err}");
    }

    #[test]
    fn kyomi_oauth_missing_access_token() {
        let ctx = UserContext {
            oauth_data: Some(serde_json::json!({
                "google_oauth_tokens": {}
            })),
            user_email: "test@example.com".into(),
            workspace_id: "ws-1".into(),
        };
        let result = resolve_kyomi_oauth_token(Some(&ctx));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing access_token"), "Error: {err}");
    }

    #[test]
    fn kyomi_oauth_success() {
        let ctx = UserContext {
            oauth_data: Some(serde_json::json!({
                "google_oauth_tokens": {
                    "access_token": "ya29.test-token"
                }
            })),
            user_email: "test@example.com".into(),
            workspace_id: "ws-1".into(),
        };
        let result = resolve_kyomi_oauth_token(Some(&ctx)).expect("should succeed");
        assert_eq!(result, "ya29.test-token");
    }

    // --- Billing project resolution ---

    #[tokio::test]
    async fn billing_project_workspace_config_wins_over_credentials() {
        // Workspace config (connection_config) takes priority over user credentials
        let creds = serde_json::json!({
            "billing_project": "creds-project",
            "oauth_access_token": "test-token",
        });
        let config = serde_json::json!({
            "auth_mode": "enterprise_oauth",
            "billing_project": "workspace-project",
        });

        let provider = BigQueryProvider::new(&config, &creds, None)
            .await
            .expect("should succeed");
        assert_eq!(provider.billing_project, "workspace-project");
    }

    #[tokio::test]
    async fn billing_project_falls_back_to_credentials() {
        // When workspace config has no billing_project, fall back to user credentials
        let creds = serde_json::json!({
            "billing_project": "creds-project",
            "oauth_access_token": "test-token",
        });
        let config = serde_json::json!({
            "auth_mode": "enterprise_oauth",
        });

        let provider = BigQueryProvider::new(&config, &creds, None)
            .await
            .expect("should succeed");
        assert_eq!(provider.billing_project, "creds-project");
    }

    #[tokio::test]
    async fn billing_project_from_connection_config() {
        let creds = serde_json::json!({
            "oauth_access_token": "test-token",
        });
        let config = serde_json::json!({
            "auth_mode": "enterprise_oauth",
            "default_billing_project": "config-project",
        });

        let provider = BigQueryProvider::new(&config, &creds, None)
            .await
            .expect("should succeed");
        assert_eq!(provider.billing_project, "config-project");
    }

    #[tokio::test]
    async fn billing_project_missing_returns_error() {
        let creds = serde_json::json!({
            "oauth_access_token": "test-token",
        });
        let config = serde_json::json!({
            "auth_mode": "enterprise_oauth",
        });

        let result = BigQueryProvider::new(&config, &creds, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("billing project"), "Error: {err}");
    }

    #[tokio::test]
    async fn unknown_auth_mode_returns_error() {
        let creds = serde_json::json!({});
        let config = serde_json::json!({
            "auth_mode": "magic_tokens",
            "default_billing_project": "project",
        });

        let result = BigQueryProvider::new(&config, &creds, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("magic_tokens"), "Error: {err}");
    }

    #[tokio::test]
    async fn enterprise_oauth_missing_token_returns_error() {
        let creds = serde_json::json!({});
        let config = serde_json::json!({
            "auth_mode": "enterprise_oauth",
            "default_billing_project": "project",
        });

        let result = BigQueryProvider::new(&config, &creds, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("oauth_access_token"), "Error: {err}");
    }
}
