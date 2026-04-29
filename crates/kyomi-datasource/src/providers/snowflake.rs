//! Snowflake datasource provider using the REST API.
//!
//! Implements query execution for Snowflake databases using the Snowflake
//! SQL Statement Execution REST API (`/api/v2/statements`). Supports
//! password authentication (via the login endpoint), OAuth token auth,
//! and key-pair JWT authentication.
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `account` | string | — | Snowflake account identifier (required) |
//! | `warehouse` | string | — | Default warehouse |
//! | `database` | string | — | Default database |
//! | `schema` | string | — | Default schema (optional) |
//! | `role` | string | — | Default role (optional) |
//!
//! ## Credentials
//!
//! **Password auth:**
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `username` | string | Snowflake username |
//! | `password` | string | Snowflake password |
//!
//! **Key-pair auth:**
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `username` | string | Snowflake username |
//! | `private_key` | string | PEM-encoded RSA private key |
//! | `private_key_passphrase` | string | Optional passphrase for encrypted key |
//!
//! **OAuth auth:**
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `oauth_access_token` | string | OAuth access token |

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use regex::Regex;
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pkcs8::EncodePublicKey;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::provider::{ColumnInfo, DatasourceProvider, DryRunResult, QueryResult, QueryStatus};
use crate::type_mapping::map_snowflake_type_code;

use kyomi_connect_protocol::Error;

/// Maximum time to wait for an async statement to complete before giving up.
const STATEMENT_POLL_TIMEOUT: Duration = Duration::from_secs(120);
/// Interval between polling requests for async statement status.
const STATEMENT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Snowflake JWT tokens are valid for at most 60 seconds.
///
/// Snowflake documentation states JWT tokens are valid for at most 1 hour,
/// but we use a short lifetime and re-generate per session, matching the
/// Python connector's behavior of 60-second expiry.
const JWT_LIFETIME_SECS: i64 = 60;

/// Distinguishes how the provider authenticated, which determines the
/// `X-Snowflake-Authorization-Token-Type` header value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthType {
    /// Session token from password login, or OAuth token.
    SessionOrOAuth,
    /// JWT token from key-pair authentication.
    KeypairJwt,
}

/// Snowflake datasource provider.
///
/// Uses the Snowflake REST API for stateless query execution. Each provider
/// instance holds a session token obtained during construction.
pub struct SnowflakeProvider {
    /// HTTP client for making requests.
    client: reqwest::Client,
    /// Snowflake account identifier (e.g., `xy12345.us-east-1`).
    account: String,
    /// Bearer token for API requests (session token, OAuth token, or JWT).
    token: String,
    /// How the token was obtained, determines the token type header.
    auth_type: AuthType,
    /// Default warehouse.
    warehouse: Option<String>,
    /// Default database.
    database: Option<String>,
    /// Default schema.
    schema: Option<String>,
    /// Default role.
    role: Option<String>,
}

impl SnowflakeProvider {
    /// Create a new Snowflake provider from connection config and credentials.
    ///
    /// Authenticates with Snowflake using one of three methods (in order of
    /// precedence):
    /// 1. OAuth (if `oauth_access_token` is present)
    /// 2. Key-pair JWT (if `private_key` is present)
    /// 3. Password (if `username` and `password` are present)
    ///
    /// # Errors
    ///
    /// Returns an error if the account is missing, credentials are invalid,
    /// or authentication fails.
    pub async fn new(
        connection_config: &Value,
        credentials: &Value,
    ) -> kyomi_connect_protocol::Result<Self> {
        let account = connection_config
            .get("account")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider("Snowflake account is required".into()))?
            .to_string();

        let warehouse = connection_config
            .get("warehouse")
            .and_then(|v| v.as_str())
            .map(String::from);

        let database = connection_config
            .get("database")
            .and_then(|v| v.as_str())
            .map(String::from);

        let schema = connection_config
            .get("schema")
            .and_then(|v| v.as_str())
            .map(String::from);

        let role = connection_config
            .get("role")
            .and_then(|v| v.as_str())
            .map(String::from);

        let client = crate::http_client()?;

        // Determine auth method: OAuth > Key-pair > Password
        let oauth_token = credentials
            .get("oauth_access_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let private_key_pem = credentials
            .get("private_key")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let (token, auth_type) = if let Some(oauth_access_token) = oauth_token {
            tracing::info!(
                account = account,
                warehouse = warehouse.as_deref().unwrap_or("(none)"),
                database = database.as_deref().unwrap_or("(none)"),
                "Connecting to Snowflake (OAuth)"
            );
            (oauth_access_token.to_string(), AuthType::SessionOrOAuth)
        } else if let Some(pem_str) = private_key_pem {
            // Key-pair JWT auth
            let username = credentials
                .get("username")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    Error::Provider(
                        "Snowflake requires username for key-pair authentication".into(),
                    )
                })?;

            let passphrase = credentials
                .get("private_key_passphrase")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());

            tracing::info!(
                account = account,
                warehouse = warehouse.as_deref().unwrap_or("(none)"),
                database = database.as_deref().unwrap_or("(none)"),
                "Connecting to Snowflake (key-pair JWT)"
            );

            let jwt = generate_keypair_jwt(&account, username, pem_str, passphrase)?;
            (jwt, AuthType::KeypairJwt)
        } else {
            // Password auth: call the login endpoint to get a session token
            let username = credentials
                .get("username")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    Error::Provider(
                        "Snowflake requires username for password authentication".into(),
                    )
                })?;

            let password = credentials
                .get("password")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    Error::Provider(
                        "Snowflake requires password for password authentication".into(),
                    )
                })?;

            tracing::info!(
                account = account,
                warehouse = warehouse.as_deref().unwrap_or("(none)"),
                database = database.as_deref().unwrap_or("(none)"),
                "Connecting to Snowflake (password)"
            );

            let session_token = login_password(&client, &account, username, password).await?;
            (session_token, AuthType::SessionOrOAuth)
        };

        Ok(Self {
            client,
            account,
            token,
            auth_type,
            warehouse,
            database,
            schema,
            role,
        })
    }

    /// Build the base URL for the Snowflake SQL API.
    fn statements_url(&self) -> String {
        format!(
            "https://{}.snowflakecomputing.com/api/v2/statements",
            self.account
        )
    }

    /// Submit a SQL statement to the Snowflake REST API and wait for results.
    ///
    /// Handles both synchronous (immediate result) and asynchronous (polling)
    /// responses.
    async fn submit_statement(&self, sql: &str) -> Result<Value, Error> {
        let mut body = serde_json::json!({
            "statement": sql,
            "timeout": 120,
        });

        if let Some(body_obj) = body.as_object_mut() {
            if let Some(ref wh) = self.warehouse {
                body_obj.insert("warehouse".into(), Value::String(wh.clone()));
            }
            if let Some(ref db) = self.database {
                body_obj.insert("database".into(), Value::String(db.clone()));
            }
            if let Some(ref s) = self.schema {
                body_obj.insert("schema".into(), Value::String(s.clone()));
            }
            if let Some(ref r) = self.role {
                body_obj.insert("role".into(), Value::String(r.clone()));
            }
        }

        let mut request = self
            .client
            .post(self.statements_url())
            .bearer_auth(&self.token)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json");

        // For key-pair JWT auth, tell Snowflake the token type
        if self.auth_type == AuthType::KeypairJwt {
            request = request.header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
        }

        let response =
            tokio::time::timeout(crate::DATASOURCE_TIMEOUT_QUERY, request.json(&body).send())
                .await
                .map_err(|_| {
                    Error::Internal(format!(
                        "Snowflake statement timed out after {}s",
                        crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                    ))
                })?
                .map_err(|e| Error::Internal(format!("Snowflake HTTP request failed: {e}")))?;

        let status = response.status();
        let response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::Internal(format!("Failed to parse Snowflake response: {e}")))?;

        // Check for errors
        if status.is_client_error() || status.is_server_error() {
            let msg = response_body
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown Snowflake error");
            return Err(Error::Internal(msg.to_string()));
        }

        // Check if the statement is still running and needs polling
        let statement_status = response_body
            .get("statementStatusUrl")
            .and_then(|u| u.as_str());

        let result_status = response_body
            .get("statementHandle")
            .and_then(|h| h.as_str());

        let code = response_body
            .get("code")
            .and_then(|c| c.as_str())
            .unwrap_or("");

        // "333334" = statement executing asynchronously
        if code == "333334" {
            if let Some(handle) = result_status {
                return self.poll_statement(handle).await;
            }
            if let Some(status_url) = statement_status {
                return self.poll_statement_url(status_url).await;
            }
        }

        Ok(response_body)
    }

    /// Poll a Snowflake statement by handle until it completes.
    async fn poll_statement(&self, handle: &str) -> Result<Value, Error> {
        let url = format!("{}/{handle}", self.statements_url());
        self.poll_statement_url(&url).await
    }

    /// Poll a Snowflake statement by URL until it completes.
    async fn poll_statement_url(&self, url: &str) -> Result<Value, Error> {
        let deadline = Instant::now() + STATEMENT_POLL_TIMEOUT;

        loop {
            if Instant::now() > deadline {
                return Err(Error::Internal(
                    "Snowflake statement polling timed out".into(),
                ));
            }

            tokio::time::sleep(STATEMENT_POLL_INTERVAL).await;

            let mut request = self
                .client
                .get(url)
                .bearer_auth(&self.token)
                .header("Accept", "application/json");

            if self.auth_type == AuthType::KeypairJwt {
                request = request.header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
            }

            let response = request
                .send()
                .await
                .map_err(|e| Error::Internal(format!("Snowflake poll failed: {e}")))?;

            let body: Value = response.json().await.map_err(|e| {
                Error::Internal(format!("Failed to parse Snowflake poll response: {e}"))
            })?;

            let code = body.get("code").and_then(|c| c.as_str()).unwrap_or("");

            // "333334" = still running
            if code == "333334" {
                continue;
            }

            // Check for error
            let status = body.get("status").and_then(|s| s.as_str()).unwrap_or("");
            if status == "FAILED_WITH_ERROR" {
                let msg = body
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Snowflake statement failed");
                return Err(Error::Internal(msg.to_string()));
            }

            return Ok(body);
        }
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for SnowflakeProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        match self.submit_statement("SELECT 1").await {
            Ok(_) => Ok(true),
            Err(e) => Err(Error::Internal(format!(
                "Snowflake test connection failed: {e}"
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

        let prepared = super::sqlx_common::prepare_query(sql, limit, offset);

        // Get total count if requested
        let total_rows = if prepared.is_select && include_total {
            get_total_count(self, &prepared.sql_stripped).await
        } else {
            None
        };

        let paginated_sql = &prepared.sql;
        let effective_limit = limit.unwrap_or(1000);

        tracing::debug!(
            sql = %paginated_sql.chars().take(200).collect::<String>(),
            "Executing Snowflake query"
        );

        let result = match self.submit_statement(paginated_sql).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "Snowflake query error");
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

        // Extract column metadata from resultSetMetaData.rowType
        let columns: Vec<ColumnInfo> = result
            .get("resultSetMetaData")
            .and_then(|meta| meta.get("rowType"))
            .and_then(|rt| rt.as_array())
            .map(|row_types| {
                row_types
                    .iter()
                    .map(|col| {
                        let name = col
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("?")
                            .to_string();

                        // Try type code first, fall back to type name
                        let col_type = col
                            .get("type")
                            .and_then(|t| t.as_str())
                            .and_then(snowflake_type_name_to_code)
                            .map(map_snowflake_type_code)
                            .unwrap_or_else(|| {
                                // Use the type name string directly
                                col.get("type")
                                    .and_then(|t| t.as_str())
                                    .map(map_snowflake_type_name)
                                    .unwrap_or(crate::provider::SimpleType::Unknown)
                            });

                        ColumnInfo { name, col_type }
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Extract row data from "data" array
        let rows: Vec<Vec<Value>> = result
            .get("data")
            .and_then(|d| d.as_array())
            .map(|data| {
                data.iter()
                    .map(|row| row.as_array().cloned().unwrap_or_default())
                    .collect()
            })
            .unwrap_or_default();

        // Build Arrow RecordBatch from the JSON rows (sole data path).
        let mut arrow_builder = if !columns.is_empty() {
            Some(crate::arrow_builder::ArrowResultBuilder::new(&columns))
        } else {
            None
        };

        if let Some(ref mut builder) = arrow_builder {
            for row in &rows {
                snowflake_row_to_arrow(row, &columns, builder);
            }
        }

        let record_batch = arrow_builder.and_then(|builder| {
            builder
                .finish()
                .map_err(|e| {
                    tracing::warn!(error = %e, "Snowflake Arrow batch construction failed");
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
            bytes_processed: None, // Snowflake REST API doesn't easily expose this
            execution_time_ms: Some(execution_time_ms),
            error: None,
            record_batch,
            job_id: None,
        })
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        let explain_sql = format!("EXPLAIN {sql}");

        match self.submit_statement(&explain_sql).await {
            Ok(_) => Ok(DryRunResult::success("Query valid")),
            Err(e) => {
                let error_msg = e.to_string();
                let line = parse_snowflake_error_line(&error_msg);
                Ok(DryRunResult::failure(error_msg, line, None))
            }
        }
    }

    async fn list_databases(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query("SHOW DATABASES", None, None, false, None)
            .await
        {
            Ok(result) => {
                // SHOW DATABASES returns: (created_on, name, is_default, is_current, origin, owner, ...)
                // name is at index 1
                let mut items: Vec<String> =
                    crate::provider::extract_string_col_from_batch(result.record_batch.as_ref(), 1)
                        .into_iter()
                        .filter(|name| {
                            let upper = name.to_uppercase();
                            upper != "SNOWFLAKE" && upper != "SNOWFLAKE_SAMPLE_DATA"
                        })
                        .collect();
                items.sort();
                crate::provider::DiscoveryResult { items, error: None }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list Snowflake databases: {e}")),
            },
        }
    }

    async fn list_warehouses(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query("SHOW WAREHOUSES", None, None, false, None)
            .await
        {
            Ok(result) => {
                // SHOW WAREHOUSES returns: (name, state, type, size, ...)
                // name is at index 0
                let mut items =
                    crate::provider::extract_string_col_from_batch(result.record_batch.as_ref(), 0);
                items.sort();
                crate::provider::DiscoveryResult { items, error: None }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list Snowflake warehouses: {e}")),
            },
        }
    }

    async fn close(&self) {
        // Stateless REST API — no persistent connection to close.
        tracing::debug!("Snowflake provider closed (stateless REST)");
    }
}

// ---------------------------------------------------------------------------
// Key-pair JWT authentication
// ---------------------------------------------------------------------------

/// Generate a JWT token for Snowflake key-pair authentication.
///
/// Parses the PEM-encoded RSA private key (handling optional passphrase
/// encryption), extracts the public key, computes the SHA-256 fingerprint,
/// and signs a JWT with the correct Snowflake claims.
///
/// The JWT claims follow the Snowflake specification:
/// - `iss`: `{ACCOUNT}.{USER}.SHA256:{public_key_fingerprint}`
/// - `sub`: `{ACCOUNT}.{USER}`
/// - `iat`: current UTC timestamp
/// - `exp`: current UTC timestamp + 60 seconds
///
/// The account identifier is normalized: uppercase, dots replaced with hyphens.
fn generate_keypair_jwt(
    account: &str,
    username: &str,
    private_key_pem: &str,
    passphrase: Option<&str>,
) -> kyomi_connect_protocol::Result<String> {
    let private_key = parse_rsa_private_key(private_key_pem, passphrase)?;
    let fingerprint = compute_public_key_fingerprint(&private_key)?;
    let qualified_name = build_qualified_name(account, username);

    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({
        "iss": format!("{qualified_name}.SHA256:{fingerprint}"),
        "sub": qualified_name,
        "iat": now,
        "exp": now + JWT_LIFETIME_SECS,
    });

    // Sign with RS256 using the original PEM key (jsonwebtoken needs PEM)
    let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(
        export_unencrypted_pkcs8_pem(&private_key)?.as_bytes(),
    )
    .map_err(|e| Error::Internal(format!("Failed to create JWT encoding key: {e}")))?;

    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    let jwt = jsonwebtoken::encode(&header, &claims, &encoding_key)
        .map_err(|e| Error::Internal(format!("Failed to sign Snowflake JWT: {e}")))?;

    Ok(jwt)
}

/// Parse an RSA private key from PEM format, handling both encrypted and
/// unencrypted keys in PKCS#8 or PKCS#1 format.
///
/// Supports:
/// - `-----BEGIN PRIVATE KEY-----` (unencrypted PKCS#8)
/// - `-----BEGIN ENCRYPTED PRIVATE KEY-----` (encrypted PKCS#8)
/// - `-----BEGIN RSA PRIVATE KEY-----` (PKCS#1 / traditional format)
fn parse_rsa_private_key(
    pem_str: &str,
    passphrase: Option<&str>,
) -> kyomi_connect_protocol::Result<RsaPrivateKey> {
    let trimmed = pem_str.trim();

    if trimmed.contains("ENCRYPTED PRIVATE KEY") {
        // Encrypted PKCS#8 format
        let password = passphrase.ok_or_else(|| {
            Error::Provider("Private key is encrypted but no passphrase was provided".into())
        })?;
        RsaPrivateKey::from_pkcs8_encrypted_pem(trimmed, password)
            .map_err(|e| Error::Internal(format!("Failed to decrypt PKCS#8 private key: {e}")))
    } else if trimmed.contains("RSA PRIVATE KEY") {
        // PKCS#1 (traditional RSA format)
        RsaPrivateKey::from_pkcs1_pem(trimmed)
            .map_err(|e| Error::Internal(format!("Failed to parse PKCS#1 RSA private key: {e}")))
    } else {
        // Unencrypted PKCS#8 format
        RsaPrivateKey::from_pkcs8_pem(trimmed)
            .map_err(|e| Error::Internal(format!("Failed to parse PKCS#8 private key: {e}")))
    }
}

/// Compute the SHA-256 fingerprint of the public key in DER (SPKI) format.
///
/// This matches the Snowflake `RSA_PUBLIC_KEY_FP` format: the SHA-256 hash
/// of the DER-encoded SubjectPublicKeyInfo, base64-encoded.
///
/// Equivalent to:
/// ```shell
/// openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
/// ```
fn compute_public_key_fingerprint(
    private_key: &RsaPrivateKey,
) -> kyomi_connect_protocol::Result<String> {
    let public_key = private_key.to_public_key();

    // Encode the public key as DER (SubjectPublicKeyInfo / SPKI format)
    let der_bytes = public_key
        .to_public_key_der()
        .map_err(|e| Error::Internal(format!("Failed to encode public key as DER: {e}")))?;

    // SHA-256 hash of the DER bytes
    let hash = Sha256::digest(der_bytes.as_ref());

    // Base64-encode the hash
    Ok(BASE64_STANDARD.encode(hash))
}

/// Build the Snowflake qualified name: `{ACCOUNT}.{USER}`.
///
/// The account identifier is normalized per Snowflake requirements:
/// - Converted to uppercase
/// - Dots replaced with hyphens (e.g., `xy12345.us-east-1` becomes
///   `XY12345-US-EAST-1`)
///
/// The username is also uppercased.
fn build_qualified_name(account: &str, username: &str) -> String {
    let normalized_account = account.to_uppercase().replace('.', "-");
    let normalized_user = username.to_uppercase();
    format!("{normalized_account}.{normalized_user}")
}

/// Re-export the private key as unencrypted PKCS#8 PEM.
///
/// This is needed because `jsonwebtoken::EncodingKey::from_rsa_pem` requires
/// PEM format, but if the original key was encrypted or in PKCS#1 format,
/// we need to convert it after parsing.
fn export_unencrypted_pkcs8_pem(
    private_key: &RsaPrivateKey,
) -> kyomi_connect_protocol::Result<String> {
    use rsa::pkcs8::EncodePrivateKey;

    private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .map(|pem| pem.to_string())
        .map_err(|e| Error::Internal(format!("Failed to export private key as PKCS#8 PEM: {e}")))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Authenticate with Snowflake using username/password via the login endpoint.
///
/// Returns a session token that can be used as a bearer token for subsequent
/// REST API calls.
async fn login_password(
    client: &reqwest::Client,
    account: &str,
    username: &str,
    password: &str,
) -> kyomi_connect_protocol::Result<String> {
    let url = format!("https://{account}.snowflakecomputing.com/session/v1/login-request");

    let body = serde_json::json!({
        "data": {
            "LOGIN_NAME": username,
            "PASSWORD": password,
            "ACCOUNT_NAME": account,
        }
    });

    let response = tokio::time::timeout(
        crate::DATASOURCE_TIMEOUT_CONNECT,
        client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .json(&body)
            .send(),
    )
    .await
    .map_err(|_| {
        Error::Internal(format!(
            "Snowflake login timed out after {}s",
            crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
        ))
    })?
    .map_err(|e| Error::Internal(format!("Snowflake login request failed: {e}")))?;

    let response_body: Value = response
        .json()
        .await
        .map_err(|e| Error::Internal(format!("Failed to parse Snowflake login response: {e}")))?;

    // Check for success
    let success = response_body
        .get("success")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    if !success {
        let msg = response_body
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("Snowflake login failed");
        return Err(Error::Internal(msg.to_string()));
    }

    // Extract the session token
    let token = response_body
        .get("data")
        .and_then(|d| d.get("token"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| Error::Internal("Snowflake login response missing token".into()))?;

    Ok(token.to_string())
}

/// Get total row count for a SELECT query.
async fn get_total_count(provider: &SnowflakeProvider, sql: &str) -> Option<i64> {
    let count_sql = format!("SELECT COUNT(*) FROM ({sql}) AS _count_subquery");

    let result = provider.submit_statement(&count_sql).await.ok()?;

    result
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

/// Map a Snowflake type name string (from REST API `rowType`) to a numeric
/// type code for use with `map_snowflake_type_code`.
///
/// Returns `None` if the type name doesn't match a known code.
fn snowflake_type_name_to_code(type_name: &str) -> Option<i32> {
    let upper = type_name.to_uppercase();
    match upper.as_str() {
        "FIXED" | "NUMBER" | "DECIMAL" | "NUMERIC" | "INT" | "INTEGER" | "BIGINT" | "SMALLINT"
        | "TINYINT" => Some(0),
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => Some(1),
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER" | "STRING" => Some(2),
        "DATE" => Some(3),
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "DATETIME" => Some(8),
        "VARIANT" => Some(5),
        "TIMESTAMP_LTZ" => Some(6),
        "TIMESTAMP_TZ" => Some(7),
        "OBJECT" => Some(9),
        "ARRAY" => Some(10),
        "BINARY" | "VARBINARY" => Some(11),
        "TIME" => Some(12),
        "BOOLEAN" => Some(13),
        _ => None,
    }
}

/// Map a Snowflake type name directly to [`SimpleType`] as a fallback
/// when the type name doesn't map to a known numeric code.
fn map_snowflake_type_name(type_name: &str) -> crate::provider::SimpleType {
    use crate::provider::SimpleType;

    let upper = type_name.to_uppercase();
    // Strip parenthesised parameters
    let base = if let Some(paren_pos) = upper.find('(') {
        upper[..paren_pos].trim()
    } else {
        upper.trim()
    };

    match base {
        "NUMBER" | "DECIMAL" | "NUMERIC" | "INT" | "INTEGER" | "BIGINT" | "SMALLINT"
        | "TINYINT" | "FLOAT" | "DOUBLE" | "REAL" => SimpleType::Number,
        "VARCHAR" | "CHAR" | "STRING" | "TEXT" | "BINARY" | "VARBINARY" | "VARIANT" | "OBJECT"
        | "ARRAY" => SimpleType::String,
        "BOOLEAN" => SimpleType::Boolean,
        "DATE" => SimpleType::Date,
        "TIME" => SimpleType::Time,
        "DATETIME" | "TIMESTAMP" | "TIMESTAMP_NTZ" => SimpleType::Timestamp,
        "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => SimpleType::TimestampTz,
        _ => SimpleType::Unknown,
    }
}

/// Regex for Snowflake "line N" error pattern, compiled once.
static SNOWFLAKE_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)line\s+(\d+)").expect("Snowflake line regex"));

/// Parse Snowflake error for line number.
///
/// Snowflake format: `"syntax error line 3 at position 15"`
fn parse_snowflake_error_line(error_msg: &str) -> Option<u32> {
    SNOWFLAKE_LINE_RE
        .captures(error_msg)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

// ---------------------------------------------------------------------------
// Arrow conversion
// ---------------------------------------------------------------------------

/// Convert a Snowflake JSON row directly to Arrow column builders.
///
/// Snowflake returns rows as JSON arrays in the `"data"` field of the response.
/// Each element in the array corresponds to a column value. Uses [`SimpleType`]
/// from `columns` to guide type-aware conversion via the shared
/// [`crate::arrow_builder::json_value_to_arrow`].
pub(crate) fn snowflake_row_to_arrow(
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

    // --- Test RSA key for unit tests (2048-bit, generated for testing only) ---
    //
    // This is NOT a production key. It was generated solely for testing the
    // JWT generation and fingerprint computation logic.
    //
    // The expected fingerprint was computed with:
    //   openssl rsa -in key.pem -pubout -outform DER 2>/dev/null \
    //     | openssl dgst -sha256 -binary | openssl enc -base64

    const TEST_PRIVATE_KEY_PKCS8: &str = "\
-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDWhHX48hfwrV0P
cqDxXNbVIbKY6TzMwSYDmPdDXc9eeT2dwKes4mmL3/gSKB8qc543SRgrcHScNts2
DOWQk+YvVkikItISfpPPlstmN51U9qqg2WTJxGW9e4MvslE0v69OQR9pZCTsQ72h
Rsh7GMsNKFo4dA1Y4QjZelTGiJdg0CGGYVp7w8uSuEVdfaxe5gD9HBv4UbR+kqJx
n81ojP73Q+mr08G1KuERATvl/o3e+IQt5Gd+W6JiG+U+39tyr5VLF7GJdufXQ11L
tH6WTI74tgPE/+2fd9DmyTsGjHVb9vHp0PQK6SQZCou1hjI0lqYN6z15DcElxC0P
8ugV43pHAgMBAAECggEADUDXzQ6f/gWn9zlcyiyzNS3Ey/+0+u1//L7pn+be1fZl
oSZy9ZJzdOnceLqz2jqUbtP8Q0rKWZBmELvRPxJ0KT6KdGGWWwAo/61QWbtb5BDt
T8y+llyk8IT+AOdibwDcwtfxKeC/Cz3QLHOkFT7d5K02jcBVxsT4d/8/15g+ygNU
6aOpduxmAEHcxEB/j9+iPH+4tuJi3/mh6Y/owbBNMLjkYsApI5QD0iYE9AoOszN9
l7UEUTclgeX3VHiiV41eREX8N+PDfnVJIxMaq8DOSfL2nwWSsaRBe5IdfKOj7pQW
SRxEAfKgaJpJpb5DV54fS9g5ftq/fd6m9hOtbPBVIQKBgQDspzwv1U7ZqNOgKqde
rrZevzbTzPaXgwyRdv8MUcLpUPuUloDOzvK9MQNaiJoQyczhUvN6ow1Fmiujw+Jm
cKyzp0rUELZrVmjE2LljCdDJwtGBUHKuJUoRf76lpnXiVuouguPq/gIcOVId16Z+
t6CoviHNx8TicJDQv7KChuVfsQKBgQDoDfV87suhw/dr+OfkVL/oSYgjJOaKZyok
wZweXz38Cq3ty6G8GWwWFNu7bQIwgBKcpYpPB/op4bRd53y95SKx/ZCEVOd44q16
ZO35CWML6YASS1EDFVZUoCjNOA7zfOyXEYlicBArA0W46dRAfQ7XsdEQqNw6B5MI
DRDRwvqvdwKBgQDnYoRYiIl2C7oPKmVHEDBD51XmNMsOTRXmzKCHHRIkKggxug3r
JzDzho6u4E0zCPyHeyGQ0QfS+/CbSJV+b8CMT4+8VTLnNC9v+C8bBKfd/dv2QgA/
ATqwbWSsdltgmHaUT2olg4HwsqL1hrrFvykYk/5dQ2vfswwE7snNEbQHoQKBgQCF
S0dg7RYhJJzh84bYXGojtuExNsgKZjoKBQB1XcYQGd5QgrCziHLSuEaDgZlJXLfU
LS6mOPHUzuY5Lngz6AOm8/zoVpDjmmmFraYYb/Dp7cV4PLUbLU16rMjjILlN2ctY
92TQG4jd/DI3hnE6XduBwI1ToXtnBeTKMh8gLnyq/wKBgQDY5iO4TmX/mGUaTjlM
mE0q3WZ9KTpE3+a0f717pA2p/A1v3NcB307WjQHvt2k93fV0EcYeZktajCFl2oqP
MNi3WXvxj3+iypsha6pu+tyNocvhvdD0uxpfWRhFF7V/CB3zUW4jFBcZqzZYVRWf
hAll+dkyiLjrpRPDdwQ5Stv5rw==
-----END PRIVATE KEY-----";

    /// Expected SHA-256 fingerprint of the test key's public key (SPKI DER),
    /// computed with:
    /// ```shell
    /// openssl rsa -in key.pem -pubout -outform DER 2>/dev/null \
    ///   | openssl dgst -sha256 -binary | openssl enc -base64
    /// ```
    const TEST_KEY_EXPECTED_FINGERPRINT: &str = "566kQJ9cDUKm2Jd/1HoBcPa/aYpQGmtlnMUDoI+Egvo=";

    // --- Key-pair JWT generation ---

    #[test]
    fn generate_keypair_jwt_produces_valid_token() {
        let jwt = generate_keypair_jwt(
            "xy12345.us-east-1",
            "testuser",
            TEST_PRIVATE_KEY_PKCS8,
            None,
        )
        .expect("JWT generation should succeed");

        // JWT has three dot-separated parts
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "JWT should have 3 parts: header.payload.signature"
        );

        // Decode header (base64url)
        let header_json = BASE64_STANDARD
            .decode(pad_base64url(parts[0]))
            .expect("header should be valid base64");
        let header: Value = serde_json::from_slice(&header_json).expect("header should be JSON");
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");

        // Decode payload
        let payload_json = BASE64_STANDARD
            .decode(pad_base64url(parts[1]))
            .expect("payload should be valid base64");
        let payload: Value = serde_json::from_slice(&payload_json).expect("payload should be JSON");

        // Verify claims
        let iss = payload["iss"].as_str().expect("iss claim");
        let expected_iss =
            format!("XY12345-US-EAST-1.TESTUSER.SHA256:{TEST_KEY_EXPECTED_FINGERPRINT}");
        assert_eq!(
            iss, expected_iss,
            "iss should be normalized account.user.SHA256:fingerprint"
        );
        assert_eq!(
            payload["sub"].as_str().expect("sub claim"),
            "XY12345-US-EAST-1.TESTUSER"
        );
        assert!(payload["iat"].is_number(), "iat should be a number");
        assert!(payload["exp"].is_number(), "exp should be a number");

        let iat = payload["iat"].as_i64().unwrap();
        let exp = payload["exp"].as_i64().unwrap();
        assert_eq!(exp - iat, JWT_LIFETIME_SECS);
    }

    #[test]
    fn generate_keypair_jwt_account_normalization() {
        let jwt = generate_keypair_jwt(
            "myorg.account.us-west-2",
            "MyUser",
            TEST_PRIVATE_KEY_PKCS8,
            None,
        )
        .expect("JWT generation should succeed");

        let parts: Vec<&str> = jwt.split('.').collect();
        let payload_json = BASE64_STANDARD
            .decode(pad_base64url(parts[1]))
            .expect("valid base64");
        let payload: Value = serde_json::from_slice(&payload_json).expect("valid JSON");

        assert_eq!(
            payload["sub"].as_str().unwrap(),
            "MYORG-ACCOUNT-US-WEST-2.MYUSER",
            "Dots should become hyphens, everything uppercase"
        );
    }

    // --- Public key fingerprint ---

    #[test]
    fn compute_public_key_fingerprint_is_deterministic() {
        let key = RsaPrivateKey::from_pkcs8_pem(TEST_PRIVATE_KEY_PKCS8).expect("parse test key");

        let fp1 = compute_public_key_fingerprint(&key).expect("fingerprint 1");
        let fp2 = compute_public_key_fingerprint(&key).expect("fingerprint 2");

        assert_eq!(fp1, fp2, "Fingerprint should be deterministic");
        assert!(!fp1.is_empty(), "Fingerprint should not be empty");

        // Base64-encoded SHA-256 is always 44 characters (32 bytes -> 44 chars with padding)
        assert_eq!(fp1.len(), 44, "Base64-encoded SHA-256 should be 44 chars");
    }

    #[test]
    fn compute_public_key_fingerprint_matches_openssl() {
        // Verify the fingerprint exactly matches the value computed by openssl:
        //   openssl rsa -in key.pem -pubout -outform DER 2>/dev/null \
        //     | openssl dgst -sha256 -binary | openssl enc -base64
        let key = RsaPrivateKey::from_pkcs8_pem(TEST_PRIVATE_KEY_PKCS8).expect("parse test key");

        let fp = compute_public_key_fingerprint(&key).expect("fingerprint");

        assert_eq!(
            fp, TEST_KEY_EXPECTED_FINGERPRINT,
            "Fingerprint should match openssl output"
        );

        // Also verify it decodes to exactly 32 bytes (SHA-256)
        let decoded = BASE64_STANDARD
            .decode(&fp)
            .expect("fingerprint should be valid base64");
        assert_eq!(
            decoded.len(),
            32,
            "SHA-256 hash should be 32 bytes, got {}",
            decoded.len()
        );
    }

    // --- Account normalization ---

    #[test]
    fn build_qualified_name_basic() {
        assert_eq!(
            build_qualified_name("xy12345", "testuser"),
            "XY12345.TESTUSER"
        );
    }

    #[test]
    fn build_qualified_name_with_dots() {
        assert_eq!(
            build_qualified_name("xy12345.us-east-1", "admin"),
            "XY12345-US-EAST-1.ADMIN"
        );
    }

    #[test]
    fn build_qualified_name_mixed_case() {
        assert_eq!(
            build_qualified_name("MyOrg.MyAccount", "MyUser"),
            "MYORG-MYACCOUNT.MYUSER"
        );
    }

    // --- PEM key parsing ---

    #[test]
    fn parse_rsa_private_key_pkcs8_unencrypted() {
        let key = parse_rsa_private_key(TEST_PRIVATE_KEY_PKCS8, None);
        assert!(key.is_ok(), "Should parse unencrypted PKCS#8 key");
    }

    #[test]
    fn parse_rsa_private_key_invalid_pem() {
        let result = parse_rsa_private_key("not a valid PEM key", None);
        assert!(result.is_err(), "Should fail on invalid PEM");
    }

    #[test]
    fn parse_rsa_private_key_encrypted_without_passphrase() {
        // Simulating an encrypted PEM key header without a passphrase
        let fake_encrypted = "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIFHDBOBgkqhkiG==\n-----END ENCRYPTED PRIVATE KEY-----";
        let result = parse_rsa_private_key(fake_encrypted, None);
        assert!(
            result.is_err(),
            "Should fail when encrypted key has no passphrase"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("no passphrase"),
            "Error should mention missing passphrase, got: {err_msg}"
        );
    }

    // --- Error line parsing ---

    #[test]
    fn parse_snowflake_error_line_found() {
        let msg = "syntax error line 3 at position 15";
        assert_eq!(parse_snowflake_error_line(msg), Some(3));
    }

    #[test]
    fn parse_snowflake_error_line_case_insensitive() {
        let msg = "SQL error LINE 7 near 'FROM'";
        assert_eq!(parse_snowflake_error_line(msg), Some(7));
    }

    #[test]
    fn parse_snowflake_error_line_not_found() {
        let msg = "Unknown column 'foo' in query";
        assert_eq!(parse_snowflake_error_line(msg), None);
    }

    // --- Type name to code mapping ---

    #[test]
    fn snowflake_type_name_to_code_numbers() {
        assert_eq!(snowflake_type_name_to_code("NUMBER"), Some(0));
        assert_eq!(snowflake_type_name_to_code("FIXED"), Some(0));
        assert_eq!(snowflake_type_name_to_code("DECIMAL"), Some(0));
        assert_eq!(snowflake_type_name_to_code("INT"), Some(0));
        assert_eq!(snowflake_type_name_to_code("BIGINT"), Some(0));
        assert_eq!(snowflake_type_name_to_code("FLOAT"), Some(1));
        assert_eq!(snowflake_type_name_to_code("DOUBLE"), Some(1));
    }

    #[test]
    fn snowflake_type_name_to_code_strings() {
        assert_eq!(snowflake_type_name_to_code("TEXT"), Some(2));
        assert_eq!(snowflake_type_name_to_code("VARCHAR"), Some(2));
        assert_eq!(snowflake_type_name_to_code("STRING"), Some(2));
    }

    #[test]
    fn snowflake_type_name_to_code_timestamps() {
        assert_eq!(snowflake_type_name_to_code("DATE"), Some(3));
        assert_eq!(snowflake_type_name_to_code("TIMESTAMP_NTZ"), Some(8));
        assert_eq!(snowflake_type_name_to_code("TIMESTAMP_LTZ"), Some(6));
        assert_eq!(snowflake_type_name_to_code("TIMESTAMP_TZ"), Some(7));
        assert_eq!(snowflake_type_name_to_code("TIME"), Some(12));
    }

    #[test]
    fn snowflake_type_name_to_code_other() {
        assert_eq!(snowflake_type_name_to_code("BOOLEAN"), Some(13));
        assert_eq!(snowflake_type_name_to_code("VARIANT"), Some(5));
        assert_eq!(snowflake_type_name_to_code("BINARY"), Some(11));
        assert_eq!(snowflake_type_name_to_code("ARRAY"), Some(10));
        assert_eq!(snowflake_type_name_to_code("OBJECT"), Some(9));
    }

    #[test]
    fn snowflake_type_name_to_code_unknown() {
        assert_eq!(snowflake_type_name_to_code("CUSTOM_TYPE"), None);
    }

    #[test]
    fn snowflake_type_name_to_code_case_insensitive() {
        assert_eq!(snowflake_type_name_to_code("number"), Some(0));
        assert_eq!(snowflake_type_name_to_code("varchar"), Some(2));
        assert_eq!(snowflake_type_name_to_code("Boolean"), Some(13));
    }

    // --- Fallback type name mapping ---

    #[test]
    fn snowflake_type_name_fallback_parameterised() {
        use crate::provider::SimpleType;
        assert_eq!(map_snowflake_type_name("NUMBER(38,0)"), SimpleType::Number);
        assert_eq!(map_snowflake_type_name("VARCHAR(256)"), SimpleType::String);
    }

    #[test]
    fn snowflake_type_name_fallback_basic() {
        use crate::provider::SimpleType;
        assert_eq!(map_snowflake_type_name("BOOLEAN"), SimpleType::Boolean);
        assert_eq!(map_snowflake_type_name("DATE"), SimpleType::Date);
        assert_eq!(map_snowflake_type_name("TIME"), SimpleType::Time);
        assert_eq!(
            map_snowflake_type_name("TIMESTAMP_LTZ"),
            SimpleType::TimestampTz
        );
    }

    // --- snowflake_row_to_arrow ---

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

    fn sf_row_to_batch(
        values: Vec<serde_json::Value>,
        col_type: SimpleType,
    ) -> arrow::record_batch::RecordBatch {
        let columns = vec![make_col("col", col_type)];
        let mut builder = ArrowResultBuilder::new(&columns);
        snowflake_row_to_arrow(&values, &columns, &mut builder);
        builder.finish().unwrap()
    }

    #[test]
    fn sf_number_as_string_not_null() {
        let batch = sf_row_to_batch(vec![serde_json::json!("42")], SimpleType::Number);
        assert!(
            !batch.column(0).is_null(0),
            "Snowflake number-as-string must not be null"
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sf_timestamp_epoch_string_not_null() {
        // Snowflake TIMESTAMP_NTZ returns epoch seconds as a string
        let epoch_secs = 1_737_000_000.0_f64;
        let batch = sf_row_to_batch(
            vec![serde_json::json!(epoch_secs.to_string())],
            SimpleType::Timestamp,
        );
        assert!(
            !batch.column(0).is_null(0),
            "Snowflake epoch timestamp must not be null"
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected = (epoch_secs * 1_000_000.0) as i64;
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn sf_timestamp_iso_string_not_null() {
        let batch = sf_row_to_batch(
            vec![serde_json::json!("2026-01-15T14:30:00")],
            SimpleType::Timestamp,
        );
        assert!(
            !batch.column(0).is_null(0),
            "Snowflake ISO timestamp must not be null"
        );
    }

    #[test]
    fn sf_date_string_not_null() {
        let batch = sf_row_to_batch(vec![serde_json::json!("2026-01-15")], SimpleType::Date);
        assert!(
            !batch.column(0).is_null(0),
            "Snowflake date must not be null"
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
    fn sf_string_value_not_null() {
        let batch = sf_row_to_batch(
            vec![serde_json::json!("hello snowflake")],
            SimpleType::String,
        );
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "hello snowflake");
    }

    #[test]
    fn sf_boolean_true_not_null() {
        let batch = sf_row_to_batch(vec![serde_json::json!("1")], SimpleType::Boolean);
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
    }

    #[test]
    fn sf_null_value_is_null() {
        let batch = sf_row_to_batch(vec![serde_json::Value::Null], SimpleType::Number);
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn sf_multi_column_row() {
        let columns = vec![
            make_col("ts", SimpleType::Timestamp),
            make_col("n", SimpleType::Number),
            make_col("s", SimpleType::String),
        ];
        let row = vec![
            serde_json::json!("2026-01-15T14:30:00"),
            serde_json::json!("99"),
            serde_json::Value::Null,
        ];
        let mut builder = ArrowResultBuilder::new(&columns);
        snowflake_row_to_arrow(&row, &columns, &mut builder);
        let batch = builder.finish().unwrap();

        assert!(!batch.column(0).is_null(0), "ts must not be null");
        assert!(!batch.column(1).is_null(0), "n must not be null");
        assert!(batch.column(2).is_null(0), "null s must be null");

        let arr_n = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr_n.value(0) - 99.0).abs() < f64::EPSILON);
    }

    // --- Helper: Pad base64url to standard base64 for decoding ---

    fn pad_base64url(input: &str) -> String {
        let mut s = input.replace('-', "+").replace('_', "/");
        let padding = (4 - s.len() % 4) % 4;
        for _ in 0..padding {
            s.push('=');
        }
        s
    }
}
