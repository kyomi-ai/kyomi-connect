//! Redshift datasource provider using `sqlx` (PostgreSQL wire protocol).
//!
//! Amazon Redshift is wire-compatible with PostgreSQL, so we use `sqlx::PgPool`
//! for connections. The main difference is in type mapping: Redshift uses
//! [`crate::type_mapping::map_redshift_type_code`] which delegates to the
//! PostgreSQL OID mapper, plus Redshift-specific error parsing for `LINE N:`
//! and `Position: N` patterns.
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `host` | string | — | Redshift cluster endpoint (required) |
//! | `port` | int | `5439` | Redshift port |
//! | `database` | string | — | Database name (required) |
//! | `ssl` | bool | `true` | Enable SSL |
//! | `sslmode` | string | `"verify-ca"` | SSL mode |
//! | `ssh_enabled` | bool | `false` | Whether to use SSH tunnel |
//!
//! ## Credentials
//!
//! **Standard auth:**
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `username` | string | Redshift username |
//! | `password` | string | Redshift password |
//!
//! **IAM auth:**
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `iam` | bool | Must be `true` to enable IAM auth |
//! | `cluster_identifier` | string | Redshift cluster identifier |
//! | `region` | string | AWS region (e.g., `us-east-1`) |
//! | `db_user` | string | Database user to assume |
//! | `access_key_id` | string | AWS access key (optional, uses default credentials if omitted) |
//! | `secret_access_key` | string | AWS secret key (optional) |

use std::sync::LazyLock;
use std::time::Instant;

use regex::Regex;
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{Column, PgPool, Row, TypeInfo};

use crate::provider::{ColumnInfo, DatasourceProvider, DryRunResult, QueryResult, QueryStatus};
use crate::providers::aws_sigv4::{self, AwsCredentials};
use crate::providers::postgres::{
    char_position_to_line_col, pg_row_value_to_json, pg_type_name_to_oid,
};
use crate::providers::sqlx_common;
#[cfg(feature = "ssh")]
use crate::ssh_tunnel::{SshTunnel, SshTunnelConfig};
use crate::type_mapping::map_redshift_type_code;

use kyomi_connect_protocol::Error;

/// Default Redshift port.
const DEFAULT_PORT: u16 = 5439;
/// Default SSL mode for Redshift.
const DEFAULT_SSL_MODE: &str = "verify-ca";
/// AWS Redshift API version used in query parameters.
const REDSHIFT_API_VERSION: &str = "2012-12-01";

/// Redshift datasource provider.
///
/// Uses `sqlx::PgPool` since Redshift speaks the PostgreSQL wire protocol.
/// Type mapping uses [`map_redshift_type_code`] which delegates to the
/// PostgreSQL OID mapper.
///
/// Supports both standard username/password authentication and IAM
/// authentication via the AWS `GetClusterCredentials` API.
pub struct RedshiftProvider {
    /// PostgreSQL-compatible connection pool.
    pool: PgPool,
    /// SSH tunnel, if configured. Held to keep the tunnel alive.
    #[cfg(feature = "ssh")]
    _ssh_tunnel: Option<SshTunnel>,
}

impl RedshiftProvider {
    /// Create a new Redshift provider from connection config and credentials.
    ///
    /// Detects the authentication mode from credentials:
    /// - If `iam` is `true`, calls the AWS `GetClusterCredentials` API to
    ///   obtain temporary database credentials, then connects with those.
    /// - Otherwise, uses the standard `username`/`password` fields.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing, SSH tunnel setup fails,
    /// IAM credential retrieval fails, or the connection cannot be established.
    pub async fn new(
        connection_config: &Value,
        credentials: &Value,
    ) -> kyomi_connect_protocol::Result<Self> {
        // When the `ssh` feature is enabled, these are reassigned to the tunnel endpoint.
        #[cfg_attr(not(feature = "ssh"), allow(unused_mut))]
        let mut host = connection_config
            .get("host")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider("Redshift host is required".into()))?
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
            .ok_or_else(|| Error::Provider("Redshift database is required".into()))?;

        let ssl_mode_str = connection_config
            .get("sslmode")
            .or_else(|| connection_config.get("ssl_mode"))
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_SSL_MODE);

        // Detect IAM auth mode
        let use_iam = credentials
            .get("iam")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let (username, password) = if use_iam {
            resolve_iam_credentials(credentials, database).await?
        } else {
            resolve_standard_credentials(credentials)?
        };

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

        // SSL mode: disable SSL when using SSH tunnel
        #[cfg(feature = "ssh")]
        let effective_ssl_mode = if ssh_tunnel.is_some() {
            "disable"
        } else {
            ssl_mode_str
        };
        #[cfg(not(feature = "ssh"))]
        let effective_ssl_mode = ssl_mode_str;

        let ssl_mode = parse_redshift_ssl_mode(effective_ssl_mode);

        tracing::info!(
            host = host,
            port = port,
            database = database,
            ssl_mode = effective_ssl_mode,
            iam_auth = use_iam,
            "Connecting to Redshift"
        );

        let mut connect_options = PgConnectOptions::new()
            .host(&host)
            .port(port)
            .database(database)
            .username(&username)
            .password(&password)
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
                "Redshift connection timed out after {}s",
                crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("Redshift connection failed: {e}")))?;

        Ok(Self {
            pool,
            #[cfg(feature = "ssh")]
            _ssh_tunnel: ssh_tunnel,
        })
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for RedshiftProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        let result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            sqlx::query("SELECT 1").execute(&self.pool),
        )
        .await
        .map_err(|_| Error::Internal("Redshift test connection timed out".into()))?
        .map_err(|e| Error::Internal(format!("Redshift test connection failed: {e}")))?;

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

        let prepared = sqlx_common::prepare_query(sql, limit, offset);

        // Get total count if requested
        let total_rows = if prepared.is_select && include_total {
            get_total_count(&self.pool, &prepared.sql_stripped).await
        } else {
            None
        };

        let effective_limit = limit.unwrap_or(1000);
        let paginated_sql = prepared.sql;

        tracing::debug!(sql = %paginated_sql.chars().take(200).collect::<String>(), "Executing Redshift query");

        // Execute with timeout
        let query_result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_QUERY,
            sqlx::query(&paginated_sql).fetch_all(&self.pool),
        )
        .await;

        let rows_result = match query_result {
            Ok(Ok(rows)) => rows,
            Ok(Err(e)) => {
                tracing::error!(error = %e, "Redshift query error");
                return Ok(QueryResult {
                    status: QueryStatus::Error,
                    columns: None,
                    rows: None,
                    total_rows: None,
                    has_more: false,
                    bytes_processed: None,
                    execution_time_ms: Some(start.elapsed().as_millis() as i64),
                    error: Some(e.to_string()),
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
                });
            }
        };

        // Extract column info using Redshift type mapping
        let columns = if let Some(first_row) = rows_result.first() {
            first_row
                .columns()
                .iter()
                .map(|col| {
                    let oid = pg_type_name_to_oid(col.type_info().name());
                    ColumnInfo {
                        name: col.name().to_string(),
                        col_type: map_redshift_type_code(oid),
                    }
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Convert rows to JSON (reuse PostgreSQL row conversion since wire format is the same)
        let mut json_rows = Vec::with_capacity(rows_result.len());
        for row in &rows_result {
            let mut row_values = Vec::with_capacity(columns.len());
            for (i, col_info) in columns.iter().enumerate() {
                let value = pg_row_value_to_json(row, i, col_info.col_type);
                row_values.push(value);
            }
            json_rows.push(row_values);
        }

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
        })
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

        let prepared = sqlx_common::prepare_query(sql, limit, offset);

        // Get total count if requested (only for SELECT/WITH queries)
        let total_rows = if prepared.is_select && include_total {
            get_total_count(&self.pool, &prepared.sql_stripped).await
        } else {
            None
        };

        tracing::debug!(
            sql = %prepared.sql.chars().take(200).collect::<String>(),
            "Streaming Redshift query"
        );

        let paginated_sql = prepared.sql;
        let pool = self.pool.clone();

        let (tx, stream) = sqlx_common::make_stream_channel();

        tokio::spawn(async move {
            let row_stream = sqlx::query(&paginated_sql).fetch(&pool);
            sqlx_common::drive_sqlx_stream(
                tx,
                row_stream,
                total_rows,
                chunk_size,
                start,
                |row: &sqlx::postgres::PgRow| {
                    use sqlx::{Column, TypeInfo};
                    row.columns()
                        .iter()
                        .map(|col| {
                            let oid = pg_type_name_to_oid(col.type_info().name());
                            ColumnInfo {
                                name: col.name().to_string(),
                                col_type: map_redshift_type_code(oid),
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
                let (line, column) = parse_redshift_error_position(&e, sql);
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

    async fn list_schemas(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query(
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE schema_name NOT IN ('pg_catalog', 'pg_internal', 'information_schema', 'pg_toast') \
                   AND schema_name NOT LIKE 'pg_temp_%' \
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
                crate::provider::DiscoveryResult {
                    items,
                    error: None,
                }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list Redshift schemas: {e}")),
            },
        }
    }

    async fn close(&self) {
        self.pool.close().await;
        tracing::debug!("Redshift connection pool closed");
    }
}

// ---------------------------------------------------------------------------
// Credential resolution
// ---------------------------------------------------------------------------

/// Resolve standard username/password credentials from the credentials object.
fn resolve_standard_credentials(
    credentials: &Value,
) -> kyomi_connect_protocol::Result<(String, String)> {
    let username = credentials
        .get("username")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Provider("Redshift requires a username".into()))?
        .to_string();

    let password = credentials
        .get("password")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Ok((username, password))
}

/// Resolve IAM credentials by calling the AWS `GetClusterCredentials` API.
///
/// Returns `(username, password)` from the temporary credentials issued by AWS.
///
/// # Required credential fields
///
/// - `cluster_identifier` - Redshift cluster identifier
/// - `region` - AWS region
/// - `db_user` - Database user to assume
///
/// # Optional credential fields
///
/// - `access_key_id` + `secret_access_key` - Explicit AWS credentials.
///   If omitted, falls back to the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
///   environment variables.
async fn resolve_iam_credentials(
    credentials: &Value,
    database: &str,
) -> kyomi_connect_protocol::Result<(String, String)> {
    let cluster_identifier = credentials
        .get("cluster_identifier")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Provider("Redshift IAM auth requires cluster_identifier".into()))?;

    let region = credentials
        .get("region")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Provider("Redshift IAM auth requires region".into()))?;

    let db_user = credentials
        .get("db_user")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Provider("Redshift IAM auth requires db_user".into()))?;

    // AWS credentials: prefer explicit fields, fall back to environment variables
    let access_key_id = credentials
        .get("access_key_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok());

    let secret_access_key = credentials
        .get("secret_access_key")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok());

    let aws_creds = match (access_key_id, secret_access_key) {
        (Some(key_id), Some(secret)) => AwsCredentials {
            access_key_id: key_id,
            secret_access_key: secret,
        },
        _ => {
            return Err(Error::Provider(
                "Redshift IAM auth requires AWS credentials: either provide access_key_id + \
                 secret_access_key in credentials, or set AWS_ACCESS_KEY_ID and \
                 AWS_SECRET_ACCESS_KEY environment variables"
                    .into(),
            ));
        }
    };

    tracing::info!(
        cluster_identifier = cluster_identifier,
        region = region,
        db_user = db_user,
        "Requesting Redshift IAM temporary credentials"
    );

    let temp_creds =
        get_cluster_credentials(&aws_creds, region, cluster_identifier, db_user, database).await?;

    tracing::info!(
        db_user = temp_creds.db_user,
        expiration = temp_creds.expiration,
        "Received Redshift IAM temporary credentials"
    );

    Ok((temp_creds.db_user, temp_creds.db_password))
}

// ---------------------------------------------------------------------------
// AWS GetClusterCredentials API
// ---------------------------------------------------------------------------

/// Temporary credentials returned by the Redshift `GetClusterCredentials` API.
#[derive(Debug)]
struct TempCredentials {
    /// Database username (prefixed with `IAM:` by AWS).
    db_user: String,
    /// Temporary database password.
    db_password: String,
    /// ISO 8601 expiration timestamp.
    expiration: String,
}

/// Call the AWS Redshift `GetClusterCredentials` API to obtain temporary
/// database credentials for IAM authentication.
///
/// Makes a signed HTTP GET request to the Redshift API endpoint using
/// AWS Signature V4 authentication.
///
/// # Arguments
///
/// * `aws_creds` - AWS access key ID and secret access key.
/// * `region` - AWS region (e.g., "us-east-1").
/// * `cluster_identifier` - Redshift cluster identifier.
/// * `db_user` - Database user to assume.
/// * `database` - Database name.
///
/// # Returns
///
/// Temporary credentials with username, password, and expiration timestamp.
async fn get_cluster_credentials(
    aws_creds: &AwsCredentials,
    region: &str,
    cluster_identifier: &str,
    db_user: &str,
    database: &str,
) -> kyomi_connect_protocol::Result<TempCredentials> {
    let host = format!("redshift.{region}.amazonaws.com");

    // Build query parameters
    let params = [
        ("Action", "GetClusterCredentials"),
        ("AutoCreate", "false"),
        ("ClusterIdentifier", cluster_identifier),
        ("DbName", database),
        ("DbUser", db_user),
        ("Version", REDSHIFT_API_VERSION),
    ];

    let canonical_query_string = aws_sigv4::build_canonical_query_string(&params);

    // Generate timestamp
    let now = chrono::Utc::now();
    let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = now.format("%Y%m%d").to_string();

    // Sign the request
    let authorization = aws_sigv4::sign_request(
        "GET",
        &host,
        "/",
        &canonical_query_string,
        region,
        "redshift",
        aws_creds,
        &datetime,
        &datestamp,
    );

    // Build the full URL
    let url = format!("https://{host}/?{canonical_query_string}");

    // Make the HTTP request
    let client = crate::http_client()?;
    let response = tokio::time::timeout(
        crate::DATASOURCE_TIMEOUT_CONNECT,
        client
            .get(&url)
            .header("Host", &host)
            .header("X-Amz-Date", &datetime)
            .header("Authorization", &authorization)
            .send(),
    )
    .await
    .map_err(|_| Error::Internal("Redshift GetClusterCredentials API request timed out".into()))?
    .map_err(|e| {
        Error::Internal(format!(
            "Redshift GetClusterCredentials API request failed: {e}"
        ))
    })?;

    let status = response.status();
    let body = response.text().await.map_err(|e| {
        Error::Internal(format!(
            "Failed to read Redshift GetClusterCredentials response: {e}"
        ))
    })?;

    if !status.is_success() {
        let error_msg = parse_aws_error_xml(&body).unwrap_or_else(|| format!("HTTP {status}"));
        return Err(Error::Internal(format!(
            "Redshift GetClusterCredentials failed: {error_msg}"
        )));
    }

    // Parse the XML response
    parse_get_cluster_credentials_response(&body)
}

/// Parse the `GetClusterCredentials` XML response to extract temporary credentials.
///
/// Expected format:
/// ```xml
/// <GetClusterCredentialsResponse>
///   <GetClusterCredentialsResult>
///     <DbUser>IAM:adminuser</DbUser>
///     <Expiration>2019-12-27T19:44:51.001Z</Expiration>
///     <DbPassword>AMAFUyyuros/...</DbPassword>
///   </GetClusterCredentialsResult>
/// </GetClusterCredentialsResponse>
/// ```
fn parse_get_cluster_credentials_response(
    xml: &str,
) -> kyomi_connect_protocol::Result<TempCredentials> {
    let db_user = extract_xml_element(xml, "DbUser").ok_or_else(|| {
        Error::Internal("Redshift GetClusterCredentials response missing DbUser element".into())
    })?;

    let db_password = extract_xml_element(xml, "DbPassword").ok_or_else(|| {
        Error::Internal("Redshift GetClusterCredentials response missing DbPassword element".into())
    })?;

    let expiration = extract_xml_element(xml, "Expiration").unwrap_or_default();

    Ok(TempCredentials {
        db_user,
        db_password,
        expiration,
    })
}

/// Extract the text content of a simple XML element.
///
/// Handles the simple case of `<Tag>content</Tag>` — does not support
/// nested elements, attributes, or CDATA sections. This is sufficient
/// for the flat structure of AWS API responses.
fn extract_xml_element(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

/// Extract an error message from an AWS XML error response.
///
/// AWS errors follow this format:
/// ```xml
/// <ErrorResponse>
///   <Error>
///     <Code>ClusterNotFound</Code>
///     <Message>Cluster xyz not found.</Message>
///   </Error>
/// </ErrorResponse>
/// ```
fn parse_aws_error_xml(xml: &str) -> Option<String> {
    let code = extract_xml_element(xml, "Code").unwrap_or_default();
    let message = extract_xml_element(xml, "Message")?;
    if code.is_empty() {
        Some(message)
    } else {
        Some(format!("{code}: {message}"))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a Redshift SSL mode string to the sqlx `PgSslMode` enum.
fn parse_redshift_ssl_mode(mode: &str) -> PgSslMode {
    match mode {
        "disable" => PgSslMode::Disable,
        "require" => PgSslMode::Require,
        "verify-ca" => PgSslMode::VerifyCa,
        "verify-full" => PgSslMode::VerifyFull,
        _ => {
            tracing::warn!(
                mode = mode,
                "Unknown Redshift ssl_mode, defaulting to VerifyCa"
            );
            PgSslMode::VerifyCa
        }
    }
}

/// Get total row count for a SELECT query on Redshift.
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
            tracing::warn!(error = %e, "Failed to get Redshift total count, continuing without it");
            None
        }
        Err(_) => {
            tracing::warn!("Redshift total count query timed out, continuing without it");
            None
        }
    }
}

/// Regex for Redshift "LINE N:" error pattern, compiled once.
static REDSHIFT_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)LINE\s+(\d+):").expect("Redshift line regex"));

/// Regex for Redshift "Position: N" error pattern, compiled once.
static REDSHIFT_POSITION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)Position:\s*(\d+)").expect("Redshift position regex"));

/// Parse Redshift error for line/column position.
///
/// Redshift errors can contain:
/// - `LINE N:` pattern (PostgreSQL-compatible)
/// - `Position: N` pattern (character offset)
/// - `at character N` pattern (PostgreSQL standard)
fn parse_redshift_error_position(error: &sqlx::Error, sql: &str) -> (Option<u32>, Option<u32>) {
    let msg = error.to_string();

    // Try LINE N: pattern first
    if let Some(line) = REDSHIFT_LINE_RE
        .captures(&msg)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
    {
        return (Some(line), None);
    }

    // Try Position: N pattern
    if let Some(pos) = REDSHIFT_POSITION_RE
        .captures(&msg)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<usize>().ok())
    {
        return char_position_to_line_col(sql, pos);
    }

    // Also try the PostgreSQL "at character N" pattern
    if let Some(idx) = msg.find("at character ") {
        let start = idx + "at character ".len();
        let num_str: String = msg[start..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(pos) = num_str.parse::<usize>() {
            return char_position_to_line_col(sql, pos);
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

    // --- SSL mode parsing ---

    #[test]
    fn parse_redshift_ssl_modes() {
        assert!(matches!(
            parse_redshift_ssl_mode("disable"),
            PgSslMode::Disable
        ));
        assert!(matches!(
            parse_redshift_ssl_mode("require"),
            PgSslMode::Require
        ));
        assert!(matches!(
            parse_redshift_ssl_mode("verify-ca"),
            PgSslMode::VerifyCa
        ));
        assert!(matches!(
            parse_redshift_ssl_mode("verify-full"),
            PgSslMode::VerifyFull
        ));
        // Default to VerifyCa for Redshift
        assert!(matches!(
            parse_redshift_ssl_mode("unknown"),
            PgSslMode::VerifyCa
        ));
    }

    // --- Error position parsing ---

    #[test]
    fn parse_redshift_line_pattern() {
        let msg = "ERROR: syntax error\nLINE 2: SELEC * FROM\n         ^";
        let line_re = Regex::new(r"(?i)LINE\s+(\d+):").expect("regex");
        let line = line_re
            .captures(msg)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<u32>().ok());
        assert_eq!(line, Some(2));
    }

    #[test]
    fn parse_redshift_position_pattern() {
        let msg = "ERROR: syntax error at or near \"SELEC\"\nPosition: 15";
        let pos_re = Regex::new(r"(?i)Position:\s*(\d+)").expect("regex");
        let pos = pos_re
            .captures(msg)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<usize>().ok());
        assert_eq!(pos, Some(15));
    }

    // --- XML parsing ---

    #[test]
    fn extract_xml_element_basic() {
        let xml = "<Root><Name>hello</Name></Root>";
        assert_eq!(extract_xml_element(xml, "Name"), Some("hello".into()));
    }

    #[test]
    fn extract_xml_element_missing() {
        let xml = "<Root><Other>value</Other></Root>";
        assert_eq!(extract_xml_element(xml, "Name"), None);
    }

    #[test]
    fn extract_xml_element_empty() {
        let xml = "<Root><Name></Name></Root>";
        assert_eq!(extract_xml_element(xml, "Name"), Some(String::new()));
    }

    #[test]
    fn parse_get_cluster_credentials_response_valid() {
        let xml = r#"<GetClusterCredentialsResponse xmlns="http://redshift.amazonaws.com/doc/2012-12-01/">
  <GetClusterCredentialsResult>
    <DbUser>IAM:adminuser</DbUser>
    <Expiration>2019-12-27T19:44:51.001Z</Expiration>
    <DbPassword>AMAFUyyuros/QjxPTtgzcsuQsqzIasdzJEN04aCtWDzXx1O9d6UmpkBtvEeqFly/EXAMPLE==</DbPassword>
  </GetClusterCredentialsResult>
  <ResponseMetadata>
    <RequestId>404b34b9-28df-11ea-a940-1b28a85fd753</RequestId>
  </ResponseMetadata>
</GetClusterCredentialsResponse>"#;

        let result = parse_get_cluster_credentials_response(xml).expect("should parse");
        assert_eq!(result.db_user, "IAM:adminuser");
        assert_eq!(
            result.db_password,
            "AMAFUyyuros/QjxPTtgzcsuQsqzIasdzJEN04aCtWDzXx1O9d6UmpkBtvEeqFly/EXAMPLE=="
        );
        assert_eq!(result.expiration, "2019-12-27T19:44:51.001Z");
    }

    #[test]
    fn parse_get_cluster_credentials_response_missing_user() {
        let xml = r#"<GetClusterCredentialsResponse>
  <GetClusterCredentialsResult>
    <DbPassword>secret</DbPassword>
  </GetClusterCredentialsResult>
</GetClusterCredentialsResponse>"#;

        let result = parse_get_cluster_credentials_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("DbUser"), "Error should mention DbUser: {err}");
    }

    #[test]
    fn parse_get_cluster_credentials_response_missing_password() {
        let xml = r#"<GetClusterCredentialsResponse>
  <GetClusterCredentialsResult>
    <DbUser>IAM:user</DbUser>
  </GetClusterCredentialsResult>
</GetClusterCredentialsResponse>"#;

        let result = parse_get_cluster_credentials_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("DbPassword"),
            "Error should mention DbPassword: {err}"
        );
    }

    #[test]
    fn parse_aws_error_xml_with_code_and_message() {
        let xml = r#"<ErrorResponse xmlns="http://redshift.amazonaws.com/doc/2012-12-01/">
  <Error>
    <Type>Sender</Type>
    <Code>ClusterNotFound</Code>
    <Message>Cluster mycluster not found.</Message>
  </Error>
  <RequestId>abc-123</RequestId>
</ErrorResponse>"#;

        let result = parse_aws_error_xml(xml);
        assert_eq!(
            result,
            Some("ClusterNotFound: Cluster mycluster not found.".into())
        );
    }

    #[test]
    fn parse_aws_error_xml_message_only() {
        let xml =
            "<ErrorResponse><Error><Message>Something went wrong</Message></Error></ErrorResponse>";
        let result = parse_aws_error_xml(xml);
        assert_eq!(result, Some("Something went wrong".into()));
    }

    #[test]
    fn parse_aws_error_xml_no_message() {
        let xml = "<ErrorResponse><Error><Code>InternalError</Code></Error></ErrorResponse>";
        let result = parse_aws_error_xml(xml);
        assert_eq!(result, None);
    }

    // --- Credential resolution ---

    #[test]
    fn resolve_standard_credentials_success() {
        let creds = serde_json::json!({
            "username": "admin",
            "password": "secret123",
        });
        let (user, pass) = resolve_standard_credentials(&creds).expect("should resolve");
        assert_eq!(user, "admin");
        assert_eq!(pass, "secret123");
    }

    #[test]
    fn resolve_standard_credentials_no_password() {
        let creds = serde_json::json!({
            "username": "admin",
        });
        let (user, pass) = resolve_standard_credentials(&creds).expect("should resolve");
        assert_eq!(user, "admin");
        assert_eq!(pass, "");
    }

    #[test]
    fn resolve_standard_credentials_missing_username() {
        let creds = serde_json::json!({
            "password": "secret",
        });
        let result = resolve_standard_credentials(&creds);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("username"),
            "Error should mention username: {err}"
        );
    }

    #[tokio::test]
    async fn resolve_iam_credentials_missing_cluster_identifier() {
        let creds = serde_json::json!({
            "iam": true,
            "region": "us-east-1",
            "db_user": "admin",
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
        });
        let result = resolve_iam_credentials(&creds, "dev").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cluster_identifier"), "Error: {err}");
    }

    #[tokio::test]
    async fn resolve_iam_credentials_missing_region() {
        let creds = serde_json::json!({
            "iam": true,
            "cluster_identifier": "mycluster",
            "db_user": "admin",
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
        });
        let result = resolve_iam_credentials(&creds, "dev").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("region"), "Error: {err}");
    }

    #[tokio::test]
    async fn resolve_iam_credentials_missing_db_user() {
        let creds = serde_json::json!({
            "iam": true,
            "cluster_identifier": "mycluster",
            "region": "us-east-1",
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
        });
        let result = resolve_iam_credentials(&creds, "dev").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("db_user"), "Error: {err}");
    }

    #[tokio::test]
    async fn resolve_iam_credentials_missing_aws_credentials() {
        let creds = serde_json::json!({
            "iam": true,
            "cluster_identifier": "mycluster",
            "region": "us-east-1",
            "db_user": "admin",
        });
        // Clear environment variables for this test
        // SAFETY: This is only called in tests which are run serially for
        // environment-dependent tests.
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        }
        let result = resolve_iam_credentials(&creds, "dev").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("AWS credentials"), "Error: {err}");
    }

    // --- IAM detection in provider construction ---

    #[test]
    fn detect_iam_auth_from_credentials() {
        let creds_iam = serde_json::json!({ "iam": true });
        let creds_standard = serde_json::json!({ "username": "admin" });
        let creds_empty = serde_json::json!({});

        assert!(
            creds_iam
                .get("iam")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        );
        assert!(
            !creds_standard
                .get("iam")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        );
        assert!(
            !creds_empty
                .get("iam")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        );
    }
}
