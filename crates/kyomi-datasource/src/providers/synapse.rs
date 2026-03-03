//! Azure Synapse Analytics datasource provider using `tiberius` (TDS protocol).
//!
//! Implements query execution for Azure Synapse databases. Shares most logic
//! with the SQL Server provider via [`super::tsql_common`], but differs in:
//!
//! - **Authentication**: Supports SQL auth, Service Principal, and Microsoft OAuth.
//! - **Dry run**: Uses `sp_describe_first_result_set` instead of `SHOWPLAN_TEXT`.
//! - **Server format**: Typically `my-workspace.sql.azuresynapse.net`.
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `server` | string | — | Synapse endpoint (e.g., `my-workspace.sql.azuresynapse.net`) |
//! | `database` | string | `"master"` | Database name |
//! | `auth_mode` | string | `"sql"` | `sql`, `service_principal`, `oauth`, or `enterprise_oauth` |
//! | `trust_server_certificate` | bool | `false` | Trust self-signed certificates (skip TLS verification) |
//!
//! ## Credentials
//!
//! | Auth Mode | Fields | Description |
//! |-----------|--------|-------------|
//! | `sql` | `username`, `password` | SQL authentication |
//! | `service_principal` | `tenant_id`, `client_id`, `client_secret` | Azure AD service principal |
//! | `oauth` / `enterprise_oauth` | `oauth_access_token` | Pre-obtained Microsoft OAuth token |

use std::sync::Arc;

use serde_json::Value;
use tiberius::{AuthMethod, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::provider::{DatasourceProvider, DryRunResult, QueryResult};

use super::tsql_common::{self, TdsClient};

use kyomi_connect_protocol::Error;

/// Default Azure Synapse TDS port.
const DEFAULT_PORT: u16 = 1433;
/// Default database name.
const DEFAULT_DATABASE: &str = "master";
/// Azure AD token endpoint template.
const AZURE_TOKEN_URL_TEMPLATE: &str =
    "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token";
/// Azure SQL/Synapse resource scope.
const AZURE_DATABASE_SCOPE: &str = "https://database.windows.net/.default";

/// Azure Synapse Analytics datasource provider.
///
/// Uses `tiberius` for TDS protocol communication. Supports three auth modes:
/// SQL auth, Service Principal (via Azure AD token exchange), and Microsoft
/// OAuth (pre-obtained token).
pub struct SynapseProvider {
    /// TDS client, behind a mutex because tiberius requires `&mut self`.
    client: Arc<Mutex<TdsClient>>,
}

impl SynapseProvider {
    /// Create a new Synapse provider from connection config and credentials.
    ///
    /// Determines the authentication mode and establishes the TDS connection.
    ///
    /// # Arguments
    ///
    /// * `connection_config` - Datasource-level configuration JSON.
    /// * `credentials` - Decrypted user-level credentials JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials are missing/invalid, token exchange
    /// fails, or the TDS connection cannot be established.
    pub async fn new(
        connection_config: &Value,
        credentials: &Value,
    ) -> kyomi_connect_protocol::Result<Self> {
        let server = connection_config
            .get("server")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider("Azure Synapse requires a server address".into()))?
            .to_string();

        let database = connection_config
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_DATABASE)
            .to_string();

        let auth_mode = connection_config
            .get("auth_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("sql");

        let trust_server_certificate = connection_config
            .get("trust_server_certificate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        tracing::info!(
            server = server,
            database = database,
            auth_mode = auth_mode,
            trust_cert = trust_server_certificate,
            "Connecting to Azure Synapse"
        );

        // Determine the auth method based on auth_mode
        let auth = match auth_mode {
            "service_principal" => {
                Self::get_service_principal_auth(credentials).await?
            }
            "oauth" | "enterprise_oauth" => {
                Self::get_oauth_auth(credentials)?
            }
            _ => {
                // Default: SQL authentication
                Self::get_sql_auth(credentials)?
            }
        };

        // Build tiberius Config — Azure always requires encryption
        let mut config = Config::new();
        config.host(&server);
        config.port(DEFAULT_PORT);
        config.database(&database);
        config.authentication(auth);
        config.encryption(EncryptionLevel::Required);

        // Only skip TLS certificate verification when explicitly configured.
        // Azure endpoints use valid certificates signed by well-known CAs,
        // so the default (verify) is correct for production use.
        if trust_server_certificate {
            config.trust_cert();
        }

        // Establish TCP connection
        let addr = config.get_addr();
        let tcp = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            TcpStream::connect(&addr),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "Azure Synapse TCP connection to {addr} timed out after {}s",
                crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| {
            Error::Internal(format!("Azure Synapse TCP connection to {addr} failed: {e}"))
        })?;

        tcp.set_nodelay(true)
            .map_err(|e| Error::Internal(format!("Failed to set TCP_NODELAY: {e}")))?;

        // Connect via TDS
        let client = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            tiberius::Client::connect(config, tcp.compat_write()),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "Azure Synapse TDS handshake timed out after {}s",
                crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("Azure Synapse TDS connection failed: {e}")))?;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Build SQL authentication from credentials.
    fn get_sql_auth(credentials: &Value) -> kyomi_connect_protocol::Result<AuthMethod> {
        let username = credentials
            .get("username")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Provider("Azure Synapse SQL auth requires a username".into())
            })?;

        let password = credentials
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        Ok(AuthMethod::sql_server(username, password))
    }

    /// Build OAuth authentication from a pre-obtained access token.
    fn get_oauth_auth(credentials: &Value) -> kyomi_connect_protocol::Result<AuthMethod> {
        let token = credentials
            .get("oauth_access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Provider(
                    "Microsoft OAuth authentication requires oauth_access_token in credentials. \
                     Please sign in with Microsoft to connect."
                        .into(),
                )
            })?;

        Ok(AuthMethod::aad_token(token))
    }

    /// Exchange Service Principal credentials for an Azure AD access token.
    ///
    /// Posts to the Azure AD token endpoint using the client credentials
    /// grant flow, then returns `AuthMethod::aad_token(...)`.
    async fn get_service_principal_auth(
        credentials: &Value,
    ) -> kyomi_connect_protocol::Result<AuthMethod> {
        let tenant_id = credentials
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Provider("Service Principal requires tenant_id".into())
            })?;

        let client_id = credentials
            .get("client_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Provider("Service Principal requires client_id".into())
            })?;

        let client_secret = credentials
            .get("client_secret")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Provider("Service Principal requires client_secret".into())
            })?;

        let token_url =
            AZURE_TOKEN_URL_TEMPLATE.replace("{tenant_id}", tenant_id);

        tracing::info!(
            tenant_id = tenant_id,
            client_id = client_id,
            "Acquiring Azure AD token via Service Principal"
        );

        let http_client = crate::http_client()?;
        let response = tokio::time::timeout(
            crate::OAUTH_REFRESH_TIMEOUT,
            http_client
                .post(&token_url)
                .form(&[
                    ("grant_type", "client_credentials"),
                    ("client_id", client_id),
                    ("client_secret", client_secret),
                    ("scope", AZURE_DATABASE_SCOPE),
                ])
                .send(),
        )
        .await
        .map_err(|_| {
            Error::Internal("Azure AD token request timed out".into())
        })?
        .map_err(|e| {
            Error::Internal(format!("Azure AD token request failed: {e}"))
        })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Internal(format!(
                "Azure AD token request failed (HTTP {status}): {body}",
            )));
        }

        let body: Value = response
            .json()
            .await
            .map_err(|e| Error::Internal(format!("Failed to parse Azure AD token response: {e}")))?;

        let access_token = body
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                let error_desc = body
                    .get("error_description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("No access_token in response");
                Error::Internal(format!("Failed to acquire Azure AD token: {error_desc}"))
            })?;

        Ok(AuthMethod::aad_token(access_token))
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for SynapseProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        let mut client = self.client.lock().await;

        let stream = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            client.simple_query("SELECT 1"),
        )
        .await
        .map_err(|_| Error::Internal("Azure Synapse test connection timed out".into()))?
        .map_err(|e| Error::Internal(format!("Azure Synapse test connection failed: {e}")))?;

        // Consume the result to ensure the query completed
        let _rows = stream
            .into_first_result()
            .await
            .map_err(|e| Error::Internal(format!("Azure Synapse test connection failed: {e}")))?;

        Ok(true)
    }

    async fn execute_query(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
    ) -> kyomi_connect_protocol::Result<QueryResult> {
        let mut client = self.client.lock().await;
        tsql_common::execute_tds_query(
            &mut client,
            sql,
            limit,
            offset,
            include_total,
            "Azure Synapse",
        )
        .await
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        let mut client = self.client.lock().await;

        // Synapse doesn't support SHOWPLAN_TEXT, so we use sp_describe_first_result_set
        // to validate SQL without executing it. The @tsql parameter requires a string
        // literal, so we escape single quotes (SQL standard: ' → '') and wrap in N'...'.
        // This is not a SQL injection risk since users can already run arbitrary SQL.
        let sql_escaped = sql.replace('\'', "''");
        let describe_sql =
            format!("EXEC sp_describe_first_result_set @tsql = N'{sql_escaped}'");

        let result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_DRY_RUN,
            client.simple_query(&describe_sql),
        )
        .await;

        match result {
            Ok(Ok(stream)) => {
                // Consume the stream to check for errors
                match stream.into_first_result().await {
                    Ok(_) => Ok(DryRunResult::success("Query valid")),
                    Err(e) => {
                        let line = tsql_common::parse_tsql_error_line(&e.to_string());
                        Ok(DryRunResult::failure(e.to_string(), line, None))
                    }
                }
            }
            Ok(Err(e)) => {
                let line = tsql_common::parse_tsql_error_line(&e.to_string());
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

    async fn execute_query_stream(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
        chunk_size: Option<u32>,
    ) -> kyomi_connect_protocol::Result<kyomi_connect_protocol::QueryStream> {
        tsql_common::execute_tds_query_stream(
            Arc::clone(&self.client),
            sql,
            limit,
            offset,
            include_total,
            chunk_size,
            "Azure Synapse",
        )
        .await
    }

    async fn list_databases(&self) -> crate::provider::DiscoveryResult {
        // Synapse includes all databases (no system exclusion) — matches Python's
        // `super().list_databases(exclude_system=False)` override.
        match self
            .execute_query(
                "SELECT name FROM sys.databases \
                 WHERE state = 0 \
                 ORDER BY name",
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
                error: Some(format!("Failed to list Synapse databases: {e}")),
            },
        }
    }

    async fn list_schemas(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query(
                "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA \
                 WHERE schema_name NOT IN (\
                   'sys', 'INFORMATION_SCHEMA', 'guest', \
                   'db_owner', 'db_accessadmin', 'db_securityadmin', \
                   'db_ddladmin', 'db_backupoperator', 'db_datareader', \
                   'db_datawriter', 'db_denydatareader', 'db_denydatawriter') \
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
                error: Some(format!("Failed to list Synapse schemas: {e}")),
            },
        }
    }

    async fn close(&self) {
        // Tiberius client doesn't have an explicit close method.
        // The TCP connection will be closed when the client is dropped.
        tracing::debug!("Azure Synapse provider closed");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_port_is_1433() {
        assert_eq!(DEFAULT_PORT, 1433);
    }

    #[test]
    fn default_database_is_master() {
        assert_eq!(DEFAULT_DATABASE, "master");
    }

    #[test]
    fn azure_token_url_template_contains_placeholder() {
        assert!(AZURE_TOKEN_URL_TEMPLATE.contains("{tenant_id}"));
    }

    #[test]
    fn azure_database_scope_is_correct() {
        assert_eq!(AZURE_DATABASE_SCOPE, "https://database.windows.net/.default");
    }

    #[test]
    fn sql_auth_requires_username() {
        let creds = serde_json::json!({});
        let result = SynapseProvider::get_sql_auth(&creds);
        assert!(result.is_err());
    }

    #[test]
    fn sql_auth_with_valid_credentials() {
        let creds = serde_json::json!({
            "username": "admin",
            "password": "secret",
        });
        let result = SynapseProvider::get_sql_auth(&creds);
        assert!(result.is_ok());
    }

    #[test]
    fn sql_auth_password_optional() {
        let creds = serde_json::json!({
            "username": "admin",
        });
        let result = SynapseProvider::get_sql_auth(&creds);
        assert!(result.is_ok());
    }

    #[test]
    fn oauth_auth_requires_token() {
        let creds = serde_json::json!({});
        let result = SynapseProvider::get_oauth_auth(&creds);
        assert!(result.is_err());
    }

    #[test]
    fn oauth_auth_with_valid_token() {
        let creds = serde_json::json!({
            "oauth_access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.test",
        });
        let result = SynapseProvider::get_oauth_auth(&creds);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_auth_mode_defaults_to_sql() {
        let config = serde_json::json!({
            "server": "test.sql.azuresynapse.net",
        });
        let auth_mode = config
            .get("auth_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("sql");
        assert_eq!(auth_mode, "sql");
    }

    #[test]
    fn parse_auth_mode_service_principal() {
        let config = serde_json::json!({
            "server": "test.sql.azuresynapse.net",
            "auth_mode": "service_principal",
        });
        let auth_mode = config
            .get("auth_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("sql");
        assert_eq!(auth_mode, "service_principal");
    }

    #[test]
    fn parse_auth_mode_oauth() {
        let config = serde_json::json!({
            "server": "test.sql.azuresynapse.net",
            "auth_mode": "oauth",
        });
        let auth_mode = config
            .get("auth_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("sql");
        assert_eq!(auth_mode, "oauth");
    }

    #[test]
    fn trust_server_certificate_defaults_to_false() {
        let config = serde_json::json!({
            "server": "test.sql.azuresynapse.net",
        });
        let trust = config
            .get("trust_server_certificate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(!trust);
    }

    #[test]
    fn trust_server_certificate_explicit_true() {
        let config = serde_json::json!({
            "server": "test.sql.azuresynapse.net",
            "trust_server_certificate": true,
        });
        let trust = config
            .get("trust_server_certificate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(trust);
    }

    #[test]
    fn trust_server_certificate_explicit_false() {
        let config = serde_json::json!({
            "server": "test.sql.azuresynapse.net",
            "trust_server_certificate": false,
        });
        let trust = config
            .get("trust_server_certificate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(!trust);
    }

    #[test]
    fn trust_server_certificate_non_bool_defaults_to_false() {
        let config = serde_json::json!({
            "server": "test.sql.azuresynapse.net",
            "trust_server_certificate": "yes",
        });
        let trust = config
            .get("trust_server_certificate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(!trust);
    }

    #[test]
    fn sql_escaping_for_dry_run() {
        let sql = "SELECT * FROM users WHERE name = 'O''Brien'";
        let escaped = sql.replace('\'', "''");
        // The inner quotes get doubled again
        assert!(escaped.contains("O''''Brien"));
    }

    #[test]
    fn service_principal_requires_tenant_id() {
        let creds = serde_json::json!({
            "client_id": "test-id",
            "client_secret": "test-secret",
        });
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(SynapseProvider::get_service_principal_auth(&creds));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("tenant_id"));
    }

    #[test]
    fn service_principal_requires_client_id() {
        let creds = serde_json::json!({
            "tenant_id": "test-tenant",
            "client_secret": "test-secret",
        });
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(SynapseProvider::get_service_principal_auth(&creds));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("client_id"));
    }

    #[test]
    fn service_principal_requires_client_secret() {
        let creds = serde_json::json!({
            "tenant_id": "test-tenant",
            "client_id": "test-id",
        });
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(SynapseProvider::get_service_principal_auth(&creds));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("client_secret"));
    }

    #[test]
    fn token_url_substitution() {
        let tenant_id = "my-tenant-123";
        let url = AZURE_TOKEN_URL_TEMPLATE.replace("{tenant_id}", tenant_id);
        assert_eq!(
            url,
            "https://login.microsoftonline.com/my-tenant-123/oauth2/v2.0/token"
        );
    }
}
