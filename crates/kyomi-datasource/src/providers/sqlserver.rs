//! SQL Server datasource provider using `tiberius` (TDS protocol).
//!
//! Implements query execution for SQL Server databases via the native TDS
//! protocol. Uses `tiberius` for connection management and query execution.
//!
//! Shares T-SQL pagination, type mapping, and error parsing logic with the
//! Azure Synapse provider via the [`super::tsql_common`] module.
//!
//! ## Concurrency Model
//!
//! The TDS client is wrapped in `Arc<Mutex<...>>` because `tiberius::Client::query`
//! requires `&mut self`, but our `DatasourceProvider` trait uses `&self`. Each
//! provider instance holds a SINGLE connection — concurrent queries on the same
//! provider serialize at the mutex. For concurrent queries, create separate
//! provider instances (matching the Python backend's per-request pattern).
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `host` | string | `"localhost"` | SQL Server hostname |
//! | `port` | int | `1433` | TDS port |
//! | `database` | string | `"master"` | Database name |
//! | `encrypt` | bool | `true` | Enable TLS encryption |
//! | `trust_server_certificate` | bool | `false` | Trust self-signed certificates |
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
//! | `username` | string | SQL Server username (required) |
//! | `password` | string | SQL Server password (optional) |

use std::sync::Arc;

use serde_json::Value;
use tiberius::{AuthMethod, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::provider::{DatasourceProvider, DryRunResult, QueryResult};
#[cfg(feature = "ssh")]
use crate::ssh_tunnel::{SshTunnel, SshTunnelConfig};

use super::tsql_common::{self, TdsClient};

use kyomi_connect_protocol::Error;

/// Default SQL Server TDS port.
const DEFAULT_PORT: u16 = 1433;
/// Default database name.
const DEFAULT_DATABASE: &str = "master";

/// SQL Server datasource provider.
///
/// Uses `tiberius` for TDS protocol communication. The client is wrapped
/// in an `Arc<Mutex<...>>` because `tiberius::Client::query` takes `&mut self`,
/// while our trait requires `&self` methods.
pub struct SqlServerProvider {
    /// TDS client, behind a mutex because tiberius requires `&mut self`.
    client: Arc<Mutex<TdsClient>>,
    /// SSH tunnel, if configured. Held to keep the tunnel alive.
    #[cfg(feature = "ssh")]
    _ssh_tunnel: Option<SshTunnel>,
}

impl SqlServerProvider {
    /// Create a new SQL Server provider from connection config and credentials.
    ///
    /// Parses connection parameters, optionally sets up an SSH tunnel,
    /// configures TDS encryption, and establishes the connection.
    ///
    /// # Arguments
    ///
    /// * `connection_config` - Datasource-level configuration JSON.
    /// * `credentials` - Decrypted user-level credentials JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials are missing, SSH tunnel setup fails,
    /// or the TDS connection cannot be established.
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

        let encrypt = connection_config
            .get("encrypt")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let trust_server_certificate = connection_config
            .get("trust_server_certificate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let username = credentials
            .get("username")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider("SQL Server requires a username".into()))?;

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

        // When using SSH tunnel, disable encryption (tunnel provides encryption)
        #[cfg(feature = "ssh")]
        let effective_encrypt = if ssh_tunnel.is_some() { false } else { encrypt };
        #[cfg(not(feature = "ssh"))]
        let effective_encrypt = encrypt;

        tracing::info!(
            host = host,
            port = port,
            database = database,
            encrypt = effective_encrypt,
            trust_cert = trust_server_certificate,
            "Connecting to SQL Server"
        );

        // Build tiberius Config
        let mut config = Config::new();
        config.host(&host);
        config.port(port);
        config.database(&database);
        config.authentication(AuthMethod::sql_server(username, password));

        if effective_encrypt {
            config.encryption(EncryptionLevel::Required);
        } else {
            config.encryption(EncryptionLevel::NotSupported);
        }

        if trust_server_certificate {
            config.trust_cert();
        }

        // Establish TCP connection
        let addr = config.get_addr();
        let tcp =
            tokio::time::timeout(crate::DATASOURCE_TIMEOUT_CONNECT, TcpStream::connect(&addr))
                .await
                .map_err(|_| {
                    Error::Internal(format!(
                        "SQL Server TCP connection to {addr} timed out after {}s",
                        crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
                    ))
                })?
                .map_err(|e| {
                    Error::Internal(format!("SQL Server TCP connection to {addr} failed: {e}"))
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
                "SQL Server TDS handshake timed out after {}s",
                crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("SQL Server TDS connection failed: {e}")))?;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            #[cfg(feature = "ssh")]
            _ssh_tunnel: ssh_tunnel,
        })
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for SqlServerProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        let mut client = self.client.lock().await;

        let stream = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            client.simple_query("SELECT 1"),
        )
        .await
        .map_err(|_| Error::Internal("SQL Server test connection timed out".into()))?
        .map_err(|e| Error::Internal(format!("SQL Server test connection failed: {e}")))?;

        // Consume the result to ensure the query completed
        let _rows = stream
            .into_first_result()
            .await
            .map_err(|e| Error::Internal(format!("SQL Server test connection failed: {e}")))?;

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
        tsql_common::execute_tds_query(&mut client, sql, limit, offset, include_total, "SQL Server")
            .await
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        let mut client = self.client.lock().await;

        // SQL Server: use SET NOEXEC ON to validate without executing
        let showplan_sql = format!("SET NOEXEC ON; {sql}; SET NOEXEC OFF;");

        let result = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_DRY_RUN,
            client.simple_query(&showplan_sql),
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

    async fn list_databases(&self) -> crate::provider::DiscoveryResult {
        match self
            .execute_query(
                "SELECT name FROM sys.databases \
                 WHERE state = 0 \
                   AND name NOT IN ('master', 'tempdb', 'model', 'msdb') \
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
                error: Some(format!("Failed to list SQL Server databases: {e}")),
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
                let items =
                    crate::provider::extract_string_col_from_batch(result.record_batch.as_ref(), 0);
                crate::provider::DiscoveryResult { items, error: None }
            }
            Err(e) => crate::provider::DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list SQL Server schemas: {e}")),
            },
        }
    }

    async fn close(&self) {
        // Tiberius client doesn't have an explicit close method.
        // The TCP connection will be closed when the client is dropped.
        tracing::debug!("SQL Server provider closed");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_config_defaults_to_true() {
        let config = serde_json::json!({
            "host": "localhost",
            "port": 1433,
        });
        let encrypt = config
            .get("encrypt")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        assert!(encrypt);
    }

    #[test]
    fn encrypt_config_explicit_false() {
        let config = serde_json::json!({
            "host": "localhost",
            "encrypt": false,
        });
        let encrypt = config
            .get("encrypt")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        assert!(!encrypt);
    }

    #[test]
    fn trust_server_certificate_defaults_to_false() {
        let config = serde_json::json!({
            "host": "localhost",
        });
        let trust = config
            .get("trust_server_certificate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(!trust);
    }

    #[test]
    fn default_port_is_1433() {
        assert_eq!(DEFAULT_PORT, 1433);
    }

    #[test]
    fn default_database_is_master() {
        assert_eq!(DEFAULT_DATABASE, "master");
    }

    #[test]
    fn parse_connection_config_with_all_fields() {
        let config = serde_json::json!({
            "host": "db.example.com",
            "port": 1434,
            "database": "production",
            "encrypt": true,
            "trust_server_certificate": true,
        });

        let host = config
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("localhost");
        let port = config
            .get("port")
            .and_then(|v| v.as_u64())
            .map(|p| p as u16)
            .unwrap_or(DEFAULT_PORT);
        let database = config
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_DATABASE);
        let encrypt = config
            .get("encrypt")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let trust = config
            .get("trust_server_certificate")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        assert_eq!(host, "db.example.com");
        assert_eq!(port, 1434);
        assert_eq!(database, "production");
        assert!(encrypt);
        assert!(trust);
    }

    #[test]
    fn parse_connection_config_defaults() {
        let config = serde_json::json!({});

        let host = config
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("localhost");
        let port = config
            .get("port")
            .and_then(|v| v.as_u64())
            .map(|p| p as u16)
            .unwrap_or(DEFAULT_PORT);
        let database = config
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_DATABASE);

        assert_eq!(host, "localhost");
        assert_eq!(port, 1433);
        assert_eq!(database, "master");
    }
}
