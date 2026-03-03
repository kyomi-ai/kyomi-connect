//! SSH tunnel support for datasource providers.
//!
//! Ports the Python `SSHTunnelMixin` as a shared module using the `russh` crate.
//! Providers that support SSH tunneling (PostgreSQL, MySQL, Redshift, ClickHouse,
//! SQL Server) use [`SshTunnel`] to set up local TCP forwarding through an SSH
//! bastion host.
//!
//! ## Usage
//!
//! ```text
//! let tunnel = SshTunnel::connect(
//!     "bastion.example.com", 22,
//!     "ubuntu",
//!     pem_key_str,
//!     "db.internal", 5432,
//! ).await?;
//!
//! let (host, port) = tunnel.local_addr();
//! // Connect to the database via host:port
//!
//! tunnel.close().await;
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use russh::keys::PrivateKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use kyomi_connect_protocol::Error;

/// Default SSH port.
const DEFAULT_SSH_PORT: u16 = 22;

/// An SSH tunnel that forwards local TCP connections to a remote target
/// through an SSH bastion host.
///
/// The tunnel binds a local TCP listener on a random port. When a client
/// connects, data is forwarded bidirectionally through an SSH
/// `direct-tcpip` channel to the target host/port.
pub struct SshTunnel {
    /// Local address the tunnel is listening on.
    local_addr: SocketAddr,
    /// Signal to shut down the listener task.
    shutdown: Arc<Notify>,
    /// Handle to the background listener task.
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl SshTunnel {
    /// Establish an SSH tunnel to the target through the bastion host.
    ///
    /// 1. Parses the PEM private key.
    /// 2. Connects to the SSH server and authenticates.
    /// 3. Binds a local TCP listener on a random port.
    /// 4. Spawns a background task that accepts connections and forwards them
    ///    through SSH `direct-tcpip` channels.
    ///
    /// # Arguments
    ///
    /// * `ssh_host` - Bastion/jump host hostname or IP.
    /// * `ssh_port` - SSH port (typically 22).
    /// * `ssh_username` - SSH username for authentication.
    /// * `ssh_private_key_pem` - PEM-encoded private key (Ed25519 or RSA).
    /// * `target_host` - Database host from the bastion's perspective.
    /// * `target_port` - Database port.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be parsed, SSH connection fails,
    /// authentication fails, or the local listener cannot bind.
    pub async fn connect(
        ssh_host: &str,
        ssh_port: u16,
        ssh_username: &str,
        ssh_private_key_pem: &str,
        target_host: &str,
        target_port: u16,
    ) -> kyomi_connect_protocol::Result<Self> {
        let ssh_port = if ssh_port == 0 {
            DEFAULT_SSH_PORT
        } else {
            ssh_port
        };

        tracing::info!(
            ssh_host = ssh_host,
            ssh_port = ssh_port,
            target_host = target_host,
            target_port = target_port,
            "Creating SSH tunnel"
        );

        // Parse the PEM private key
        let private_key = Self::parse_private_key(ssh_private_key_pem)?;

        // Connect to the SSH server
        let config = Arc::new(russh::client::Config::default());
        let handler = TunnelClientHandler;

        let addr = format!("{ssh_host}:{ssh_port}");
        let mut handle = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            russh::client::connect(config, &addr, handler),
        )
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "SSH connection to {addr} timed out after {}s",
                crate::DATASOURCE_TIMEOUT_CONNECT.as_secs()
            ))
        })?
        .map_err(|e| Error::Internal(format!("SSH connection to {addr} failed: {e}")))?;

        // Authenticate with the private key
        let username = ssh_username.to_string();
        let key_with_alg = russh::keys::PrivateKeyWithHashAlg::new(Arc::new(private_key), None);
        let auth_result = handle
            .authenticate_publickey(&username, key_with_alg)
            .await
            .map_err(|e| Error::Internal(format!("SSH authentication failed: {e}")))?;

        if !auth_result.success() {
            return Err(Error::Internal(
                "SSH authentication failed: server rejected the key".into(),
            ));
        }

        tracing::info!("SSH authentication successful");

        // Bind local TCP listener on random port
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
            Error::Internal(format!("Failed to bind local SSH tunnel listener: {e}"))
        })?;

        let local_addr = listener
            .local_addr()
            .map_err(|e| Error::Internal(format!("Failed to get local address: {e}")))?;

        tracing::info!(
            local_port = local_addr.port(),
            "SSH tunnel listening on 127.0.0.1:{}",
            local_addr.port()
        );

        // Spawn the forwarding task
        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();
        let target_host_owned = target_host.to_string();
        let handle = Arc::new(handle);

        let task_handle = tokio::spawn(async move {
            Self::run_listener(
                listener,
                handle,
                target_host_owned,
                target_port,
                shutdown_clone,
            )
            .await;
        });

        Ok(Self {
            local_addr,
            shutdown,
            task_handle: Some(task_handle),
        })
    }

    /// Returns the local address `("127.0.0.1", port)` that clients should
    /// connect to in order to reach the tunneled target.
    pub fn local_addr(&self) -> (&str, u16) {
        ("127.0.0.1", self.local_addr.port())
    }

    /// Gracefully shut down the SSH tunnel.
    ///
    /// Signals the background listener task to stop and waits for it to finish.
    pub async fn close(&mut self) {
        tracing::info!("Closing SSH tunnel on 127.0.0.1:{}", self.local_addr.port());
        self.shutdown.notify_waiters();

        if let Some(handle) = self.task_handle.take() {
            // Give the task a moment to shut down
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        // If the tunnel task was not consumed by close(), abort it to prevent leaks.
        if let Some(handle) = self.task_handle.take() {
            tracing::warn!("SshTunnel dropped without calling close(), aborting background task");
            handle.abort();
        }
    }
}

impl SshTunnel {
    /// Parse a PEM-encoded SSH private key (Ed25519 or RSA).
    fn parse_private_key(pem: &str) -> kyomi_connect_protocol::Result<PrivateKey> {
        russh::keys::decode_secret_key(pem, None)
            .map_err(|e| Error::Internal(format!("Failed to parse SSH private key: {e}")))
    }

    /// Background task that accepts TCP connections on the local listener
    /// and forwards them through SSH direct-tcpip channels.
    async fn run_listener(
        listener: TcpListener,
        ssh_handle: Arc<russh::client::Handle<TunnelClientHandler>>,
        target_host: String,
        target_port: u16,
        shutdown: Arc<Notify>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    tracing::debug!("SSH tunnel listener shutting down");
                    break;
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((tcp_stream, peer_addr)) => {
                            tracing::debug!(
                                peer = %peer_addr,
                                "Accepted connection on SSH tunnel"
                            );
                            let ssh = ssh_handle.clone();
                            let host = target_host.clone();
                            tokio::spawn(async move {
                                if let Err(e) = Self::forward_connection(
                                    tcp_stream, ssh, &host, target_port, peer_addr,
                                ).await {
                                    tracing::warn!(
                                        error = %e,
                                        "SSH tunnel forwarding error"
                                    );
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "SSH tunnel accept error");
                        }
                    }
                }
            }
        }
    }

    /// Forward a single TCP connection through an SSH direct-tcpip channel.
    async fn forward_connection(
        mut tcp_stream: tokio::net::TcpStream,
        ssh_handle: Arc<russh::client::Handle<TunnelClientHandler>>,
        target_host: &str,
        target_port: u16,
        peer_addr: SocketAddr,
    ) -> kyomi_connect_protocol::Result<()> {
        // Open a direct-tcpip channel
        let channel = ssh_handle
            .channel_open_direct_tcpip(
                target_host,
                target_port as u32,
                &peer_addr.ip().to_string(),
                peer_addr.port() as u32,
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to open SSH channel: {e}")))?;

        let mut channel_stream = channel.into_stream();

        // Bidirectional copy between TCP and SSH channel
        let (mut tcp_read, mut tcp_write) = tcp_stream.split();
        let (mut ch_read, mut ch_write) = tokio::io::split(&mut channel_stream);

        let tcp_to_ssh = async {
            let mut buf = vec![0u8; 8192];
            loop {
                let n = tcp_read.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                ch_write.write_all(&buf[..n]).await?;
                ch_write.flush().await?;
            }
            Ok::<(), std::io::Error>(())
        };

        let ssh_to_tcp = async {
            let mut buf = vec![0u8; 8192];
            loop {
                let n = ch_read.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                tcp_write.write_all(&buf[..n]).await?;
                tcp_write.flush().await?;
            }
            Ok::<(), std::io::Error>(())
        };

        // Run both directions concurrently; when one ends, we're done
        tokio::select! {
            result = tcp_to_ssh => {
                if let Err(e) = result {
                    tracing::debug!(error = %e, "TCP->SSH copy ended");
                }
            }
            result = ssh_to_tcp => {
                if let Err(e) = result {
                    tracing::debug!(error = %e, "SSH->TCP copy ended");
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Minimal SSH client handler for tunneling
// ---------------------------------------------------------------------------

/// Minimal SSH client handler for tunnel connections.
///
/// We don't need to handle any server-initiated messages for port forwarding;
/// this handler just satisfies the `client::Handler` trait requirement.
struct TunnelClientHandler;

impl russh::client::Handler for TunnelClientHandler {
    type Error = russh::Error;

    /// Accept any host key. In a production SSH client you would verify
    /// the host key against known_hosts, but for database tunneling the
    /// SSH configuration is admin-controlled.
    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        tracing::warn!(
            key_type = %format!("{:?}", server_public_key.algorithm()),
            "Accepting SSH host key without verification (admin-controlled tunnel config)"
        );
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Helper to extract SSH config from connection_config JSON
// ---------------------------------------------------------------------------

/// Configuration for SSH tunnel extracted from a datasource's `connection_config`.
#[derive(Debug)]
pub struct SshTunnelConfig {
    /// Whether SSH tunneling is enabled.
    pub enabled: bool,
    /// SSH bastion host.
    pub host: String,
    /// SSH port (default 22).
    pub port: u16,
    /// SSH username.
    pub username: String,
    /// PEM-encoded SSH private key.
    pub private_key: String,
}

impl SshTunnelConfig {
    /// Extract SSH tunnel configuration from a datasource's `connection_config` JSON.
    ///
    /// Returns `None` if SSH is not enabled (i.e., `ssh_enabled` is false or absent).
    /// Returns `Some(Err(...))` if SSH is enabled but configuration is incomplete.
    pub fn from_connection_config(
        config: &serde_json::Value,
    ) -> Option<kyomi_connect_protocol::Result<Self>> {
        let enabled = config
            .get("ssh_enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !enabled {
            return None;
        }

        let host = match config.get("ssh_host").and_then(|v| v.as_str()) {
            Some(h) if !h.is_empty() => h.to_string(),
            _ => return Some(Err(Error::Provider("SSH tunnel requires ssh_host".into()))),
        };

        let port = config
            .get("ssh_port")
            .and_then(|v| v.as_u64())
            .map(|p| p as u16)
            .unwrap_or(DEFAULT_SSH_PORT);

        let username = match config.get("ssh_username").and_then(|v| v.as_str()) {
            Some(u) if !u.is_empty() => u.to_string(),
            _ => {
                return Some(Err(Error::Provider(
                    "SSH tunnel requires ssh_username".into(),
                )));
            }
        };

        let private_key = match config.get("ssh_private_key").and_then(|v| v.as_str()) {
            Some(k) if !k.is_empty() => k.to_string(),
            _ => {
                return Some(Err(Error::Provider(
                    "SSH tunnel requires ssh_private_key".into(),
                )));
            }
        };

        Some(Ok(Self {
            enabled,
            host,
            port,
            username,
            private_key,
        }))
    }
}
