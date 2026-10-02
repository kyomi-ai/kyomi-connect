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
//! let cfg = SshTunnelConfig::from_connection_config(&connection_config)
//!     .expect("ssh enabled")?;
//! let tunnel = SshTunnel::connect(&cfg, "db.internal", 5432).await?;
//!
//! let (host, port) = tunnel.local_addr();
//! // Connect to the database via host:port
//!
//! tunnel.close().await;
//! ```
//!
//! ## Security
//!
//! * **Encrypted private keys**: set `ssh_passphrase` in `connection_config` if
//!   `ssh_private_key` is passphrase-protected.
//! * **Host key verification**: by default, any SSH host key is accepted (the
//!   bastion is admin-controlled, so this mirrors the historical behavior).
//!   Set `ssh_host_fingerprint` to the bastion's `SHA256:...` fingerprint
//!   (as printed by `ssh-keygen -lf`) to pin it — the tunnel will refuse to
//!   connect if the presented host key doesn't match.

use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::Arc;

use russh::keys::PrivateKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::Notify;

use kyomi_connect_protocol::Error;

// macOS sockaddr_un.sun_path is 104 bytes, including the terminating NUL.
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 103;

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
    unix_task_handle: Option<tokio::task::JoinHandle<()>>,
    unix_socket_dir: Option<PathBuf>,
    unix_socket_path: Option<PathBuf>,
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
    /// * `cfg` - SSH tunnel configuration (bastion host, credentials,
    ///   optional passphrase and pinned host-key fingerprint).
    /// * `target_host` - Database host from the bastion's perspective.
    /// * `target_port` - Database port.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be parsed (including a
    /// passphrase-protected key with no or an incorrect passphrase), the SSH
    /// connection fails, the host key doesn't match a pinned fingerprint,
    /// authentication fails, or the local listener cannot bind.
    pub async fn connect(
        cfg: &SshTunnelConfig,
        target_host: &str,
        target_port: u16,
    ) -> kyomi_connect_protocol::Result<Self> {
        let ssh_port = if cfg.port == 0 {
            DEFAULT_SSH_PORT
        } else {
            cfg.port
        };

        tracing::info!(
            ssh_host = cfg.host.as_str(),
            ssh_port = ssh_port,
            target_host = target_host,
            target_port = target_port,
            "Creating SSH tunnel"
        );

        // Parse the PEM private key
        let private_key = Self::parse_private_key(&cfg.private_key, cfg.passphrase.as_deref())?;

        // Connect to the SSH server
        let config = Arc::new(russh::client::Config::default());
        let handler = TunnelClientHandler {
            expected_fingerprint: cfg.host_fingerprint.clone(),
        };

        let addr = format!("{}:{ssh_port}", cfg.host);
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
        let username = cfg.username.clone();
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
            unix_task_handle: None,
            unix_socket_dir: None,
            unix_socket_path: None,
        })
    }

    /// Provide a local Unix socket for drivers that use one host field for both
    /// TCP routing and TLS identity. The driver can keep the database hostname
    /// while its transport goes through this socket and the SSH tunnel.
    /// PostgreSQL expects `.s.PGSQL.<port>` inside a directory; MySQL takes
    /// the socket path itself.
    pub async fn bind_unix_socket(
        &mut self,
        database_port: u16,
    ) -> kyomi_connect_protocol::Result<PathBuf> {
        let dir = create_socket_directory(&std::env::temp_dir(), database_port)?;
        let path = dir.join(format!(".s.PGSQL.{database_port}"));
        let listener = match UnixListener::bind(&path) {
            Ok(listener) => listener,
            Err(e) => {
                let _ = std::fs::remove_dir(&dir);
                return Err(Error::Internal(format!(
                    "Failed to bind SSH tunnel socket: {e}"
                )));
            }
        };
        let local_addr = self.local_addr;
        let shutdown = self.shutdown.clone();
        self.unix_task_handle = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    accepted = listener.accept() => match accepted {
                        Ok((mut stream, _)) => {
                            tokio::spawn(async move {
                                match tokio::net::TcpStream::connect(local_addr).await {
                                    Ok(mut tcp) => {
                                        let _ = tokio::io::copy_bidirectional(&mut stream, &mut tcp).await;
                                    }
                                    Err(e) => tracing::warn!(error = %e, "SSH tunnel socket relay failed"),
                                }
                            });
                        }
                        Err(e) => tracing::warn!(error = %e, "SSH tunnel socket accept failed"),
                    }
                }
            }
        }));
        self.unix_socket_dir = Some(dir);
        self.unix_socket_path = Some(path.clone());
        Ok(path)
    }

    /// Directory containing the PostgreSQL-compatible socket name.
    pub fn unix_socket_dir(&self) -> Option<&std::path::Path> {
        self.unix_socket_dir.as_deref()
    }

    /// Full socket path accepted by MySQL.
    pub fn unix_socket_path(&self) -> Option<&std::path::Path> {
        self.unix_socket_path.as_deref()
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
        self.cleanup_unix_socket();
    }

    fn cleanup_unix_socket(&mut self) {
        if let Some(handle) = self.unix_task_handle.take() {
            handle.abort();
        }
        if let Some(dir) = self.unix_socket_dir.take() {
            if let Ok(entries) = std::fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
            let _ = std::fs::remove_dir(dir);
        }
        self.unix_socket_path = None;
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        // If the tunnel task was not consumed by close(), abort it to prevent leaks.
        if let Some(handle) = self.task_handle.take() {
            tracing::warn!("SshTunnel dropped without calling close(), aborting background task");
            handle.abort();
        }
        self.cleanup_unix_socket();
    }
}

impl SshTunnel {
    /// Parse a PEM-encoded SSH private key (Ed25519 or RSA).
    ///
    /// `passphrase` decrypts the key if it is passphrase-protected. Pass
    /// `None` for unencrypted keys.
    fn parse_private_key(
        pem: &str,
        passphrase: Option<&str>,
    ) -> kyomi_connect_protocol::Result<PrivateKey> {
        russh::keys::decode_secret_key(pem, passphrase).map_err(|e| {
            if matches!(e, russh::keys::Error::KeyIsEncrypted) {
                Error::Provider(
                    "SSH private key is encrypted; set ssh_passphrase to decrypt it".into(),
                )
            } else {
                Error::Internal(format!("Failed to parse SSH private key: {e}"))
            }
        })
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
/// We don't need to handle any server-initiated messages for port forwarding
/// beyond host-key verification.
struct TunnelClientHandler {
    /// Pinned SHA256 host-key fingerprint (`SHA256:...`), if configured via
    /// `ssh_host_fingerprint`. When `None`, any host key is accepted (the
    /// historical, backward-compatible behavior — the bastion is
    /// admin-controlled). When `Some`, the server's host key must match or
    /// the connection is rejected.
    expected_fingerprint: Option<String>,
}

impl russh::client::Handler for TunnelClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        let Some(expected) = &self.expected_fingerprint else {
            tracing::warn!(
                key_type = %format!("{:?}", server_public_key.algorithm()),
                "Accepting SSH host key without verification (no ssh_host_fingerprint configured)"
            );
            return Ok(true);
        };

        let actual = server_public_key
            .fingerprint(russh::keys::HashAlg::Sha256)
            .to_string();

        if fingerprint_matches(expected, &actual) {
            tracing::info!(fingerprint = %actual, "SSH host key fingerprint verified");
            Ok(true)
        } else {
            tracing::error!(
                expected = %expected,
                actual = %actual,
                "SSH host key fingerprint mismatch — rejecting connection"
            );
            Ok(false)
        }
    }
}

/// Compare a pinned SSH host-key fingerprint (from `ssh_host_fingerprint`)
/// against the fingerprint computed from the server's actual host key.
///
/// Extracted as a pure function so the comparison logic is unit-testable
/// without a live SSH server.
///
/// Trims surrounding whitespace on the pinned value — users typically paste
/// the fingerprint from `ssh-keygen -lf` / `ssh-keyscan` output into a config
/// field, which often carries a trailing newline. Case is significant (the
/// digest is base64), so it is preserved.
fn fingerprint_matches(expected: &str, actual: &str) -> bool {
    expected.trim() == actual.trim()
}

/// Create a private socket directory under a path short enough for macOS
/// `sockaddr_un`. A long TMPDIR must not make every tunneled connection fail.
fn create_socket_directory(
    preferred_dir: &std::path::Path,
    database_port: u16,
) -> kyomi_connect_protocol::Result<PathBuf> {
    let name = format!("kyomi-ssh-{}", uuid::Uuid::new_v4().simple());
    let socket_name = format!(".s.PGSQL.{database_port}");
    let mut last_error = None;
    for base in [
        preferred_dir,
        std::path::Path::new("/tmp"),
        std::path::Path::new("/var/tmp"),
    ] {
        let dir = base.join(&name);
        let socket_path = dir.join(&socket_name);
        if socket_path.as_os_str().as_bytes().len() > MAX_UNIX_SOCKET_PATH_BYTES {
            continue;
        }
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(&dir) {
            Ok(()) => return Ok(dir),
            Err(e) => last_error = Some(e),
        }
    }
    Err(Error::Internal(format!(
        "Failed to create short SSH tunnel socket directory: {}",
        last_error.map_or_else(|| "no usable short path".to_string(), |e| e.to_string())
    )))
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
    /// Passphrase to decrypt `private_key`, if it is passphrase-protected.
    /// `None` if the key is unencrypted or no passphrase was configured.
    pub passphrase: Option<String>,
    /// Pinned SSH host-key fingerprint, in standard OpenSSH `SHA256:...`
    /// format (as printed by `ssh-keygen -lf`). When set, the tunnel refuses
    /// to connect if the bastion's host key doesn't match. `None` means any
    /// host key is accepted (backward-compatible default).
    pub host_fingerprint: Option<String>,
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

        let passphrase = config
            .get("ssh_passphrase")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let host_fingerprint = config
            .get("ssh_host_fingerprint")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        Some(Ok(Self {
            enabled,
            host,
            port,
            username,
            private_key,
            passphrase,
            host_fingerprint,
        }))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> serde_json::Value {
        serde_json::json!({
            "ssh_enabled": true,
            "ssh_host": "bastion.example.com",
            "ssh_username": "ubuntu",
            "ssh_private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----",
        })
    }

    // -- ssh_passphrase parsing -----------------------------------------

    #[test]
    fn passphrase_absent_is_none() {
        let config = base_config();
        let cfg = SshTunnelConfig::from_connection_config(&config)
            .expect("ssh enabled")
            .expect("valid config");
        assert_eq!(cfg.passphrase, None);
    }

    #[test]
    fn passphrase_empty_string_is_none() {
        let mut config = base_config();
        config["ssh_passphrase"] = serde_json::json!("");
        let cfg = SshTunnelConfig::from_connection_config(&config)
            .expect("ssh enabled")
            .expect("valid config");
        assert_eq!(cfg.passphrase, None);
    }

    #[test]
    fn passphrase_present_is_parsed() {
        let mut config = base_config();
        config["ssh_passphrase"] = serde_json::json!("hunter42");
        let cfg = SshTunnelConfig::from_connection_config(&config)
            .expect("ssh enabled")
            .expect("valid config");
        assert_eq!(cfg.passphrase.as_deref(), Some("hunter42"));
    }

    // -- ssh_host_fingerprint parsing ------------------------------------

    #[test]
    fn host_fingerprint_absent_is_none() {
        let config = base_config();
        let cfg = SshTunnelConfig::from_connection_config(&config)
            .expect("ssh enabled")
            .expect("valid config");
        assert_eq!(cfg.host_fingerprint, None);
    }

    #[test]
    fn host_fingerprint_empty_string_is_none() {
        let mut config = base_config();
        config["ssh_host_fingerprint"] = serde_json::json!("");
        let cfg = SshTunnelConfig::from_connection_config(&config)
            .expect("ssh enabled")
            .expect("valid config");
        assert_eq!(cfg.host_fingerprint, None);
    }

    #[test]
    fn host_fingerprint_present_is_parsed() {
        let mut config = base_config();
        config["ssh_host_fingerprint"] =
            serde_json::json!("SHA256:ldyiXa1JQakitNU5tErauu8DvWQ1dZ7aXu+rm7KQuog");
        let cfg = SshTunnelConfig::from_connection_config(&config)
            .expect("ssh enabled")
            .expect("valid config");
        assert_eq!(
            cfg.host_fingerprint.as_deref(),
            Some("SHA256:ldyiXa1JQakitNU5tErauu8DvWQ1dZ7aXu+rm7KQuog")
        );
    }

    // -- fingerprint_matches ----------------------------------------------

    #[test]
    fn fingerprint_matches_identical_strings() {
        let fp = "SHA256:ldyiXa1JQakitNU5tErauu8DvWQ1dZ7aXu+rm7KQuog";
        assert!(fingerprint_matches(fp, fp));
    }

    #[test]
    fn fingerprint_matches_ignores_surrounding_whitespace() {
        let fp = "SHA256:ldyiXa1JQakitNU5tErauu8DvWQ1dZ7aXu+rm7KQuog";
        assert!(fingerprint_matches(&format!("  {fp}\n"), fp));
    }

    #[test]
    fn fingerprint_matches_rejects_different_strings() {
        assert!(!fingerprint_matches(
            "SHA256:ldyiXa1JQakitNU5tErauu8DvWQ1dZ7aXu+rm7KQuog",
            "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ));
    }

    #[test]
    fn fingerprint_matches_rejects_empty_actual() {
        assert!(!fingerprint_matches(
            "SHA256:ldyiXa1JQakitNU5tErauu8DvWQ1dZ7aXu+rm7KQuog",
            "",
        ));
    }

    // -- parse_private_key passphrase round-trip --------------------------
    //
    // Fixture is the AES256-CTR-encrypted Ed25519 test key from the
    // `ssh-key` crate's own test suite (RustCrypto/SSH, Apache-2.0/MIT),
    // encrypted with the password "hunter42". Using a known-good published
    // fixture exercises the real decrypt path through
    // `russh::keys::decode_secret_key` without adding a new dev-dependency
    // (e.g. `ssh-key`) just to generate one at test time.
    const ENCRYPTED_ED25519_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----\n\
b3BlbnNzaC1rZXktdjEAAAAACmFlczI1Ni1jdHIAAAAGYmNyeXB0AAAAGAAAABBKH96ujW\n\
umB6/WnTNPjTeaAAAAEAAAAAEAAAAzAAAAC3NzaC1lZDI1NTE5AAAAILM+rvN+ot98qgEN\n\
796jTiQfZfG1KaT0PtFDJ/XFSqtiAAAAoFzvbvyFMhAiwBOXF0mhUUacPUCMZXivG2up2c\n\
hEnAw1b6BLRPyWbY5cC2n9ggD4ivJ1zSts6sBgjyiXQAReyrP35myYvT/OIB/NpwZM/xIJ\n\
N7MHSUzlkX4adBrga3f7GS4uv4ChOoxC4XsE5HsxtGsq1X8jzqLlZTmOcxkcEneYQexrUc\n\
bQP0o+gL5aKK8cQgiIlXeDbRjqhc4+h4EF6lY=\n\
-----END OPENSSH PRIVATE KEY-----\n";

    #[test]
    fn parse_private_key_with_correct_passphrase_succeeds() {
        let result = SshTunnel::parse_private_key(ENCRYPTED_ED25519_KEY, Some("hunter42"));
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    #[test]
    fn parse_private_key_encrypted_without_passphrase_gives_clear_error() {
        let result = SshTunnel::parse_private_key(ENCRYPTED_ED25519_KEY, None);
        let err = result.expect_err("encrypted key with no passphrase must fail");
        let message = err.to_string().to_lowercase();
        assert!(
            message.contains("passphrase") || message.contains("encrypted"),
            "error message should mention the missing passphrase, got: {err}"
        );
    }

    #[test]
    fn parse_private_key_with_wrong_passphrase_fails() {
        let result = SshTunnel::parse_private_key(ENCRYPTED_ED25519_KEY, Some("wrong-password"));
        assert!(result.is_err());
    }

    #[test]
    fn parse_private_key_unencrypted_garbage_still_errors_cleanly() {
        let result = SshTunnel::parse_private_key("not a key", None);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn unix_socket_forwards_to_tunnel_and_is_removed_on_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let echo = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4];
            stream.read_exact(&mut bytes).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
        });
        let mut tunnel = SshTunnel {
            local_addr: addr,
            shutdown: Arc::new(Notify::new()),
            task_handle: None,
            unix_task_handle: None,
            unix_socket_dir: None,
            unix_socket_path: None,
        };
        let path = tunnel.bind_unix_socket(5432).await.unwrap();
        assert!(path.ends_with(".s.PGSQL.5432"));
        assert_eq!(tunnel.unix_socket_path(), Some(path.as_path()));
        let mut client = tokio::net::UnixStream::connect(&path).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut response = [0; 4];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"ping");
        echo.await.unwrap();
        drop(client);
        drop(tunnel);
        assert!(!path.exists());
        assert!(!path.parent().unwrap().exists());
    }

    #[test]
    fn long_temp_directory_falls_back_to_short_socket_path() {
        let long_tmpdir = std::path::PathBuf::from(format!("/tmp/{}", "x".repeat(150)));
        let dir = create_socket_directory(&long_tmpdir, 65535).unwrap();
        let socket_path = dir.join(".s.PGSQL.65535");
        assert!(socket_path.as_os_str().as_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES);
        assert!(dir.starts_with("/tmp") || dir.starts_with("/var/tmp"));
        assert!(!dir.starts_with(long_tmpdir));
        std::fs::remove_dir(dir).unwrap();
    }

    #[cfg(feature = "clickhouse")]
    #[tokio::test]
    async fn reqwest_ip_literal_routes_through_unix_tunnel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 1024];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&buf[..n]);
                if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        let mut tunnel = SshTunnel {
            local_addr: addr,
            shutdown: Arc::new(Notify::new()),
            task_handle: None,
            unix_task_handle: None,
            unix_socket_dir: None,
            unix_socket_path: None,
        };
        let socket_path = tunnel.bind_unix_socket(8123).await.unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(socket_path.as_path())
            .build()
            .unwrap();
        let body = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.get("http://192.0.2.10:8123/path").send(),
        )
        .await
        .expect("IP-literal request bypassed tunnel")
        .unwrap()
        .text()
        .await
        .unwrap();
        assert_eq!(body, "ok");
        let request = server.await.unwrap();
        assert!(request.contains("host: 192.0.2.10:8123"), "{request}");
    }

    #[cfg(feature = "clickhouse")]
    #[tokio::test]
    async fn reqwest_ip_literal_rejects_wrong_tls_identity_over_tunnel() {
        use tokio_rustls::TlsAcceptor;
        use tokio_rustls::rustls::ServerConfig;
        use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let cert =
            CertificateDer::from(include_bytes!("../tests/fixtures/tls/db.cert.der").to_vec());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            include_bytes!("../tests/fixtures/tls/db.key.der").to_vec(),
        ));
        let tls = TlsAcceptor::from(Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .unwrap(),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            // The client must reach this server through the relay, then reject
            // its db.example.test certificate for the 192.0.2.10 URL.
            tls.accept(tcp).await.is_err()
        });
        let mut tunnel = SshTunnel {
            local_addr: addr,
            shutdown: Arc::new(Notify::new()),
            task_handle: None,
            unix_task_handle: None,
            unix_socket_dir: None,
            unix_socket_path: None,
        };
        let socket_path = tunnel.bind_unix_socket(8443).await.unwrap();
        let ca =
            reqwest::Certificate::from_pem(include_bytes!("../tests/fixtures/tls/db.cert.pem"))
                .unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(socket_path.as_path())
            .add_root_certificate(ca)
            .build()
            .unwrap();
        let (result, server_result) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(client.get("https://192.0.2.10:8443/").send(), server)
            })
            .await
            .expect("TLS request did not reach tunneled server");
        assert!(server_result.unwrap(), "TLS certificate was accepted");
        let error = result.expect_err("IP certificate name must be checked");
        assert!(
            format!("{error:?}").contains("InvalidCertificate"),
            "{error:?}"
        );
    }

    #[cfg(feature = "postgres")]
    async fn postgres_tls_handshake(host: &str, trusted: bool) -> (bool, String) {
        use sqlx::Connection;
        use sqlx::postgres::{PgConnectOptions, PgConnection, PgSslMode};
        use tokio_rustls::TlsAcceptor;
        use tokio_rustls::rustls::ServerConfig;
        use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tls/");
        let cert =
            CertificateDer::from(include_bytes!("../tests/fixtures/tls/db.cert.der").to_vec());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            include_bytes!("../tests/fixtures/tls/db.key.der").to_vec(),
        ));
        let tls = TlsAcceptor::from(Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .unwrap(),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut ssl_request = [0; 8];
            tcp.read_exact(&mut ssl_request).await.unwrap();
            assert_eq!(ssl_request, [0, 0, 0, 8, 4, 210, 22, 47]);
            tcp.write_all(b"S").await.unwrap();
            tls.accept(tcp).await.is_ok()
        });
        let mut tunnel = SshTunnel {
            local_addr: addr,
            shutdown: Arc::new(Notify::new()),
            task_handle: None,
            unix_task_handle: None,
            unix_socket_dir: None,
            unix_socket_path: None,
        };
        tunnel.bind_unix_socket(5432).await.unwrap();
        let mut options = PgConnectOptions::new()
            .host(host)
            .port(5432)
            .socket(tunnel.unix_socket_dir().unwrap())
            .username("test")
            .database("test")
            .ssl_mode(PgSslMode::VerifyFull);
        if trusted {
            options = options.ssl_root_cert(format!("{fixture}db.cert.pem"));
        }
        let client = PgConnection::connect_with(&options);
        let (client_result, server_result) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(client, server)
            })
            .await
            .expect("TLS handshake timed out");
        let error = client_result.expect_err("mock server does not complete login");
        (server_result.unwrap(), error.to_string())
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_tls_over_tunnel_validates_database_certificate_and_hostname() {
        let (accepted, error) = postgres_tls_handshake("db.example.test", true).await;
        assert!(accepted, "valid database certificate rejected: {error}");
        let (accepted, error) = postgres_tls_handshake("other.example.test", true).await;
        assert!(!accepted);
        assert!(error.contains("not valid for name"), "{error}");
        let (accepted, error) = postgres_tls_handshake("db.example.test", false).await;
        assert!(!accepted);
        assert!(error.contains("UnknownIssuer"), "{error}");
    }
}
