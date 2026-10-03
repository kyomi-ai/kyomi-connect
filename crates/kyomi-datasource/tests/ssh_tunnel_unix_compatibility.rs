//! Published Unix-only APIs remain available to downstream crates.
#![cfg(all(unix, feature = "ssh"))]

use std::path::{Path, PathBuf};

use kyomi_datasource::ssh_tunnel::SshTunnel;

async fn bind_socket_signature(tunnel: &mut SshTunnel) -> kyomi_connect_protocol::Result<PathBuf> {
    tunnel.bind_unix_socket(5432).await
}

#[test]
fn unix_socket_methods_preserve_published_signatures() {
    let _bind = bind_socket_signature;
    let _: fn(&SshTunnel) -> Option<&Path> = SshTunnel::unix_socket_dir;
    let _: fn(&SshTunnel) -> Option<&Path> = SshTunnel::unix_socket_path;
}
