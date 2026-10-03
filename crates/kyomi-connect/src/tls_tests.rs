use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

const CA: &[u8] = include_bytes!("../tests/fixtures/tls/ca.der");
const CERT: &[u8] = include_bytes!("../tests/fixtures/tls/server.der");
const KEY: &[u8] = include_bytes!("../tests/fixtures/tls/server-key.der");

#[test]
#[ignore = "requires KYOMI_TLS_SMOKE_URL pointing to a public-CA TLS WebSocket endpoint"]
fn production_tls_websocket_smoke() {
    let url = std::env::var("KYOMI_TLS_SMOKE_URL").expect("set KYOMI_TLS_SMOKE_URL");
    assert!(url.starts_with("wss://"), "smoke endpoint must use TLS");
    super::install_crypto_provider();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tokio::time::timeout(
            Duration::from_secs(20),
            crate::ws_client::WsClient::new(url, "synthetic-smoke-token".into()).connect_once(),
        )
        .await
        .expect("production TLS WebSocket smoke timed out")
        .expect("production connect_async TLS WebSocket handshake failed");
    });
}

#[test]
fn startup_provider_and_verified_tls_websocket() {
    // Rustls's process default is write-once. A child isolates this regression
    // from other tests that might create a TLS client first.
    const CHILD: &str = "KYOMI_TLS_REGRESSION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tls::tests::startup_provider_and_verified_tls_websocket",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "TLS regression child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    super::install_crypto_provider();
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(15), async {
            verified_handshake("localhost", true, true).await;
            verified_handshake("127.0.0.1", true, false).await;
            verified_handshake("localhost", false, false).await;
        })
        .await
        .expect("local TLS regression timed out");
    });
}

async fn verified_handshake(host: &str, trust_ca: bool, succeeds: bool) {
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(CERT.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY.to_vec())),
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        match tokio_rustls::TlsAcceptor::from(Arc::new(server_config))
            .accept(tcp)
            .await
        {
            Ok(tls) => tokio_tungstenite::accept_async(tls).await.is_ok(),
            Err(_) => false,
        }
    });

    let mut roots = rustls::RootCertStore::empty();
    if trust_ca {
        roots.add(CertificateDer::from(CA.to_vec())).unwrap();
    }
    // Only the trust anchors differ from connect_async's bundled public roots;
    // this builder still selects the process provider and verifies name/chain.
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let result = tokio_tungstenite::connect_async_tls_with_config(
        format!("wss://{host}:{port}/connect/v1"),
        None,
        false,
        Some(tokio_tungstenite::Connector::Rustls(Arc::new(
            client_config,
        ))),
    )
    .await;
    if succeeds {
        let (_, response) = result.expect("trusted localhost TLS WebSocket must connect");
        assert_eq!(response.status(), http::StatusCode::SWITCHING_PROTOCOLS);
    } else {
        let error = result.expect_err("invalid TLS certificate must be rejected");
        let tokio_tungstenite::tungstenite::Error::Io(ref io_error) = error else {
            panic!("expected TLS verification error, got {error}");
        };
        let tls_error = io_error
            .get_ref()
            .and_then(|source| source.downcast_ref::<rustls::Error>());
        match tls_error {
            Some(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) if trust_ca => {}
            Some(rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer))
                if !trust_ca => {}
            _ => panic!("unexpected TLS verification error: {error}"),
        }
    }
    assert_eq!(server.await.unwrap(), succeeds);
}
