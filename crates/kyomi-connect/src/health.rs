use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::{Json, Router, extract::State, response::IntoResponse, routing::get};

#[derive(Clone)]
struct HealthState {
    ws_connected: Arc<AtomicBool>,
    db_healthy: Arc<AtomicBool>,
}

/// Start the health check HTTP server.
pub async fn start_health_server(
    port: u16,
    ws_connected: Arc<AtomicBool>,
    db_healthy: Arc<AtomicBool>,
) {
    let app = health_router(ws_connected, db_healthy);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(port, "Health check server starting");

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(port, error = %e, "Failed to bind health check server");
            return;
        }
    };

    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!(error = %e, "Health check server error");
    }
}

/// Refresh readiness from a bounded provider check. Log only state transitions
/// and fixed reason codes: provider errors may contain credentials.
pub async fn refresh_database_readiness(
    db_healthy: &AtomicBool,
    timeout: std::time::Duration,
    check: impl std::future::Future<Output = anyhow::Result<bool>>,
) {
    let (reachable, reason) = match tokio::time::timeout(timeout, check).await {
        Ok(Ok(true)) => (true, "connected"),
        Ok(Ok(false)) => (false, "unreachable"),
        Ok(Err(_)) => (false, "connection_test_failed"),
        Err(_) => (false, "connection_test_timeout"),
    };
    if db_healthy.swap(reachable, Ordering::Relaxed) != reachable {
        tracing::info!(reachable, reason, "Database readiness changed");
    }
}

fn health_router(ws_connected: Arc<AtomicBool>, db_healthy: Arc<AtomicBool>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/livez", get(|| async { axum::http::StatusCode::OK }))
        .with_state(HealthState {
            ws_connected,
            db_healthy,
        })
}

async fn healthz(State(state): State<HealthState>) -> impl IntoResponse {
    let ws = state.ws_connected.load(Ordering::Relaxed);
    let db = state.db_healthy.load(Ordering::Relaxed);

    let status = if ws && db {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(serde_json::json!({
            "status": if ws && db { "healthy" } else { "unhealthy" },
            "ws_connected": ws,
            "db_reachable": db,
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn database_readiness_detects_failure_timeout_and_recovery() {
        let flag = AtomicBool::new(true);
        let deadline = Duration::from_millis(20);
        refresh_database_readiness(&flag, deadline, async {
            Err(anyhow::anyhow!("fixture connection failure"))
        })
        .await;
        assert!(!flag.load(Ordering::Relaxed));
        refresh_database_readiness(&flag, deadline, async { Ok(true) }).await;
        assert!(flag.load(Ordering::Relaxed));
        refresh_database_readiness(&flag, deadline, async { Ok(false) }).await;
        assert!(!flag.load(Ordering::Relaxed));
        refresh_database_readiness(&flag, deadline, async { Ok(true) }).await;
        assert!(flag.load(Ordering::Relaxed));
        tokio::time::timeout(
            Duration::from_secs(1),
            refresh_database_readiness(&flag, deadline, std::future::pending()),
        )
        .await
        .expect("provider timeout must bound readiness check");
        assert!(!flag.load(Ordering::Relaxed));
        refresh_database_readiness(&flag, deadline, async { Ok(true) }).await;
        assert!(flag.load(Ordering::Relaxed));
    }

    // Real loopback HTTP and WebSocket servers: no token, database, or network
    // outside localhost. A blocked query must not hold readiness true after
    // the backend closes the connection.
    #[tokio::test]
    async fn readiness_tracks_disconnect_and_reconnect_without_liveness_failure() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let ws_connected = Arc::new(AtomicBool::new(false));
            let db_healthy = Arc::new(AtomicBool::new(true));
            let health_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let health_url = format!("http://{}", health_listener.local_addr().unwrap());
            let app = health_router(ws_connected.clone(), db_healthy.clone());
            let health_server = tokio::spawn(async move {
                axum::serve(health_listener, app).await.unwrap();
            });
            let backend = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws_url = format!("ws://{}/connect/v1", backend.local_addr().unwrap());
            let query_started = Arc::new(tokio::sync::Notify::new());
            let started = query_started.clone();
            let agent = tokio::spawn(async move {
                crate::ws_client::WsClient::new(ws_url, "fixture-token".into())
                    .run_forever(ws_connected, move |_| {
                        let started = started.clone();
                        async move {
                            started.notify_one();
                            std::future::pending().await
                        }
                    })
                    .await;
            });
            let client = reqwest::Client::new();
            async fn wait_status(client: &reqwest::Client, url: &str, expected: u16) {
                loop {
                    if client.get(url).send().await.unwrap().status().as_u16() == expected {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            wait_status(&client, &format!("{health_url}/healthz"), 503).await;
            let (tcp, _) = backend.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            wait_status(&client, &format!("{health_url}/healthz"), 200).await;
            socket
                .send(Message::Text(
                    r#"{"id":"blocked","op":"test_connection"}"#.into(),
                ))
                .await
                .unwrap();
            query_started.notified().await;
            socket.close(None).await.unwrap();
            wait_status(&client, &format!("{health_url}/healthz"), 503).await;
            assert_eq!(
                client
                    .get(format!("{health_url}/livez"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            let (tcp, _) = backend.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            wait_status(&client, &format!("{health_url}/healthz"), 200).await;
            refresh_database_readiness(&db_healthy, Duration::from_secs(1), async { Ok(false) })
                .await;
            wait_status(&client, &format!("{health_url}/healthz"), 503).await;
            assert_eq!(
                client
                    .get(format!("{health_url}/livez"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            refresh_database_readiness(&db_healthy, Duration::from_secs(1), async { Ok(true) })
                .await;
            wait_status(&client, &format!("{health_url}/healthz"), 200).await;
            // Verify heartbeat responses still work after reconnect.
            socket.send(Message::Ping(vec![1].into())).await.unwrap();
            assert!(matches!(
                socket.next().await.unwrap().unwrap(),
                Message::Pong(_)
            ));
            agent.abort();
            health_server.abort();
        })
        .await
        .expect("health/reconnect smoke timed out");
    }
}
