use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};

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
    let state = HealthState {
        ws_connected,
        db_healthy,
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .with_state(state);

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
