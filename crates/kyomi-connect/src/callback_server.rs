//! Local HTTP callback server for browser-based token delivery.
//!
//! Binds to `127.0.0.1:0` (random port), serves a single `GET /callback`
//! endpoint that receives the Connect token from the browser redirect.

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::Query;
use axum::response::Html;
use rand::Rng;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::cors::{Any, CorsLayer};

/// Result of starting the callback server.
pub struct CallbackServer {
    /// The port the server is listening on.
    pub port: u16,
    /// CSRF state parameter (base64url-encoded random bytes).
    pub state: String,
    /// Receives the token when the browser callback arrives.
    pub token_rx: oneshot::Receiver<String>,
}

#[derive(Deserialize)]
struct CallbackParams {
    token: Option<String>,
    state: Option<String>,
}

/// Start the local callback server on a random available port.
///
/// Returns the port, state, and a oneshot receiver for the token.
/// The server shuts down automatically after receiving a valid callback.
pub async fn start() -> anyhow::Result<CallbackServer> {
    let state = generate_state();
    let (token_tx, token_rx) = oneshot::channel::<String>();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    let expected_state = state.clone();
    let token_tx = Arc::new(Mutex::new(Some(token_tx)));

    // Allow browser fetch() from any origin (the setup page is on app.kyomi.ai)
    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any);

    let app = Router::new()
        .route(
            "/callback",
            axum::routing::get(move |Query(params): Query<CallbackParams>| {
                let expected = expected_state.clone();
                let tx = token_tx.clone();
                async move {
                    // Validate state parameter (CSRF protection)
                    let provided_state = match params.state {
                        Some(s) => s,
                        None => return Html(error_page("Missing state parameter.")),
                    };

                    if provided_state != expected {
                        return Html(error_page("Invalid state parameter. Please try again."));
                    }

                    let token = match params.token {
                        Some(t) if !t.is_empty() => t,
                        _ => return Html(error_page("Missing or empty token.")),
                    };

                    // Send token to the waiting CLI task
                    if let Some(sender) = tx.lock().unwrap_or_else(|e| e.into_inner()).take() {
                        let _ = sender.send(token);
                    }

                    Html(success_page())
                }
            }),
        )
        .layer(cors);

    // Spawn the server — it runs until the process exits
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    Ok(CallbackServer {
        port,
        state,
        token_rx,
    })
}

/// Generate a random state string for CSRF protection (16 bytes, base64url).
fn generate_state() -> String {
    let bytes: [u8; 16] = rand::rng().random();
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes)
}

fn success_page() -> String {
    r#"<!DOCTYPE html>
<html>
<head>
  <title>Kyomi Connect</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #0a0a0a; color: #fafafa; }
    .card { text-align: center; padding: 3rem; }
    .check { font-size: 3rem; margin-bottom: 1rem; }
    h1 { font-size: 1.5rem; margin-bottom: 0.5rem; }
    p { color: #a1a1aa; }
  </style>
</head>
<body>
  <div class="card">
    <div class="check">&#10003;</div>
    <h1>Token received!</h1>
    <p>You can close this tab and return to your terminal.</p>
  </div>
</body>
</html>"#
        .to_string()
}

fn error_page(message: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html>
<head>
  <title>Kyomi Connect — Error</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #0a0a0a; color: #fafafa; }}
    .card {{ text-align: center; padding: 3rem; }}
    .icon {{ font-size: 3rem; margin-bottom: 1rem; }}
    h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; }}
    p {{ color: #a1a1aa; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">&#10007;</div>
    <h1>Something went wrong</h1>
    <p>{message}</p>
  </div>
</body>
</html>"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_state_is_22_chars() {
        // 16 bytes base64url without padding = 22 characters
        let state = generate_state();
        assert_eq!(state.len(), 22);
    }

    #[test]
    fn generate_state_is_unique() {
        let a = generate_state();
        let b = generate_state();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn callback_server_binds_to_random_port() {
        let server = start().await.unwrap();
        assert!(server.port > 0);
        assert_eq!(server.state.len(), 22);
    }

    #[tokio::test]
    async fn callback_delivers_token() {
        let server = start().await.unwrap();
        let port = server.port;
        let state = server.state.clone();

        // Simulate browser redirect
        let client = reqwest::Client::new();
        let resp = client
            .get(format!(
                "http://127.0.0.1:{port}/callback?token=test-jwt-token&state={state}"
            ))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        // Token should arrive via the channel
        let token = server.token_rx.await.unwrap();
        assert_eq!(token, "test-jwt-token");
    }

    #[tokio::test]
    async fn callback_rejects_wrong_state() {
        let server = start().await.unwrap();
        let port = server.port;

        let client = reqwest::Client::new();
        let resp = client
            .get(format!(
                "http://127.0.0.1:{port}/callback?token=test-jwt-token&state=wrong-state"
            ))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success()); // Still 200, but HTML shows error

        let body = resp.text().await.unwrap();
        assert!(body.contains("Invalid state"));
    }
}
