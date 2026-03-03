use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;

use kyomi_connect_protocol::wire::{ConnectRequest, ConnectResponse};

/// Connect wire protocol version. Sent as `X-Kyomi-Protocol` header.
/// Version 2 = streaming capable (multi-message responses).
const PROTOCOL_VERSION: &str = "2";

/// WebSocket client that maintains a persistent connection to Kyomi.
///
/// Uses a concurrent reader/writer architecture:
/// - **Writer task**: owns `SplitSink`, sends messages from an mpsc channel
///   and handles heartbeat pongs.
/// - **Reader task**: owns `SplitStream`, deserializes requests and spawns
///   a tokio task per request for concurrent execution.
pub struct WsClient {
    url: String,
    token: String,
}

impl WsClient {
    pub fn new(url: String, token: String) -> Self {
        Self { url, token }
    }

    /// Attempt a single WebSocket connection to verify connectivity.
    /// Returns Ok(()) if the handshake succeeds, Err on failure.
    /// The connection is immediately closed after verification.
    pub async fn connect_once(&self) -> anyhow::Result<()> {
        let (ws_sender, _ws_receiver) = self.connect().await?;
        // Drop immediately — we just needed to verify the handshake
        drop(ws_sender);
        Ok(())
    }

    /// Main loop: connect, process messages, reconnect on drop.
    /// Never returns — runs until the process exits.
    pub async fn run_forever<F, Fut>(
        &self,
        ws_connected: Arc<AtomicBool>,
        handler: F,
    ) -> !
    where
        F: Fn(ConnectRequest) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<ConnectResponse>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let mut backoff = Duration::from_secs(1);

        loop {
            match self.connect().await {
                Ok((ws_sender, ws_receiver)) => {
                    backoff = Duration::from_secs(1);
                    ws_connected.store(true, Ordering::Relaxed);
                    tracing::info!("Connected to Kyomi (protocol v{PROTOCOL_VERSION})");

                    self.run_session(ws_sender, ws_receiver, handler.clone())
                        .await;

                    ws_connected.store(false, Ordering::Relaxed);
                    tracing::warn!("Disconnected from Kyomi, reconnecting...");
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        delay_secs = backoff.as_secs(),
                        "Failed to connect, retrying..."
                    );
                }
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
    }

    /// Establish WebSocket connection with Authorization and protocol version headers.
    async fn connect(
        &self,
    ) -> anyhow::Result<(
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            tungstenite::Message,
        >,
        futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    )> {
        let request = http::Request::builder()
            .uri(&self.url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("X-Kyomi-Protocol", PROTOCOL_VERSION)
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tungstenite::handshake::client::generate_key(),
            )
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header(
                "Host",
                extract_host(&self.url).unwrap_or("api.kyomi.ai"),
            )
            .body(())
            .map_err(|e| anyhow::anyhow!("Failed to build request: {e}"))?;

        let (ws_stream, _response) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| anyhow::anyhow!("WebSocket connection failed: {e}"))?;

        Ok(ws_stream.split())
    }

    /// Run a single WebSocket session with concurrent reader/writer tasks.
    ///
    /// Returns when the connection is lost or the server sends a close frame.
    async fn run_session<F, Fut>(
        &self,
        ws_sender: futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            tungstenite::Message,
        >,
        mut ws_receiver: futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
        handler: Arc<F>,
    ) where
        F: Fn(ConnectRequest) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<ConnectResponse>> + Send + 'static,
    {
        // Channel for sending messages to the writer task
        let (write_tx, write_rx) = mpsc::channel::<tungstenite::Message>(64);

        // Spawn the writer task (owns ws_sender)
        let writer_handle = tokio::spawn(writer_task(ws_sender, write_rx));

        // Reader loop: deserialize requests and spawn handler tasks
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(tungstenite::Message::Text(text)) => {
                    let request: ConnectRequest = match serde_json::from_str(&text) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(error = %e, "Failed to parse incoming message");
                            continue;
                        }
                    };

                    let request_id = request.id.clone();
                    let write_tx = write_tx.clone();
                    let handler = handler.clone();

                    tokio::spawn(async move {
                        let responses = handler(request).await;
                        for response in responses {
                            let json = match serde_json::to_string(&response) {
                                Ok(j) => j,
                                Err(e) => {
                                    tracing::error!(
                                        request_id,
                                        error = %e,
                                        "Failed to serialize response"
                                    );
                                    continue;
                                }
                            };

                            if write_tx
                                .send(tungstenite::Message::Text(json.into()))
                                .await
                                .is_err()
                            {
                                tracing::debug!(request_id, "Writer closed, dropping responses");
                                return;
                            }
                        }
                    });
                }
                Ok(tungstenite::Message::Ping(data)) => {
                    if write_tx
                        .send(tungstenite::Message::Pong(data))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(tungstenite::Message::Close(_)) => {
                    tracing::info!("Server sent close frame");
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "WebSocket error");
                    break;
                }
            }
        }

        // Drop the sender so the writer task exits
        drop(write_tx);
        let _ = writer_handle.await;
    }
}

/// Writer task: drains the mpsc channel and sends messages over the WebSocket.
async fn writer_task(
    mut ws_sender: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tungstenite::Message,
    >,
    mut rx: mpsc::Receiver<tungstenite::Message>,
) {
    while let Some(msg) = rx.recv().await {
        if let Err(e) = ws_sender.send(msg).await {
            tracing::warn!(error = %e, "Writer: failed to send, closing");
            break;
        }
    }
}

/// Extract host from a URL string.
fn extract_host(url: &str) -> Option<&str> {
    url.strip_prefix("wss://")
        .or_else(|| url.strip_prefix("ws://"))
        .and_then(|rest| rest.split('/').next())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_host_wss() {
        assert_eq!(
            extract_host("wss://api.kyomi.ai/connect/v1"),
            Some("api.kyomi.ai")
        );
    }

    #[test]
    fn extract_host_ws() {
        assert_eq!(
            extract_host("ws://localhost:8001/connect/v1"),
            Some("localhost:8001")
        );
    }

    #[test]
    fn extract_host_no_scheme() {
        assert_eq!(extract_host("api.kyomi.ai/connect/v1"), None);
    }
}
