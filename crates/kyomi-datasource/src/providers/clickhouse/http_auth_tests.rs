use super::*;
use futures_util::StreamExt;
use serde_json::json;
use wiremock::matchers::{body_string, header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

const USERNAME: &str = "fixture-user&=?";
const PASSWORD: &str = "fixture-key&=?%+";
const DATABASE: &str = "fixture database&=?";

async fn provider(uri: &str, password: &str) -> ClickHouseProvider {
    let url = reqwest::Url::parse(uri).unwrap();
    ClickHouseProvider::new(
        &json!({"host": url.host_str().unwrap(), "port": url.port().unwrap(), "database": DATABASE}),
        &json!({"username": USERNAME, "password": password}),
    )
    .await
    .unwrap()
}

async fn authenticated_response(server: &MockServer, sql: &str, body: &str, password: &str) {
    Mock::given(method("POST"))
        .and(header("X-ClickHouse-User", USERNAME))
        .and(header("X-ClickHouse-Key", password))
        .and(body_string(sql))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn every_http_path_authenticates_without_url_credentials() {
    let server = MockServer::start().await;
    authenticated_response(&server, "SELECT timezone()", "Europe/Paris", PASSWORD).await;
    authenticated_response(&server, "SELECT 1", "1", PASSWORD).await;
    authenticated_response(&server, "EXPLAIN SELECT 1", "ok", PASSWORD).await;
    let response = r#"{"meta":[{"name":"name","type":"String"}],"data":[["fixture_db"]]}"#;
    // Query, discovery and streaming share this JSON response, while auth is required.
    Mock::given(method("POST"))
        .and(header("X-ClickHouse-User", USERNAME))
        .and(header("X-ClickHouse-Key", PASSWORD))
        .and(|request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).starts_with("SELECT name")
        })
        .respond_with(ResponseTemplate::new(200).set_body_string(response))
        .expect(3)
        .mount(&server)
        .await;

    let provider = provider(&server.uri(), PASSWORD).await;
    let client_debug = format!("{:?}", provider.client);
    assert!(client_debug.contains("x-clickhouse-user"));
    assert!(client_debug.contains("x-clickhouse-key"));
    assert!(!client_debug.contains(USERNAME));
    assert!(!client_debug.contains(PASSWORD));
    assert!(!provider.server_tz_is_utc);
    assert!(provider.test_connection().await.unwrap());
    assert!(provider.dry_run("SELECT 1").await.unwrap().valid);
    let result = provider
        .execute_query("SELECT name", None, None, false, None)
        .await
        .unwrap();
    assert_eq!(result.status, QueryStatus::Success);
    assert_eq!(result.record_batch.unwrap().num_rows(), 1);
    assert_eq!(provider.list_databases().await.items, vec!["fixture_db"]);
    let mut stream = provider
        .execute_query_stream_arrow("SELECT name LIMIT 1", None, None, false, None)
        .await
        .unwrap();
    let mut batches = 0;
    let mut completed = false;
    while let Some(event) = stream.next().await {
        match event.unwrap() {
            kyomi_connect_protocol::ArrowStreamEvent::Batch { .. } => batches += 1,
            kyomi_connect_protocol::ArrowStreamEvent::Complete {
                total_rows_returned,
                ..
            } => {
                assert_eq!(total_rows_returned, 1);
                completed = true;
            }
            _ => {}
        }
    }
    assert_eq!(batches, 1);
    assert!(completed);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 6);
    for request in requests {
        let params: Vec<_> = request.url.query_pairs().collect();
        assert!(
            params
                .iter()
                .any(|(key, value)| key == "database" && value == DATABASE)
        );
        assert!(
            params
                .iter()
                .all(|(key, _)| key != "user" && key != "password")
        );
        assert!(!request.url.as_str().contains(USERNAME));
        assert!(!request.url.as_str().contains(PASSWORD));
    }
    server.verify().await;
}

#[tokio::test]
async fn empty_password_is_supported() {
    let server = MockServer::start().await;
    authenticated_response(&server, "SELECT timezone()", "UTC", "").await;
    authenticated_response(&server, "SELECT 1", "1", "").await;
    let provider = provider(&server.uri(), "").await;
    assert!(provider.server_tz_is_utc);
    assert!(provider.test_connection().await.unwrap());
    server.verify().await;
}

fn assert_redacted(message: &str, uri: &str) {
    assert!(message.contains("ClickHouse"), "{message}");
    for secret in [
        USERNAME,
        PASSWORD,
        &urlencoded(USERNAME),
        &urlencoded(PASSWORD),
        uri,
        &urlencoded(DATABASE),
    ] {
        assert!(!message.contains(secret), "Leaked {secret} in {message}");
    }
}

#[tokio::test]
async fn transport_errors_redact_urls_in_connection_query_dry_run_and_stream() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    // A real HTTP endpoint returning an invalid status line triggers reqwest errors.
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            if socket.read(&mut buffer).await.unwrap() == 0 {
                continue;
            }
            socket
                .write_all(b"invalid HTTP response\r\n\r\n")
                .await
                .unwrap();
        }
    });
    let provider = provider(&uri, PASSWORD).await;
    assert_redacted(
        &provider.test_connection().await.unwrap_err().to_string(),
        &uri,
    );
    let result = provider
        .execute_query("SELECT 1", None, None, false, None)
        .await
        .unwrap();
    assert_eq!(result.status, QueryStatus::Error);
    assert_redacted(result.error.as_deref().unwrap(), &uri);
    let result = provider.dry_run("SELECT 1").await.unwrap();
    assert!(!result.valid);
    assert_redacted(&result.message, &uri);
    let mut stream = provider
        .execute_query_stream_arrow("SELECT 1", None, None, false, None)
        .await
        .unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_redacted(&error.to_string(), &uri);
    server.abort();
}

#[tokio::test]
async fn redirects_cannot_forward_authentication_headers() {
    let destination = MockServer::start().await;
    let origin = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(307).insert_header("Location", destination.uri()))
        .mount(&origin)
        .await;
    let provider = provider(&origin.uri(), PASSWORD).await;
    assert!(provider.test_connection().await.is_err());
    assert!(!provider.dry_run("SELECT 1").await.unwrap().valid);
    assert_eq!(
        provider
            .execute_query("SELECT 1", None, None, false, None)
            .await
            .unwrap()
            .status,
        QueryStatus::Error
    );
    let mut stream = provider
        .execute_query_stream_arrow("SELECT 1", None, None, false, None)
        .await
        .unwrap();
    assert!(stream.next().await.unwrap().is_err());
    assert_eq!(origin.received_requests().await.unwrap().len(), 5);
    assert!(destination.received_requests().await.unwrap().is_empty());
}
