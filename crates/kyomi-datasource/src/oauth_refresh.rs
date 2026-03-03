//! Centralized OAuth token refresh for datasource providers.
//!
//! Several datasource types (BigQuery enterprise, Snowflake, Databricks, Azure
//! Synapse) use per-datasource OAuth tokens that may expire.  This module
//! provides [`ensure_valid_oauth_credentials`] to transparently refresh tokens
//! before provider construction.
//!
//! ## Supported Refresh Flows
//!
//! | Datasource | Token Endpoint |
//! |-----------|---------------|
//! | BigQuery (enterprise) | `https://oauth2.googleapis.com/token` |
//! | Snowflake | `https://{account}.snowflakecomputing.com/oauth/token-request` |
//! | Databricks | `https://{host}/oidc/v1/token` |
//! | Synapse | `https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token` |

use chrono::{DateTime, Utc};
use kyomi_connect_protocol::DatasourceType;
use kyomi_connect_protocol::Error;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Check if OAuth credentials need refreshing, and refresh if expired.
///
/// Returns updated credentials JSON with a new `oauth_access_token` and
/// `oauth_token_expiry` when the current token is expired or about to
/// expire (within a 5-minute buffer).
///
/// For datasource types that do not use per-datasource OAuth, the
/// credentials are returned unchanged.
///
/// # Important — Credential Mutation
///
/// The returned `Value` may differ from the input.  **The caller must
/// persist the returned credentials** back to the database so that
/// subsequent requests use the refreshed token and do not trigger
/// unnecessary refresh cycles.  On irrecoverable grant errors the
/// `oauth_refresh_token` field is cleared in the returned value to
/// signal that re-authorization is required.
///
/// # Errors
///
/// Returns an error if token refresh fails (network error, invalid grant,
/// etc.).
pub async fn ensure_valid_oauth_credentials(
    credentials: &Value,
    connection_config: &Value,
    ds_type: &DatasourceType,
) -> kyomi_connect_protocol::Result<Value> {
    // Only certain types use per-datasource OAuth
    if !uses_per_datasource_oauth(ds_type) {
        return Ok(credentials.clone());
    }

    // If there is no refresh token, there is nothing to refresh
    let refresh_token = credentials
        .get("oauth_refresh_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    if refresh_token.is_none() {
        // Databricks M2M (service principal): client_id + client_secret with no
        // refresh_token.  We cache the exchanged access_token + expiry in the
        // credentials so subsequent calls reuse it until it expires.
        if *ds_type == DatasourceType::Databricks {
            return handle_databricks_m2m(credentials, connection_config).await;
        }

        return Ok(credentials.clone());
    }

    // If the token has not expired, return as-is
    if !is_token_expired(credentials, TOKEN_EXPIRY_BUFFER_SECS) {
        return Ok(credentials.clone());
    }

    tracing::info!(
        ds_type = %ds_type,
        "OAuth token expired, attempting refresh"
    );

    let Some(refresh_token) = refresh_token else {
        return Ok(credentials.clone());
    };
    let client = crate::http_client()?;

    let refresh_result = match ds_type {
        DatasourceType::BigQuery => {
            refresh_bigquery_enterprise(&client, refresh_token, connection_config).await
        }
        DatasourceType::Snowflake => {
            refresh_snowflake(&client, refresh_token, connection_config).await
        }
        DatasourceType::Databricks => {
            refresh_databricks(&client, refresh_token, connection_config).await
        }
        DatasourceType::Synapse => refresh_synapse(&client, refresh_token, connection_config).await,
        _ => return Ok(credentials.clone()),
    };

    match refresh_result {
        Ok(token_response) => {
            let mut updated = credentials.clone();
            apply_refresh_response(&mut updated, &token_response);
            Ok(updated)
        }
        Err(e) => {
            let err_msg = e.to_string();
            if is_irrecoverable_grant_error(&err_msg) {
                tracing::warn!(
                    ds_type = %ds_type,
                    "OAuth refresh token revoked or expired, clearing refresh token"
                );
                let mut updated = credentials.clone();
                if let Some(obj) = updated.as_object_mut() {
                    obj.remove("oauth_refresh_token");
                }
                return Err(Error::Internal(format!(
                    "OAuth refresh failed (re-authorization required): {err_msg}"
                )));
            }
            Err(Error::Internal(format!(
                "OAuth token refresh failed: {err_msg}"
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// Token expiry helpers
// ---------------------------------------------------------------------------

/// Buffer in seconds before actual expiry when we consider the token expired.
const TOKEN_EXPIRY_BUFFER_SECS: i64 = 300;

/// Check whether the OAuth access token in `credentials` has expired (or will
/// expire within `buffer_secs` seconds).
///
/// Returns `true` (expired) if:
/// - `oauth_token_expiry` is missing or unparseable (assume expired)
/// - The expiry time minus the buffer is in the past
///
/// Returns `false` (still valid) if the token has enough remaining lifetime.
pub fn is_token_expired(credentials: &Value, buffer_secs: i64) -> bool {
    let expiry_value = credentials.get("oauth_token_expiry");

    let Some(expiry_value) = expiry_value else {
        // No expiry recorded — assume expired to be safe
        return true;
    };

    let Some(expiry_dt) = parse_token_expiry(expiry_value) else {
        // Could not parse — assume expired
        return true;
    };

    let now = Utc::now();
    let buffer = chrono::Duration::seconds(buffer_secs);
    now >= expiry_dt - buffer
}

/// Parse a token expiry value from credentials JSON.
///
/// Handles multiple formats:
/// - ISO 8601 string with `Z` suffix: `"2025-06-15T12:00:00Z"`
/// - ISO 8601 string with `+00:00` offset: `"2025-06-15T12:00:00+00:00"`
/// - ISO 8601 string without timezone (assumed UTC): `"2025-06-15T12:00:00"`
/// - Unix timestamp as a number: `1750000000`
/// - Unix timestamp as a string: `"1750000000"`
pub fn parse_token_expiry(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return None;
            }

            // Try ISO 8601 with timezone info first
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Some(dt.with_timezone(&Utc));
            }

            // Try ISO 8601 without timezone (assume UTC)
            if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                return Some(naive.and_utc());
            }

            // Try with fractional seconds
            if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                return Some(naive.and_utc());
            }

            // Try as Unix timestamp string
            if let Ok(ts) = s.parse::<i64>() {
                return DateTime::from_timestamp(ts, 0);
            }

            // Try as floating-point Unix timestamp
            if let Ok(ts) = s.parse::<f64>() {
                return DateTime::from_timestamp(ts as i64, 0);
            }

            None
        }
        Value::Number(n) => {
            if let Some(ts) = n.as_i64() {
                DateTime::from_timestamp(ts, 0)
            } else if let Some(ts) = n.as_f64() {
                DateTime::from_timestamp(ts as i64, 0)
            } else {
                None
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// OAuth type detection
// ---------------------------------------------------------------------------

/// Returns `true` if the given datasource type uses per-datasource OAuth
/// tokens that may need refreshing.
fn uses_per_datasource_oauth(ds_type: &DatasourceType) -> bool {
    matches!(
        ds_type,
        DatasourceType::BigQuery
            | DatasourceType::Snowflake
            | DatasourceType::Databricks
            | DatasourceType::Synapse
    )
}

// ---------------------------------------------------------------------------
// Provider-specific refresh implementations
// ---------------------------------------------------------------------------

/// Refresh a BigQuery enterprise OAuth token.
async fn refresh_bigquery_enterprise(
    client: &reqwest::Client,
    refresh_token: &str,
    connection_config: &Value,
) -> Result<Value, Error> {
    let client_id = connection_config
        .get("oauth_client_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::Provider(
                "BigQuery enterprise OAuth requires oauth_client_id in connection config".into(),
            )
        })?;

    let client_secret = connection_config
        .get("oauth_client_secret")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::Provider(
                "BigQuery enterprise OAuth requires oauth_client_secret in connection config"
                    .into(),
            )
        })?;

    let params = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", client_id),
        ("client_secret", client_secret),
    ];

    post_token_request(client, "https://oauth2.googleapis.com/token", &params).await
}

/// Refresh a Snowflake OAuth token.
async fn refresh_snowflake(
    client: &reqwest::Client,
    refresh_token: &str,
    connection_config: &Value,
) -> Result<Value, Error> {
    let account = connection_config
        .get("account")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::Provider("Snowflake OAuth refresh requires account in connection config".into())
        })?;

    let client_id = connection_config
        .get("oauth_client_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let client_secret = connection_config
        .get("oauth_client_secret")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let url = format!("https://{account}.snowflakecomputing.com/oauth/token-request");

    let params = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", client_id),
        ("client_secret", client_secret),
    ];

    post_token_request(client, &url, &params).await
}

/// Refresh a Databricks OAuth token.
///
/// Includes `client_secret` alongside `client_id` in the refresh request,
/// matching the Python `DatabricksOAuthService.refresh_access_token` behavior.
async fn refresh_databricks(
    client: &reqwest::Client,
    refresh_token: &str,
    connection_config: &Value,
) -> Result<Value, Error> {
    let server_hostname = connection_config
        .get("server_hostname")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::Provider(
                "Databricks OAuth refresh requires server_hostname in connection config".into(),
            )
        })?;

    let client_id = connection_config
        .get("oauth_client_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let client_secret = connection_config
        .get("oauth_client_secret")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let url = format!("https://{server_hostname}/oidc/v1/token");

    let params = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", client_id),
        ("client_secret", client_secret),
    ];

    post_token_request(client, &url, &params).await
}

/// Refresh a Synapse (Azure AD) OAuth token.
async fn refresh_synapse(
    client: &reqwest::Client,
    refresh_token: &str,
    connection_config: &Value,
) -> Result<Value, Error> {
    let tenant_id = connection_config
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::Provider("Synapse OAuth refresh requires tenant_id in connection config".into())
        })?;

    let client_id = connection_config
        .get("oauth_client_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::Provider(
                "Synapse OAuth refresh requires oauth_client_id in connection config".into(),
            )
        })?;

    let client_secret = connection_config
        .get("oauth_client_secret")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let url = format!("https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token");

    let params = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", client_id),
        ("client_secret", client_secret),
    ];

    post_token_request(client, &url, &params).await
}

// ---------------------------------------------------------------------------
// Databricks M2M (client_credentials) token caching
// ---------------------------------------------------------------------------

/// Handle Databricks M2M (service principal) credentials.
///
/// If `client_id` + `client_secret` are present with no `oauth_refresh_token`,
/// this is a Machine-to-Machine flow.  We cache the exchanged access token in
/// the credentials so it is reused until it expires, avoiding a token exchange
/// on every request.
///
/// Returns credentials unchanged if this is not an M2M flow (no client_id /
/// client_secret), or returns updated credentials with `oauth_access_token` +
/// `oauth_token_expiry` set.
#[cfg(feature = "databricks")]
async fn handle_databricks_m2m(
    credentials: &Value,
    connection_config: &Value,
) -> kyomi_connect_protocol::Result<Value> {
    let client_id = credentials
        .get("client_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    let client_secret = credentials
        .get("client_secret")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    let (Some(client_id), Some(client_secret)) = (client_id, client_secret) else {
        // Not an M2M flow — return as-is
        return Ok(credentials.clone());
    };

    // If there is already a cached token that has not expired, reuse it
    let has_cached_token = credentials
        .get("oauth_access_token")
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.is_empty());

    if has_cached_token && !is_token_expired(credentials, TOKEN_EXPIRY_BUFFER_SECS) {
        return Ok(credentials.clone());
    }

    // Exchange client_id + client_secret for a fresh access token
    let server_hostname = connection_config
        .get("server_hostname")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::Provider(
                "Databricks M2M OAuth requires server_hostname in connection config".into(),
            )
        })?;

    let token_url = format!("https://{server_hostname}/oidc/v1/token");
    let http = crate::http_client()?;

    tracing::info!("Databricks M2M token expired or missing, exchanging client_credentials");

    let result = crate::providers::databricks::exchange_m2m_token(
        &http,
        &token_url,
        client_id,
        client_secret,
    )
    .await?;

    // Store the token + expiry in credentials so the caller can persist them
    let mut updated = credentials.clone();
    if let Some(obj) = updated.as_object_mut() {
        obj.insert(
            "oauth_access_token".into(),
            Value::String(result.access_token),
        );
        if let Some(expires_in) = result.expires_in {
            let expiry = Utc::now() + chrono::Duration::seconds(expires_in);
            obj.insert(
                "oauth_token_expiry".into(),
                Value::String(expiry.to_rfc3339()),
            );
        }
    }

    Ok(updated)
}

/// Fallback when the databricks feature is not enabled.
#[cfg(not(feature = "databricks"))]
async fn handle_databricks_m2m(
    credentials: &Value,
    _connection_config: &Value,
) -> kyomi_connect_protocol::Result<Value> {
    // Without the databricks feature, M2M token exchange is not available.
    // Return credentials as-is (the caller will fail naturally if a valid
    // access token is not already present).
    Ok(credentials.clone())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// POST a form-encoded token refresh request and return the parsed JSON body.
async fn post_token_request(
    client: &reqwest::Client,
    url: &str,
    params: &[(&str, &str)],
) -> Result<Value, Error> {
    let response = tokio::time::timeout(
        crate::OAUTH_REFRESH_TIMEOUT,
        client.post(url).form(params).send(),
    )
    .await
    .map_err(|_| {
        Error::Internal(format!(
            "OAuth token refresh timed out after {}s",
            crate::OAUTH_REFRESH_TIMEOUT.as_secs()
        ))
    })?
    .map_err(|e| Error::Internal(format!("OAuth token refresh HTTP request failed: {e}")))?;

    let status = response.status();
    let body: Value = response.json().await.map_err(|e| {
        Error::Internal(format!("Failed to parse OAuth token refresh response: {e}"))
    })?;

    if !status.is_success() {
        let error = body
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("unknown_error");
        let description = body
            .get("error_description")
            .and_then(|d| d.as_str())
            .unwrap_or("No description");
        return Err(Error::Internal(format!(
            "OAuth refresh failed ({error}): {description}"
        )));
    }

    Ok(body)
}

/// Apply refresh response fields to the credentials JSON.
///
/// Updates `oauth_access_token`, `oauth_token_expiry`, and optionally
/// `oauth_refresh_token` (if the provider rotated it).
fn apply_refresh_response(credentials: &mut Value, token_response: &Value) {
    let Some(obj) = credentials.as_object_mut() else {
        return;
    };

    // Update access token
    if let Some(access_token) = token_response.get("access_token") {
        obj.insert("oauth_access_token".into(), access_token.clone());
    }

    // Update expiry: calculate from expires_in if present
    if let Some(expires_in) = token_response.get("expires_in").and_then(|v| v.as_i64()) {
        let expiry = Utc::now() + chrono::Duration::seconds(expires_in);
        obj.insert(
            "oauth_token_expiry".into(),
            Value::String(expiry.to_rfc3339()),
        );
    }

    // Update refresh token if rotated
    if let Some(new_refresh) = token_response.get("refresh_token") {
        obj.insert("oauth_refresh_token".into(), new_refresh.clone());
    }
}

/// Check if the error indicates an irrecoverable token error where the
/// refresh token itself is invalid/revoked.
fn is_irrecoverable_grant_error(error_msg: &str) -> bool {
    let lower = error_msg.to_lowercase();
    lower.contains("invalid_grant")
        || lower.contains("invalid_token")
        || lower.contains("token has been expired or revoked")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_token_expiry ---

    #[test]
    fn parse_expiry_iso_with_z() {
        let val = Value::String("2025-06-15T12:00:00Z".into());
        let dt = parse_token_expiry(&val).expect("should parse");
        assert_eq!(dt.to_rfc3339(), "2025-06-15T12:00:00+00:00");
    }

    #[test]
    fn parse_expiry_iso_with_offset() {
        let val = Value::String("2025-06-15T12:00:00+00:00".into());
        let dt = parse_token_expiry(&val).expect("should parse");
        assert_eq!(dt.to_rfc3339(), "2025-06-15T12:00:00+00:00");
    }

    #[test]
    fn parse_expiry_iso_without_timezone() {
        let val = Value::String("2025-06-15T12:00:00".into());
        let dt = parse_token_expiry(&val).expect("should parse");
        assert_eq!(dt.to_rfc3339(), "2025-06-15T12:00:00+00:00");
    }

    #[test]
    fn parse_expiry_iso_with_fractional_seconds() {
        let val = Value::String("2025-06-15T12:00:00.123456".into());
        let dt = parse_token_expiry(&val).expect("should parse");
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            "2025-06-15T12:00:00"
        );
    }

    #[test]
    fn parse_expiry_unix_timestamp_number() {
        // 1750075200 = 2025-06-16T12:00:00Z
        let val = Value::Number(serde_json::Number::from(1_750_075_200_i64));
        let dt = parse_token_expiry(&val).expect("should parse");
        assert_eq!(dt.to_rfc3339(), "2025-06-16T12:00:00+00:00");
    }

    #[test]
    fn parse_expiry_unix_timestamp_string() {
        let val = Value::String("1750075200".into());
        let dt = parse_token_expiry(&val).expect("should parse");
        assert_eq!(dt.to_rfc3339(), "2025-06-16T12:00:00+00:00");
    }

    #[test]
    fn parse_expiry_float_timestamp() {
        let val = serde_json::json!(1_750_075_200.5);
        let dt = parse_token_expiry(&val).expect("should parse");
        assert_eq!(dt.to_rfc3339(), "2025-06-16T12:00:00+00:00");
    }

    #[test]
    fn parse_expiry_none_value() {
        assert!(parse_token_expiry(&Value::Null).is_none());
    }

    #[test]
    fn parse_expiry_empty_string() {
        let val = Value::String(String::new());
        assert!(parse_token_expiry(&val).is_none());
    }

    #[test]
    fn parse_expiry_invalid_string() {
        let val = Value::String("not-a-date".into());
        assert!(parse_token_expiry(&val).is_none());
    }

    #[test]
    fn parse_expiry_bool_returns_none() {
        assert!(parse_token_expiry(&Value::Bool(true)).is_none());
    }

    // --- is_token_expired ---

    #[test]
    fn token_not_expired_with_future_expiry() {
        let future = Utc::now() + chrono::Duration::hours(1);
        let creds = serde_json::json!({
            "oauth_token_expiry": future.to_rfc3339(),
        });
        assert!(!is_token_expired(&creds, 300));
    }

    #[test]
    fn token_expired_with_past_expiry() {
        let past = Utc::now() - chrono::Duration::hours(1);
        let creds = serde_json::json!({
            "oauth_token_expiry": past.to_rfc3339(),
        });
        assert!(is_token_expired(&creds, 300));
    }

    #[test]
    fn token_expired_within_buffer() {
        // Token expires in 2 minutes, buffer is 5 minutes -> considered expired
        let soon = Utc::now() + chrono::Duration::minutes(2);
        let creds = serde_json::json!({
            "oauth_token_expiry": soon.to_rfc3339(),
        });
        assert!(is_token_expired(&creds, 300));
    }

    #[test]
    fn token_not_expired_outside_buffer() {
        // Token expires in 10 minutes, buffer is 5 minutes -> not expired
        let later = Utc::now() + chrono::Duration::minutes(10);
        let creds = serde_json::json!({
            "oauth_token_expiry": later.to_rfc3339(),
        });
        assert!(!is_token_expired(&creds, 300));
    }

    #[test]
    fn token_expired_when_no_expiry_field() {
        let creds = serde_json::json!({
            "oauth_access_token": "some-token",
        });
        assert!(is_token_expired(&creds, 300));
    }

    #[test]
    fn token_expired_when_expiry_unparseable() {
        let creds = serde_json::json!({
            "oauth_token_expiry": "not-a-date",
        });
        assert!(is_token_expired(&creds, 300));
    }

    #[test]
    fn token_expired_with_zero_buffer() {
        let future = Utc::now() + chrono::Duration::minutes(1);
        let creds = serde_json::json!({
            "oauth_token_expiry": future.to_rfc3339(),
        });
        // With zero buffer, a future expiry is not expired
        assert!(!is_token_expired(&creds, 0));
    }

    // --- uses_per_datasource_oauth ---

    #[test]
    fn oauth_types_detected_correctly() {
        assert!(uses_per_datasource_oauth(&DatasourceType::BigQuery));
        assert!(uses_per_datasource_oauth(&DatasourceType::Snowflake));
        assert!(uses_per_datasource_oauth(&DatasourceType::Databricks));
        assert!(uses_per_datasource_oauth(&DatasourceType::Synapse));
    }

    #[test]
    fn non_oauth_types_detected_correctly() {
        assert!(!uses_per_datasource_oauth(&DatasourceType::Postgres));
        assert!(!uses_per_datasource_oauth(&DatasourceType::MySQL));
        assert!(!uses_per_datasource_oauth(&DatasourceType::Redshift));
        assert!(!uses_per_datasource_oauth(&DatasourceType::ClickHouse));
        assert!(!uses_per_datasource_oauth(&DatasourceType::SqlServer));
    }

    // --- apply_refresh_response ---

    #[test]
    fn apply_refresh_updates_access_token() {
        let mut creds = serde_json::json!({
            "oauth_access_token": "old-token",
        });
        let response = serde_json::json!({
            "access_token": "new-token",
            "expires_in": 3600,
        });
        apply_refresh_response(&mut creds, &response);
        assert_eq!(creds["oauth_access_token"], "new-token");
        assert!(creds["oauth_token_expiry"].is_string());
    }

    #[test]
    fn apply_refresh_updates_refresh_token_when_rotated() {
        let mut creds = serde_json::json!({
            "oauth_access_token": "old-token",
            "oauth_refresh_token": "old-refresh",
        });
        let response = serde_json::json!({
            "access_token": "new-token",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
        });
        apply_refresh_response(&mut creds, &response);
        assert_eq!(creds["oauth_refresh_token"], "new-refresh");
    }

    #[test]
    fn apply_refresh_preserves_refresh_token_when_not_rotated() {
        let mut creds = serde_json::json!({
            "oauth_access_token": "old-token",
            "oauth_refresh_token": "keep-this",
        });
        let response = serde_json::json!({
            "access_token": "new-token",
            "expires_in": 3600,
        });
        apply_refresh_response(&mut creds, &response);
        assert_eq!(creds["oauth_refresh_token"], "keep-this");
    }

    // --- is_irrecoverable_grant_error ---

    #[test]
    fn detects_invalid_grant() {
        assert!(is_irrecoverable_grant_error(
            "OAuth refresh failed (invalid_grant): Token has been revoked"
        ));
    }

    #[test]
    fn detects_invalid_token() {
        assert!(is_irrecoverable_grant_error(
            "Error: invalid_token - The token is expired"
        ));
    }

    #[test]
    fn detects_token_expired_or_revoked() {
        assert!(is_irrecoverable_grant_error(
            "Token has been expired or revoked"
        ));
    }

    #[test]
    fn does_not_flag_transient_errors() {
        assert!(!is_irrecoverable_grant_error("Connection timed out"));
        assert!(!is_irrecoverable_grant_error(
            "HTTP 500 Internal Server Error"
        ));
    }

    // --- ensure_valid_oauth_credentials (non-OAuth types pass through) ---

    #[tokio::test]
    async fn non_oauth_type_returns_credentials_unchanged() {
        let creds = serde_json::json!({"username": "user", "password": "pass"});
        let config = serde_json::json!({"host": "localhost"});

        let result = ensure_valid_oauth_credentials(&creds, &config, &DatasourceType::Postgres)
            .await
            .expect("should succeed");
        assert_eq!(result, creds);
    }

    #[tokio::test]
    async fn oauth_type_without_refresh_token_returns_unchanged() {
        let creds = serde_json::json!({
            "oauth_access_token": "some-token",
        });
        let config = serde_json::json!({"account": "test"});

        let result = ensure_valid_oauth_credentials(&creds, &config, &DatasourceType::Snowflake)
            .await
            .expect("should succeed");
        assert_eq!(result, creds);
    }

    // --- Databricks M2M cached token reuse ---

    #[tokio::test]
    async fn databricks_m2m_with_valid_cached_token_returns_unchanged() {
        let future = Utc::now() + chrono::Duration::hours(1);
        let creds = serde_json::json!({
            "client_id": "sp-client-id",
            "client_secret": "sp-client-secret",
            "oauth_access_token": "cached-m2m-token",
            "oauth_token_expiry": future.to_rfc3339(),
        });
        let config = serde_json::json!({"server_hostname": "dbc-test.cloud.databricks.com"});

        // Should return as-is because the cached token is still valid
        let result = ensure_valid_oauth_credentials(&creds, &config, &DatasourceType::Databricks)
            .await
            .expect("should succeed");
        assert_eq!(result["oauth_access_token"], "cached-m2m-token");
    }

    #[tokio::test]
    async fn databricks_m2m_without_client_credentials_returns_unchanged() {
        // No client_id/client_secret and no refresh_token — not an M2M flow
        let creds = serde_json::json!({
            "oauth_access_token": "some-token",
        });
        let config = serde_json::json!({"server_hostname": "dbc-test.cloud.databricks.com"});

        let result = ensure_valid_oauth_credentials(&creds, &config, &DatasourceType::Databricks)
            .await
            .expect("should succeed");
        assert_eq!(result, creds);
    }

    #[tokio::test]
    async fn oauth_type_with_valid_token_returns_unchanged() {
        let future = Utc::now() + chrono::Duration::hours(1);
        let creds = serde_json::json!({
            "oauth_access_token": "valid-token",
            "oauth_refresh_token": "refresh-token",
            "oauth_token_expiry": future.to_rfc3339(),
        });
        let config = serde_json::json!({"account": "test"});

        let result = ensure_valid_oauth_credentials(&creds, &config, &DatasourceType::Snowflake)
            .await
            .expect("should succeed");
        assert_eq!(result, creds);
    }
}
