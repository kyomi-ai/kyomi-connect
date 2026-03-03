//! Interactive setup wizard for Kyomi Connect.
//!
//! Guides the operator through: paste token -> verify JWT -> fetch datasource info
//! -> enter DB credentials -> save config -> show next steps.

use std::io::IsTerminal;
use std::str::FromStr;

use dialoguer::{Input, Password, Select};
use serde::Deserialize;

use crate::callback_server;
use crate::config::default_port;
use crate::config_file::ConfigFile;

/// SSL mode options presented to the user.
const SSL_OPTIONS: &[(&str, &str)] = &[
    ("prefer", "Try TLS, fall back to unencrypted (recommended)"),
    ("require", "Require TLS (cloud databases)"),
    ("disable", "No TLS (local/trusted network)"),
    ("verify-ca", "Require TLS + verify CA certificate"),
    ("verify-full", "Require TLS + verify CA + hostname"),
];

// ---------------------------------------------------------------------------
// Public types (used by the Status command in Task 10)
// ---------------------------------------------------------------------------

/// Lightweight peek at JWT claims without cryptographic verification.
/// Used to extract issuer and database type before JWKS fetch.
pub struct TokenPeek {
    pub iss: String,
    pub db: String,
    pub url: String,
}

/// Response from `GET /api/v1/connect/info`.
#[derive(Debug, Deserialize)]
pub struct ConnectInfoResponse {
    pub datasource_name: String,
    pub datasource_type: String,
    pub datasource_type_label: String,
    pub workspace_name: String,
    pub default_port: Option<u16>,
}

// ---------------------------------------------------------------------------
// Public helpers (reusable by Status command)
// ---------------------------------------------------------------------------

/// Peek at JWT claims without verifying the signature.
/// Returns `None` if the token is malformed or missing required fields.
pub fn peek_token_safe(token: &str) -> Option<TokenPeek> {
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::ES256);
    validation.insecure_disable_signature_validation();
    validation.set_required_spec_claims::<&str>(&[]);

    let data = jsonwebtoken::decode::<serde_json::Value>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(&[]),
        &validation,
    )
    .ok()?;

    let claims = data.claims;
    Some(TokenPeek {
        iss: claims.get("iss")?.as_str()?.to_string(),
        db: claims.get("db")?.as_str()?.to_string(),
        url: claims.get("url")?.as_str()?.to_string(),
    })
}

/// Fetch datasource info from the Kyomi API.
/// Returns `None` on any error (network, auth, deserialization).
pub async fn fetch_connect_info_safe(base_url: &str, token: &str) -> Option<ConnectInfoResponse> {
    let url = format!("{}/api/v1/connect/info", base_url.trim_end_matches('/'));

    let client = reqwest::Client::new();
    let resp = client.get(&url).bearer_auth(token).send().await.ok()?;

    if !resp.status().is_success() {
        return None;
    }

    resp.json::<ConnectInfoResponse>().await.ok()
}

// ---------------------------------------------------------------------------
// JWT verification
// ---------------------------------------------------------------------------

/// Verify the JWT signature against the JWKS endpoint at `{iss}/.well-known/jwks.json`.
async fn verify_token_signature(token: &str, iss: &str) -> anyhow::Result<()> {
    let jwks_url = format!("{}/.well-known/jwks.json", iss.trim_end_matches('/'));

    let jwks_response = reqwest::get(&jwks_url)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch JWKS from {jwks_url}: {e}"))?;

    let jwks: serde_json::Value = jwks_response
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse JWKS response: {e}"))?;

    let keys = jwks
        .get("keys")
        .and_then(|k| k.as_array())
        .ok_or_else(|| anyhow::anyhow!("Invalid JWKS response: no 'keys' array"))?;

    if keys.is_empty() {
        anyhow::bail!("JWKS has no keys");
    }

    // Try each key — the JWKS may contain multiple keys during rotation.
    // We try kid-matching first (if the JWT has a kid header), then fall back
    // to trying every P-256 key.
    let header = jsonwebtoken::decode_header(token)
        .map_err(|e| anyhow::anyhow!("Invalid JWT header: {e}"))?;
    let token_kid = header.kid.as_deref();

    for key in keys {
        let crv = key.get("crv").and_then(|v| v.as_str()).unwrap_or_default();
        if crv != "P-256" {
            continue;
        }

        // If the JWT has a kid, only try keys that match.
        if let Some(kid) = token_kid {
            let key_kid = key.get("kid").and_then(|v| v.as_str()).unwrap_or_default();
            if key_kid != kid {
                continue;
            }
        }

        let ec_jwk = serde_json::json!({
            "kty": "EC",
            "crv": "P-256",
            "x": key.get("x").and_then(|v| v.as_str()).unwrap_or_default(),
            "y": key.get("y").and_then(|v| v.as_str()).unwrap_or_default(),
        });

        let decoding_key = match jsonwebtoken::DecodingKey::from_jwk(
            &serde_json::from_value(ec_jwk)
                .map_err(|e| anyhow::anyhow!("Invalid JWK structure: {e}"))?,
        ) {
            Ok(k) => k,
            Err(_) => continue,
        };

        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::ES256);
        validation.validate_exp = false; // Connect tokens don't expire
        validation.set_required_spec_claims::<&str>(&[]);

        match jsonwebtoken::decode::<serde_json::Value>(token, &decoding_key, &validation) {
            Ok(_) => return Ok(()),
            Err(_) => continue,
        }
    }

    anyhow::bail!("Token signature verification failed — token was not signed by Kyomi")
}

// ---------------------------------------------------------------------------
// Token acquisition
// ---------------------------------------------------------------------------

/// The base URL for the Connect setup page.
const SETUP_PAGE_BASE: &str = "https://app.kyomi.ai/connect/setup";

/// Resolve the token from CLI args or interactive browser flow.
fn resolve_token(
    token_arg: Option<String>,
    token_file_arg: Option<String>,
) -> anyhow::Result<String> {
    // 1. --token flag
    if let Some(t) = token_arg {
        let t = t.trim().to_string();
        if t.is_empty() {
            anyhow::bail!("--token value is empty");
        }
        return Ok(t);
    }

    // 2. --token-file flag
    if let Some(path) = token_file_arg {
        let content = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("Failed to read token file '{path}': {e}"))?;
        let t = content.trim().to_string();
        if t.is_empty() {
            anyhow::bail!("Token file '{path}' is empty");
        }
        return Ok(t);
    }

    // 3. Non-interactive: bail with usage message
    if !std::io::stdin().is_terminal() {
        anyhow::bail!(
            "No token provided. Use --token <TOKEN> or --token-file <PATH>, \
             or run interactively to open browser-based setup."
        );
    }

    // 4. Interactive: try browser-based flow
    anyhow::bail!("use_browser_flow");
}

/// Resolve token via browser-based flow: start local callback server,
/// open browser to setup page, wait for token or manual paste.
async fn resolve_token_interactive() -> anyhow::Result<String> {
    // Start the local callback server
    let server = match callback_server::start().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  Could not start local callback server: {e}");
            eprintln!("  Falling back to manual token entry.");
            eprintln!();
            return resolve_token_manual_paste();
        }
    };

    let url = format!(
        "{SETUP_PAGE_BASE}?callback_port={}&state={}",
        server.port, server.state
    );

    // Try to open browser
    eprintln!();
    match open::that(&url) {
        Ok(()) => {
            eprintln!("  Opening browser for Connect setup...");
        }
        Err(_) => {
            eprintln!("  Could not open browser automatically.");
        }
    }

    eprintln!();
    eprintln!("  If the browser didn't open, visit this URL:");
    eprintln!();
    eprintln!("    {url}");
    eprintln!();
    eprintln!("  Waiting for authorization...");
    eprintln!("  (or paste a token here and press Enter)");
    eprintln!();

    // Race: browser callback vs manual stdin paste vs timeout
    let stdin_task = tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).ok();
        line.trim().to_string()
    });

    tokio::select! {
        // Browser callback received
        result = server.token_rx => {
            match result {
                Ok(token) => {
                    eprintln!("  Token received from browser.");
                    Ok(token)
                }
                Err(_) => anyhow::bail!("Callback channel closed unexpectedly"),
            }
        }
        // Manual paste from stdin
        result = stdin_task => {
            match result {
                Ok(token) if !token.is_empty() => {
                    eprintln!("  Token received from stdin.");
                    Ok(token)
                }
                _ => anyhow::bail!("No token entered"),
            }
        }
        // 10-minute timeout
        _ = tokio::time::sleep(std::time::Duration::from_secs(600)) => {
            anyhow::bail!("Timed out waiting for token (10 minutes). Run setup again to retry.");
        }
    }
}

/// Fallback: simple manual token paste (when callback server fails to start).
fn resolve_token_manual_paste() -> anyhow::Result<String> {
    eprintln!("  Paste your Connect token from the Kyomi dashboard.");
    eprintln!("  (Settings > Datasources > your datasource > Connect tab)");
    eprintln!();

    let token: String = Input::new()
        .with_prompt("  Token")
        .interact_text()
        .map_err(|e| anyhow::anyhow!("Failed to read token: {e}"))?;

    let token = token.trim().to_string();
    if token.is_empty() {
        anyhow::bail!("No token entered");
    }

    Ok(token)
}

// ---------------------------------------------------------------------------
// Main setup wizard
// ---------------------------------------------------------------------------

/// Run the interactive setup wizard.
///
/// All `Option` parameters come from clap CLI args; `None` means the user
/// didn't pass the flag and we should prompt interactively.
#[allow(clippy::too_many_arguments)]
pub async fn run_setup(
    token_arg: Option<String>,
    token_file_arg: Option<String>,
    db_host_arg: Option<String>,
    db_port_arg: Option<u16>,
    db_name_arg: Option<String>,
    db_user_arg: Option<String>,
    db_password_file_arg: Option<String>,
    db_ssl_mode_arg: Option<String>,
) -> anyhow::Result<()> {
    eprintln!();
    eprintln!("  Kyomi Connect — Setup Wizard");
    eprintln!("  ────────────────────────────");

    // Load existing config (if any) to use as defaults
    let existing = ConfigFile::load();
    let has_existing = existing.is_some();

    if has_existing {
        eprintln!();
        eprintln!("  Existing configuration found. Press Enter to keep current values.");
    }

    // -----------------------------------------------------------------------
    // Step 1: Get the token
    // -----------------------------------------------------------------------
    let token = match resolve_token(token_arg, token_file_arg) {
        Ok(t) => t,
        Err(e) if e.to_string() == "use_browser_flow" => {
            // If we have an existing valid token, offer to keep it
            if let Some(ref cfg) = existing {
                eprintln!();
                let keep = Select::new()
                    .with_prompt("  Token")
                    .items(&["Keep current token", "Enter new token"])
                    .default(0)
                    .interact()
                    .map_err(|e| anyhow::anyhow!("Failed to read selection: {e}"))?;
                if keep == 0 {
                    cfg.token.clone()
                } else {
                    resolve_token_interactive().await?
                }
            } else {
                resolve_token_interactive().await?
            }
        }
        Err(e) => return Err(e),
    };

    // -----------------------------------------------------------------------
    // Step 2: Verify token and fetch datasource info
    // -----------------------------------------------------------------------
    eprintln!();
    eprint!("  Verifying token... ");

    let peek = peek_token_safe(&token).ok_or_else(|| anyhow::anyhow!("Invalid token format"))?;

    verify_token_signature(&token, &peek.iss).await?;

    let info = fetch_connect_info_safe(&peek.iss, &token)
        .await
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not reach Kyomi at {}. Check that the token is valid.",
                peek.iss
            )
        })?;

    eprintln!("ok");
    eprintln!();
    eprintln!(
        "  Datasource:  {} ({})",
        info.datasource_name, info.datasource_type_label
    );
    eprintln!("  Workspace:   {}", info.workspace_name);

    // -----------------------------------------------------------------------
    // Step 5: Collect database credentials
    // -----------------------------------------------------------------------
    let is_interactive = std::io::stdin().is_terminal();

    eprintln!();
    eprintln!("  Database Connection");
    eprintln!("  ──────────────────");

    let port_default = info.default_port.unwrap_or_else(|| default_port(&peek.db));

    // Load existing password (for default display)
    let existing_password = existing.as_ref().and_then(|cfg| {
        cfg.db_password_file.as_ref().and_then(|path| {
            std::fs::read_to_string(path)
                .ok()
                .map(|s| s.trim().to_string())
        })
    });

    let db_host = match db_host_arg {
        Some(h) => h,
        None if is_interactive => {
            let default = existing
                .as_ref()
                .map(|c| c.db_host.clone())
                .unwrap_or_else(|| "localhost".to_string());
            Input::new()
                .with_prompt("  Host")
                .with_initial_text(&default)
                .interact_text()?
        }
        None => existing
            .as_ref()
            .map(|c| c.db_host.clone())
            .ok_or_else(|| anyhow::anyhow!("--db-host is required in non-interactive mode"))?,
    };

    let db_port: u16 = match db_port_arg {
        Some(p) => p,
        None if is_interactive => {
            let default = existing.as_ref().map(|c| c.db_port).unwrap_or(port_default);
            Input::new()
                .with_prompt("  Port")
                .with_initial_text(default.to_string())
                .interact_text()?
        }
        None => existing.as_ref().map(|c| c.db_port).unwrap_or(port_default),
    };

    let db_name = match db_name_arg {
        Some(n) => n,
        None if is_interactive => {
            let default = existing.as_ref().map(|c| c.db_name.as_str()).unwrap_or("");
            Input::new()
                .with_prompt("  Database")
                .with_initial_text(default)
                .allow_empty(false)
                .interact_text()?
        }
        None => existing
            .as_ref()
            .map(|c| c.db_name.clone())
            .ok_or_else(|| anyhow::anyhow!("--db-name is required in non-interactive mode"))?,
    };

    let db_user = match db_user_arg {
        Some(u) => u,
        None if is_interactive => {
            let default = existing.as_ref().map(|c| c.db_user.as_str()).unwrap_or("");
            Input::new()
                .with_prompt("  Username")
                .with_initial_text(default)
                .allow_empty(false)
                .interact_text()?
        }
        None => existing
            .as_ref()
            .map(|c| c.db_user.clone())
            .ok_or_else(|| anyhow::anyhow!("--db-user is required in non-interactive mode"))?,
    };

    // Password: from file, interactive prompt, or keep existing
    let db_password = if let Some(ref pw_path) = db_password_file_arg {
        std::fs::read_to_string(pw_path)
            .map_err(|e| anyhow::anyhow!("Failed to read password file '{pw_path}': {e}"))?
            .trim()
            .to_string()
    } else if is_interactive {
        if let Some(existing_pw) = existing_password {
            let keep = Select::new()
                .with_prompt("  Password")
                .items(&["Keep current password", "Enter new password"])
                .default(0)
                .interact()
                .map_err(|e| anyhow::anyhow!("Failed to read selection: {e}"))?;
            if keep == 0 {
                existing_pw
            } else {
                Password::new()
                    .with_prompt("  New password")
                    .interact()
                    .map_err(|e| anyhow::anyhow!("Failed to read password: {e}"))?
            }
        } else {
            Password::new()
                .with_prompt("  Password")
                .interact()
                .map_err(|e| anyhow::anyhow!("Failed to read password: {e}"))?
        }
    } else if let Some(pw) = existing_password {
        pw
    } else {
        anyhow::bail!("--db-password-file is required in non-interactive mode");
    };

    if db_password.is_empty() {
        anyhow::bail!("Password cannot be empty");
    }

    // SSL mode: from flag, interactive prompt, or keep existing
    let existing_ssl = existing.as_ref().and_then(|c| c.db_ssl_mode.clone());

    let db_ssl_mode = match db_ssl_mode_arg {
        Some(m) => m,
        None if is_interactive => {
            eprintln!();
            let labels: Vec<String> = SSL_OPTIONS
                .iter()
                .map(|(mode, desc)| format!("{mode} — {desc}"))
                .collect();
            let default_idx = existing_ssl
                .as_ref()
                .and_then(|m| SSL_OPTIONS.iter().position(|(mode, _)| mode == m))
                .unwrap_or(0);
            let selection = Select::new()
                .with_prompt("  SSL mode")
                .items(&labels)
                .default(default_idx)
                .interact()
                .map_err(|e| anyhow::anyhow!("Failed to read SSL mode: {e}"))?;
            SSL_OPTIONS[selection].0.to_string()
        }
        None => existing_ssl.unwrap_or_else(|| "prefer".to_string()),
    };

    // -----------------------------------------------------------------------
    // Step 6: Test database connection
    // -----------------------------------------------------------------------
    eprintln!();
    eprint!("  Testing database connection... ");

    let test_result = test_database_connection(
        &peek.db,
        &db_host,
        db_port,
        &db_name,
        &db_user,
        &db_password,
        &db_ssl_mode,
    )
    .await;

    match test_result {
        Ok(()) => {
            eprintln!("ok");
        }
        Err(e) => {
            eprintln!("failed");
            eprintln!();
            // Extract the root cause from nested error chains
            let msg = e.to_string();
            let display_msg = msg.rsplit_once(": ").map(|(_, root)| root).unwrap_or(&msg);
            eprintln!("  Error: {display_msg}");

            if !is_interactive {
                anyhow::bail!("Database connection test failed: {display_msg}");
            }

            eprintln!();
            eprintln!("  Check your database settings and try again.");
            eprintln!("  The config will be saved anyway — you can edit it later.");
            eprintln!();
        }
    }

    // -----------------------------------------------------------------------
    // Step 7: Save config
    // -----------------------------------------------------------------------

    // Save password to separate file first so we can reference the path
    let password_path = ConfigFile::save_password(&db_password)?;

    let config_file = ConfigFile {
        token: token.clone(),
        db_host: db_host.clone(),
        db_port,
        db_name: db_name.clone(),
        db_user: db_user.clone(),
        db_password_file: Some(password_path.display().to_string()),
        db_ssl_mode: Some(db_ssl_mode),
        db_ssl_ca: None,
        health_port: None,
    };

    let config_path = config_file.save()?;

    eprintln!();
    eprintln!("  Config saved to:    {}", config_path.display());
    eprintln!("  Password saved to:  {}", password_path.display());

    // -----------------------------------------------------------------------
    // Step 8: Done — agent will start automatically
    // -----------------------------------------------------------------------
    eprintln!();
    eprintln!("  Setup complete!");
    eprintln!();

    Ok(())
}

/// Test a database connection with the given credentials.
/// Returns Ok(()) if the connection succeeds, Err with a user-friendly message otherwise.
async fn test_database_connection(
    db_type: &str,
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
    ssl_mode: &str,
) -> anyhow::Result<()> {
    let ds_type = kyomi_connect_protocol::DatasourceType::from_str(db_type)
        .map_err(|e| anyhow::anyhow!("Unsupported database type '{db_type}': {e}"))?;

    let connection_config = serde_json::json!({
        "host": host,
        "port": port,
        "database": database,
        "ssl_mode": ssl_mode,
    });

    let credentials = serde_json::json!({
        "username": username,
        "password": password,
    });

    let provider =
        kyomi_datasource::create_provider(&ds_type, &connection_config, &credentials, None).await?;

    provider
        .test_connection()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peek_token_safe_returns_none_for_garbage() {
        assert!(peek_token_safe("not-a-jwt").is_none());
    }

    #[test]
    fn peek_token_safe_returns_none_for_empty() {
        assert!(peek_token_safe("").is_none());
    }

    #[test]
    fn resolve_token_from_arg() {
        let result = resolve_token(Some("my-token".to_string()), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "my-token");
    }

    #[test]
    fn resolve_token_empty_arg_errors() {
        let result = resolve_token(Some("".to_string()), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn resolve_token_from_file() {
        let tmp =
            std::env::temp_dir().join(format!("kyomi-connect-test-token-{}", std::process::id()));
        std::fs::write(&tmp, "  file-token  \n").unwrap();

        let result = resolve_token(None, Some(tmp.display().to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "file-token");

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn resolve_token_missing_file_errors() {
        let result = resolve_token(None, Some("/nonexistent/path/to/token".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn resolve_token_empty_file_errors() {
        let tmp = std::env::temp_dir().join(format!(
            "kyomi-connect-test-empty-token-{}",
            std::process::id()
        ));
        std::fs::write(&tmp, "  \n").unwrap();

        let result = resolve_token(None, Some(tmp.display().to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn resolve_token_arg_takes_priority_over_file() {
        let result = resolve_token(
            Some("arg-token".to_string()),
            Some("/some/file".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "arg-token");
    }
}
