use crate::config_file::ConfigFile;
use serde::Deserialize;

/// Configuration for the Connect binary.
/// Loaded from environment variables + verified JWT payload.
pub struct ConnectConfig {
    /// The raw JWT token (from KYOMI_TOKEN env var)
    pub token: String,
    /// Database type from JWT payload (e.g., "postgres", "mysql")
    pub db_type: String,
    /// WebSocket URL from JWT payload (e.g., "wss://api.kyomi.ai/connect/v1")
    pub ws_url: String,
    /// Database connection details from env vars
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
    pub db_ssl_mode: Option<String>,
    pub db_ssl_ca: Option<String>,
    /// Health check port (default 9090)
    pub health_port: u16,
}

/// JWT claims from Kyomi-issued Connect token
#[derive(Debug, Deserialize)]
struct ConnectClaims {
    /// Database type (e.g., "postgres")
    db: String,
    /// WebSocket URL
    url: String,
    /// Issuer
    iss: String,
}

impl ConnectConfig {
    /// Load config from environment, with config file fallback.
    ///
    /// 1. Reads KYOMI_TOKEN (env var -> config file -> error)
    /// 2. Decodes JWT header (without verification) to find the issuer
    /// 3. Fetches JWKS from {issuer}/.well-known/jwks.json
    /// 4. Verifies JWT signature using ES256 public key
    /// 5. Extracts db_type and ws_url from verified payload
    /// 6. Reads DB_* (env vars -> _FILE variants -> config file)
    pub async fn from_env() -> anyhow::Result<Self> {
        // Load config file as fallback (None if no file exists)
        let file_config = ConfigFile::load();

        let token = std::env::var("KYOMI_TOKEN")
            .or_else(|_| {
                file_config
                    .as_ref()
                    .map(|cf| cf.token.clone())
                    .ok_or(std::env::VarError::NotPresent)
            })
            .map_err(|_| {
                anyhow::anyhow!(
                    "KYOMI_TOKEN not set. Run 'kyomi-connect setup' to configure, \
                     or set KYOMI_TOKEN env var."
                )
            })?;

        // Decode JWT header to get kid for JWKS lookup
        let header = jsonwebtoken::decode_header(&token)
            .map_err(|e| anyhow::anyhow!("Invalid JWT token: {e}"))?;

        // Peek at claims to get issuer for JWKS URL (without verifying signature yet)
        let unsafe_claims: ConnectClaims = {
            let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::ES256);
            validation.insecure_disable_signature_validation();
            validation.set_required_spec_claims::<&str>(&[]);
            let data = jsonwebtoken::decode::<ConnectClaims>(
                &token,
                &jsonwebtoken::DecodingKey::from_secret(&[]),
                &validation,
            )?;
            data.claims
        };

        // Fetch JWKS from the issuer's well-known endpoint
        let jwks_url = format!(
            "{}/.well-known/jwks.json",
            unsafe_claims.iss.trim_end_matches('/')
        );
        tracing::info!(jwks_url = %jwks_url, "Fetching JWKS for token verification");

        let jwks_response = reqwest::get(&jwks_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch JWKS from {jwks_url}: {e}"))?;
        let jwks: serde_json::Value = jwks_response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse JWKS response: {e}"))?;

        // Find the matching key by kid
        let kid = header
            .kid
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("JWT has no 'kid' header"))?;

        let keys = jwks
            .get("keys")
            .and_then(|k| k.as_array())
            .ok_or_else(|| anyhow::anyhow!("JWKS has no 'keys' array"))?;

        let jwk = keys
            .iter()
            .find(|k| k.get("kid").and_then(|v| v.as_str()) == Some(kid))
            .ok_or_else(|| anyhow::anyhow!("No matching key found in JWKS for kid '{kid}'"))?;

        // Build decoding key from JWK (EC P-256)
        let x = jwk
            .get("x")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("JWK missing 'x' coordinate"))?;
        let y = jwk
            .get("y")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("JWK missing 'y' coordinate"))?;

        // Construct the EC public key components for jsonwebtoken
        let ec_key = serde_json::json!({
            "kty": "EC",
            "crv": "P-256",
            "x": x,
            "y": y,
        });
        let decoding_key = jsonwebtoken::DecodingKey::from_jwk(&serde_json::from_value(ec_key)?)?;

        // Now verify the token properly
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::ES256);
        validation.validate_exp = false; // Connect tokens don't expire (revoked via jti)
        validation.set_required_spec_claims::<&str>(&[]);
        let token_data = jsonwebtoken::decode::<ConnectClaims>(&token, &decoding_key, &validation)
            .map_err(|e| anyhow::anyhow!("JWT verification failed: {e}"))?;

        let claims = token_data.claims;
        tracing::info!(
            db_type = %claims.db,
            "JWT verified successfully"
        );

        // Read database connection env vars, falling back to config file
        let db_host = read_env_or_file("DB_HOST")
            .or_else(|| file_config.as_ref().map(|cf| cf.db_host.clone()))
            .ok_or_else(|| anyhow::anyhow!("DB_HOST (or DB_HOST_FILE) is required"))?;
        let db_port: u16 = read_env_or_file("DB_PORT")
            .or_else(|| file_config.as_ref().map(|cf| cf.db_port.to_string()))
            .unwrap_or_else(|| default_port(&claims.db).to_string())
            .parse()
            .map_err(|_| anyhow::anyhow!("DB_PORT must be a valid port number"))?;
        let db_user = read_env_or_file("DB_USER")
            .or_else(|| file_config.as_ref().map(|cf| cf.db_user.clone()))
            .ok_or_else(|| anyhow::anyhow!("DB_USER (or DB_USER_FILE) is required"))?;
        let db_password = read_env_or_file("DB_PASSWORD")
            .or_else(|| {
                // Config file stores a password file path, not the password itself
                file_config
                    .as_ref()
                    .and_then(|cf| cf.db_password_file.as_ref())
                    .and_then(|path| std::fs::read_to_string(path).ok())
                    .map(|s| s.trim().to_string())
            })
            .ok_or_else(|| anyhow::anyhow!("DB_PASSWORD (or DB_PASSWORD_FILE) is required"))?;
        let db_name = read_env_or_file("DB_NAME")
            .or_else(|| file_config.as_ref().map(|cf| cf.db_name.clone()))
            .ok_or_else(|| anyhow::anyhow!("DB_NAME (or DB_NAME_FILE) is required"))?;
        let db_ssl_mode = read_env_or_file("DB_SSL_MODE")
            .or_else(|| file_config.as_ref().and_then(|cf| cf.db_ssl_mode.clone()));
        let db_ssl_ca = read_env_or_file("DB_SSL_CA")
            .or_else(|| file_config.as_ref().and_then(|cf| cf.db_ssl_ca.clone()));
        let health_port: u16 = read_env_or_file("HEALTH_PORT")
            .or_else(|| {
                file_config
                    .as_ref()
                    .and_then(|cf| cf.health_port.map(|p| p.to_string()))
            })
            .unwrap_or_else(|| "9090".to_string())
            .parse()
            .map_err(|_| anyhow::anyhow!("HEALTH_PORT must be a valid port number"))?;

        Ok(Self {
            token,
            db_type: claims.db,
            ws_url: claims.url,
            db_host,
            db_port,
            db_user,
            db_password,
            db_name,
            db_ssl_mode,
            db_ssl_ca,
            health_port,
        })
    }

    /// Build the connection_config JSON that kyomi-datasource providers expect.
    pub fn connection_config(&self) -> serde_json::Value {
        // Default to "prefer" — try TLS first, fall back to unencrypted.
        // This is the right default for Connect where users often connect to
        // local databases without TLS. The provider default is "require" which
        // fails on databases that don't support TLS.
        let ssl_mode = self.db_ssl_mode.as_deref().unwrap_or("prefer");

        let mut config = serde_json::json!({
            "host": self.db_host,
            "port": self.db_port,
            "database": self.db_name,
            "ssl_mode": ssl_mode,
        });
        if let Some(ref ssl_ca) = self.db_ssl_ca {
            config["ssl_ca"] = serde_json::json!(ssl_ca);
        }
        config
    }

    /// Build the credentials JSON that kyomi-datasource providers expect.
    pub fn credentials(&self) -> serde_json::Value {
        serde_json::json!({
            "username": self.db_user,
            "password": self.db_password,
        })
    }
}

/// Read an environment variable, falling back to reading from a file
/// (Docker secrets pattern: DB_PASSWORD_FILE=/run/secrets/db_password).
fn read_env_or_file(name: &str) -> Option<String> {
    if let Ok(val) = std::env::var(name) {
        return Some(val);
    }
    if let Ok(path) = std::env::var(format!("{name}_FILE")) {
        return Some(std::fs::read_to_string(&path).ok()?.trim().to_string());
    }
    None
}

/// Default database port for each datasource type.
pub(crate) fn default_port(db_type: &str) -> u16 {
    match db_type {
        "postgres" | "redshift" => 5432,
        "mysql" => 3306,
        "clickhouse" => 8123,
        "sqlserver" | "synapse" => 1433,
        "snowflake" => 443,
        "databricks" => 443,
        _ => 5432,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_env_or_file_from_env() {
        // SAFETY: test-only, single-threaded
        unsafe { std::env::set_var("TEST_CONFIG_VAR", "hello") };
        assert_eq!(read_env_or_file("TEST_CONFIG_VAR"), Some("hello".into()));
        unsafe { std::env::remove_var("TEST_CONFIG_VAR") };
    }

    #[test]
    fn read_env_or_file_missing() {
        assert_eq!(read_env_or_file("NONEXISTENT_VAR_12345"), None);
    }

    #[test]
    fn default_port_postgres() {
        assert_eq!(default_port("postgres"), 5432);
    }

    #[test]
    fn default_port_mysql() {
        assert_eq!(default_port("mysql"), 3306);
    }

    #[test]
    fn default_port_clickhouse() {
        assert_eq!(default_port("clickhouse"), 8123);
    }

    #[test]
    fn default_port_unknown_defaults_to_5432() {
        assert_eq!(default_port("unknown_db"), 5432);
    }

    #[test]
    fn connection_config_json() {
        let config = ConnectConfig {
            token: "t".into(),
            db_type: "postgres".into(),
            ws_url: "wss://api.kyomi.ai/connect/v1".into(),
            db_host: "localhost".into(),
            db_port: 5432,
            db_user: "user".into(),
            db_password: "pass".into(),
            db_name: "mydb".into(),
            db_ssl_mode: Some("require".into()),
            db_ssl_ca: None,
            health_port: 9090,
        };
        let json = config.connection_config();
        assert_eq!(json["host"], "localhost");
        assert_eq!(json["port"], 5432);
        assert_eq!(json["database"], "mydb");
        assert_eq!(json["ssl_mode"], "require");
    }

    #[test]
    fn credentials_json() {
        let config = ConnectConfig {
            token: "t".into(),
            db_type: "postgres".into(),
            ws_url: "wss://api.kyomi.ai/connect/v1".into(),
            db_host: "localhost".into(),
            db_port: 5432,
            db_user: "admin".into(),
            db_password: "secret".into(),
            db_name: "mydb".into(),
            db_ssl_mode: None,
            db_ssl_ca: None,
            health_port: 9090,
        };
        let json = config.credentials();
        assert_eq!(json["username"], "admin");
        assert_eq!(json["password"], "secret");
    }

    #[test]
    fn connection_config_defaults_ssl_to_prefer() {
        let config = ConnectConfig {
            token: "t".into(),
            db_type: "postgres".into(),
            ws_url: "wss://connect.kyomi.ai/v1".into(),
            db_host: "localhost".into(),
            db_port: 5432,
            db_user: "user".into(),
            db_password: "pass".into(),
            db_name: "mydb".into(),
            db_ssl_mode: None,
            db_ssl_ca: None,
            health_port: 9090,
        };
        let json = config.connection_config();
        assert_eq!(json["ssl_mode"], "prefer");
    }
}
