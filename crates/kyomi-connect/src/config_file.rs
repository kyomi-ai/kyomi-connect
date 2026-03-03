use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigFile {
    pub token: String,
    pub db_host: String,
    pub db_port: u16,
    pub db_name: String,
    pub db_user: String,
    /// Path to file containing password (not the password itself)
    pub db_password_file: Option<String>,
    pub db_ssl_mode: Option<String>,
    pub db_ssl_ca: Option<String>,
    pub health_port: Option<u16>,
}

impl ConfigFile {
    /// Return the default config directory path.
    pub fn default_config_dir() -> anyhow::Result<PathBuf> {
        dirs::config_dir()
            .map(|d| d.join("kyomi-connect"))
            .ok_or_else(|| anyhow::anyhow!("Could not determine config directory"))
    }

    /// Standard config file paths, in priority order.
    pub fn config_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if let Some(config_dir) = dirs::config_dir() {
            paths.push(config_dir.join("kyomi-connect").join("config.toml"));
        }
        paths.push(PathBuf::from("/etc/kyomi-connect/config.toml"));
        paths
    }

    /// Load config from the first existing file in standard paths.
    pub fn load() -> Option<Self> {
        for path in Self::config_paths() {
            if path.exists() {
                let content = std::fs::read_to_string(&path).ok()?;
                let config: Self = toml::from_str(&content).ok()?;
                tracing::info!(path = %path.display(), "Loaded config file");
                return Some(config);
            }
        }
        None
    }

    /// Save config to a specific directory.
    pub fn save_to(&self, config_dir: &std::path::Path) -> anyhow::Result<PathBuf> {
        std::fs::create_dir_all(config_dir)?;
        let path = config_dir.join("config.toml");
        let content = toml::to_string_pretty(self)?;
        std::fs::write(&path, &content)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }

        Ok(path)
    }

    /// Save config to the default user config directory.
    pub fn save(&self) -> anyhow::Result<PathBuf> {
        self.save_to(&Self::default_config_dir()?)
    }

    /// Save the database password to a separate file in a specific directory.
    pub fn save_password_to(
        config_dir: &std::path::Path,
        password: &str,
    ) -> anyhow::Result<PathBuf> {
        std::fs::create_dir_all(config_dir)?;
        let path = config_dir.join(".db-password");
        std::fs::write(&path, password)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }

        Ok(path)
    }

    /// Save the database password to the default user config directory.
    pub fn save_password(password: &str) -> anyhow::Result<PathBuf> {
        Self::save_password_to(&Self::default_config_dir()?, password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ConfigFile {
        ConfigFile {
            token: "eyJ...".to_string(),
            db_host: "localhost".to_string(),
            db_port: 5432,
            db_name: "mydb".to_string(),
            db_user: "user".to_string(),
            db_password_file: Some("/etc/kyomi-connect/.db-password".to_string()),
            db_ssl_mode: None,
            db_ssl_ca: None,
            health_port: Some(9090),
        }
    }

    #[test]
    fn round_trip_toml() {
        let config = test_config();
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let loaded: ConfigFile = toml::from_str(&toml_str).unwrap();
        assert_eq!(loaded.token, config.token);
        assert_eq!(loaded.db_host, config.db_host);
        assert_eq!(loaded.db_port, config.db_port);
        assert_eq!(loaded.db_name, config.db_name);
        assert_eq!(loaded.db_user, config.db_user);
        assert_eq!(loaded.db_password_file, config.db_password_file);
        assert_eq!(loaded.db_ssl_mode, config.db_ssl_mode);
        assert_eq!(loaded.db_ssl_ca, config.db_ssl_ca);
        assert_eq!(loaded.health_port, config.health_port);
    }

    #[test]
    fn default_config_dir_returns_path() {
        let result = ConfigFile::default_config_dir();
        assert!(result.is_ok());
        let path = result.unwrap();
        assert!(path.ends_with("kyomi-connect"));
    }

    #[test]
    fn save_to_writes_config_and_sets_permissions() {
        let tmp = std::env::temp_dir().join(format!(
            "kyomi-connect-test-save-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);

        let config = test_config();
        let path = config.save_to(&tmp).unwrap();

        assert!(path.exists());
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains("token = \"eyJ...\""));
        assert!(written.contains("db_host = \"localhost\""));
        assert!(written.contains("db_port = 5432"));

        // Verify round-trip through file
        let loaded: ConfigFile = toml::from_str(&written).unwrap();
        assert_eq!(loaded.token, config.token);
        assert_eq!(loaded.db_name, config.db_name);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::metadata(&path).unwrap().permissions();
            assert_eq!(perms.mode() & 0o777, 0o600);
        }

        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn save_password_to_writes_file_and_sets_permissions() {
        let tmp = std::env::temp_dir().join(format!(
            "kyomi-connect-test-pw-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);

        let path = ConfigFile::save_password_to(&tmp, "my_secret_password").unwrap();

        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "my_secret_password");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::metadata(&path).unwrap().permissions();
            assert_eq!(perms.mode() & 0o777, 0o600);
        }

        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn save_writes_to_temp_dir() {
        // NEVER use save() (default dir) in tests — it overwrites real user config!
        let tmp = std::env::temp_dir().join(format!(
            "kyomi-connect-test-default-save-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);

        let config = test_config();
        let path = config.save_to(&tmp).unwrap();
        assert!(path.exists());
        assert!(path.ends_with("config.toml"));

        // Verify content
        let content = std::fs::read_to_string(&path).unwrap();
        let loaded: ConfigFile = toml::from_str(&content).unwrap();
        assert_eq!(loaded.token, config.token);

        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn save_password_writes_to_temp_dir() {
        // NEVER use save_password() (default dir) in tests — it overwrites real user data!
        let tmp = std::env::temp_dir().join(format!(
            "kyomi-connect-test-default-pw-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);

        let path = ConfigFile::save_password_to(&tmp, "test_password_for_unit_test").unwrap();
        assert!(path.exists());
        assert!(path.ends_with(".db-password"));

        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "test_password_for_unit_test");

        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn config_paths_not_empty() {
        let paths = ConfigFile::config_paths();
        assert!(!paths.is_empty());
        assert_eq!(
            paths.last().unwrap(),
            &PathBuf::from("/etc/kyomi-connect/config.toml")
        );
    }
}
