//! Provider factory — creates concrete [`DatasourceProvider`] instances.
//!
//! The factory matches on [`DatasourceType`] and constructs the appropriate
//! provider with resolved credentials and connection configuration.
//!
//! ## Phase 6B Providers (sqlx-based)
//! - PostgreSQL — via `sqlx::PgPool`
//! - MySQL — via `sqlx::MySqlPool`
//! - Redshift — via `sqlx::PgPool` (Postgres-compatible)
//!
//! ## Phase 6C Providers (HTTP/REST-based)
//! - ClickHouse — via HTTP REST API
//! - Snowflake — via REST API (`/api/v2/statements`)
//! - Databricks — via SQL Statement Execution API
//!
//! ## Phase 6D Providers (TDS-based)
//! - SQL Server — via `tiberius` (TDS protocol)
//! - Azure Synapse — via `tiberius` (TDS protocol)
//!
//! ## Phase 6E Providers (REST/OAuth-based)
//! - BigQuery — via REST API (3 auth modes: kyomi_oauth, enterprise_oauth, service_account)

use kyomi_connect_protocol::DatasourceType;

use crate::provider::DatasourceProvider;

#[cfg(feature = "postgres")]
use crate::providers::postgres::PostgresProvider;
#[cfg(feature = "mysql")]
use crate::providers::mysql::MySqlProvider;
#[cfg(feature = "redshift")]
use crate::providers::redshift::RedshiftProvider;
#[cfg(feature = "clickhouse")]
use crate::providers::clickhouse::ClickHouseProvider;
#[cfg(feature = "snowflake")]
use crate::providers::snowflake::SnowflakeProvider;
#[cfg(feature = "databricks")]
use crate::providers::databricks::DatabricksProvider;
#[cfg(feature = "sqlserver")]
use crate::providers::sqlserver::SqlServerProvider;
#[cfg(feature = "synapse")]
use crate::providers::synapse::SynapseProvider;
#[cfg(feature = "bigquery")]
use crate::providers::bigquery::BigQueryProvider;

// ---------------------------------------------------------------------------
// UserContext — additional context for BigQuery OAuth
// ---------------------------------------------------------------------------

/// Additional user context needed by BigQuery providers.
///
/// BigQuery's `kyomi_oauth` auth mode requires the user's OAuth data
/// (stored in `User.oauth_data`), which is not part of the datasource
/// credential system.
#[derive(Debug, Clone)]
pub struct UserContext {
    /// User's OAuth data (from `User.oauth_data` column).
    /// Contains `access_token`, `refresh_token`, etc. for Google OAuth.
    pub oauth_data: Option<serde_json::Value>,
    /// User's email address.
    pub user_email: String,
    /// Current workspace ID.
    pub workspace_id: String,
}

// ---------------------------------------------------------------------------
// Shared credential resolution
// ---------------------------------------------------------------------------

/// Resolve credentials from connection config when shared credentials are enabled.
///
/// When `shared_credentials` is `true` in the connection config, the username
/// and password are stored in the connection config itself (as `shared_username`
/// and `shared_password`) rather than in per-user credential records.
///
/// Returns the original credentials if shared credentials are not enabled,
/// or a new JSON object with the shared credentials merged in.
pub fn resolve_shared_credentials(
    connection_config: &serde_json::Value,
    credentials: &serde_json::Value,
) -> serde_json::Value {
    let shared = connection_config
        .get("shared_credentials")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if !shared {
        return credentials.clone();
    }

    // Start with the existing credentials, ensuring it's a JSON object
    let mut resolved = if credentials.is_object() {
        credentials.clone()
    } else {
        serde_json::json!({})
    };

    let Some(obj) = resolved.as_object_mut() else {
        return credentials.clone();
    };

    // Copy shared_username -> username
    if let Some(username) = connection_config.get("shared_username")
        && username.is_string()
        && !username.as_str().unwrap_or("").is_empty()
    {
        obj.insert("username".into(), username.clone());
    }

    // Copy shared_password -> password
    if let Some(password) = connection_config.get("shared_password")
        && password.is_string()
        && !password.as_str().unwrap_or("").is_empty()
    {
        obj.insert("password".into(), password.clone());
    }

    resolved
}

// ---------------------------------------------------------------------------
// Provider factory
// ---------------------------------------------------------------------------

/// Create a [`DatasourceProvider`] for the given datasource type.
///
/// This is the main entry point for creating provider instances. It:
/// 1. Resolves shared credentials if applicable.
/// 2. Matches on the datasource type to construct the appropriate provider.
/// 3. Returns a boxed trait object for use by the query execution layer.
///
/// # Arguments
/// * `ds_type` - The datasource type to create a provider for.
/// * `connection_config` - Datasource-level configuration from `DatasourceConfig`.
/// * `credentials` - Decrypted user-level credentials.
/// * `user_context` - Optional additional context (required for BigQuery).
///
/// # Errors
/// Returns an error if the provider type is not yet implemented (Phase 6E)
/// or if the configuration is invalid.
pub async fn create_provider(
    ds_type: &DatasourceType,
    connection_config: &serde_json::Value,
    credentials: &serde_json::Value,
    #[cfg_attr(not(feature = "bigquery"), allow(unused_variables))]
    user_context: Option<&UserContext>,
) -> kyomi_connect_protocol::Result<Box<dyn DatasourceProvider>> {
    // Resolve shared credentials before passing to provider constructors.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "redshift",
              feature = "clickhouse", feature = "snowflake", feature = "databricks",
              feature = "sqlserver", feature = "synapse", feature = "bigquery"))]
    let resolved_credentials = resolve_shared_credentials(connection_config, credentials);
    #[cfg(not(any(feature = "postgres", feature = "mysql", feature = "redshift",
                  feature = "clickhouse", feature = "snowflake", feature = "databricks",
                  feature = "sqlserver", feature = "synapse", feature = "bigquery")))]
    let _ = (connection_config, credentials);

    match ds_type {
        #[cfg(feature = "postgres")]
        DatasourceType::Postgres => {
            let provider = PostgresProvider::new(connection_config, &resolved_credentials).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "postgres"))]
        DatasourceType::Postgres => Err(kyomi_connect_protocol::Error::NotSupported(
            "PostgreSQL provider is not enabled (feature 'postgres')".into(),
        )),

        #[cfg(feature = "mysql")]
        DatasourceType::MySQL => {
            let provider = MySqlProvider::new(connection_config, &resolved_credentials).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "mysql"))]
        DatasourceType::MySQL => Err(kyomi_connect_protocol::Error::NotSupported(
            "MySQL provider is not enabled (feature 'mysql')".into(),
        )),

        #[cfg(feature = "redshift")]
        DatasourceType::Redshift => {
            let provider = RedshiftProvider::new(connection_config, &resolved_credentials).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "redshift"))]
        DatasourceType::Redshift => Err(kyomi_connect_protocol::Error::NotSupported(
            "Redshift provider is not enabled (feature 'redshift')".into(),
        )),

        #[cfg(feature = "clickhouse")]
        DatasourceType::ClickHouse => {
            let provider = ClickHouseProvider::new(connection_config, &resolved_credentials).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "clickhouse"))]
        DatasourceType::ClickHouse => Err(kyomi_connect_protocol::Error::NotSupported(
            "ClickHouse provider is not enabled (feature 'clickhouse')".into(),
        )),

        #[cfg(feature = "snowflake")]
        DatasourceType::Snowflake => {
            let provider = SnowflakeProvider::new(connection_config, &resolved_credentials).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "snowflake"))]
        DatasourceType::Snowflake => Err(kyomi_connect_protocol::Error::NotSupported(
            "Snowflake provider is not enabled (feature 'snowflake')".into(),
        )),

        #[cfg(feature = "databricks")]
        DatasourceType::Databricks => {
            let provider = DatabricksProvider::new(connection_config, &resolved_credentials).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "databricks"))]
        DatasourceType::Databricks => Err(kyomi_connect_protocol::Error::NotSupported(
            "Databricks provider is not enabled (feature 'databricks')".into(),
        )),

        #[cfg(feature = "sqlserver")]
        DatasourceType::SqlServer => {
            let provider =
                SqlServerProvider::new(connection_config, &resolved_credentials).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "sqlserver"))]
        DatasourceType::SqlServer => Err(kyomi_connect_protocol::Error::NotSupported(
            "SQL Server provider is not enabled (feature 'sqlserver')".into(),
        )),

        #[cfg(feature = "synapse")]
        DatasourceType::Synapse => {
            let provider =
                SynapseProvider::new(connection_config, &resolved_credentials).await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "synapse"))]
        DatasourceType::Synapse => Err(kyomi_connect_protocol::Error::NotSupported(
            "Azure Synapse provider is not enabled (feature 'synapse')".into(),
        )),

        #[cfg(feature = "bigquery")]
        DatasourceType::BigQuery => {
            let provider =
                BigQueryProvider::new(connection_config, &resolved_credentials, user_context)
                    .await?;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "bigquery"))]
        DatasourceType::BigQuery => Err(kyomi_connect_protocol::Error::NotSupported(
            "BigQuery provider is not enabled (feature 'bigquery')".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_shared_credentials_when_not_shared() {
        let config = serde_json::json!({
            "host": "localhost",
            "port": 5432,
        });
        let creds = serde_json::json!({
            "username": "user1",
            "password": "pass1",
        });
        let resolved = resolve_shared_credentials(&config, &creds);
        assert_eq!(resolved["username"], "user1");
        assert_eq!(resolved["password"], "pass1");
    }

    #[test]
    fn resolve_shared_credentials_when_shared() {
        let config = serde_json::json!({
            "host": "localhost",
            "port": 5432,
            "shared_credentials": true,
            "shared_username": "shared_user",
            "shared_password": "shared_pass",
        });
        let creds = serde_json::json!({});
        let resolved = resolve_shared_credentials(&config, &creds);
        assert_eq!(resolved["username"], "shared_user");
        assert_eq!(resolved["password"], "shared_pass");
    }

    #[test]
    fn resolve_shared_credentials_shared_overrides_existing() {
        let config = serde_json::json!({
            "shared_credentials": true,
            "shared_username": "shared_user",
            "shared_password": "shared_pass",
        });
        let creds = serde_json::json!({
            "username": "original_user",
            "password": "original_pass",
        });
        let resolved = resolve_shared_credentials(&config, &creds);
        assert_eq!(resolved["username"], "shared_user");
        assert_eq!(resolved["password"], "shared_pass");
    }

    #[test]
    fn resolve_shared_credentials_shared_flag_false() {
        let config = serde_json::json!({
            "shared_credentials": false,
            "shared_username": "shared_user",
            "shared_password": "shared_pass",
        });
        let creds = serde_json::json!({
            "username": "user1",
            "password": "pass1",
        });
        let resolved = resolve_shared_credentials(&config, &creds);
        assert_eq!(resolved["username"], "user1");
        assert_eq!(resolved["password"], "pass1");
    }

    #[test]
    fn resolve_shared_credentials_empty_shared_values_not_applied() {
        let config = serde_json::json!({
            "shared_credentials": true,
            "shared_username": "",
            "shared_password": "",
        });
        let creds = serde_json::json!({
            "username": "user1",
        });
        let resolved = resolve_shared_credentials(&config, &creds);
        // Empty shared values should not overwrite existing
        assert_eq!(resolved["username"], "user1");
    }

    #[cfg(feature = "bigquery")]
    #[tokio::test]
    async fn create_provider_bigquery_requires_auth() {
        // BigQuery without proper auth should fail with a meaningful error.
        let config = serde_json::json!({
            "auth_mode": "enterprise_oauth",
        });
        let creds = serde_json::json!({});

        let result = create_provider(&DatasourceType::BigQuery, &config, &creds, None).await;
        assert!(result.is_err(), "Expected error for BigQuery without credentials");
        let err_msg = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("Expected error for BigQuery without credentials"),
        };
        assert!(
            err_msg.contains("oauth_access_token"),
            "Error should mention missing oauth_access_token, got: {err_msg}"
        );
    }
}
