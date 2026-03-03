//! kyomi-datasource — Database provider implementations for Kyomi Connect.
//!
//! This crate contains the [`DatasourceProvider`] trait, result types, type
//! mappings, and concrete provider implementations for all 9 supported
//! datasource types (PostgreSQL, MySQL, Redshift, ClickHouse, Snowflake,
//! Databricks, SQL Server, Azure Synapse, BigQuery).
//!
//! Each provider can be independently enabled via Cargo feature flags.
//!
//! ## Architecture
//!
//! - **`provider`** — The async [`DatasourceProvider`] trait and result types
//!   ([`QueryResult`], [`DryRunResult`], [`ColumnInfo`], [`SimpleType`]).
//! - **`type_mapping`** — Functions to map provider-specific types (OIDs, type
//!   codes, type names) to the unified [`SimpleType`] enum.
//! - **`factory`** — The [`create_provider`] function that constructs concrete
//!   providers from datasource configuration and credentials.

use std::time::Duration;

pub mod factory;
pub mod oauth_refresh;
pub mod provider;
pub mod providers;
#[cfg(feature = "ssh")]
pub mod ssh_tunnel;
pub mod stream;
pub mod type_mapping;

// ---------------------------------------------------------------------------
// Re-exports — convenient access to key types
// ---------------------------------------------------------------------------

pub use factory::{UserContext, create_provider, resolve_shared_credentials};
pub use oauth_refresh::ensure_valid_oauth_credentials;
pub use provider::{
    ColumnInfo, DatasourceProvider, DiscoveryResult, DryRunResult, QueryResult, QueryStatus,
    SimpleType,
};
pub use stream::{collect_stream_to_result, query_result_to_stream};

// ---------------------------------------------------------------------------
// Timeout constants
// ---------------------------------------------------------------------------

/// Timeout for establishing a connection during test-connection operations.
pub const DATASOURCE_TIMEOUT_CONNECT: Duration = Duration::from_secs(30);

/// Timeout for query execution.
pub const DATASOURCE_TIMEOUT_QUERY: Duration = Duration::from_secs(120);

/// Timeout for dry-run / SQL validation operations.
pub const DATASOURCE_TIMEOUT_DRY_RUN: Duration = Duration::from_secs(30);

/// Timeout for OAuth token refresh HTTP requests.
pub const OAUTH_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// Build a shared HTTP client with a proper User-Agent header.
///
/// Some APIs (notably Snowflake) reject requests without a User-Agent.
/// All HTTP clients in this crate should use this function.
pub fn http_client() -> kyomi_connect_protocol::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("Kyomi/1.0")
        .build()
        .map_err(|e| {
            kyomi_connect_protocol::Error::Internal(format!("Failed to build HTTP client: {e}"))
        })
}
