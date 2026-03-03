//! Concrete datasource provider implementations.
//!
//! Each module implements [`crate::DatasourceProvider`] for a specific database
//! engine. Providers are constructed by the [`crate::factory::create_provider`]
//! function.
//!
//! All providers are feature-gated — only enabled providers are compiled.

// Shared modules — conditionally compiled based on which providers are enabled
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "redshift",
    feature = "clickhouse",
    feature = "snowflake",
    feature = "databricks",
    feature = "bigquery",
    feature = "sqlserver",
    feature = "synapse",
))]
pub(crate) mod sqlx_common;

#[cfg(any(feature = "sqlserver", feature = "synapse"))]
pub(crate) mod tsql_common;

#[cfg(feature = "redshift")]
pub(crate) mod aws_sigv4;

// Provider modules
#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "redshift")]
pub mod redshift;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;

#[cfg(feature = "snowflake")]
pub mod snowflake;

#[cfg(feature = "databricks")]
pub mod databricks;

#[cfg(feature = "sqlserver")]
pub mod sqlserver;

#[cfg(feature = "synapse")]
pub mod synapse;

#[cfg(feature = "bigquery")]
pub mod bigquery;
