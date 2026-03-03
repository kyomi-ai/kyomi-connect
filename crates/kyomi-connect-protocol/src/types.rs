//! Datasource type enum — the canonical list of supported datasource types.
//!
//! Extracted from `kyomi-core::datasource_registry` so that the Connect binary
//! can depend on this crate without pulling in the full backend.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// DatasourceType enum
// ---------------------------------------------------------------------------

/// All supported datasource types.
///
/// The 9 variants match the canonical list used across the Kyomi platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatasourceType {
    BigQuery,
    ClickHouse,
    Snowflake,
    Databricks,
    Redshift,
    Postgres,
    MySQL,
    SqlServer,
    Synapse,
}

/// All variants in the canonical order.
const ALL_TYPES: [DatasourceType; 9] = [
    DatasourceType::BigQuery,
    DatasourceType::ClickHouse,
    DatasourceType::Snowflake,
    DatasourceType::Databricks,
    DatasourceType::Redshift,
    DatasourceType::Postgres,
    DatasourceType::MySQL,
    DatasourceType::SqlServer,
    DatasourceType::Synapse,
];

impl DatasourceType {
    /// Lowercase string form used in the database and API (e.g., `"postgres"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BigQuery => "bigquery",
            Self::ClickHouse => "clickhouse",
            Self::Snowflake => "snowflake",
            Self::Databricks => "databricks",
            Self::Redshift => "redshift",
            Self::Postgres => "postgres",
            Self::MySQL => "mysql",
            Self::SqlServer => "sqlserver",
            Self::Synapse => "synapse",
        }
    }

    /// Human-readable name for UI display (e.g., `"PostgreSQL"`).
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::BigQuery => "BigQuery",
            Self::ClickHouse => "ClickHouse",
            Self::Snowflake => "Snowflake",
            Self::Databricks => "Databricks",
            Self::Redshift => "Amazon Redshift",
            Self::Postgres => "PostgreSQL",
            Self::MySQL => "MySQL",
            Self::SqlServer => "SQL Server",
            Self::Synapse => "Azure Synapse",
        }
    }

    /// Brief description for UI tooltips.
    pub fn description(&self) -> &'static str {
        match self {
            Self::BigQuery => "Google Cloud BigQuery",
            Self::ClickHouse => "ClickHouse analytics database",
            Self::Snowflake => "Snowflake cloud data warehouse",
            Self::Databricks => "Databricks SQL warehouse",
            Self::Redshift => "Amazon Redshift data warehouse",
            Self::Postgres => "PostgreSQL database",
            Self::MySQL => "MySQL database server",
            Self::SqlServer => "Microsoft SQL Server database",
            Self::Synapse => "Azure Synapse Analytics (SQL pools)",
        }
    }

    /// Default port, or `None` for API-based services (BigQuery, Snowflake).
    pub fn default_port(&self) -> Option<u16> {
        match self {
            Self::BigQuery => None,
            Self::ClickHouse => Some(8123),
            Self::Snowflake => None,
            Self::Databricks => Some(443),
            Self::Redshift => Some(5439),
            Self::Postgres => Some(5432),
            Self::MySQL => Some(3306),
            Self::SqlServer => Some(1433),
            Self::Synapse => Some(1433),
        }
    }
}

impl FromStr for DatasourceType {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "bigquery" => Ok(Self::BigQuery),
            "clickhouse" => Ok(Self::ClickHouse),
            "snowflake" => Ok(Self::Snowflake),
            "databricks" => Ok(Self::Databricks),
            "redshift" => Ok(Self::Redshift),
            "postgres" => Ok(Self::Postgres),
            "mysql" => Ok(Self::MySQL),
            "sqlserver" => Ok(Self::SqlServer),
            "synapse" => Ok(Self::Synapse),
            _ => Err(crate::Error::Internal(format!(
                "unsupported datasource type: '{s}'. Must be one of: {}",
                ALL_TYPES
                    .iter()
                    .map(|t| t.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }
}

impl fmt::Display for DatasourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for DatasourceType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DatasourceType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}
