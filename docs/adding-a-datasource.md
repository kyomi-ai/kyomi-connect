# Adding a New Datasource

This guide walks through adding a new database driver to Kyomi Connect. By the end, your new database will be a compile-time feature flag that customers can include in their builds.

## Prerequisites

Before starting, familiarize yourself with:

- The `DatasourceProvider` trait in `crates/kyomi-datasource/src/provider.rs`
- An existing provider implementation (e.g., `providers/postgres.rs` for a sqlx-based driver or `providers/clickhouse.rs` for an HTTP-based driver)
- The `SimpleType` enum in `crates/kyomi-connect-protocol/src/stream.rs`

## Overview

Adding a new datasource requires changes in both the `kyomi-connect-protocol` and `kyomi-datasource` crates:

1. Add a `DatasourceType` variant (in `kyomi-connect-protocol`)
2. Add a feature flag (in `kyomi-datasource`)
3. Create the provider module
4. Implement the `DatasourceProvider` trait
5. Map native types to `SimpleType`
6. Register in the factory
7. Register in `providers/mod.rs`
8. Write tests

## Step 1: Add DatasourceType Variant

In `crates/kyomi-connect-protocol/src/types.rs`, add a new variant to the `DatasourceType` enum:

```rust
pub enum DatasourceType {
    // ... existing variants ...
    YourDb,
}
```

Then update all the `match` arms in the same file:

- `as_str()` -- the lowercase slug (e.g., `"yourdb"`)
- `display_name()` -- the human-readable name (e.g., `"YourDB"`)
- `description()` -- a brief description for UI tooltips
- `default_port()` -- the default port, or `None` for API-based services
- `FromStr` impl -- the reverse mapping from string to variant

Also update `ALL_TYPES` to include the new variant.

## Step 2: Add Feature Flag

In `crates/kyomi-datasource/Cargo.toml`, add a feature flag for the new driver:

```toml
[features]
default = ["all"]
all = ["postgres", "mysql", "redshift", "clickhouse", "snowflake",
       "databricks", "sqlserver", "synapse", "bigquery", "yourdb"]
yourdb = ["dep:your-driver-crate"]  # or [] if using reqwest (already a dependency)
```

Add any required dependencies:

```toml
[dependencies]
your-driver-crate = { version = "1.0", optional = true }
```

## Step 3: Create Provider Module

Create a new file at `crates/kyomi-datasource/src/providers/yourdb.rs`.

Start with the module structure:

```rust
//! YourDB datasource provider.

use kyomi_connect_protocol::{ColumnInfo, SimpleType};
use crate::provider::{DatasourceProvider, DryRunResult, QueryResult, QueryStatus};

/// YourDB provider.
pub struct YourDbProvider {
    // Connection pool, HTTP client, or whatever your driver needs
}

impl YourDbProvider {
    /// Create a new provider from connection config and credentials.
    pub async fn new(
        connection_config: &serde_json::Value,
        credentials: &serde_json::Value,
    ) -> kyomi_connect_protocol::Result<Self> {
        // Parse host, port, database, username, password from the JSON values
        let host = connection_config
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("localhost");
        let port = connection_config
            .get("port")
            .and_then(|v| v.as_u64())
            .unwrap_or(YOUR_DEFAULT_PORT) as u16;
        let database = connection_config
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let username = credentials
            .get("username")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let password = credentials
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Establish connection
        // ...

        Ok(Self { /* fields */ })
    }
}
```

## Step 4: Implement DatasourceProvider

The trait requires these methods:

```rust
#[async_trait::async_trait]
impl DatasourceProvider for YourDbProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        // Execute a simple query like "SELECT 1" to verify connectivity.
        // Return Ok(true) on success, or Err(...) with a descriptive message.
        todo!()
    }

    async fn execute_query(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
    ) -> kyomi_connect_protocol::Result<QueryResult> {
        // 1. Execute the SQL query against the database.
        // 2. Map result columns to ColumnInfo with SimpleType.
        // 3. Convert rows to Vec<Vec<serde_json::Value>>.
        // 4. Return a QueryResult with status, columns, rows, etc.
        todo!()
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        // Validate SQL without executing. Use EXPLAIN or equivalent.
        // Return DryRunResult::success(...) or DryRunResult::failure(...).
        // If not supported, the default impl returns a success message.
        todo!()
    }

    async fn close(&self) {
        // Clean up connections, pools, tunnels, etc.
    }
}
```

### Optional Methods

The trait provides default implementations for these methods. Override them if your database supports the corresponding concept:

- `execute_query_stream()` -- override if the driver supports native streaming/cursors for better memory efficiency on large result sets
- `list_databases()` -- override if the database has a database-level container
- `list_schemas()` -- override if the database has a schema-level container
- `list_projects()` -- override for cloud services with project-level containers (like BigQuery)
- `list_warehouses()` -- override for Snowflake-style warehouse selection
- `list_catalogs()` -- override for Databricks-style catalog selection

### get_catalog (via discover_catalog)

The catalog discovery is handled by the executor, which calls `execute_query` with SQL queries specific to the database's information schema. However, if your database requires a non-SQL approach to schema discovery, you may need to coordinate with the executor. Look at how existing providers handle this.

## Step 5: Type Mapping

Map your database's native column types to the `SimpleType` enum. Create a mapping function:

```rust
/// Map a YourDB native type to SimpleType.
fn map_type(native_type: &str) -> SimpleType {
    match native_type.to_uppercase().as_str() {
        // Text types
        "VARCHAR" | "TEXT" | "CHAR" | "STRING" => SimpleType::String,

        // Numeric types
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT"
        | "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => SimpleType::Number,

        // Boolean
        "BOOLEAN" | "BOOL" => SimpleType::Boolean,

        // Date/time types
        "DATE" => SimpleType::Date,
        "TIME" => SimpleType::Time,
        "TIMESTAMP" | "DATETIME" => SimpleType::Timestamp,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => SimpleType::TimestampTz,

        // Fallback
        _ => SimpleType::Unknown,
    }
}
```

Be thorough with type mappings -- cover all types your database supports. Look at the PostgreSQL or MySQL provider for examples of comprehensive mappings.

## Step 6: Register in Factory

In `crates/kyomi-datasource/src/factory.rs`:

1. Add a feature-gated import at the top:

```rust
#[cfg(feature = "yourdb")]
use crate::providers::yourdb::YourDbProvider;
```

2. Add a match arm in `create_provider()`:

```rust
#[cfg(feature = "yourdb")]
DatasourceType::YourDb => {
    let provider = YourDbProvider::new(connection_config, &resolved_credentials).await?;
    Ok(Box::new(provider))
}
#[cfg(not(feature = "yourdb"))]
DatasourceType::YourDb => Err(kyomi_connect_protocol::Error::NotSupported(
    "YourDB provider is not enabled (feature 'yourdb')".into(),
)),
```

## Step 7: Register in providers/mod.rs

In `crates/kyomi-datasource/src/providers/mod.rs`, add the feature-gated module:

```rust
#[cfg(feature = "yourdb")]
pub mod yourdb;
```

## Step 8: Write Tests

### Type Mapping Tests

Test that every native type maps correctly:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_mapping_text_types() {
        assert_eq!(map_type("VARCHAR"), SimpleType::String);
        assert_eq!(map_type("TEXT"), SimpleType::String);
    }

    #[test]
    fn type_mapping_numeric_types() {
        assert_eq!(map_type("INT"), SimpleType::Number);
        assert_eq!(map_type("DECIMAL"), SimpleType::Number);
    }

    #[test]
    fn type_mapping_unknown_falls_back() {
        assert_eq!(map_type("GEOMETRY"), SimpleType::Unknown);
    }
}
```

### Connection Config Parsing Tests

Test that your provider correctly parses connection config and credentials JSON:

```rust
#[test]
fn parses_connection_config() {
    let config = serde_json::json!({
        "host": "db.example.com",
        "port": 5432,
        "database": "mydb"
    });
    // Verify parsing logic
}
```

### Feature Flag Verification

Verify the build succeeds with only your feature enabled:

```bash
cargo build -p kyomi-datasource --no-default-features --features yourdb
cargo test -p kyomi-datasource --no-default-features --features yourdb
```

## Checklist

Before submitting your pull request:

- [ ] `DatasourceType` variant added with all `match` arms updated
- [ ] Feature flag added to `Cargo.toml` and included in `all`
- [ ] Provider struct with `new()` constructor
- [ ] `DatasourceProvider` trait implemented (`test_connection`, `execute_query`, `dry_run`, `close`)
- [ ] Type mapping covers all native types
- [ ] Factory match arm added (both `cfg(feature)` and `cfg(not(feature))` arms)
- [ ] Module registered in `providers/mod.rs`
- [ ] Unit tests for type mapping and config parsing
- [ ] `cargo build --no-default-features --features yourdb` succeeds
- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-features -- -D warnings` passes
- [ ] `cargo fmt --all` applied
