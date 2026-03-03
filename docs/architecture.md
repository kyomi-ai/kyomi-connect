# Architecture

This document describes how Kyomi Connect is structured, how it fits into the Kyomi platform, and how data flows through the system.

## Overview

Kyomi Connect is an on-premise agent deployed on customer infrastructure. It establishes a persistent WebSocket connection to Kyomi Cloud and executes database operations on behalf of the platform. The key architectural constraint is that **database credentials never leave the customer's network** -- only query results transit the WebSocket connection.

```
┌─────────────────────────────────────────────────────────┐
│                  Customer Infrastructure                  │
│                                                           │
│   ┌──────────────┐        ┌──────────────────────────┐   │
│   │  Database     │ <───> │  Kyomi Connect Agent      │   │
│   │  (PostgreSQL, │        │                           │   │
│   │   MySQL, etc.)│        │  - WebSocket client       │   │
│   └──────────────┘        │  - Query executor          │   │
│                            │  - Health check endpoint   │   │
│   Credentials stored       └────────────┬─────────────┘   │
│   locally in config                     │                  │
│   file or env vars                      │ WebSocket (TLS)  │
└─────────────────────────────────────────┼──────────────────┘
                                          │
                              ┌───────────▼──────────────┐
                              │     Kyomi Cloud           │
                              │                           │
                              │  - API server             │
                              │  - AI agent               │
                              │  - Dashboard engine       │
                              │  - Knowledge base         │
                              └───────────────────────────┘
```

## Crate Structure

The repository is organized as a Cargo workspace with three crates. Each crate has a focused responsibility:

```
kyomi-connect/
├── crates/
│   ├── kyomi-connect/               # Binary: the agent
│   ├── kyomi-connect-protocol/      # Library: wire protocol types
│   └── kyomi-datasource/            # Library: database drivers
```

### Crate Dependency Graph

```
kyomi-connect (binary)
  ├── kyomi-connect-protocol (wire types, error types, streaming types)
  └── kyomi-datasource (DatasourceProvider trait + driver implementations)
        └── kyomi-connect-protocol (shared types: SimpleType, ColumnInfo, etc.)
```

### kyomi-connect (binary)

The CLI agent. This is what customers install and run.

| Module | Responsibility |
|--------|---------------|
| `main.rs` | CLI entry point (clap). Subcommands: `run`, `setup`, `status`, `service install/uninstall`. |
| `config.rs` | JWT verification, environment variable parsing, configuration building. |
| `executor.rs` | Command executor. Receives `ConnectRequest`, dispatches to the appropriate provider method (query, dry-run, catalog, test-connection). |
| `ws_client.rs` | WebSocket client. Connects to Kyomi Cloud, sends/receives messages, handles reconnection with backoff. |
| `wizard.rs` | Interactive setup wizard. Walks through token, database type, and connection details. |
| `health.rs` | Health check HTTP server. Serves `/healthz` on a configurable port (default 9090). |
| `service.rs` | systemd service management. Install/uninstall unit files. |
| `callback_server.rs` | Local HTTP callback server for browser-based token delivery. |
| `config_file.rs` | TOML configuration file management (`~/.config/kyomi-connect/config.toml`). |

### kyomi-connect-protocol (library)

The lightest crate. Contains only serializable types with no business logic. This is the contract between Kyomi Cloud and the Connect agent.

| Module | Responsibility |
|--------|---------------|
| `wire.rs` | `ConnectOp`, `ConnectRequest`, `ConnectResponse`, `ConnectResponseBody`, `QueryParams`, `DryRunParams`, `CatalogResult`, `CatalogContainer`, `CatalogTable`, `CatalogColumn` |
| `types.rs` | `DatasourceType` enum (the 9 supported database types) |
| `stream.rs` | `QueryStreamEvent` (Header, Chunk, Complete), `QueryStream` type alias, `ColumnInfo`, `SimpleType` |
| `error.rs` | `Error` enum (Provider, Connection, NotSupported, Internal, SerdeJson), `Result` type alias |

Dependencies: `serde`, `serde_json`, `async-trait`, `chrono`, `thiserror`, `futures-util`.

### kyomi-datasource (library)

Contains the `DatasourceProvider` trait and all concrete database driver implementations. Each driver is behind a compile-time feature flag.

| Module | Responsibility |
|--------|---------------|
| `provider.rs` | `DatasourceProvider` trait definition, `QueryResult`, `DryRunResult`, `DiscoveryResult`, `QueryStatus` |
| `factory.rs` | `create_provider()` factory function that constructs the appropriate provider by `DatasourceType` |
| `type_mapping.rs` | Unified native-type-to-`SimpleType` mapping utilities |
| `stream.rs` | Stream utilities (convert `QueryResult` to `QueryStream`, collect stream to result) |
| `ssh_tunnel.rs` | SSH tunnel support (feature-gated behind `ssh`) |
| `oauth_refresh.rs` | OAuth token refresh for Snowflake, Databricks, BigQuery |
| `providers/` | One module per database driver, plus shared modules (`sqlx_common.rs`, `tsql_common.rs`, `aws_sigv4.rs`) |

#### Feature Flags

```toml
[features]
default = ["all"]
all = ["postgres", "mysql", "redshift", "clickhouse", "snowflake",
       "databricks", "sqlserver", "synapse", "bigquery"]
postgres   = ["dep:sqlx"]
mysql      = ["dep:sqlx"]
redshift   = ["dep:sqlx"]
clickhouse = []                              # Uses reqwest (always available)
snowflake  = ["dep:jsonwebtoken", "dep:rsa"]
databricks = []                              # Uses reqwest (always available)
sqlserver  = ["dep:tiberius", "dep:tokio-util"]
synapse    = ["dep:tiberius", "dep:tokio-util"]
bigquery   = ["dep:jsonwebtoken"]
ssh        = ["dep:russh"]
```

#### Shared Code Modules

To avoid duplication across providers with similar underlying protocols:

- **`sqlx_common.rs`** -- shared code for all sqlx-based providers (PostgreSQL, MySQL, Redshift, Databricks)
- **`tsql_common.rs`** -- shared code for TDS-based providers (SQL Server, Azure Synapse)
- **`aws_sigv4.rs`** -- AWS Signature V4 signing for Redshift IAM authentication

## Data Flow

### 1. Connection Establishment

1. Customer runs `kyomi-connect` (or `kyomi-connect setup` for first-time configuration).
2. The agent loads configuration from TOML file or environment variables.
3. The JWT token is verified against the JWKS endpoint on Kyomi Cloud.
4. The agent tests the local database connection.
5. A WebSocket connection is established to `wss://<kyomi-host>/connect/v1`.
6. The health check HTTP server starts on port 9090.

### 2. Query Execution (Non-Streaming)

```
Kyomi Cloud                    Connect Agent                   Database
    │                               │                              │
    │  ConnectRequest               │                              │
    │  (op: execute_query,          │                              │
    │   params: {sql, limit, ...})  │                              │
    ├──────────────────────────────>│                              │
    │                               │  SQL query                   │
    │                               ├─────────────────────────────>│
    │                               │                              │
    │                               │  Result rows                 │
    │                               │<─────────────────────────────┤
    │                               │                              │
    │  ConnectResponse              │                              │
    │  (type: result,               │                              │
    │   result: {status, columns,   │                              │
    │            rows, ...})        │                              │
    │<──────────────────────────────┤                              │
```

### 3. Query Execution (Streaming)

For large result sets, the response is streamed as multiple WebSocket messages:

```
Kyomi Cloud                    Connect Agent                   Database
    │                               │                              │
    │  ConnectRequest               │                              │
    │  (streaming: true)            │                              │
    ├──────────────────────────────>│                              │
    │                               │  SQL query                   │
    │                               ├─────────────────────────────>│
    │                               │                              │
    │  ConnectResponse              │  Row batches                 │
    │  (type: stream_header,        │<─────────────────────────────┤
    │   columns: [...])             │                              │
    │<──────────────────────────────┤                              │
    │                               │                              │
    │  ConnectResponse              │                              │
    │  (type: stream_chunk,         │                              │
    │   rows: [[...], ...],         │                              │
    │   chunk_index: 0)             │                              │
    │<──────────────────────────────┤                              │
    │                               │                              │
    │  ... more chunks ...          │                              │
    │                               │                              │
    │  ConnectResponse              │                              │
    │  (type: stream_complete,      │                              │
    │   total_chunks: N,            │                              │
    │   total_rows_returned: M)     │                              │
    │<──────────────────────────────┤                              │
```

### 4. Other Operations

| Operation | Request | Response |
|-----------|---------|----------|
| **Test Connection** | `op: test_connection`, no params | `result: true` or `error: "..."` |
| **Dry Run** | `op: dry_run`, `params: {sql}` | `result: {valid, message, line, column}` |
| **Discover Catalog** | `op: discover_catalog`, no params | `result: {containers: [{name, tables: [{name, columns: [...]}]}]}` |

## Security Model

### Authentication

The Connect agent authenticates with Kyomi Cloud using a JWT token:

1. The token is issued by Kyomi Cloud when the user creates a Connect datasource.
2. On startup, the agent includes the token in the WebSocket upgrade request.
3. Kyomi Cloud verifies the token signature using JWKS.
4. The token encodes the workspace ID and datasource ID, scoping the connection.

### Credential Isolation

Database credentials are configured locally through one of:

- **TOML config file**: `~/.config/kyomi-connect/config.toml`
- **Environment variables**: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

These values are read by the agent process and used to establish direct connections to the database. They are never serialized, logged, or transmitted over the WebSocket connection.

### Transport Security

The WebSocket connection to Kyomi Cloud uses TLS (wss://). Data in transit between the agent and Kyomi Cloud is encrypted.

The connection between the agent and the local database uses the database's native protocol and can optionally use SSL/TLS (configured via `DB_SSLMODE`).

## Reconnection

The WebSocket client automatically reconnects with exponential backoff if the connection drops. The `run_forever` loop in `ws_client.rs` handles:

- Network interruptions
- Server restarts
- Token expiry (reconnects with a fresh handshake)

The health check endpoint at `/healthz` reports whether the WebSocket connection is currently active and the database is reachable.
