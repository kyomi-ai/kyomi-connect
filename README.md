# Kyomi Connect

Secure on-premise agent for proxying database queries between [Kyomi](https://kyomi.ai) and your data warehouse.

## What is Kyomi Connect?

Kyomi Connect is a lightweight agent that runs on your infrastructure and acts as a secure bridge between the Kyomi cloud platform and your databases. Instead of sending database credentials to the cloud, Kyomi Connect keeps them on-premise and only transmits query results over an encrypted WebSocket connection.

When a user asks Kyomi to query a database, the request is routed through Connect. The agent receives the query, executes it against the local database, and streams the results back. At no point do your database credentials leave your network.

Kyomi Connect supports 9 database engines out of the box, with compile-time feature flags so you can build a minimal binary containing only the drivers you need.

## Security Model

- **JWT-authenticated WebSocket**: Connect authenticates with Kyomi Cloud using a signed JWT token. The token is verified against a JWKS endpoint.
- **Credentials stay on-premise**: Database usernames, passwords, and connection strings are stored locally (TOML config file or environment variables) and are never transmitted to Kyomi Cloud.
- **TLS-encrypted transport**: The WebSocket connection uses TLS. Only query results cross the network boundary.
- **Auditable source code**: This repository is open source under the Apache 2.0 license so your security team can audit exactly what runs on your infrastructure.

## Quick Start

### Option 1: Install Script

```bash
curl -fsSL https://github.com/kyomi-ai/kyomi-connect/releases/latest/download/install.sh | sh
```

The installer detects your OS and architecture, downloads the binary, and launches an interactive setup wizard.

### Option 2: Docker

```bash
docker run -e KYOMI_TOKEN=<token> -e DB_HOST=<host> ghcr.io/kyomi-ai/kyomi-connect:latest
```

### Option 3: Cargo

```bash
cargo install kyomi-connect
kyomi-connect setup
```

## Supported Databases

| Database | Feature Flag | Protocol |
|----------|-------------|----------|
| PostgreSQL | `postgres` | libpq (sqlx) |
| MySQL | `mysql` | MySQL protocol (sqlx) |
| ClickHouse | `clickhouse` | HTTP API |
| Redshift | `redshift` | PostgreSQL-compatible (sqlx) |
| Snowflake | `snowflake` | REST API + JWT |
| Databricks | `databricks` | REST API |
| SQL Server | `sqlserver` | TDS (tiberius) |
| Azure Synapse | `synapse` | TDS (tiberius) |
| BigQuery | `bigquery` | REST API + OAuth |

All drivers are enabled by default. See [Custom Builds](#custom-builds) for building with a subset of drivers.

## Architecture

```
┌─────────────────┐         WebSocket (TLS)         ┌──────────────┐
│  Your Database   │ <──── Kyomi Connect Agent ────> │  Kyomi Cloud  │
│  (on-premise)    │         (on-premise)             │  Platform     │
└─────────────────┘                                   └──────────────┘
     Credentials                                      Only query results
     stay here                                        cross the boundary
```

Kyomi Connect consists of three crates:

- **`kyomi-connect`** -- the binary agent with WebSocket client, query executor, and setup wizard
- **`kyomi-connect-protocol`** -- wire protocol types shared between the agent and Kyomi Cloud
- **`kyomi-datasource`** -- the `DatasourceProvider` trait and all database driver implementations

For detailed architecture documentation, see [docs/architecture.md](docs/architecture.md).

## Documentation

- [Architecture](docs/architecture.md) -- how Connect fits into the Kyomi platform, crate structure, data flow
- [Wire Protocol](docs/protocol.md) -- request/response format, operations, streaming, type system
- [Adding a Datasource](docs/adding-a-datasource.md) -- step-by-step guide for contributing a new database driver
- [Deployment](docs/deployment.md) -- binary installation, Docker, Kubernetes with Helm, systemd

## Building from Source

```bash
git clone https://github.com/kyomi-ai/kyomi-connect.git
cd kyomi-connect
cargo build --release -p kyomi-connect
```

The binary is output to `target/release/kyomi-connect`.

### Custom Builds

Build with only the drivers you need for a smaller binary:

```bash
cargo build --release -p kyomi-connect --no-default-features --features postgres,mysql
```

This is useful in containerized environments where you only connect to a single database type.

## CLI Commands

```
kyomi-connect              # Auto-detect: run if configured, setup if not
kyomi-connect setup        # Run the interactive setup wizard
kyomi-connect run          # Run the agent (fail if not configured)
kyomi-connect status       # Show connection status and datasource info
kyomi-connect service install    # Install as a systemd service
kyomi-connect service uninstall  # Remove the systemd service
```

The `setup` subcommand also accepts flags for non-interactive configuration:

```bash
kyomi-connect setup \
  --token <jwt-token> \
  --db-host localhost \
  --db-port 5432 \
  --db-name mydb \
  --db-user postgres \
  --db-password-file /run/secrets/db-password \
  --db-ssl-mode require
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

## Contributing

We welcome contributions. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a pull request.

For adding a new database driver, see the dedicated guide: [docs/adding-a-datasource.md](docs/adding-a-datasource.md).

## Security

To report a security vulnerability, please email [security@kyomi.ai](mailto:security@kyomi.ai) or use [GitHub Security Advisories](https://github.com/kyomi-ai/kyomi-connect/security/advisories).

Do not file a public issue for security vulnerabilities.
