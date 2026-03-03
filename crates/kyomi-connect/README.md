# Kyomi Connect

A customer-deployed database proxy that allows Kyomi to query your databases without ever storing credentials in the cloud. Credentials stay in your network, and only query results are sent back to Kyomi.

## Quick Start

The easiest way to get started is with the interactive setup wizard:

```bash
./setup.sh
```

This will walk you through:
1. Your Kyomi API connection details (provided after creating a Connect datasource in Kyomi)
2. Database type selection
3. Database connection details
4. Optional configuration

## Manual Setup

If you prefer to set environment variables manually:

```bash
export KYOMI_CONNECT_WEBSOCKET_URL="ws://kyomi-api.example.com/connect/v1"
export KYOMI_CONNECT_TOKEN="<token-from-kyomi-ui>"
export DB_TYPE="postgres"
export DB_HOST="your-database-host"
export DB_PORT="5432"
export DB_NAME="your-database"
export DB_USER="your-username"
export DB_PASSWORD="your-password"

./kyomi-connect
```

## Environment Variables

### Required
- `KYOMI_CONNECT_WEBSOCKET_URL` - WebSocket address of Kyomi API (e.g., `ws://localhost:8003/connect/v1`)
- `KYOMI_CONNECT_TOKEN` - One-time token issued after creating a Connect datasource
- `DB_TYPE` - Database type: `postgres`, `mysql`, `clickhouse`, `sqlserver`, or `redshift`
- `DB_HOST` - Database hostname
- `DB_PORT` - Database port
- `DB_NAME` - Database name to connect to
- `DB_USER` - Database username
- `DB_PASSWORD` - Database password

### Optional
- `KYOMI_CONNECT_HEALTH_PORT` - Health check port (default: 9090)
- `RUST_LOG` - Log level: `debug`, `info`, `warn`, `error` (default: `info`)

## How It Works

1. **Token Verification** - Kyomi Connect validates the JWT token against Kyomi's JWKS endpoint
2. **Database Connection** - Connects to your database and verifies connectivity
3. **WebSocket Connection** - Opens a persistent connection to Kyomi
4. **Query Execution** - Receives query commands, executes them locally, and returns results
5. **Health Monitoring** - Exposes a health check endpoint on port 9090

## Supported Databases

- **PostgreSQL** - Queries via standard PostgreSQL driver
- **MySQL** - Queries via MySQL driver
- **ClickHouse** - Queries via HTTP API
- **SQL Server** - Queries via TDS protocol
- **Redshift** - Queries via PostgreSQL-compatible driver

## Health Check

Once running, you can check the health of the connection:

```bash
curl http://localhost:9090/healthz
```

Returns `{"status":"ok"}` when connected, or details about what's unhealthy.

## Monitoring

Logs are output to stdout in JSON format. Use `RUST_LOG` to control verbosity:

```bash
RUST_LOG=debug ./kyomi-connect
```

## Docker

The Dockerfile includes everything needed to run Kyomi Connect in a container:

```bash
docker build -f Dockerfile -t kyomi-connect .
docker run -e KYOMI_CONNECT_WEBSOCKET_URL="ws://kyomi:8003/connect/v1" \
           -e KYOMI_CONNECT_TOKEN="<token>" \
           -e DB_TYPE="postgres" \
           -e DB_HOST="postgres" \
           -e DB_PORT="5432" \
           -e DB_NAME="mydb" \
           -e DB_USER="user" \
           -e DB_PASSWORD="password" \
           kyomi-connect
```

## Troubleshooting

### Token Verification Failed
- Ensure the token hasn't expired (tokens are one-time use)
- Check that `KYOMI_CONNECT_WEBSOCKET_URL` points to the correct Kyomi instance
- Verify the token was copied correctly from the Kyomi UI

### Database Connection Failed
- Test connectivity manually: `psql -h $DB_HOST -U $DB_USER -d $DB_NAME`
- Verify all connection parameters (host, port, username, password)
- Check database firewall rules allow connections from Kyomi Connect

### WebSocket Connection Failed
- Verify `KYOMI_CONNECT_WEBSOCKET_URL` is reachable from this machine
- Check network connectivity to Kyomi API server
- Ensure the URL uses `ws://` (or `wss://` for secure)

### Logs Only Show INFO Messages
- Set `RUST_LOG=debug` for more detailed output
- Logs are in JSON format - pipe through `jq` for readability:
  ```bash
  ./kyomi-connect 2>&1 | jq .
  ```

## Architecture

Kyomi Connect uses:
- **Tokio** - async runtime for concurrent operations
- **Tungstenite** - WebSocket client
- **Tokio-TLS** - secure WebSocket connections
- **Database drivers** - native drivers for each database type

The binary is built with a multi-stage Docker build that results in a minimal ~10MB image with only required runtime libraries.
