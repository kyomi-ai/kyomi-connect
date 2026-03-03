# Deployment

This guide covers all the ways to deploy Kyomi Connect on your infrastructure.

## Prerequisites

Before deploying, you need a **Connect token** from the Kyomi dashboard:

1. Log in to [Kyomi](https://app.kyomi.ai).
2. Go to **Settings > Datasources > Add Datasource**.
3. Choose **Connect** as the connection method.
4. Copy the generated token.

## Binary Installation

### Install Script

The fastest way to install on Linux or macOS:

```bash
curl -fsSL https://github.com/kyomi-ai/kyomi-connect/releases/latest/download/install.sh | sh
```

The script:
1. Detects your OS (Linux, macOS) and architecture (amd64, arm64).
2. Downloads the latest binary from GitHub Releases.
3. Verifies the SHA-256 checksum.
4. Installs to `/usr/local/bin/kyomi-connect`.
5. Launches the interactive setup wizard.

To pass the token non-interactively:

```bash
curl -fsSL https://github.com/kyomi-ai/kyomi-connect/releases/latest/download/install.sh | sh -s -- --token <your-token>
```

### Manual Download

Download binaries directly from [GitHub Releases](https://github.com/kyomi-ai/kyomi-connect/releases):

```bash
# Linux amd64
curl -fsSL -o kyomi-connect https://github.com/kyomi-ai/kyomi-connect/releases/latest/download/kyomi-connect-linux-amd64
chmod +x kyomi-connect
sudo mv kyomi-connect /usr/local/bin/

# macOS arm64 (Apple Silicon)
curl -fsSL -o kyomi-connect https://github.com/kyomi-ai/kyomi-connect/releases/latest/download/kyomi-connect-macos-arm64
chmod +x kyomi-connect
sudo mv kyomi-connect /usr/local/bin/
```

Then run the setup wizard:

```bash
kyomi-connect setup
```

### Cargo

If you have Rust installed:

```bash
cargo install kyomi-connect
kyomi-connect setup
```

## Docker

### Basic Usage

```bash
docker run -d \
  --name kyomi-connect \
  -e KYOMI_TOKEN=<your-token> \
  -e DB_HOST=<database-host> \
  -e DB_PORT=5432 \
  -e DB_NAME=<database> \
  -e DB_USER=<user> \
  -e DB_PASSWORD=<password> \
  ghcr.io/kyomi-ai/kyomi-connect:latest
```

### With Health Check Exposed

```bash
docker run -d \
  --name kyomi-connect \
  -p 9090:9090 \
  -e KYOMI_TOKEN=<your-token> \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_NAME=analytics \
  -e DB_USER=kyomi_reader \
  -e DB_PASSWORD=<password> \
  ghcr.io/kyomi-ai/kyomi-connect:latest
```

### Custom Docker Build (Single Driver)

Build an image with only the driver you need for a smaller footprint:

```bash
docker build --build-arg FEATURES="postgres" -t kyomi-connect-pg .
```

This significantly reduces the image size by excluding unused database drivers and their dependencies.

### Docker Compose

```yaml
services:
  kyomi-connect:
    image: ghcr.io/kyomi-ai/kyomi-connect:latest
    restart: unless-stopped
    environment:
      KYOMI_TOKEN: ${KYOMI_TOKEN}
      DB_HOST: postgres
      DB_PORT: "5432"
      DB_NAME: analytics
      DB_USER: kyomi_reader
      DB_PASSWORD: ${DB_PASSWORD}
    ports:
      - "9090:9090"
    depends_on:
      - postgres
```

## Kubernetes with Helm

### Install from OCI Registry

```bash
helm install kyomi-connect oci://ghcr.io/kyomi-ai/charts/kyomi-connect \
  --set token=<your-token> \
  --set target.host=<db-host> \
  --set target.port=5432 \
  --set target.database=<database> \
  --set target.user=<user> \
  --set target.passwordSecretName=<secret-name> \
  --set target.passwordSecretKey=password
```

### Using an Existing Secret for the Token

Create a Kubernetes secret with your Connect token:

```bash
kubectl create secret generic kyomi-connect-token --from-literal=token=<your-token>
```

Then reference it in the Helm install:

```bash
helm install kyomi-connect oci://ghcr.io/kyomi-ai/charts/kyomi-connect \
  --set existingSecret.name=kyomi-connect-token \
  --set existingSecret.key=token \
  --set target.host=<db-host> \
  --set target.port=5432 \
  --set target.database=<database> \
  --set target.user=<user> \
  --set target.passwordSecretName=db-credentials \
  --set target.passwordSecretKey=password
```

### Helm Values Reference

| Value | Default | Description |
|-------|---------|-------------|
| `token` | `""` | Connect token (required unless `existingSecret` is set) |
| `existingSecret.name` | `""` | Name of existing secret containing the token |
| `existingSecret.key` | `"token"` | Key within the secret |
| `target.host` | `""` | Database hostname |
| `target.port` | `5432` | Database port |
| `target.database` | `""` | Database name |
| `target.user` | `""` | Database username |
| `target.passwordSecretName` | `""` | Kubernetes secret containing database password |
| `target.passwordSecretKey` | `"password"` | Key within the password secret |
| `image.repository` | `ghcr.io/kyomi-ai/kyomi-connect` | Container image repository |
| `image.tag` | `latest` | Container image tag |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy |
| `resources.requests.cpu` | `50m` | CPU request |
| `resources.requests.memory` | `64Mi` | Memory request |
| `resources.limits.cpu` | `500m` | CPU limit |
| `resources.limits.memory` | `256Mi` | Memory limit |
| `healthPort` | `9090` | Health check port |
| `serviceAccount.create` | `true` | Create a service account |
| `serviceAccount.name` | `""` | Service account name (generated if empty) |
| `podAnnotations` | `{}` | Additional pod annotations |
| `nodeSelector` | `{}` | Node selector constraints |
| `tolerations` | `[]` | Pod tolerations |

## systemd Service

For long-running deployments on Linux servers, install Kyomi Connect as a systemd service:

### Install

```bash
# First, configure the agent
kyomi-connect setup

# Then install the systemd service
kyomi-connect service install
```

This creates a systemd unit file that:
- Starts the agent on boot.
- Restarts automatically on failure.
- Runs as the current user.

### Manage

```bash
# Check status
systemctl --user status kyomi-connect

# View logs
journalctl --user -u kyomi-connect -f

# Restart
systemctl --user restart kyomi-connect
```

### Uninstall

```bash
kyomi-connect service uninstall
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KYOMI_TOKEN` | yes | -- | JWT token from the Kyomi dashboard |
| `DB_HOST` | yes | -- | Database hostname |
| `DB_PORT` | no | varies by type | Database port |
| `DB_NAME` | yes | -- | Database name |
| `DB_USER` | yes | -- | Database username |
| `DB_PASSWORD` | yes | -- | Database password |
| `DB_TYPE` | no | from token | Database type (postgres, mysql, clickhouse, sqlserver, redshift, snowflake, databricks, synapse, bigquery) |
| `DB_SSLMODE` | no | `prefer` | SSL mode (disable, prefer, require, verify-ca, verify-full) |
| `HEALTH_PORT` | no | `9090` | Port for the health check HTTP endpoint |

### TOML Config File

When using the interactive setup wizard, configuration is saved to `~/.config/kyomi-connect/config.toml`:

```toml
token = "eyJhbGciOi..."
db_host = "localhost"
db_port = 5432
db_name = "analytics"
db_user = "kyomi_reader"
db_password = "secret"
health_port = 9090
```

Environment variables take precedence over the config file.

## Health Check

Kyomi Connect exposes a health check HTTP endpoint:

```bash
curl http://localhost:9090/healthz
```

The endpoint reports:
- Whether the WebSocket connection to Kyomi Cloud is active.
- Whether the database is reachable.

Use this endpoint for Docker health checks, Kubernetes liveness/readiness probes, and monitoring.

### Kubernetes Probe Example

The Helm chart configures this automatically. For manual deployments:

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 9090
  initialDelaySeconds: 10
  periodSeconds: 30
readinessProbe:
  httpGet:
    path: /healthz
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Troubleshooting

### "No configuration found"

The agent cannot find a token or database configuration. Either:
- Run `kyomi-connect setup` interactively to configure.
- Set `KYOMI_TOKEN` and `DB_*` environment variables.

### "Token Valid" shows a red X

The JWT token is invalid or expired. Generate a new token from the Kyomi dashboard (**Settings > Datasources > your Connect datasource > Regenerate Token**).

### "Database Connection" shows a red X

The agent cannot reach the database. Verify:
- `DB_HOST` is correct and resolvable from the agent's network.
- `DB_PORT` is correct and the firewall allows the connection.
- `DB_USER` and `DB_PASSWORD` are correct.
- The database server is running and accepting connections.
- SSL settings match the database's requirements (`DB_SSLMODE`).

### "Kyomi Connection" shows a red X

The agent cannot reach Kyomi Cloud. Check:
- Your server has outbound internet access on port 443.
- No proxy or firewall is blocking WebSocket connections.
- The Kyomi service is operational (check [status.kyomi.ai](https://status.kyomi.ai) if available).

The agent will keep retrying the connection automatically with exponential backoff.

### Connection drops and reconnects

This is normal behavior. The WebSocket client automatically reconnects when the connection drops due to network interruptions, server restarts, or idle timeouts. Check the logs for the reconnection frequency -- occasional reconnects are expected, but frequent reconnects may indicate network instability.

### Setup wizard hangs at password prompt

This is expected behavior. The password input does not echo characters for security. Type your password and press Enter.

### High memory usage

If connecting to a database with very large result sets, consider:
- Using the streaming mode (enabled by default for large queries).
- Setting appropriate `LIMIT` clauses in your queries.
- Building a custom binary with only the drivers you need.
