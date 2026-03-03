#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════╗"
echo "║       Kyomi Connect - Setup Wizard                  ║"
echo "╚════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo ""

# Check if binary exists
BINARY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_PATH="$BINARY_DIR/../../target/release/kyomi-connect"

if [ ! -f "$BINARY_PATH" ]; then
    echo -e "${RED}Error: kyomi-connect binary not found at $BINARY_PATH${NC}"
    echo "Please build it first: cd $BINARY_DIR/../../ && cargo build --release -p kyomi-connect"
    exit 1
fi

echo -e "${YELLOW}Step 1: Kyomi API Connection${NC}"
read -p "Kyomi WebSocket URL (default: ws://localhost:8003/connect/v1): " WS_URL
WS_URL="${WS_URL:-ws://localhost:8003/connect/v1}"

read -sp "Kyomi Connect Token (from datasource setup): " TOKEN
echo ""

if [ -z "$TOKEN" ]; then
    echo -e "${RED}Error: Token is required${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}Step 2: Database Type${NC}"
echo "Select your database type:"
echo "  1) PostgreSQL"
echo "  2) MySQL"
echo "  3) ClickHouse"
echo "  4) SQL Server"
echo "  5) Redshift"

read -p "Enter choice (1-5): " DB_CHOICE

case $DB_CHOICE in
    1) DB_TYPE="postgres"; DEFAULT_PORT="5432" ;;
    2) DB_TYPE="mysql"; DEFAULT_PORT="3306" ;;
    3) DB_TYPE="clickhouse"; DEFAULT_PORT="9000" ;;
    4) DB_TYPE="sqlserver"; DEFAULT_PORT="1433" ;;
    5) DB_TYPE="redshift"; DEFAULT_PORT="5439" ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo -e "${YELLOW}Step 3: Database Connection${NC}"
read -p "Database host (default: localhost): " DB_HOST
DB_HOST="${DB_HOST:-localhost}"

read -p "Database port (default: $DEFAULT_PORT): " DB_PORT
DB_PORT="${DB_PORT:-$DEFAULT_PORT}"

read -p "Database name: " DB_NAME
if [ -z "$DB_NAME" ]; then
    echo -e "${RED}Error: Database name is required${NC}"
    exit 1
fi

read -p "Database user: " DB_USER
if [ -z "$DB_USER" ]; then
    echo -e "${RED}Error: Database user is required${NC}"
    exit 1
fi

read -sp "Database password: " DB_PASSWORD
echo ""

# Optional: Health check port
echo ""
echo -e "${YELLOW}Step 4: Optional Configuration${NC}"
read -p "Health check port (default: 9090): " HEALTH_PORT
HEALTH_PORT="${HEALTH_PORT:-9090}"

# Summary
echo ""
echo -e "${BLUE}Configuration Summary:${NC}"
echo "  Kyomi WebSocket URL: $WS_URL"
echo "  Database Type: $DB_TYPE"
echo "  Database Host: $DB_HOST"
echo "  Database Port: $DB_PORT"
echo "  Database Name: $DB_NAME"
echo "  Database User: $DB_USER"
echo "  Health Port: $HEALTH_PORT"
echo ""

read -p "Start Kyomi Connect with this configuration? (y/n): " CONFIRM
if [ "$CONFIRM" != "y" ]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo -e "${GREEN}Starting Kyomi Connect...${NC}"
echo ""

# Export environment variables and run
export KYOMI_CONNECT_WEBSOCKET_URL="$WS_URL"
export KYOMI_CONNECT_TOKEN="$TOKEN"
export DB_TYPE="$DB_TYPE"
export DB_HOST="$DB_HOST"
export DB_PORT="$DB_PORT"
export DB_NAME="$DB_NAME"
export DB_USER="$DB_USER"
export DB_PASSWORD="$DB_PASSWORD"
export KYOMI_CONNECT_HEALTH_PORT="$HEALTH_PORT"
export RUST_LOG=info

exec "$BINARY_PATH"
