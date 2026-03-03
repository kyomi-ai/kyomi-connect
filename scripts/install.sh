#!/usr/bin/env bash
# Kyomi Connect installer — downloads the latest release binary for your platform.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/kyomi-ai/kyomi-connect/main/scripts/install.sh | bash
#
# Environment variables:
#   KYOMI_CONNECT_VERSION  — specific version to install (default: latest)
#   INSTALL_DIR            — installation directory (default: /usr/local/bin)

set -euo pipefail

REPO="kyomi-ai/kyomi-connect"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
BINARY_NAME="kyomi-connect"

# Detect platform
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$OS" in
    linux)  OS="linux" ;;
    darwin) OS="macos" ;;
    *)      echo "Unsupported OS: $OS" >&2; exit 1 ;;
esac

case "$ARCH" in
    x86_64|amd64) ARCH="x86_64" ;;
    aarch64|arm64) ARCH="aarch64" ;;
    *)             echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

# Determine version
if [ -n "${KYOMI_CONNECT_VERSION:-}" ]; then
    VERSION="$KYOMI_CONNECT_VERSION"
else
    VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"v?([^"]+)".*/\1/')"
fi

ASSET_NAME="${BINARY_NAME}-${OS}-${ARCH}"
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/v${VERSION}/${ASSET_NAME}.tar.gz"

echo "Installing Kyomi Connect v${VERSION} (${OS}/${ARCH})..."

# Download and extract
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

curl -fsSL "$DOWNLOAD_URL" -o "${TMPDIR}/${ASSET_NAME}.tar.gz"
tar -xzf "${TMPDIR}/${ASSET_NAME}.tar.gz" -C "$TMPDIR"

# Install binary
if [ -w "$INSTALL_DIR" ]; then
    cp "${TMPDIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
else
    echo "Installing to ${INSTALL_DIR} (requires sudo)..."
    sudo cp "${TMPDIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
fi

chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

echo "Kyomi Connect v${VERSION} installed to ${INSTALL_DIR}/${BINARY_NAME}"
echo ""
echo "Get started:"
echo "  kyomi-connect setup"
