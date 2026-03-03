# ---------------------------------------------------------------------------
# Kyomi Connect — customer-deployed database proxy
# ---------------------------------------------------------------------------
# Build context must be the repo root:
#   docker build -t kyomi-connect .
# ---------------------------------------------------------------------------

# ===== Stage 1: Build static Rust binary =====
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build

# Copy workspace
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release -p kyomi-connect && \
    cp target/release/kyomi-connect /tmp/kyomi-connect

# ===== Stage 2: Scratch runtime =====
FROM scratch

# CA certificates (for outbound TLS to Kyomi API)
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Binary
COPY --from=builder /tmp/kyomi-connect /app/kyomi-connect

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    HOME=/tmp \
    TMPDIR=/tmp \
    RUST_LOG=info

# Health check port
EXPOSE 9090

USER 1000
ENTRYPOINT ["/app/kyomi-connect"]
