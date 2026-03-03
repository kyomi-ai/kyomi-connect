# ---------------------------------------------------------------------------
# Kyomi Connect — customer-deployed database proxy
# ---------------------------------------------------------------------------
# Build context must be the repo root:
#   docker build -t kyomi-connect .
# ---------------------------------------------------------------------------

# ===== Stage 1: Build Rust binary =====
FROM rust:1-bookworm AS builder

WORKDIR /build

# Copy workspace
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release -p kyomi-connect && \
    cp target/release/kyomi-connect /tmp/kyomi-connect

# Collect minimal shared libraries
RUN mkdir -p /tmp/runtime-libs && \
    ldd /tmp/kyomi-connect | awk '/=>/ {print $3}' | while read lib; do cp "$lib" /tmp/runtime-libs/; done && \
    cp /lib64/ld-linux-x86-64.so.2 /tmp/runtime-libs/ && \
    cp /lib/x86_64-linux-gnu/libnss_dns.so.2 /tmp/runtime-libs/ && \
    cp /lib/x86_64-linux-gnu/libnss_files.so.2 /tmp/runtime-libs/ && \
    cp /lib/x86_64-linux-gnu/libresolv.so.2 /tmp/runtime-libs/

# ===== Stage 2: Scratch runtime =====
FROM scratch

# Dynamic linker + shared libraries
COPY --from=builder /tmp/runtime-libs/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2
COPY --from=builder /tmp/runtime-libs/ /lib/x86_64-linux-gnu/

# CA certificates (for outbound TLS to Kyomi API)
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# DNS config
COPY --from=builder /etc/nsswitch.conf /etc/nsswitch.conf

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
