# Kyomi SQLx postgres transport extension

This package contains the unmodified SQLx postgres 0.8.6 driver source except
for `src/options/mod.rs` and `src/connection/stream.rs`.
The upstream sources are MIT OR Apache-2.0; both licenses are included.
Upstream: https://github.com/launchbadge/sqlx/tree/v0.8.6/sqlx-postgres

The added `transport_addr(SocketAddr)` option overrides only the TCP dial
endpoint. It takes precedence over Unix sockets and is intentionally omitted
from URL serialization. The original hostname, port, authentication options,
and TLS modes still feed the upstream handshake. SQLx core remains the exact
registry version 0.8.6, shared by both driver packages.

For an upstream update, compare these two files against the tagged source,
reapply this narrow extension, and run the portable datasource TLS suite.
The included zero-context diff applies to the tagged driver directory with
`git apply --unidiff-zero transport.patch`.
Preserve upstream license notices and source formatting. These packages are
excluded from workspace lint/format membership because the vendored source
has upstream lint allowances and is maintained as an independently publishable
dependency. The added transport paths are exercised by datasource tests.

Publish this package before `kyomi-datasource`; datasource declares a path plus
exact registry version dependency. No downstream root Cargo patch is required.
The release pipeline orders publication and skips already published versions.
