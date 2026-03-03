# Contributing to Kyomi Connect

Thank you for your interest in contributing to Kyomi Connect. This document covers the process for submitting changes, development setup, and project conventions.

## Developer Certificate of Origin (DCO)

All commits must be signed off to certify that you wrote the code or have the right to submit it under the project's open-source license. This is a lightweight alternative to a Contributor License Agreement (CLA).

Sign off your commits with:

```bash
git commit -s -m "your commit message"
```

This adds a `Signed-off-by` line to your commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

By signing off, you certify the following (from [developercertificate.org](https://developercertificate.org/)):

> I certify that I have the right to submit this contribution under the open source license indicated in the file.

All commits in a pull request must be signed off. The CI pipeline will reject unsigned commits.

## Development Setup

### Prerequisites

- **Rust** (stable, 1.85+): [rustup.rs](https://rustup.rs/)
- **cargo** (included with Rust)

### Clone and Build

```bash
git clone https://github.com/kyomi-ai/kyomi-connect.git
cd kyomi-connect
cargo build --workspace
```

### Run Tests

```bash
cargo test --workspace
```

To test with a specific feature flag subset:

```bash
cargo test --workspace --no-default-features --features postgres,mysql
```

## Code Style

### Formatting

All code must be formatted with `rustfmt`:

```bash
cargo fmt --all
```

### Linting

All code must pass `clippy` with no warnings:

```bash
cargo clippy --workspace --all-features -- -D warnings
```

### General Conventions

- No `#[allow(dead_code)]` -- if code is unused, remove it.
- No `unwrap()` in library code -- use proper error handling with `?` and the `Error` enum in `kyomi-connect-protocol`.
- Feature-gate all database-specific code behind the appropriate feature flag.
- Write doc comments (`///`) for all public types and functions.

## Testing

### Unit Tests

Each crate contains unit tests in `#[cfg(test)]` modules. Run them with:

```bash
cargo test --workspace
```

### Feature Flag Testing

When adding or modifying a provider, verify the build succeeds with only that feature enabled:

```bash
cargo build -p kyomi-datasource --no-default-features --features postgres
cargo test -p kyomi-datasource --no-default-features --features postgres
```

Also verify the build succeeds with no features (protocol-only):

```bash
cargo build -p kyomi-datasource --no-default-features
```

### Type Mapping Tests

Every provider should include tests for its native-type-to-`SimpleType` mapping. See existing providers for examples.

## Adding a New Datasource

For a comprehensive step-by-step guide, see [docs/adding-a-datasource.md](docs/adding-a-datasource.md).

In summary:

1. Add a feature flag to `crates/kyomi-datasource/Cargo.toml`
2. Create a provider module at `crates/kyomi-datasource/src/providers/your_db.rs`
3. Implement the `DatasourceProvider` trait
4. Map native types to `SimpleType`
5. Register in `factory.rs` and `providers/mod.rs`
6. Add the `DatasourceType` variant in `kyomi-connect-protocol`
7. Write tests

## Pull Request Process

1. **Fork the repository** and create a feature branch from `main`.
2. **Make your changes** following the conventions above.
3. **Write tests** for new functionality.
4. **Ensure CI passes**: formatting, linting, and all tests.
5. **Sign off all commits** with `git commit -s`.
6. **Submit a pull request** against `main` with a clear description of the change.

### PR Title Conventions

Use conventional commit prefixes in PR titles:

- `feat: add Oracle database provider`
- `fix: handle NULL columns in ClickHouse type mapping`
- `docs: update deployment guide for Helm v3`
- `refactor: consolidate TDS shared code`
- `test: add integration tests for MySQL provider`

### Review

A maintainer from the Kyomi team will review your PR. We aim to provide feedback within a few business days. Large changes or new datasource drivers may require more thorough review.

## Reporting Bugs

File a bug report via [GitHub Issues](https://github.com/kyomi-ai/kyomi-connect/issues). Include:

- Kyomi Connect version (`kyomi-connect --version`)
- Operating system and architecture
- Database type and version
- Steps to reproduce
- Expected vs. actual behavior
- Relevant log output (redact any credentials or tokens)

## Security Vulnerabilities

**Do not file public issues for security vulnerabilities.**

Report security issues by emailing [security@kyomi.ai](mailto:security@kyomi.ai) or via [GitHub Security Advisories](https://github.com/kyomi-ai/kyomi-connect/security/advisories). We will acknowledge receipt within 48 hours and work with you on a fix before public disclosure.

## License

By contributing to Kyomi Connect, you agree that your contributions will be licensed under the [Apache License, Version 2.0](LICENSE).
