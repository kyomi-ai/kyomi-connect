# Team Configuration

- **Team key:** KYO
- **Team name:** Kyomi Connect
- **GitHub repo:** kyomi-ai/kyomi-connect

## Versioning

- **Method:** semver
- **Tag format:** vMAJOR.MINOR.PATCH
- **Version source:** git tag
- Release tags version the distribution (binaries, Docker image, Helm chart).
- Crate versions are independent: bump changed packages in their Cargo.toml and
  workspace dependency declarations before tagging. An existing crate version is
  immutable and will be skipped during publication.

## Release

- **Pipelines:** Release (.github/workflows/release.yml)
- Tag a clean, reviewed main commit after its CI passes.
- CI publishes crates in dependency order through crates.io trusted publishing,
  then creates the GitHub Release, Docker image, and Helm chart.
- Verify the GitHub Release, all Release jobs, and the exact published crate
  versions in the crates.io sparse index. Download kyomi-datasource and confirm
  the released source contains the intended fix before downstream adoption.
- Kyomi adoption requires a separate dependency/lockfile PR in kyomi-ai/kyomi;
  a Connect release alone does not remediate the hosted application.
