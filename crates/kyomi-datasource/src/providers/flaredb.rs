//! FlareDB datasource provider using Arrow Flight SQL.
//!
//! Implements query execution for FlareDB databases using the Arrow Flight SQL
//! protocol via gRPC. FlareDB is built on Apache DataFusion + Arrow + Parquet,
//! and exposes a Flight SQL endpoint for zero-copy Arrow data transfer.
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `host` | string | `"localhost"` | FlareDB server hostname |
//! | `port` | int | `8815` | Flight SQL gRPC port |
