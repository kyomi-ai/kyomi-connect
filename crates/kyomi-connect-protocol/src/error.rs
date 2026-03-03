//! Lightweight error types for the Connect protocol.
//!
//! These errors are backend-agnostic — no sqlx, redis, or axum dependencies.
//! The [`Error`] enum covers the small set of failure modes relevant to the
//! wire protocol and provider abstraction layer.

/// Errors that can occur in the Connect protocol layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A provider-level error (e.g., query execution failure).
    #[error("{0}")]
    Provider(String),

    /// Connection to the datasource failed.
    #[error("connection failed: {0}")]
    Connection(String),

    /// The requested operation is not supported.
    #[error("not supported: {0}")]
    NotSupported(String),

    /// An internal error that does not fit other categories.
    #[error("{0}")]
    Internal(String),

    /// JSON serialization/deserialization error.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

/// Convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
