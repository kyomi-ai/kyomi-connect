//! Shared upstream SQLx API with the portable transport driver extensions.
pub use sqlx_core::column::Column;
#[cfg(all(test, feature = "ssh"))]
pub use sqlx_core::connection::Connection;
pub use sqlx_core::error::Error;
pub use sqlx_core::query::query;
pub use sqlx_core::query_scalar::query_scalar;
pub use sqlx_core::row::Row;
pub use sqlx_core::type_info::TypeInfo;
#[cfg(feature = "mysql")]
pub use sqlx_mysql::{self as mysql, MySqlPool};
#[cfg(feature = "postgres")]
pub use sqlx_postgres::{self as postgres, PgPool};
