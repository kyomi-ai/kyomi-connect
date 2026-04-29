//! Datasource provider trait and result types.
//!
//! Defines the async interface that concrete providers (PostgreSQL, BigQuery,
//! ClickHouse, etc.) must implement, plus the shared result types used for
//! query execution and dry-run validation.
//!
//! Wire-compatible with the Python `datasources/base.py` result types.

use arrow::array::Array;
use serde::{Deserialize, Serialize, Serializer};

// ---------------------------------------------------------------------------
// SimpleType & ColumnInfo — canonical definitions live in kyomi-connect-protocol
// ---------------------------------------------------------------------------

pub use kyomi_connect_protocol::{ColumnInfo, SimpleType};

// ---------------------------------------------------------------------------
// QueryStatus
// ---------------------------------------------------------------------------

/// Status of a query execution.
///
/// Serializes as `"success"` or `"error"` to match the Python wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatus {
    /// Query executed successfully.
    Success,
    /// Query execution failed.
    Error,
}

impl QueryStatus {
    /// Lowercase string representation used in API responses.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

impl Serialize for QueryStatus {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for QueryStatus {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "success" => Ok(Self::Success),
            "error" => Ok(Self::Error),
            other => Err(serde::de::Error::custom(format!(
                "unknown QueryStatus: '{other}'"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// QueryResult
// ---------------------------------------------------------------------------

/// Standard result format for query execution across all datasource providers.
///
/// Uses row-based format for consistency with frontend expectations.
/// Supports pagination with `total_rows` and `has_more` indicators.
///
/// Wire-compatible with Python's `QueryResult` dataclass.
///
/// In addition to the JSON `rows` field, `record_batch` carries the same data
/// in Arrow columnar format for consumers that can use it directly (e.g., the
/// Arrow-native export pipeline). The field is excluded from serialization
/// because `RecordBatch` is not serde-serializable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// `"success"` or `"error"`.
    pub status: QueryStatus,
    /// Column metadata with types. `None` for error responses or dry-run.
    pub columns: Option<Vec<ColumnInfo>>,
    /// Row-based data: each row is a list of JSON values. `None` for errors.
    pub rows: Option<Vec<Vec<serde_json::Value>>>,
    /// Total result count (may be estimated). `None` if not requested or unavailable.
    pub total_rows: Option<i64>,
    /// Whether more pages are available beyond the current result set.
    pub has_more: bool,
    /// Bytes processed by the query engine (BigQuery, Snowflake, etc.).
    pub bytes_processed: Option<i64>,
    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: Option<i64>,
    /// Error message if `status == "error"`.
    pub error: Option<String>,
    /// Arrow columnar representation of the same rows, populated by providers
    /// that implement native Arrow conversion. Skipped during serialization.
    #[serde(skip)]
    pub record_batch: Option<arrow::record_batch::RecordBatch>,
    /// Server-side job identifier for stateful pagination.
    ///
    /// Some providers (e.g., BigQuery) maintain server-side query cursors
    /// identified by a job ID. When paginating, the caller passes this ID back
    /// via `execute_query`'s `job_id` parameter so the provider can resume from
    /// the existing job instead of re-executing the query. `None` for providers
    /// that don't support stateful pagination, or for the first page of a query.
    ///
    /// This field is not part of the JSON wire format — it is only used
    /// internally by the Arrow HTTP endpoint for multi-page streaming.
    #[serde(skip)]
    pub job_id: Option<String>,
}

impl QueryResult {
    /// Create a successful empty result (e.g., for DDL statements).
    #[must_use]
    pub fn success_empty() -> Self {
        Self {
            status: QueryStatus::Success,
            columns: None,
            rows: None,
            total_rows: None,
            has_more: false,
            bytes_processed: None,
            execution_time_ms: None,
            error: None,
            record_batch: None,
            job_id: None,
        }
    }

    /// Create an error result.
    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: QueryStatus::Error,
            columns: None,
            rows: None,
            total_rows: None,
            has_more: false,
            bytes_processed: None,
            execution_time_ms: None,
            error: Some(message.into()),
            record_batch: None,
            job_id: None,
        }
    }
}

// ---------------------------------------------------------------------------
// DryRunResult
// ---------------------------------------------------------------------------

/// Result from dry-run query validation.
///
/// Dry run validates query syntax without executing, using database-native
/// mechanisms like `EXPLAIN` or BigQuery's `dryRun: true` flag.
///
/// Wire-compatible with Python's `DryRunResult` dataclass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunResult {
    /// `true` if the query is syntactically valid.
    pub valid: bool,
    /// Provider-formatted message to display in the UI.
    pub message: String,
    /// Error line number (1-indexed). `None` if not applicable.
    pub line: Option<u32>,
    /// Error column number. `None` if not applicable.
    pub column: Option<u32>,
}

impl DryRunResult {
    /// Create a successful dry-run result.
    #[must_use]
    pub fn success(message: impl Into<String>) -> Self {
        Self {
            valid: true,
            message: message.into(),
            line: None,
            column: None,
        }
    }

    /// Create a failed dry-run result with optional error location.
    #[must_use]
    pub fn failure(message: impl Into<String>, line: Option<u32>, column: Option<u32>) -> Self {
        Self {
            valid: false,
            message: message.into(),
            line,
            column,
        }
    }
}

// ---------------------------------------------------------------------------
// DiscoveryResult
// ---------------------------------------------------------------------------

/// Standard result format for catalog discovery operations.
///
/// Wire-compatible with Python's `DiscoveryResult` dataclass in `base.py`.
/// Used by `list_databases()`, `list_schemas()`, `list_warehouses()`, and
/// `list_catalogs()` trait methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryResult {
    /// Discovered item names (database names, schema names, etc.).
    pub items: Vec<String>,
    /// Error message if discovery failed; `None` on success.
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Arrow batch helpers
// ---------------------------------------------------------------------------

/// Extract string values from a single column of an Arrow [`RecordBatch`].
///
/// Used by discovery methods (`list_databases`, `list_schemas`, etc.) that
/// call `execute_query` and then need to read text values out of the result.
/// Now that `execute_query` sets `rows: None`, callers must read from
/// `record_batch` instead.
///
/// Returns an empty `Vec` if the batch is `None` or the column index is out
/// of range. Only works for `Utf8` (string) columns.
pub fn extract_string_col_from_batch(
    batch: Option<&arrow::record_batch::RecordBatch>,
    col_idx: usize,
) -> Vec<String> {
    let Some(batch) = batch else {
        return Vec::new();
    };
    if col_idx >= batch.num_columns() {
        return Vec::new();
    }
    let col = batch.column(col_idx);
    col.as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .map(|arr| {
            (0..arr.len())
                .filter_map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_string())
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// DatasourceProvider trait
// ---------------------------------------------------------------------------

/// Async trait for datasource providers.
///
/// Each supported datasource type (PostgreSQL, BigQuery, ClickHouse, etc.)
/// implements this trait. The provider knows how to establish connections,
/// execute queries, validate SQL, and clean up resources.
///
/// All methods are async and must be `Send + Sync` safe for use across
/// Tokio tasks.
#[async_trait::async_trait]
pub trait DatasourceProvider: Send + Sync {
    /// Test if a connection can be established with the configured credentials.
    ///
    /// Returns `Ok(true)` on success, or an error describing the failure.
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool>;

    /// Execute a SQL query with optional pagination.
    ///
    /// # Arguments
    /// * `sql` - SQL query to execute.
    /// * `limit` - Maximum rows to return (page size). `None` for no limit.
    /// * `offset` - Number of rows to skip (for pagination). `None` for no offset.
    /// * `include_total` - If `true`, include total row count (may be slow).
    /// * `job_id` - Optional server-side job identifier for stateful pagination.
    ///   Some providers (e.g., BigQuery) maintain server-side query cursors
    ///   identified by a job ID. On the first page, pass `None` to execute the
    ///   query fresh. On subsequent pages, pass back the `job_id` from the
    ///   previous `QueryResult` so the provider can resume from the existing
    ///   server-side cursor without re-executing the query. Ignored by providers
    ///   that don't support stateful pagination.
    async fn execute_query(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
        job_id: Option<&str>,
    ) -> kyomi_connect_protocol::Result<QueryResult>;

    /// Validate SQL syntax without executing.
    ///
    /// Uses database-native mechanisms (e.g., `EXPLAIN`, `dryRun: true`) to
    /// check syntax. Providers should override this to provide actual validation.
    ///
    /// The default implementation returns a success result indicating that
    /// dry-run validation is not available for the provider.
    async fn dry_run(&self, _sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        Ok(DryRunResult::success(
            "Dry run not available for this provider",
        ))
    }

    /// List accessible projects (e.g., GCP projects for BigQuery).
    ///
    /// Only meaningful for datasource types that have a project-level container
    /// (currently BigQuery). Other providers return an error by default.
    ///
    /// Returns a list of project IDs that the user has access to.
    async fn list_projects(&self) -> kyomi_connect_protocol::Result<Vec<String>> {
        Err(kyomi_connect_protocol::Error::NotSupported(
            "Project listing is not supported for this datasource type".into(),
        ))
    }

    /// List accessible databases.
    ///
    /// Implemented by providers that have a database-level container
    /// (PostgreSQL, MySQL, ClickHouse, Snowflake, SQL Server, Synapse).
    ///
    /// Returns a `DiscoveryResult` with item names and optional error.
    async fn list_databases(&self) -> DiscoveryResult {
        DiscoveryResult {
            items: vec![],
            error: Some("Database listing is not supported for this datasource type".into()),
        }
    }

    /// List accessible schemas.
    ///
    /// Implemented by providers that have a schema-level container
    /// (PostgreSQL, Redshift, SQL Server, Synapse, MySQL).
    ///
    /// Returns a `DiscoveryResult` with item names and optional error.
    async fn list_schemas(&self) -> DiscoveryResult {
        DiscoveryResult {
            items: vec![],
            error: Some("Schema listing is not supported for this datasource type".into()),
        }
    }

    /// List accessible warehouses (Snowflake only).
    ///
    /// Returns a `DiscoveryResult` with warehouse names and optional error.
    async fn list_warehouses(&self) -> DiscoveryResult {
        DiscoveryResult {
            items: vec![],
            error: Some("Warehouse listing is not supported for this datasource type".into()),
        }
    }

    /// List accessible catalogs (Databricks Unity Catalog only).
    ///
    /// Returns a `DiscoveryResult` with catalog names and optional error.
    async fn list_catalogs(&self) -> DiscoveryResult {
        DiscoveryResult {
            items: vec![],
            error: Some("Catalog listing is not supported for this datasource type".into()),
        }
    }

    /// Execute a SQL query and return results as a stream of Arrow IPC events.
    ///
    /// The default implementation calls [`execute_query`](Self::execute_query)
    /// and wraps the result using
    /// [`query_result_to_arrow_stream`](crate::stream::query_result_to_arrow_stream).
    /// All rows are delivered in a single batch.
    ///
    /// Providers that support native streaming (e.g., PostgreSQL, MySQL, Redshift)
    /// should override this to yield rows in multiple batches via
    /// [`crate::providers::sqlx_common::drive_sqlx_stream_arrow`].
    ///
    /// # Arguments
    /// * `sql` - SQL query to execute.
    /// * `limit` - Maximum rows to return. `None` for no limit.
    /// * `offset` - Number of rows to skip. `None` for no offset.
    /// * `include_total` - If `true`, include total row count estimate.
    /// * `_chunk_size` - Target rows per batch (unused in default impl).
    async fn execute_query_stream_arrow(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        include_total: bool,
        _chunk_size: Option<u32>,
    ) -> kyomi_connect_protocol::Result<kyomi_connect_protocol::ArrowStream> {
        let result = self
            .execute_query(sql, limit, offset, include_total, None)
            .await?;
        crate::stream::query_result_to_arrow_stream(result)
    }

    /// Clean up any open connections or resources.
    ///
    /// Called when the provider is no longer needed. Implementations should
    /// close connection pools, SSH tunnels, and any other held resources.
    async fn close(&self);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_status_as_str() {
        assert_eq!(QueryStatus::Success.as_str(), "success");
        assert_eq!(QueryStatus::Error.as_str(), "error");
    }

    #[test]
    fn query_status_serializes_as_string() {
        let json = serde_json::to_string(&QueryStatus::Success).expect("serialize");
        assert_eq!(json, "\"success\"");

        let json = serde_json::to_string(&QueryStatus::Error).expect("serialize");
        assert_eq!(json, "\"error\"");
    }

    #[test]
    fn query_status_roundtrip() {
        for status in [QueryStatus::Success, QueryStatus::Error] {
            let json = serde_json::to_string(&status).expect("serialize");
            let parsed: QueryStatus = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn query_result_error_constructor() {
        let result = QueryResult::error("something went wrong");
        assert_eq!(result.status, QueryStatus::Error);
        assert_eq!(result.error.as_deref(), Some("something went wrong"));
        assert!(result.columns.is_none());
        assert!(result.rows.is_none());
        assert!(!result.has_more);
    }

    #[test]
    fn query_result_success_empty_constructor() {
        let result = QueryResult::success_empty();
        assert_eq!(result.status, QueryStatus::Success);
        assert!(result.error.is_none());
        assert!(result.columns.is_none());
        assert!(result.rows.is_none());
        assert!(!result.has_more);
    }

    #[test]
    fn query_result_full_serializes_correctly() {
        let result = QueryResult {
            status: QueryStatus::Success,
            columns: Some(vec![
                ColumnInfo {
                    name: "id".into(),
                    col_type: SimpleType::Number,
                },
                ColumnInfo {
                    name: "name".into(),
                    col_type: SimpleType::String,
                },
            ]),
            rows: Some(vec![
                vec![serde_json::json!(1), serde_json::json!("Alice")],
                vec![serde_json::json!(2), serde_json::json!("Bob")],
            ]),
            total_rows: Some(2),
            has_more: false,
            bytes_processed: Some(5_000_000),
            execution_time_ms: Some(1234),
            error: None,
            record_batch: None,
            job_id: None,
        };
        let json = serde_json::to_value(&result).expect("serialize");
        assert_eq!(json["status"], "success");
        assert_eq!(json["columns"][0]["name"], "id");
        assert_eq!(json["columns"][0]["type"], "number");
        assert_eq!(json["columns"][1]["type"], "string");
        assert_eq!(json["rows"][0][0], 1);
        assert_eq!(json["rows"][0][1], "Alice");
        assert_eq!(json["total_rows"], 2);
        assert_eq!(json["has_more"], false);
        assert_eq!(json["bytes_processed"], 5_000_000);
        assert_eq!(json["execution_time_ms"], 1234);
        assert!(json["error"].is_null());
    }

    #[test]
    fn dry_run_result_success_constructor() {
        let result = DryRunResult::success("Query validated successfully");
        assert!(result.valid);
        assert_eq!(result.message, "Query validated successfully");
        assert!(result.line.is_none());
        assert!(result.column.is_none());
    }

    #[test]
    fn dry_run_result_failure_constructor() {
        let result = DryRunResult::failure("Syntax error near 'FORM'", Some(1), Some(15));
        assert!(!result.valid);
        assert_eq!(result.message, "Syntax error near 'FORM'");
        assert_eq!(result.line, Some(1));
        assert_eq!(result.column, Some(15));
    }

    #[test]
    fn dry_run_result_serializes_correctly() {
        let result = DryRunResult::failure("Error at line 2", Some(2), Some(10));
        let json = serde_json::to_value(&result).expect("serialize");
        assert_eq!(json["valid"], false);
        assert_eq!(json["message"], "Error at line 2");
        assert_eq!(json["line"], 2);
        assert_eq!(json["column"], 10);
    }
}
