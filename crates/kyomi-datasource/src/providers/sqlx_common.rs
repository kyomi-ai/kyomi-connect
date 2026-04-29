//! Shared helpers for datasource providers.
//!
//! Contains three categories of shared code:
//!
//! - **Query preparation**: [`prepare_query`] (SQL normalisation + pagination)
//!   and [`prepare_query_databricks`] (Databricks-specific variant). Used by
//!   PostgreSQL, MySQL, Redshift, ClickHouse, Snowflake, and Databricks.
//!
//! - **Arrow stream channel**: [`make_arrow_stream_channel`] creates an mpsc
//!   channel and converts the receiver into an [`ArrowStream`]. Used by all
//!   providers that implement `execute_query_stream_arrow`.
//!
//! - **sqlx Arrow streaming driver**: [`drive_sqlx_stream_arrow`] drives a
//!   sqlx row stream through the Schema → Batch* → Complete event protocol.
//!   Used by PostgreSQL, MySQL, and Redshift.

use std::time::Instant;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "redshift"))]
use futures_util::StreamExt;

use crate::provider::ColumnInfo;
use kyomi_connect_protocol::Error;

use crate::arrow_builder::ArrowResultBuilder;
use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use kyomi_connect_protocol::ArrowStreamEvent;

// ---------------------------------------------------------------------------
// prepare_query — shared SQL normalisation + pagination
// ---------------------------------------------------------------------------

/// Result of query preparation: the final SQL and whether it is a SELECT.
pub(crate) struct PreparedQuery {
    /// The final SQL string with pagination applied (if applicable).
    pub sql: String,
    /// The stripped SQL (no trailing semicolons, no pagination) — for count queries.
    pub sql_stripped: String,
    /// Whether the original SQL is a SELECT/WITH (used to decide total-count).
    pub is_select: bool,
}

/// Strip trailing semicolons, detect SELECT/WITH, and apply LIMIT/OFFSET
/// pagination for providers that use a default 1000-row limit.
///
/// Used by PostgreSQL, MySQL, ClickHouse, and Snowflake (all share the same
/// normalisation rules).
pub(crate) fn prepare_query(sql: &str, limit: Option<u32>, offset: Option<u32>) -> PreparedQuery {
    let sql_stripped = sql.trim().trim_end_matches(';').trim();
    let sql_upper = sql_stripped.to_uppercase();
    let is_select = sql_upper.starts_with("SELECT") || sql_upper.starts_with("WITH");

    let effective_limit = limit.unwrap_or(1000);
    let effective_offset = offset.unwrap_or(0);

    let paginated_sql = if is_select && !sql_upper.contains("LIMIT") {
        format!("{sql_stripped} LIMIT {effective_limit} OFFSET {effective_offset}")
    } else {
        sql_stripped.to_string()
    };

    PreparedQuery {
        sql: paginated_sql,
        sql_stripped: sql_stripped.to_string(),
        is_select,
    }
}

/// Result of Databricks query preparation.
#[cfg(feature = "databricks")]
pub(crate) struct PreparedQueryDatabricks {
    /// The final SQL string with pagination applied (if applicable).
    pub sql: String,
    /// The stripped SQL (no trailing semicolons, no pagination) — for count queries.
    pub sql_stripped: String,
    /// Whether the original SQL is a SELECT/WITH.
    pub is_select: bool,
    /// Whether the SQL is a metadata command (SHOW, DESCRIBE, DESC).
    pub is_metadata_command: bool,
}

/// Prepare a query for Databricks, which has different pagination rules:
/// - No default 1000-row limit (only applies LIMIT when explicitly provided)
/// - Skips metadata commands (SHOW, DESCRIBE, DESC) from total count
#[cfg(feature = "databricks")]
pub(crate) fn prepare_query_databricks(
    sql: &str,
    limit: Option<u32>,
    offset: Option<u32>,
) -> PreparedQueryDatabricks {
    let sql_stripped = sql.trim().trim_end_matches(';').trim();
    let sql_upper = sql_stripped.to_uppercase();
    let is_select = sql_upper.starts_with("SELECT") || sql_upper.starts_with("WITH");

    let is_metadata_command = sql_upper.starts_with("SHOW ")
        || sql_upper.starts_with("DESCRIBE ")
        || sql_upper.starts_with("DESC ");

    let paginated_sql = if let Some(lim) = limit {
        if !sql_upper.contains("LIMIT") {
            let effective_offset = offset.unwrap_or(0);
            format!("{sql_stripped} LIMIT {lim} OFFSET {effective_offset}")
        } else {
            sql_stripped.to_string()
        }
    } else {
        sql_stripped.to_string()
    };

    PreparedQueryDatabricks {
        sql: paginated_sql,
        sql_stripped: sql_stripped.to_string(),
        is_select,
        is_metadata_command,
    }
}

// ---------------------------------------------------------------------------
// Arrow streaming helpers
// ---------------------------------------------------------------------------

/// Serialize an Arrow [`Schema`] to IPC stream bytes.
///
/// Creates a StreamWriter, which writes the schema as its header, then
/// immediately finishes. The resulting bytes contain just the schema message
/// and the end-of-stream marker.
pub(crate) fn schema_to_ipc_bytes(schema: &Schema) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)?;
    writer.finish()?;
    Ok(buf)
}

/// Create an mpsc channel and convert the receiver into an [`ArrowStream`].
///
/// Returns `(tx, stream)` where `tx` is the sender to pass to
/// [`drive_sqlx_stream_arrow`] and `stream` is the `ArrowStream` to return
/// from `execute_query_stream_arrow`.
pub(crate) fn make_arrow_stream_channel() -> (
    tokio::sync::mpsc::Sender<kyomi_connect_protocol::Result<ArrowStreamEvent>>,
    kyomi_connect_protocol::ArrowStream,
) {
    let (tx, rx) =
        tokio::sync::mpsc::channel::<kyomi_connect_protocol::Result<ArrowStreamEvent>>(4);

    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    (tx, Box::pin(stream))
}

/// Drive a sqlx row stream through the Arrow Schema -> Batch* -> Complete
/// event protocol.
///
/// This is the Arrow counterpart of the deleted JSON streaming driver. Instead of
/// collecting JSON rows into `Vec<Vec<Value>>` chunks, it uses
/// [`ArrowResultBuilder`] to build Arrow RecordBatches and emits
/// [`ArrowStreamEvent`] events.
///
/// The caller supplies:
/// - `tx` — sender half of the mpsc channel
/// - `row_stream` — an already-created sqlx row stream
/// - `total_rows` — pre-computed total count (or None)
/// - `chunk_size` — rows per batch event
/// - `start` — timer started before the query began
/// - `extract_columns` — closure: `&R -> Vec<ColumnInfo>` (called once on first row)
/// - `convert_row_arrow` — closure: `(&R, &[ColumnInfo], &mut ArrowResultBuilder)`
///   that appends column values and calls `builder.finish_row()`
#[cfg(any(feature = "postgres", feature = "mysql", feature = "redshift"))]
pub(crate) async fn drive_sqlx_stream_arrow<R, S, FC, FR>(
    tx: tokio::sync::mpsc::Sender<kyomi_connect_protocol::Result<ArrowStreamEvent>>,
    row_stream: S,
    total_rows: Option<i64>,
    chunk_size: usize,
    start: Instant,
    extract_columns: FC,
    convert_row_arrow: FR,
) where
    R: Send,
    S: futures_util::Stream<Item = Result<R, sqlx::Error>> + Unpin,
    FC: FnOnce(&R) -> Vec<ColumnInfo>,
    FR: Fn(&R, &[ColumnInfo], &mut ArrowResultBuilder),
{
    let mut row_stream = std::pin::pin!(row_stream);

    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut columns_ready = false;
    let mut extract_columns = Some(extract_columns);
    let mut builder: Option<ArrowResultBuilder> = None;
    let mut chunk_index: u32 = 0;
    let mut total_rows_returned: u64 = 0;

    loop {
        let next = tokio::time::timeout(crate::DATASOURCE_TIMEOUT_QUERY, row_stream.next()).await;

        let row_item = match next {
            Ok(Some(Ok(row))) => Some(row),
            Ok(Some(Err(e))) => {
                let _ = tx
                    .send(Err(Error::Internal(format!("Query error: {e}"))))
                    .await;
                return;
            }
            Ok(None) => None,
            Err(_) => {
                let _ = tx
                    .send(Err(Error::Internal(format!(
                        "Query timed out after {}s",
                        crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                    ))))
                    .await;
                return;
            }
        };

        match row_item {
            Some(row) => {
                // Extract column metadata from the first row and send Schema event
                if !columns_ready {
                    if let Some(extract) = extract_columns.take() {
                        columns = extract(&row);
                    }
                    columns_ready = true;

                    let arrow_builder = ArrowResultBuilder::new(&columns);
                    let schema_ipc = match schema_to_ipc_bytes(arrow_builder.schema()) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            let _ = tx
                                .send(Err(Error::Internal(format!(
                                    "Arrow schema serialization error: {e}"
                                ))))
                                .await;
                            return;
                        }
                    };

                    if tx
                        .send(Ok(ArrowStreamEvent::Schema {
                            schema_ipc,
                            columns: columns.clone(),
                            total_rows,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }

                    builder = Some(arrow_builder);
                }

                // Convert row to Arrow values
                let b = builder
                    .as_mut()
                    .expect("builder initialized with first row");
                convert_row_arrow(&row, &columns, b);
                total_rows_returned += 1;

                // Flush batch when full
                if b.row_count() >= chunk_size {
                    let full_builder = builder.take().unwrap();
                    match full_builder.finish_to_ipc() {
                        Ok(ipc_bytes) => {
                            if tx
                                .send(Ok(ArrowStreamEvent::Batch {
                                    ipc_bytes,
                                    chunk_index,
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            chunk_index += 1;
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Err(Error::Internal(format!(
                                    "Arrow IPC serialization error: {e}"
                                ))))
                                .await;
                            return;
                        }
                    }
                    builder = Some(ArrowResultBuilder::new(&columns));
                }
            }
            None => {
                // Stream exhausted -- emit schema if we never got any rows
                if !columns_ready {
                    let empty_builder = ArrowResultBuilder::new(&[]);
                    let schema_ipc = match schema_to_ipc_bytes(empty_builder.schema()) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            let _ = tx
                                .send(Err(Error::Internal(format!(
                                    "Arrow schema serialization error: {e}"
                                ))))
                                .await;
                            return;
                        }
                    };

                    if tx
                        .send(Ok(ArrowStreamEvent::Schema {
                            schema_ipc,
                            columns: Vec::new(),
                            total_rows,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }

                // Flush remaining rows
                if let Some(remaining) = builder.take().filter(|b| b.row_count() > 0) {
                    match remaining.finish_to_ipc() {
                        Ok(ipc_bytes) => {
                            if tx
                                .send(Ok(ArrowStreamEvent::Batch {
                                    ipc_bytes,
                                    chunk_index,
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            chunk_index += 1;
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Err(Error::Internal(format!(
                                    "Arrow IPC serialization error: {e}"
                                ))))
                                .await;
                            return;
                        }
                    }
                }

                let execution_time_ms = start.elapsed().as_millis() as i64;
                let _ = tx
                    .send(Ok(ArrowStreamEvent::Complete {
                        execution_time_ms: Some(execution_time_ms),
                        bytes_processed: None,
                        total_chunks: chunk_index,
                        total_rows_returned,
                    }))
                    .await;
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_query_strips_semicolons_and_whitespace() {
        let pq = prepare_query("  SELECT 1 ;  ", None, None);
        assert_eq!(pq.sql, "SELECT 1 LIMIT 1000 OFFSET 0");
        assert!(pq.is_select);
    }

    #[test]
    fn prepare_query_applies_custom_limit_offset() {
        let pq = prepare_query("SELECT * FROM t", Some(50), Some(10));
        assert_eq!(pq.sql, "SELECT * FROM t LIMIT 50 OFFSET 10");
        assert!(pq.is_select);
    }

    #[test]
    fn prepare_query_preserves_existing_limit() {
        let pq = prepare_query("SELECT * FROM t LIMIT 5", Some(50), Some(10));
        assert_eq!(pq.sql, "SELECT * FROM t LIMIT 5");
        assert!(pq.is_select);
    }

    #[test]
    fn prepare_query_non_select() {
        let pq = prepare_query("INSERT INTO t VALUES (1)", None, None);
        assert_eq!(pq.sql, "INSERT INTO t VALUES (1)");
        assert!(!pq.is_select);
    }

    #[test]
    fn prepare_query_with_cte() {
        let pq = prepare_query("WITH cte AS (SELECT 1) SELECT * FROM cte", None, None);
        assert!(pq.is_select);
        assert!(pq.sql.contains("LIMIT 1000"));
    }

    #[cfg(feature = "databricks")]
    #[test]
    fn prepare_query_databricks_no_default_limit() {
        let pq = prepare_query_databricks("SELECT * FROM t", None, None);
        assert_eq!(pq.sql, "SELECT * FROM t");
        assert!(pq.is_select);
        assert!(!pq.is_metadata_command);
    }

    #[cfg(feature = "databricks")]
    #[test]
    fn prepare_query_databricks_with_explicit_limit() {
        let pq = prepare_query_databricks("SELECT * FROM t", Some(100), Some(20));
        assert_eq!(pq.sql, "SELECT * FROM t LIMIT 100 OFFSET 20");
        assert!(pq.is_select);
    }

    #[cfg(feature = "databricks")]
    #[test]
    fn prepare_query_databricks_metadata_command() {
        let pq = prepare_query_databricks("SHOW CATALOGS", None, None);
        assert!(pq.is_metadata_command);
        let pq = prepare_query_databricks("DESCRIBE TABLE t", None, None);
        assert!(pq.is_metadata_command);
        let pq = prepare_query_databricks("DESC TABLE t", None, None);
        assert!(pq.is_metadata_command);
    }
}
