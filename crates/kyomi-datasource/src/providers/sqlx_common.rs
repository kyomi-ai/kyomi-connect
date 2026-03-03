//! Shared helpers for datasource providers.
//!
//! Contains three categories of shared code:
//!
//! - **Query preparation**: [`prepare_query`] (SQL normalisation + pagination)
//!   and [`prepare_query_databricks`] (Databricks-specific variant). Used by
//!   PostgreSQL, MySQL, Redshift, ClickHouse, Snowflake, and Databricks.
//!
//! - **Stream channel**: [`make_stream_channel`] creates an mpsc channel and
//!   converts the receiver into a [`QueryStream`](kyomi_connect_protocol::QueryStream).
//!   Used by all providers.
//!
//! - **sqlx streaming driver**: [`drive_sqlx_stream`] drives a sqlx row stream
//!   through the Header → Chunk* → Complete event protocol. Used by
//!   PostgreSQL, MySQL, and Redshift.

use std::time::Instant;

use futures_util::StreamExt;
use serde_json::Value;

use crate::provider::ColumnInfo;
use kyomi_connect_protocol::QueryStreamEvent;
use kyomi_connect_protocol::Error;

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
pub(crate) fn prepare_query(
    sql: &str,
    limit: Option<u32>,
    offset: Option<u32>,
) -> PreparedQuery {
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
// run_sqlx_stream — shared streaming boilerplate
// ---------------------------------------------------------------------------

/// Drive a sqlx row stream through the standard Header -> Chunk* -> Complete
/// event protocol.
///
/// This is the async body of the spawned task. Callers should use
/// [`make_stream_channel`] to create the channel and stream, then
/// spawn this function as a tokio task with the sender half.
///
/// The caller supplies:
/// - `tx` — sender half of the mpsc channel
/// - `row_stream` — an already-created sqlx row stream (must be created in the
///   same scope where the SQL string lives, i.e., inside the spawned task)
/// - `total_rows` — pre-computed total count (or None)
/// - `chunk_size` — rows per chunk event
/// - `start` — timer started before the query began
/// - `extract_columns` — closure: `&R -> Vec<ColumnInfo>` (called once on first row)
/// - `convert_row` — closure: `(&R, &[ColumnInfo]) -> Vec<Value>`
#[cfg(any(feature = "postgres", feature = "mysql", feature = "redshift"))]
pub(crate) async fn drive_sqlx_stream<R, S, FC, FR>(
    tx: tokio::sync::mpsc::Sender<kyomi_connect_protocol::Result<QueryStreamEvent>>,
    row_stream: S,
    total_rows: Option<i64>,
    chunk_size: usize,
    start: Instant,
    extract_columns: FC,
    convert_row: FR,
) where
    R: Send,
    S: futures_util::Stream<Item = Result<R, sqlx::Error>> + Unpin,
    FC: FnOnce(&R) -> Vec<ColumnInfo>,
    FR: Fn(&R, &[ColumnInfo]) -> Vec<Value>,
{
    let mut row_stream = std::pin::pin!(row_stream);

    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut columns_ready = false;
    let mut extract_columns = Some(extract_columns);
    let mut chunk_buffer: Vec<Vec<Value>> = Vec::with_capacity(chunk_size);
    let mut chunk_index: u32 = 0;
    let mut total_rows_returned: u64 = 0;

    loop {
        // Apply per-row timeout so a stalled connection doesn't hang forever.
        let next = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_QUERY,
            row_stream.next(),
        )
        .await;

        let row_item = match next {
            Ok(Some(Ok(row))) => Some(row),
            Ok(Some(Err(e))) => {
                let _ = tx
                    .send(Err(Error::Internal(format!("Query error: {e}"))))
                    .await;
                return;
            }
            Ok(None) => None, // Stream exhausted
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
                // Extract column metadata from the first row
                if !columns_ready {
                    if let Some(extract) = extract_columns.take() {
                        columns = extract(&row);
                    }
                    columns_ready = true;

                    if tx
                        .send(Ok(QueryStreamEvent::Header {
                            columns: columns.clone(),
                            total_rows,
                        }))
                        .await
                        .is_err()
                    {
                        return; // Consumer dropped
                    }
                }

                // Convert row to JSON values
                let row_values = convert_row(&row, &columns);
                chunk_buffer.push(row_values);
                total_rows_returned += 1;

                // Flush chunk when full
                if chunk_buffer.len() >= chunk_size {
                    let rows = std::mem::replace(
                        &mut chunk_buffer,
                        Vec::with_capacity(chunk_size),
                    );
                    if tx
                        .send(Ok(QueryStreamEvent::Chunk {
                            rows,
                            chunk_index,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    chunk_index += 1;
                }
            }
            None => {
                // Stream exhausted -- emit header if we never got any rows
                if !columns_ready {
                    if tx
                        .send(Ok(QueryStreamEvent::Header {
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
                if !chunk_buffer.is_empty() {
                    let rows = std::mem::take(&mut chunk_buffer);
                    if tx
                        .send(Ok(QueryStreamEvent::Chunk {
                            rows,
                            chunk_index,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    chunk_index += 1;
                }

                let execution_time_ms = start.elapsed().as_millis() as i64;
                let _ = tx
                    .send(Ok(QueryStreamEvent::Complete {
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

/// Create an mpsc channel and convert the receiver into a [`QueryStream`].
///
/// Returns `(tx, stream)` where `tx` is the sender to pass to
/// [`drive_sqlx_stream`] and `stream` is the `QueryStream` to return
/// from `execute_query_stream`.
pub(crate) fn make_stream_channel() -> (
    tokio::sync::mpsc::Sender<kyomi_connect_protocol::Result<QueryStreamEvent>>,
    kyomi_connect_protocol::QueryStream,
) {
    // Buffer of 4 events: allows pipeline of Header + a few Chunks.
    // Each Chunk event carries chunk_size rows.
    let (tx, rx) = tokio::sync::mpsc::channel::<kyomi_connect_protocol::Result<QueryStreamEvent>>(4);

    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    (tx, Box::pin(stream))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kyomi_connect_protocol::SimpleType;

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

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "redshift"))]
    #[tokio::test]
    async fn drive_sqlx_stream_with_empty_stream() {
        let (tx, stream) = make_stream_channel();

        tokio::spawn(async move {
            let empty_stream =
                futures_util::stream::empty::<Result<Vec<Value>, sqlx::Error>>();
            drive_sqlx_stream(
                tx,
                empty_stream,
                Some(42),
                100,
                Instant::now(),
                |_row: &Vec<Value>| vec![],
                |_row: &Vec<Value>, _cols: &[ColumnInfo]| vec![],
            )
            .await;
        });

        let events: Vec<QueryStreamEvent> = stream
            .map(|e| e.expect("event should be Ok"))
            .collect()
            .await;

        assert_eq!(events.len(), 2); // Header + Complete (no Chunks)

        match &events[0] {
            QueryStreamEvent::Header { columns, total_rows } => {
                assert!(columns.is_empty());
                assert_eq!(*total_rows, Some(42));
            }
            other => panic!("expected Header, got {other:?}"),
        }

        match &events[1] {
            QueryStreamEvent::Complete { total_rows_returned, .. } => {
                assert_eq!(*total_rows_returned, 0);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "redshift"))]
    #[tokio::test]
    async fn drive_sqlx_stream_with_rows() {
        let (tx, stream) = make_stream_channel();

        tokio::spawn(async move {
            let rows: Vec<Result<Vec<Value>, sqlx::Error>> = vec![
                Ok(vec![serde_json::json!(1), serde_json::json!("Alice")]),
                Ok(vec![serde_json::json!(2), serde_json::json!("Bob")]),
                Ok(vec![serde_json::json!(3), serde_json::json!("Charlie")]),
            ];
            let row_stream = futures_util::stream::iter(rows);

            drive_sqlx_stream(
                tx,
                row_stream,
                None,
                2, // chunk_size = 2
                Instant::now(),
                |_row: &Vec<Value>| {
                    vec![
                        ColumnInfo { name: "id".into(), col_type: SimpleType::Number },
                        ColumnInfo { name: "name".into(), col_type: SimpleType::String },
                    ]
                },
                |row: &Vec<Value>, _cols: &[ColumnInfo]| row.clone(),
            )
            .await;
        });

        let events: Vec<QueryStreamEvent> = stream
            .map(|e| e.expect("event should be Ok"))
            .collect()
            .await;

        // Header + Chunk(2 rows) + Chunk(1 row) + Complete = 4
        assert_eq!(events.len(), 4);

        match &events[0] {
            QueryStreamEvent::Header { columns, .. } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
            }
            other => panic!("expected Header, got {other:?}"),
        }

        match &events[1] {
            QueryStreamEvent::Chunk { rows, chunk_index } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(*chunk_index, 0);
            }
            other => panic!("expected Chunk, got {other:?}"),
        }

        match &events[2] {
            QueryStreamEvent::Chunk { rows, chunk_index } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(*chunk_index, 1);
            }
            other => panic!("expected Chunk, got {other:?}"),
        }

        match &events[3] {
            QueryStreamEvent::Complete { total_rows_returned, total_chunks, .. } => {
                assert_eq!(*total_rows_returned, 3);
                assert_eq!(*total_chunks, 2);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }
}
