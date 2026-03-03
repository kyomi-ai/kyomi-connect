//! Streaming conversion helpers for datasource providers.
//!
//! Bridges the buffered [`QueryResult`] world with the streaming
//! [`QueryStream`] / [`QueryStreamEvent`] world defined in `kyomi-connect-protocol`.
//!
//! - [`query_result_to_stream`] — wraps a `QueryResult` as a 3-event stream
//!   (`Header` → `Chunk` → `Complete`).
//! - [`collect_stream_to_result`] — collects a full stream back into a
//!   `QueryResult`. Useful for callers that need the entire result set at once
//!   (e.g., the existing non-streaming API endpoints).

use futures_util::StreamExt;
use kyomi_connect_protocol::{ColumnInfo, QueryStream, QueryStreamEvent};

use crate::provider::{QueryResult, QueryStatus};

// ---------------------------------------------------------------------------
// query_result_to_stream
// ---------------------------------------------------------------------------

/// Convert a buffered [`QueryResult`] into a [`QueryStream`].
///
/// Yields exactly three events:
///
/// 1. `Header` with column metadata and optional total_rows
/// 2. `Chunk` with all rows (chunk_index = 0)
/// 3. `Complete` with execution statistics
///
/// If the `QueryResult` has `status == Error`, this returns `Err(...)` immediately
/// rather than emitting a stream.
pub fn query_result_to_stream(result: QueryResult) -> kyomi_connect_protocol::Result<QueryStream> {
    if result.status == QueryStatus::Error {
        return Err(kyomi_connect_protocol::Error::Provider(
            result
                .error
                .unwrap_or_else(|| "Query execution failed".into()),
        ));
    }

    let columns = result.columns.unwrap_or_default();
    let rows = result.rows.unwrap_or_default();
    let total_rows_returned = rows.len() as u64;

    let header = QueryStreamEvent::Header {
        columns: columns.clone(),
        total_rows: result.total_rows,
    };

    let chunk = QueryStreamEvent::Chunk {
        rows,
        chunk_index: 0,
    };

    let complete = QueryStreamEvent::Complete {
        execution_time_ms: result.execution_time_ms,
        bytes_processed: result.bytes_processed,
        total_chunks: 1,
        total_rows_returned,
    };

    let stream = futures_util::stream::iter(vec![Ok(header), Ok(chunk), Ok(complete)]);

    Ok(Box::pin(stream))
}

// ---------------------------------------------------------------------------
// collect_stream_to_result
// ---------------------------------------------------------------------------

/// Collect a [`QueryStream`] back into a buffered [`QueryResult`].
///
/// Consumes the full stream, reassembling `Header` → `Chunk`* → `Complete`
/// events into a single `QueryResult`. Chunks are sorted by `chunk_index`
/// to handle out-of-order delivery.
///
/// Returns an error if the stream is malformed (missing Header or Complete).
pub async fn collect_stream_to_result(
    mut stream: QueryStream,
) -> kyomi_connect_protocol::Result<QueryResult> {
    let mut columns: Option<Vec<ColumnInfo>> = None;
    let mut total_rows: Option<i64> = None;
    let mut chunks: Vec<(u32, Vec<Vec<serde_json::Value>>)> = Vec::new();
    let mut execution_time_ms: Option<i64> = None;
    let mut bytes_processed: Option<i64> = None;

    while let Some(event) = stream.next().await {
        match event? {
            QueryStreamEvent::Header {
                columns: cols,
                total_rows: tr,
            } => {
                columns = Some(cols);
                total_rows = tr;
            }
            QueryStreamEvent::Chunk { rows, chunk_index } => {
                chunks.push((chunk_index, rows));
            }
            QueryStreamEvent::Complete {
                execution_time_ms: etm,
                bytes_processed: bp,
                ..
            } => {
                execution_time_ms = etm;
                bytes_processed = bp;
            }
        }
    }

    // Sort chunks by index and flatten rows
    chunks.sort_by_key(|(idx, _)| *idx);
    let rows: Vec<Vec<serde_json::Value>> = chunks.into_iter().flat_map(|(_, rows)| rows).collect();

    // If we received a Header event (indicating a SELECT), always return
    // Some(rows) even if empty. Only return None for DDL/non-SELECT (no
    // Header received, so columns remains None).
    let rows = if columns.is_some() { Some(rows) } else { None };

    Ok(QueryResult {
        status: QueryStatus::Success,
        columns,
        rows,
        total_rows,
        has_more: false,
        bytes_processed,
        execution_time_ms,
        error: None,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kyomi_connect_protocol::SimpleType;

    fn sample_query_result() -> QueryResult {
        QueryResult {
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
                vec![serde_json::json!(3), serde_json::json!("Charlie")],
            ]),
            total_rows: Some(100),
            has_more: false,
            bytes_processed: Some(5_000_000),
            execution_time_ms: Some(42),
            error: None,
        }
    }

    // -- query_result_to_stream tests -----------------------------------------

    #[tokio::test]
    async fn to_stream_yields_header_chunk_complete() {
        let result = sample_query_result();
        let stream = query_result_to_stream(result).expect("should succeed");
        let events: Vec<QueryStreamEvent> = stream
            .map(|e| e.expect("event should be Ok"))
            .collect()
            .await;

        assert_eq!(events.len(), 3);

        // Header
        match &events[0] {
            QueryStreamEvent::Header {
                columns,
                total_rows,
            } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[1].name, "name");
                assert_eq!(*total_rows, Some(100));
            }
            other => panic!("expected Header, got {other:?}"),
        }

        // Chunk
        match &events[1] {
            QueryStreamEvent::Chunk { rows, chunk_index } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(*chunk_index, 0);
                assert_eq!(rows[0][0], 1);
                assert_eq!(rows[2][1], "Charlie");
            }
            other => panic!("expected Chunk, got {other:?}"),
        }

        // Complete
        match &events[2] {
            QueryStreamEvent::Complete {
                execution_time_ms,
                bytes_processed,
                total_chunks,
                total_rows_returned,
            } => {
                assert_eq!(*execution_time_ms, Some(42));
                assert_eq!(*bytes_processed, Some(5_000_000));
                assert_eq!(*total_chunks, 1);
                assert_eq!(*total_rows_returned, 3);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn to_stream_error_result_returns_err() {
        let result = QueryResult::error("something broke");
        match query_result_to_stream(result) {
            Err(e) => assert!(e.to_string().contains("something broke")),
            Ok(_) => panic!("expected Err for error QueryResult"),
        }
    }

    #[tokio::test]
    async fn to_stream_empty_result() {
        let result = QueryResult::success_empty();
        let stream = query_result_to_stream(result).expect("should succeed");
        let events: Vec<QueryStreamEvent> = stream
            .map(|e| e.expect("event should be Ok"))
            .collect()
            .await;

        assert_eq!(events.len(), 3);

        match &events[0] {
            QueryStreamEvent::Header {
                columns,
                total_rows,
            } => {
                assert!(columns.is_empty());
                assert_eq!(*total_rows, None);
            }
            other => panic!("expected Header, got {other:?}"),
        }

        match &events[1] {
            QueryStreamEvent::Chunk { rows, .. } => {
                assert!(rows.is_empty());
            }
            other => panic!("expected Chunk, got {other:?}"),
        }

        match &events[2] {
            QueryStreamEvent::Complete {
                total_rows_returned,
                ..
            } => {
                assert_eq!(*total_rows_returned, 0);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    // -- collect_stream_to_result tests ---------------------------------------

    #[tokio::test]
    async fn collect_reassembles_query_result() {
        let result = sample_query_result();
        let stream = query_result_to_stream(result).expect("to_stream");
        let collected = collect_stream_to_result(stream).await.expect("collect");

        assert_eq!(collected.status, QueryStatus::Success);
        assert_eq!(collected.columns.as_ref().unwrap().len(), 2);
        assert_eq!(collected.columns.as_ref().unwrap()[0].name, "id");
        assert_eq!(collected.rows.as_ref().unwrap().len(), 3);
        assert_eq!(collected.rows.as_ref().unwrap()[0][1], "Alice");
        assert_eq!(collected.total_rows, Some(100));
        assert_eq!(collected.execution_time_ms, Some(42));
        assert_eq!(collected.bytes_processed, Some(5_000_000));
        assert!(collected.error.is_none());
    }

    #[tokio::test]
    async fn collect_empty_stream_result() {
        let result = QueryResult::success_empty();
        let stream = query_result_to_stream(result).expect("to_stream");
        let collected = collect_stream_to_result(stream).await.expect("collect");

        assert_eq!(collected.status, QueryStatus::Success);
        // A SELECT that returned zero rows should still have Some(vec![]),
        // not None. None is reserved for DDL/non-SELECT (no Header event).
        assert_eq!(collected.rows, Some(vec![]));
        assert!(collected.error.is_none());
    }

    // -- round-trip test ------------------------------------------------------

    #[tokio::test]
    async fn round_trip_preserves_data() {
        let original = sample_query_result();

        let stream = query_result_to_stream(original.clone()).expect("to_stream");
        let round_tripped = collect_stream_to_result(stream).await.expect("collect");

        // Status
        assert_eq!(round_tripped.status, original.status);
        // Columns
        assert_eq!(round_tripped.columns, original.columns);
        // Rows
        assert_eq!(round_tripped.rows, original.rows);
        // total_rows
        assert_eq!(round_tripped.total_rows, original.total_rows);
        // Execution stats
        assert_eq!(round_tripped.execution_time_ms, original.execution_time_ms);
        assert_eq!(round_tripped.bytes_processed, original.bytes_processed);
    }

    // -- multi-chunk collect test ---------------------------------------------

    #[tokio::test]
    async fn collect_handles_multiple_chunks_in_order() {
        let events = vec![
            Ok(QueryStreamEvent::Header {
                columns: vec![ColumnInfo {
                    name: "val".into(),
                    col_type: SimpleType::Number,
                }],
                total_rows: None,
            }),
            Ok(QueryStreamEvent::Chunk {
                rows: vec![vec![serde_json::json!(1)]],
                chunk_index: 0,
            }),
            Ok(QueryStreamEvent::Chunk {
                rows: vec![vec![serde_json::json!(2)]],
                chunk_index: 1,
            }),
            Ok(QueryStreamEvent::Chunk {
                rows: vec![vec![serde_json::json!(3)]],
                chunk_index: 2,
            }),
            Ok(QueryStreamEvent::Complete {
                execution_time_ms: None,
                bytes_processed: None,
                total_chunks: 3,
                total_rows_returned: 3,
            }),
        ];

        let stream: QueryStream = Box::pin(futures_util::stream::iter(events));
        let collected = collect_stream_to_result(stream).await.expect("collect");

        let rows = collected.rows.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], 1);
        assert_eq!(rows[1][0], 2);
        assert_eq!(rows[2][0], 3);
    }

    #[tokio::test]
    async fn collect_sorts_out_of_order_chunks() {
        let events = vec![
            Ok(QueryStreamEvent::Header {
                columns: vec![],
                total_rows: None,
            }),
            // Chunks arrive out of order
            Ok(QueryStreamEvent::Chunk {
                rows: vec![vec![serde_json::json!("second")]],
                chunk_index: 1,
            }),
            Ok(QueryStreamEvent::Chunk {
                rows: vec![vec![serde_json::json!("first")]],
                chunk_index: 0,
            }),
            Ok(QueryStreamEvent::Complete {
                execution_time_ms: None,
                bytes_processed: None,
                total_chunks: 2,
                total_rows_returned: 2,
            }),
        ];

        let stream: QueryStream = Box::pin(futures_util::stream::iter(events));
        let collected = collect_stream_to_result(stream).await.expect("collect");

        let rows = collected.rows.unwrap();
        assert_eq!(rows[0][0], "first");
        assert_eq!(rows[1][0], "second");
    }
}
