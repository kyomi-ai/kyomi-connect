//! Streaming conversion helpers for datasource providers.
//!
//! Bridges the buffered [`QueryResult`] world with the Arrow streaming world
//! defined in `kyomi-connect-protocol`.
//!
//! - [`query_result_to_arrow_stream`] — wraps a `QueryResult` (with `record_batch`)
//!   as a 3-event Arrow stream (`Schema` → `Batch` → `Complete`).

use kyomi_connect_protocol::{ArrowStream, ArrowStreamEvent};

use crate::provider::{QueryResult, QueryStatus};

// ---------------------------------------------------------------------------
// query_result_to_arrow_stream
// ---------------------------------------------------------------------------

/// Convert a buffered [`QueryResult`] (with `record_batch` populated) into an
/// [`ArrowStream`].
///
/// Yields exactly three events:
///
/// 1. `Schema` with the Arrow IPC schema bytes, column metadata, and optional
///    total_rows
/// 2. `Batch` with the entire RecordBatch serialized as IPC bytes
///    (chunk_index = 0)
/// 3. `Complete` with execution statistics
///
/// If `record_batch` is `None`, an empty Schema event + Complete event are emitted
/// with zero rows (the Batch event is skipped). This handles providers that did
/// not populate the batch (e.g., DDL statements).
///
/// If the `QueryResult` has `status == Error`, this returns `Err(...)` immediately.
pub fn query_result_to_arrow_stream(
    result: QueryResult,
) -> kyomi_connect_protocol::Result<ArrowStream> {
    if result.status == QueryStatus::Error {
        return Err(kyomi_connect_protocol::Error::Provider(
            result
                .error
                .unwrap_or_else(|| "Query execution failed".into()),
        ));
    }

    let columns = result.columns.unwrap_or_default();

    match result.record_batch {
        Some(batch) => {
            let total_rows_returned = batch.num_rows() as u64;

            let schema_ipc = crate::arrow_builder::schema_to_ipc_bytes(batch.schema_ref())
                .map_err(|e| {
                    kyomi_connect_protocol::Error::Internal(format!(
                        "Arrow schema serialization error: {e}"
                    ))
                })?;

            let ipc_bytes = crate::arrow_builder::batch_to_ipc_bytes(&batch).map_err(|e| {
                kyomi_connect_protocol::Error::Internal(format!(
                    "Arrow batch serialization error: {e}"
                ))
            })?;

            let schema_event = ArrowStreamEvent::Schema {
                schema_ipc,
                columns,
                total_rows: result.total_rows,
            };

            let batch_event = ArrowStreamEvent::Batch {
                ipc_bytes,
                chunk_index: 0,
            };

            let complete_event = ArrowStreamEvent::Complete {
                execution_time_ms: result.execution_time_ms,
                bytes_processed: result.bytes_processed,
                total_chunks: 1,
                total_rows_returned,
            };

            let stream = futures_util::stream::iter(vec![
                Ok(schema_event),
                Ok(batch_event),
                Ok(complete_event),
            ]);

            Ok(Box::pin(stream))
        }
        None => {
            // No record batch (e.g., DDL statement): emit empty Schema + Complete.
            let empty_builder = crate::arrow_builder::ArrowResultBuilder::new(&columns);
            let schema_ipc = crate::arrow_builder::schema_to_ipc_bytes(empty_builder.schema())
                .map_err(|e| {
                    kyomi_connect_protocol::Error::Internal(format!(
                        "Arrow schema serialization error: {e}"
                    ))
                })?;

            let schema_event = ArrowStreamEvent::Schema {
                schema_ipc,
                columns,
                total_rows: result.total_rows,
            };

            let complete_event = ArrowStreamEvent::Complete {
                execution_time_ms: result.execution_time_ms,
                bytes_processed: result.bytes_processed,
                total_chunks: 0,
                total_rows_returned: 0,
            };

            let stream = futures_util::stream::iter(vec![Ok(schema_event), Ok(complete_event)]);

            Ok(Box::pin(stream))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kyomi_connect_protocol::{ColumnInfo, SimpleType};

    fn sample_query_result_with_batch() -> QueryResult {
        use crate::arrow_builder::ArrowResultBuilder;

        let columns = vec![
            ColumnInfo {
                name: "id".into(),
                col_type: SimpleType::Number,
            },
            ColumnInfo {
                name: "name".into(),
                col_type: SimpleType::String,
            },
        ];

        let mut builder = ArrowResultBuilder::new(&columns);
        builder.append_i64(0, 1);
        builder.append_string(1, "Alice");
        builder.finish_row();
        builder.append_i64(0, 2);
        builder.append_string(1, "Bob");
        builder.finish_row();
        let batch = builder.finish().expect("build batch");

        QueryResult {
            status: QueryStatus::Success,
            columns: Some(columns),
            rows: None,
            total_rows: Some(100),
            has_more: false,
            bytes_processed: Some(5_000_000),
            execution_time_ms: Some(42),
            error: None,
            record_batch: Some(batch),
        }
    }

    #[tokio::test]
    async fn arrow_stream_yields_schema_batch_complete() {
        use futures_util::StreamExt;

        let result = sample_query_result_with_batch();
        let stream = query_result_to_arrow_stream(result).expect("should succeed");
        let events: Vec<ArrowStreamEvent> = stream
            .map(|e| e.expect("event should be Ok"))
            .collect()
            .await;

        assert_eq!(events.len(), 3);

        match &events[0] {
            ArrowStreamEvent::Schema {
                columns,
                total_rows,
                ..
            } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert_eq!(*total_rows, Some(100));
            }
            other => panic!("expected Schema, got {other:?}"),
        }

        match &events[1] {
            ArrowStreamEvent::Batch { chunk_index, .. } => {
                assert_eq!(*chunk_index, 0);
            }
            other => panic!("expected Batch, got {other:?}"),
        }

        match &events[2] {
            ArrowStreamEvent::Complete {
                execution_time_ms,
                bytes_processed,
                total_chunks,
                total_rows_returned,
            } => {
                assert_eq!(*execution_time_ms, Some(42));
                assert_eq!(*bytes_processed, Some(5_000_000));
                assert_eq!(*total_chunks, 1);
                assert_eq!(*total_rows_returned, 2);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn arrow_stream_error_result_returns_err() {
        let result = QueryResult::error("something broke");
        match query_result_to_arrow_stream(result) {
            Err(e) => assert!(e.to_string().contains("something broke")),
            Ok(_) => panic!("expected Err for error QueryResult"),
        }
    }

    #[tokio::test]
    async fn arrow_stream_no_batch_emits_empty_schema_and_complete() {
        use futures_util::StreamExt;

        let result = QueryResult {
            status: QueryStatus::Success,
            columns: Some(vec![]),
            rows: None,
            total_rows: None,
            has_more: false,
            bytes_processed: None,
            execution_time_ms: Some(10),
            error: None,
            record_batch: None,
        };

        let stream = query_result_to_arrow_stream(result).expect("should succeed");
        let events: Vec<ArrowStreamEvent> = stream
            .map(|e| e.expect("event should be Ok"))
            .collect()
            .await;

        assert_eq!(events.len(), 2);

        match &events[0] {
            ArrowStreamEvent::Schema { columns, .. } => {
                assert!(columns.is_empty());
            }
            other => panic!("expected Schema, got {other:?}"),
        }

        match &events[1] {
            ArrowStreamEvent::Complete {
                total_rows_returned,
                total_chunks,
                ..
            } => {
                assert_eq!(*total_rows_returned, 0);
                assert_eq!(*total_chunks, 0);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }
}
