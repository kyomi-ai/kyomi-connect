//! Wire protocol types for Kyomi Connect.
//!
//! Defines the request/response message format used between the Kyomi backend
//! and the customer-deployed Connect binary over WebSocket. These types are
//! shared so both sides can depend on them.
//!
//! The protocol is intentionally simple:
//! - Backend sends [`ConnectRequest`] with an operation code and parameters.
//! - Connect sends back [`ConnectResponse`] with a result or error.
//!
//! Result payloads are passed as `serde_json::Value` so the protocol layer
//! doesn't need to know the concrete result types (`QueryResult`, `DryRunResult`,
//! etc.) -- it just passes JSON through. Each side deserializes the params/result
//! into the appropriate typed struct based on the operation.

use serde::{Deserialize, Serialize};

use crate::stream::ColumnInfo;

// ---------------------------------------------------------------------------
// ConnectOp -- operation codes
// ---------------------------------------------------------------------------

/// Operations the backend can request from Connect.
///
/// Serializes to/from snake_case strings (e.g., `ExecuteQuery` <-> `"execute_query"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectOp {
    /// Execute a SQL query and return results.
    ExecuteQuery,
    /// Validate SQL syntax without executing.
    DryRun,
    /// Test if the datasource connection is working.
    TestConnection,
    /// Discover the datasource's catalog (schemas, tables, columns).
    DiscoverCatalog,
}

// ---------------------------------------------------------------------------
// ConnectRequest -- backend -> Connect
// ---------------------------------------------------------------------------

/// Request sent from the Kyomi backend to the Connect binary.
///
/// The `params` field is a raw JSON value whose structure depends on the `op`.
/// The receiver reads `op` first, then deserializes `params` into the appropriate
/// typed struct (e.g., [`QueryParams`] for `execute_query`, [`DryRunParams`] for
/// `dry_run`). Operations like `test_connection` and `discover_catalog` have no
/// parameters, so `params` is `None`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectRequest {
    /// Unique request identifier for correlating responses.
    pub id: String,
    /// The operation to perform.
    pub op: ConnectOp,
    /// Operation-specific parameters. `None` for parameterless operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    /// When `true`, the response will be streamed as multiple messages
    /// (StreamHeader -> StreamChunk* -> StreamComplete). Used by the cross-replica
    /// command listener to choose between oneshot and mpsc response channels.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub streaming: bool,
}

// ---------------------------------------------------------------------------
// Typed parameter structs (serialized into / deserialized from params Value)
// ---------------------------------------------------------------------------

/// Parameters for the `execute_query` operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParams {
    /// SQL query to execute.
    pub sql: String,
    /// Maximum rows to return (page size). `None` for no limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Number of rows to skip (for pagination). `None` for no offset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    /// Whether to include a total row count (may be slow).
    pub include_total: bool,
    /// Requested result format. Defaults to [`QueryFormat::Json`] for backward
    /// compatibility — servers that don't send this field get JSON responses.
    #[serde(default)]
    pub format: crate::stream::QueryFormat,
}

/// Parameters for the `dry_run` operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunParams {
    /// SQL query to validate.
    pub sql: String,
}

// ---------------------------------------------------------------------------
// ConnectResponse -- Connect -> backend
// ---------------------------------------------------------------------------

/// Response sent from the Connect binary to the Kyomi backend.
///
/// The `id` field matches the originating [`ConnectRequest::id`] so the backend
/// can correlate responses to pending requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectResponse {
    /// Request ID this response corresponds to.
    pub id: String,
    /// The response payload -- either a result or an error.
    #[serde(flatten)]
    pub body: ConnectResponseBody,
}

/// The response payload -- a successful result, error, or streaming event.
///
/// Uses `#[serde(tag = "type")]` (internally tagged):
/// - Success: `{"type": "result", "result": <Value>}`
/// - Error: `{"type": "error", "error": "message"}`
/// - Streaming: `{"type": "stream_header", ...}`, `{"type": "stream_chunk", ...}`,
///   `{"type": "stream_complete", ...}`
///
/// Combined with `#[serde(flatten)]` on [`ConnectResponse::body`], the `type`
/// discriminator merges into the top-level JSON alongside `id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConnectResponseBody {
    /// Successful result. The `result` value is a JSON-serialized typed result
    /// (QueryResult, DryRunResult, CatalogResult, or a simple boolean for
    /// test_connection).
    Result { result: serde_json::Value },
    /// Error response with a human-readable message.
    Error { error: String },
    /// First streaming event: column metadata and optional row count estimate.
    StreamHeader {
        /// Column definitions for the result set.
        columns: Vec<ColumnInfo>,
        /// Estimated total row count, if available.
        #[serde(skip_serializing_if = "Option::is_none")]
        total_rows: Option<i64>,
    },
    /// One batch of rows in a streaming response.
    StreamChunk {
        /// Row data -- each row is a list of JSON values matching column order.
        rows: Vec<Vec<serde_json::Value>>,
        /// Zero-based chunk index for ordering verification.
        chunk_index: u32,
    },
    /// Final streaming event: summary statistics. Signals end of stream.
    StreamComplete {
        /// Wall-clock execution time in milliseconds.
        #[serde(skip_serializing_if = "Option::is_none")]
        execution_time_ms: Option<i64>,
        /// Bytes processed by the query engine.
        #[serde(skip_serializing_if = "Option::is_none")]
        bytes_processed: Option<i64>,
        /// Number of chunks sent.
        total_chunks: u32,
        /// Total rows across all chunks.
        total_rows_returned: u64,
    },
    /// Arrow IPC response: schema message with column metadata.
    ///
    /// Sent as the first message when [`QueryParams::format`] is
    /// [`QueryFormat::Arrow`]. The IPC bytes encode the Arrow schema only
    /// (no row data), allowing the receiver to set up its reader before
    /// any batch data arrives.
    ArrowHeader {
        /// Arrow IPC schema bytes (base64-encoded for JSON transport).
        #[serde(with = "crate::stream::base64_bytes")]
        schema_ipc: Vec<u8>,
        /// Column metadata kept for non-Arrow consumers.
        columns: Vec<ColumnInfo>,
        /// Estimated total row count, if available.
        #[serde(skip_serializing_if = "Option::is_none")]
        total_rows: Option<i64>,
    },
    /// Arrow IPC response: one [`RecordBatch`] as IPC stream bytes.
    ///
    /// The IPC bytes use the Arrow streaming format and include the schema
    /// followed by the batch data. The receiver can read it with
    /// `arrow::ipc::reader::StreamReader`.
    ArrowBatch {
        /// Arrow IPC bytes for one RecordBatch (base64-encoded).
        #[serde(with = "crate::stream::base64_bytes")]
        ipc_bytes: Vec<u8>,
        /// Zero-based chunk index for ordering verification.
        chunk_index: u32,
    },
    /// Arrow IPC response: final summary event, signals end of stream.
    ArrowComplete {
        /// Wall-clock execution time in milliseconds.
        #[serde(skip_serializing_if = "Option::is_none")]
        execution_time_ms: Option<i64>,
        /// Bytes processed by the query engine (BigQuery, Snowflake, etc.).
        #[serde(skip_serializing_if = "Option::is_none")]
        bytes_processed: Option<i64>,
        /// Total Arrow batches sent.
        total_chunks: u32,
        /// Total rows across all batches.
        total_rows_returned: u64,
    },
}

// ---------------------------------------------------------------------------
// Catalog types -- returned by discover_catalog
// ---------------------------------------------------------------------------

/// Full catalog discovery result returned by the `discover_catalog` operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogResult {
    /// Top-level containers (schemas, datasets, etc.).
    pub containers: Vec<CatalogContainer>,
}

/// A catalog container (schema, dataset, database, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogContainer {
    /// Container name (e.g., "public", "my_dataset").
    pub name: String,
    /// Tables within this container.
    pub tables: Vec<CatalogTable>,
}

/// A table within a catalog container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogTable {
    /// Table name.
    pub name: String,
    /// Native table type (e.g., "BASE TABLE", "VIEW"). `None` if unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_type: Option<String>,
    /// Columns in this table.
    pub columns: Vec<CatalogColumn>,
}

/// A column within a catalog table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogColumn {
    /// Column name.
    pub name: String,
    /// Native database type (e.g., "int4", "varchar(255)", "TIMESTAMP").
    pub native_type: String,
    /// Column description from database comments. `None` if not available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::{QueryFormat, SimpleType};
    use serde_json::json;

    // -----------------------------------------------------------------------
    // ConnectOp serialization
    // -----------------------------------------------------------------------

    #[test]
    fn connect_op_serializes_to_snake_case() {
        assert_eq!(
            serde_json::to_string(&ConnectOp::ExecuteQuery).unwrap(),
            "\"execute_query\""
        );
        assert_eq!(
            serde_json::to_string(&ConnectOp::DryRun).unwrap(),
            "\"dry_run\""
        );
        assert_eq!(
            serde_json::to_string(&ConnectOp::TestConnection).unwrap(),
            "\"test_connection\""
        );
        assert_eq!(
            serde_json::to_string(&ConnectOp::DiscoverCatalog).unwrap(),
            "\"discover_catalog\""
        );
    }

    #[test]
    fn connect_op_deserializes_from_snake_case() {
        let op: ConnectOp = serde_json::from_str("\"execute_query\"").unwrap();
        assert_eq!(op, ConnectOp::ExecuteQuery);

        let op: ConnectOp = serde_json::from_str("\"dry_run\"").unwrap();
        assert_eq!(op, ConnectOp::DryRun);

        let op: ConnectOp = serde_json::from_str("\"test_connection\"").unwrap();
        assert_eq!(op, ConnectOp::TestConnection);

        let op: ConnectOp = serde_json::from_str("\"discover_catalog\"").unwrap();
        assert_eq!(op, ConnectOp::DiscoverCatalog);
    }

    #[test]
    fn connect_op_roundtrip_all_variants() {
        let ops = [
            ConnectOp::ExecuteQuery,
            ConnectOp::DryRun,
            ConnectOp::TestConnection,
            ConnectOp::DiscoverCatalog,
        ];
        for op in ops {
            let json = serde_json::to_string(&op).unwrap();
            let parsed: ConnectOp = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, op);
        }
    }

    #[test]
    fn connect_op_unknown_value_fails() {
        let result: Result<ConnectOp, _> = serde_json::from_str("\"drop_database\"");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // ConnectRequest serialization
    // -----------------------------------------------------------------------

    #[test]
    fn request_execute_query_serializes_with_params() {
        let params = QueryParams {
            sql: "SELECT 1".into(),
            limit: Some(100),
            offset: None,
            include_total: false,
            format: QueryFormat::Json,
        };
        let req = ConnectRequest {
            id: "req-1".into(),
            op: ConnectOp::ExecuteQuery,
            params: Some(serde_json::to_value(&params).unwrap()),
            streaming: false,
        };

        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["id"], "req-1");
        assert_eq!(json["op"], "execute_query");
        assert_eq!(json["params"]["sql"], "SELECT 1");
        assert_eq!(json["params"]["limit"], 100);
        assert!(json["params"].get("offset").is_none()); // skip_serializing_if
        assert_eq!(json["params"]["include_total"], false);
    }

    #[test]
    fn request_dry_run_serializes_with_params() {
        let params = DryRunParams {
            sql: "SELECT * FROM users".into(),
        };
        let req = ConnectRequest {
            id: "req-2".into(),
            op: ConnectOp::DryRun,
            params: Some(serde_json::to_value(&params).unwrap()),
            streaming: false,
        };

        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["id"], "req-2");
        assert_eq!(json["op"], "dry_run");
        assert_eq!(json["params"]["sql"], "SELECT * FROM users");
    }

    #[test]
    fn request_test_connection_serializes_without_params() {
        let req = ConnectRequest {
            id: "req-3".into(),
            op: ConnectOp::TestConnection,
            params: None,
            streaming: false,
        };

        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["id"], "req-3");
        assert_eq!(json["op"], "test_connection");
        assert!(json.get("params").is_none());
    }

    #[test]
    fn request_discover_catalog_serializes_without_params() {
        let req = ConnectRequest {
            id: "req-4".into(),
            op: ConnectOp::DiscoverCatalog,
            params: None,
            streaming: false,
        };

        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["id"], "req-4");
        assert_eq!(json["op"], "discover_catalog");
        assert!(json.get("params").is_none());
    }

    #[test]
    fn request_roundtrip_with_params() {
        let params = QueryParams {
            sql: "SELECT id, name FROM users LIMIT 10".into(),
            limit: Some(10),
            offset: Some(0),
            include_total: true,
            format: QueryFormat::Json,
        };
        let req = ConnectRequest {
            id: "rt-1".into(),
            op: ConnectOp::ExecuteQuery,
            params: Some(serde_json::to_value(&params).unwrap()),
            streaming: false,
        };

        let serialized = serde_json::to_string(&req).unwrap();
        let deserialized: ConnectRequest = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.id, "rt-1");
        assert_eq!(deserialized.op, ConnectOp::ExecuteQuery);

        let parsed_params: QueryParams =
            serde_json::from_value(deserialized.params.unwrap()).unwrap();
        assert_eq!(parsed_params.sql, "SELECT id, name FROM users LIMIT 10");
        assert_eq!(parsed_params.limit, Some(10));
        assert_eq!(parsed_params.offset, Some(0));
        assert!(parsed_params.include_total);
    }

    #[test]
    fn request_roundtrip_without_params() {
        let req = ConnectRequest {
            id: "rt-2".into(),
            op: ConnectOp::TestConnection,
            params: None,
            streaming: false,
        };

        let serialized = serde_json::to_string(&req).unwrap();
        let deserialized: ConnectRequest = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.id, "rt-2");
        assert_eq!(deserialized.op, ConnectOp::TestConnection);
        assert!(deserialized.params.is_none());
    }

    // -----------------------------------------------------------------------
    // ConnectResponse serialization (tagged format)
    // -----------------------------------------------------------------------

    #[test]
    fn response_success_serializes_with_type_tag() {
        let resp = ConnectResponse {
            id: "req-1".into(),
            body: ConnectResponseBody::Result {
                result: json!({"status": "success", "rows": [[1, "Alice"]]}),
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "req-1");
        assert_eq!(json["type"], "result");
        assert_eq!(json["result"]["status"], "success");
        assert_eq!(json["result"]["rows"][0][0], 1);
        assert!(json.get("error").is_none());
    }

    #[test]
    fn response_error_serializes_with_type_tag() {
        let resp = ConnectResponse {
            id: "req-2".into(),
            body: ConnectResponseBody::Error {
                error: "Connection refused".into(),
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "req-2");
        assert_eq!(json["type"], "error");
        assert_eq!(json["error"], "Connection refused");
        assert!(json.get("result").is_none());
    }

    #[test]
    fn response_success_roundtrip() {
        let resp = ConnectResponse {
            id: "rt-1".into(),
            body: ConnectResponseBody::Result {
                result: json!({"valid": true, "message": "OK"}),
            },
        };

        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: ConnectResponse = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.id, "rt-1");
        match &deserialized.body {
            ConnectResponseBody::Result { result } => {
                assert_eq!(result["valid"], true);
                assert_eq!(result["message"], "OK");
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn response_error_roundtrip() {
        let resp = ConnectResponse {
            id: "rt-2".into(),
            body: ConnectResponseBody::Error {
                error: "Timeout after 30s".into(),
            },
        };

        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: ConnectResponse = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.id, "rt-2");
        match &deserialized.body {
            ConnectResponseBody::Error { error } => {
                assert_eq!(error, "Timeout after 30s");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn response_deserializes_from_raw_json_result() {
        let raw = r#"{"id": "x-1", "type": "result", "result": {"status": "success"}}"#;
        let resp: ConnectResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.id, "x-1");
        assert!(matches!(resp.body, ConnectResponseBody::Result { .. }));
    }

    #[test]
    fn response_deserializes_from_raw_json_error() {
        let raw = r#"{"id": "x-2", "type": "error", "error": "bad SQL"}"#;
        let resp: ConnectResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.id, "x-2");
        match resp.body {
            ConnectResponseBody::Error { ref error } => assert_eq!(error, "bad SQL"),
            _ => panic!("expected Error variant"),
        }
    }

    // -----------------------------------------------------------------------
    // Streaming variant serialization
    // -----------------------------------------------------------------------

    #[test]
    fn response_stream_header_serializes_correctly() {
        let resp = ConnectResponse {
            id: "sh-1".into(),
            body: ConnectResponseBody::StreamHeader {
                columns: vec![
                    ColumnInfo {
                        name: "id".into(),
                        col_type: SimpleType::Number,
                    },
                    ColumnInfo {
                        name: "name".into(),
                        col_type: SimpleType::String,
                    },
                ],
                total_rows: Some(1000),
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "sh-1");
        assert_eq!(json["type"], "stream_header");
        assert_eq!(json["columns"][0]["name"], "id");
        assert_eq!(json["columns"][0]["type"], "number");
        assert_eq!(json["columns"][1]["name"], "name");
        assert_eq!(json["total_rows"], 1000);
    }

    #[test]
    fn response_stream_header_omits_null_total_rows() {
        let resp = ConnectResponse {
            id: "sh-2".into(),
            body: ConnectResponseBody::StreamHeader {
                columns: vec![],
                total_rows: None,
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "stream_header");
        assert!(json.get("total_rows").is_none());
    }

    #[test]
    fn response_stream_chunk_serializes_correctly() {
        let resp = ConnectResponse {
            id: "sc-1".into(),
            body: ConnectResponseBody::StreamChunk {
                rows: vec![vec![json!(1), json!("Alice")], vec![json!(2), json!("Bob")]],
                chunk_index: 0,
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "sc-1");
        assert_eq!(json["type"], "stream_chunk");
        assert_eq!(json["rows"][0][0], 1);
        assert_eq!(json["rows"][0][1], "Alice");
        assert_eq!(json["chunk_index"], 0);
    }

    #[test]
    fn response_stream_complete_serializes_correctly() {
        let resp = ConnectResponse {
            id: "sco-1".into(),
            body: ConnectResponseBody::StreamComplete {
                execution_time_ms: Some(456),
                bytes_processed: Some(10_000_000),
                total_chunks: 5,
                total_rows_returned: 5000,
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "sco-1");
        assert_eq!(json["type"], "stream_complete");
        assert_eq!(json["execution_time_ms"], 456);
        assert_eq!(json["bytes_processed"], 10_000_000);
        assert_eq!(json["total_chunks"], 5);
        assert_eq!(json["total_rows_returned"], 5000);
    }

    #[test]
    fn response_stream_complete_omits_null_optional_fields() {
        let resp = ConnectResponse {
            id: "sco-2".into(),
            body: ConnectResponseBody::StreamComplete {
                execution_time_ms: None,
                bytes_processed: None,
                total_chunks: 1,
                total_rows_returned: 10,
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "stream_complete");
        assert!(json.get("execution_time_ms").is_none());
        assert!(json.get("bytes_processed").is_none());
        assert_eq!(json["total_chunks"], 1);
    }

    #[test]
    fn response_stream_header_roundtrip() {
        let resp = ConnectResponse {
            id: "rt-sh".into(),
            body: ConnectResponseBody::StreamHeader {
                columns: vec![ColumnInfo {
                    name: "id".into(),
                    col_type: SimpleType::Number,
                }],
                total_rows: Some(42),
            },
        };

        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ConnectResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "rt-sh");
        match parsed.body {
            ConnectResponseBody::StreamHeader {
                columns,
                total_rows,
            } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "id");
                assert_eq!(total_rows, Some(42));
            }
            other => panic!("expected StreamHeader, got {other:?}"),
        }
    }

    #[test]
    fn response_stream_chunk_roundtrip() {
        let resp = ConnectResponse {
            id: "rt-sc".into(),
            body: ConnectResponseBody::StreamChunk {
                rows: vec![vec![json!(99)]],
                chunk_index: 3,
            },
        };

        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ConnectResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "rt-sc");
        match parsed.body {
            ConnectResponseBody::StreamChunk { rows, chunk_index } => {
                assert_eq!(rows[0][0], 99);
                assert_eq!(chunk_index, 3);
            }
            other => panic!("expected StreamChunk, got {other:?}"),
        }
    }

    #[test]
    fn response_stream_complete_roundtrip() {
        let resp = ConnectResponse {
            id: "rt-sco".into(),
            body: ConnectResponseBody::StreamComplete {
                execution_time_ms: Some(789),
                bytes_processed: None,
                total_chunks: 2,
                total_rows_returned: 2000,
            },
        };

        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ConnectResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "rt-sco");
        match parsed.body {
            ConnectResponseBody::StreamComplete {
                execution_time_ms,
                bytes_processed,
                total_chunks,
                total_rows_returned,
            } => {
                assert_eq!(execution_time_ms, Some(789));
                assert_eq!(bytes_processed, None);
                assert_eq!(total_chunks, 2);
                assert_eq!(total_rows_returned, 2000);
            }
            other => panic!("expected StreamComplete, got {other:?}"),
        }
    }

    #[test]
    fn response_stream_deserializes_from_raw_json() {
        let raw = r#"{"id":"s-1","type":"stream_chunk","rows":[[1,"x"]],"chunk_index":0}"#;
        let resp: ConnectResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.id, "s-1");
        match resp.body {
            ConnectResponseBody::StreamChunk { rows, chunk_index } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(chunk_index, 0);
            }
            other => panic!("expected StreamChunk, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Full operation round-trips (request -> response for each op)
    // -----------------------------------------------------------------------

    #[test]
    fn full_roundtrip_execute_query() {
        // Request
        let params = QueryParams {
            sql: "SELECT * FROM orders".into(),
            limit: Some(50),
            offset: Some(100),
            include_total: true,
            format: QueryFormat::Json,
        };
        let req = ConnectRequest {
            id: "eq-1".into(),
            op: ConnectOp::ExecuteQuery,
            params: Some(serde_json::to_value(&params).unwrap()),
            streaming: false,
        };
        let req_json = serde_json::to_string(&req).unwrap();
        let req_parsed: ConnectRequest = serde_json::from_str(&req_json).unwrap();
        assert_eq!(req_parsed.op, ConnectOp::ExecuteQuery);
        let parsed_params: QueryParams =
            serde_json::from_value(req_parsed.params.unwrap()).unwrap();
        assert_eq!(parsed_params.sql, "SELECT * FROM orders");
        assert_eq!(parsed_params.limit, Some(50));
        assert_eq!(parsed_params.offset, Some(100));
        assert!(parsed_params.include_total);

        // Response
        let resp = ConnectResponse {
            id: "eq-1".into(),
            body: ConnectResponseBody::Result {
                result: json!({
                    "status": "success",
                    "columns": [{"name": "id", "type": "number"}],
                    "rows": [[1], [2]],
                    "total_rows": 200,
                    "has_more": true,
                    "bytes_processed": null,
                    "execution_time_ms": 42,
                    "error": null
                }),
            },
        };
        let resp_json = serde_json::to_string(&resp).unwrap();
        let resp_parsed: ConnectResponse = serde_json::from_str(&resp_json).unwrap();
        assert_eq!(resp_parsed.id, "eq-1");
        match resp_parsed.body {
            ConnectResponseBody::Result { result } => {
                assert_eq!(result["status"], "success");
                assert_eq!(result["total_rows"], 200);
                assert_eq!(result["has_more"], true);
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn full_roundtrip_dry_run() {
        let params = DryRunParams {
            sql: "SELECT * FORM users".into(),
        };
        let req = ConnectRequest {
            id: "dr-1".into(),
            op: ConnectOp::DryRun,
            params: Some(serde_json::to_value(&params).unwrap()),
            streaming: false,
        };
        let req_json = serde_json::to_string(&req).unwrap();
        let req_parsed: ConnectRequest = serde_json::from_str(&req_json).unwrap();
        assert_eq!(req_parsed.op, ConnectOp::DryRun);
        let parsed_params: DryRunParams =
            serde_json::from_value(req_parsed.params.unwrap()).unwrap();
        assert_eq!(parsed_params.sql, "SELECT * FORM users");

        let resp = ConnectResponse {
            id: "dr-1".into(),
            body: ConnectResponseBody::Result {
                result: json!({
                    "valid": false,
                    "message": "Syntax error near 'FORM'",
                    "line": 1,
                    "column": 10
                }),
            },
        };
        let resp_json = serde_json::to_string(&resp).unwrap();
        let resp_parsed: ConnectResponse = serde_json::from_str(&resp_json).unwrap();
        assert_eq!(resp_parsed.id, "dr-1");
        match resp_parsed.body {
            ConnectResponseBody::Result { result } => {
                assert_eq!(result["valid"], false);
                assert_eq!(result["message"], "Syntax error near 'FORM'");
                assert_eq!(result["line"], 1);
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn full_roundtrip_test_connection() {
        let req = ConnectRequest {
            id: "tc-1".into(),
            op: ConnectOp::TestConnection,
            params: None,
            streaming: false,
        };
        let req_json = serde_json::to_string(&req).unwrap();
        let req_parsed: ConnectRequest = serde_json::from_str(&req_json).unwrap();
        assert_eq!(req_parsed.op, ConnectOp::TestConnection);
        assert!(req_parsed.params.is_none());

        // Success
        let resp = ConnectResponse {
            id: "tc-1".into(),
            body: ConnectResponseBody::Result {
                result: json!(true),
            },
        };
        let resp_json = serde_json::to_string(&resp).unwrap();
        let resp_parsed: ConnectResponse = serde_json::from_str(&resp_json).unwrap();
        assert_eq!(resp_parsed.id, "tc-1");
        match resp_parsed.body {
            ConnectResponseBody::Result { result } => {
                assert_eq!(result, json!(true));
            }
            other => panic!("expected Result, got {other:?}"),
        }

        // Connection failure
        let resp_err = ConnectResponse {
            id: "tc-1".into(),
            body: ConnectResponseBody::Error {
                error: "Connection refused: port 5432".into(),
            },
        };
        let resp_err_json = serde_json::to_string(&resp_err).unwrap();
        let resp_err_parsed: ConnectResponse = serde_json::from_str(&resp_err_json).unwrap();
        match resp_err_parsed.body {
            ConnectResponseBody::Error { error } => {
                assert_eq!(error, "Connection refused: port 5432");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn full_roundtrip_discover_catalog() {
        let req = ConnectRequest {
            id: "dc-1".into(),
            op: ConnectOp::DiscoverCatalog,
            params: None,
            streaming: false,
        };
        let req_json = serde_json::to_string(&req).unwrap();
        let req_parsed: ConnectRequest = serde_json::from_str(&req_json).unwrap();
        assert_eq!(req_parsed.op, ConnectOp::DiscoverCatalog);
        assert!(req_parsed.params.is_none());

        let catalog = CatalogResult {
            containers: vec![CatalogContainer {
                name: "public".into(),
                tables: vec![CatalogTable {
                    name: "users".into(),
                    native_type: Some("BASE TABLE".into()),
                    columns: vec![
                        CatalogColumn {
                            name: "id".into(),
                            native_type: "int4".into(),
                            description: Some("Primary key".into()),
                        },
                        CatalogColumn {
                            name: "email".into(),
                            native_type: "varchar(255)".into(),
                            description: None,
                        },
                    ],
                }],
            }],
        };
        let resp = ConnectResponse {
            id: "dc-1".into(),
            body: ConnectResponseBody::Result {
                result: serde_json::to_value(&catalog).unwrap(),
            },
        };
        let resp_json = serde_json::to_string(&resp).unwrap();
        let resp_parsed: ConnectResponse = serde_json::from_str(&resp_json).unwrap();
        assert_eq!(resp_parsed.id, "dc-1");
        match resp_parsed.body {
            ConnectResponseBody::Result { result } => {
                let parsed_catalog: CatalogResult = serde_json::from_value(result).unwrap();
                assert_eq!(parsed_catalog.containers.len(), 1);
                assert_eq!(parsed_catalog.containers[0].name, "public");
                assert_eq!(parsed_catalog.containers[0].tables[0].name, "users");
                assert_eq!(
                    parsed_catalog.containers[0].tables[0]
                        .native_type
                        .as_deref(),
                    Some("BASE TABLE")
                );
                assert_eq!(parsed_catalog.containers[0].tables[0].columns.len(), 2);
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Param type serialization
    // -----------------------------------------------------------------------

    #[test]
    fn query_params_serialization() {
        let params = QueryParams {
            sql: "SELECT 1".into(),
            limit: Some(10),
            offset: None,
            include_total: false,
            format: QueryFormat::Json,
        };
        let json = serde_json::to_value(&params).unwrap();
        assert_eq!(json["sql"], "SELECT 1");
        assert_eq!(json["limit"], 10);
        assert!(json.get("offset").is_none());
        assert_eq!(json["include_total"], false);
    }

    #[test]
    fn query_params_roundtrip() {
        let params = QueryParams {
            sql: "SELECT * FROM t".into(),
            limit: Some(100),
            offset: Some(50),
            include_total: true,
            format: QueryFormat::Json,
        };
        let json = serde_json::to_string(&params).unwrap();
        let parsed: QueryParams = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sql, "SELECT * FROM t");
        assert_eq!(parsed.limit, Some(100));
        assert_eq!(parsed.offset, Some(50));
        assert!(parsed.include_total);
    }

    #[test]
    fn dry_run_params_roundtrip() {
        let params = DryRunParams {
            sql: "SELECT 1".into(),
        };
        let json = serde_json::to_string(&params).unwrap();
        let parsed: DryRunParams = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sql, "SELECT 1");
    }

    // -----------------------------------------------------------------------
    // Catalog type serialization
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_result_empty_roundtrip() {
        let catalog = CatalogResult { containers: vec![] };
        let json = serde_json::to_string(&catalog).unwrap();
        let parsed: CatalogResult = serde_json::from_str(&json).unwrap();
        assert!(parsed.containers.is_empty());
    }

    #[test]
    fn catalog_result_full_roundtrip() {
        let catalog = CatalogResult {
            containers: vec![
                CatalogContainer {
                    name: "public".into(),
                    tables: vec![CatalogTable {
                        name: "orders".into(),
                        native_type: Some("BASE TABLE".into()),
                        columns: vec![CatalogColumn {
                            name: "total".into(),
                            native_type: "numeric(10,2)".into(),
                            description: Some("Order total in USD".into()),
                        }],
                    }],
                },
                CatalogContainer {
                    name: "analytics".into(),
                    tables: vec![],
                },
            ],
        };
        let json = serde_json::to_string(&catalog).unwrap();
        let parsed: CatalogResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.containers.len(), 2);
        assert_eq!(parsed.containers[0].name, "public");
        assert_eq!(parsed.containers[0].tables[0].name, "orders");
        assert_eq!(
            parsed.containers[0].tables[0].columns[0]
                .description
                .as_deref(),
            Some("Order total in USD")
        );
        assert_eq!(parsed.containers[1].name, "analytics");
        assert!(parsed.containers[1].tables.is_empty());
    }

    #[test]
    fn catalog_table_without_native_type_skips_field() {
        let table = CatalogTable {
            name: "events".into(),
            native_type: None,
            columns: vec![],
        };
        let json = serde_json::to_value(&table).unwrap();
        assert!(json.get("native_type").is_none());
    }

    #[test]
    fn catalog_column_without_description_skips_field() {
        let col = CatalogColumn {
            name: "id".into(),
            native_type: "int4".into(),
            description: None,
        };
        let json = serde_json::to_value(&col).unwrap();
        assert!(json.get("description").is_none());
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn response_with_null_result_value() {
        let resp = ConnectResponse {
            id: "null-1".into(),
            body: ConnectResponseBody::Result {
                result: serde_json::Value::Null,
            },
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ConnectResponse = serde_json::from_str(&json).unwrap();
        match parsed.body {
            ConnectResponseBody::Result { result } => {
                assert!(result.is_null());
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn response_with_empty_error_string() {
        let resp = ConnectResponse {
            id: "empty-err".into(),
            body: ConnectResponseBody::Error {
                error: String::new(),
            },
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ConnectResponse = serde_json::from_str(&json).unwrap();
        match parsed.body {
            ConnectResponseBody::Error { error } => {
                assert!(error.is_empty());
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn response_missing_type_tag_fails() {
        let raw = r#"{"id": "x-3"}"#;
        let result: Result<ConnectResponse, _> = serde_json::from_str(raw);
        assert!(
            result.is_err(),
            "expected deserialization to fail for response with no type tag"
        );
    }

    #[test]
    fn response_unknown_type_tag_fails() {
        let raw = r#"{"id": "x-4", "type": "unknown_variant"}"#;
        let result: Result<ConnectResponse, _> = serde_json::from_str(raw);
        assert!(
            result.is_err(),
            "expected deserialization to fail for unknown type tag"
        );
    }

    // -----------------------------------------------------------------------
    // QueryParams.format field
    // -----------------------------------------------------------------------

    #[test]
    fn query_params_format_defaults_to_json_on_deserialize() {
        // Old wire format without a `format` field — must default to Json.
        let raw = r#"{"sql":"SELECT 1","include_total":false}"#;
        let params: QueryParams = serde_json::from_str(raw).unwrap();
        assert_eq!(params.format, QueryFormat::Json);
    }

    #[test]
    fn query_params_format_arrow_roundtrip() {
        let params = QueryParams {
            sql: "SELECT 1".into(),
            limit: None,
            offset: None,
            include_total: false,
            format: QueryFormat::Arrow,
        };
        let json = serde_json::to_string(&params).unwrap();
        let parsed: QueryParams = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.format, QueryFormat::Arrow);
    }

    // -----------------------------------------------------------------------
    // Arrow IPC wire variants
    // -----------------------------------------------------------------------

    #[test]
    fn response_arrow_header_serializes_correctly() {
        use base64::Engine;
        let schema_bytes = vec![0xAA, 0xBB, 0xCC];
        let resp = ConnectResponse {
            id: "ah-1".into(),
            body: ConnectResponseBody::ArrowHeader {
                schema_ipc: schema_bytes.clone(),
                columns: vec![ColumnInfo {
                    name: "id".into(),
                    col_type: SimpleType::Number,
                }],
                total_rows: Some(500),
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "ah-1");
        assert_eq!(json["type"], "arrow_header");
        // schema_ipc is base64-encoded
        let encoded = json["schema_ipc"].as_str().expect("schema_ipc should be a string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("valid base64");
        assert_eq!(decoded, schema_bytes);
        assert_eq!(json["columns"][0]["name"], "id");
        assert_eq!(json["total_rows"], 500);
    }

    #[test]
    fn response_arrow_header_omits_null_total_rows() {
        let resp = ConnectResponse {
            id: "ah-2".into(),
            body: ConnectResponseBody::ArrowHeader {
                schema_ipc: vec![0x01],
                columns: vec![],
                total_rows: None,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "arrow_header");
        assert!(json.get("total_rows").is_none());
    }

    #[test]
    fn response_arrow_batch_serializes_correctly() {
        use base64::Engine;
        let ipc_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let resp = ConnectResponse {
            id: "ab-1".into(),
            body: ConnectResponseBody::ArrowBatch {
                ipc_bytes: ipc_data.clone(),
                chunk_index: 2,
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "ab-1");
        assert_eq!(json["type"], "arrow_batch");
        assert_eq!(json["chunk_index"], 2);
        let encoded = json["ipc_bytes"].as_str().expect("ipc_bytes should be a string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("valid base64");
        assert_eq!(decoded, ipc_data);
    }

    #[test]
    fn response_arrow_complete_serializes_correctly() {
        let resp = ConnectResponse {
            id: "ac-1".into(),
            body: ConnectResponseBody::ArrowComplete {
                execution_time_ms: Some(789),
                bytes_processed: Some(2_000_000),
                total_chunks: 3,
                total_rows_returned: 3000,
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "ac-1");
        assert_eq!(json["type"], "arrow_complete");
        assert_eq!(json["execution_time_ms"], 789);
        assert_eq!(json["bytes_processed"], 2_000_000);
        assert_eq!(json["total_chunks"], 3);
        assert_eq!(json["total_rows_returned"], 3000);
    }

    #[test]
    fn response_arrow_complete_omits_null_optional_fields() {
        let resp = ConnectResponse {
            id: "ac-2".into(),
            body: ConnectResponseBody::ArrowComplete {
                execution_time_ms: None,
                bytes_processed: None,
                total_chunks: 1,
                total_rows_returned: 10,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "arrow_complete");
        assert!(json.get("execution_time_ms").is_none());
        assert!(json.get("bytes_processed").is_none());
    }

    #[test]
    fn response_arrow_header_roundtrip() {
        let resp = ConnectResponse {
            id: "rt-ah".into(),
            body: ConnectResponseBody::ArrowHeader {
                schema_ipc: vec![0x01, 0x02, 0x03],
                columns: vec![ColumnInfo {
                    name: "col".into(),
                    col_type: SimpleType::String,
                }],
                total_rows: Some(99),
            },
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ConnectResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "rt-ah");
        match parsed.body {
            ConnectResponseBody::ArrowHeader {
                schema_ipc,
                columns,
                total_rows,
            } => {
                assert_eq!(schema_ipc, vec![0x01, 0x02, 0x03]);
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "col");
                assert_eq!(total_rows, Some(99));
            }
            other => panic!("expected ArrowHeader, got {other:?}"),
        }
    }

    #[test]
    fn response_arrow_batch_roundtrip() {
        let resp = ConnectResponse {
            id: "rt-ab".into(),
            body: ConnectResponseBody::ArrowBatch {
                ipc_bytes: vec![0xCA, 0xFE],
                chunk_index: 5,
            },
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ConnectResponse = serde_json::from_str(&json).unwrap();
        match parsed.body {
            ConnectResponseBody::ArrowBatch {
                ipc_bytes,
                chunk_index,
            } => {
                assert_eq!(ipc_bytes, vec![0xCA, 0xFE]);
                assert_eq!(chunk_index, 5);
            }
            other => panic!("expected ArrowBatch, got {other:?}"),
        }
    }

    #[test]
    fn response_arrow_complete_roundtrip() {
        let resp = ConnectResponse {
            id: "rt-ac".into(),
            body: ConnectResponseBody::ArrowComplete {
                execution_time_ms: Some(100),
                bytes_processed: None,
                total_chunks: 2,
                total_rows_returned: 200,
            },
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ConnectResponse = serde_json::from_str(&json).unwrap();
        match parsed.body {
            ConnectResponseBody::ArrowComplete {
                execution_time_ms,
                bytes_processed,
                total_chunks,
                total_rows_returned,
            } => {
                assert_eq!(execution_time_ms, Some(100));
                assert_eq!(bytes_processed, None);
                assert_eq!(total_chunks, 2);
                assert_eq!(total_rows_returned, 200);
            }
            other => panic!("expected ArrowComplete, got {other:?}"),
        }
    }
}
