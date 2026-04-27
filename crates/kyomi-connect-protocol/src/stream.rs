//! Streaming query types shared across all crates.
//!
//! Defines the universal streaming currency: [`QueryStreamEvent`] flows from
//! providers -> Connect wire protocol -> browser WebSocket.
//!
//! [`ColumnInfo`] and [`SimpleType`] are the canonical definitions -- downstream
//! crates re-export them.

use std::pin::Pin;

use serde::{Deserialize, Serialize, Serializer};

// ---------------------------------------------------------------------------
// SimpleType -- column type after mapping from provider-specific types
// ---------------------------------------------------------------------------

/// Simplified column type used across all datasource providers.
///
/// Each provider maps its native types (OIDs, type codes, type names) to one
/// of these variants via the functions in `kyomi_datasource::type_mapping`.
///
/// Serializes to a lowercase string (e.g., `SimpleType::String` -> `"string"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SimpleType {
    /// Text / character data.
    String,
    /// Integer or floating-point numeric data.
    Number,
    /// True / false.
    Boolean,
    /// Calendar date without time component.
    Date,
    /// Time of day without date component.
    Time,
    /// Date + time without timezone.
    Timestamp,
    /// Date + time with timezone.
    TimestampTz,
    /// Type could not be mapped.
    Unknown,
}

impl SimpleType {
    /// Lowercase string representation used in API responses.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Date => "date",
            Self::Time => "time",
            Self::Timestamp => "timestamp",
            Self::TimestampTz => "timestamptz",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for SimpleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for SimpleType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SimpleType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "string" => Ok(Self::String),
            "number" => Ok(Self::Number),
            "boolean" => Ok(Self::Boolean),
            "date" => Ok(Self::Date),
            "time" => Ok(Self::Time),
            "timestamp" => Ok(Self::Timestamp),
            "timestamptz" => Ok(Self::TimestampTz),
            "unknown" => Ok(Self::Unknown),
            other => Err(serde::de::Error::custom(format!(
                "unknown SimpleType: '{other}'"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// ColumnInfo
// ---------------------------------------------------------------------------

/// Column metadata returned with query results.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Mapped column type.
    #[serde(rename = "type")]
    pub col_type: SimpleType,
}

// ---------------------------------------------------------------------------
// QueryStreamEvent -- the universal streaming currency
// ---------------------------------------------------------------------------

/// A single event in a streaming query result.
///
/// Every query result -- whether from a direct provider, Connect wire protocol,
/// or browser WebSocket -- is expressed as a sequence of these events:
///
/// `Header` -> `Chunk`* -> `Complete`
///
/// For small results (< 1000 rows), there is exactly one `Chunk`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum QueryStreamEvent {
    /// First event: column metadata and optional row count estimate.
    Header {
        /// Column definitions for the result set.
        columns: Vec<ColumnInfo>,
        /// Estimated total row count, if available. `None` when unknown.
        #[serde(skip_serializing_if = "Option::is_none")]
        total_rows: Option<i64>,
    },
    /// One batch of rows. Sent one or more times between Header and Complete.
    Chunk {
        /// Row data -- each row is a list of JSON values matching column order.
        rows: Vec<Vec<serde_json::Value>>,
        /// Zero-based chunk index for ordering verification.
        chunk_index: u32,
    },
    /// Final event: summary statistics. Signals end of stream.
    Complete {
        /// Wall-clock execution time in milliseconds.
        #[serde(skip_serializing_if = "Option::is_none")]
        execution_time_ms: Option<i64>,
        /// Bytes processed by the query engine (BigQuery, Snowflake, etc.).
        #[serde(skip_serializing_if = "Option::is_none")]
        bytes_processed: Option<i64>,
        /// Number of chunks sent (for verification).
        total_chunks: u32,
        /// Total rows across all chunks.
        total_rows_returned: u64,
    },
}

// ---------------------------------------------------------------------------
// QueryStream -- the stream type alias
// ---------------------------------------------------------------------------

/// A stream of [`QueryStreamEvent`]s.
///
/// This is the return type of `execute_query_stream` on `DatasourceProvider`.
/// Each provider yields `Header` -> `Chunk`* -> `Complete` events.
pub type QueryStream =
    Pin<Box<dyn futures_util::Stream<Item = crate::Result<QueryStreamEvent>> + Send>>;

// ---------------------------------------------------------------------------
// QueryFormat -- requested result format
// ---------------------------------------------------------------------------

/// Requested result format for query execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryFormat {
    /// JSON rows (default, backward compatible).
    #[default]
    Json,
    /// Arrow IPC binary format.
    Arrow,
}

// ---------------------------------------------------------------------------
// base64_bytes -- serde helper for binary data over JSON
// ---------------------------------------------------------------------------

/// Serde helper that encodes `Vec<u8>` as base64 for JSON transport.
pub(crate) mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(&s)
            .map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// ArrowStreamEvent -- streaming currency for Arrow IPC binary data
// ---------------------------------------------------------------------------

/// A streaming event carrying Arrow IPC binary data instead of JSON rows.
///
/// Used when the client requests Arrow format via [`QueryFormat::Arrow`].
/// The flow mirrors [`QueryStreamEvent`]:
///
/// `Schema` -> `Batch`* -> `Complete`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ArrowStreamEvent {
    /// Arrow schema as IPC message bytes (Schema message only, no data).
    Schema {
        /// Arrow IPC schema bytes (serialized via `arrow::ipc::writer::schema_to_bytes`).
        #[serde(with = "base64_bytes")]
        schema_ipc: Vec<u8>,
        /// Column metadata (kept for non-Arrow consumers that need type names).
        columns: Vec<ColumnInfo>,
        /// Estimated total row count, if available.
        #[serde(skip_serializing_if = "Option::is_none")]
        total_rows: Option<i64>,
    },
    /// One Arrow IPC RecordBatch as bytes.
    Batch {
        /// Arrow IPC bytes for one RecordBatch.
        #[serde(with = "base64_bytes")]
        ipc_bytes: Vec<u8>,
        /// Zero-based chunk index.
        chunk_index: u32,
    },
    /// Final event: summary statistics. Signals end of stream.
    Complete {
        /// Wall-clock execution time in milliseconds.
        #[serde(skip_serializing_if = "Option::is_none")]
        execution_time_ms: Option<i64>,
        /// Bytes processed by the query engine (BigQuery, Snowflake, etc.).
        #[serde(skip_serializing_if = "Option::is_none")]
        bytes_processed: Option<i64>,
        /// Number of batches sent (for verification).
        total_chunks: u32,
        /// Total rows across all batches.
        total_rows_returned: u64,
    },
}

// ---------------------------------------------------------------------------
// ArrowStream -- the stream type alias
// ---------------------------------------------------------------------------

/// A stream of [`ArrowStreamEvent`]s.
///
/// This is the return type when a provider yields Arrow IPC data.
/// Each provider yields `Schema` -> `Batch`* -> `Complete` events.
pub type ArrowStream =
    Pin<Box<dyn futures_util::Stream<Item = crate::Result<ArrowStreamEvent>> + Send>>;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    // -- SimpleType tests ----------------------------------------------------

    #[test]
    fn simple_type_as_str() {
        assert_eq!(SimpleType::String.as_str(), "string");
        assert_eq!(SimpleType::Number.as_str(), "number");
        assert_eq!(SimpleType::Boolean.as_str(), "boolean");
        assert_eq!(SimpleType::Date.as_str(), "date");
        assert_eq!(SimpleType::Time.as_str(), "time");
        assert_eq!(SimpleType::Timestamp.as_str(), "timestamp");
        assert_eq!(SimpleType::TimestampTz.as_str(), "timestamptz");
        assert_eq!(SimpleType::Unknown.as_str(), "unknown");
    }

    #[test]
    fn simple_type_display() {
        assert_eq!(SimpleType::String.to_string(), "string");
        assert_eq!(SimpleType::TimestampTz.to_string(), "timestamptz");
    }

    #[test]
    fn simple_type_serializes_as_string() {
        let json = serde_json::to_string(&SimpleType::String).expect("serialize");
        assert_eq!(json, "\"string\"");

        let json = serde_json::to_string(&SimpleType::TimestampTz).expect("serialize");
        assert_eq!(json, "\"timestamptz\"");
    }

    #[test]
    fn simple_type_deserializes_from_string() {
        let parsed: SimpleType = serde_json::from_str("\"number\"").expect("deserialize");
        assert_eq!(parsed, SimpleType::Number);

        let parsed: SimpleType = serde_json::from_str("\"timestamptz\"").expect("deserialize");
        assert_eq!(parsed, SimpleType::TimestampTz);
    }

    #[test]
    fn simple_type_deserialize_unknown_value_fails() {
        let result: Result<SimpleType, _> = serde_json::from_str("\"foobar\"");
        assert!(result.is_err());
    }

    #[test]
    fn simple_type_roundtrip() {
        let types = [
            SimpleType::String,
            SimpleType::Number,
            SimpleType::Boolean,
            SimpleType::Date,
            SimpleType::Time,
            SimpleType::Timestamp,
            SimpleType::TimestampTz,
            SimpleType::Unknown,
        ];
        for t in types {
            let json = serde_json::to_string(&t).expect("serialize");
            let parsed: SimpleType = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(parsed, t);
        }
    }

    // -- ColumnInfo tests ----------------------------------------------------

    #[test]
    fn column_info_serializes_with_type_field() {
        let col = ColumnInfo {
            name: "id".into(),
            col_type: SimpleType::Number,
        };
        let json = serde_json::to_value(&col).expect("serialize");
        assert_eq!(json["name"], "id");
        assert_eq!(json["type"], "number");
    }

    #[test]
    fn column_info_roundtrip() {
        let col = ColumnInfo {
            name: "created_at".into(),
            col_type: SimpleType::TimestampTz,
        };
        let json = serde_json::to_string(&col).expect("serialize");
        let parsed: ColumnInfo = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.name, "created_at");
        assert_eq!(parsed.col_type, SimpleType::TimestampTz);
    }

    // -- QueryStreamEvent tests ----------------------------------------------

    #[test]
    fn header_event_serializes_correctly() {
        let event = QueryStreamEvent::Header {
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
            total_rows: Some(42),
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["event"], "header");
        assert_eq!(json["columns"][0]["name"], "id");
        assert_eq!(json["columns"][0]["type"], "number");
        assert_eq!(json["columns"][1]["name"], "name");
        assert_eq!(json["total_rows"], 42);
    }

    #[test]
    fn header_event_omits_null_total_rows() {
        let event = QueryStreamEvent::Header {
            columns: vec![],
            total_rows: None,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["event"], "header");
        assert!(json.get("total_rows").is_none());
    }

    #[test]
    fn chunk_event_serializes_correctly() {
        let event = QueryStreamEvent::Chunk {
            rows: vec![
                vec![serde_json::json!(1), serde_json::json!("Alice")],
                vec![serde_json::json!(2), serde_json::json!("Bob")],
            ],
            chunk_index: 0,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["event"], "chunk");
        assert_eq!(json["rows"][0][0], 1);
        assert_eq!(json["rows"][0][1], "Alice");
        assert_eq!(json["rows"][1][0], 2);
        assert_eq!(json["chunk_index"], 0);
    }

    #[test]
    fn complete_event_serializes_correctly() {
        let event = QueryStreamEvent::Complete {
            execution_time_ms: Some(123),
            bytes_processed: Some(5_000_000),
            total_chunks: 3,
            total_rows_returned: 2500,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["event"], "complete");
        assert_eq!(json["execution_time_ms"], 123);
        assert_eq!(json["bytes_processed"], 5_000_000);
        assert_eq!(json["total_chunks"], 3);
        assert_eq!(json["total_rows_returned"], 2500);
    }

    #[test]
    fn complete_event_omits_null_optional_fields() {
        let event = QueryStreamEvent::Complete {
            execution_time_ms: None,
            bytes_processed: None,
            total_chunks: 1,
            total_rows_returned: 10,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["event"], "complete");
        assert!(json.get("execution_time_ms").is_none());
        assert!(json.get("bytes_processed").is_none());
        assert_eq!(json["total_chunks"], 1);
        assert_eq!(json["total_rows_returned"], 10);
    }

    #[test]
    fn stream_event_roundtrip_header() {
        let event = QueryStreamEvent::Header {
            columns: vec![ColumnInfo {
                name: "id".into(),
                col_type: SimpleType::Number,
            }],
            total_rows: Some(100),
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: QueryStreamEvent = serde_json::from_str(&json).expect("deserialize");
        match parsed {
            QueryStreamEvent::Header {
                columns,
                total_rows,
            } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "id");
                assert_eq!(total_rows, Some(100));
            }
            _ => panic!("expected Header"),
        }
    }

    #[test]
    fn stream_event_roundtrip_chunk() {
        let event = QueryStreamEvent::Chunk {
            rows: vec![vec![serde_json::json!(42)]],
            chunk_index: 7,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: QueryStreamEvent = serde_json::from_str(&json).expect("deserialize");
        match parsed {
            QueryStreamEvent::Chunk { rows, chunk_index } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], 42);
                assert_eq!(chunk_index, 7);
            }
            _ => panic!("expected Chunk"),
        }
    }

    #[test]
    fn stream_event_roundtrip_complete() {
        let event = QueryStreamEvent::Complete {
            execution_time_ms: Some(999),
            bytes_processed: None,
            total_chunks: 5,
            total_rows_returned: 4200,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: QueryStreamEvent = serde_json::from_str(&json).expect("deserialize");
        match parsed {
            QueryStreamEvent::Complete {
                execution_time_ms,
                bytes_processed,
                total_chunks,
                total_rows_returned,
            } => {
                assert_eq!(execution_time_ms, Some(999));
                assert_eq!(bytes_processed, None);
                assert_eq!(total_chunks, 5);
                assert_eq!(total_rows_returned, 4200);
            }
            _ => panic!("expected Complete"),
        }
    }

    #[test]
    fn stream_event_deserializes_from_raw_json() {
        let raw = r#"{"event":"chunk","rows":[[1,"x"],[2,"y"]],"chunk_index":0}"#;
        let event: QueryStreamEvent = serde_json::from_str(raw).expect("deserialize");
        match event {
            QueryStreamEvent::Chunk { rows, chunk_index } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(chunk_index, 0);
            }
            _ => panic!("expected Chunk"),
        }
    }

    // -- QueryFormat tests ---------------------------------------------------

    #[test]
    fn query_format_default_is_json() {
        assert_eq!(QueryFormat::default(), QueryFormat::Json);
    }

    #[test]
    fn query_format_serializes_as_snake_case() {
        let json = serde_json::to_string(&QueryFormat::Json).expect("serialize");
        assert_eq!(json, "\"json\"");

        let arrow = serde_json::to_string(&QueryFormat::Arrow).expect("serialize");
        assert_eq!(arrow, "\"arrow\"");
    }

    #[test]
    fn query_format_roundtrip() {
        for fmt in [QueryFormat::Json, QueryFormat::Arrow] {
            let json = serde_json::to_string(&fmt).expect("serialize");
            let parsed: QueryFormat = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(parsed, fmt);
        }
    }

    // -- ArrowStreamEvent tests ----------------------------------------------

    #[test]
    fn arrow_schema_event_serializes_with_base64() {
        let raw_bytes = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
        let event = ArrowStreamEvent::Schema {
            schema_ipc: raw_bytes.clone(),
            columns: vec![ColumnInfo {
                name: "id".into(),
                col_type: SimpleType::Number,
            }],
            total_rows: Some(100),
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["event"], "schema");
        // Verify schema_ipc is base64-encoded, not raw bytes
        let encoded = json["schema_ipc"].as_str().expect("should be string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("valid base64");
        assert_eq!(decoded, raw_bytes);
        assert_eq!(json["columns"][0]["name"], "id");
        assert_eq!(json["total_rows"], 100);
    }

    #[test]
    fn arrow_schema_event_omits_null_total_rows() {
        let event = ArrowStreamEvent::Schema {
            schema_ipc: vec![0x42],
            columns: vec![],
            total_rows: None,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert!(json.get("total_rows").is_none());
    }

    #[test]
    fn arrow_batch_event_serializes_with_base64() {
        let ipc_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let event = ArrowStreamEvent::Batch {
            ipc_bytes: ipc_data.clone(),
            chunk_index: 3,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["event"], "batch");
        assert_eq!(json["chunk_index"], 3);
        let encoded = json["ipc_bytes"].as_str().expect("should be string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("valid base64");
        assert_eq!(decoded, ipc_data);
    }

    #[test]
    fn arrow_complete_event_serializes_correctly() {
        let event = ArrowStreamEvent::Complete {
            execution_time_ms: Some(456),
            bytes_processed: Some(1_000_000),
            total_chunks: 5,
            total_rows_returned: 5000,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["event"], "complete");
        assert_eq!(json["execution_time_ms"], 456);
        assert_eq!(json["bytes_processed"], 1_000_000);
        assert_eq!(json["total_chunks"], 5);
        assert_eq!(json["total_rows_returned"], 5000);
    }

    #[test]
    fn arrow_complete_event_omits_null_optional_fields() {
        let event = ArrowStreamEvent::Complete {
            execution_time_ms: None,
            bytes_processed: None,
            total_chunks: 1,
            total_rows_returned: 10,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert!(json.get("execution_time_ms").is_none());
        assert!(json.get("bytes_processed").is_none());
    }

    #[test]
    fn arrow_stream_event_roundtrip_schema() {
        let event = ArrowStreamEvent::Schema {
            schema_ipc: vec![0x00, 0x01, 0x02, 0x03, 0xFF],
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
            total_rows: Some(42),
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: ArrowStreamEvent = serde_json::from_str(&json).expect("deserialize");
        match parsed {
            ArrowStreamEvent::Schema {
                schema_ipc,
                columns,
                total_rows,
            } => {
                assert_eq!(schema_ipc, vec![0x00, 0x01, 0x02, 0x03, 0xFF]);
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[1].name, "name");
                assert_eq!(total_rows, Some(42));
            }
            _ => panic!("expected Schema"),
        }
    }

    #[test]
    fn arrow_stream_event_roundtrip_batch() {
        let event = ArrowStreamEvent::Batch {
            ipc_bytes: vec![0xCA, 0xFE, 0xBA, 0xBE],
            chunk_index: 2,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: ArrowStreamEvent = serde_json::from_str(&json).expect("deserialize");
        match parsed {
            ArrowStreamEvent::Batch {
                ipc_bytes,
                chunk_index,
            } => {
                assert_eq!(ipc_bytes, vec![0xCA, 0xFE, 0xBA, 0xBE]);
                assert_eq!(chunk_index, 2);
            }
            _ => panic!("expected Batch"),
        }
    }

    #[test]
    fn arrow_stream_event_roundtrip_complete() {
        let event = ArrowStreamEvent::Complete {
            execution_time_ms: Some(789),
            bytes_processed: None,
            total_chunks: 3,
            total_rows_returned: 1500,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: ArrowStreamEvent = serde_json::from_str(&json).expect("deserialize");
        match parsed {
            ArrowStreamEvent::Complete {
                execution_time_ms,
                bytes_processed,
                total_chunks,
                total_rows_returned,
            } => {
                assert_eq!(execution_time_ms, Some(789));
                assert_eq!(bytes_processed, None);
                assert_eq!(total_chunks, 3);
                assert_eq!(total_rows_returned, 1500);
            }
            _ => panic!("expected Complete"),
        }
    }

    #[test]
    fn arrow_batch_event_deserializes_from_raw_json() {
        // Manually construct JSON with base64-encoded bytes
        use base64::Engine;
        let bytes = vec![0x01, 0x02, 0x03];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let raw = format!(r#"{{"event":"batch","ipc_bytes":"{}","chunk_index":0}}"#, b64);
        let event: ArrowStreamEvent = serde_json::from_str(&raw).expect("deserialize");
        match event {
            ArrowStreamEvent::Batch {
                ipc_bytes,
                chunk_index,
            } => {
                assert_eq!(ipc_bytes, bytes);
                assert_eq!(chunk_index, 0);
            }
            _ => panic!("expected Batch"),
        }
    }
}
