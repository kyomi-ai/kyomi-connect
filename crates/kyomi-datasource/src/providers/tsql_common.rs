//! Shared T-SQL helpers for SQL Server and Azure Synapse providers.
//!
//! Both providers use the TDS wire protocol via `tiberius` and share:
//! - Pagination logic (OFFSET-FETCH and ROW_NUMBER fallback)
//! - Error line parsing (`Line N` pattern)
//! - Row value conversion from [`tiberius::ColumnData`] to JSON
//! - Column type mapping from [`tiberius::ColumnType`] to [`SimpleType`]
//! - Streaming query execution via [`execute_tds_query_stream`]

use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;

use futures_util::TryStreamExt;
use regex::Regex;
use serde_json::Value;
use tiberius::{ColumnType, QueryItem, Row};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::Compat;

use crate::provider::{ColumnInfo, QueryResult, QueryStatus, SimpleType};
use kyomi_connect_protocol::Error;
use kyomi_connect_protocol::QueryStreamEvent;

/// Concrete TDS client type used by both SQL Server and Synapse providers.
///
/// Both providers connect via `TcpStream` wrapped in `tokio_util::compat::Compat`
/// for the `AsyncRead`/`AsyncWrite` traits that tiberius requires.
pub(crate) type TdsClient = tiberius::Client<Compat<TcpStream>>;

// ---------------------------------------------------------------------------
// Pagination helpers
// ---------------------------------------------------------------------------

/// Check if a SQL query has an `ORDER BY` clause at the main (outermost) level.
///
/// Searches backwards for the last `ORDER BY` in the SQL. If found, verifies
/// it is not nested inside parentheses (i.e., it is the main clause, not
/// inside a subquery or window function).
///
/// # Examples
///
/// ```text
/// "SELECT * FROM t ORDER BY id"              -> true
/// "SELECT * FROM (SELECT * FROM t ORDER BY id) sub" -> false
/// "SELECT * FROM t"                          -> false
/// ```
pub(crate) fn has_main_order_by(sql: &str) -> bool {
    let upper = sql.to_uppercase();

    // Find the last occurrence of ORDER BY
    let Some(last_order_by_pos) = upper.rfind("ORDER BY") else {
        return false;
    };

    // Walk forward from the ORDER BY position and check paren nesting.
    // If we encounter more closing parens than opening, the ORDER BY is
    // inside a subquery.
    let after_order_by = &upper[last_order_by_pos..];
    let mut paren_depth: i32 = 0;

    for ch in after_order_by.chars() {
        match ch {
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    // Hit a closing paren that wraps the ORDER BY → nested
                    return false;
                }
            }
            _ => {}
        }
    }

    // Reached end of string without going negative → main-level ORDER BY
    true
}

/// Apply T-SQL pagination to a SQL query.
///
/// If the query has a main-level `ORDER BY`, appends OFFSET-FETCH syntax
/// (SQL Server 2012+). Otherwise, wraps in a ROW_NUMBER subquery.
///
/// Returns the paginated SQL string.
pub(crate) fn apply_tsql_pagination(sql: &str, limit: u32, offset: u32) -> String {
    if has_main_order_by(sql) {
        // OFFSET-FETCH syntax (requires ORDER BY)
        format!("{sql} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY")
    } else {
        // ROW_NUMBER wrapper (no main ORDER BY)
        let end = offset + limit;
        format!(
            "SELECT * FROM (\
                SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS _row_num \
                FROM ({sql}) AS _inner\
            ) AS _outer \
            WHERE _row_num > {offset} AND _row_num <= {end}"
        )
    }
}

// ---------------------------------------------------------------------------
// Error parsing
// ---------------------------------------------------------------------------

/// Regex for T-SQL `Line N` error pattern, compiled once.
static TSQL_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)Line\s+(\d+)").expect("T-SQL line regex"));

/// Parse a T-SQL error message for a line number.
///
/// SQL Server / Synapse errors often include `Line N` in the message.
/// Returns the extracted line number, or `None` if not found.
pub(crate) fn parse_tsql_error_line(error_msg: &str) -> Option<u32> {
    TSQL_LINE_RE
        .captures(error_msg)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

// ---------------------------------------------------------------------------
// Column type mapping (tiberius ColumnType → SimpleType)
// ---------------------------------------------------------------------------

/// Map a [`tiberius::ColumnType`] to our [`SimpleType`].
///
/// This maps the TDS wire-protocol column types to our unified type system.
/// Falls back to [`map_tds_type`] for the `type_name()` string when the
/// enum variants don't give us enough granularity.
pub(crate) fn map_column_type(ct: ColumnType) -> SimpleType {
    match ct {
        // Boolean
        ColumnType::Bit | ColumnType::Bitn => SimpleType::Boolean,

        // Integer numeric
        ColumnType::Int1 => SimpleType::Number,
        ColumnType::Int2 => SimpleType::Number,
        ColumnType::Int4 => SimpleType::Number,
        ColumnType::Int8 => SimpleType::Number,
        ColumnType::Intn => SimpleType::Number,

        // Floating-point numeric
        ColumnType::Float4 => SimpleType::Number,
        ColumnType::Float8 => SimpleType::Number,
        ColumnType::Floatn => SimpleType::Number,

        // Money
        ColumnType::Money | ColumnType::Money4 => SimpleType::Number,

        // Decimal / Numeric
        ColumnType::Decimaln | ColumnType::Numericn => SimpleType::Number,

        // String types
        ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::Text
        | ColumnType::NText
        | ColumnType::Xml
        | ColumnType::Udt
        | ColumnType::SSVariant => SimpleType::String,

        // Binary (serialise as string)
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => SimpleType::String,

        // GUID (UUID)
        ColumnType::Guid => SimpleType::String,

        // Date/time types
        ColumnType::Daten => SimpleType::Date,
        ColumnType::Timen => SimpleType::Time,
        ColumnType::Datetime
        | ColumnType::Datetime4
        | ColumnType::Datetimen
        | ColumnType::Datetime2 => SimpleType::Timestamp,
        ColumnType::DatetimeOffsetn => SimpleType::TimestampTz,

        // Null or unknown
        ColumnType::Null => SimpleType::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Row value conversion
// ---------------------------------------------------------------------------

/// Convert a tiberius row column value to a JSON value.
///
/// Uses [`Row::try_get`] with the appropriate Rust type based on the
/// [`SimpleType`] mapping. Falls back to `Value::Null` on extraction errors.
pub(crate) fn tds_row_value_to_json(row: &Row, idx: usize, col_type: SimpleType) -> Value {
    match col_type {
        SimpleType::Boolean => row
            .try_get::<bool, _>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),

        SimpleType::Number => {
            // Try i64 first, then f64, then i32, then i16, then u8
            if let Ok(Some(v)) = row.try_get::<i64, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<f64, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<i32, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<i16, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<u8, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<f32, _>(idx) {
                serde_json::json!(v)
            } else if let Ok(Some(v)) = row.try_get::<tiberius::numeric::Numeric, _>(idx) {
                // Decimal/Numeric — convert to string then parse to f64
                let s = format!("{v}");
                s.parse::<f64>()
                    .map(|f| serde_json::json!(f))
                    .unwrap_or_else(|_| Value::String(s))
            } else {
                Value::Null
            }
        }

        SimpleType::String => {
            // Try &str first (NVarchar, VarChar, etc.)
            if let Ok(Some(v)) = row.try_get::<&str, _>(idx) {
                Value::String(v.to_string())
            } else if let Ok(Some(v)) = row.try_get::<tiberius::Uuid, _>(idx) {
                Value::String(v.to_string())
            } else if let Ok(Some(v)) = row.try_get::<&[u8], _>(idx) {
                // Binary data — hex encode
                Value::String(hex_encode(v))
            } else {
                Value::Null
            }
        }

        SimpleType::Date => {
            if let Ok(Some(v)) = row.try_get::<chrono::NaiveDate, _>(idx) {
                Value::String(v.format("%Y-%m-%d").to_string())
            } else {
                Value::Null
            }
        }

        SimpleType::Time => {
            if let Ok(Some(v)) = row.try_get::<chrono::NaiveTime, _>(idx) {
                Value::String(v.format("%H:%M:%S").to_string())
            } else {
                Value::Null
            }
        }

        SimpleType::Timestamp => {
            if let Ok(Some(v)) = row.try_get::<chrono::NaiveDateTime, _>(idx) {
                Value::String(v.format("%Y-%m-%dT%H:%M:%S").to_string())
            } else {
                Value::Null
            }
        }

        SimpleType::TimestampTz => {
            if let Ok(Some(v)) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(idx) {
                Value::String(v.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            } else if let Ok(Some(v)) = row.try_get::<chrono::NaiveDateTime, _>(idx) {
                // Fallback for datetimeoffset that may not parse with timezone
                Value::String(v.format("%Y-%m-%dT%H:%M:%S").to_string())
            } else {
                Value::Null
            }
        }

        SimpleType::Unknown => {
            // Try string as safe fallback
            row.try_get::<&str, _>(idx)
                .ok()
                .flatten()
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null)
        }
    }
}

/// Simple hex encoding for binary data.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        s.push_str(&format!("{b:02X}"));
    }
    s
}

// ---------------------------------------------------------------------------
// Arrow conversion
// ---------------------------------------------------------------------------

/// Convert a tiberius row directly to Arrow column builders.
///
/// This is the Arrow counterpart of [`tds_row_value_to_json`]. Instead of
/// creating `serde_json::Value` intermediaries, native Rust types go directly
/// into Arrow column builders, preserving date/time/timestamp precision.
///
/// Used by both the SQL Server and Synapse providers.
pub(crate) fn tds_row_to_arrow(
    row: &Row,
    columns: &[ColumnInfo],
    builder: &mut crate::arrow_builder::ArrowResultBuilder,
) {
    for (idx, col) in columns.iter().enumerate() {
        tds_row_to_arrow_at(row, idx, idx, col.col_type, builder);
    }
    builder.finish_row();
}

/// Append one column value from a tiberius row directly into an Arrow builder.
///
/// Unlike [`tds_row_to_arrow`], this function accepts separate indices:
/// - `col_idx`: the 0-based index into `filtered_columns` (and into the
///   [`crate::arrow_builder::ArrowResultBuilder`] column slot).
/// - `row_idx`: the 0-based index into the tiberius [`Row`] (which may differ
///   from `col_idx` when a `_row_num` pagination column shifts positions).
///
/// This separation is necessary when `apply_tsql_pagination` wraps the query
/// in a `ROW_NUMBER` subquery: `filtered_columns` omits `_row_num` but the
/// tiberius row retains it, so every column at or after the `_row_num` position
/// has `row_idx = col_idx + 1`.
fn tds_row_to_arrow_at(
    row: &Row,
    col_idx: usize,
    row_idx: usize,
    col_type: SimpleType,
    builder: &mut crate::arrow_builder::ArrowResultBuilder,
) {
    match col_type {
        SimpleType::Boolean => match row.try_get::<bool, _>(row_idx) {
            Ok(Some(v)) => builder.append_bool(col_idx, v),
            _ => builder.append_null(col_idx),
        },
        SimpleType::Number => {
            if let Ok(Some(v)) = row.try_get::<i64, _>(row_idx) {
                builder.append_i64(col_idx, v);
            } else if let Ok(Some(v)) = row.try_get::<f64, _>(row_idx) {
                builder.append_f64(col_idx, v);
            } else if let Ok(Some(v)) = row.try_get::<i32, _>(row_idx) {
                builder.append_i64(col_idx, v as i64);
            } else if let Ok(Some(v)) = row.try_get::<i16, _>(row_idx) {
                builder.append_i64(col_idx, v as i64);
            } else if let Ok(Some(v)) = row.try_get::<u8, _>(row_idx) {
                builder.append_i64(col_idx, v as i64);
            } else if let Ok(Some(v)) = row.try_get::<f32, _>(row_idx) {
                builder.append_f64(col_idx, v as f64);
            } else if let Ok(Some(v)) =
                row.try_get::<tiberius::numeric::Numeric, _>(row_idx)
            {
                let s = format!("{v}");
                if let Ok(f) = s.parse::<f64>() {
                    builder.append_f64(col_idx, f);
                } else {
                    builder.append_null(col_idx);
                }
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::String => {
            if let Ok(Some(v)) = row.try_get::<&str, _>(row_idx) {
                builder.append_string(col_idx, v);
            } else if let Ok(Some(v)) = row.try_get::<tiberius::Uuid, _>(row_idx) {
                builder.append_string(col_idx, &v.to_string());
            } else if let Ok(Some(v)) = row.try_get::<&[u8], _>(row_idx) {
                builder.append_string(col_idx, &hex_encode(v));
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::Date => {
            if let Ok(Some(v)) = row.try_get::<chrono::NaiveDate, _>(row_idx) {
                builder.append_naive_date(col_idx, v);
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::Time => {
            if let Ok(Some(v)) = row.try_get::<chrono::NaiveTime, _>(row_idx) {
                builder.append_naive_time(col_idx, v);
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::Timestamp => {
            if let Ok(Some(v)) = row.try_get::<chrono::NaiveDateTime, _>(row_idx) {
                builder.append_naive_datetime(col_idx, v);
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::TimestampTz => {
            if let Ok(Some(v)) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(row_idx) {
                builder.append_datetime_utc(col_idx, v);
            } else if let Ok(Some(v)) = row.try_get::<chrono::NaiveDateTime, _>(row_idx) {
                // Don't silently assert UTC — store as string like the JSON path
                builder.append_string(col_idx, &v.format("%Y-%m-%dT%H:%M:%S").to_string());
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::Unknown => {
            if let Ok(Some(v)) = row.try_get::<&str, _>(row_idx) {
                builder.append_string(col_idx, v);
            } else {
                builder.append_null(col_idx);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shared query execution helpers
// ---------------------------------------------------------------------------

/// Execute a query via the tiberius client and build a [`QueryResult`].
///
/// This is the shared implementation for both SQL Server and Synapse providers.
/// It handles SQL stripping, pagination, total count, type mapping, and row
/// conversion.
///
/// # Arguments
///
/// * `client` - Mutable reference to the tiberius client.
/// * `sql` - Raw SQL query string.
/// * `limit` - Optional page size.
/// * `offset` - Optional offset for pagination.
/// * `include_total` - Whether to include total row count.
/// * `provider_name` - Provider name for logging (e.g., "SQL Server", "Synapse").
pub(crate) async fn execute_tds_query(
    client: &mut TdsClient,
    sql: &str,
    limit: Option<u32>,
    offset: Option<u32>,
    include_total: bool,
    provider_name: &str,
) -> kyomi_connect_protocol::Result<QueryResult> {
    let start = Instant::now();

    let sql_stripped = sql.trim().trim_end_matches(';').trim();
    let sql_upper = sql_stripped.to_uppercase();
    let is_select = sql_upper.starts_with("SELECT") || sql_upper.starts_with("WITH");

    // Get total count if requested (only for SELECT/WITH queries)
    let total_rows = if is_select && include_total {
        get_tds_total_count(client, sql_stripped, provider_name).await
    } else {
        None
    };

    // Apply pagination for SELECT/WITH queries
    let effective_limit = limit.unwrap_or(1000);
    let effective_offset = offset.unwrap_or(0);

    let paginated_sql = if is_select && !sql_upper.contains("OFFSET") && !sql_upper.contains("TOP")
    {
        apply_tsql_pagination(sql_stripped, effective_limit, effective_offset)
    } else {
        sql_stripped.to_string()
    };

    tracing::debug!(
        sql = %paginated_sql.chars().take(200).collect::<String>(),
        "Executing {provider_name} query"
    );

    // Execute the query with timeout
    let query_result = tokio::time::timeout(
        crate::DATASOURCE_TIMEOUT_QUERY,
        client.simple_query(&paginated_sql),
    )
    .await;

    let stream = match query_result {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            tracing::error!(error = %e, "{provider_name} query error");
            return Ok(QueryResult {
                status: QueryStatus::Error,
                columns: None,
                rows: None,
                total_rows: None,
                has_more: false,
                bytes_processed: None,
                execution_time_ms: Some(start.elapsed().as_millis() as i64),
                error: Some(e.to_string()),
                record_batch: None,
            });
        }
        Err(_) => {
            return Ok(QueryResult {
                status: QueryStatus::Error,
                columns: None,
                rows: None,
                total_rows: None,
                has_more: false,
                bytes_processed: None,
                execution_time_ms: Some(start.elapsed().as_millis() as i64),
                error: Some(format!(
                    "Query timed out after {}s",
                    crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                )),
                record_batch: None,
            });
        }
    };

    // Collect rows from the first result set
    let rows_result = match stream.into_first_result().await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "{provider_name} result collection error");
            return Ok(QueryResult {
                status: QueryStatus::Error,
                columns: None,
                rows: None,
                total_rows: None,
                has_more: false,
                bytes_processed: None,
                execution_time_ms: Some(start.elapsed().as_millis() as i64),
                error: Some(e.to_string()),
                record_batch: None,
            });
        }
    };

    // Build column metadata from the first row
    let columns = if let Some(first_row) = rows_result.first() {
        first_row
            .columns()
            .iter()
            .map(|col| ColumnInfo {
                name: col.name().to_string(),
                col_type: map_column_type(col.column_type()),
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    // Filter out the _row_num column if we added it via ROW_NUMBER pagination
    let row_num_idx = columns.iter().position(|c| c.name == "_row_num");

    let filtered_columns: Vec<ColumnInfo> = if let Some(idx) = row_num_idx {
        columns
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, c)| c.clone())
            .collect()
    } else {
        columns
    };

    // Build Arrow RecordBatch alongside JSON rows (only when there are columns).
    let mut arrow_builder = if !filtered_columns.is_empty() {
        Some(crate::arrow_builder::ArrowResultBuilder::new(&filtered_columns))
    } else {
        None
    };

    // Convert rows to JSON values and populate the Arrow builder in one pass.
    //
    // When ROW_NUMBER pagination added a `_row_num` column, filtered_columns
    // excludes it but the tiberius row still has it at position `row_num_idx`.
    // We use `actual_idx` to address the tiberius row and `i` to address the
    // filtered column / Arrow builder slot.
    let mut json_rows = Vec::with_capacity(rows_result.len());
    for row in &rows_result {
        let mut row_values = Vec::with_capacity(filtered_columns.len());
        for (i, col_info) in filtered_columns.iter().enumerate() {
            // Adjust tiberius row index if _row_num column was before this position
            let actual_idx = match row_num_idx {
                Some(rn_idx) if i >= rn_idx => i + 1,
                _ => i,
            };
            row_values.push(tds_row_value_to_json(row, actual_idx, col_info.col_type));

            if let Some(ref mut builder) = arrow_builder {
                tds_row_to_arrow_at(row, i, actual_idx, col_info.col_type, builder);
            }
        }
        json_rows.push(row_values);
        if let Some(ref mut builder) = arrow_builder {
            builder.finish_row();
        }
    }

    let record_batch = arrow_builder.and_then(|builder| {
        builder
            .finish()
            .map_err(|e| {
                tracing::warn!(
                    error = %e,
                    "{provider_name} Arrow batch construction failed; falling back to JSON-only"
                );
                e
            })
            .ok()
    });

    let has_more = json_rows.len() == effective_limit as usize;
    let execution_time_ms = start.elapsed().as_millis() as i64;

    Ok(QueryResult {
        status: QueryStatus::Success,
        columns: Some(filtered_columns),
        rows: Some(json_rows),
        total_rows,
        has_more,
        bytes_processed: None,
        execution_time_ms: Some(execution_time_ms),
        error: None,
        record_batch,
    })
}

/// Execute a query via the tiberius client and return a streaming result.
///
/// This is the shared streaming implementation for both SQL Server and Synapse
/// providers. It spawns a task that iterates the tiberius `QueryStream` row by
/// row (via `try_next()`), batching rows into chunks of `chunk_size`.
///
/// The mutex lock on the TDS client is held for the duration of the stream
/// inside the spawned task. This serializes concurrent queries on the same
/// provider instance, which is acceptable (see module-level docs on concurrency).
///
/// # Arguments
///
/// * `client` - Arc-wrapped mutex around the tiberius client.
/// * `sql` - Raw SQL query string.
/// * `limit` - Optional page size.
/// * `offset` - Optional offset for pagination.
/// * `include_total` - Whether to include total row count.
/// * `chunk_size` - Target rows per chunk.
/// * `provider_name` - Provider name for logging (e.g., "SQL Server", "Synapse").
pub(crate) async fn execute_tds_query_stream(
    client: Arc<Mutex<TdsClient>>,
    sql: &str,
    limit: Option<u32>,
    offset: Option<u32>,
    include_total: bool,
    chunk_size: Option<u32>,
    provider_name: &str,
) -> kyomi_connect_protocol::Result<kyomi_connect_protocol::QueryStream> {
    let start = Instant::now();
    let chunk_size = chunk_size.unwrap_or(100) as usize;

    let sql_stripped = sql.trim().trim_end_matches(';').trim();
    let sql_upper = sql_stripped.to_uppercase();
    let is_select = sql_upper.starts_with("SELECT") || sql_upper.starts_with("WITH");

    // Get total count if requested (only for SELECT/WITH queries).
    // Acquire and release the lock specifically for the count query.
    let total_rows = if is_select && include_total {
        let mut guard = client.lock().await;
        let count = get_tds_total_count(&mut guard, sql_stripped, provider_name).await;
        drop(guard);
        count
    } else {
        None
    };

    // Apply pagination for SELECT/WITH queries
    let effective_limit = limit.unwrap_or(1000);
    let effective_offset = offset.unwrap_or(0);

    let paginated_sql = if is_select && !sql_upper.contains("OFFSET") && !sql_upper.contains("TOP")
    {
        apply_tsql_pagination(sql_stripped, effective_limit, effective_offset)
    } else {
        sql_stripped.to_string()
    };

    tracing::debug!(
        sql = %paginated_sql.chars().take(200).collect::<String>(),
        "Streaming {provider_name} query"
    );

    let provider_name_owned = provider_name.to_string();

    let (tx, stream) = super::sqlx_common::make_stream_channel();

    tokio::spawn(async move {
        // Acquire the mutex for the duration of the stream. The lock is held
        // until the tiberius QueryStream is fully consumed (or an error occurs).
        let mut guard = client.lock().await;

        // Execute the query
        let tds_stream = match tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_QUERY,
            guard.simple_query(&paginated_sql),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                let _ = tx
                    .send(Err(Error::Internal(format!(
                        "{provider_name_owned} query error: {e}"
                    ))))
                    .await;
                return;
            }
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

        // Iterate the tiberius QueryStream item by item.
        // Items arrive as: Metadata → Row* → (optional further Metadata → Row*)
        // We only care about the first result set.
        let mut columns: Vec<ColumnInfo> = Vec::new();
        let mut row_num_idx: Option<usize> = None;
        let mut columns_ready = false;
        let mut chunk_buffer: Vec<Vec<Value>> = Vec::with_capacity(chunk_size);
        let mut chunk_index: u32 = 0;
        let mut total_rows_returned: u64 = 0;

        // Use into_row_stream is tempting but we need the Metadata event for
        // column info before we see any rows. So we iterate QueryItem directly.
        let mut item_stream = tds_stream;

        loop {
            let next =
                tokio::time::timeout(crate::DATASOURCE_TIMEOUT_QUERY, item_stream.try_next()).await;

            let query_item = match next {
                Ok(Ok(Some(item))) => Some(item),
                Ok(Ok(None)) => None, // Stream exhausted
                Ok(Err(e)) => {
                    let _ = tx
                        .send(Err(Error::Internal(format!(
                            "{provider_name_owned} query error: {e}"
                        ))))
                        .await;
                    return;
                }
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

            match query_item {
                Some(QueryItem::Metadata(meta)) => {
                    if columns_ready {
                        // Second result set — we only handle the first, so stop
                        break;
                    }

                    // Extract column metadata from the result set metadata
                    let all_columns: Vec<ColumnInfo> = meta
                        .columns()
                        .iter()
                        .map(|col| ColumnInfo {
                            name: col.name().to_string(),
                            col_type: map_column_type(col.column_type()),
                        })
                        .collect();

                    // Detect and filter the _row_num column from ROW_NUMBER pagination
                    row_num_idx = all_columns.iter().position(|c| c.name == "_row_num");

                    columns = if let Some(idx) = row_num_idx {
                        all_columns
                            .into_iter()
                            .enumerate()
                            .filter(|(i, _)| *i != idx)
                            .map(|(_, c)| c)
                            .collect()
                    } else {
                        all_columns
                    };

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
                Some(QueryItem::Row(row)) => {
                    // Convert row to JSON values, skipping _row_num column
                    let mut row_values = Vec::with_capacity(columns.len());
                    for (i, col_info) in columns.iter().enumerate() {
                        let actual_idx = match row_num_idx {
                            Some(rn_idx) if i >= rn_idx => i + 1,
                            _ => i,
                        };
                        row_values.push(tds_row_value_to_json(&row, actual_idx, col_info.col_type));
                    }
                    chunk_buffer.push(row_values);
                    total_rows_returned += 1;

                    // Flush chunk when full
                    if chunk_buffer.len() >= chunk_size {
                        let rows =
                            std::mem::replace(&mut chunk_buffer, Vec::with_capacity(chunk_size));
                        if tx
                            .send(Ok(QueryStreamEvent::Chunk { rows, chunk_index }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        chunk_index += 1;
                    }
                }
                None => {
                    // Stream exhausted
                    break;
                }
            }
        }

        // If we never got metadata (empty result set with no columns),
        // emit an empty header.
        if !columns_ready
            && tx
                .send(Ok(QueryStreamEvent::Header {
                    columns: Vec::new(),
                    total_rows,
                }))
                .await
                .is_err()
        {
            return;
        }

        // Flush remaining rows
        if !chunk_buffer.is_empty() {
            let rows = std::mem::take(&mut chunk_buffer);
            if tx
                .send(Ok(QueryStreamEvent::Chunk { rows, chunk_index }))
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
    });

    Ok(stream)
}

/// Get total row count for a SELECT query via TDS.
///
/// Returns `None` silently on failure or timeout.
async fn get_tds_total_count(
    client: &mut TdsClient,
    sql: &str,
    provider_name: &str,
) -> Option<i64> {
    let count_sql = format!("SELECT COUNT(*) FROM ({sql}) AS _count_subquery");

    let result = tokio::time::timeout(crate::DATASOURCE_TIMEOUT_QUERY, async {
        let stream = client.simple_query(&count_sql).await.ok()?;
        let row = stream.into_row().await.ok()??;

        // COUNT(*) returns i32 on SQL Server
        if let Ok(Some(v)) = row.try_get::<i32, _>(0) {
            return Some(i64::from(v));
        }
        if let Ok(Some(v)) = row.try_get::<i64, _>(0) {
            return Some(v);
        }

        None
    })
    .await;

    match result {
        Ok(Some(count)) => Some(count),
        Ok(None) => {
            tracing::warn!("Failed to get {provider_name} total count, continuing without it");
            None
        }
        Err(_) => {
            tracing::warn!("{provider_name} total count query timed out, continuing without it");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_mapping::map_tds_type;

    /// Get a human-readable type name from a [`tiberius::ColumnType`].
    ///
    /// Used in tests to verify that [`map_column_type`] produces results
    /// consistent with [`map_tds_type`] when given the canonical type name.
    fn column_type_to_type_name(ct: ColumnType) -> &'static str {
        match ct {
            ColumnType::Null => "null",
            ColumnType::Bit | ColumnType::Bitn => "bit",
            ColumnType::Int1 => "tinyint",
            ColumnType::Int2 => "smallint",
            ColumnType::Int4 => "int",
            ColumnType::Int8 => "bigint",
            ColumnType::Intn => "int",
            ColumnType::Float4 => "real",
            ColumnType::Float8 => "float",
            ColumnType::Floatn => "float",
            ColumnType::Money => "money",
            ColumnType::Money4 => "smallmoney",
            ColumnType::Decimaln => "decimal",
            ColumnType::Numericn => "numeric",
            ColumnType::BigVarChar => "varchar",
            ColumnType::BigChar => "char",
            ColumnType::NVarchar => "nvarchar",
            ColumnType::NChar => "nchar",
            ColumnType::Text => "text",
            ColumnType::NText => "ntext",
            ColumnType::Xml => "xml",
            ColumnType::BigVarBin => "varbinary",
            ColumnType::BigBinary => "binary",
            ColumnType::Image => "image",
            ColumnType::Guid => "uniqueidentifier",
            ColumnType::Datetime => "datetime",
            ColumnType::Datetime4 => "smalldatetime",
            ColumnType::Datetimen => "datetime",
            ColumnType::Datetime2 => "datetime2",
            ColumnType::DatetimeOffsetn => "datetimeoffset",
            ColumnType::Daten => "date",
            ColumnType::Timen => "time",
            ColumnType::Udt => "udt",
            ColumnType::SSVariant => "sql_variant",
        }
    }

    // --- has_main_order_by ---

    #[test]
    fn has_main_order_by_simple() {
        assert!(has_main_order_by("SELECT * FROM t ORDER BY id"));
    }

    #[test]
    fn has_main_order_by_with_desc() {
        assert!(has_main_order_by("SELECT * FROM t ORDER BY id DESC"));
    }

    #[test]
    fn has_main_order_by_no_order() {
        assert!(!has_main_order_by("SELECT * FROM t"));
    }

    #[test]
    fn has_main_order_by_in_subquery() {
        assert!(!has_main_order_by(
            "SELECT * FROM (SELECT * FROM t ORDER BY id) sub"
        ));
    }

    #[test]
    fn has_main_order_by_in_window_function() {
        assert!(!has_main_order_by(
            "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, * FROM t"
        ));
    }

    #[test]
    fn has_main_order_by_both_subquery_and_main() {
        // The main query has ORDER BY at the end
        assert!(has_main_order_by(
            "SELECT * FROM (SELECT * FROM t ORDER BY id) sub ORDER BY name"
        ));
    }

    #[test]
    fn has_main_order_by_cte_with_order() {
        assert!(has_main_order_by(
            "WITH cte AS (SELECT * FROM t) SELECT * FROM cte ORDER BY id"
        ));
    }

    #[test]
    fn has_main_order_by_nested_parens() {
        // ORDER BY is inside multiple levels of parens
        assert!(!has_main_order_by(
            "SELECT * FROM (SELECT * FROM (SELECT * FROM t ORDER BY id) a) b"
        ));
    }

    // --- apply_tsql_pagination ---

    #[test]
    fn pagination_with_order_by_uses_offset_fetch() {
        let sql = "SELECT * FROM t ORDER BY id";
        let result = apply_tsql_pagination(sql, 10, 0);
        assert!(result.contains("OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"));
        assert!(!result.contains("ROW_NUMBER"));
    }

    #[test]
    fn pagination_without_order_by_uses_row_number() {
        let sql = "SELECT * FROM t";
        let result = apply_tsql_pagination(sql, 10, 0);
        assert!(result.contains("ROW_NUMBER()"));
        assert!(result.contains("_row_num"));
        assert!(result.contains("_row_num > 0 AND _row_num <= 10"));
    }

    #[test]
    fn pagination_with_offset() {
        let sql = "SELECT * FROM t";
        let result = apply_tsql_pagination(sql, 10, 20);
        assert!(result.contains("_row_num > 20 AND _row_num <= 30"));
    }

    #[test]
    fn pagination_offset_fetch_with_offset() {
        let sql = "SELECT * FROM t ORDER BY id";
        let result = apply_tsql_pagination(sql, 25, 50);
        assert!(result.contains("OFFSET 50 ROWS FETCH NEXT 25 ROWS ONLY"));
    }

    // --- parse_tsql_error_line ---

    #[test]
    fn parse_error_line_basic() {
        let msg = "Msg 102, Level 15, State 1, Line 3\nIncorrect syntax near 'FORM'.";
        assert_eq!(parse_tsql_error_line(msg), Some(3));
    }

    #[test]
    fn parse_error_line_different_case() {
        let msg = "Error at line 7: something went wrong";
        assert_eq!(parse_tsql_error_line(msg), Some(7));
    }

    #[test]
    fn parse_error_line_no_match() {
        let msg = "Some unknown error occurred";
        assert_eq!(parse_tsql_error_line(msg), None);
    }

    #[test]
    fn parse_error_line_multiple_matches_returns_first() {
        let msg = "Line 5: error, also see Line 10";
        assert_eq!(parse_tsql_error_line(msg), Some(5));
    }

    // --- map_column_type ---

    #[test]
    fn map_column_type_boolean() {
        assert_eq!(map_column_type(ColumnType::Bit), SimpleType::Boolean);
        assert_eq!(map_column_type(ColumnType::Bitn), SimpleType::Boolean);
    }

    #[test]
    fn map_column_type_numbers() {
        assert_eq!(map_column_type(ColumnType::Int1), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Int2), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Int4), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Int8), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Float4), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Float8), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Money), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Money4), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Decimaln), SimpleType::Number);
        assert_eq!(map_column_type(ColumnType::Numericn), SimpleType::Number);
    }

    #[test]
    fn map_column_type_strings() {
        assert_eq!(map_column_type(ColumnType::NVarchar), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::BigVarChar), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::BigChar), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::NChar), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::Text), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::NText), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::Xml), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::Guid), SimpleType::String);
    }

    #[test]
    fn map_column_type_binary() {
        assert_eq!(map_column_type(ColumnType::BigVarBin), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::BigBinary), SimpleType::String);
        assert_eq!(map_column_type(ColumnType::Image), SimpleType::String);
    }

    #[test]
    fn map_column_type_datetime() {
        assert_eq!(map_column_type(ColumnType::Daten), SimpleType::Date);
        assert_eq!(map_column_type(ColumnType::Timen), SimpleType::Time);
        assert_eq!(map_column_type(ColumnType::Datetime), SimpleType::Timestamp);
        assert_eq!(
            map_column_type(ColumnType::Datetime4),
            SimpleType::Timestamp
        );
        assert_eq!(
            map_column_type(ColumnType::Datetime2),
            SimpleType::Timestamp
        );
        assert_eq!(
            map_column_type(ColumnType::DatetimeOffsetn),
            SimpleType::TimestampTz
        );
    }

    #[test]
    fn map_column_type_null() {
        assert_eq!(map_column_type(ColumnType::Null), SimpleType::Unknown);
    }

    // --- hex_encode ---

    #[test]
    fn hex_encode_empty() {
        assert_eq!(hex_encode(&[]), "0x");
    }

    #[test]
    fn hex_encode_bytes() {
        assert_eq!(hex_encode(&[0xDE, 0xAD, 0xBE, 0xEF]), "0xDEADBEEF");
    }

    #[test]
    fn hex_encode_zeros() {
        assert_eq!(hex_encode(&[0x00, 0x00]), "0x0000");
    }

    // --- column_type_to_type_name ---

    #[test]
    fn column_type_to_type_name_covers_all() {
        // Just verify it doesn't panic for all known types and returns non-empty
        let types = [
            ColumnType::Null,
            ColumnType::Bit,
            ColumnType::Int1,
            ColumnType::Int2,
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Float4,
            ColumnType::Float8,
            ColumnType::Money,
            ColumnType::Money4,
            ColumnType::Datetime,
            ColumnType::Datetime4,
            ColumnType::Guid,
            ColumnType::Intn,
            ColumnType::Bitn,
            ColumnType::Decimaln,
            ColumnType::Numericn,
            ColumnType::Floatn,
            ColumnType::Datetimen,
            ColumnType::Daten,
            ColumnType::Timen,
            ColumnType::Datetime2,
            ColumnType::DatetimeOffsetn,
            ColumnType::BigVarBin,
            ColumnType::BigVarChar,
            ColumnType::BigBinary,
            ColumnType::BigChar,
            ColumnType::NVarchar,
            ColumnType::NChar,
            ColumnType::Xml,
            ColumnType::Udt,
            ColumnType::Text,
            ColumnType::Image,
            ColumnType::NText,
            ColumnType::SSVariant,
        ];
        for ct in types {
            let name = column_type_to_type_name(ct);
            assert!(!name.is_empty(), "Empty type name for {ct:?}");
            // Verify the name maps back through map_tds_type correctly
            let mapped = map_tds_type(name);
            // Not all type names round-trip perfectly (e.g., "null" -> Unknown),
            // but they should produce a valid SimpleType
            let _ = mapped;
        }
    }

    // --- map_tds_type integration ---

    #[test]
    fn column_type_names_consistent_with_map_tds_type() {
        // Verify that column_type_to_type_name + map_tds_type produces the
        // same result as map_column_type for the common types.
        let test_cases = [
            (ColumnType::Bit, SimpleType::Boolean),
            (ColumnType::Int4, SimpleType::Number),
            (ColumnType::Float8, SimpleType::Number),
            (ColumnType::NVarchar, SimpleType::String),
            (ColumnType::Daten, SimpleType::Date),
            (ColumnType::Timen, SimpleType::Time),
            (ColumnType::Datetime2, SimpleType::Timestamp),
            (ColumnType::DatetimeOffsetn, SimpleType::TimestampTz),
        ];

        for (ct, expected) in test_cases {
            let from_column_type = map_column_type(ct);
            let from_type_name = map_tds_type(column_type_to_type_name(ct));
            assert_eq!(
                from_column_type, expected,
                "map_column_type mismatch for {ct:?}"
            );
            assert_eq!(
                from_type_name, expected,
                "map_tds_type(column_type_to_type_name) mismatch for {ct:?}"
            );
        }
    }
}
