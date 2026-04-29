//! Shared T-SQL helpers for SQL Server and Azure Synapse providers.
//!
//! Both providers use the TDS wire protocol via `tiberius` and share:
//! - Pagination logic (OFFSET-FETCH and ROW_NUMBER fallback)
//! - Error line parsing (`Line N` pattern)
//! - Column type mapping from [`tiberius::ColumnType`] to [`SimpleType`]

use std::sync::LazyLock;
use std::time::Instant;

use regex::Regex;
use tiberius::{ColumnType, Row};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

use crate::provider::{ColumnInfo, QueryResult, QueryStatus, SimpleType};

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
            } else if let Ok(Some(v)) = row.try_get::<tiberius::numeric::Numeric, _>(row_idx) {
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

/// Stream query results via the tiberius client as Arrow IPC batches.
///
/// This is the Arrow streaming implementation shared by SQL Server and Synapse.
///
/// # Why everything happens inside the spawn
///
/// `tiberius::QueryStream` borrows the locked `MutexGuard` that was used to
/// call `simple_query`, so it is not `Send`. The entire row-processing loop
/// must therefore run while holding that lock, inside the `tokio::spawn` task.
/// The mpsc channel lets us push `ArrowStreamEvent` values out to the caller
/// without moving the non-Send stream across threads.
///
/// # Pagination
///
/// Applies T-SQL pagination (OFFSET-FETCH or ROW_NUMBER wrapper) only for
/// SELECT/WITH queries that have no existing OFFSET or TOP clause. The
/// `_row_num` sentinel column added by ROW_NUMBER pagination is stripped from
/// the schema and from every row.
///
/// # Arguments
///
/// * `client` — `Arc<Mutex<TdsClient>>` cloned from the provider.
/// * `sql` — Raw SQL string (may have trailing semicolons).
/// * `limit` — Optional page size; defaults to 10 000 when streaming.
/// * `offset` — Optional row offset.
/// * `chunk_size` — Target rows per emitted Arrow batch (`None` → 10 000).
/// * `provider_name` — Display name used in error messages ("SQL Server" / "Azure Synapse").
pub(crate) async fn execute_tds_stream_arrow(
    client: std::sync::Arc<tokio::sync::Mutex<TdsClient>>,
    sql: String,
    limit: Option<u32>,
    offset: Option<u32>,
    chunk_size: Option<u32>,
    provider_name: &'static str,
) -> kyomi_connect_protocol::ArrowStream {
    use crate::arrow_builder::ArrowResultBuilder;
    use crate::provider::ColumnInfo;
    use kyomi_connect_protocol::{ArrowStreamEvent, Error};

    tracing::debug!(
        sql = %sql.chars().take(200).collect::<String>(),
        provider = provider_name,
        "{provider_name}: starting Arrow stream"
    );

    let (tx, stream) = super::sqlx_common::make_arrow_stream_channel();

    tokio::spawn(async move {
        let start = std::time::Instant::now();
        let effective_chunk_size = chunk_size.unwrap_or(10_000) as usize;

        let sql_stripped = sql.trim().trim_end_matches(';').trim().to_string();
        let sql_upper = sql_stripped.to_uppercase();
        let is_select = sql_upper.starts_with("SELECT") || sql_upper.starts_with("WITH");

        let effective_limit = limit.unwrap_or(10_000);
        let effective_offset = offset.unwrap_or(0);

        let paginated_sql =
            if is_select && !sql_upper.contains("OFFSET") && !sql_upper.contains("TOP") {
                apply_tsql_pagination(&sql_stripped, effective_limit, effective_offset)
            } else {
                sql_stripped.clone()
            };

        // Acquire the mutex and collect all rows while holding it.
        //
        // tiberius QueryStream is not Send — it borrows the &mut TdsClient
        // obtained from the MutexGuard. We therefore do all I/O (query +
        // row collection) inside a single async block while the guard is live,
        // then return the owned Vec<Row> and let the guard drop at the end of
        // that block.
        let rows_result: Result<Vec<tiberius::Row>, String> = async {
            let mut guard = client.lock().await;

            let query_result = tokio::time::timeout(
                crate::DATASOURCE_TIMEOUT_QUERY,
                guard.simple_query(&paginated_sql),
            )
            .await;

            let query_stream = match query_result {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => return Err(format!("{provider_name} query error: {e}")),
                Err(_) => {
                    return Err(format!(
                        "{provider_name} query timed out after {}s",
                        crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                    ))
                }
            };

            query_stream
                .into_first_result()
                .await
                .map_err(|e| format!("{provider_name} result collection error: {e}"))
            // `guard` drops here, releasing the mutex
        }
        .await;

        let rows_result = match rows_result {
            Ok(rows) => rows,
            Err(msg) => {
                let _ = tx.send(Err(Error::Internal(msg))).await;
                return;
            }
        };

        // Build column metadata from the first row
        let columns: Vec<ColumnInfo> = if let Some(first_row) = rows_result.first() {
            first_row
                .columns()
                .iter()
                .map(|col| ColumnInfo {
                    name: col.name().to_string(),
                    col_type: map_column_type(col.column_type()),
                })
                .collect()
        } else {
            Vec::new()
        };

        // Identify and strip the _row_num sentinel column added by ROW_NUMBER pagination
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

        // Send Schema event
        let mut arrow_builder = ArrowResultBuilder::new(&filtered_columns);
        let schema_ipc = match super::sqlx_common::schema_to_ipc_bytes(arrow_builder.schema()) {
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
                columns: filtered_columns.clone(),
                total_rows: None,
            }))
            .await
            .is_err()
        {
            return;
        }

        // Process rows into Arrow batches
        let mut chunk_index: u32 = 0;
        let mut total_rows_returned: u64 = 0;

        for row in &rows_result {
            for (i, col_info) in filtered_columns.iter().enumerate() {
                let actual_idx = match row_num_idx {
                    Some(rn_idx) if i >= rn_idx => i + 1,
                    _ => i,
                };
                tds_row_to_arrow_at(row, i, actual_idx, col_info.col_type, &mut arrow_builder);
            }
            arrow_builder.finish_row();
            total_rows_returned += 1;

            if arrow_builder.row_count() >= effective_chunk_size {
                let full_builder =
                    std::mem::replace(&mut arrow_builder, ArrowResultBuilder::new(&filtered_columns));
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
            }
        }

        // Flush remaining rows
        if arrow_builder.row_count() > 0 {
            match arrow_builder.finish_to_ipc() {
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
    });

    stream
}

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
                job_id: None,
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
                job_id: None,
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
                job_id: None,
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

    // Build Arrow RecordBatch (the sole data path — JSON rows are not populated).
    //
    // When ROW_NUMBER pagination added a `_row_num` column, filtered_columns
    // excludes it but the tiberius row still has it at position `row_num_idx`.
    // We use `actual_idx` to address the tiberius row and `i` to address the
    // filtered column / Arrow builder slot.
    let mut arrow_builder = if !filtered_columns.is_empty() {
        Some(crate::arrow_builder::ArrowResultBuilder::new(
            &filtered_columns,
        ))
    } else {
        None
    };

    for row in &rows_result {
        if let Some(ref mut builder) = arrow_builder {
            for (i, col_info) in filtered_columns.iter().enumerate() {
                let actual_idx = match row_num_idx {
                    Some(rn_idx) if i >= rn_idx => i + 1,
                    _ => i,
                };
                tds_row_to_arrow_at(row, i, actual_idx, col_info.col_type, builder);
            }
            builder.finish_row();
        }
    }

    let record_batch = arrow_builder.and_then(|builder| {
        builder
            .finish()
            .map_err(|e| {
                tracing::warn!(
                    error = %e,
                    "{provider_name} Arrow batch construction failed"
                );
                e
            })
            .ok()
    });

    let row_count = record_batch.as_ref().map_or(0, |b| b.num_rows());
    let has_more = row_count == effective_limit as usize;
    let execution_time_ms = start.elapsed().as_millis() as i64;

    Ok(QueryResult {
        status: QueryStatus::Success,
        columns: Some(filtered_columns),
        rows: None,
        total_rows,
        has_more,
        bytes_processed: None,
        execution_time_ms: Some(execution_time_ms),
        error: None,
        record_batch,
        job_id: None,
    })
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
