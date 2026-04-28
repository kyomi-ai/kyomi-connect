//! Typed Arrow `RecordBatch` builder for streaming query results.
//!
//! [`ArrowResultBuilder`] accumulates typed Rust values as they stream in from
//! database drivers (sqlx, tiberius, reqwest) and finalizes them into an Arrow
//! [`RecordBatch`]. This avoids the lossy intermediate step of converting
//! database values to `serde_json::Value`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use kyomi_connect_protocol::stream::{ColumnInfo, SimpleType};

// ---------------------------------------------------------------------------
// SimpleType -> Arrow DataType mapping
// ---------------------------------------------------------------------------

/// Map a [`SimpleType`] to its corresponding Arrow [`DataType`].
fn simple_type_to_arrow(st: SimpleType) -> DataType {
    match st {
        SimpleType::Number => DataType::Float64,
        SimpleType::Boolean => DataType::Boolean,
        SimpleType::String | SimpleType::Unknown => DataType::Utf8,
        SimpleType::Date => DataType::Date32,
        SimpleType::Time => DataType::Time64(TimeUnit::Microsecond),
        SimpleType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        SimpleType::TimestampTz => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        }
    }
}

// ---------------------------------------------------------------------------
// TypedColumnBuilder -- one builder per column, matching the Arrow type
// ---------------------------------------------------------------------------

enum TypedColumnBuilder {
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    Date32(Date32Builder),
    Time64Micro(Time64MicrosecondBuilder),
    TimestampMicro(TimestampMicrosecondBuilder),
    TimestampMicroTz(TimestampMicrosecondBuilder),
}

impl TypedColumnBuilder {
    fn from_simple_type(st: SimpleType) -> Self {
        match st {
            SimpleType::Number => Self::Float64(Float64Builder::new()),
            SimpleType::Boolean => Self::Boolean(BooleanBuilder::new()),
            SimpleType::String | SimpleType::Unknown => Self::Utf8(StringBuilder::new()),
            SimpleType::Date => Self::Date32(Date32Builder::new()),
            SimpleType::Time => Self::Time64Micro(Time64MicrosecondBuilder::new()),
            SimpleType::Timestamp => Self::TimestampMicro(TimestampMicrosecondBuilder::new()),
            SimpleType::TimestampTz => Self::TimestampMicroTz(TimestampMicrosecondBuilder::new()),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Float64(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::Time64Micro(b) => b.append_null(),
            Self::TimestampMicro(b) => b.append_null(),
            Self::TimestampMicroTz(b) => b.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::Time64Micro(mut b) => Arc::new(b.finish()),
            Self::TimestampMicro(mut b) => Arc::new(b.finish().with_timezone_opt(None::<&str>)),
            Self::TimestampMicroTz(mut b) => Arc::new(b.finish().with_timezone_opt(Some("UTC"))),
        }
    }
}

// ---------------------------------------------------------------------------
// ArrowResultBuilder
// ---------------------------------------------------------------------------

/// Accumulates typed values column-by-column and produces an Arrow
/// [`RecordBatch`] when finalized.
///
/// # Usage
///
/// ```ignore
/// let mut builder = ArrowResultBuilder::new(&columns);
/// for row in rows {
///     builder.append_f64(0, row.id);
///     builder.append_string(1, &row.name);
///     builder.finish_row();
/// }
/// let batch = builder.finish()?;
/// ```
pub struct ArrowResultBuilder {
    schema: Arc<Schema>,
    builders: Vec<TypedColumnBuilder>,
    row_count: usize,
}

impl ArrowResultBuilder {
    /// Create a new builder from column metadata.
    pub fn new(columns: &[ColumnInfo]) -> Self {
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, simple_type_to_arrow(c.col_type), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let builders = columns
            .iter()
            .map(|c| TypedColumnBuilder::from_simple_type(c.col_type))
            .collect();
        Self {
            schema,
            builders,
            row_count: 0,
        }
    }

    /// Append a null value for the column at `col_idx`.
    pub fn append_null(&mut self, col_idx: usize) {
        self.builders[col_idx].append_null();
    }

    /// Append an `f64` value (for Number columns).
    ///
    /// If the column is not a Float64 builder, the value is converted to a
    /// string representation as a best-effort fallback.
    pub fn append_f64(&mut self, col_idx: usize, value: f64) {
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::Float64(b) => b.append_value(value),
            TypedColumnBuilder::Utf8(b) => b.append_value(value.to_string()),
            other => {
                // Type mismatch -- store null rather than panic.
                other.append_null();
            }
        }
    }

    /// Append an `i64` value (for Number columns, stored as f64).
    ///
    /// If the column is not a Float64 builder, the value is converted to a
    /// string representation as a best-effort fallback.
    pub fn append_i64(&mut self, col_idx: usize, value: i64) {
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::Float64(b) => b.append_value(value as f64),
            TypedColumnBuilder::Utf8(b) => b.append_value(value.to_string()),
            other => {
                other.append_null();
            }
        }
    }

    /// Append a boolean value.
    pub fn append_bool(&mut self, col_idx: usize, value: bool) {
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::Boolean(b) => b.append_value(value),
            TypedColumnBuilder::Utf8(b) => b.append_value(value.to_string()),
            other => {
                other.append_null();
            }
        }
    }

    /// Append a string value.
    pub fn append_string(&mut self, col_idx: usize, value: &str) {
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::Utf8(b) => b.append_value(value),
            // Best-effort: try to parse into the target type from string.
            TypedColumnBuilder::Float64(b) => {
                if let Ok(v) = value.parse::<f64>() {
                    b.append_value(v);
                } else {
                    b.append_null();
                }
            }
            TypedColumnBuilder::Boolean(b) => match value {
                "true" | "1" | "t" | "yes" => b.append_value(true),
                "false" | "0" | "f" | "no" => b.append_value(false),
                _ => b.append_null(),
            },
            other => {
                other.append_null();
            }
        }
    }

    /// Append a date as days since Unix epoch (for Date columns).
    pub fn append_date_days(&mut self, col_idx: usize, days: i32) {
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::Date32(b) => b.append_value(days),
            TypedColumnBuilder::Utf8(b) => {
                // Convert days-since-epoch back to a date string.
                if let Some(date) = NaiveDate::from_num_days_from_ce_opt(days + 719_163) {
                    b.append_value(date.to_string());
                } else {
                    b.append_null();
                }
            }
            other => other.append_null(),
        }
    }

    /// Append a [`NaiveDate`] (for Date columns).
    pub fn append_naive_date(&mut self, col_idx: usize, date: NaiveDate) {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = date.signed_duration_since(epoch).num_days() as i32;
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::Date32(b) => b.append_value(days),
            TypedColumnBuilder::Utf8(b) => b.append_value(date.to_string()),
            other => other.append_null(),
        }
    }

    /// Append a [`NaiveTime`] (for Time columns).
    pub fn append_naive_time(&mut self, col_idx: usize, time: NaiveTime) {
        let micros =
            time.num_seconds_from_midnight() as i64 * 1_000_000 + time.nanosecond() as i64 / 1_000;
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::Time64Micro(b) => b.append_value(micros),
            TypedColumnBuilder::Utf8(b) => b.append_value(time.to_string()),
            other => other.append_null(),
        }
    }

    /// Append a [`NaiveDateTime`] (for Timestamp columns without timezone).
    pub fn append_naive_datetime(&mut self, col_idx: usize, dt: NaiveDateTime) {
        let micros = dt.and_utc().timestamp_micros();
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::TimestampMicro(b) => b.append_value(micros),
            TypedColumnBuilder::TimestampMicroTz(b) => b.append_value(micros),
            TypedColumnBuilder::Utf8(b) => b.append_value(dt.to_string()),
            other => other.append_null(),
        }
    }

    /// Append a [`DateTime<Utc>`] (for TimestampTz columns).
    pub fn append_datetime_utc(&mut self, col_idx: usize, dt: DateTime<Utc>) {
        let micros = dt.timestamp_micros();
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::TimestampMicroTz(b) => b.append_value(micros),
            TypedColumnBuilder::TimestampMicro(b) => b.append_value(micros),
            TypedColumnBuilder::Utf8(b) => b.append_value(dt.to_rfc3339()),
            other => other.append_null(),
        }
    }

    /// Append a raw timestamp as microseconds since Unix epoch.
    pub fn append_timestamp_micros(&mut self, col_idx: usize, micros: i64) {
        match &mut self.builders[col_idx] {
            TypedColumnBuilder::TimestampMicro(b) => b.append_value(micros),
            TypedColumnBuilder::TimestampMicroTz(b) => b.append_value(micros),
            TypedColumnBuilder::Float64(b) => b.append_value(micros as f64),
            TypedColumnBuilder::Utf8(b) => b.append_value(micros.to_string()),
            other => other.append_null(),
        }
    }

    /// Mark end of a row (increments the row counter).
    pub fn finish_row(&mut self) {
        self.row_count += 1;
    }

    /// Get the current row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the Arrow schema.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Finalize all column builders and return a [`RecordBatch`].
    ///
    /// Consumes the builder.
    pub fn finish(self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let arrays: Vec<ArrayRef> = self.builders.into_iter().map(|b| b.finish()).collect();
        RecordBatch::try_new(self.schema, arrays)
    }

    /// Finalize and serialize the [`RecordBatch`] to Arrow IPC bytes.
    ///
    /// The output uses the IPC streaming format and can be read back with
    /// `arrow::ipc::reader::StreamReader`.
    pub fn finish_to_ipc(self) -> Result<Vec<u8>, arrow::error::ArrowError> {
        let schema = Arc::clone(&self.schema);
        let batch = self.finish()?;
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// IPC helper functions
// ---------------------------------------------------------------------------

/// Serialize an Arrow [`Schema`] to IPC bytes using the streaming format.
///
/// The output contains a schema message only (no record batch data). It can be
/// read back with `arrow::ipc::reader::StreamReader` to recover the schema.
///
/// Used by the Connect wire protocol to populate `ArrowHeader::schema_ipc`.
pub fn schema_to_ipc_bytes(schema: &Schema) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        // Finish immediately — we only want the schema preamble, no data batches.
        writer.finish()?;
    }
    Ok(buf)
}

/// Serialize a [`RecordBatch`] to Arrow IPC streaming bytes.
///
/// The output uses the IPC streaming format: a schema message followed by one
/// data batch message. It can be read back with
/// `arrow::ipc::reader::StreamReader`.
///
/// Used by the Connect wire protocol to populate `ArrowBatch::ipc_bytes`.
pub fn batch_to_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Shared JSON-to-Arrow conversion for REST API providers
// ---------------------------------------------------------------------------

/// Convert a JSON value to the appropriate Arrow type and append it to the builder.
///
/// Used by REST API providers (ClickHouse, BigQuery, Snowflake, Databricks)
/// where data arrives as `serde_json::Value`. The `col_type` guides Arrow type
/// selection rather than inferring from the JSON structure.
///
/// Handles common REST API quirks:
/// - Numbers returned as strings (BigQuery, ClickHouse)
/// - Booleans returned as "1"/"0" or "true"/"false" strings
/// - Timestamps in various string formats
/// - Epoch-based timestamps (BigQuery)
pub fn json_value_to_arrow(
    value: &serde_json::Value,
    col_type: SimpleType,
    builder: &mut ArrowResultBuilder,
    col_idx: usize,
) {
    if value.is_null() {
        builder.append_null(col_idx);
        return;
    }
    match col_type {
        SimpleType::Number => {
            if let Some(n) = value.as_f64() {
                builder.append_f64(col_idx, n);
            } else if let Some(n) = value.as_i64() {
                builder.append_i64(col_idx, n);
            } else if let Some(s) = value.as_str() {
                // BigQuery/ClickHouse return numbers as strings
                if let Ok(i) = s.parse::<i64>() {
                    builder.append_i64(col_idx, i);
                } else if let Ok(f) = s.parse::<f64>() {
                    builder.append_f64(col_idx, f);
                } else {
                    builder.append_null(col_idx);
                }
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::Boolean => {
            if let Some(b) = value.as_bool() {
                builder.append_bool(col_idx, b);
            } else if let Some(s) = value.as_str() {
                builder.append_bool(col_idx, s == "1" || s.eq_ignore_ascii_case("true"));
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::Date => {
            if let Some(s) = value.as_str() {
                if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    builder.append_naive_date(col_idx, d);
                } else {
                    builder.append_string(col_idx, s); // fallback
                }
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::Timestamp => {
            if let Some(s) = value.as_str() {
                // Try common timestamp formats
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                    builder.append_naive_datetime(col_idx, dt);
                } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    builder.append_naive_datetime(col_idx, dt);
                } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                    builder.append_naive_datetime(col_idx, dt);
                } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
                    builder.append_naive_datetime(col_idx, dt);
                // RFC3339 with timezone suffix (e.g. "2026-01-15T14:30:00Z" produced by
                // the ClickHouse coercion path). Strip the timezone and treat as naive.
                } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    builder.append_naive_datetime(col_idx, dt.naive_utc());
                // Epoch-second strings (Snowflake returns timestamps this way)
                } else if let Ok(f) = s.parse::<f64>() {
                    let micros = (f * 1_000_000.0) as i64;
                    builder.append_timestamp_micros(col_idx, micros);
                } else {
                    builder.append_string(col_idx, s);
                }
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::TimestampTz => {
            if let Some(s) = value.as_str() {
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    builder.append_datetime_utc(col_idx, dt.with_timezone(&Utc));
                } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                    // Treat as UTC if no timezone info
                    builder.append_datetime_utc(col_idx, dt.and_utc());
                } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    builder.append_datetime_utc(col_idx, dt.and_utc());
                // Epoch-second strings (Snowflake returns timestamps this way)
                } else if let Ok(f) = s.parse::<f64>() {
                    let micros = (f * 1_000_000.0) as i64;
                    builder.append_timestamp_micros(col_idx, micros);
                } else {
                    builder.append_string(col_idx, s);
                }
            } else if let Some(epoch_f) = value.as_f64() {
                // BigQuery returns timestamps as epoch seconds
                let micros = (epoch_f * 1_000_000.0) as i64;
                builder.append_timestamp_micros(col_idx, micros);
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::Time => {
            if let Some(s) = value.as_str() {
                if let Ok(t) = NaiveTime::parse_from_str(s, "%H:%M:%S") {
                    builder.append_naive_time(col_idx, t);
                } else if let Ok(t) = NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                    builder.append_naive_time(col_idx, t);
                } else {
                    builder.append_string(col_idx, s);
                }
            } else {
                builder.append_null(col_idx);
            }
        }
        SimpleType::String | SimpleType::Unknown => {
            if let Some(s) = value.as_str() {
                builder.append_string(col_idx, s);
            } else {
                builder.append_string(col_idx, &value.to_string());
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
    use arrow::array::{
        Array, BooleanArray, Date32Array, Float64Array, StringArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
    };
    use arrow::ipc::reader::StreamReader;

    fn make_columns(specs: &[(&str, SimpleType)]) -> Vec<ColumnInfo> {
        specs
            .iter()
            .map(|(name, st)| ColumnInfo {
                name: name.to_string(),
                col_type: *st,
            })
            .collect()
    }

    // -- Schema mapping -------------------------------------------------------

    #[test]
    fn schema_maps_all_simple_types() {
        let columns = make_columns(&[
            ("num", SimpleType::Number),
            ("flag", SimpleType::Boolean),
            ("text", SimpleType::String),
            ("dt", SimpleType::Date),
            ("tm", SimpleType::Time),
            ("ts", SimpleType::Timestamp),
            ("tstz", SimpleType::TimestampTz),
            ("unk", SimpleType::Unknown),
        ]);
        let builder = ArrowResultBuilder::new(&columns);
        let schema = builder.schema();

        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
        assert_eq!(schema.field(1).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(4).data_type(),
            &DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );
        // Unknown -> Utf8
        assert_eq!(schema.field(7).data_type(), &DataType::Utf8);
    }

    // -- Basic value appending ------------------------------------------------

    #[test]
    fn append_f64_and_i64() {
        let columns = make_columns(&[("val", SimpleType::Number)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        builder.append_f64(0, 3.14);
        builder.finish_row();
        builder.append_i64(0, 42);
        builder.finish_row();

        assert_eq!(builder.row_count(), 2);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 3.14).abs() < f64::EPSILON);
        assert!((arr.value(1) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn append_bool() {
        let columns = make_columns(&[("flag", SimpleType::Boolean)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        builder.append_bool(0, true);
        builder.finish_row();
        builder.append_bool(0, false);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn append_string() {
        let columns = make_columns(&[("name", SimpleType::String)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        builder.append_string(0, "hello");
        builder.finish_row();
        builder.append_string(0, "world");
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "world");
    }

    // -- Null handling --------------------------------------------------------

    #[test]
    fn null_handling() {
        let columns = make_columns(&[
            ("num", SimpleType::Number),
            ("text", SimpleType::String),
            ("flag", SimpleType::Boolean),
        ]);
        let mut builder = ArrowResultBuilder::new(&columns);

        // Row 0: all nulls
        builder.append_null(0);
        builder.append_null(1);
        builder.append_null(2);
        builder.finish_row();

        // Row 1: values
        builder.append_f64(0, 1.0);
        builder.append_string(1, "hi");
        builder.append_bool(2, true);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Row 0 should be null
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
        assert!(batch.column(2).is_null(0));

        // Row 1 should have values
        assert!(!batch.column(0).is_null(1));
        assert!(!batch.column(1).is_null(1));
        assert!(!batch.column(2).is_null(1));
    }

    // -- Date / Time / Timestamp -----------------------------------------------

    #[test]
    fn append_naive_date() {
        let columns = make_columns(&[("d", SimpleType::Date)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        let date = NaiveDate::from_ymd_opt(2026, 3, 21).unwrap();
        builder.append_naive_date(0, date);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let expected_days = date.signed_duration_since(epoch).num_days() as i32;
        assert_eq!(arr.value(0), expected_days);
    }

    #[test]
    fn append_date_days() {
        let columns = make_columns(&[("d", SimpleType::Date)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        // 2026-03-21 is 20533 days since 1970-01-01
        let date = NaiveDate::from_ymd_opt(2026, 3, 21).unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = date.signed_duration_since(epoch).num_days() as i32;

        builder.append_date_days(0, days);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(arr.value(0), days);
    }

    #[test]
    fn append_naive_time() {
        let columns = make_columns(&[("t", SimpleType::Time)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        let time = NaiveTime::from_hms_micro_opt(14, 30, 15, 123_456).unwrap();
        builder.append_naive_time(0, time);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();

        let expected_micros =
            14 * 3600 * 1_000_000i64 + 30 * 60 * 1_000_000 + 15 * 1_000_000 + 123_456;
        assert_eq!(arr.value(0), expected_micros);
    }

    #[test]
    fn append_naive_datetime() {
        let columns = make_columns(&[("ts", SimpleType::Timestamp)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        let dt = NaiveDate::from_ymd_opt(2026, 3, 21)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        builder.append_naive_datetime(0, dt);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(arr.value(0), dt.and_utc().timestamp_micros());
    }

    #[test]
    fn append_datetime_utc() {
        let columns = make_columns(&[("tstz", SimpleType::TimestampTz)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        let dt: DateTime<Utc> = "2026-03-21T12:00:00Z".parse().unwrap();
        builder.append_datetime_utc(0, dt);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(arr.value(0), dt.timestamp_micros());
    }

    #[test]
    fn append_timestamp_micros() {
        let columns = make_columns(&[("ts", SimpleType::Timestamp)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        let micros = 1_711_000_000_000_000i64;
        builder.append_timestamp_micros(0, micros);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(arr.value(0), micros);
    }

    // -- Type mismatch fallback -----------------------------------------------

    #[test]
    fn f64_to_string_column_converts() {
        let columns = make_columns(&[("text", SimpleType::String)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        builder.append_f64(0, 3.14);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "3.14");
    }

    #[test]
    fn string_to_number_column_parses() {
        let columns = make_columns(&[("num", SimpleType::Number)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        builder.append_string(0, "42.5");
        builder.finish_row();
        builder.append_string(0, "not_a_number");
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 42.5).abs() < f64::EPSILON);
        assert!(arr.is_null(1));
    }

    #[test]
    fn bool_to_string_column_converts() {
        let columns = make_columns(&[("text", SimpleType::String)]);
        let mut builder = ArrowResultBuilder::new(&columns);

        builder.append_bool(0, true);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "true");
    }

    // -- Multi-column batch ---------------------------------------------------

    #[test]
    fn multi_column_batch() {
        let columns = make_columns(&[
            ("id", SimpleType::Number),
            ("name", SimpleType::String),
            ("active", SimpleType::Boolean),
            ("created", SimpleType::Date),
        ]);
        let mut builder = ArrowResultBuilder::new(&columns);

        let date = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();

        builder.append_i64(0, 1);
        builder.append_string(1, "Alice");
        builder.append_bool(2, true);
        builder.append_naive_date(3, date);
        builder.finish_row();

        builder.append_i64(0, 2);
        builder.append_string(1, "Bob");
        builder.append_bool(2, false);
        builder.append_null(3);
        builder.finish_row();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        // Verify column names
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
        assert_eq!(batch.schema().field(2).name(), "active");
        assert_eq!(batch.schema().field(3).name(), "created");

        // Verify values
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((ids.value(0) - 1.0).abs() < f64::EPSILON);
        assert!((ids.value(1) - 2.0).abs() < f64::EPSILON);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");

        // created: row 0 has value, row 1 is null
        assert!(!batch.column(3).is_null(0));
        assert!(batch.column(3).is_null(1));
    }

    // -- IPC roundtrip --------------------------------------------------------

    #[test]
    fn ipc_roundtrip() {
        let columns = make_columns(&[
            ("id", SimpleType::Number),
            ("name", SimpleType::String),
            ("flag", SimpleType::Boolean),
        ]);
        let mut builder = ArrowResultBuilder::new(&columns);

        builder.append_f64(0, 99.9);
        builder.append_string(1, "test");
        builder.append_bool(2, true);
        builder.finish_row();

        builder.append_null(0);
        builder.append_string(1, "null_id");
        builder.append_bool(2, false);
        builder.finish_row();

        let ipc_bytes = builder.finish_to_ipc().unwrap();
        assert!(!ipc_bytes.is_empty());

        // Read it back
        let cursor = std::io::Cursor::new(ipc_bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        // Verify schema survived roundtrip
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(1).name(), "name");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);

        // Verify values survived roundtrip
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((ids.value(0) - 99.9).abs() < f64::EPSILON);
        assert!(ids.is_null(1));

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "test");
        assert_eq!(names.value(1), "null_id");

        // No more batches
        assert!(reader.next().is_none());
    }

    // -- All SimpleType variants in one batch ---------------------------------

    #[test]
    fn all_simple_types_roundtrip() {
        let columns = make_columns(&[
            ("num", SimpleType::Number),
            ("flag", SimpleType::Boolean),
            ("text", SimpleType::String),
            ("dt", SimpleType::Date),
            ("tm", SimpleType::Time),
            ("ts", SimpleType::Timestamp),
            ("tstz", SimpleType::TimestampTz),
            ("unk", SimpleType::Unknown),
        ]);
        let mut builder = ArrowResultBuilder::new(&columns);

        let date = NaiveDate::from_ymd_opt(2026, 3, 21).unwrap();
        let time = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
        let naive_dt = date.and_hms_opt(12, 0, 0).unwrap();
        let utc_dt: DateTime<Utc> = "2026-03-21T12:00:00Z".parse().unwrap();

        builder.append_f64(0, 42.0);
        builder.append_bool(1, true);
        builder.append_string(2, "hello");
        builder.append_naive_date(3, date);
        builder.append_naive_time(4, time);
        builder.append_naive_datetime(5, naive_dt);
        builder.append_datetime_utc(6, utc_dt);
        builder.append_string(7, "unknown_val");
        builder.finish_row();

        let ipc_bytes = builder.finish_to_ipc().unwrap();

        // Read back
        let cursor = std::io::Cursor::new(ipc_bytes);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 8);

        // Spot-check values
        let nums = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((nums.value(0) - 42.0).abs() < f64::EPSILON);

        let flags = batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flags.value(0));

        let texts = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(texts.value(0), "hello");

        let dates = batch
            .column(3)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(
            dates.value(0),
            date.signed_duration_since(epoch).num_days() as i32
        );

        let times = batch
            .column(4)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(
            times.value(0),
            14 * 3600 * 1_000_000i64 + 30 * 60 * 1_000_000
        );

        // Unknown col is Utf8
        let unknowns = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(unknowns.value(0), "unknown_val");
    }

    // -- Empty batch ----------------------------------------------------------

    #[test]
    fn empty_batch() {
        let columns = make_columns(&[("x", SimpleType::Number)]);
        let builder = ArrowResultBuilder::new(&columns);
        assert_eq!(builder.row_count(), 0);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }

    // -- json_value_to_arrow: Number ------------------------------------------

    fn single_col(st: SimpleType) -> Vec<ColumnInfo> {
        make_columns(&[("val", st)])
    }

    fn finish_one(st: SimpleType, value: &serde_json::Value) -> arrow::record_batch::RecordBatch {
        let columns = single_col(st);
        let mut builder = ArrowResultBuilder::new(&columns);
        json_value_to_arrow(value, st, &mut builder, 0);
        builder.finish_row();
        builder.finish().unwrap()
    }

    #[test]
    fn jva_number_json_integer() {
        let batch = finish_one(SimpleType::Number, &serde_json::json!(42));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn jva_number_json_float() {
        let batch = finish_one(SimpleType::Number, &serde_json::json!(3.14));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 3.14).abs() < 1e-10);
    }

    #[test]
    fn jva_number_string_integer() {
        let batch = finish_one(SimpleType::Number, &serde_json::json!("123"));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 123.0).abs() < f64::EPSILON);
    }

    #[test]
    fn jva_number_string_float() {
        let batch = finish_one(SimpleType::Number, &serde_json::json!("9.99"));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((arr.value(0) - 9.99).abs() < 1e-10);
    }

    #[test]
    fn jva_number_null() {
        let batch = finish_one(SimpleType::Number, &serde_json::Value::Null);
        assert!(batch.column(0).is_null(0));
    }

    // -- json_value_to_arrow: Boolean -----------------------------------------

    #[test]
    fn jva_boolean_true() {
        let batch = finish_one(SimpleType::Boolean, &serde_json::json!(true));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
    }

    #[test]
    fn jva_boolean_false() {
        let batch = finish_one(SimpleType::Boolean, &serde_json::json!(false));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!arr.value(0));
    }

    #[test]
    fn jva_boolean_string_one() {
        let batch = finish_one(SimpleType::Boolean, &serde_json::json!("1"));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
    }

    #[test]
    fn jva_boolean_string_true() {
        let batch = finish_one(SimpleType::Boolean, &serde_json::json!("true"));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(arr.value(0));
    }

    #[test]
    fn jva_boolean_null() {
        let batch = finish_one(SimpleType::Boolean, &serde_json::Value::Null);
        assert!(batch.column(0).is_null(0));
    }

    // -- json_value_to_arrow: String ------------------------------------------

    #[test]
    fn jva_string_plain() {
        let batch = finish_one(SimpleType::String, &serde_json::json!("hello world"));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "hello world");
    }

    #[test]
    fn jva_string_null() {
        let batch = finish_one(SimpleType::String, &serde_json::Value::Null);
        // Null JSON → null cell (append_null)
        assert!(batch.column(0).is_null(0));
    }

    // -- json_value_to_arrow: Date --------------------------------------------

    #[test]
    fn jva_date_valid() {
        let batch = finish_one(SimpleType::Date, &serde_json::json!("2026-01-15"));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let expected = NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .signed_duration_since(epoch)
            .num_days() as i32;
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn jva_date_null() {
        let batch = finish_one(SimpleType::Date, &serde_json::Value::Null);
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn jva_date_malformed_falls_back_to_string() {
        // Malformed date falls back to append_string, but the builder is Date32
        // so append_string → append_null on the Date32 builder.
        let batch = finish_one(SimpleType::Date, &serde_json::json!("not-a-date"));
        // The Date branch calls append_string as fallback, which on a Date32
        // builder hits the `other => other.append_null()` arm.
        assert!(batch.column(0).is_null(0));
    }

    // -- json_value_to_arrow: Timestamp ---------------------------------------

    #[test]
    fn jva_timestamp_iso_t_format() {
        let batch = finish_one(
            SimpleType::Timestamp,
            &serde_json::json!("2026-01-15T14:30:00"),
        );
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected = NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(14, 30, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn jva_timestamp_space_format() {
        let batch = finish_one(
            SimpleType::Timestamp,
            &serde_json::json!("2026-01-15 14:30:00"),
        );
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected = NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(14, 30, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn jva_timestamp_with_subseconds() {
        let batch = finish_one(
            SimpleType::Timestamp,
            &serde_json::json!("2026-01-15T14:30:00.123456"),
        );
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected = NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_micro_opt(14, 30, 0, 123_456)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn jva_timestamp_epoch_seconds_as_string() {
        // Snowflake returns timestamps as epoch-seconds strings
        let epoch_secs = 1_737_000_000.0_f64;
        let value = serde_json::json!(epoch_secs.to_string());
        let batch = finish_one(SimpleType::Timestamp, &value);
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected = (epoch_secs * 1_000_000.0) as i64;
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn jva_timestamp_null() {
        let batch = finish_one(SimpleType::Timestamp, &serde_json::Value::Null);
        assert!(batch.column(0).is_null(0));
    }

    /// Timestamp with Z suffix (produced by ClickHouse coercion path) must not
    /// result in null. This is the regression test for the known ClickHouse bug.
    #[test]
    fn jva_timestamp_with_z_suffix_not_null() {
        // ClickHouse coerces "2026-01-15 14:30:00" → "2026-01-15T14:30:00Z"
        // then calls json_value_to_arrow with SimpleType::TimestampTz.
        // Separately, if Timestamp is used instead the Z suffix must not break parsing.
        let batch = finish_one(
            SimpleType::Timestamp,
            &serde_json::json!("2026-01-15T14:30:00Z"),
        );
        assert!(
            !batch.column(0).is_null(0),
            "Z-suffixed timestamp must not be null for Timestamp type"
        );
    }

    // -- json_value_to_arrow: TimestampTz -------------------------------------

    #[test]
    fn jva_timestamptz_rfc3339_z() {
        let batch = finish_one(
            SimpleType::TimestampTz,
            &serde_json::json!("2026-01-15T14:30:00Z"),
        );
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected: DateTime<Utc> = "2026-01-15T14:30:00Z".parse().unwrap();
        assert_eq!(arr.value(0), expected.timestamp_micros());
    }

    #[test]
    fn jva_timestamptz_rfc3339_offset() {
        let batch = finish_one(
            SimpleType::TimestampTz,
            &serde_json::json!("2026-01-15T14:30:00+00:00"),
        );
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected: DateTime<Utc> = "2026-01-15T14:30:00Z".parse().unwrap();
        assert_eq!(arr.value(0), expected.timestamp_micros());
    }

    #[test]
    fn jva_timestamptz_null() {
        let batch = finish_one(SimpleType::TimestampTz, &serde_json::Value::Null);
        assert!(batch.column(0).is_null(0));
    }

    // -- json_value_to_arrow: Time --------------------------------------------

    #[test]
    fn jva_time_valid() {
        let batch = finish_one(SimpleType::Time, &serde_json::json!("14:30:00"));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        let expected = 14 * 3600 * 1_000_000i64 + 30 * 60 * 1_000_000;
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn jva_time_null() {
        let batch = finish_one(SimpleType::Time, &serde_json::Value::Null);
        assert!(batch.column(0).is_null(0));
    }

    // -- json_value_to_arrow: Unknown -----------------------------------------

    #[test]
    fn jva_unknown_string_passthrough() {
        let batch = finish_one(SimpleType::Unknown, &serde_json::json!("some raw value"));
        assert!(!batch.column(0).is_null(0));
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "some raw value");
    }

    #[test]
    fn jva_unknown_null() {
        // Null JSON → the value.is_null() check fires, append_null is called.
        let batch = finish_one(SimpleType::Unknown, &serde_json::Value::Null);
        assert!(batch.column(0).is_null(0));
    }

    // -- IPC helper functions -------------------------------------------------

    #[test]
    fn schema_to_ipc_bytes_produces_readable_schema() {
        let columns = make_columns(&[("id", SimpleType::Number), ("name", SimpleType::String)]);
        let builder = ArrowResultBuilder::new(&columns);
        let schema = builder.schema().as_ref().clone();

        let ipc = schema_to_ipc_bytes(&schema).unwrap();
        assert!(!ipc.is_empty());

        // Read the schema back — finish() writes an EOS marker so the reader
        // will return None immediately after opening (no batches).
        let cursor = std::io::Cursor::new(ipc);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let recovered = reader.schema();
        assert_eq!(recovered.fields().len(), 2);
        assert_eq!(recovered.field(0).name(), "id");
        assert_eq!(recovered.field(1).name(), "name");
    }

    #[test]
    fn batch_to_ipc_bytes_roundtrip() {
        let columns = make_columns(&[("val", SimpleType::Number), ("label", SimpleType::String)]);
        let mut builder = ArrowResultBuilder::new(&columns);
        builder.append_f64(0, 1.5);
        builder.append_string(1, "hello");
        builder.finish_row();

        let batch = builder.finish().unwrap();
        let ipc = batch_to_ipc_bytes(&batch).unwrap();
        assert!(!ipc.is_empty());

        let cursor = std::io::Cursor::new(ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let recovered = reader.next().unwrap().unwrap();
        assert_eq!(recovered.num_rows(), 1);

        let vals = recovered
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 1.5).abs() < f64::EPSILON);

        let labels = recovered
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(labels.value(0), "hello");

        // No more batches.
        assert!(reader.next().is_none());
    }
}
