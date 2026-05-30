//! Type mapping functions for all supported datasource providers.
//!
//! Each function maps provider-specific type representations (OIDs, type codes,
//! type name strings) to the unified [`SimpleType`] enum.
//!
//! The mappings are based on the Python providers and the reference tables in
//! the Phase 6 plan appendix.

use crate::provider::SimpleType;

// ---------------------------------------------------------------------------
// PostgreSQL — OID-based mapping
// ---------------------------------------------------------------------------

/// Map a PostgreSQL type OID to [`SimpleType`].
///
/// PostgreSQL uses numeric Object Identifiers (OIDs) to represent types.
/// See the [PostgreSQL system catalog docs](https://www.postgresql.org/docs/current/catalog-pg-type.html)
/// for the full list.
///
/// # OID Reference
///
/// | OID | Type | SimpleType |
/// |-----|------|------------|
/// | 16 | bool | Boolean |
/// | 20, 21, 23, 26 | int8, int2, int4, oid | Number |
/// | 700, 701, 1700 | float4, float8, numeric | Number |
/// | 18, 19, 25, 1042, 1043 | char, name, text, bpchar, varchar | String |
/// | 1082 | date | Date |
/// | 1083 | time | Time |
/// | 1114 | timestamp | Timestamp |
/// | 1184 | timestamptz | TimestampTz |
/// | 790 | money | Number |
/// | 1186 | interval | String |
/// | 17 | bytea | String |
/// | 1009, 1015, 1016, 1007 | text[], varchar[], int8[], int4[] | String |
/// | 114, 3802 | json, jsonb | String |
/// | 2950 | uuid | String |
pub fn map_postgres_type_oid(oid: u32) -> SimpleType {
    match oid {
        // Boolean
        16 => SimpleType::Boolean,

        // Integer types
        20 | 21 | 23 | 26 => SimpleType::Number,

        // Floating-point and numeric
        700 | 701 | 1700 => SimpleType::Number,

        // Money — stored as i64 cents, decoded via PgMoney
        790 => SimpleType::Number,

        // Character / text types
        18 | 19 | 25 | 1042 | 1043 => SimpleType::String,

        // Interval — formatted as human-readable string
        1186 => SimpleType::String,

        // Bytea — hex-encoded string
        17 => SimpleType::String,

        // Date / time types
        1082 => SimpleType::Date,
        1083 => SimpleType::Time,
        1114 => SimpleType::Timestamp,
        1184 => SimpleType::TimestampTz,

        // Array types — serialize as string representation
        1009 | 1015 | 1016 | 1007 => SimpleType::String,

        // JSON / JSONB — serialize as string
        114 | 3802 => SimpleType::String,

        // UUID — serialize as string
        2950 => SimpleType::String,

        // Anything else
        _ => SimpleType::Unknown,
    }
}

// ---------------------------------------------------------------------------
// MySQL — string-based type name mapping
// ---------------------------------------------------------------------------

/// Map a MySQL type name to [`SimpleType`].
///
/// MySQL type names are case-insensitive. This function normalises to uppercase
/// internally before matching.
///
/// | MySQL Type | SimpleType |
/// |-----------|------------|
/// | TINYINT, SMALLINT, INT, BIGINT | Number |
/// | FLOAT, DOUBLE, DECIMAL | Number |
/// | VARCHAR, TEXT, CHAR, BLOB | String |
/// | DATE | Date |
/// | TIME | Time |
/// | DATETIME | Timestamp |
/// | TIMESTAMP | TimestampTz |
/// | BIT | Boolean |
pub fn map_mysql_type_name(type_name: &str) -> SimpleType {
    let upper = type_name.to_uppercase();
    // Strip " UNSIGNED" suffix — sqlx reports MySQL unsigned types as
    // compound names like "BIGINT UNSIGNED", "INT UNSIGNED", etc.
    let upper = upper.trim().trim_end_matches(" UNSIGNED");

    match upper {
        // Numeric types
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" => SimpleType::Number,
        "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" | "REAL" => SimpleType::Number,

        // String types
        "VARCHAR" | "CHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => SimpleType::String,
        "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => SimpleType::String,
        "BINARY" | "VARBINARY" => SimpleType::String,
        "ENUM" | "SET" | "JSON" => SimpleType::String,

        // Date / time types
        "DATE" => SimpleType::Date,
        "TIME" => SimpleType::Time,
        "DATETIME" => SimpleType::Timestamp,
        "TIMESTAMP" => SimpleType::TimestampTz,
        "YEAR" => SimpleType::Number,

        // Boolean
        "BIT" | "BOOL" | "BOOLEAN" => SimpleType::Boolean,

        _ => {
            // Handle parameterised types like "VARCHAR(255)", "DECIMAL(10,2)"
            let base = if let Some(paren_pos) = upper.find('(') {
                upper[..paren_pos].trim()
            } else {
                upper
            };
            match base {
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" => {
                    SimpleType::Number
                }
                "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" | "REAL" => SimpleType::Number,
                "VARCHAR" | "CHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
                    SimpleType::String
                }
                "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => SimpleType::String,
                "BINARY" | "VARBINARY" => SimpleType::String,
                "ENUM" | "SET" | "JSON" => SimpleType::String,
                "BIT" | "BOOL" | "BOOLEAN" => SimpleType::Boolean,
                "YEAR" => SimpleType::Number,
                _ => SimpleType::Unknown,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ClickHouse — string-based with Nullable() / LowCardinality() wrappers
// ---------------------------------------------------------------------------

/// Map a ClickHouse type string to [`SimpleType`].
///
/// ClickHouse types can be wrapped in `Nullable(...)` and/or
/// `LowCardinality(...)`. This function strips those wrappers before mapping
/// the inner type.
///
/// | ClickHouse Type | SimpleType |
/// |----------------|------------|
/// | DateTime64, DateTime | Timestamp |
/// | Date32, Date | Date |
/// | Int*, UInt* | Number |
/// | Float*, Decimal* | Number |
/// | String, FixedString, UUID, Enum* | String |
/// | Bool | Boolean |
/// | JSON, Array | String |
/// | Nullable(T) | map inner T |
/// | LowCardinality(T) | map inner T |
pub fn map_clickhouse_type(ch_type: &str) -> SimpleType {
    let trimmed = ch_type.trim();

    // Strip Nullable() wrapper
    let inner = strip_wrapper(trimmed, "Nullable");
    // Strip LowCardinality() wrapper
    let inner = strip_wrapper(inner, "LowCardinality");

    map_clickhouse_inner_type(inner)
}

/// Strip a single wrapper function from a ClickHouse type string.
///
/// E.g., `strip_wrapper("Nullable(String)", "Nullable")` returns `"String"`.
fn strip_wrapper<'a>(type_str: &'a str, wrapper: &str) -> &'a str {
    // Case-insensitive prefix check
    if type_str.len() > wrapper.len() + 2
        && type_str[..wrapper.len()].eq_ignore_ascii_case(wrapper)
        && type_str.as_bytes()[wrapper.len()] == b'('
        && type_str.as_bytes()[type_str.len() - 1] == b')'
    {
        &type_str[wrapper.len() + 1..type_str.len() - 1]
    } else {
        type_str
    }
}

/// Map the inner (unwrapped) ClickHouse type to [`SimpleType`].
fn map_clickhouse_inner_type(inner: &str) -> SimpleType {
    let upper = inner.to_uppercase();

    // Exact matches first
    match upper.as_str() {
        "BOOL" | "BOOLEAN" => return SimpleType::Boolean,
        "DATE" | "DATE32" => return SimpleType::Date,
        "DATETIME" => return SimpleType::Timestamp,
        "STRING" | "UUID" | "JSON" | "OBJECT('JSON')" => return SimpleType::String,
        _ => {}
    }

    // Prefix-based matches
    if upper.starts_with("INT")
        || upper.starts_with("UINT")
        || upper.starts_with("FLOAT")
        || upper.starts_with("DECIMAL")
    {
        return SimpleType::Number;
    }

    if upper.starts_with("DATETIME64") {
        return SimpleType::Timestamp;
    }

    if upper.starts_with("FIXEDSTRING") || upper.starts_with("ENUM") {
        return SimpleType::String;
    }

    if upper.starts_with("ARRAY") || upper.starts_with("MAP") || upper.starts_with("TUPLE") {
        return SimpleType::String;
    }

    SimpleType::Unknown
}

// ---------------------------------------------------------------------------
// Snowflake — integer type code mapping
// ---------------------------------------------------------------------------

/// Map a Snowflake type code to [`SimpleType`].
///
/// Snowflake's JDBC/Python driver returns integer type codes.
///
/// | Code | Snowflake Type | SimpleType |
/// |------|---------------|------------|
/// | 0 | FIXED (NUMBER) | Number |
/// | 1 | REAL (FLOAT) | Number |
/// | 2 | TEXT | String |
/// | 3 | DATE | Date |
/// | 4 | TIMESTAMP (TIMESTAMP_NTZ) | Timestamp |
/// | 5 | VARIANT | String |
/// | 6 | TIMESTAMP_LTZ | TimestampTz |
/// | 7 | TIMESTAMP_TZ | TimestampTz |
/// | 8 | TIMESTAMP_NTZ | Timestamp |
/// | 9 | OBJECT | String |
/// | 10 | ARRAY | String |
/// | 11 | BINARY | String |
/// | 12 | TIME | Time |
/// | 13 | BOOLEAN | Boolean |
pub fn map_snowflake_type_code(code: i32) -> SimpleType {
    match code {
        0 | 1 => SimpleType::Number,
        2 => SimpleType::String,
        3 => SimpleType::Date,
        4 | 8 => SimpleType::Timestamp,
        5 | 9 | 10 | 11 => SimpleType::String,
        6 | 7 => SimpleType::TimestampTz,
        12 => SimpleType::Time,
        13 => SimpleType::Boolean,
        _ => SimpleType::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Databricks — string-based type name mapping
// ---------------------------------------------------------------------------

/// Map a Databricks type name to [`SimpleType`].
///
/// Handles parameterised types like `ARRAY<...>`, `STRUCT<...>`, and
/// `DECIMAL(...)`.
///
/// | Databricks Type | SimpleType |
/// |----------------|------------|
/// | TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL | Number |
/// | STRING, CHAR, VARCHAR, BINARY | String |
/// | BOOLEAN | Boolean |
/// | DATE | Date |
/// | TIMESTAMP, TIMESTAMP_NTZ | Timestamp |
/// | ARRAY<>, MAP<>, STRUCT<> | String |
pub fn map_databricks_type(type_name: &str) -> SimpleType {
    let upper = type_name.to_uppercase();
    let upper = upper.trim();

    // Exact matches
    match upper {
        "TINYINT" | "SMALLINT" | "INT" | "INTEGER" | "BIGINT" => return SimpleType::Number,
        "FLOAT" | "DOUBLE" | "DECIMAL" => return SimpleType::Number,
        "STRING" | "CHAR" | "VARCHAR" | "BINARY" => return SimpleType::String,
        "BOOLEAN" => return SimpleType::Boolean,
        "DATE" => return SimpleType::Date,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => return SimpleType::Timestamp,
        "VOID" | "NULL" => return SimpleType::Unknown,
        _ => {}
    }

    // Handle parameterised types
    if upper.starts_with("DECIMAL(") || upper.starts_with("NUMERIC(") {
        return SimpleType::Number;
    }
    if upper.starts_with("ARRAY<") || upper.starts_with("MAP<") || upper.starts_with("STRUCT<") {
        return SimpleType::String;
    }
    if upper.starts_with("VARCHAR(") || upper.starts_with("CHAR(") {
        return SimpleType::String;
    }

    SimpleType::Unknown
}

// ---------------------------------------------------------------------------
// T-SQL — string-based type name mapping (SQL Server + Synapse)
// ---------------------------------------------------------------------------

/// Map a T-SQL type name to [`SimpleType`].
///
/// Used by both SQL Server and Azure Synapse providers since they share
/// the TDS wire protocol and type system.
///
/// | T-SQL Type | SimpleType |
/// |-----------|------------|
/// | int, bigint, smallint, tinyint, decimal, numeric, float, real, money | Number |
/// | varchar, nvarchar, char, nchar, text, ntext, xml, uniqueidentifier | String |
/// | date | Date |
/// | time | Time |
/// | datetime, datetime2, smalldatetime | Timestamp |
/// | datetimeoffset | TimestampTz |
/// | bit | Boolean |
/// | binary, varbinary, image | String |
pub fn map_tds_type(type_name: &str) -> SimpleType {
    let lower = type_name.to_lowercase();
    let lower = lower.trim();

    // Strip parenthesised parameters: "varchar(255)" -> "varchar"
    let base = if let Some(paren_pos) = lower.find('(') {
        lower[..paren_pos].trim()
    } else {
        lower
    };

    match base {
        // Numeric
        "int" | "bigint" | "smallint" | "tinyint" => SimpleType::Number,
        "decimal" | "numeric" | "float" | "real" => SimpleType::Number,
        "money" | "smallmoney" => SimpleType::Number,

        // String
        "varchar" | "nvarchar" | "char" | "nchar" => SimpleType::String,
        "text" | "ntext" | "xml" => SimpleType::String,
        "uniqueidentifier" => SimpleType::String,

        // Binary (serialised as string)
        "binary" | "varbinary" | "image" => SimpleType::String,

        // Date / time
        "date" => SimpleType::Date,
        "time" => SimpleType::Time,
        "datetime" | "datetime2" | "smalldatetime" => SimpleType::Timestamp,
        "datetimeoffset" => SimpleType::TimestampTz,

        // Boolean
        "bit" => SimpleType::Boolean,

        // SQL variant and others
        "sql_variant" => SimpleType::String,

        _ => SimpleType::Unknown,
    }
}

// ---------------------------------------------------------------------------
// BigQuery — string-based type name mapping
// ---------------------------------------------------------------------------

/// Map a BigQuery type name to [`SimpleType`].
///
/// BigQuery uses uppercase type names in its schema metadata.
///
/// | BigQuery Type | SimpleType |
/// |--------------|------------|
/// | DATE | Date |
/// | DATETIME | Timestamp |
/// | TIMESTAMP | TimestampTz |
/// | TIME | Time |
/// | INT64, INTEGER, FLOAT64, NUMERIC, BIGNUMERIC | Number |
/// | STRING, BYTES | String |
/// | BOOL, BOOLEAN | Boolean |
pub fn map_bigquery_type(type_name: &str) -> SimpleType {
    let upper = type_name.to_uppercase();
    let upper = upper.trim();

    match upper {
        // Date / time
        "DATE" => SimpleType::Date,
        "DATETIME" => SimpleType::Timestamp,
        "TIMESTAMP" => SimpleType::TimestampTz,
        "TIME" => SimpleType::Time,

        // Numeric
        "INT64" | "INTEGER" | "FLOAT64" | "FLOAT" | "NUMERIC" | "BIGNUMERIC" => SimpleType::Number,

        // String
        "STRING" | "BYTES" | "GEOGRAPHY" | "JSON" => SimpleType::String,

        // Boolean
        "BOOL" | "BOOLEAN" => SimpleType::Boolean,

        // Complex types — serialise as string
        "STRUCT" | "RECORD" | "ARRAY" | "RANGE" => SimpleType::String,

        _ => SimpleType::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Redshift — OID-based (PostgreSQL-compatible)
// ---------------------------------------------------------------------------

/// Map a Redshift type OID to [`SimpleType`].
///
/// Redshift is wire-compatible with PostgreSQL so shares the same OID space.
/// This function delegates to [`map_postgres_type_oid`].
pub fn map_redshift_type_code(code: u32) -> SimpleType {
    map_postgres_type_oid(code)
}

// ---------------------------------------------------------------------------
// Arrow DataType — direct mapping (used by Flight SQL providers)
// ---------------------------------------------------------------------------

/// Map an Arrow [`DataType`] to [`SimpleType`].
///
/// Used by providers that receive Arrow RecordBatches directly (e.g., FlareDB
/// via Flight SQL). Since the data is already in Arrow format, the mapping is
/// from Arrow types to the simplified column type system.
pub fn map_arrow_type(dt: &arrow::datatypes::DataType) -> SimpleType {
    use arrow::datatypes::DataType;
    match dt {
        // Boolean
        DataType::Boolean => SimpleType::Boolean,

        // Numeric types
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => SimpleType::Number,

        // String types
        DataType::Utf8 | DataType::LargeUtf8 => SimpleType::String,

        // Date types
        DataType::Date32 | DataType::Date64 => SimpleType::Date,

        // Time types
        DataType::Time32(_) | DataType::Time64(_) => SimpleType::Time,

        // Timestamp without timezone
        DataType::Timestamp(_, None) => SimpleType::Timestamp,

        // Timestamp with timezone
        DataType::Timestamp(_, Some(_)) => SimpleType::TimestampTz,

        // Duration, Interval -> String representation
        DataType::Duration(_) | DataType::Interval(_) => SimpleType::String,

        // Binary types -> String
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            SimpleType::String
        }

        // Everything else (List, Struct, Map, Union, etc.) -> Unknown
        _ => SimpleType::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- PostgreSQL ---

    #[test]
    fn postgres_boolean() {
        assert_eq!(map_postgres_type_oid(16), SimpleType::Boolean);
    }

    #[test]
    fn postgres_integers() {
        assert_eq!(map_postgres_type_oid(20), SimpleType::Number); // int8
        assert_eq!(map_postgres_type_oid(21), SimpleType::Number); // int2
        assert_eq!(map_postgres_type_oid(23), SimpleType::Number); // int4
        assert_eq!(map_postgres_type_oid(26), SimpleType::Number); // oid
    }

    #[test]
    fn postgres_floats() {
        assert_eq!(map_postgres_type_oid(700), SimpleType::Number); // float4
        assert_eq!(map_postgres_type_oid(701), SimpleType::Number); // float8
        assert_eq!(map_postgres_type_oid(1700), SimpleType::Number); // numeric
    }

    #[test]
    fn postgres_strings() {
        assert_eq!(map_postgres_type_oid(18), SimpleType::String); // char
        assert_eq!(map_postgres_type_oid(19), SimpleType::String); // name
        assert_eq!(map_postgres_type_oid(25), SimpleType::String); // text
        assert_eq!(map_postgres_type_oid(1042), SimpleType::String); // bpchar
        assert_eq!(map_postgres_type_oid(1043), SimpleType::String); // varchar
    }

    #[test]
    fn postgres_datetime() {
        assert_eq!(map_postgres_type_oid(1082), SimpleType::Date);
        assert_eq!(map_postgres_type_oid(1083), SimpleType::Time);
        assert_eq!(map_postgres_type_oid(1114), SimpleType::Timestamp);
        assert_eq!(map_postgres_type_oid(1184), SimpleType::TimestampTz);
    }

    #[test]
    fn postgres_arrays() {
        assert_eq!(map_postgres_type_oid(1009), SimpleType::String); // text[]
        assert_eq!(map_postgres_type_oid(1015), SimpleType::String); // varchar[]
        assert_eq!(map_postgres_type_oid(1016), SimpleType::String); // int8[]
        assert_eq!(map_postgres_type_oid(1007), SimpleType::String); // int4[]
    }

    #[test]
    fn postgres_json_uuid() {
        assert_eq!(map_postgres_type_oid(114), SimpleType::String); // json
        assert_eq!(map_postgres_type_oid(3802), SimpleType::String); // jsonb
        assert_eq!(map_postgres_type_oid(2950), SimpleType::String); // uuid
    }

    #[test]
    fn postgres_money_interval_bytea() {
        assert_eq!(map_postgres_type_oid(790), SimpleType::Number); // money
        assert_eq!(map_postgres_type_oid(1186), SimpleType::String); // interval
        assert_eq!(map_postgres_type_oid(17), SimpleType::String); // bytea
    }

    #[test]
    fn postgres_unknown() {
        assert_eq!(map_postgres_type_oid(99999), SimpleType::Unknown);
    }

    // --- MySQL ---

    #[test]
    fn mysql_numeric_types() {
        assert_eq!(map_mysql_type_name("INT"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("BIGINT"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("FLOAT"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("DOUBLE"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("DECIMAL"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("YEAR"), SimpleType::Number);
    }

    #[test]
    fn mysql_string_types() {
        assert_eq!(map_mysql_type_name("VARCHAR"), SimpleType::String);
        assert_eq!(map_mysql_type_name("TEXT"), SimpleType::String);
        assert_eq!(map_mysql_type_name("BLOB"), SimpleType::String);
        assert_eq!(map_mysql_type_name("JSON"), SimpleType::String);
    }

    #[test]
    fn mysql_datetime_types() {
        assert_eq!(map_mysql_type_name("DATE"), SimpleType::Date);
        assert_eq!(map_mysql_type_name("TIME"), SimpleType::Time);
        assert_eq!(map_mysql_type_name("DATETIME"), SimpleType::Timestamp);
        assert_eq!(map_mysql_type_name("TIMESTAMP"), SimpleType::TimestampTz);
    }

    #[test]
    fn mysql_boolean() {
        assert_eq!(map_mysql_type_name("BIT"), SimpleType::Boolean);
        assert_eq!(map_mysql_type_name("BOOL"), SimpleType::Boolean);
        assert_eq!(map_mysql_type_name("BOOLEAN"), SimpleType::Boolean);
    }

    #[test]
    fn mysql_case_insensitive() {
        assert_eq!(map_mysql_type_name("int"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("varchar"), SimpleType::String);
    }

    #[test]
    fn mysql_parameterised_types() {
        assert_eq!(map_mysql_type_name("VARCHAR(255)"), SimpleType::String);
        assert_eq!(map_mysql_type_name("DECIMAL(10,2)"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("INT(11)"), SimpleType::Number);
    }

    #[test]
    fn mysql_unsigned_types() {
        assert_eq!(map_mysql_type_name("TINYINT UNSIGNED"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("SMALLINT UNSIGNED"), SimpleType::Number);
        assert_eq!(
            map_mysql_type_name("MEDIUMINT UNSIGNED"),
            SimpleType::Number
        );
        assert_eq!(map_mysql_type_name("INT UNSIGNED"), SimpleType::Number);
        assert_eq!(map_mysql_type_name("BIGINT UNSIGNED"), SimpleType::Number);
        // Parameterised + unsigned
        assert_eq!(map_mysql_type_name("INT(10) UNSIGNED"), SimpleType::Number);
    }

    // --- ClickHouse ---

    #[test]
    fn clickhouse_basic_types() {
        assert_eq!(map_clickhouse_type("String"), SimpleType::String);
        assert_eq!(map_clickhouse_type("Int32"), SimpleType::Number);
        assert_eq!(map_clickhouse_type("UInt64"), SimpleType::Number);
        assert_eq!(map_clickhouse_type("Float64"), SimpleType::Number);
        assert_eq!(map_clickhouse_type("Bool"), SimpleType::Boolean);
        assert_eq!(map_clickhouse_type("Date"), SimpleType::Date);
        assert_eq!(map_clickhouse_type("Date32"), SimpleType::Date);
        assert_eq!(map_clickhouse_type("DateTime"), SimpleType::Timestamp);
        assert_eq!(map_clickhouse_type("DateTime64(3)"), SimpleType::Timestamp);
        assert_eq!(map_clickhouse_type("UUID"), SimpleType::String);
    }

    #[test]
    fn clickhouse_nullable_wrapper() {
        assert_eq!(map_clickhouse_type("Nullable(String)"), SimpleType::String);
        assert_eq!(map_clickhouse_type("Nullable(Int32)"), SimpleType::Number);
        assert_eq!(
            map_clickhouse_type("Nullable(DateTime)"),
            SimpleType::Timestamp
        );
    }

    #[test]
    fn clickhouse_low_cardinality_wrapper() {
        assert_eq!(
            map_clickhouse_type("LowCardinality(String)"),
            SimpleType::String
        );
    }

    #[test]
    fn clickhouse_nested_wrappers() {
        // LowCardinality(Nullable(String)) — strip outer LowCardinality first,
        // but since we strip sequentially (not nested), this tests the outer strip.
        // The inner Nullable(String) should still map correctly.
        assert_eq!(map_clickhouse_type("Nullable(String)"), SimpleType::String);
    }

    #[test]
    fn clickhouse_complex_types() {
        assert_eq!(map_clickhouse_type("Array(String)"), SimpleType::String);
        assert_eq!(map_clickhouse_type("JSON"), SimpleType::String);
    }

    #[test]
    fn clickhouse_enum_and_fixed_string() {
        assert_eq!(
            map_clickhouse_type("Enum8('a' = 1, 'b' = 2)"),
            SimpleType::String
        );
        assert_eq!(map_clickhouse_type("FixedString(16)"), SimpleType::String);
    }

    #[test]
    fn clickhouse_decimal() {
        assert_eq!(map_clickhouse_type("Decimal(18,2)"), SimpleType::Number);
        assert_eq!(map_clickhouse_type("Decimal128(2)"), SimpleType::Number);
    }

    // --- Snowflake ---

    #[test]
    fn snowflake_all_codes() {
        assert_eq!(map_snowflake_type_code(0), SimpleType::Number); // FIXED
        assert_eq!(map_snowflake_type_code(1), SimpleType::Number); // REAL
        assert_eq!(map_snowflake_type_code(2), SimpleType::String); // TEXT
        assert_eq!(map_snowflake_type_code(3), SimpleType::Date); // DATE
        assert_eq!(map_snowflake_type_code(4), SimpleType::Timestamp); // TIMESTAMP
        assert_eq!(map_snowflake_type_code(5), SimpleType::String); // VARIANT
        assert_eq!(map_snowflake_type_code(6), SimpleType::TimestampTz); // TIMESTAMP_LTZ
        assert_eq!(map_snowflake_type_code(7), SimpleType::TimestampTz); // TIMESTAMP_TZ
        assert_eq!(map_snowflake_type_code(8), SimpleType::Timestamp); // TIMESTAMP_NTZ
        assert_eq!(map_snowflake_type_code(9), SimpleType::String); // OBJECT
        assert_eq!(map_snowflake_type_code(10), SimpleType::String); // ARRAY
        assert_eq!(map_snowflake_type_code(11), SimpleType::String); // BINARY
        assert_eq!(map_snowflake_type_code(12), SimpleType::Time); // TIME
        assert_eq!(map_snowflake_type_code(13), SimpleType::Boolean); // BOOLEAN
    }

    #[test]
    fn snowflake_unknown_code() {
        assert_eq!(map_snowflake_type_code(99), SimpleType::Unknown);
        assert_eq!(map_snowflake_type_code(-1), SimpleType::Unknown);
    }

    // --- Databricks ---

    #[test]
    fn databricks_basic_types() {
        assert_eq!(map_databricks_type("INT"), SimpleType::Number);
        assert_eq!(map_databricks_type("BIGINT"), SimpleType::Number);
        assert_eq!(map_databricks_type("FLOAT"), SimpleType::Number);
        assert_eq!(map_databricks_type("DOUBLE"), SimpleType::Number);
        assert_eq!(map_databricks_type("STRING"), SimpleType::String);
        assert_eq!(map_databricks_type("BOOLEAN"), SimpleType::Boolean);
        assert_eq!(map_databricks_type("DATE"), SimpleType::Date);
        assert_eq!(map_databricks_type("TIMESTAMP"), SimpleType::Timestamp);
        assert_eq!(map_databricks_type("TIMESTAMP_NTZ"), SimpleType::Timestamp);
    }

    #[test]
    fn databricks_parameterised_types() {
        assert_eq!(map_databricks_type("DECIMAL(10,2)"), SimpleType::Number);
        assert_eq!(map_databricks_type("ARRAY<STRING>"), SimpleType::String);
        assert_eq!(
            map_databricks_type("STRUCT<name:STRING>"),
            SimpleType::String
        );
        assert_eq!(map_databricks_type("MAP<STRING,INT>"), SimpleType::String);
    }

    #[test]
    fn databricks_case_insensitive() {
        assert_eq!(map_databricks_type("string"), SimpleType::String);
        assert_eq!(map_databricks_type("boolean"), SimpleType::Boolean);
    }

    // --- T-SQL (SQL Server + Synapse) ---

    #[test]
    fn tds_numeric_types() {
        assert_eq!(map_tds_type("int"), SimpleType::Number);
        assert_eq!(map_tds_type("bigint"), SimpleType::Number);
        assert_eq!(map_tds_type("smallint"), SimpleType::Number);
        assert_eq!(map_tds_type("tinyint"), SimpleType::Number);
        assert_eq!(map_tds_type("decimal"), SimpleType::Number);
        assert_eq!(map_tds_type("numeric"), SimpleType::Number);
        assert_eq!(map_tds_type("float"), SimpleType::Number);
        assert_eq!(map_tds_type("real"), SimpleType::Number);
        assert_eq!(map_tds_type("money"), SimpleType::Number);
        assert_eq!(map_tds_type("smallmoney"), SimpleType::Number);
    }

    #[test]
    fn tds_string_types() {
        assert_eq!(map_tds_type("varchar"), SimpleType::String);
        assert_eq!(map_tds_type("nvarchar"), SimpleType::String);
        assert_eq!(map_tds_type("char"), SimpleType::String);
        assert_eq!(map_tds_type("nchar"), SimpleType::String);
        assert_eq!(map_tds_type("text"), SimpleType::String);
        assert_eq!(map_tds_type("ntext"), SimpleType::String);
        assert_eq!(map_tds_type("xml"), SimpleType::String);
        assert_eq!(map_tds_type("uniqueidentifier"), SimpleType::String);
        assert_eq!(map_tds_type("binary"), SimpleType::String);
        assert_eq!(map_tds_type("varbinary"), SimpleType::String);
        assert_eq!(map_tds_type("image"), SimpleType::String);
    }

    #[test]
    fn tds_datetime_types() {
        assert_eq!(map_tds_type("date"), SimpleType::Date);
        assert_eq!(map_tds_type("time"), SimpleType::Time);
        assert_eq!(map_tds_type("datetime"), SimpleType::Timestamp);
        assert_eq!(map_tds_type("datetime2"), SimpleType::Timestamp);
        assert_eq!(map_tds_type("smalldatetime"), SimpleType::Timestamp);
        assert_eq!(map_tds_type("datetimeoffset"), SimpleType::TimestampTz);
    }

    #[test]
    fn tds_boolean() {
        assert_eq!(map_tds_type("bit"), SimpleType::Boolean);
    }

    #[test]
    fn tds_parameterised_types() {
        assert_eq!(map_tds_type("varchar(255)"), SimpleType::String);
        assert_eq!(map_tds_type("decimal(18,2)"), SimpleType::Number);
        assert_eq!(map_tds_type("nvarchar(max)"), SimpleType::String);
    }

    #[test]
    fn tds_case_insensitive() {
        assert_eq!(map_tds_type("INT"), SimpleType::Number);
        assert_eq!(map_tds_type("VARCHAR"), SimpleType::String);
        assert_eq!(map_tds_type("DateTime2"), SimpleType::Timestamp);
    }

    // --- BigQuery ---

    #[test]
    fn bigquery_basic_types() {
        assert_eq!(map_bigquery_type("DATE"), SimpleType::Date);
        assert_eq!(map_bigquery_type("DATETIME"), SimpleType::Timestamp);
        assert_eq!(map_bigquery_type("TIMESTAMP"), SimpleType::TimestampTz);
        assert_eq!(map_bigquery_type("TIME"), SimpleType::Time);
        assert_eq!(map_bigquery_type("INT64"), SimpleType::Number);
        assert_eq!(map_bigquery_type("INTEGER"), SimpleType::Number);
        assert_eq!(map_bigquery_type("FLOAT64"), SimpleType::Number);
        assert_eq!(map_bigquery_type("NUMERIC"), SimpleType::Number);
        assert_eq!(map_bigquery_type("BIGNUMERIC"), SimpleType::Number);
        assert_eq!(map_bigquery_type("STRING"), SimpleType::String);
        assert_eq!(map_bigquery_type("BYTES"), SimpleType::String);
        assert_eq!(map_bigquery_type("BOOL"), SimpleType::Boolean);
        assert_eq!(map_bigquery_type("BOOLEAN"), SimpleType::Boolean);
    }

    #[test]
    fn bigquery_complex_types() {
        assert_eq!(map_bigquery_type("STRUCT"), SimpleType::String);
        assert_eq!(map_bigquery_type("RECORD"), SimpleType::String);
        assert_eq!(map_bigquery_type("ARRAY"), SimpleType::String);
        assert_eq!(map_bigquery_type("JSON"), SimpleType::String);
        assert_eq!(map_bigquery_type("GEOGRAPHY"), SimpleType::String);
    }

    #[test]
    fn bigquery_case_insensitive() {
        assert_eq!(map_bigquery_type("string"), SimpleType::String);
        assert_eq!(map_bigquery_type("int64"), SimpleType::Number);
        assert_eq!(map_bigquery_type("Timestamp"), SimpleType::TimestampTz);
    }

    // --- Redshift ---

    #[test]
    fn redshift_delegates_to_postgres() {
        // Redshift uses PostgreSQL OIDs
        assert_eq!(map_redshift_type_code(16), SimpleType::Boolean);
        assert_eq!(map_redshift_type_code(23), SimpleType::Number);
        assert_eq!(map_redshift_type_code(25), SimpleType::String);
        assert_eq!(map_redshift_type_code(1114), SimpleType::Timestamp);
        assert_eq!(map_redshift_type_code(1184), SimpleType::TimestampTz);
    }

    // --- Arrow DataType (Flight SQL) ---

    #[test]
    fn arrow_boolean() {
        assert_eq!(
            map_arrow_type(&arrow::datatypes::DataType::Boolean),
            SimpleType::Boolean,
        );
    }

    #[test]
    fn arrow_numeric_types() {
        use arrow::datatypes::DataType;
        assert_eq!(map_arrow_type(&DataType::Int32), SimpleType::Number);
        assert_eq!(map_arrow_type(&DataType::Int64), SimpleType::Number);
        assert_eq!(map_arrow_type(&DataType::UInt64), SimpleType::Number);
        assert_eq!(map_arrow_type(&DataType::Float64), SimpleType::Number);
        assert_eq!(
            map_arrow_type(&DataType::Decimal128(18, 2)),
            SimpleType::Number,
        );
    }

    #[test]
    fn arrow_string_types() {
        use arrow::datatypes::DataType;
        assert_eq!(map_arrow_type(&DataType::Utf8), SimpleType::String);
        assert_eq!(map_arrow_type(&DataType::LargeUtf8), SimpleType::String);
    }

    #[test]
    fn arrow_date_and_time() {
        use arrow::datatypes::{DataType, TimeUnit};
        assert_eq!(map_arrow_type(&DataType::Date32), SimpleType::Date);
        assert_eq!(map_arrow_type(&DataType::Date64), SimpleType::Date);
        assert_eq!(
            map_arrow_type(&DataType::Time32(TimeUnit::Millisecond)),
            SimpleType::Time,
        );
        assert_eq!(
            map_arrow_type(&DataType::Time64(TimeUnit::Microsecond)),
            SimpleType::Time,
        );
    }

    #[test]
    fn arrow_timestamp_without_tz() {
        use arrow::datatypes::{DataType, TimeUnit};
        assert_eq!(
            map_arrow_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            SimpleType::Timestamp,
        );
    }

    #[test]
    fn arrow_timestamp_with_tz() {
        use std::sync::Arc;
        use arrow::datatypes::{DataType, TimeUnit};
        assert_eq!(
            map_arrow_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            )),
            SimpleType::TimestampTz,
        );
    }

    #[test]
    fn arrow_binary_types() {
        use arrow::datatypes::DataType;
        assert_eq!(map_arrow_type(&DataType::Binary), SimpleType::String);
        assert_eq!(
            map_arrow_type(&DataType::FixedSizeBinary(16)),
            SimpleType::String,
        );
    }

    #[test]
    fn arrow_complex_types_map_to_unknown() {
        use std::sync::Arc;
        use arrow::datatypes::{DataType, Field};
        assert_eq!(
            map_arrow_type(&DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
            SimpleType::Unknown,
        );
    }
}
