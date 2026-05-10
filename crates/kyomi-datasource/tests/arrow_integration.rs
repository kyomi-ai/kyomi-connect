//! Integration tests for Arrow-native data pipeline.
//!
//! Tests each provider's `execute_query` against real databases to verify that
//! `record_batch` is populated with correct Arrow types. Requires test database
//! containers running (see `docker-compose.test.yml` in the kyomi repo).
//!
//! Run with: cargo test --test arrow_integration -- --ignored
//!
//! Database ports (from docker-compose.test.yml):
//!   - Postgres: 5434 (user: test_user, pass: test_password, db: test_db)
//!   - MySQL: 3308 (user: test_user, pass: test_password, db: test_db)
//!   - ClickHouse: 8124 (user: test_user, pass: test_password, db: test_db)
//!   - SQL Server: 1434 (user: sa, pass: TestPassword123!)

use arrow::array::*;
use arrow::datatypes::DataType;
use kyomi_datasource::provider::DatasourceProvider;
use serde_json::json;

async fn create_test_provider(
    db_type: &str,
    config: serde_json::Value,
    credentials: serde_json::Value,
) -> Box<dyn DatasourceProvider> {
    let ds_type: kyomi_connect_protocol::DatasourceType = db_type.parse().unwrap();
    kyomi_datasource::create_provider(&ds_type, &config, &credentials, None)
        .await
        .unwrap_or_else(|e| panic!("Failed to create {db_type} provider: {e}"))
}

fn assert_batch_has_timestamps(batch: &RecordBatch, col_name: &str) {
    let schema = batch.schema();
    let col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == col_name)
        .unwrap_or_else(|| panic!("Column '{col_name}' not found in schema"));

    let dt = schema.field(col_idx).data_type();
    assert!(
        matches!(dt, DataType::Timestamp(_, _)),
        "Expected Timestamp type for '{col_name}', got {dt:?}"
    );

    assert!(
        !batch.column(col_idx).is_null(0),
        "First row of '{col_name}' should not be null"
    );
}

fn assert_batch_has_dates(batch: &RecordBatch, col_name: &str) {
    let schema = batch.schema();
    let col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == col_name)
        .unwrap_or_else(|| panic!("Column '{col_name}' not found in schema"));

    let dt = schema.field(col_idx).data_type();
    assert!(
        matches!(dt, DataType::Date32),
        "Expected Date32 type for '{col_name}', got {dt:?}"
    );
}

// =============================================================================
// Postgres
// =============================================================================

#[tokio::test]
#[ignore = "requires Postgres test container on port 5434"]
async fn postgres_arrow_timestamps() {
    let config = json!({
        "host": "localhost",
        "port": 5434,
        "database": "test_db",
        "ssl_mode": "disable"
    });
    let credentials = json!({
        "username": "test_user",
        "password": "test_password"
    });

    let provider = create_test_provider("postgres", config, credentials).await;

    // Create table with timestamps
    let _ = provider
        .execute_query(
            "CREATE TABLE IF NOT EXISTS arrow_test (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMPTZ DEFAULT NOW(),
                dt DATE DEFAULT CURRENT_DATE,
                val NUMERIC(10,4) DEFAULT 3.1415,
                name TEXT DEFAULT 'test'
            )",
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    let _ = provider
        .execute_query(
            "INSERT INTO arrow_test (id) VALUES (1), (2), (3) ON CONFLICT DO NOTHING",
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    let result = provider
        .execute_query(
            "SELECT id, ts, dt, val, name FROM arrow_test ORDER BY id",
            Some(10),
            None,
            false,
            None,
        )
        .await
        .unwrap();

    assert!(
        result.record_batch.is_some(),
        "Postgres: record_batch should be populated"
    );
    let batch = result.record_batch.unwrap();
    assert!(
        batch.num_rows() >= 3,
        "Expected at least 3 rows, got {}",
        batch.num_rows()
    );

    // Verify Arrow types
    assert_batch_has_timestamps(&batch, "ts");
    assert_batch_has_dates(&batch, "dt");

    // Verify the batch has no null timestamps
    let ts_col = batch.column(1);
    for i in 0..batch.num_rows() {
        assert!(
            !ts_col.is_null(i),
            "Postgres: ts column row {i} should not be null"
        );
    }

    provider.close().await;
    println!(
        "Postgres: ✅ timestamps={}, dates={}, rows={}",
        batch.num_rows(),
        batch.num_columns(),
        batch.num_rows()
    );
}

// =============================================================================
// MySQL
// =============================================================================

#[tokio::test]
#[ignore = "requires MySQL test container on port 3308"]
async fn mysql_arrow_timestamps() {
    let config = json!({
        "host": "127.0.0.1",
        "port": 3308,
        "database": "test_db"
    });
    let credentials = json!({
        "username": "test_user",
        "password": "test_password"
    });

    let provider = create_test_provider("mysql", config, credentials).await;

    let _ = provider
        .execute_query(
            "CREATE TABLE IF NOT EXISTS arrow_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                dt DATE DEFAULT (CURRENT_DATE),
                val DECIMAL(10,4) DEFAULT 3.1415,
                name VARCHAR(50) DEFAULT 'test'
            )",
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    let _ = provider
        .execute_query(
            "INSERT IGNORE INTO arrow_test (id) VALUES (1), (2), (3)",
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    let result = provider
        .execute_query(
            "SELECT id, ts, dt, val, name FROM arrow_test ORDER BY id",
            Some(10),
            None,
            false,
            None,
        )
        .await
        .unwrap();

    assert!(
        result.record_batch.is_some(),
        "MySQL: record_batch should be populated"
    );
    let batch = result.record_batch.unwrap();
    assert!(batch.num_rows() >= 3);

    assert_batch_has_timestamps(&batch, "ts");
    assert_batch_has_dates(&batch, "dt");

    provider.close().await;
    println!("MySQL: ✅ timestamps correct, {} rows", batch.num_rows());
}

// =============================================================================
// ClickHouse
// =============================================================================

#[tokio::test]
#[ignore = "requires ClickHouse test container on port 8124"]
async fn clickhouse_arrow_timestamps() {
    let config = json!({
        "host": "localhost",
        "port": 8124,
        "database": "test_db"
    });
    let credentials = json!({
        "username": "test_user",
        "password": "test_password"
    });

    let provider = create_test_provider("clickhouse", config, credentials).await;

    let _ = provider
        .execute_query(
            "CREATE TABLE IF NOT EXISTS arrow_test (
                id UInt32,
                ts DateTime DEFAULT now(),
                dt Date DEFAULT today(),
                val Float64 DEFAULT 3.14159,
                name String DEFAULT 'test'
            ) ENGINE = MergeTree() ORDER BY id",
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    let _ = provider
        .execute_query(
            "INSERT INTO arrow_test (id) VALUES (1), (2), (3)",
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    let result = provider
        .execute_query(
            "SELECT id, ts, dt, val, name FROM arrow_test ORDER BY id",
            Some(10),
            None,
            false,
            None,
        )
        .await
        .unwrap();

    assert!(
        result.record_batch.is_some(),
        "ClickHouse: record_batch should be populated"
    );
    let batch = result.record_batch.unwrap();
    assert!(batch.num_rows() >= 3);

    // ClickHouse DateTime maps to Timestamp
    assert_batch_has_timestamps(&batch, "ts");
    assert_batch_has_dates(&batch, "dt");

    // Critical: verify timestamps are NOT null (this was the bug we fixed)
    let ts_col = batch.column(1);
    for i in 0..batch.num_rows() {
        assert!(
            !ts_col.is_null(i),
            "ClickHouse: ts column row {i} should NOT be null (this was the DateTime null bug)"
        );
    }

    provider.close().await;
    println!(
        "ClickHouse: ✅ timestamps correct (non-null), {} rows",
        batch.num_rows()
    );
}

// =============================================================================
// SQL Server
// =============================================================================

#[tokio::test]
#[ignore = "requires SQL Server test container on port 1434"]
async fn sqlserver_arrow_timestamps() {
    let config = json!({
        "host": "localhost",
        "port": 1434,
        "database": "master",
        "encrypt": false
    });
    let credentials = json!({
        "username": "sa",
        "password": "TestPassword123!"
    });

    let provider = create_test_provider("sqlserver", config, credentials).await;

    let _ = provider
        .execute_query(
            "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'arrow_test')
             CREATE TABLE arrow_test (
                id INT IDENTITY PRIMARY KEY,
                ts DATETIME2 DEFAULT GETDATE(),
                dt DATE DEFAULT CAST(GETDATE() AS DATE),
                val DECIMAL(10,4) DEFAULT 3.1415,
                name NVARCHAR(50) DEFAULT 'test'
             )",
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    let _ = provider
        .execute_query(
            "IF NOT EXISTS (SELECT * FROM arrow_test WHERE id = 1)
             BEGIN
                INSERT INTO arrow_test DEFAULT VALUES;
                INSERT INTO arrow_test DEFAULT VALUES;
                INSERT INTO arrow_test DEFAULT VALUES;
             END",
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    let result = provider
        .execute_query(
            "SELECT id, ts, dt, val, name FROM arrow_test ORDER BY id",
            Some(10),
            None,
            false,
            None,
        )
        .await
        .unwrap();

    assert!(
        result.record_batch.is_some(),
        "SQL Server: record_batch should be populated"
    );
    let batch = result.record_batch.unwrap();
    assert!(batch.num_rows() >= 3);

    assert_batch_has_timestamps(&batch, "ts");
    assert_batch_has_dates(&batch, "dt");
    provider.close().await;
    println!(
        "SQL Server: ✅ timestamps correct, {} rows",
        batch.num_rows()
    );
}
