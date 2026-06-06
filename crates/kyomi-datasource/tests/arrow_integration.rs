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

// =============================================================================
// ClickHouse DataFusion Provider — Comparison Tests
// =============================================================================
// These tests run the same queries through both the HTTP-based ClickHouseProvider
// and the native DataFusionClickHouseProvider, comparing Arrow RecordBatch output.
// Requires: ClickHouse test container on port 8124 AND both features enabled:
//   cargo test --test arrow_integration --features "clickhouse,datafusion-providers" -- --ignored

#[cfg(all(feature = "clickhouse", feature = "datafusion-providers"))]
mod clickhouse_comparison {
    use super::*;
    use kyomi_datasource::providers::clickhouse::ClickHouseProvider;
    use kyomi_datasource::providers::clickhouse_datafusion::DataFusionClickHouseProvider;

    const CH_PORT: u16 = 8124;

    fn ch_config() -> serde_json::Value {
        json!({
            "host": "localhost",
            "port": CH_PORT,
            "database": "test_db"
        })
    }

    fn ch_credentials() -> serde_json::Value {
        json!({
            "username": "test_user",
            "password": "test_password"
        })
    }

    async fn create_both_providers() -> (ClickHouseProvider, DataFusionClickHouseProvider) {
        let config = ch_config();
        let creds = ch_credentials();
        let old = ClickHouseProvider::new(&config, &creds)
            .await
            .expect("ClickHouseProvider::new failed");
        let new = DataFusionClickHouseProvider::new(&config, &creds)
            .await
            .expect("DataFusionClickHouseProvider::new failed");
        (old, new)
    }

    /// Set up the comparison_test table with seed data.
    /// Each test calls this to be self-contained (no execution-order dependency).
    /// Drops the table first to avoid duplicate rows from parallel test execution.
    async fn setup_table(provider: &impl DatasourceProvider) {
        let _ = provider
            .execute_query(
                "DROP TABLE IF EXISTS comparison_test",
                None,
                None,
                false,
                None,
            )
            .await;

        provider
            .execute_query(
                "CREATE TABLE comparison_test (
                    id UInt32,
                    name String,
                    value Float64
                ) ENGINE = MergeTree() ORDER BY id",
                None,
                None,
                false,
                None,
            )
            .await
            .expect("setup: CREATE TABLE failed");

        provider
            .execute_query(
                "INSERT INTO comparison_test VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, 'carol', 3.5)",
                None,
                None,
                false,
                None,
            )
            .await
            .expect("setup: INSERT failed");
    }

    fn assert_batches_equivalent(
        old: &kyomi_datasource::provider::QueryResult,
        new: &kyomi_datasource::provider::QueryResult,
        query_label: &str,
    ) {
        assert_eq!(old.status, new.status, "{query_label}: status mismatch");

        let old_batch = old
            .record_batch
            .as_ref()
            .unwrap_or_else(|| panic!("{query_label}: old provider returned no batch"));
        let new_batch = new
            .record_batch
            .as_ref()
            .unwrap_or_else(|| panic!("{query_label}: new provider returned no batch"));

        // Compare schemas (column names AND types).
        let old_schema = old_batch.schema();
        let new_schema = new_batch.schema();
        assert_eq!(
            old_schema.fields().len(),
            new_schema.fields().len(),
            "{query_label}: column count mismatch"
        );
        for i in 0..old_schema.fields().len() {
            assert_eq!(
                old_schema.field(i).name(),
                new_schema.field(i).name(),
                "{query_label}: column {i} name mismatch"
            );
            assert_eq!(
                old_schema.field(i).data_type(),
                new_schema.field(i).data_type(),
                "{query_label}: column {i} type mismatch"
            );
        }

        // Compare row counts.
        assert_eq!(
            old_batch.num_rows(),
            new_batch.num_rows(),
            "{query_label}: row count mismatch (old={}, new={})",
            old_batch.num_rows(),
            new_batch.num_rows()
        );

        // Compare column metadata.
        let old_cols = old.columns.as_ref().unwrap();
        let new_cols = new.columns.as_ref().unwrap();
        assert_eq!(
            old_cols.len(),
            new_cols.len(),
            "{query_label}: column metadata count mismatch"
        );
        for i in 0..old_cols.len() {
            assert_eq!(
                old_cols[i].name, new_cols[i].name,
                "{query_label}: column metadata {i} name mismatch"
            );
        }
    }

    /// Compare cell values column-by-column for columns that are comparable
    /// (numeric and string types). Skips timestamp/date columns because the
    /// two providers may format them differently.
    fn assert_cell_values_match(
        old_batch: &RecordBatch,
        new_batch: &RecordBatch,
        query_label: &str,
    ) {
        assert_eq!(
            old_batch.num_rows(),
            new_batch.num_rows(),
            "{query_label}: row count mismatch for value comparison"
        );
        let num_cols = old_batch.num_columns();
        let num_rows = old_batch.num_rows();

        for col_idx in 0..num_cols {
            let dt = old_batch.schema().field(col_idx).data_type().clone();
            // Skip timestamp/date — formatting may differ.
            if matches!(
                dt,
                DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
            ) {
                continue;
            }

            for row_idx in 0..num_rows {
                let old_null = old_batch.column(col_idx).is_null(row_idx);
                let new_null = new_batch.column(col_idx).is_null(row_idx);
                assert_eq!(
                    old_null, new_null,
                    "{query_label}: col {col_idx} row {row_idx} null mismatch"
                );
                if old_null {
                    continue;
                }

                // Compare using debug string representation (works for all types).
                let old_val = arrow::array::Array::as_any(old_batch.column(col_idx).as_ref());
                let new_val = arrow::array::Array::as_any(new_batch.column(col_idx).as_ref());

                // Downcast and compare based on type.
                macro_rules! compare_numeric {
                    ($arrty:ty) => {
                        if let (Some(old_arr), Some(new_arr)) = (
                            old_val.downcast_ref::<$arrty>(),
                            new_val.downcast_ref::<$arrty>(),
                        ) {
                            assert_eq!(
                                old_arr.value(row_idx),
                                new_arr.value(row_idx),
                                "{query_label}: col {col_idx} row {row_idx} value mismatch"
                            );
                        }
                    };
                }

                compare_numeric!(arrow::array::Int8Array);
                compare_numeric!(arrow::array::Int16Array);
                compare_numeric!(arrow::array::Int32Array);
                compare_numeric!(arrow::array::Int64Array);
                compare_numeric!(arrow::array::UInt8Array);
                compare_numeric!(arrow::array::UInt16Array);
                compare_numeric!(arrow::array::UInt32Array);
                compare_numeric!(arrow::array::UInt64Array);
                compare_numeric!(arrow::array::Float32Array);
                compare_numeric!(arrow::array::Float64Array);

                if let (Some(old_arr), Some(new_arr)) = (
                    old_val.downcast_ref::<arrow::array::StringArray>(),
                    new_val.downcast_ref::<arrow::array::StringArray>(),
                ) {
                    assert_eq!(
                        old_arr.value(row_idx),
                        new_arr.value(row_idx),
                        "{query_label}: col {col_idx} row {row_idx} string mismatch"
                    );
                }
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse test container on port 8124"]
    async fn comparison_select_simple() {
        let (old, new) = create_both_providers().await;
        setup_table(&old).await;

        let sql = "SELECT id, name, value FROM comparison_test ORDER BY id";
        let old_result = old
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();
        let new_result = new
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();

        assert_batches_equivalent(&old_result, &new_result, "SELECT simple");
        assert_cell_values_match(
            old_result.record_batch.as_ref().unwrap(),
            new_result.record_batch.as_ref().unwrap(),
            "SELECT simple",
        );

        old.close().await;
        new.close().await;
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse test container on port 8124"]
    async fn comparison_select_with_where() {
        let (old, new) = create_both_providers().await;
        setup_table(&old).await;

        let sql = "SELECT id, name FROM comparison_test WHERE id > 1 ORDER BY id";
        let old_result = old
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();
        let new_result = new
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();

        assert_batches_equivalent(&old_result, &new_result, "SELECT with WHERE");
        assert_cell_values_match(
            old_result.record_batch.as_ref().unwrap(),
            new_result.record_batch.as_ref().unwrap(),
            "SELECT with WHERE",
        );

        old.close().await;
        new.close().await;
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse test container on port 8124"]
    async fn comparison_group_by() {
        let (old, new) = create_both_providers().await;
        setup_table(&old).await;

        let sql = "SELECT name, count(*) as cnt, sum(value) as total FROM comparison_test GROUP BY name ORDER BY name";
        let old_result = old
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();
        let new_result = new
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();

        assert_batches_equivalent(&old_result, &new_result, "GROUP BY");
        assert_cell_values_match(
            old_result.record_batch.as_ref().unwrap(),
            new_result.record_batch.as_ref().unwrap(),
            "GROUP BY",
        );

        old.close().await;
        new.close().await;
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse test container on port 8124"]
    async fn comparison_aggregation() {
        let (old, new) = create_both_providers().await;
        setup_table(&old).await;

        let sql = "SELECT count(*) as cnt, sum(value) as total FROM comparison_test";
        let old_result = old
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();
        let new_result = new
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();

        assert_batches_equivalent(&old_result, &new_result, "aggregation");
        assert_cell_values_match(
            old_result.record_batch.as_ref().unwrap(),
            new_result.record_batch.as_ref().unwrap(),
            "aggregation",
        );

        old.close().await;
        new.close().await;
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse test container on port 8124"]
    async fn comparison_order_by_desc() {
        let (old, new) = create_both_providers().await;
        setup_table(&old).await;

        let sql = "SELECT id, value FROM comparison_test ORDER BY value DESC";
        let old_result = old
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();
        let new_result = new
            .execute_query(sql, None, None, false, None)
            .await
            .unwrap();

        assert_batches_equivalent(&old_result, &new_result, "ORDER BY DESC");
        assert_cell_values_match(
            old_result.record_batch.as_ref().unwrap(),
            new_result.record_batch.as_ref().unwrap(),
            "ORDER BY DESC",
        );

        // Verify actual ordering: values should be descending.
        let batch = old_result.record_batch.as_ref().unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        for i in 1..values.len() {
            assert!(
                values.value(i - 1) >= values.value(i),
                "ORDER BY DESC: values should be descending, got {} before {}",
                values.value(i - 1),
                values.value(i)
            );
        }

        old.close().await;
        new.close().await;
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse test container on port 8124"]
    async fn comparison_limit_offset() {
        let (old, new) = create_both_providers().await;
        setup_table(&old).await;

        let sql = "SELECT id, name FROM comparison_test ORDER BY id";
        let old_result = old
            .execute_query(sql, Some(2), Some(1), false, None)
            .await
            .unwrap();
        let new_result = new
            .execute_query(sql, Some(2), Some(1), false, None)
            .await
            .unwrap();

        assert_batches_equivalent(&old_result, &new_result, "LIMIT OFFSET");
        assert_cell_values_match(
            old_result.record_batch.as_ref().unwrap(),
            new_result.record_batch.as_ref().unwrap(),
            "LIMIT OFFSET",
        );

        // Verify correct rows returned (id=2, id=3 after OFFSET 1).
        let batch = old_result.record_batch.as_ref().unwrap();
        assert_eq!(batch.num_rows(), 2, "LIMIT 2 should return 2 rows");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2, "First row after OFFSET 1 should be id=2");
        assert_eq!(ids.value(1), 3, "Second row after OFFSET 1 should be id=3");

        old.close().await;
        new.close().await;
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse test container on port 8124"]
    async fn datafusion_dry_run_validation() {
        let (_old, new) = create_both_providers().await;

        let result = new.dry_run("SELECT 1").await.unwrap();
        assert!(result.valid, "dry_run should validate SELECT 1");

        let result = new
            .dry_run("SELECT * FROM comparison_test WHERE id = 1")
            .await
            .unwrap();
        assert!(result.valid, "dry_run should validate SELECT with WHERE");

        let result = new.dry_run("SELCT 1").await.unwrap();
        assert!(!result.valid, "dry_run should reject invalid SQL");

        new.close().await;
    }

    #[tokio::test]
    #[ignore = "requires ClickHouse test container on port 8124"]
    async fn comparison_list_databases() {
        let (old, new) = create_both_providers().await;

        let old_dbs = old.list_databases().await;
        let new_dbs = new.list_databases().await;

        assert!(
            old_dbs.error.is_none(),
            "old list_databases error: {:?}",
            old_dbs.error
        );
        assert!(
            new_dbs.error.is_none(),
            "new list_databases error: {:?}",
            new_dbs.error
        );

        // Both should include test_db.
        assert!(old_dbs.items.contains(&"test_db".to_string()));
        assert!(new_dbs.items.contains(&"test_db".to_string()));

        old.close().await;
        new.close().await;
    }
}
