use std::str::FromStr;

use arrow::array::Array;
use futures_util::StreamExt;
use kyomi_connect_protocol::ArrowStreamEvent;
use kyomi_connect_protocol::stream::QueryFormat;
use kyomi_connect_protocol::wire::{
    CatalogColumn, CatalogContainer, CatalogResult, CatalogTable, ConnectOp, ConnectRequest,
    ConnectResponse, ConnectResponseBody, DryRunParams, QueryParams,
};
use kyomi_datasource::arrow_builder::{batch_to_ipc_bytes, schema_to_ipc_bytes};
use kyomi_datasource::provider::DatasourceProvider;

/// Streaming threshold: queries requesting more than this many rows (or no
/// limit at all) use the streaming path that returns multiple messages.
const STREAMING_THRESHOLD: u32 = 1000;

/// Executes commands from Kyomi against the local database.
pub struct CommandExecutor {
    provider: Box<dyn DatasourceProvider>,
    db_type: String,
}

impl CommandExecutor {
    /// Create an executor from config using kyomi-datasource factory.
    pub async fn from_config(config: &super::config::ConnectConfig) -> anyhow::Result<Self> {
        let ds_type = kyomi_connect_protocol::DatasourceType::from_str(&config.db_type)
            .map_err(|e| anyhow::anyhow!("Unsupported database type '{}': {e}", config.db_type))?;

        let provider = kyomi_datasource::create_provider(
            &ds_type,
            &config.connection_config(),
            &config.credentials(),
            None, // No user context for Connect
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create database provider: {e}"))?;

        // Test connection immediately -- fail fast
        provider
            .test_connection()
            .await
            .map_err(|e| anyhow::anyhow!("Database connection test failed: {e}"))?;

        tracing::info!(db_type = %config.db_type, "Database connection verified");

        Ok(Self {
            provider,
            db_type: config.db_type.clone(),
        })
    }

    /// Execute a command and return one or more responses.
    ///
    /// Most operations return a single response. `ExecuteQuery` with large or
    /// unlimited result sets returns an Arrow streaming sequence:
    /// `ArrowHeader` → `ArrowBatch`* → `ArrowComplete`.
    pub async fn execute(&self, request: ConnectRequest) -> Vec<ConnectResponse> {
        let request_id = request.id.clone();

        match request.op {
            ConnectOp::ExecuteQuery => {
                self.handle_execute_query_maybe_stream(&request_id, request.params)
                    .await
            }
            other_op => {
                let result = match other_op {
                    ConnectOp::TestConnection => self.handle_test_connection().await,
                    ConnectOp::DryRun => self.handle_dry_run(request.params).await,
                    ConnectOp::DiscoverCatalog => self.handle_discover_catalog().await,
                    ConnectOp::ExecuteQuery => unreachable!(),
                };
                vec![ConnectResponse {
                    id: request_id,
                    body: match result {
                        Ok(value) => ConnectResponseBody::Result { result: value },
                        Err(e) => ConnectResponseBody::Error {
                            error: e.to_string(),
                        },
                    },
                }]
            }
        }
    }

    async fn handle_test_connection(&self) -> anyhow::Result<serde_json::Value> {
        let ok = self
            .provider
            .test_connection()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(serde_json::json!(ok))
    }

    /// Handle execute_query: use buffered path for small queries, Arrow streaming for large.
    async fn handle_execute_query_maybe_stream(
        &self,
        request_id: &str,
        params: Option<serde_json::Value>,
    ) -> Vec<ConnectResponse> {
        let params: QueryParams = match params
            .ok_or_else(|| anyhow::anyhow!("execute_query requires params"))
            .and_then(|v| serde_json::from_value(v).map_err(|e| anyhow::anyhow!("{e}")))
        {
            Ok(p) => p,
            Err(e) => {
                return vec![ConnectResponse {
                    id: request_id.to_string(),
                    body: ConnectResponseBody::Error {
                        error: e.to_string(),
                    },
                }];
            }
        };

        let use_streaming = match params.limit {
            Some(limit) if limit <= STREAMING_THRESHOLD => false,
            _ => true, // No limit or limit > threshold
        };

        if !use_streaming {
            // Buffered path: single Result response (zero overhead for common case)
            let result = match self
                .provider
                .execute_query(
                    &params.sql,
                    params.limit,
                    params.offset,
                    params.include_total,
                    params.job_id.as_deref(),
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return vec![ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::Error {
                            error: e.to_string(),
                        },
                    }];
                }
            };

            // When Arrow format is requested and the provider populated record_batch,
            // return three Arrow IPC messages: ArrowHeader → ArrowBatch → ArrowComplete.
            // If record_batch is None (provider didn't populate it), fall through to JSON.
            if params.format == QueryFormat::Arrow
                && let Some(batch) = result.record_batch
            {
                let columns = result.columns.unwrap_or_default();
                let total_rows = result.total_rows;
                let execution_time_ms = result.execution_time_ms;
                let bytes_processed = result.bytes_processed;
                let row_count = batch.num_rows() as u64;

                let schema_ipc = match schema_to_ipc_bytes(batch.schema_ref()) {
                    Ok(b) => b,
                    Err(e) => {
                        return vec![ConnectResponse {
                            id: request_id.to_string(),
                            body: ConnectResponseBody::Error {
                                error: format!("Arrow schema serialization failed: {e}"),
                            },
                        }];
                    }
                };

                let ipc_bytes = match batch_to_ipc_bytes(&batch) {
                    Ok(b) => b,
                    Err(e) => {
                        return vec![ConnectResponse {
                            id: request_id.to_string(),
                            body: ConnectResponseBody::Error {
                                error: format!("Arrow batch serialization failed: {e}"),
                            },
                        }];
                    }
                };

                return vec![
                    ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::ArrowHeader {
                            schema_ipc,
                            columns,
                            total_rows,
                        },
                    },
                    ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::ArrowBatch {
                            ipc_bytes,
                            chunk_index: 0,
                        },
                    },
                    ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::ArrowComplete {
                            execution_time_ms,
                            bytes_processed,
                            total_chunks: 1,
                            total_rows_returned: row_count,
                            job_id: result.job_id.clone(),
                        },
                    },
                ];
            }

            if params.format == QueryFormat::Arrow {
                // record_batch is None — provider didn't build Arrow data.
                // Fall through to JSON path below.
                tracing::debug!(
                    "Arrow format requested but provider did not populate record_batch; \
                     falling back to JSON"
                );
            }

            let value = match serde_json::to_value(&result) {
                Ok(v) => v,
                Err(e) => {
                    return vec![ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::Error {
                            error: e.to_string(),
                        },
                    }];
                }
            };

            vec![ConnectResponse {
                id: request_id.to_string(),
                body: ConnectResponseBody::Result { result: value },
            }]
        } else {
            // Arrow streaming path: ArrowHeader → ArrowBatch* → ArrowComplete
            self.execute_query_streaming_arrow(request_id, &params)
                .await
        }
    }

    /// Execute a query via the Arrow streaming path, returning multiple responses
    /// (`ArrowHeader` → `ArrowBatch*` → `ArrowComplete`).
    async fn execute_query_streaming_arrow(
        &self,
        request_id: &str,
        params: &QueryParams,
    ) -> Vec<ConnectResponse> {
        let mut stream = match self
            .provider
            .execute_query_stream_arrow(
                &params.sql,
                params.limit,
                params.offset,
                params.include_total,
                None, // Use default chunk size
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return vec![ConnectResponse {
                    id: request_id.to_string(),
                    body: ConnectResponseBody::Error {
                        error: e.to_string(),
                    },
                }];
            }
        };

        let mut responses = Vec::new();

        while let Some(event) = stream.next().await {
            match event {
                Ok(ArrowStreamEvent::Schema {
                    schema_ipc,
                    columns,
                    total_rows,
                }) => {
                    responses.push(ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::ArrowHeader {
                            schema_ipc,
                            columns,
                            total_rows,
                        },
                    });
                }
                Ok(ArrowStreamEvent::Batch {
                    ipc_bytes,
                    chunk_index,
                }) => {
                    responses.push(ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::ArrowBatch {
                            ipc_bytes,
                            chunk_index,
                        },
                    });
                }
                Ok(ArrowStreamEvent::Complete {
                    execution_time_ms,
                    bytes_processed,
                    total_chunks,
                    total_rows_returned,
                }) => {
                    responses.push(ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::ArrowComplete {
                            execution_time_ms,
                            bytes_processed,
                            total_chunks,
                            total_rows_returned,
                            job_id: None,
                        },
                    });
                }
                Err(e) => {
                    // Mid-stream error: send Error response and stop
                    responses.push(ConnectResponse {
                        id: request_id.to_string(),
                        body: ConnectResponseBody::Error {
                            error: e.to_string(),
                        },
                    });
                    break;
                }
            }
        }

        responses
    }

    async fn handle_dry_run(
        &self,
        params: Option<serde_json::Value>,
    ) -> anyhow::Result<serde_json::Value> {
        let params: DryRunParams = params
            .ok_or_else(|| anyhow::anyhow!("dry_run requires params"))
            .and_then(|v| serde_json::from_value(v).map_err(|e| anyhow::anyhow!("{e}")))?;

        let result = self
            .provider
            .dry_run(&params.sql)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        serde_json::to_value(&result).map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Discover the full catalog (containers/tables/columns) using
    /// information_schema queries specific to the database type.
    async fn handle_discover_catalog(&self) -> anyhow::Result<serde_json::Value> {
        let containers = self.discover_containers().await?;
        let mut catalog_containers = Vec::new();

        for container_name in &containers {
            let tables = self.discover_tables(container_name).await?;
            let mut catalog_tables = Vec::new();

            for (table_name, table_type) in &tables {
                let columns = self.discover_columns(container_name, table_name).await?;
                catalog_tables.push(CatalogTable {
                    name: table_name.clone(),
                    native_type: Some(table_type.clone()),
                    columns,
                });
            }

            catalog_containers.push(CatalogContainer {
                name: container_name.clone(),
                tables: catalog_tables,
            });
        }

        let result = CatalogResult {
            containers: catalog_containers,
        };
        serde_json::to_value(&result).map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Discover containers (schemas/databases) for the datasource type.
    async fn discover_containers(&self) -> anyhow::Result<Vec<String>> {
        let sql = match self.db_type.as_str() {
            "postgres" => {
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                 ORDER BY schema_name"
            }
            "redshift" => {
                "SELECT schema_name FROM svv_all_schemas \
                 WHERE database_name = current_database() \
                   AND schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_internal', 'pg_toast', \
                                           'pg_automv', 'pg_auto_copy', 'pg_mv', 'pg_s3', 'catalog_history') \
                 ORDER BY schema_name"
            }
            "mysql" => {
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA \
                 WHERE SCHEMA_NAME NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') \
                 ORDER BY SCHEMA_NAME"
            }
            "clickhouse" => {
                "SELECT name FROM system.databases \
                 WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') \
                 ORDER BY name"
            }
            "sqlserver" | "synapse" => {
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA \
                 WHERE SCHEMA_NAME NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest') \
                 ORDER BY SCHEMA_NAME"
            }
            other => {
                return Err(anyhow::anyhow!(
                    "Unsupported database type for catalog discovery: {other}"
                ));
            }
        };

        let result = self
            .provider
            .execute_query(sql, None, None, false, None)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list containers: {e}"))?;

        let items = kyomi_datasource::provider::extract_string_col_from_batch(
            result.record_batch.as_ref(),
            0,
        );

        Ok(items)
    }

    /// Discover tables in a container.
    /// Returns (table_name, table_type) pairs.
    async fn discover_tables(&self, container: &str) -> anyhow::Result<Vec<(String, String)>> {
        let sql = match self.db_type.as_str() {
            "postgres" | "redshift" => format!(
                "SELECT table_name, table_type FROM information_schema.tables \
                 WHERE table_schema = '{}' ORDER BY table_name",
                escape_sql_literal(container)
            ),
            "mysql" => format!(
                "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = '{}' ORDER BY TABLE_NAME",
                escape_sql_literal(container)
            ),
            "clickhouse" => format!(
                "SELECT name, engine FROM system.tables \
                 WHERE database = '{}' ORDER BY name",
                escape_sql_literal(container)
            ),
            "sqlserver" | "synapse" => format!(
                "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES \
                 WHERE TABLE_SCHEMA = '{}' ORDER BY TABLE_NAME",
                escape_sql_literal(container)
            ),
            other => {
                return Err(anyhow::anyhow!("Unsupported database type: {other}"));
            }
        };

        let result = self
            .provider
            .execute_query(&sql, None, None, false, None)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables in '{container}': {e}"))?;

        let items = if let Some(batch) = result.record_batch.as_ref() {
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>();
            let types = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>();
            match (names, types) {
                (Some(names), Some(types)) => (0..batch.num_rows())
                    .filter_map(|i| {
                        if names.is_null(i) {
                            return None;
                        }
                        let name = names.value(i).to_string();
                        let table_type = if types.is_null(i) {
                            "TABLE".to_string()
                        } else {
                            types.value(i).to_string()
                        };
                        Some((name, table_type))
                    })
                    .collect(),
                _ => vec![],
            }
        } else {
            vec![]
        };

        Ok(items)
    }

    /// Discover columns for a specific table.
    async fn discover_columns(
        &self,
        container: &str,
        table_name: &str,
    ) -> anyhow::Result<Vec<CatalogColumn>> {
        let esc_container = escape_sql_literal(container);
        let esc_table = escape_sql_literal(table_name);

        let sql = match self.db_type.as_str() {
            "postgres" | "redshift" => format!(
                "SELECT column_name, data_type, '' as description \
                 FROM information_schema.columns \
                 WHERE table_schema = '{esc_container}' AND table_name = '{esc_table}' \
                 ORDER BY ordinal_position"
            ),
            "mysql" => format!(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT \
                 FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = '{esc_container}' AND TABLE_NAME = '{esc_table}' \
                 ORDER BY ORDINAL_POSITION"
            ),
            "clickhouse" => format!(
                "SELECT name, type, comment \
                 FROM system.columns \
                 WHERE database = '{esc_container}' AND table = '{esc_table}' \
                 ORDER BY position"
            ),
            "sqlserver" | "synapse" => format!(
                "SELECT COLUMN_NAME, DATA_TYPE, '' as description \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_SCHEMA = '{esc_container}' AND TABLE_NAME = '{esc_table}' \
                 ORDER BY ORDINAL_POSITION"
            ),
            other => {
                return Err(anyhow::anyhow!("Unsupported database type: {other}"));
            }
        };

        let result = self
            .provider
            .execute_query(&sql, None, None, false, None)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to list columns for '{container}.{table_name}': {e}")
            })?;

        let columns = if let Some(batch) = result.record_batch.as_ref() {
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>();
            let types = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>();
            let descs = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>();
            match (names, types, descs) {
                (Some(names), Some(types), Some(descs)) => (0..batch.num_rows())
                    .filter_map(|i| {
                        if names.is_null(i) {
                            return None;
                        }
                        let name = names.value(i).to_string();
                        let native_type = if types.is_null(i) {
                            "unknown".to_string()
                        } else {
                            types.value(i).to_string()
                        };
                        let description = if descs.is_null(i) {
                            None
                        } else {
                            let s = descs.value(i);
                            if s.is_empty() {
                                None
                            } else {
                                Some(s.to_string())
                            }
                        };
                        Some(CatalogColumn {
                            name,
                            native_type,
                            description,
                        })
                    })
                    .collect(),
                _ => vec![],
            }
        } else {
            vec![]
        };

        Ok(columns)
    }
}

/// Escape single quotes in SQL string literals.
fn escape_sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_sql_literal_no_quotes() {
        assert_eq!(escape_sql_literal("public"), "public");
    }

    #[test]
    fn escape_sql_literal_with_quotes() {
        assert_eq!(escape_sql_literal("it's"), "it''s");
    }

    #[test]
    fn escape_sql_literal_multiple_quotes() {
        assert_eq!(escape_sql_literal("a'b'c"), "a''b''c");
    }
}
