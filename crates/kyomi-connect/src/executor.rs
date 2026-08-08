use std::str::FromStr;

use arrow::array::Array;
use futures_util::StreamExt;
use kyomi_connect_protocol::ArrowStreamEvent;
use kyomi_connect_protocol::stream::QueryFormat;
use kyomi_connect_protocol::wire::{
    CatalogColumn, CatalogContainer, CatalogResult, CatalogTable, ConnectOp, ConnectRequest,
    ConnectResponse, ConnectResponseBody, DiscoverCatalogParams, DryRunParams, QueryParams,
};
use kyomi_datasource::arrow_builder::{batch_to_ipc_bytes, schema_to_ipc_bytes};
use kyomi_datasource::provider::{DatasourceProvider, QueryResult, QueryStatus};

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
                    ConnectOp::DiscoverCatalog => {
                        self.handle_discover_catalog(request.params).await
                    }
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

    /// Discover the catalog (containers/tables/columns) using information_schema
    /// queries specific to the database type.
    ///
    /// Honors the optional [`DiscoverCatalogParams`] scope (KYO-162):
    /// - `containers`: restrict discovery to the named schemas/databases
    ///   (case-insensitive). `None`/empty means "all containers".
    /// - `containers_only`: return just container names with empty `tables`,
    ///   skipping the per-table column crawl (used to populate the scope picker).
    ///
    /// `params == None` (a legacy caller) is treated as an unscoped full crawl.
    async fn handle_discover_catalog(
        &self,
        params: Option<serde_json::Value>,
    ) -> anyhow::Result<serde_json::Value> {
        let params: DiscoverCatalogParams = match params {
            Some(value) => serde_json::from_value(value)
                .map_err(|e| anyhow::anyhow!("invalid discover_catalog params: {e}"))?,
            None => DiscoverCatalogParams::default(),
        };

        let containers = filter_containers_to_scope(
            self.discover_containers().await?,
            params.containers.as_deref(),
        );

        // Lightweight listing for the scope picker: container names only.
        if params.containers_only {
            let catalog_containers = containers
                .into_iter()
                .map(|name| CatalogContainer {
                    name,
                    tables: Vec::new(),
                })
                .collect();
            let result = CatalogResult {
                containers: catalog_containers,
                errors: Vec::new(),
            };
            return serde_json::to_value(&result).map_err(|e| anyhow::anyhow!("{e}"));
        }

        // Tolerate a single container's (or table's) failure rather than
        // aborting the whole crawl (KYO-268). Before 66267bb, a
        // permission-denied container silently yielded zero tables for
        // itself while the rest still enumerated; 66267bb correctly made
        // that failure visible via `ensure_query_ok`, but as a side effect
        // also made it fatal for every other container. Losing one schema
        // out of ten should not blind search/NL discovery on the other nine.
        let mut catalog_containers = Vec::new();
        let mut errors = Vec::new();
        for container_name in &containers {
            let tables = match self.discover_tables(container_name).await {
                Ok(tables) => tables,
                Err(e) => {
                    tracing::warn!(
                        container = %container_name,
                        error = %e,
                        "discover_tables failed for container; skipping it"
                    );
                    errors.push(e.to_string());
                    continue;
                }
            };
            let mut catalog_tables = Vec::new();

            for (table_name, table_type) in &tables {
                let columns = match self.discover_columns(container_name, table_name).await {
                    Ok(columns) => columns,
                    Err(e) => {
                        tracing::warn!(
                            container = %container_name,
                            table = %table_name,
                            error = %e,
                            "discover_columns failed for table; skipping it"
                        );
                        errors.push(e.to_string());
                        continue;
                    }
                };
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

        // An empty-but-successful result is exactly the silent-empty
        // behavior `66267bb` set out to kill. If there was at least one
        // container to crawl and every single one of them failed, that's a
        // total failure, not a successful empty catalog.
        if !containers.is_empty() && catalog_containers.is_empty() {
            return Err(anyhow::anyhow!(
                "discover_catalog failed for every container ({} attempted): {}",
                containers.len(),
                errors.join("; ")
            ));
        }

        let result = CatalogResult {
            containers: catalog_containers,
            errors,
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

        ensure_query_ok(&result, "Failed to list containers")?;

        let items = kyomi_datasource::provider::extract_string_col_from_batch(
            result.record_batch.as_ref(),
            0,
        );

        // Defense-in-depth: mirror the direct-path Rust filter (KYO-128) so the
        // Connect and direct catalog paths return the same schema set even if
        // svv_all_schemas ever surfaces a session-scoped pg_temp_* namespace or
        // an unexpectedly-cased system schema. Only applies to Redshift.
        let items = if self.db_type == "redshift" {
            items
                .into_iter()
                .filter(|n| !is_redshift_system_schema(n))
                .collect()
        } else {
            items
        };

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

        ensure_query_ok(&result, &format!("Failed to list tables in '{container}'"))?;

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

        ensure_query_ok(
            &result,
            &format!("Failed to list columns for '{container}.{table_name}'"),
        )?;

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

/// Convert a query-level failure into a Rust error.
///
/// Providers report permission errors, timeouts, and bad SQL as
/// `Ok(QueryResult { status: QueryStatus::Error, error: Some(..) })` rather
/// than `Err` (see `kyomi_datasource::provider::QueryResult`). Discovery code
/// that reads `record_batch` without checking `status` first turns a
/// permission denial into "0 rows discovered" — a Redshift role without
/// catalog access looked like an empty, successfully-indexed schema
/// (KYO-126). This must be called immediately after every `execute_query`
/// used for discovery, before any `record_batch` extraction.
///
/// Every current provider implementation
/// (`crates/kyomi-datasource/src/providers/*.rs`) sets `error: None`
/// whenever `status == QueryStatus::Success`, so checking `status` alone
/// would be sufficient for today's providers. This also treats a populated
/// `error` on a non-`Error` status as a failure: it costs nothing given that
/// invariant, and it means a future provider that sets `error` but forgets
/// to flip `status` still gets caught here instead of silently
/// reintroducing this exact bug.
fn ensure_query_ok(result: &QueryResult, context: &str) -> anyhow::Result<()> {
    if result.status == QueryStatus::Error || result.error.is_some() {
        let message = result
            .error
            .as_deref()
            .unwrap_or("query failed with no error message");
        return Err(anyhow::anyhow!("{context}: {message}"));
    }
    Ok(())
}

/// System schemas excluded from Redshift catalog discovery (case-insensitive).
/// Mirrors the direct-path filter in the kyomi-agent redshift indexer (KYO-128).
const REDSHIFT_SYSTEM_SCHEMAS: &[&str] = &[
    "pg_catalog",
    "pg_internal",
    "information_schema",
    "pg_toast",
    "pg_automv",
    "pg_auto_copy",
    "pg_mv",
    "pg_s3",
    "catalog_history",
];

/// Prefixes for temp schemas excluded dynamically (e.g. pg_temp_1, pg_temp_99).
const REDSHIFT_SYSTEM_SCHEMA_PREFIXES: &[&str] = &["pg_temp_"];

/// Check if a schema name is a Redshift system schema (case-insensitive).
fn is_redshift_system_schema(name: &str) -> bool {
    let lower = name.to_lowercase();
    REDSHIFT_SYSTEM_SCHEMAS.iter().any(|s| lower == *s)
        || REDSHIFT_SYSTEM_SCHEMA_PREFIXES
            .iter()
            .any(|p| lower.starts_with(p))
}

/// Restrict a discovered container list to a requested scope (KYO-162).
///
/// A `None` or empty scope means "all containers" (the historical behavior),
/// so the full list is returned unchanged. Otherwise only containers whose name
/// matches an entry in `scope` (case-insensitively) are kept; requested names
/// that don't exist are simply absent from the result.
fn filter_containers_to_scope(containers: Vec<String>, scope: Option<&[String]>) -> Vec<String> {
    match scope {
        Some(scope) if !scope.is_empty() => containers
            .into_iter()
            .filter(|name| scope.iter().any(|s| s.eq_ignore_ascii_case(name)))
            .collect(),
        _ => containers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_containers_none_scope_returns_all() {
        let all = vec!["public".to_string(), "analytics".to_string()];
        assert_eq!(filter_containers_to_scope(all.clone(), None), all);
    }

    #[test]
    fn filter_containers_empty_scope_returns_all() {
        // An explicit empty list must NOT mean "index nothing" here — the
        // agent-side default is "all"; the backend enforces empty-means-none
        // before it ever sends a scope. See ConnectIndexer.
        let all = vec!["public".to_string(), "analytics".to_string()];
        assert_eq!(filter_containers_to_scope(all.clone(), Some(&[])), all);
    }

    #[test]
    fn filter_containers_keeps_only_requested_case_insensitive() {
        let all = vec![
            "public".to_string(),
            "Analytics".to_string(),
            "staging".to_string(),
        ];
        let scope = vec!["ANALYTICS".to_string(), "public".to_string()];
        let kept = filter_containers_to_scope(all, Some(&scope));
        assert_eq!(kept, vec!["public".to_string(), "Analytics".to_string()]);
    }

    #[test]
    fn filter_containers_ignores_unknown_requested_names() {
        let all = vec!["public".to_string()];
        let scope = vec!["public".to_string(), "does_not_exist".to_string()];
        assert_eq!(
            filter_containers_to_scope(all, Some(&scope)),
            vec!["public".to_string()]
        );
    }

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

    #[test]
    fn redshift_system_schema_detection() {
        assert!(is_redshift_system_schema("pg_catalog"));
        assert!(is_redshift_system_schema("PG_CATALOG"));
        assert!(is_redshift_system_schema("pg_internal"));
        assert!(is_redshift_system_schema("information_schema"));
        assert!(is_redshift_system_schema("pg_toast"));
        assert!(is_redshift_system_schema("pg_temp_1"));
        assert!(is_redshift_system_schema("pg_temp_99"));
        assert!(is_redshift_system_schema("pg_automv"));
        assert!(is_redshift_system_schema("PG_AUTOMV"));
        assert!(is_redshift_system_schema("pg_auto_copy"));
        assert!(is_redshift_system_schema("pg_mv"));
        assert!(is_redshift_system_schema("pg_s3"));
        assert!(is_redshift_system_schema("catalog_history"));

        assert!(!is_redshift_system_schema("public"));
        assert!(!is_redshift_system_schema("myschema"));
        assert!(!is_redshift_system_schema("analytics"));
    }

    // -----------------------------------------------------------------
    // ensure_query_ok (KYO-126)
    // -----------------------------------------------------------------

    #[test]
    fn ensure_query_ok_errors_on_error_status_with_message() {
        let result = QueryResult::error("permission denied for schema svv_all_schemas");
        let err = ensure_query_ok(&result, "Failed to list containers")
            .expect_err("error status must propagate as Err");
        let message = err.to_string();
        assert!(
            message.contains("Failed to list containers"),
            "expected context in message, got: {message}"
        );
        assert!(
            message.contains("permission denied for schema svv_all_schemas"),
            "expected provider message verbatim, got: {message}"
        );
    }

    #[test]
    fn ensure_query_ok_errors_on_error_status_with_no_message() {
        // `error` can legitimately be `None` alongside `status == Error` if a
        // provider forgets to populate it -- must still fail closed with a
        // sensible fallback rather than panicking or claiming success.
        let result = QueryResult {
            status: QueryStatus::Error,
            error: None,
            ..QueryResult::success_empty()
        };
        let err = ensure_query_ok(&result, "Failed to list tables in 'public'")
            .expect_err("error status must propagate as Err even without a message");
        let message = err.to_string();
        assert!(message.contains("Failed to list tables in 'public'"));
        assert!(message.contains("no error message"));
    }

    #[test]
    fn ensure_query_ok_errors_when_error_message_set_without_error_status() {
        // Defense-in-depth: a populated `error` should fail closed even if a
        // provider forgot to flip `status` to `Error`. No current provider
        // does this (verified against every impl in
        // crates/kyomi-datasource/src/providers/), but a helper that only
        // trusted `status` would silently reintroduce KYO-126 the moment one
        // did.
        let result = QueryResult {
            status: QueryStatus::Success,
            error: Some("unexpected but populated".to_string()),
            ..QueryResult::success_empty()
        };
        assert!(ensure_query_ok(&result, "ctx").is_err());
    }

    #[test]
    fn ensure_query_ok_succeeds_on_success_status() {
        let result = QueryResult::success_empty();
        assert!(ensure_query_ok(&result, "Failed to list containers").is_ok());
    }

    #[test]
    fn ensure_query_ok_succeeds_on_genuinely_empty_result() {
        // The entire point of KYO-126: an accessible schema with zero tables
        // is a *successful* discovery, not an error. A `Success` result with
        // no record_batch/rows must not be treated as a failure.
        let result = QueryResult {
            status: QueryStatus::Success,
            record_batch: None,
            ..QueryResult::success_empty()
        };
        assert!(ensure_query_ok(&result, "Failed to list containers").is_ok());
    }

    // -----------------------------------------------------------------
    // discover_containers propagation (KYO-126)
    // -----------------------------------------------------------------

    /// Minimal stub provider whose `execute_query` returns a preset
    /// `QueryResult` regardless of the SQL it's given, so discovery methods
    /// can be exercised without a real database connection.
    struct StubProvider {
        result: QueryResult,
    }

    #[async_trait::async_trait]
    impl DatasourceProvider for StubProvider {
        async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
            Ok(true)
        }

        async fn execute_query(
            &self,
            _sql: &str,
            _limit: Option<u32>,
            _offset: Option<u32>,
            _include_total: bool,
            _job_id: Option<&str>,
        ) -> kyomi_connect_protocol::Result<QueryResult> {
            Ok(self.result.clone())
        }

        async fn close(&self) {}
    }

    fn executor_with_stub_result(result: QueryResult) -> CommandExecutor {
        CommandExecutor {
            provider: Box::new(StubProvider { result }),
            db_type: "postgres".to_string(),
        }
    }

    #[tokio::test]
    async fn discover_containers_propagates_query_level_failure_as_err() {
        // Simulates a Redshift/Postgres role without catalog read permission:
        // the provider returns `Ok(QueryResult { status: Error, .. })`, not a
        // Rust `Err`. Before the KYO-126 fix this silently produced `Ok(vec![])`.
        let executor =
            executor_with_stub_result(QueryResult::error("permission denied for relation"));

        let err = executor
            .discover_containers()
            .await
            .expect_err("query-level failure must propagate as Err, not an empty Ok");
        assert!(err.to_string().contains("permission denied for relation"));
    }

    #[tokio::test]
    async fn discover_containers_returns_empty_vec_for_genuinely_empty_schema_list() {
        // An accessible database with zero user schemas is a successful
        // discovery of zero containers -- not a failure.
        let executor = executor_with_stub_result(QueryResult::success_empty());

        let containers = executor
            .discover_containers()
            .await
            .expect("a successful, empty result must not be treated as an error");
        assert!(containers.is_empty());
    }

    // -----------------------------------------------------------------
    // discover_tables propagation (KYO-126)
    //
    // Same `ensure_query_ok` guard as discover_containers, exercised here to
    // remove doubt about wiring at this call site specifically (raised in
    // KYO-126 review): a role that can list schemas but is denied
    // information_schema.tables access must not silently become "0 tables".
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn discover_tables_propagates_query_level_failure_as_err() {
        // Simulates a role that can see the schema but is denied read access
        // to information_schema.tables: the provider returns
        // `Ok(QueryResult { status: Error, .. })`, not a Rust `Err`. Before
        // the KYO-126 fix this silently produced `Ok(vec![])`.
        let executor = executor_with_stub_result(QueryResult::error(
            "permission denied for relation information_schema.tables",
        ));

        let err = executor
            .discover_tables("public")
            .await
            .expect_err("query-level failure must propagate as Err, not an empty Ok");
        assert!(
            err.to_string()
                .contains("permission denied for relation information_schema.tables"),
            "expected provider message verbatim, got: {err}"
        );
    }

    // -----------------------------------------------------------------
    // handle_discover_catalog partial-tolerance (KYO-268)
    //
    // 66267bb (KYO-126) correctly made a per-container query failure
    // visible instead of silently masking it as "0 rows", but as a side
    // effect the bare `?` on discover_tables/discover_columns turned a
    // single container's failure into a total failure of the whole crawl.
    // These tests lock in the partial-tolerance behavior: one bad
    // container/table is recorded and skipped, everything else still comes
    // back, and total failure is still reported as `Err`.
    // -----------------------------------------------------------------

    /// Builds a two-column Arrow `RecordBatch` of UTF-8 strings, mirroring
    /// the `(name, type)` / `(name, type, description)` shapes returned by
    /// `information_schema.tables` / `information_schema.columns`.
    fn string_record_batch(columns: Vec<Vec<&str>>) -> arrow::record_batch::RecordBatch {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let fields: Vec<Field> = (0..columns.len())
            .map(|i| Field::new(format!("col{i}"), DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
            .into_iter()
            .map(|col| Arc::new(StringArray::from(col)) as Arc<dyn arrow::array::Array>)
            .collect();
        arrow::record_batch::RecordBatch::try_new(schema, arrays)
            .expect("fixed-size test columns must build a valid RecordBatch")
    }

    fn tables_result(rows: Vec<(&str, &str)>) -> QueryResult {
        if rows.is_empty() {
            return QueryResult::success_empty();
        }
        let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
        let types: Vec<&str> = rows.iter().map(|(_, t)| *t).collect();
        QueryResult {
            record_batch: Some(string_record_batch(vec![names, types])),
            ..QueryResult::success_empty()
        }
    }

    fn columns_result(rows: Vec<(&str, &str)>) -> QueryResult {
        let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
        let types: Vec<&str> = rows.iter().map(|(_, t)| *t).collect();
        let descs: Vec<&str> = rows.iter().map(|_| "").collect();
        QueryResult {
            record_batch: Some(string_record_batch(vec![names, types, descs])),
            ..QueryResult::success_empty()
        }
    }

    /// A `DatasourceProvider` whose response to `execute_query` depends on
    /// which discovery query it's asked -- containers, a specific
    /// container's tables, or a specific table's columns -- determined by
    /// pattern-matching the generated SQL text. Lets a single test exercise
    /// `handle_discover_catalog`'s full per-container/per-table loop against
    /// a scripted mix of successes and failures, the same way a real
    /// datasource would.
    struct ScriptedProvider {
        containers: QueryResult,
        tables: std::collections::HashMap<String, QueryResult>,
        columns: std::collections::HashMap<(String, String), QueryResult>,
    }

    #[async_trait::async_trait]
    impl DatasourceProvider for ScriptedProvider {
        async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
            Ok(true)
        }

        async fn execute_query(
            &self,
            sql: &str,
            _limit: Option<u32>,
            _offset: Option<u32>,
            _include_total: bool,
            _job_id: Option<&str>,
        ) -> kyomi_connect_protocol::Result<QueryResult> {
            if sql.contains("information_schema.schemata") {
                return Ok(self.containers.clone());
            }
            if sql.contains("information_schema.columns") {
                let hit = self.columns.iter().find(|((container, table), _)| {
                    sql.contains(&format!("table_schema = '{container}'"))
                        && sql.contains(&format!("table_name = '{table}'"))
                });
                let (_, result) =
                    hit.unwrap_or_else(|| panic!("no scripted columns result for sql: {sql}"));
                return Ok(result.clone());
            }
            if sql.contains("information_schema.tables") {
                let hit = self
                    .tables
                    .iter()
                    .find(|(container, _)| sql.contains(&format!("table_schema = '{container}'")));
                let (_, result) =
                    hit.unwrap_or_else(|| panic!("no scripted tables result for sql: {sql}"));
                return Ok(result.clone());
            }
            panic!("unexpected sql in ScriptedProvider: {sql}");
        }

        async fn close(&self) {}
    }

    /// Builds a `CommandExecutor` around a `ScriptedProvider`, for a Postgres
    /// datasource with the given containers list and per-container/per-table
    /// scripted results.
    fn executor_with_scripted_catalog(
        containers: Vec<&str>,
        tables: Vec<(&str, QueryResult)>,
        columns: Vec<((&str, &str), QueryResult)>,
    ) -> CommandExecutor {
        let containers_result = if containers.is_empty() {
            QueryResult::success_empty()
        } else {
            QueryResult {
                record_batch: Some(string_record_batch(vec![containers.clone()])),
                ..QueryResult::success_empty()
            }
        };
        CommandExecutor {
            provider: Box::new(ScriptedProvider {
                containers: containers_result,
                tables: tables
                    .into_iter()
                    .map(|(name, result)| (name.to_string(), result))
                    .collect(),
                columns: columns
                    .into_iter()
                    .map(|((container, table), result)| {
                        ((container.to_string(), table.to_string()), result)
                    })
                    .collect(),
            }),
            db_type: "postgres".to_string(),
        }
    }

    #[tokio::test]
    async fn multi_container_discovery_tolerates_one_containers_discover_tables_failure() {
        let executor = executor_with_scripted_catalog(
            vec!["public", "restricted", "analytics"],
            vec![
                ("public", tables_result(vec![("users", "BASE TABLE")])),
                (
                    "restricted",
                    QueryResult::error("permission denied for schema restricted"),
                ),
                ("analytics", tables_result(vec![])),
            ],
            vec![(
                ("public", "users"),
                columns_result(vec![("id", "int4"), ("email", "varchar")]),
            )],
        );

        let value = executor
            .handle_discover_catalog(None)
            .await
            .expect("one bad container must not fail the whole crawl");
        let result: CatalogResult = serde_json::from_value(value).unwrap();

        let names: Vec<&str> = result.containers.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["public", "analytics"]);
        assert_eq!(result.containers[0].tables.len(), 1);
        assert_eq!(result.containers[0].tables[0].name, "users");

        assert_eq!(result.errors.len(), 1);
        assert!(
            result.errors[0].contains("restricted"),
            "error must name the failed container, got: {:?}",
            result.errors
        );
    }

    #[tokio::test]
    async fn container_survives_one_tables_discover_columns_failure() {
        let executor = executor_with_scripted_catalog(
            vec!["public"],
            vec![(
                "public",
                tables_result(vec![("users", "BASE TABLE"), ("logs", "BASE TABLE")]),
            )],
            vec![
                (
                    ("public", "users"),
                    columns_result(vec![("id", "int4"), ("email", "varchar")]),
                ),
                (
                    ("public", "logs"),
                    QueryResult::error("permission denied for relation logs"),
                ),
            ],
        );

        let value = executor
            .handle_discover_catalog(None)
            .await
            .expect("one bad table must not drop the whole container");
        let result: CatalogResult = serde_json::from_value(value).unwrap();

        assert_eq!(result.containers.len(), 1);
        assert_eq!(result.containers[0].name, "public");
        let table_names: Vec<&str> = result.containers[0]
            .tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert_eq!(table_names, vec!["users"]);

        assert_eq!(result.errors.len(), 1);
        assert!(
            result.errors[0].contains("logs"),
            "error must name the failed table, got: {:?}",
            result.errors
        );
    }

    #[tokio::test]
    async fn every_container_failing_is_still_an_err() {
        let executor = executor_with_scripted_catalog(
            vec!["a", "b"],
            vec![
                ("a", QueryResult::error("permission denied for schema a")),
                ("b", QueryResult::error("permission denied for schema b")),
            ],
            vec![],
        );

        let err = executor
            .handle_discover_catalog(None)
            .await
            .expect_err("every container failing must not report an empty-but-successful result");
        let message = err.to_string();
        assert!(message.contains('a') && message.contains('b'));
    }

    #[tokio::test]
    async fn zero_containers_is_ok_empty_and_error_free() {
        let executor = executor_with_scripted_catalog(vec![], vec![], vec![]);

        let value = executor
            .handle_discover_catalog(None)
            .await
            .expect("a genuinely empty container list is success, not failure");
        let result: CatalogResult = serde_json::from_value(value).unwrap();

        assert!(result.containers.is_empty());
        assert!(result.errors.is_empty());
    }
}
