//! FlareDB datasource provider using Arrow Flight SQL.
//!
//! Implements query execution for FlareDB databases using the Arrow Flight SQL
//! protocol via gRPC. FlareDB is built on Apache DataFusion + Arrow + Parquet,
//! and exposes a Flight SQL endpoint for zero-copy Arrow data transfer.
//!
//! ## Connection Config
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `host` | string | `"localhost"` | FlareDB server hostname |
//! | `port` | int | `8815` | Flight SQL gRPC port |
//!
//! ## Credentials
//!
//! No auth for v1 (FlareDB doesn't have auth yet).

use std::time::Instant;

use arrow::compute::concat_batches;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures_util::TryStreamExt;
use serde_json::Value;
use tonic::transport::Channel;

use crate::arrow_builder::{batch_to_ipc_bytes, schema_to_ipc_bytes};
use crate::provider::{
    ColumnInfo, DatasourceProvider, DiscoveryResult, DryRunResult, QueryResult, QueryStatus,
};
use crate::type_mapping::map_arrow_type;

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: u16 = 8815;

/// FlareDB datasource provider using Arrow Flight SQL.
///
/// Wraps a tonic [`Channel`] (which is `Clone` and cheap to clone — it's an
/// `Arc` internally). A new [`FlightSqlServiceClient`] is created per call
/// because its methods take `&mut self`, which is incompatible with the
/// `&self` methods of [`DatasourceProvider`].
pub struct FlareDbProvider {
    channel: Channel,
}

impl FlareDbProvider {
    /// Create a new FlareDB provider from connection config and credentials.
    ///
    /// Connects to the FlareDB Flight SQL endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URI is invalid or the TCP connection
    /// cannot be established.
    pub async fn new(
        connection_config: &Value,
        _credentials: &Value, // No auth for v1
    ) -> kyomi_connect_protocol::Result<Self> {
        let host = connection_config
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_HOST);
        let port = connection_config
            .get("port")
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::from(DEFAULT_PORT)) as u16;

        let endpoint = format!("http://{host}:{port}");
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| {
                kyomi_connect_protocol::Error::Internal(format!(
                    "Invalid FlareDB endpoint '{endpoint}': {e}"
                ))
            })?
            .connect()
            .await
            .map_err(|e| {
                kyomi_connect_protocol::Error::Internal(format!(
                    "Failed to connect to FlareDB at {endpoint}: {e}"
                ))
            })?;

        tracing::info!(host, port, "Connected to FlareDB");

        Ok(Self { channel })
    }

    /// Create a new [`FlightSqlServiceClient`] bound to the shared channel.
    ///
    /// The channel is an `Arc`-wrapped connection pool, so cloning is cheap.
    fn client(&self) -> FlightSqlServiceClient<Channel> {
        FlightSqlServiceClient::new(self.channel.clone())
    }
}

#[async_trait::async_trait]
impl DatasourceProvider for FlareDbProvider {
    async fn test_connection(&self) -> kyomi_connect_protocol::Result<bool> {
        let mut client = self.client();

        let flight_info = tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_CONNECT,
            client.execute("SELECT 1".to_string(), None),
        )
        .await
        .map_err(|_| {
            kyomi_connect_protocol::Error::Internal("FlareDB test connection timed out".into())
        })?
        .map_err(|e| {
            kyomi_connect_protocol::Error::Internal(format!("FlareDB test connection failed: {e}"))
        })?;

        // Consume the first endpoint to confirm data is accessible.
        for endpoint in flight_info.endpoint {
            let Some(ticket) = endpoint.ticket else {
                continue;
            };
            tokio::time::timeout(crate::DATASOURCE_TIMEOUT_CONNECT, client.do_get(ticket))
                .await
                .map_err(|_| {
                    kyomi_connect_protocol::Error::Internal(
                        "FlareDB test connection do_get timed out".into(),
                    )
                })?
                .map_err(|e| {
                    kyomi_connect_protocol::Error::Internal(format!(
                        "FlareDB test connection do_get failed: {e}"
                    ))
                })?;
            break;
        }

        Ok(true)
    }

    async fn execute_query(
        &self,
        sql: &str,
        limit: Option<u32>,
        offset: Option<u32>,
        _include_total: bool,
        _job_id: Option<&str>,
    ) -> kyomi_connect_protocol::Result<QueryResult> {
        let start = Instant::now();

        // Build paginated SQL — strip trailing semicolons, then apply
        // LIMIT/OFFSET for SELECT/WITH queries that don't already have them.
        // Default to LIMIT 1000 OFFSET 0 to match the sqlx_common convention.
        let sql_stripped = sql.trim().trim_end_matches(';').trim();
        let sql_upper = sql_stripped.to_uppercase();
        let is_select = sql_upper.starts_with("SELECT") || sql_upper.starts_with("WITH");
        let already_has_limit = sql_upper.contains("LIMIT");

        let effective_limit = limit.unwrap_or(1000);
        let effective_offset = offset.unwrap_or(0);

        let paginated_sql = if is_select && !already_has_limit {
            format!("{sql_stripped} LIMIT {effective_limit} OFFSET {effective_offset}")
        } else {
            sql_stripped.to_string()
        };

        tracing::debug!(
            sql = %paginated_sql.chars().take(200).collect::<String>(),
            "Executing FlareDB query"
        );

        let mut client = self.client();

        let flight_info = match tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_QUERY,
            client.execute(paginated_sql.clone(), None),
        )
        .await
        {
            Ok(Ok(fi)) => fi,
            Ok(Err(e)) => {
                return Ok(QueryResult {
                    status: QueryStatus::Error,
                    columns: None,
                    total_rows: None,
                    has_more: false,
                    bytes_processed: None,
                    execution_time_ms: Some(start.elapsed().as_millis() as i64),
                    error: Some(format!("FlareDB execute failed: {e}")),
                    record_batch: None,
                    job_id: None,
                });
            }
            Err(_) => {
                return Ok(QueryResult {
                    status: QueryStatus::Error,
                    columns: None,
                    total_rows: None,
                    has_more: false,
                    bytes_processed: None,
                    execution_time_ms: Some(start.elapsed().as_millis() as i64),
                    error: Some(format!(
                        "FlareDB query timed out after {}s",
                        crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                    )),
                    record_batch: None,
                    job_id: None,
                });
            }
        };

        // Collect all RecordBatches from all Flight endpoints.
        let mut all_batches = Vec::new();

        for endpoint in flight_info.endpoint {
            let Some(ticket) = endpoint.ticket else {
                continue;
            };

            let stream =
                match tokio::time::timeout(crate::DATASOURCE_TIMEOUT_QUERY, client.do_get(ticket))
                    .await
                {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        return Ok(QueryResult {
                            status: QueryStatus::Error,
                            columns: None,
                            total_rows: None,
                            has_more: false,
                            bytes_processed: None,
                            execution_time_ms: Some(start.elapsed().as_millis() as i64),
                            error: Some(format!("FlareDB do_get failed: {e}")),
                            record_batch: None,
                            job_id: None,
                        });
                    }
                    Err(_) => {
                        return Ok(QueryResult {
                            status: QueryStatus::Error,
                            columns: None,
                            total_rows: None,
                            has_more: false,
                            bytes_processed: None,
                            execution_time_ms: Some(start.elapsed().as_millis() as i64),
                            error: Some(format!(
                                "FlareDB do_get timed out after {}s",
                                crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                            )),
                            record_batch: None,
                            job_id: None,
                        });
                    }
                };

            let batches: Vec<arrow::record_batch::RecordBatch> =
                match stream.try_collect::<Vec<_>>().await {
                    Ok(b) => b,
                    Err(e) => {
                        return Ok(QueryResult {
                            status: QueryStatus::Error,
                            columns: None,
                            total_rows: None,
                            has_more: false,
                            bytes_processed: None,
                            execution_time_ms: Some(start.elapsed().as_millis() as i64),
                            error: Some(format!("FlareDB stream collect failed: {e}")),
                            record_batch: None,
                            job_id: None,
                        });
                    }
                };

            all_batches.extend(batches);
        }

        if all_batches.is_empty() {
            return Ok(QueryResult::success_empty());
        }

        // Build column metadata from the first batch's schema.
        let first_schema = all_batches[0].schema();
        let columns: Vec<ColumnInfo> = first_schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                col_type: map_arrow_type(f.data_type()),
            })
            .collect();

        let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        let has_more = total_rows == effective_limit as usize;

        // Concatenate all batches into one using the first batch's schema.
        let record_batch = match concat_batches(&first_schema, &all_batches) {
            Ok(b) => b,
            Err(e) => {
                return Ok(QueryResult {
                    status: QueryStatus::Error,
                    columns: None,
                    total_rows: None,
                    has_more: false,
                    bytes_processed: None,
                    execution_time_ms: Some(start.elapsed().as_millis() as i64),
                    error: Some(format!("FlareDB batch concatenation failed: {e}")),
                    record_batch: None,
                    job_id: None,
                });
            }
        };

        let execution_time_ms = start.elapsed().as_millis() as i64;

        Ok(QueryResult {
            status: QueryStatus::Success,
            columns: Some(columns),
            total_rows: None,
            has_more,
            bytes_processed: None,
            execution_time_ms: Some(execution_time_ms),
            error: None,
            record_batch: Some(record_batch),
            job_id: None,
        })
    }

    // Flight SQL streams the full result natively; SQL-level pagination is not
    // applied to the stream path (unlike execute_query). Batch boundaries are
    // determined by the Flight endpoint, not by chunk_size. FlareDB does not
    // expose row counts in the Flight protocol, so include_total is unused.
    async fn execute_query_stream_arrow(
        &self,
        sql: &str,
        _limit: Option<u32>,
        _offset: Option<u32>,
        _include_total: bool,
        _chunk_size: Option<u32>,
    ) -> kyomi_connect_protocol::Result<kyomi_connect_protocol::ArrowStream> {
        use kyomi_connect_protocol::ArrowStreamEvent;

        let start = Instant::now();

        tracing::debug!(
            sql = %sql.chars().take(200).collect::<String>(),
            "FlareDB: starting Arrow stream"
        );

        // Clone the channel so the spawned task owns it.
        let channel = self.channel.clone();
        let sql = sql.to_string();

        let (tx, stream) = crate::stream::make_arrow_stream_channel();

        tokio::spawn(async move {
            let mut client = FlightSqlServiceClient::new(channel);

            let flight_info = match tokio::time::timeout(
                crate::DATASOURCE_TIMEOUT_QUERY,
                client.execute(sql, None),
            )
            .await
            {
                Ok(Ok(fi)) => fi,
                Ok(Err(e)) => {
                    let _ = tx
                        .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                            "FlareDB execute failed: {e}"
                        ))))
                        .await;
                    return;
                }
                Err(_) => {
                    let _ = tx
                        .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                            "FlareDB query timed out after {}s",
                            crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                        ))))
                        .await;
                    return;
                }
            };

            let mut schema_sent = false;
            let mut chunk_index: u32 = 0;
            let mut total_rows_returned: u64 = 0;

            for endpoint in flight_info.endpoint {
                let Some(ticket) = endpoint.ticket else {
                    continue;
                };

                let mut batch_stream = match tokio::time::timeout(
                    crate::DATASOURCE_TIMEOUT_QUERY,
                    client.do_get(ticket),
                )
                .await
                {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        let _ = tx
                            .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                "FlareDB do_get failed: {e}"
                            ))))
                            .await;
                        return;
                    }
                    Err(_) => {
                        let _ = tx
                            .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                "FlareDB do_get timed out after {}s",
                                crate::DATASOURCE_TIMEOUT_QUERY.as_secs()
                            ))))
                            .await;
                        return;
                    }
                };

                // Drive the batch stream, sending Schema on first batch and
                // Batch events for each subsequent RecordBatch.
                loop {
                    let next = futures_util::StreamExt::next(&mut batch_stream).await;
                    let batch = match next {
                        None => break,
                        Some(Ok(b)) => b,
                        Some(Err(e)) => {
                            let _ = tx
                                .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                    "FlareDB stream error: {e}"
                                ))))
                                .await;
                            return;
                        }
                    };

                    // Send Schema event on the very first batch.
                    if !schema_sent {
                        let schema = batch.schema();
                        let columns: Vec<ColumnInfo> = schema
                            .fields()
                            .iter()
                            .map(|f| ColumnInfo {
                                name: f.name().clone(),
                                col_type: map_arrow_type(f.data_type()),
                            })
                            .collect();

                        let schema_ipc = match schema_to_ipc_bytes(&schema) {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                let _ = tx
                                    .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                        "Arrow schema serialization error: {e}"
                                    ))))
                                    .await;
                                return;
                            }
                        };

                        if tx
                            .send(Ok(ArrowStreamEvent::Schema {
                                schema_ipc,
                                columns,
                                total_rows: None,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        schema_sent = true;
                    }

                    // Serialize the batch to IPC and send a Batch event.
                    let rows_in_batch = batch.num_rows() as u64;
                    let ipc_bytes = match batch_to_ipc_bytes(&batch) {
                        Ok(b) => b,
                        Err(e) => {
                            let _ = tx
                                .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                    "Arrow IPC serialization error: {e}"
                                ))))
                                .await;
                            return;
                        }
                    };

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
                    total_rows_returned += rows_in_batch;
                }
            }

            // If no batches were received (empty result), send an empty Schema.
            if !schema_sent {
                let empty_schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
                let schema_ipc = match schema_to_ipc_bytes(&empty_schema) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        let _ = tx
                            .send(Err(kyomi_connect_protocol::Error::Internal(format!(
                                "Arrow schema serialization error: {e}"
                            ))))
                            .await;
                        return;
                    }
                };
                let _ = tx
                    .send(Ok(ArrowStreamEvent::Schema {
                        schema_ipc,
                        columns: Vec::new(),
                        total_rows: None,
                    }))
                    .await;
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

        Ok(stream)
    }

    async fn dry_run(&self, sql: &str) -> kyomi_connect_protocol::Result<DryRunResult> {
        let explain_sql = format!("EXPLAIN {sql}");
        let mut client = self.client();

        match tokio::time::timeout(
            crate::DATASOURCE_TIMEOUT_DRY_RUN,
            client.execute(explain_sql, None),
        )
        .await
        {
            Ok(Ok(_)) => Ok(DryRunResult::success("Query valid")),
            Ok(Err(e)) => Ok(DryRunResult::failure(
                format!("FlareDB dry run failed: {e}"),
                None,
                None,
            )),
            Err(_) => Ok(DryRunResult::failure(
                format!(
                    "FlareDB dry run timed out after {}s",
                    crate::DATASOURCE_TIMEOUT_DRY_RUN.as_secs()
                ),
                None,
                None,
            )),
        }
    }

    async fn list_databases(&self) -> DiscoveryResult {
        // FlareDB is single-database.
        DiscoveryResult {
            items: vec!["default".to_string()],
            error: None,
        }
    }

    async fn list_schemas(&self) -> DiscoveryResult {
        let result = self
            .execute_query(
                "SELECT DISTINCT table_schema \
                 FROM information_schema.tables \
                 ORDER BY table_schema",
                None,
                None,
                false,
                None,
            )
            .await;

        match result {
            Ok(qr) => {
                let items =
                    crate::provider::extract_string_col_from_batch(qr.record_batch.as_ref(), 0);
                DiscoveryResult { items, error: None }
            }
            Err(e) => DiscoveryResult {
                items: vec![],
                error: Some(format!("Failed to list FlareDB schemas: {e}")),
            },
        }
    }

    async fn close(&self) {
        // Channel drops automatically when the provider is dropped.
        tracing::debug!("FlareDB provider closed");
    }
}
