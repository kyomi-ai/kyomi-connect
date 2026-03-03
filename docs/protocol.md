# Wire Protocol

This document specifies the wire protocol used between Kyomi Cloud and the Kyomi Connect agent. All communication happens over a WebSocket connection using JSON-encoded messages.

## Message Flow

The protocol follows a request-response pattern:

1. **Kyomi Cloud** sends a `ConnectRequest` to the agent.
2. **Connect agent** processes the request and sends back one or more `ConnectResponse` messages.
3. Each response includes the request `id` for correlation.

For non-streaming operations, there is exactly one response per request. For streaming queries, the response is a sequence of messages: one `stream_header`, zero or more `stream_chunk` messages, and one `stream_complete`.

## Operations

The `ConnectOp` enum defines the four operations the backend can request:

| Operation | Wire Value | Parameters | Description |
|-----------|-----------|------------|-------------|
| ExecuteQuery | `"execute_query"` | `QueryParams` | Execute a SQL query and return results |
| DryRun | `"dry_run"` | `DryRunParams` | Validate SQL syntax without executing |
| TestConnection | `"test_connection"` | None | Verify the database connection is working |
| DiscoverCatalog | `"discover_catalog"` | None | Discover the database schema (containers, tables, columns) |

## Request Format

### ConnectRequest

Every request from Kyomi Cloud follows this structure:

```json
{
  "id": "req-abc123",
  "op": "execute_query",
  "params": { ... },
  "streaming": false
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | yes | Unique request identifier for correlating responses |
| `op` | string | yes | One of: `"execute_query"`, `"dry_run"`, `"test_connection"`, `"discover_catalog"` |
| `params` | object | no | Operation-specific parameters. Omitted for `test_connection` and `discover_catalog`. |
| `streaming` | boolean | no | When `true`, the response will be streamed as multiple messages. Defaults to `false`. |

### QueryParams

Parameters for `execute_query`:

```json
{
  "sql": "SELECT id, name FROM users LIMIT 10",
  "limit": 100,
  "offset": 0,
  "include_total": true
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sql` | string | yes | SQL query to execute |
| `limit` | integer | no | Maximum rows to return (page size). Omitted for no limit. |
| `offset` | integer | no | Number of rows to skip (for pagination). Omitted for no offset. |
| `include_total` | boolean | yes | Whether to include a total row count (may be slow on large tables) |

### DryRunParams

Parameters for `dry_run`:

```json
{
  "sql": "SELECT * FROM users WHERE active = true"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sql` | string | yes | SQL query to validate |

## Response Format

### ConnectResponse

Every response from the Connect agent includes the request `id` and a `type` discriminator:

```json
{
  "id": "req-abc123",
  "type": "result",
  ...
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | yes | Matches the originating request's `id` |
| `type` | string | yes | One of: `"result"`, `"error"`, `"stream_header"`, `"stream_chunk"`, `"stream_complete"` |

The remaining fields depend on the `type` value.

### Success Response (type: "result")

```json
{
  "id": "req-abc123",
  "type": "result",
  "result": { ... }
}
```

The `result` field contains a JSON value whose structure depends on the original operation:

**For `execute_query`:**

```json
{
  "status": "success",
  "columns": [
    {"name": "id", "type": "number"},
    {"name": "name", "type": "string"}
  ],
  "rows": [[1, "Alice"], [2, "Bob"]],
  "total_rows": 200,
  "has_more": true,
  "bytes_processed": null,
  "execution_time_ms": 42,
  "error": null
}
```

**For `dry_run`:**

```json
{
  "valid": true,
  "message": "Query validated successfully",
  "line": null,
  "column": null
}
```

Or on failure:

```json
{
  "valid": false,
  "message": "Syntax error near 'FORM'",
  "line": 1,
  "column": 10
}
```

**For `test_connection`:**

```json
true
```

**For `discover_catalog`:**

See [Catalog Result](#catalog-result) below.

### Error Response (type: "error")

```json
{
  "id": "req-abc123",
  "type": "error",
  "error": "Connection refused: port 5432"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `error` | string | Human-readable error message |

## Streaming Responses

When a request has `"streaming": true`, the agent responds with a sequence of messages instead of a single result.

### Stream Header (type: "stream_header")

Sent first. Contains column metadata and an optional row count estimate.

```json
{
  "id": "req-abc123",
  "type": "stream_header",
  "columns": [
    {"name": "id", "type": "number"},
    {"name": "name", "type": "string"}
  ],
  "total_rows": 1000
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `columns` | array | yes | Column definitions (see [ColumnInfo](#columninfo)) |
| `total_rows` | integer | no | Estimated total row count. Omitted when unknown. |

### Stream Chunk (type: "stream_chunk")

Sent one or more times. Each chunk contains a batch of rows.

```json
{
  "id": "req-abc123",
  "type": "stream_chunk",
  "rows": [[1, "Alice"], [2, "Bob"]],
  "chunk_index": 0
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `rows` | array | yes | Row data. Each row is an array of JSON values matching column order. |
| `chunk_index` | integer | yes | Zero-based chunk index for ordering verification |

### Stream Complete (type: "stream_complete")

Sent last. Signals end of stream with summary statistics.

```json
{
  "id": "req-abc123",
  "type": "stream_complete",
  "execution_time_ms": 456,
  "bytes_processed": 10000000,
  "total_chunks": 5,
  "total_rows_returned": 5000
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `execution_time_ms` | integer | no | Wall-clock execution time in milliseconds |
| `bytes_processed` | integer | no | Bytes processed by the query engine |
| `total_chunks` | integer | yes | Number of chunks sent |
| `total_rows_returned` | integer | yes | Total rows across all chunks |

## Catalog Result

The `discover_catalog` operation returns the database schema as a hierarchy of containers, tables, and columns.

```json
{
  "containers": [
    {
      "name": "public",
      "tables": [
        {
          "name": "users",
          "native_type": "BASE TABLE",
          "columns": [
            {
              "name": "id",
              "native_type": "int4",
              "description": "Primary key"
            },
            {
              "name": "email",
              "native_type": "varchar(255)"
            }
          ]
        }
      ]
    }
  ]
}
```

### CatalogResult

| Field | Type | Description |
|-------|------|-------------|
| `containers` | array | Top-level containers (schemas, datasets, databases) |

### CatalogContainer

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Container name (e.g., `"public"`, `"my_dataset"`) |
| `tables` | array | Tables within this container |

### CatalogTable

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Table name |
| `native_type` | string | no | Native table type (e.g., `"BASE TABLE"`, `"VIEW"`). Omitted if unknown. |
| `columns` | array | yes | Columns in this table |

### CatalogColumn

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Column name |
| `native_type` | string | yes | Native database type (e.g., `"int4"`, `"varchar(255)"`, `"TIMESTAMP"`) |
| `description` | string | no | Column description from database comments. Omitted if not available. |

## ColumnInfo

Column metadata returned with query results and stream headers.

```json
{"name": "id", "type": "number"}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Column name |
| `type` | string | Mapped column type (see [SimpleType](#simpletype)) |

## SimpleType

The type system maps native database types to a simplified set of types. Each provider is responsible for mapping its native types to one of these values.

| Value | Description |
|-------|-------------|
| `"string"` | Text / character data |
| `"number"` | Integer or floating-point numeric data |
| `"boolean"` | True / false |
| `"date"` | Calendar date without time component |
| `"time"` | Time of day without date component |
| `"timestamp"` | Date + time without timezone |
| `"timestamptz"` | Date + time with timezone |
| `"unknown"` | Type could not be mapped |

## Error Types

The `Error` enum in `kyomi-connect-protocol` defines the error categories:

| Variant | Description | Example |
|---------|-------------|---------|
| `Provider` | A provider-level error (query execution failure, permission denied) | `"relation \"users\" does not exist"` |
| `Connection` | Connection to the datasource failed | `"connection failed: Connection refused (os error 111)"` |
| `NotSupported` | The requested operation is not supported by this provider | `"not supported: Dry run not available for ClickHouse"` |
| `Internal` | An internal error that does not fit other categories | `"failed to parse response"` |
| `SerdeJson` | JSON serialization/deserialization error | `"expected value at line 1 column 1"` |

On the wire, errors are transmitted as the `"error"` response type with a human-readable string message.

## QueryStreamEvent (Internal)

Within the agent codebase, the `QueryStreamEvent` enum is the internal streaming currency used by providers. It mirrors the wire streaming types:

| Variant | Wire Equivalent |
|---------|----------------|
| `Header { columns, total_rows }` | `stream_header` |
| `Chunk { rows, chunk_index }` | `stream_chunk` |
| `Complete { execution_time_ms, bytes_processed, total_chunks, total_rows_returned }` | `stream_complete` |

The executor converts `QueryStreamEvent` values into `ConnectResponse` messages for transmission over the WebSocket. The sequence is always: `Header` -> `Chunk`* -> `Complete`.
