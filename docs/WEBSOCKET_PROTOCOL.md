# WebSocket Protocol v2

LiveTable's optional Actix server exposes a JSON-over-WebSocket protocol for
editing a base table and subscribing to connection-local, server-computed view
pipelines.

The wire version is `2` (`impl/src/messages.rs::PROTOCOL_VERSION`). The bundled
React client expects the same version.

## Running the server

From the repository root:

```bash
cd impl
cargo run --bin livetable-server --features server
```

Defaults:

- WebSocket: `ws://127.0.0.1:8080/ws`
- Health check: `http://127.0.0.1:8080/health`
- `HOST` and `PORT` override the bind address.

The server currently seeds one in-memory table named `demo`. State is not
persisted across restarts. CORS is permissive because this server is intended
for local development and demos.

## Message envelope

Every message is a JSON object with a string `type`. Clients send text frames;
binary frames are ignored. Errors use:

```json
{"type":"Error","message":"..."}
```

## Base-table protocol

### Subscribe

```json
{"type":"Subscribe","table_name":"demo"}
```

The server registers the connection for base-table mutation broadcasts and
replies:

```json
{"type":"Subscribed","table_name":"demo","protocol_version":2}
```

`Subscribe` does not include a snapshot. Send `Query` as well when using the
flat-table protocol. The bundled flat-table hook sends both on connection.

### Query

```json
{"type":"Query","table_name":"demo"}
```

Response:

```json
{
  "type": "TableData",
  "table_name": "demo",
  "seq": 2,
  "columns": ["region", "product", "amount"],
  "rows": [
    {"row_id": 1, "row": {"region": "West", "product": "Widget", "amount": 100.5}}
  ]
}
```

`row_id` is server-assigned stable identity. It is independent of the row's
current array index and should be used for updates and deletes.

### Insert a row

```json
{
  "type": "InsertRow",
  "table_name": "demo",
  "row": {"region": "West", "product": "Premium", "amount": 700.0}
}
```

The server validates the row against the table schema, assigns a stable wire
`row_id` (which is not a table column), and broadcasts:

```json
{
  "type": "RowInserted",
  "table_name": "demo",
  "seq": 3,
  "index": 2,
  "row_id": 3,
  "row": {"region": "West", "product": "Premium", "amount": 700.0}
}
```

### Update a cell

```json
{
  "type": "UpdateCell",
  "table_name": "demo",
  "row_id": 3,
  "column": "amount",
  "value": 450.0
}
```

Successful update broadcast:

```json
{
  "type": "CellUpdated",
  "table_name": "demo",
  "seq": 4,
  "row_id": 3,
  "column": "amount",
  "value": 450.0
}
```

### Delete a row

```json
{"type":"DeleteRow","table_name":"demo","row_id":3}
```

Successful delete broadcast:

```json
{"type":"RowDeleted","table_name":"demo","seq":5,"row_id":3}
```

## Server-computed pipelines

`SetPipeline` defines or replaces one connection's view DAG over a base table.
Pipelines are connection-local: two clients may use different definitions over
the same table without affecting each other.

Nodes must be in topological order. Each `source_id` must be `base` or the ID of
an earlier node.

```json
{
  "type": "SetPipeline",
  "table_name": "demo",
  "pipeline_generation": 7,
  "nodes": [
    {
      "id": "filtered",
      "source_id": "base",
      "kind": "filter",
      "predicate": "amount >= 500"
    },
    {
      "id": "ranked",
      "source_id": "filtered",
      "kind": "sort",
      "keys": [{"column": "amount", "descending": true}]
    },
    {
      "id": "totals",
      "source_id": "ranked",
      "kind": "group",
      "group_by": ["region"],
      "aggs": [
        {"alias": "total", "op": "sum", "column": "amount"},
        {"alias": "p95", "op": "p95", "column": "amount"}
      ]
    }
  ]
}
```

Supported node kinds:

- `filter`: one non-empty expression in the same syntax as `filter_expr`.
- `sort`: one or more `{column, descending}` keys.
- `group`: one or more `group_by` columns and aggregate specs.

Aggregate operations are case-insensitive and accept `sum`, `count`, `avg`,
`average`, `mean`, `min`, `max`, `median`, `pNN`, and `percentile(x)` where
`x` is in `0.0..=1.0`. `count` means `COUNT(column)`, so `column` is always
required.

### View snapshots

The engine can maintain `base -> filter -> group` incrementally. Internal
filter changesets are separate from the wire protocol: `ViewData` still carries
full snapshots, and `seq` retains its existing generation/node-scoped meaning.
Sort nodes do not emit changesets, so groups below a sort still rebuild.

The server immediately sends a snapshot for the synthetic `base` node and each
successfully built node. It sends new full snapshots for nodes whose version
changes after a base mutation:

```json
{
  "type": "ViewData",
  "table_name": "demo",
  "pipeline_generation": 7,
  "node_id": "totals",
  "source_id": "ranked",
  "kind": "group",
  "seq": 9,
  "columns": ["region", "total", "p95"],
  "rows": [
    {"row_id": null, "row": {"region": "West", "total": 700.0, "p95": 700.0}}
  ]
}
```

Only the synthetic `base` snapshot has stable `row_id` values. Derived rows use
`null`, including filtered and sorted rows, because the current protocol does
not expose derived-row identity.

Pipeline errors are scoped to a generation and node:

```json
{
  "type": "ViewError",
  "table_name": "demo",
  "pipeline_generation": 7,
  "node_id": "filtered",
  "message": "..."
}
```

Structural validation is atomic: an invalid DAG leaves the previous pipeline
installed. If a real view fails while building, the valid prefix is installed
and the first failing node receives `ViewError`.

## Reconciliation rules

Base-table `seq` is the table's monotonic change count:

It need not start at zero: compacted or seeded mutations remain part of the
absolute count.

1. Treat `TableData.seq` as the baseline.
2. Drop deltas with `seq <= baseline` because the snapshot already contains
   them.
3. Apply post-snapshot deltas only in contiguous order.
4. Re-query when a sequence gap persists.

Pipeline responses require two keys:

1. Discard `ViewData` and `ViewError` whose `pipeline_generation` is not the
   client's current generation.
2. Compare `seq` only within the same generation and node. Rebuilding a
   pipeline resets view-local version counters.

The bundled hooks implement these rules and reconnect with exponential backoff.
Pipeline expression changes are debounced by 250 ms. The current `usePipeline`
hook keeps the last snapshot for a node until current-generation data for that
same node ID arrives. Callers that remove or rename node IDs should clear or
filter the snapshot map; the bundled cascade demo uses stable IDs.

## Resource limits

The server validates a complete definition before allocating views:

| Item | Limit |
|------|-------|
| Nodes per pipeline | 32 |
| Filter expression | 4096 bytes |
| Node ID | 64 bytes |
| Column/alias field | 128 bytes |
| Sort keys per node | 16 |
| Group keys per node | 16 |
| Aggregates per node | 32 |

Node IDs may contain ASCII letters, digits, `_`, and `-`; `base` is reserved.
Duplicate IDs, forward references, cycles, duplicate keys, and empty required
fields are rejected.

## Verification

```bash
cd impl
cargo test --features server --test protocol_v2_websocket
cargo test --lib --features server websocket

cd ../frontend
npm run test
```

The first command performs a real TCP/WebSocket handshake against an ephemeral
Actix server and verifies subscribe, pipeline snapshots, insert, update, and
delete propagation.
