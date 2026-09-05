# WebSocket Protocol v3

LiveTable's optional Actix server exposes a JSON-over-WebSocket protocol for
editing a base table and subscribing to connection-local, server-computed view
pipelines.

The wire version is `3` (`impl/src/messages.rs::PROTOCOL_VERSION`). The bundled
React client expects the same version. Upgrade server and client together:
pipeline delivery sequences have changed meaning since v2, and clients must
handle `ViewDelta`, `PipelineStatus`, and `QueryView`. Flat-table message shapes
and change-count sequencing are unchanged.

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

By default the server seeds one in-memory table named `demo`. Add `-- --lab`
to also enable synthetic `lab` data and the
[Orders Lab control extension](ORDERS_LAB.md#demo-control-extension):
`LabCommand`, `LabComplete`, and `LabError`. These additive v3 messages do not
change existing sequencing contracts. The `--lab` CLI requires a loopback host.
State is not
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
{"type":"Subscribed","table_name":"demo","protocol_version":3}
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

`pipeline_generation` is a client-selected `u32`. It must strictly increase
when replacing an installed pipeline on the same connection/table; reuse or
regression is rejected without replacing the current pipeline. A new connection
has no previous generation.

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

The server immediately sends a snapshot for the synthetic `base` node and each
successfully built node, each at delivery sequence zero. After mutations,
base/filter/sort nodes send bounded deltas when history is available. Empty
filter/sort output sends no node payload and does not advance its delivery
sequence. Groups retain full snapshots when their inherited version changes;
they may still send a snapshot after an excluded edit.

Missing history, view rebuilds, and oversized deltas produce a new snapshot.
Filters/sorts below a group may rebuild and therefore also use snapshots. See
[the internal sorted pipeline contract](INCREMENTAL_SORTED_PIPELINE.md).

```json
{
  "type": "ViewData",
  "table_name": "demo",
  "pipeline_generation": 7,
  "node_id": "totals",
  "source_id": "ranked",
  "kind": "group",
  "seq": 0,
  "columns": ["region", "total", "p95"],
  "rows": [
    {"row_id": null, "row": {"region": "West", "total": 700.0, "p95": 700.0}}
  ]
}
```

Only the synthetic `base` snapshot has stable `row_id` values. Derived rows use
`null`, including filtered and sorted rows, because the current protocol does
not expose derived-row identity.

### Ordered node deltas

```json
{
  "type": "ViewDelta",
  "table_name": "demo",
  "pipeline_generation": 7,
  "node_id": "ranked",
  "from_seq": 0,
  "seq": 1,
  "changes": [
    {"type": "RowDeleted", "index": 2},
    {"type": "RowInserted", "index": 0,
     "row": {"row_id": null, "row": {"region": "West", "product": "Premium", "amount": 900.0}}},
    {"type": "CellUpdated", "index": 1, "column": "product", "value": "Updated"}
  ]
}
```

Apply the entire batch atomically in listed order, only to a baseline with
exactly `seq == from_seq`. Every index is in this node's coordinates **at that
step**. Insertion allows `index == rows.length`; deletion/update require an
existing row. Inserts include the complete row; updates must name an existing
column. A sorted row move is a delete followed by an insert, not a stable-ID
move event. NULL cell values serialize as JSON `null`.

Base insertions carry stable base IDs. Derived insertions carry `row_id: null`.
No derived identity is needed for this ordered, baseline-checked replay; editing
and deleting still require a real base row ID. A delta contains 1–512 operations.

### Node resynchronization

```json
{"type":"QueryView","table_name":"demo","pipeline_generation":7,"node_id":"ranked"}
```

Returns `ViewData` for that installed node, without rebuilding the pipeline.
Only the requesting connection's node sequence advances, even if no rows have
changed. This distinguishes the response from duplicates and older in-flight
snapshots. A missing node, wrong connection, or stale generation returns
`ViewError`; it cannot reset another pipeline's delivery state.

The server sends inexpensive checkpoints approximately once per second:

```json
{"type":"PipelineStatus","table_name":"demo","pipeline_generation":7,
 "sequences":{"base":12,"filtered":5,"ranked":6,"totals":12}}
```

These are the server's emitted delivery sequences, not acknowledgements of
client receipt. Request a snapshot for any installed node whose watermark is
ahead of the local baseline, or whose initial snapshot is missing. Checkpoints
detect a dropped **final** delivery even when no later mutation reveals a gap.
They contain no rows and do not advance any sequence.

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

Pipeline sequencing is independent of that flat-table protocol:

1. Scope all state to connection, table, generation, and node. Ignore wrong
   tables, old generations, removed/unknown nodes, and obsolete socket callbacks.
2. Initial snapshots start at zero. Every emitted delta or subsequent snapshot
   advances that node's delivery sequence by one. It is **not** a view version
   or changeset cursor, and is not comparable with another node or connection.
3. Ignore duplicates/older messages (`seq <= applied`). Accept a newer snapshot
   as a replacement baseline, even across gaps.
4. Apply a delta only on exactly `from_seq`; `seq` must equal `from_seq + 1`.
   Reject the whole batch on a bad index, column, or row shape. A mismatch,
   malformed delivery, or missing baseline must never partially mutate rows.
   Snapshots must match their declared columns and identity rules; duplicate
   base IDs are rejected. Syntactically malformed frames discarded by the
   parser are detected as missing deliveries by the next checkpoint.
5. On a gap or invalid operation, send `QueryView`. The bundled hook keeps the
   last coherent display, ignores further deltas for that node while repairing,
   and waits for a newer snapshot. It does not accumulate a delta queue.
6. Use checkpoints to detect missing initial/final deliveries. The hook retries
   outstanding repairs at 3-second checks, at most once per node per check, and
   clears them on a new snapshot, pipeline generation, disconnect, or unmount.

The hooks reconnect with exponential backoff. Pipeline expression changes are
debounced by 250 ms. Sending a new definition clears old node baselines and
repair state; reconnect sends a new generation and obtains fresh snapshots.
Atomicity is per node/batch, not a simultaneous multi-node browser transaction.

### Remaining limits

Groups still use snapshots; stable derived-row identity is not implemented.
Client delta application shallow-copies the row array, so it still has O(N)
reference-copy work, and sorted index maintenance can remain O(N). Full initial
and recovery snapshots are not chunked. Checkpoints support eventual recovery
after transient delivery loss, not durable replay or an acknowledgement log.
See [delivery measurements](PIPELINE_DELIVERY.md) for the measured boundary.

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
| Operations per delta / retained pending base journal | 512 |
| Entries per checkpoint / outstanding client node repairs | 33 (including base) |

Node IDs may contain ASCII letters, digits, `_`, and `-`; `base` is reserved.
Duplicate IDs, forward references, cycles, duplicate keys, and empty required
fields are rejected.

## Verification

```bash
cd impl
cargo test --features server --test protocol_v3_websocket
cargo test --lib --features server websocket
cargo test --lib --features server engine

cd ../frontend
npm run test
```

The first command performs a real TCP/WebSocket handshake against an ephemeral
Actix server and verifies subscribe, snapshots/deltas, insert/update/delete,
checkpoint-based repair of a deliberately dropped final delivery, replacement
generations, and independent connections. Engine tests compare reconstructed
clients with fresh pipelines across randomized mixed batches; frontend tests
cover duplicates, gaps, malformed batches, retries, stale sockets, and rendering.
