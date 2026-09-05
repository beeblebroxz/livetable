# Server-Computed View Pipeline — Design

**Date:** 2026-06-28
**Status:** Implemented 2026-08-02
**Goal:** Make the "Forward Propagation" browser demo run on LiveTable's real
Rust engine instead of a client-side JavaScript reimplementation.

This is an archived design, including its original constraints and alternatives.
Use the [protocol reference](../../WEBSOCKET_PROTOCOL.md) for current message
shapes, generation/sequence rules, and limits. Subsequent bounded mixed-batch
[filter](../../INCREMENTAL_FILTER_PIPELINE.md) and
[sorted](../../INCREMENTAL_SORTED_PIPELINE.md) propagation supersede the
single-change-only verification note below. Full pipeline snapshots remain the
wire format; pipeline deltas are planned separately.

## Original problem

`frontend/src/pages/CascadeDemo.tsx` advertised "live base rows flow through
editable derived tables on every tick," but reimplemented filter / sort /
group-by in ~340 lines of TypeScript. None of the Rust engine's incremental
forward propagation ran, and the WebSocket protocol could carry only a single
flat table.

Protocol v2 now resolves this: `TableEngineActor` owns the real per-connection
view DAG, `ViewData` streams each node, and `usePipeline` drives the cascade
demo with generation-aware server snapshots. The local evaluator was removed.

The core engine's incremental propagation is now verified correct (differential
fuzz over filter / sort / group / join / computed, single-change and batched;
three `AggregateView` bugs fixed). This design exposes that engine over the
wire so the demo becomes truthful.

## Original approved decisions (from brainstorming)

1. **Faithful incremental** — the server holds real Rust views and propagates
   via `TickableTable::tick()` on every mutation (single change per tick — the
   path the differential fuzz verifies).
2. **Per-node snapshot wire format** — after each tick, every changed node
   streams a full `ViewData{ node_id, seq, columns, rows }`. Simplest and
   robust (sidesteps view-row identity for sort/group); indistinguishable from
   deltas at demo scale. Engine stays incremental internally.
3. **Insert + edit + delete** interactions — so a viewer can watch a row cross
   the `amount >= 500` filter boundary on edit and a group's running SUM shrink
   on delete.
4. **Real engine syntax** — the editable boxes use the actual API surface:
   `filter_expr` SQL (`amount >= 500 AND region != 'West'`), sort
   `column [asc|desc]`, group `region | total=sum(amount), p95=p95(amount)`
   → parsed into real `FilterView` / `SortedView` / `AggregateView`.

## Decisive constraint

All view types hold `Rc<RefCell<dyn ReadableTable>>` and are therefore `!Send`.
`Table` itself is `Send` (only owned storage), which is why it currently lives
in `Arc<Mutex<HashMap<…>>>`. **Views cannot live in that shared state.** They
must be owned by a single thread.

`view.sync()` must also run **before** `clear_changeset()`; the engine's
`TickableTable` already orders sync-then-compact correctly.

## Architecture

Split engine logic from concurrency so the logic is unit-testable without actix:

- **`TableEngine`** (plain struct, single-threaded, no `Send` bound) owns all
  `Rc`-based state: base `Rc<RefCell<Table>>` tables, a `TickableTable` per base,
  per-connection pipelines, and subscriber handles. Plain synchronous methods:
  `query`, `insert_row`, `update_cell`, `delete_row`, `set_pipeline`,
  `tick_and_collect`, `drop_connection`. The existing `websocket.rs` mutation
  logic (two-phase commit, row-id mapping, JSON conversion) moves here largely
  unchanged, and the existing unit tests retarget from `AppState::*` to
  `TableEngine::*`.
- **`TableEngineActor`** — a thin actix actor holding `TableEngine` as a field
  (actor fields need not be `Send`, so the `Rc` views are legal here). Handles
  inbound messages by calling `TableEngine` methods and pushing results to
  subscriber `Addr`s.
- **`AppState`** (`web::Data`) changes from `Arc<Mutex<HashMap<…>>>` to holding
  the actor's `Addr<TableEngineActor>` (which is `Send + Sync`). Each
  `TableWebSocket` connection actor sends messages to it.

The existing base-table protocol (`Subscribe`/`Query`/`InsertRow`/`UpdateCell`/
`DeleteRow` → `TableData`/`RowInserted`/`CellUpdated`/`RowDeleted`) is preserved
unchanged, so the plain editor page (`LiveTable.tsx`) keeps working. The
pipeline messages are purely additive.

### Pipeline ownership — DECIDED: per-connection

The engine holds `HashMap<ConnId, Pipeline>`. Each cascade-demo client defines
and edits its own filter→sort→group over the shared base table; edits are
independent, matching today's per-browser behavior. The single-threaded engine
ticks every connection's pipeline on each base mutation and pushes each
connection its own `ViewData`. Disconnect drops that connection's pipeline.

(Considered and rejected: a single shared pipeline per base table — simpler
state, but cross-tab edits would be surprising for this demo.)

## Wire protocol (additive)

New `ClientMessage`:
- `SetPipeline { table_name, pipeline_generation, nodes: [ViewNodeSpec] }` —
  defines/replaces this connection's pipeline. `pipeline_generation` is a
  client-selected `u32`, incremented for every complete definition and echoed
  by all pipeline responses. Re-sent (debounced ~250ms) on any expression
  edit, since a view's predicate/keys are fixed at construction (rebuild on
  edit).

`ViewNodeSpec` (tagged union; server parses into real engine constructs):
```
{ id, kind:"filter", source_id, predicate:"amount >= 500 AND region != 'West'" }
{ id, kind:"sort",   source_id, keys:[{column:"amount", descending:true}] }
{ id, kind:"group",  source_id, group_by:["region"],
      aggs:[{alias:"total", op:"sum", column:"amount"},
            {alias:"p95",   op:"p95", column:"amount"}] }
```
`source_id` of `"base"` means the root table. Filter predicate → `expr::parse_expr`
wrapped in a closure for `FilterView::new`. Sort → `Vec<SortKey>`. Group →
`Vec<(alias, source_col, AggregateFunction)>`, with an op-string parser mapping
`sum|avg|min|max|count|median|pXX|percentile(x)` → `AggregateFunction`.

New `ServerMessage`:
- `ViewData { table_name, pipeline_generation, node_id, source_id, kind, seq,
  columns, rows }` — one per node whose output changed on a tick. `rows` use a
  nullable `row_id` (present for base rows, null for derived rows without a
  stable identity) so the demo can target base edit/delete without an unsafe
  integer sentinel. The base table is streamed as node `"base"` so the client
  handles one uniform message type.
- `ViewError { table_name, pipeline_generation, node_id, message }` — bad
  expression / unknown column, scoped to the definition that failed. The
  client already renders a rose error box.

`PROTOCOL_VERSION` bumps; the client's `SUPPORTED_PROTOCOL_VERSION` bumps to match.

## Propagation flow

On a base mutation message (`InsertRow`/`UpdateCell`/`DeleteRow`) the engine, on
its single thread:
1. applies the change to the `Rc<RefCell<Table>>` base (reused two-phase-commit
   mutation logic),
2. calls `tickable.tick()` — the real incremental sync of every registered view
   (head filter from the base changeset; sort/group via version-checked refresh;
   parent-before-child; min-cursor compaction),
3. for each node whose `version()` advanced, snapshots its rows and pushes
   `ViewData{ pipeline_generation, node_id, seq = view.version() }` to that
   connection's subscriber,
4. still broadcasts the base `RowInserted`/etc. to plain base subscribers (the
   editor page).

`SetPipeline` tears down the old view chain, builds the new one (errors →
`ViewError`), registers the head with the `TickableTable`, and emits initial
`ViewData` for every node. Per-node `seq` = `ReadableTable::version()` (own
counter + parent version), monotonic only within a pipeline generation. The
client first compares `pipeline_generation`, then drops stale `seq` values
within the active generation. This is required because rebuilding a view can
reset its own version counter.

## Frontend changes

- **`CascadeDemo.tsx`**: delete `applyFilter`/`applySort`/`applyGroup`/
  `parseGroupExpression`/`evaluatePipeline`/`compareValues` (~340 lines). Nodes
  render server-pushed `ViewData` keyed by `node_id`. A node "ticks" (flashes +
  increments) when its `ViewData` arrives — which also retires the old buggy
  highlight `useEffect` (no more `nodes`-ref dependency; the stuck-highlight and
  StrictMode bugs disappear). Edit/delete controls on the base node send
  `UpdateCell`/`DeleteRow` by `row_id`.
- **`usePipeline` hook** — sends `SetPipeline`, holds `Record<nodeId,
  {columns, rows, seq}>`, applies `ViewData`/`ViewError`, drops older
  generations and stale `seq` values within the active generation.
  Expression edits debounced ~250ms (fixing the un-debounced keystroke tick
  inflation).
- Default pipeline + the three expression boxes use **real engine syntax**.

## Testing

- Rust: `TableEngine` unit tests (build pipeline, mutate, assert per-node
  snapshots) — reuse the differential-rebuild idea; assert engine-computed node
  output equals a fresh build.
- Protocol round-trip: serialize/deserialize the new messages.
- Frontend: extend `useTableWebSocket`/hook tests for `ViewData`/`ViewError`
  application and stale-`seq` drop.
- Manual: run server + frontend, watch a cell edit cross the filter boundary and
  a delete shrink a group.

## Out of scope / future

- Per-node computed *deltas* on the wire (snapshots are enough at demo scale).
- Joins/computed columns over the wire (protocol only exposes filter/sort/group).
- Multi-base-table pipelines.

## Docs to update (per CLAUDE.md checklist)

README.md (features + example), CLAUDE.md (WebSocket protocol notes + Python API
if surface changes), docs/PYTHON_BINDINGS_README.md, docs/ORIGINAL_VISION.md
(mark server-side view streaming implemented), and the protocol-version note.
