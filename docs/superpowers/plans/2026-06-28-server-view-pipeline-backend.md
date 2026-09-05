# Server-Computed View Pipeline (Backend) Implementation Plan

**Archived, implemented plan.** Instructions and checklists below record the
original work, not tasks to execute now. The [protocol reference](../../WEBSOCKET_PROTOCOL.md)
is authoritative for current wire behavior. Since this plan, bounded mixed-batch
[filter](../../INCREMENTAL_FILTER_PIPELINE.md) and
[sorted](../../INCREMENTAL_SORTED_PIPELINE.md) propagation have landed; the
single-change-only guidance below describes the original milestone. Subsequent
[protocol-v3 delivery](../../PIPELINE_DELIVERY.md) adds base/filter/sort deltas
with snapshot recovery; the snapshot-only instructions below remain historical.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose LiveTable's real Rust view engine over the WebSocket protocol so a client can subscribe to a server-computed filter→sort→group pipeline that updates incrementally on every base-table mutation.

**Architecture:** A single-threaded `TableEngine` owns the `Rc`-based views (which are `!Send`) and lives inside a thin actix `TableEngineActor`; `AppState` holds the actor's `Addr` instead of `Arc<Mutex<…>>`. Each connection registers a per-connection pipeline of real `FilterView`/`SortedView`/`AggregateView`; a base mutation calls `TickableTable::tick()` and the engine pushes each affected node's snapshot as `ViewData`. The existing flat-table protocol is preserved unchanged.

**Tech Stack:** Rust, actix / actix-web-actors, serde/serde_json, PyO3 (unaffected), existing `crate::view`, `crate::expr`, `crate::table`.

## Status (updated 2026-08-03)

**Implementation complete and committed.** Protocol v2, bounded pipeline
validation, `TableEngine`, the single-threaded Actix owner, `usePipeline`, the
migrated cascade demo, and both integration-test layers landed in `845028a`
(`Implement protocol v2 server view pipelines`).

Prior commits (newest first):
- `5785552` Task 2 — `pipeline_spec` (spec → real views; `count` requires a column)
- `cf85c56` Task 1 — `SetPipeline`/`ViewData`/`ViewError` wire types (protocol v2)
- `b97b0c9` this backend plan · `bb9a1ea` design spec · `efe8eac`+`07a5735` core forward-prop fixes + differential fuzz

Verification completed: full Rust server/core/fuzz/doc-test suite, Clippy with
server features and warnings denied, 23 frontend tests, frontend lint/build,
and an automated real-WebSocket test (`base`→filter→sort→group plus
insert/update/delete propagation). The engine differential test also checks 500
randomized mutation ticks against an independent shadow model.

**Gotchas already learned:**
- Server modules (`messages`/`websocket`/`server`/`engine`/`pipeline_spec`) are
  `#[cfg(feature = "server")]`; test with `cargo test --lib --features server`
  (plain `cargo test --lib` silently skips them and won't even recompile them).
- `count` is `COUNT(col)` (non-null count); the engine has no row-count
  aggregate. The frontend's default group spec must use `count(amount)`, not
  `count()`. The implemented cascade parser and default spec follow this rule.
- The finalized protocol scopes every pipeline response with a client-selected
  `pipeline_generation: u32`; node `seq` is monotonic only within a generation
  because rebuilding a view resets its own counter.
- Derived snapshots use `WireViewRow { row_id: Option<u64>, row }`. Send null
  when a derived row lacks stable identity; `u64::MAX` is not safe in
  JavaScript.
- Call `validate_pipeline_spec()` before allocating views; it validates ordered
  sources, IDs, required fields, and resource limits.
- Single change per tick is the verified-correct engine path; the server ticks
  once per mutation message.

Commands: core views `cargo test --lib`; server `cargo test --lib --features server`;
fuzz `cargo test --test forward_prop_fuzz`; build server
`cargo build --bin livetable-server --features server`. All under
`env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1` from `impl/`.

---

## Global Constraints

- Build with `env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1` (per CLAUDE.md).
- Server builds under the `server` feature: `cargo build --bin livetable-server --features server`.
- Views are `!Send`: never store a view in `Arc<Mutex<…>>` or send one across threads. They live only inside `TableEngine`, which lives only inside the actor.
- `view.sync()` must precede `clear_changeset()`; always propagate via `TickableTable::tick()` (sync-then-compact), never raw `clear_changeset()`, on tables that have registered views.
- Single change per tick is the verified-correct path; the engine ticks once per mutation message.
- Keep `seq` populated on every server→client message (existing reconciliation invariant).
- Preserve two-phase-commit mutation semantics (validate-all-before-mutate; `truncate_to`/re-insert rollback) when moving mutation logic.
- The `messages`/`websocket`/`server` modules are gated behind `#[cfg(feature = "server")]`; the new `engine`/`pipeline_spec` modules depend on `messages` and so must also be `#[cfg(feature = "server")]`. **Test them with `cargo test --lib --features server`** (plain `cargo test --lib` silently skips them). Run the core view suite with `cargo test --lib` and the differential fuzz with `cargo test --test forward_prop_fuzz`.

---

## File Structure

- **Create `impl/src/engine.rs`** — `TableEngine` (owns `Rc`-based base tables + `TickableTable` + per-connection pipelines), `Pipeline`, `ViewNode`. Pure logic, no actix. Unit-tested directly.
- **Create `impl/src/pipeline_spec.rs`** — `ViewSpec` → real view construction: predicate parse (`expr`), sort-key build, aggregate op-string parser (`sum|avg|min|max|count|median|pXX|percentile(x)`). Pure functions, unit-tested.
- **Modify `impl/src/messages.rs`** — add `ViewSpec`, `ClientMessage::SetPipeline`, `ServerMessage::ViewData`/`ViewError`; bump `PROTOCOL_VERSION`.
- **Modify `impl/src/websocket.rs`** — replace `AppState`'s `Arc<Mutex<…>>` with `Addr<TableEngineActor>`; add `TableEngineActor` wrapping `TableEngine`; route connection messages (including `SetPipeline`) through the actor; push `ViewData`/`ViewError` to the originating connection.
- **Modify `impl/src/lib.rs`** — `pub mod engine; pub mod pipeline_spec;`.
- **Test files:** `impl/src/engine.rs` (`#[cfg(test)]`), `impl/src/pipeline_spec.rs` (`#[cfg(test)]`), `impl/src/messages.rs` (extend existing tests), and reuse `impl/tests/forward_prop_fuzz.rs` patterns for the engine node-snapshot oracle.

Connection identity: the engine keys pipelines by a `ConnId(u64)` the actor assigns per connection (monotonic counter in the actor), passed on `SetPipeline`/`DropConnection`.

---

### Task 1: Protocol types — ViewSpec, SetPipeline, ViewData, ViewError

**Files:**
- Modify: `impl/src/messages.rs`
- Test: `impl/src/messages.rs` (`#[cfg(test)]` module — extend existing)

**Interfaces:**
- Produces:
  - `enum ViewKindSpec { Filter { predicate: String }, Sort { keys: Vec<SortKeySpec> }, Group { group_by: Vec<String>, aggs: Vec<AggSpec> } }`
  - `struct SortKeySpec { column: String, descending: bool }`
  - `struct AggSpec { alias: String, op: String, column: String }`
  - `struct ViewNodeSpec { id: String, source_id: String, #[serde(flatten)] kind: ViewKindSpec }`
  - `ClientMessage::SetPipeline { table_name: String, pipeline_generation: u32, nodes: Vec<ViewNodeSpec> }`
  - `ServerMessage::ViewData { table_name: String, pipeline_generation: u32, node_id: String, source_id: String, kind: String, seq: u64, columns: Vec<String>, rows: Vec<WireViewRow> }`
  - `ServerMessage::ViewError { table_name: String, pipeline_generation: u32, node_id: String, message: String }`
  - `PROTOCOL_VERSION` bumped to `2`.
  - `WireTableRow` remains unchanged for base-table snapshots. `WireViewRow`
    carries `row_id: Option<u64>` + `row: JsonRow`; derived rows without stable
    identity use `None` and serialize as null.

- [x] **Step 1: Write failing round-trip tests** in the `messages.rs` test module:

```rust
#[test]
fn set_pipeline_deserializes_tagged_specs() {
    let json = r#"{"type":"SetPipeline","table_name":"demo","pipeline_generation":1,"nodes":[
        {"id":"f","source_id":"base","kind":"filter","predicate":"amount >= 500"},
        {"id":"s","source_id":"f","kind":"sort","keys":[{"column":"amount","descending":true}]},
        {"id":"g","source_id":"s","kind":"group","group_by":["region"],
         "aggs":[{"alias":"total","op":"sum","column":"amount"},
                 {"alias":"p95","op":"p95","column":"amount"}]}
    ]}"#;
    let msg: ClientMessage = serde_json::from_str(json).unwrap();
    match msg {
        ClientMessage::SetPipeline { table_name, pipeline_generation, nodes } => {
            assert_eq!(table_name, "demo");
            assert_eq!(pipeline_generation, 1);
            assert_eq!(nodes.len(), 3);
            assert_eq!(nodes[0].id, "f");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn view_data_serializes_with_seq() {
    let msg = ServerMessage::ViewData {
        table_name: "demo".into(), pipeline_generation: 1,
        node_id: "g".into(), source_id: "s".into(),
        kind: "group".into(), seq: 7, columns: vec!["region".into(), "total".into()],
        rows: vec![],
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"type\":\"ViewData\""));
    assert!(json.contains("\"seq\":7"));
}
```

- [x] **Step 2: Run, verify they fail to compile** (variants/types absent).

Run: `cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib messages`
Expected: compile error — `ViewKindSpec`/`SetPipeline`/`ViewData` not found.

- [x] **Step 3: Add the types.** In `messages.rs`, alongside the existing enums. Use serde tag conventions matching the existing `#[serde(tag = "type")]` on `ClientMessage`/`ServerMessage`. For `ViewKindSpec`, use `#[serde(tag = "kind", rename_all = "lowercase")]`. Add the structs (`#[derive(Debug, Clone, Serialize, Deserialize)]`). Add the two `ClientMessage`/`ServerMessage` variants. Bump `pub const PROTOCOL_VERSION: u32 = 2;`.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKeySpec { pub column: String, pub descending: bool }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggSpec { pub alias: String, pub op: String, pub column: String }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ViewKindSpec {
    Filter { predicate: String },
    Sort { keys: Vec<SortKeySpec> },
    Group { group_by: Vec<String>, aggs: Vec<AggSpec> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewNodeSpec {
    pub id: String,
    pub source_id: String,
    #[serde(flatten)]
    pub kind: ViewKindSpec,
}
```
Add to `ClientMessage`: `SetPipeline { table_name: String, pipeline_generation: u32, nodes: Vec<ViewNodeSpec> }`.
Add to `ServerMessage`: the `ViewData { … }` and `ViewError { … }` variants above.

- [x] **Step 4: Run tests, verify pass.**

Run: `cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib messages`
Expected: PASS.

- [x] **Step 5: Commit.**

```bash
git add impl/src/messages.rs
git commit -m "Add SetPipeline/ViewData/ViewError wire types (protocol v2)"
```

---

### Task 2: `pipeline_spec` — build real views from specs

**Files:**
- Create: `impl/src/pipeline_spec.rs`
- Modify: `impl/src/lib.rs` (add `pub mod pipeline_spec;`)
- Test: `impl/src/pipeline_spec.rs` (`#[cfg(test)]`)

**Interfaces:**
- Consumes: `crate::expr::{parse_expr, eval_expr}`, `crate::view::{FilterView, SortedView, AggregateView, SortKey, SortOrder, AggregateFunction}`, `crate::readable::ReadableTable`, `messages::{ViewNodeSpec, ViewKindSpec, AggSpec, SortKeySpec}`.
- Produces:
  - `fn parse_agg_function(op: &str) -> Result<AggregateFunction, String>` — maps `"sum"|"count"|"avg"|"min"|"max"|"median"|"p<NN>"|"percentile(<f>)"` (case-insensitive) to the enum; errors on unknown.
  - `fn build_filter(parent: Rc<RefCell<dyn ReadableTable>>, id: &str, predicate: &str) -> Result<Rc<RefCell<FilterView>>, String>`
  - `fn build_sort(parent: …, id: &str, keys: &[SortKeySpec]) -> Result<Rc<RefCell<SortedView>>, String>`
  - `fn build_group(parent: …, id: &str, group_by: &[String], aggs: &[AggSpec]) -> Result<Rc<RefCell<AggregateView>>, String>`

- [x] **Step 1: Write failing tests.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::view::AggregateFunction;

    #[test]
    fn parses_agg_ops() {
        assert_eq!(parse_agg_function("sum").unwrap(), AggregateFunction::Sum);
        assert_eq!(parse_agg_function("AVG").unwrap(), AggregateFunction::Avg);
        assert_eq!(parse_agg_function("median").unwrap(), AggregateFunction::Median);
        assert_eq!(parse_agg_function("p95").unwrap(), AggregateFunction::Percentile(0.95));
        assert_eq!(parse_agg_function("percentile(0.25)").unwrap(), AggregateFunction::Percentile(0.25));
        assert!(parse_agg_function("bogus").is_err());
    }
}
```

- [x] **Step 2: Run, verify fail.** Run: `cargo test --lib pipeline_spec` → compile error (module absent). Add `pub mod pipeline_spec;` to `lib.rs` and a stub `pub fn parse_agg_function(...)` returning `Err` first if needed so it compiles to a real FAIL.

- [x] **Step 3: Implement.** `parse_agg_function`: lowercase, match `sum/count/avg/min/max/median`; `pNN` → `Percentile(NN/100.0)`; `percentile(x)` → parse `x` as `f64`, validate `0.0..=1.0`. The build_* functions: filter parses the predicate once and captures it in the closure:

```rust
pub fn build_filter(
    parent: Rc<RefCell<dyn ReadableTable>>,
    id: &str,
    predicate: &str,
) -> Result<Rc<RefCell<FilterView>>, String> {
    let expr = crate::expr::parse_expr(predicate)?;
    let view = FilterView::new(id.to_string(), parent, move |row| crate::expr::eval_expr(&expr, row));
    Ok(Rc::new(RefCell::new(view)))
}

pub fn build_sort(parent: Rc<RefCell<dyn ReadableTable>>, id: &str, keys: &[SortKeySpec]) -> Result<Rc<RefCell<SortedView>>, String> {
    let sort_keys = keys.iter().map(|k| if k.descending { SortKey::descending(&k.column) } else { SortKey::ascending(&k.column) }).collect();
    Ok(Rc::new(RefCell::new(SortedView::new(id.to_string(), parent, sort_keys)?)))
}

pub fn build_group(parent: Rc<RefCell<dyn ReadableTable>>, id: &str, group_by: &[String], aggs: &[AggSpec]) -> Result<Rc<RefCell<AggregateView>>, String> {
    let specs = aggs.iter().map(|a| {
        let func = parse_agg_function(&a.op)?;
        let col = a.column.clone(); // count is COUNT(column), never count()
        Ok((a.alias.clone(), col, func))
    }).collect::<Result<Vec<_>, String>>()?;
    Ok(Rc::new(RefCell::new(AggregateView::new(id.to_string(), parent, group_by.to_vec(), specs)?)))
}
```

- [x] **Step 4: Run tests, verify pass.** Run: `cargo test --lib pipeline_spec`. Expected: PASS.

- [x] **Step 5: Commit.** `git add impl/src/pipeline_spec.rs impl/src/lib.rs && git commit -m "Add pipeline_spec: build real views from ViewSpec"`

---

### Task 3: `TableEngine` — owns bases, tables mutations, and per-connection pipelines

**Files:**
- Create: `impl/src/engine.rs`
- Modify: `impl/src/lib.rs` (`pub mod engine;`)
- Test: `impl/src/engine.rs` (`#[cfg(test)]`)

**Interfaces:**
- Consumes: Task 2 build_* functions, `crate::view::TickableTable`, `crate::table::{Table, Schema}`, `messages::{ViewNodeSpec, ViewKindSpec, WireTableRow, WireViewRow}`, the existing JSON conversion helpers (move `column_value_to_json`, `row_to_json`, `convert_row_for_schema`, `json_to_column_value_typed`, date helpers from `websocket.rs` into `engine.rs` or a shared `wire_convert.rs`; pick one and keep `websocket.rs` using it).
- Produces:
  - `struct TableEngine` with `new() -> Self` (seeds the `demo` table identically to today).
  - `type ConnId = u64;`
  - `struct NodeSnapshot { pipeline_generation, node_id, source_id, kind, seq, columns, rows: Vec<WireViewRow> }`
  - `fn query_table(&self, table: &str) -> Result<(u64 /*seq*/, Vec<String>, Vec<WireTableRow>), String>`
  - `fn insert_row(&mut self, table, JsonRow) -> Result<(u64 seq, usize index, u64 row_id, JsonRow), String>` (and `update_cell`, `delete_row`) — same return shape the existing `ServerMessage` builders need.
  - `fn set_pipeline(&mut self, conn: ConnId, table: &str, pipeline_generation: u32, nodes: &[ViewNodeSpec]) -> Vec<Result<NodeSnapshot, (String /*node_id*/, String /*err*/)>>` — validate and (re)build that connection's pipeline; returns initial per-node snapshots or per-node errors scoped to the generation.
  - `fn tick_and_collect(&mut self, table: &str) -> HashMap<ConnId, Vec<NodeSnapshot>>` — run `TickableTable::tick()`, then for each connection pipeline on this base, snapshot every node whose `version()` advanced since last collect.
  - `fn drop_connection(&mut self, conn: ConnId)` — remove and unregister that connection's pipeline.

**Internal model:**
```rust
struct ViewNode { id: String, source_id: String, kind: &'static str, view: Rc<RefCell<dyn ReadableTable>>, last_seq: Cell<u64> }
struct Pipeline { generation: u32, nodes: Vec<ViewNode> } // includes a synthetic "base" node referencing the base table
struct BaseState {
    table: Rc<RefCell<Table>>,
    tickable: TickableTable,
    row_ids: Vec<u64>,
    next_row_id: u64,
    pipelines: HashMap<ConnId, Pipeline>,
}
struct TableEngine { bases: HashMap<String, BaseState> }
```
Notes:
- The base table must be `Rc<RefCell<Table>>` now (was a plain `Table`). `TickableTable::new(base.clone())` wraps it; mutations go through `base.borrow_mut()`.
- `set_pipeline` resolves each node's `source_id` against already-built nodes in array order (`"base"` → the base table coerced to `Rc<RefCell<dyn ReadableTable>>`), calls the Task-2 builder, registers head/each view with `tickable` (filter→`register_filter`, etc.), and stores the `Rc` view. On a build error for a node, record the error and stop building downstream nodes (they'd have no valid source).
- `tick_and_collect` calls `tickable.tick()` once; then per connection per node, compares `view.version()` to `last_seq`; if changed, emit a `NodeSnapshot` and update `last_seq`. The synthetic base node uses the base table's `changeset().total_len()` as its seq.
- Snapshot building: `columns = view.column_names()`; for each `i in 0..view.len()`, `view.get_row(i)` → JSON. Keep it simple: **only the synthetic base node carries `Some(real_row_id)`**; all derived nodes send `row_id: None`, serialized as null. (Edit/delete in the UI act on the base node.)

- [x] **Step 1: Write failing test — engine builds a pipeline and ticks it.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use crate::messages::{ViewNodeSpec, ViewKindSpec, SortKeySpec, AggSpec};

    fn demo_pipeline() -> Vec<ViewNodeSpec> {
        vec![
            ViewNodeSpec { id: "f".into(), source_id: "base".into(),
                kind: ViewKindSpec::Filter { predicate: "amount >= 150".into() } },
            ViewNodeSpec { id: "g".into(), source_id: "f".into(),
                kind: ViewKindSpec::Group { group_by: vec!["region".into()],
                    aggs: vec![AggSpec { alias: "total".into(), op: "sum".into(), column: "amount".into() }] } },
        ]
    }

    #[test]
    fn pipeline_propagates_on_insert() {
        let mut eng = TableEngine::new();
        let snaps = eng.set_pipeline(1, "demo", 1, &demo_pipeline());
        assert!(snaps.iter().all(|s| s.is_ok()));

        // Insert a West row above the filter threshold.
        eng.insert_row("demo", std::collections::HashMap::from([
            ("region".into(), json!("West")), ("product".into(), json!("X")), ("amount".into(), json!(500.0)),
        ])).unwrap();

        let collected = eng.tick_and_collect("demo");
        let nodes = &collected[&1];
        let group = nodes.iter().find(|n| n.node_id == "g").expect("group node updated");
        // West total reflects the new 500 row.
        assert!(group.rows.iter().any(|r|
            r.row.get("region") == Some(&json!("West")) && r.row.get("total") == Some(&json!(500.0))));
    }
}
```

- [x] **Step 2: Run, verify fail.** Run: `cargo test --lib engine::tests::pipeline_propagates_on_insert` → fails to compile (engine absent). Add `pub mod engine;` and stubs to reach a real FAIL.

- [x] **Step 3: Implement `TableEngine`.** Move the JSON conversion + mutation logic from `websocket.rs` (`TableState::{insert_row, update_cell, delete_row}`, `table_to_json`, `convert_row_for_schema`, etc.) into `engine.rs`, adapting `table: Table` → `table: Rc<RefCell<Table>>` and routing propagation through `tickable.tick()` instead of `clear_changeset()`. Implement `set_pipeline`, `tick_and_collect`, `drop_connection`, and the snapshot helper per the model above. The base node is always present in every pipeline (id `"base"`).

- [x] **Step 4: Run test, verify pass.** Run: `cargo test --lib engine`. Expected: PASS.

- [x] **Step 5: Add a differential engine test** mirroring `forward_prop_fuzz`: build a filter→sort→group pipeline via `set_pipeline`, apply random insert/update/delete through engine methods + `tick_and_collect`, and assert each node snapshot equals a fresh `clone`-built pipeline. (Reuse the multiset/ordered comparison from `forward_prop_fuzz.rs`.) Keep it small (10 trials × 50 steps) since the engine path is thin over the already-fuzzed views.

- [x] **Step 6: Run, verify pass; commit.** Completed in consolidated commit `845028a`.

```bash
git add impl/src/engine.rs impl/src/lib.rs
git commit -m "Add TableEngine: per-connection server-side view pipelines"
```

---

### Task 4: `TableEngineActor` + actor-based `AppState`

**Files:**
- Modify: `impl/src/websocket.rs`
- Test: `impl/src/websocket.rs` (`#[cfg(test)]` — adapt existing tests to call `TableEngine` directly where they previously called `AppState`; add actor message tests).

**Interfaces:**
- Consumes: `engine::{TableEngine, ConnId, NodeSnapshot}`, `messages::*`.
- Produces:
  - `struct TableEngineActor { engine: TableEngine, subscribers: HashMap<String, Vec<(ConnId, Addr<TableWebSocket>)>>, next_conn: ConnId }` — `Actor<Context = Context<Self>>`. NOT `Send` (holds `Rc` via `engine`); started once on the system arbiter; its `Addr` is `Send`.
  - actix messages (each `#[derive(Message)]`, `Send` payloads only): `Query{table}`, `Insert{table,row}`, `Update{table,row_id,column,value}`, `Delete{table,row_id}`, `SetPipeline{conn,table,pipeline_generation,nodes,requester: Addr<TableWebSocket>}`, `SubscribeBase{table,conn,addr}`, `Disconnect{conn,table}`. Handlers call `engine` methods, then broadcast: base mutations → existing `RowInserted`/etc. to base subscribers AND `tick_and_collect` → `ViewData` to each pipeline connection's `Addr`.
  - `AppState { engine: Addr<TableEngineActor> }`.
- `TableWebSocket` gains a `conn_id: ConnId` (assigned on connect via a `RegisterConn` round-trip or a locally-unique id) and forwards `ClientMessage::SetPipeline` to the actor.

- [x] **Step 1: Write failing test** — actor handles `SetPipeline` then an insert and the requester receives `ViewData`. Use `actix::test` (`#[actix_rt::test]`) with a stub recipient actor capturing `BroadcastMessage`s; assert a `ViewData{node_id:"g"}` arrives after an insert.

- [x] **Step 2: Run, verify fail** (actor/types absent).

- [x] **Step 3: Implement.** Replace `AppState`'s fields with `engine: Addr<TableEngineActor>`; in `main`/server setup start the actor on the system arbiter and put its `Addr` in `web::Data`. Implement each `Handler<…>`; the connection actor (`TableWebSocket`) sends messages and, on `SetPipeline`, includes its own `ctx.address()` so the actor can push `ViewData` back. Base `Subscribe`/`Query`/`Insert`/`Update`/`Delete` keep their existing client-visible behavior (the actor just owns the data now). On mutation, the actor: applies via engine, broadcasts the base delta to base subscribers, then `tick_and_collect` and sends each connection its `ViewData`/`ViewError`.

- [x] **Step 4: Run tests** (`cargo test --lib websocket` + the adapted existing tests). Expected: PASS.

- [x] **Step 5: Commit.** Completed in consolidated commit `845028a`.

---

### Task 5: Build, server smoke test, docs

**Files:**
- Modify: `impl/src/server.rs` / bin entrypoint if actor startup lives there.
- Modify: README.md, CLAUDE.md (WebSocket protocol notes), docs/PYTHON_BINDINGS_README.md (if surface changed — it does not), docs/ORIGINAL_VISION.md (mark server-side view streaming implemented).

- [x] **Step 1: Build the server.** Run: `cd impl && cargo build --bin livetable-server --features server`. Expected: success.
- [x] **Step 2: Full Rust suite + fuzz.** Run: `cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib && cargo test --test forward_prop_fuzz`. Expected: all green.
- [x] **Step 3: Manual smoke** — run the server, connect a `wscat`/script client, send `Subscribe` + `SetPipeline`, then `InsertRow`, and confirm `ViewData` for `base`/`f`/`s`/`g` arrives with the new row reflected; send `UpdateCell` crossing the filter boundary and confirm the row leaves the filter node and the group total drops.
- [x] **Step 4: Update docs** per the CLAUDE.md checklist; add the new protocol messages and `PROTOCOL_VERSION = 2` note to CLAUDE.md's WebSocket section.
- [x] **Step 5: Commit.** Completed in consolidated commit `845028a`.

---

## Self-Review

**Spec coverage:** TableEngine + actor (Task 3/4) ✓; `!Send` constraint honored (engine lives only in actor) ✓; per-connection pipelines (Task 3 `HashMap<ConnId, Pipeline>`) ✓; SetPipeline/ViewData/ViewError + ViewSpec (Task 1) ✓; real-syntax parsing incl. median/percentile (Task 2) ✓; per-mutation tick propagation (Task 3 `tick_and_collect`) ✓; base-protocol preserved (Task 4 keeps base subscribe/query/deltas) ✓; testing (engine unit + differential + actor + smoke) ✓; docs (Task 5) ✓.

**Frontend integration is complete:** `usePipeline` performs generation/sequence
reconciliation and the cascade demo renders and edits the server-owned DAG.

**Resolved Task-4 risk:** the `!Send` engine compiles and runs inside
`TableEngineActor`, while connection actors communicate through its `Addr`.
Actor tests, a manual smoke, and the real-server WebSocket integration test all
exercise this ownership boundary successfully.
