# Aggregate Output Changesets — Design

**Date:** 2026-09-23
**Status:** Proposed
**Goal:** Give `AggregateView` an output changeset in its own group
coordinates. Group nodes then receive wire deltas instead of full snapshots, and
views built on a group update incrementally instead of rebuilding.

## Problem

Filters and sorts publish bounded output history; aggregates do not. Two
consequences follow:

- **Wire:** `AggregateView::changeset()` is `None`, so delivery snapshots every
  group node whenever its version (which includes ancestors) moves. That
  includes edits the group never sees: Orders Lab scenario 01 and the recorded
  benchmark's excluded edit both still send a full group snapshot.
  `docs/PIPELINE_DELIVERY.md` notes that high-cardinality group snapshots can
  dominate delivery cost.
- **Engine:** `pipeline_spec` accepts a group as a later node's source, and
  Rust callers can chain views over an aggregate. Every such child currently
  takes the version-checked full rebuild path on each tick.

## Decisions

1. **Deltas only.** Group rows keep `row_id: null`. Delta operations are
   addressed by index, so they need no row identity. Stable derived-row
   identity stays on the Planned list as a separate milestone covering
   filter/sort/group rows (all derived rows send `null` today).
2. **Net diff per batch** (approach A below).
3. **Protocol version 3 → 4.** Message shapes do not change, but a v3 client
   rejects group deltas (`pipelineReconciliation.ts` returns `null` for
   `kind === 'group'`) and would request a repair on every tick. Per the
   project rule, bump `PROTOCOL_VERSION` and `SUPPORTED_PROTOCOL_VERSION`
   together.

## Approaches considered

- **A. Net diff per batch (chosen).** Record each group's output row the
  first time the batch touches it. When the batch finishes, including deferred
  MIN/MAX recalculation, emit only what changed. Output size tracks touched
  groups, not input events, and values are final.
- **B. Emit as each input event is applied** (the filter's approach).
  Rejected: MIN/MAX recalculation is deferred to the end of the batch
  (mid-batch rescans read misaligned rows — the bug the fuzz caught), so
  intermediate values are unknown. It would also turn 256 updates to one
  group into 256×k events, far past the 512-operation delta limit.
- **C. Diff the whole output before and after each sync.** Rejected: O(groups)
  per tick regardless of batch size, and CLAUDE.md forbids building deltas by
  scanning or diffing full node snapshots.

## Output contract

The contract mirrors `SortedView` (see `docs/INCREMENTAL_SORTED_PIPELINE.md`):

- `output_changes: Changeset` holds `TableChange` events in group coordinates:
  positions in `group_order`, with sequential step-local indices.
- `changeset()` returns it only while `parent.version() == last_parent_version`.
- Each non-empty input batch applied incrementally clears the history first
  (after the pre-passes that may still choose a rebuild), so exactly one
  batch is retained. A no-op sync keeps it.
- `rebuild_index()` calls `output_changes.invalidate()`. That one call covers
  every rebuild: refresh, missing or compacted parent history, a parent without
  history, more than 256 group-key updates, and replay failures.
- **An empty input batch records the parent version** and keeps history,
  mirroring `sorted.rs` (the "filter can advance its ancestor version without
  emitting rows" case). Today `sync()` returns early without updating
  `last_parent_version`. Left as is, an excluded edit upstream would leave
  `changeset()` as `None`, and delivery would still snapshot the group:
  exactly the case this milestone removes.
- Aggregate values, NaN/NULL exclusion, group-key canonicalization, and the
  deferred MIN/MAX recalculation are unchanged. `sync()`'s boolean return value
  is unchanged.

## Emission (single incremental path)

Only `apply_changes` needs hooks. The integer fast path runs only inside
`rebuild_index`, which invalidates.

Per-batch scratch state is `touched: HashMap<GroupKey, Touch>`:

| State | Meaning |
|-------|---------|
| `Existing(Row)` | Group existed before the batch; `Row` is its output row captured on first touch, before any mutation |
| `New` | Group created during this batch, including re-creating a key whose pre-batch group was deleted |
| `Deleted` | Group removed during this batch; any required `RowDeleted` is already emitted |

Hooks:

- **Before mutating an existing group** (row added, row removed, aggregated
  column updated): if the key is not in `touched`, record
  `Existing(row_for_key(key))`.
- **On group creation:** mark it `New`.
- **On group removal:** if it is `Existing(row)`, emit
  `RowDeleted { index: position, data: row }` immediately. In both cases mark
  it `Deleted`.

After the loop and the deferred MIN/MAX recalculation:

1. For each surviving `Existing(before)` group, emit a `CellUpdated` at the
   group's position for each aggregate result column whose value changed,
   carrying the old and new values. Floats are compared by bit pattern, so a
   NaN result (for example, SUM of +inf and -inf) does not re-emit on every
   touch. Group-by columns never change for a live group.
2. For each surviving `New` group, in ascending position order, emit
   `RowInserted { index: position, data: row_for_key(key) }`.

**Why the indices are coherent:** new groups only ever append, so every
pre-existing group precedes every new group in `group_order`. When a
pre-existing group is removed, its current position therefore equals its
position in a consumer that has replayed the emitted deletions so far. After
the loop no further structural changes occur, so survivors' positions are
final and new groups occupy the tail in creation order. A group that empties
and is re-created in one batch becomes delete + insert at the tail, matching
today's ordering behavior.

`RowDeleted` carries the complete pre-batch row, and `CellUpdated` carries the
exact old value, as filter/sort/join replay requires.

## Group positions

`CellUpdated` needs each group's final index. A linear search of `group_order`
per touched group would make the common tick (one value update) O(groups)
instead of O(1). Instead, add `position: usize` to `GroupState`:

- Assigned when a group is appended, including both rebuild paths.
- On removal, `group_order.remove(position)` then renumber the tail. This
  replaces today's `group_order.retain(...)`, which is already O(groups) per
  removal. Group removal stays O(groups); updates and first-touch captures
  stay O(1) in the number of groups.

`get_row(index)` delegates to a new `row_for_key(&GroupKey)`, which the
emission code shares.

## Downstream consumers

- **Filter/sort children** use the aggregate's history through their existing
  bounded replay (256 input events; larger batches rebuild). No code changes.
- **Join children** also start replaying: `JoinView::sync` uses any parent
  history that is available. The fuzz must cover a join over an aggregate.
- **Delivery** needs no changes: `ViewNode::collect` already takes the delta
  path for any node with history and falls back to a snapshot for missing
  history or more than 512 operations. Empty output sends nothing and keeps
  the node's delivery sequence unchanged.
- **Python** `PyAggregateView` wraps the Rust view and gains history
  internally. No Python API changes; Python still cannot chain views over an
  aggregate.

## Client and Orders Lab

- `applyViewDelta`: remove the `previous.kind === 'group'` rejection.
  `validRow` already requires `row_id === null` for group rows, which
  `view_change` emits.
- Bump `SUPPORTED_PROTOCOL_VERSION` to 4.
- Scenario copy in `frontend/src/lib/lab.ts` and `docs/ORDERS_LAB.md`:
  - 01 `group may snapshot` → no group delivery.
  - 02/03 `group snapshot` → group delta.

## Costs and bounds

Per incremental batch, extra work is O(touched groups × aggregate columns):
one row capture per touched pre-existing group and one comparison per
aggregate column. Retained history holds one batch of output events. No new
limits are needed:

- A batch with many touched groups may exceed 512 output operations and
  deliver a snapshot.
- A child receiving more than 256 events rebuilds, under existing rules.

Snapshots remain O(groups) and still happen on rebuilds, repairs, and
initial installs.

## Testing

- **Contract tests** in a new `impl/tests/aggregate_pipeline.rs`, alongside
  `filter_pipeline.rs` and `sorted_pipeline.rs`, reusing the `replay` pattern
  that asserts deletion payloads and old values:
  - A value update emits exactly the changed aggregate cells at the group's
    index.
  - An update that changes no aggregate result emits nothing.
  - New group → tail insert. Emptied group → delete with the full historical
    row. Empty-and-re-create in one batch → delete + tail insert.
  - Moving a row between groups (group-key update).
  - Deleting the current MAX in a mixed batch → `CellUpdated` with the
    rescanned value.
  - Rebuild paths invalidate lagging cursors.
  - A stale parent returns `None`.
  - An excluded upstream edit (filter parent) leaves `changeset()` as `Some`
    with no new events.
  - Filter and sort over an aggregate update without rebuilding, checked with
    `sorted_pipeline.rs`'s counted-reads wrapper.
- **Differential fuzz** (`forward_prop_fuzz.rs`):
  - For every aggregate in the chained, batched, and int-group fuzzes, replay
    its output history over the pre-tick snapshot and assert it equals the
    post-tick snapshot, like `assert_sorted_replay`.
  - Add filter-over-aggregate, sort-over-aggregate, and join-over-aggregate
    nodes, checked against from-scratch oracles.
- **Engine and WebSocket:**
  - `group_descendants_keep_snapshot_fallback` becomes a test that group
    nodes and their filter children receive deltas.
  - The engine shadow-model test asserts that excluded edits produce no group
    delivery.
  - `protocol_v3_websocket.rs` accepts group deltas.
- **Frontend:** reconciliation unit tests apply group deltas. The v3 → v4
  version check is tested. The Orders Lab e2e run still passes.
- **Benchmark:** `benchmarks/pipeline_delivery.mjs` currently ends each sample
  when the group `ViewData` arrives (line 60). Excluded edits will no longer
  send one. Replace this with a per-workload completion condition: the last
  expected node delivery, with no group message expected for excluded edits.
  Then re-record a before/after comparison with commit and environment
  provenance in `docs/PIPELINE_DELIVERY.md`.

## Documentation

Update:

- `docs/WEBSOCKET_PROTOCOL.md`: v4, group deltas, remaining limits.
- `docs/PIPELINE_DELIVERY.md`
- `docs/INCREMENTAL_FILTER_PIPELINE.md` and
  `docs/INCREMENTAL_SORTED_PIPELINE.md` limits: "views other than filters and
  sorts" becomes "other than filters, sorts, and aggregates".
- `docs/ORIGINAL_VISION.md`: split the Planned item into completed "aggregate
  deltas" and remaining "stable derived-row identity".
- CLAUDE.md key patterns: aggregate history contract; groups no longer
  snapshot.
- `docs/ORDERS_LAB.md` and `docs/API_GUIDE.md`.

## Non-goals

- Stable row identity for any derived rows.
- Output changesets for join, projection, or computed views.
- Python API for chaining views over an aggregate.
- Chunked snapshots.
- Changing group ordering. Groups stay in first-appearance order within the
  incremental path; a rebuild may reorder them, and it invalidates history.

## Risks

- **Coordinate bugs in emission.** Mitigated by the replay assertion on
  every fuzzed aggregate, and by `replay` checking deletion payloads and old
  values.
- **Join over aggregate** switches from rebuild to replay. Mitigated by the
  new join-over-aggregate fuzz node.
- **Removal cost:** tail renumbering hashes each subsequent key, while today's
  `retain` compares them. Same O(groups) order, higher constant. If removals
  on high-cardinality groups show up in measurements, a follow-up can defer
  renumbering until the batch ends.
