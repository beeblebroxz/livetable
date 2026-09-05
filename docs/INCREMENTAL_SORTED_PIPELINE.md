# Incremental filter → sort → aggregate propagation

Small batches now propagate through `Table -> FilterView -> SortedView ->
AggregateView` without full re-sorts or aggregate rebuilds. Python exposes
`SortedView.group_by(by, agg)` so the same chain is available through the
simplified API:

```python
filtered = table.filter(lambda r: r["amount"] is not None and r["amount"] >= 5)
ranked = filtered.sort("rank")
totals = ranked.group_by("region", agg=[("total", "amount", "sum")])
table.tick()  # syncs parents before children
```

## Contract and design

- A sort retains cached sort-key columns in parent-row order, its sorted
  permutation, and the inverse permutation. It does not cache full source rows.
  Comparisons during replay use those historical keys, not the live parent's
  already-advanced values. Non-sort cell edits use the inverse index and emit
  a scalar change without scanning source rows or reordering.
- Input batches of up to 256 events replay. Sort-key updates use the shared
  `filter_changes::row_after_update` helper to reconstruct full historical
  payloads across later updates, index shifts, and deletions. Payloads are
  prepared before mutating the cached state. Historical reconciliation is
  O(B²) worst-case for B events; larger batches use a full rebuild.
- All emitted indices are in the sort's own row coordinates at that event.
  An insertion/deletion emits the corresponding sorted-position event. A
  moved row emits `RowDeleted(old position, old full row)` followed by
  `RowInserted(new position, new full row)`. A stationary edit emits
  `CellUpdated`, including edits to non-sort columns. No new public change
  variant or wire message was introduced.
- Equal sort keys preserve parent order. This also holds for duplicate sort
  keys and signed-zero ties. Float NaNs now have a transitive ordering: all
  NaNs tie, after numbers ascending and before numbers descending. Previously
  treating NaN as equal to every number could disagree between rebuild and
  incremental insertion. NULL placement remains independently configurable.
- A sort retains one successful non-empty **input** batch's output (at most
  512 events). No-op sync preserves that history. A filter can advance its
  ancestor version without emitting events; the sort updates its parent-version
  baseline without discarding output or forcing a downstream rebuild.
- Each consumer owns an independent cursor. Lagging consumers rebuild if
  their cursor predates retained history. Explicit refresh, missing history,
  and oversized input batches invalidate output, including for consumers
  previously at the stream's end. A stale sort exposes no coherent changeset;
  children constructed then rebaseline after parent sync.
- Derived cursors never constrain root-table compaction. Parent-before-child
  sync is required. A refresh without a root mutation still requires an
  explicit child `sync()`; root `tick()` does not run without pending mutations.
- Aggregates batch index maintenance for inputs with at least two structural
  events and at most 512 total events. Old rows retain temporary pre-batch
  identities and inserted rows get fresh identities. Aggregate values update
  incrementally; index hashes are remapped once before any MIN/MAX rescan.
  This avoids repeated hash-map rebuilding for each sorted delete/insert pair.
  Unchanged final identity mappings skip even that final remap. Existing
  group-key-update fallbacks remain in place above 256 events.
- Python `ranked.group_by(by, agg)` accepts the same columns and aggregation
  strings as `table.group_by()`. It registers the sort once before its child;
  this also registers an explicitly constructed, previously manual-sync sort.
  `SortedView.sync()` now returns true for applied non-sort cell edits too.

## Costs and limits

For N sorted rows and K keys, permanent sort state is O(NK + N), plus bounded
output history. Cached string keys own their values. Aggregate batch remapping
uses O(N + B) temporary index storage, not a table snapshot. Construction and
initial cache allocation are not measured by the update benchmark below.

This is not an O(1) row-movement implementation. Moving rows, middle insertions,
and deletions still shift vectors and inverse indices; structural batches can
do O(BN) vector work even though aggregate hash remapping happens just once.
Requested MIN/MAX can rescan affected groups, and percentiles maintain sorted
vectors. A sort's 256 input events can produce 512 output events, so a child
filter/sort with the same 256-input bound can legitimately rebuild.

Views other than filters and sorts still expose no output changeset. The
WebSocket server emitted full `ViewData` snapshots at this milestone; the subsequent
[protocol-v3 delivery milestone](PIPELINE_DELIVERY.md) adds bounded base/filter/sort
deltas with snapshot recovery. The measurements below still exclude Python
callbacks, serialization, network delivery, and rendering.

## Reproducible benchmark

Harness: `impl/examples/sorted_pipeline_benchmark.rs`.

```sh
cd impl
cargo run --release --example sorted_pipeline_benchmark -- 10000 100000
```

Recorded September 4, 2026 (America/Los_Angeles), Apple M2, 24 GiB RAM,
macOS 26.5.2 (25F84), arm64, `rustc 1.91.1 (ed61e7d7e 2025-11-07)`,
release profile, array storage. Before: `f49ea3c0a1205bab1e6f88ae4260145444a8a156`
plus the identical new benchmark harness, measured before implementation.
After: `cdbae9bcb3a7c8cac0e620d9b5060c6b3af1d023`, including
batched aggregate index remapping. The final run was isolated from other
agent-started test/build jobs.

Schema: Int64 ID and rank, string region, Float64 amount. Half the source rows
qualify (`amount >= 5`); they sort by ascending rank and SUM(amount) into four
regions. Each case constructs a fresh pipeline. Five warmup ticks precede 31
samples. Each sample measures the entire mutation batch plus one tick, excluding
construction and the post-tick black-box aggregate read. Reported p95 uses the
nearest-rank sample.

Workloads: a qualifying middle-row amount edit; an excluded amount edit; 16
qualifying amount edits; a row moving between first and last; 16 rows moving
between the front and tail; a mixed batch with a qualifying amount edit,
temporary middle insertion/deletion, and excluded edit; and four aggregates
consuming the same sorted scalar update. Values alternate across samples.

Median / p95, microseconds:

| Source rows | Workload | Consumers | Before | After |
|---:|---|---:|---:|---:|
| 10,000 | Non-sort edit | 1 | 1806.542 / 2665.667 | 2.125 / 4.958 |
| 10,000 | Excluded edit | 1 | 1557.416 / 1605.250 | 0.834 / 1.000 |
| 10,000 | 16 non-sort edits | 1 | 1591.958 / 1652.292 | 14.792 / 16.500 |
| 10,000 | One row move | 1 | 1539.375 / 1567.125 | 269.917 / 288.750 |
| 10,000 | 16 row moves | 1 | 1596.500 / 1658.709 | 307.541 / 310.875 |
| 10,000 | Mixed batch | 1 | 1622.125 / 1657.708 | 40.375 / 42.042 |
| 10,000 | Non-sort edit | 4 | 6128.167 / 6325.667 | 1.625 / 1.708 |
| 100,000 | Non-sort edit | 1 | 15617.334 / 16023.416 | 0.833 / 0.875 |
| 100,000 | Excluded edit | 1 | 15602.667 / 15743.917 | 0.500 / 0.583 |
| 100,000 | 16 non-sort edits | 1 | 16178.917 / 17671.792 | 9.583 / 9.708 |
| 100,000 | One row move | 1 | 15672.209 / 15886.625 | 2470.000 / 2590.709 |
| 100,000 | 16 row moves | 1 | 16277.875 / 17130.459 | 3060.542 / 3115.833 |
| 100,000 | Mixed batch | 1 | 16475.292 / 17539.042 | 353.167 / 385.208 |
| 100,000 | Non-sort edit | 4 | 64675.542 / 71799.167 | 1.750 / 1.917 |

This is a warm, deterministic workload repeatedly editing a small row subset,
not a throughput benchmark or a general-purpose dataframe comparison.
Sub-microsecond samples are sensitive to timer resolution; CPU frequency,
scheduling, and thermal state are uncontrolled. Differences between the tiny
scalar samples are not scaling guarantees. CI enforces correctness and source
read counts, not timing thresholds.

## Validation

- `impl/tests/sorted_pipeline.rs`: exact historical event replay, mixed
  updates/inserts/deletes, both directions and NULL placements, ties, duplicate
  keys, NaN/signed zero, 256/257 input boundary, 512 output events, multiple and
  lagging consumers, no-op history, refresh/compaction, stale-child creation,
  nested sorts/filters, and zero source reads for non-sort and aggregate updates.
- `impl/tests/forward_prop_fuzz.rs`: randomized single/mixed batches compare
  each live stage to fresh recomputation, including all aggregate functions.
  Sorted output is also replayed independently, checking every old payload and
  every column, not just sort order and aggregated values. These tests run in CI.
- `tests/python/test_sorted_batches.py`: an independent stable-sort/group model
  on both storage backends with manual sync and tick, history fallbacks, nullable
  integer groups, explicit-constructor registration, and child lifetimes.
- At the recorded implementation commit, verified 198 Rust unit tests, 9 filter contracts, 10 sorted contracts, 6
  randomized tests, 1 real WebSocket integration test, 422 Python tests, and 23
  frontend tests (669 total). Core/server/Python Clippy checks and frontend
  lint/build also passed. These counts record that milestone, not a permanent
  test-suite size; current commands are in the [test guide](../tests/README.md).
