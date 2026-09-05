# Incremental filter-to-aggregate propagation

`Table -> FilterView -> AggregateView` now maintains small mutation batches
incrementally in Rust and Python. A scalar SUM/COUNT/AVG update inside the
filter uses its emitted old/new values; an update that stays outside the
filter emits nothing and the aggregate's sync does no work.

## Contract

- Every changeset uses its source's own row coordinates. Insertions/deletions
  shift subsequent indices; updates refer to the row at that point in replay.
- `filter_changes.rs` is shared by native filters and Python callbacks. It
  reconstructs each updated row by following later index shifts, using a
  later deletion's row payload when necessary, and undoing later cell updates.
  It copies affected rows only, not a snapshot of the source table.
- The filter emits an insertion on entry, a deletion with the previous row on
  exit, or a cell update for a surviving row. Edits to excluded rows emit no
  output, even when they shift the source indices of surviving rows.
- Small batches (up to 256 source changes) replay. Reconciliation is O(B²) in
  the worst case for B changes, so larger batches explicitly rebuild. Source
  reads/predicate evaluations for replay depend on changed rows, not N.
- Predicates must be pure functions of their row. Python evaluates all
  callbacks before publishing indices/history; a raised exception leaves the
  previous state intact. A callback that mutates its parent is rejected.
- Filters retain the output of one successful non-empty **input** batch. A
  no-op sync preserves that window; an excluded-only input batch retires the
  preceding output. Consumers that fall behind the retained base rebuild.
  Each consumer maintains its own cursor, so one does not drain another's data.
- Refresh, unavailable input history, or a large batch invalidates the filter
  history. Invalidation reserves a sequence position so a consumer at the old
  end cannot mistake a rebuilt result for an unchanged result.
- A filter with an unsynchronized parent temporarily exposes no history. A
  child constructed then must establish a new baseline after parent sync,
  avoiding double-application of changes already visible through live reads.
- Root compaction uses `root_changeset_cursor`, which excludes derived-stream
  cursors. Root and filter sequence numbers are independent.
- Sync parents before children. Auto-registered views follow this order during
  a root mutation's `tick()`. After an explicit filter refresh with no root
  mutation, manually call the child's `sync()`.

Aggregate group-key updates use the same historical-row reconstruction, so
small mixed batches can migrate rows between groups safely. Large group-key
batches retain a rebuild fallback. Unrequested MIN/MAX statistics do not
trigger rescans for SUM/COUNT/AVG-only views.
The integer grouping fast path retains NULL keys, matching incremental
maintenance and the general grouping path.

## Limits

This does not make every operation constant time. Membership changes and
middle insertions/deletions can shift O(N) index bookkeeping. Requested
MIN/MAX can rescan the affected group, and percentile maintenance uses sorted
vectors. The integer-group fast path lazily builds its row-to-group map on
the first incremental update; benchmark warmup includes that cost.

The subsequent [sorted pipeline milestone](INCREMENTAL_SORTED_PIPELINE.md)
adds bounded sorted-coordinate output replay and aggregate index remapping.
The benchmark below records the earlier filter-only milestone, before that
extension. Other output-view types retain version-checked rebuilds for their
children. View versions still include ancestors for
staleness and iterator checks, independently of emitted changes.

The WebSocket server continues to serialize full view snapshots. These
measurements cover the Rust mutation + tick path, not Python callback time,
snapshot serialization, network delivery, or browser rendering.

## Reproducible benchmark

Harness: `impl/examples/filter_pipeline_benchmark.rs`.

```sh
cd impl
cargo run --release --example filter_pipeline_benchmark -- 10000 100000
```

Environment: Apple M2, 24 GiB RAM, macOS 26.5.2 (25F84), aarch64,
`rustc 1.91.1 (ed61e7d7e 2025-11-07)`, release profile, array storage.
Recorded September 4, 2026. Before: `cdf3fc441f18c8948e2e50a24457908c6a5b6072`
with the same added benchmark harness. After:
`f49ea3c0a1205bab1e6f88ae4260145444a8a156` (the original filter milestone).

The schema has Int64 IDs, eight possible string regions, and Float64 amounts.
Half the rows pass the filter, producing four initial groups. Each aggregate
computes SUM(amount). There are five warmup ticks and 31 timed samples per
case; construction, warmup, and a post-tick result read are excluded. Each
sample includes its whole mutation batch and one tick. Median and nearest-rank
p95 are reported in microseconds. This is a warm, deterministic workload with
repeated updates to a small set of rows, not a throughput or general-purpose
dataframe benchmark. Sub-microsecond samples are sensitive to timer resolution.
CPU frequency, core scheduling, and thermal state are not controlled; variation
between these small samples should not be interpreted as a scaling guarantee.

Workloads:

- Included/excluded update: alternate one row's amount without changing its
  filter membership.
- Batch: update 16 qualifying rows, then tick once.
- Mixed batch: update a qualifying row, insert and delete a temporary
  qualifying row in the middle, update an excluded row, then tick once.
- Four consumers: four separate aggregates subscribe to the same filter.

Recorded results (median / p95, microseconds):

| Rows | Workload | Consumers | Before | After |
|---:|---|---:|---:|---:|
| 10,000 | Included update | 1 | 1816.125 / 2467.375 | 1.458 / 3.375 |
| 10,000 | Excluded update | 1 | 1533.542 / 1624.083 | 0.750 / 0.875 |
| 10,000 | Batch of 16 | 1 | 3251.334 / 3382.125 | 11.500 / 11.750 |
| 10,000 | Mixed batch | 1 | 3267.208 / 3348.333 | 705.333 / 836.375 |
| 10,000 | Included update | 4 | 6074.750 / 6169.833 | 1.459 / 1.542 |
| 100,000 | Included update | 1 | 15491.500 / 15755.792 | 0.708 / 1.000 |
| 100,000 | Excluded update | 1 | 15462.167 / 15651.209 | 0.417 / 0.459 |
| 100,000 | Batch of 16 | 1 | 32981.333 / 33303.250 | 8.083 / 8.208 |
| 100,000 | Mixed batch | 1 | 33104.458 / 33398.958 | 6928.125 / 7134.667 |
| 100,000 | Included update | 4 | 63720.875 / 65184.083 | 1.500 / 1.667 |

The scalar cases avoid the former full aggregate rebuild. The mixed case
still scales with row count because insertion/deletion shifts index maps.
These timings accompany deterministic row-read assertions; they are not CI
performance thresholds or claims about unmeasured workloads.

## Validation

`impl/tests/filter_pipeline.rs` checks exact event replay, zero downstream
row reads for scalar/excluded edits, multiple and delayed consumers, bounded
history, invalidation, children created while a parent is stale, nested filters,
and cursor isolation with sorted and joined consumers. It runs in CI.
It also checks nullable integer groups across initial build and incremental
updates; the Python suite verifies their results remain stable across refresh.

`impl/tests/forward_prop_fuzz.rs` additionally compares a direct filter-to-group
chain against fresh recomputation across randomized single and mixed batches,
including all supported aggregate functions. Python tests exercise the same
shared replay via callbacks with both storage backends, NULLs, errors/retry,
explicit refresh, and multiple consumers.
