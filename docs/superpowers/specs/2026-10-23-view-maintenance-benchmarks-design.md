# View maintenance benchmarks

**Status:** Implemented 2026-10-23
Plan: `docs/superpowers/plans/2026-10-23-view-maintenance-benchmarks.md`.
Report: `docs/VIEW_MAINTENANCE_BENCHMARK.md` (raw output in
`benchmarks/view_maintenance_2026-10-23.jsonl`). The full run took 178 s, so no
samples were cut. Two-sided variants are reported as workloads
`two_sided_both_then_tick` and `two_sided_tick_each_side`.

## Why

The 2026-10-23 review named two performance suspects that nothing in the
repo measures:

1. **The join's two-sided rebuild.** `JoinView::sync()` rebuilds the whole
   join (O(N+M+R)) when both parents change structure in one sync
   (`impl/src/view/join.rs`, frame-mixing guard). Mutating both joined tables
   before ticking either is enough to trigger it. A rebuild also invalidates
   the join's history, so every view over the join rebuilds too. The only
   numbers are one-off probes in the join output changesets spec, at 100k
   orders: the join's own rebuild took 8.2 ms, and a filter plus a SUM
   aggregate over the join took about 128 ms to rebuild instead of 16 µs to
   replay. The cascade, not the join, is the suspected cost.
2. **O(N) per structural event in sorted and filter views.** Every delete,
   and every insert that is not at the tail, walks the view's whole index to
   shift parent positions (`impl/src/view/sorted.rs` RowInserted/RowDeleted,
   `impl/src/changeset.rs` `apply_filter_row_inserted`). With FastUpdates
   storage the root edit is O(√N), so the views may cancel the benefit.

The spec's builder check found more O(N) work per single event, which the
report must also rank: `AggregateView` rebuilds its `row_to_group` map and
every group's `row_indices` on each structural event
(`adjust_indices_for_insert`/`_delete` in `impl/src/view/aggregate.rs`), and
a one-sided join sync builds a hash lookup over the whole other parent
(`build_left_lookup`/`build_right_lookup`) or a position map over the join
index (`right_position_map`, for right value edits).

The Criterion benches (`impl/benches/livetable_benchmarks.rs`) cover only
sequences and table access. CLAUDE.md requires benchmark claims to come from
checked-in harnesses with a recorded environment. This work produces the
numbers that decide which of the two fixes, if either, comes next.

## Goals

1. A checked-in harness that measures one tick's cost per view type and
   workload, at 10k, 100k and 1M rows, on both storage hints.
2. It separates the root mutation's cost from the tick's (view maintenance)
   cost, so view overhead on top of FastUpdates is visible.
3. It quantifies the two-sided join rebuild, including the forced rebuild of
   views over the join, against the same edits ticked one side at a time.
4. A recorded report with environment provenance, raw output, and a written
   answer to both questions above, ending in a recommendation for what to
   do next.

## Non-goals

- Optimizing anything. Fixes are separate specs, chosen from this report.
- CI performance thresholds (the repo enforces none; keep it that way).
- Python, WebSocket delivery, memory use, and aggregate functions other than
  SUM. MIN/MAX rescans and percentiles are a separate question.
- Replacing the existing filter/sorted pipeline harnesses.

## Anti-goals

- Timings that include table construction, view construction, or snapshot
  copies.
- State that drifts across samples (a table that grows, a sort that moves a
  row further each time), so results depend on sample count.
- Claims beyond the data. Three sizes show a trend, not a proven complexity
  class; say "grows about 10× per 10× rows", not "is O(N)".

## Design

### Harness

New example `impl/examples/view_maintenance_benchmark.rs`, following
`sorted_pipeline_benchmark.rs`. It takes row counts as arguments (default
`10000 100000 1000000`), runs 5 warmups and 31 measured samples per
workload, and prints one JSON line per (size, storage, view, workload) with
`rows`, `storage`, `view`, `workload`, `samples`, `mutate_median_us`,
`tick_median_us`, `tick_p95_us`. Run with:

```bash
cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 10000 100000 1000000
```

(Author's call: an example harness, not Criterion, so results match the
existing pipeline reports' method and print a table directly.)

### Data

- `orders` root, N rows, both storage hints: `id` Int64, `rank` Int64
  (= id), `cust` Int32, `region` String (8 values), `amount` Float64 (half
  the rows 10.0, half 0.0).
- `customers` root, N/10 rows, same storage hint as `orders`: `cid` Int32,
  `name` String. Each order's `cust` is a valid `cid`, so about 10 orders
  per customer.

### Pipelines

Each measured pipeline has exactly one view (or none) registered on its
own `TickableTable`, so a tick's time is that view's maintenance plus
compaction:

| View | Definition |
|---|---|
| `none` | root only: the tick cost floor |
| `filter` | `amount >= 5.0` (about half the rows) |
| `sort` | `rank` ascending |
| `aggregate` | SUM(`amount`) by `region` |
| `join` | `orders` INNER JOIN `customers` on `cust = cid`, registered on both roots |
| `join_chain` | `join`, plus a FilterView (`amount >= 5.0`) and an AggregateView (SUM(`amount`) by `right_name`) over it, registered on both roots after the join |

(Author's call: `join_chain` mirrors the 2026-09-27 probe, so a rebuild's
cascade into children is measured, not only the join's own rebuild.)

Two TickableTables must never share a root, because each compacts to its own
views' minimum cursor. Build each pipeline on a fresh root, or drop the
previous pipeline's views and tickable and clear the root changeset first.

### Workloads

Each sample is timed in two parts: the mutation (`mutate`), then `tick()`
(`tick`). Every workload returns the table to its starting state within a
sample pair, so samples measure the same state.

| Workload | Views | Timed mutation |
|---|---|---|
| `value_edit` | all | `amount` of the middle row to 11.0, then back to 10.0 (stays in the filter) |
| `middle_insert` / `middle_delete` | all | insert a passing row at parent index N/2 with `rank` N/2, so it also lands mid-sort; then delete it. Each is its own sample |
| `tail_append` / `tail_delete` | all | append a passing row with `rank` N, so it lands at the sort's tail (the append fast path); then delete the last row |
| `sort_key_move` | sort | `rank` of the middle row to -1 (moves to the top), then back |
| `right_value_edit` | join, join_chain | `name` of the middle customer to an alternate name, then back. The output fans out to about 10 rows, but the sync also builds a position map over the whole join index |
| `right_insert` / `right_delete` | join, join_chain | append a customer with no orders, then delete it (a one-sided right structural sync) |
| `two_sided` | join, join_chain | see below |

Inserted and appended rows use a new unique `id`, an existing middle
customer's `cust`, `region` "region0", and `amount` 10.0. The two insert
workloads deliberately differ: `middle_insert` is the general case, and
`tail_append` is the append-only best case.

**Two-sided join workload.** Each sample adds a new customer and that
customer's first order (inserted at N/2), then ticks both roots. Report two
variants of the same edits:

- `both_then_tick`: insert both rows, then tick `orders`, then tick
  `customers`. The join sees both sides' structural changes in one sync and
  rebuilds.
- `tick_each_side`: insert the order, tick `orders`; insert the customer,
  tick `customers`.

Report each tick separately (`tick_orders_median_us`,
`tick_customers_median_us`) as well as their sum. Both variants include
full-parent hash lookups, so `tick_each_side` is the incremental baseline,
not a near-free one; the per-tick split shows how much of each variant is
the rebuild and how much the lookups. The untimed cleanup deletes the two
rows one side at a time, ticking after each, so cleanup does not rebuild.

Check in the harness, outside the timing, that `both_then_tick` invalidated
the join's history and `tick_each_side` did not, so the report measures what
it claims. Read the join's cursor before the inserts, because
`JoinView::changeset()` returns None while a parent has unsynced mutations,
and check each tick of `tick_each_side` against its own pre-tick cursor
(`changeset().changes_from(cursor)` returns None after an invalidation).

### Report

New report `docs/VIEW_MAINTENANCE_BENCHMARK.md`, linked from the benchmark
surfaces in `docs/PERFORMANCE_COMPARISON.md`. It contains:

- The run command, and the environment in the format `PERFORMANCE_COMPARISON.md`
  asks for: commit, CPU, memory, OS, Rust toolchain, and whether anything
  else was running.
- Result tables (median tick, p95 tick, median mutate) grouped by view.
- The raw JSON lines, committed as `benchmarks/view_maintenance_2026-10-23.jsonl`
  (author's call, so the numbers stay reproducible from retained output).
- Answers:
  1. `both_then_tick` against `tick_each_side` at each size, as a ratio and
     in absolute time.
  2. How `middle_*` and `tail_*` tick costs for `filter`, `sort` and
     `aggregate` grow from 10k to 1M on each storage hint, against
     `value_edit` and the `none` floor, and whether FastUpdates' root
     advantage survives the views.
  3. How much of a one-sided join sync is the full-parent lookup or
     position-map build (`middle_insert`, `right_insert`, `right_value_edit`
     growth with N).
- A recommendation ranking every candidate the data shows (the two-sided
  rebuild cascade, per-event index shifting in filter/sort, aggregate
  re-indexing, join lookups), with the numbers each rests on. The decision
  is the user's; the report states the trade-offs.

## Testing and acceptance

- The harness asserts, outside the timing, that each pipeline's views still
  match their expected row counts after every sample pair (including
  `join_chain`'s filter and aggregate), and that the two `two_sided`
  variants did and did not rebuild. A run that trips an assertion produces
  no report.
- `cargo clippy --all-targets --features server -- -D warnings` passes (the
  example builds under `--all-targets`).
- A 10k-only run finishes in seconds and is shown in the report's
  reproduction section. The full 10k/100k/1M run is recorded once, on an
  otherwise idle machine, with its wall time.

## Docs

`docs/PERFORMANCE_COMPARISON.md` (benchmark surfaces and the incremental
pipeline commands) and the new report. Under the `feature-docs` skill
nothing else changes, since this adds no API.

## Risks

- **1M-row runs are slow.** Building the 1M-row tables and views is likely
  to dominate, along with `join_chain` rebuild cascades. If the full run
  takes over 15 minutes, cut the slowest workloads' samples at 1M (never
  below 11 measured) and say so in the report.
- **Noise** from an uncontrolled laptop (thermal, scheduling). Report p95
  beside the median, and record that frequency and thermal state were
  uncontrolled, as earlier reports did.
