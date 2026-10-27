# View maintenance benchmark

What one `tick()` costs each view type after a single edit, insert or delete,
at 10k, 100k and 1M rows on both storage hints. It also covers a join whose two
parents change in the same tick. Design:
[2026-10-23 spec](superpowers/specs/2026-10-23-view-maintenance-benchmarks-design.md).
Harness: [`impl/examples/view_maintenance_benchmark.rs`](../impl/examples/view_maintenance_benchmark.rs).
Raw output: [`benchmarks/view_maintenance_2026-10-23.jsonl`](../benchmarks/view_maintenance_2026-10-23.jsonl).

These are measurements of one machine, commit and workload, not guarantees.

## Findings

All figures are FastReads medians at 1M orders unless stated otherwise.
FastUpdates tick costs match FastReads to within about 10% everywhere.

1. **Two-sided join rebuild.** Adding a customer and that customer's first
   order, then ticking both tables:

   | Pipeline | Ticked together (rebuild) | One side at a time | Ratio |
   |---|---:|---:|---:|
   | join alone, 10k | 683 µs | 625 µs | 1.1× |
   | join alone, 100k | 7.2 ms | 6.4 ms | 1.1× |
   | join alone, 1M | 82.0 ms | 75.9 ms | 1.1× |
   | join + filter + SUM over it, 10k | 9.1 ms | 0.95 ms | 9.6× |
   | join + filter + SUM over it, 100k | 95.2 ms | 9.6 ms | 9.9× |
   | join + filter + SUM over it, 1M | 1,027 ms | 122 ms | 8.4× |

   - **The join's own rebuild costs little extra.** The one-sided path is
     already expensive, because the customer insert alone builds a lookup
     over all orders (66 ms of the 75.9 ms).
   - **The views over the join are the real cost.** The rebuild invalidates
     the join's history, so the filter and aggregate over it rebuild too.
     That adds about 0.9 s at 1M.
2. **Per-event cost in filter, sort and aggregate grows about 10× per 10×
   rows** for every structural event, except a tail append in filter and
   sort:

   | View | Middle insert | Middle delete | Tail delete | Tail append |
   |---|---:|---:|---:|---:|
   | filter | 214 µs | 344 µs | 294 µs | 0.54 µs |
   | sort | 930 µs | 942 µs | 352 µs | 0.75 µs |
   | aggregate (SUM by 8 regions) | 90.3 ms | 88.6 ms | 85.4 ms | 2.6 ms |

   - **Aggregate is about 95× worse than sort.** A middle insert and any
     delete rebuild its row-to-group map and every group's row list, so each
     costs about 90 ms, even with only 8 groups.
   - **Aggregate tail appends are not fast either:** 22.6 µs, then 165 µs,
     then 2.6 ms. The append path scans every row's entry for the largest
     index (`row_to_group.keys().max()`).
   - **Value edits stay flat.** They cost 0.1–0.8 µs at every size.
   - **Only an append to the end is fast, and only in filter and sort.**
     They skip the shift for a tail append, but not for a tail delete.
   - **The views shrink FastUpdates' advantage.** At 1M, a middle insert
     costs the root itself 14.5 µs on FastUpdates against 508 µs on
     FastReads, 35× less. The views' ticks are the same on both, so the
     totals (mutation plus tick) are much closer. Filter keeps a 3.2×
     advantage, sort keeps 1.6×, and aggregate keeps none:

     | View | FastUpdates total | FastReads total | Advantage |
     |---|---:|---:|---:|
     | filter | 225 µs | 722 µs | 3.2× |
     | sort | 923 µs | 1,476 µs | 1.6× |
     | aggregate | about 90 ms | about 90 ms | none |
3. **A one-sided join sync hashes the whole other table:**

   | Edit on one table | Join tick at 1M | Cost comes from |
   |---|---:|---:|
   | Customer insert (no orders) | 64.1 ms | a lookup over 1M orders |
   | Customer name edit (fans out to about 10 rows) | 31.3 ms | a position map over the 1M-row join output |
   | Order insert, middle | 11.5 ms | a lookup over 100k customers (about 9 ms), plus shifting the join index (about 2.4 ms) |
   | Order insert, tail | 9.1 ms | a lookup over 100k customers |
   | Order delete, middle | 1.0 ms | shifting the join index; no lookup |
   | Order delete, tail | 0.59 ms | shifting the join index; no lookup |
   | Customer delete | 1.8 ms | shifting the join index; no lookup |

   - **Growth:** each figure grows about 9–13× per 10× rows.
   - **Lookups are only part of it.** Every structural sync also makes at
     least one pass over the join index to shift positions. The deletes show
     that residual cost: 0.6–1.8 ms at 1M.

## Recommendation

Item 1 was implemented on 2026-10-26; see
[After the aggregate fix](#after-the-aggregate-fix-2026-10-26).

**Workload assumption.** The ranking below assumes a mixed workload: rows
inserted and deleted anywhere in the table, on pipelines like the ones
measured here. That assumption is a judgment, not something this benchmark
measured, and the order changes for other workloads (see the end of this
section).

1. **Aggregate re-indexing.** About 90 ms per middle insert or any delete at
   1M (7 ms at 100k), and 2.6 ms per tail append.
   - It hits every aggregate over a table that changes structure, including
     aggregates downstream of filters and joins. The join + filter + SUM
     pipeline's 56 ms middle insert is mostly this; 11.5 ms of it is the
     join.
   - The fix is contained in `impl/src/view/aggregate.rs`, which the
     aggregate contract tests and two fuzz drivers cover. It would replace
     both the row-to-group map and each group's `row_indices` set (used by
     MIN/MAX rescans and the empty-group check) with position-indexed
     structures shifted the way filters shift their index. It would also
     drop the tail-append key scan.
   - The expected result is sub-millisecond events at 1M, like filter and
     sort. That is a prediction to measure, not a result.
2. **Join key lookups.** 9–64 ms per one-sided insert at 1M, and 31 ms per
   customer value edit.
   - A key index kept across syncs, instead of a lookup built on every
     insert, would remove the lookup part: about 9 ms of an order insert and
     64 ms of a customer insert.
   - It would not remove the 0.6–1.8 ms of shifting the join index that
     every structural sync pays.
   - The index must not itself shift positions on every insert or delete,
     or it repeats item 1's problem. It needs stable row identities, or an
     equally cheap shifting scheme.
   - It also costs memory for two indexes, and it touches the most
     intricate view code. The benefit is a prediction, conditional on that
     design.
3. **Two-sided rebuild.** About 0.9 s extra at 1M, 88% of that pipeline's
   cost each time it happens, when views sit over the join.
   - **When it happens:** both parents have pending structural changes
     (an insert, a delete or a join-key update) at the moment the join
     syncs. For example, both tables are mutated before either is ticked. A
     value edit on one side does not trigger it.
   - **How often is unmeasured.** The server's pipeline specs build no
     joins, so today it arises only through the Rust and Python APIs.
   - **The one-sided path still costs 122 ms.** About 74 ms of that is the
     join's lookups (item 2), and about 46 ms is the aggregate's
     re-indexing (item 1). So fixing this alone helps most when items 1 and
     2 are fixed too.
4. **Filter and sort index shifting.** 0.2–0.9 ms per event at 1M.
   - Real and linear, but the cheapest in absolute terms here. It mainly
     erodes FastUpdates' advantage rather than causing visible stalls.
   - Fixing it is a redesign of positional indexes. Defer it until the costs
     above it are gone.

**Other workloads.**
- **Append-only streams (tail appends only):** join lookups (9.1 ms per
  order append) outrank the aggregate (2.6 ms), and filter and sort cost
  under 1 µs. The order becomes 2, 1, then 3 if both joined tables change
  between ticks.
- **Both joined tables often change between ticks:** item 3 moves to the
  top, since fixing it alone removes about 88% of that pipeline's cost each
  time it happens.

Which to take on, if any, is the user's decision. For the mixed workload,
item 1 has the best ratio of benefit to risk on these numbers.

## After the aggregate fix (2026-10-26)

Design:
[2026-10-26 spec](superpowers/specs/2026-10-26-aggregate-reindexing-design.md).

**What changed.** `AggregateView` now maps rows to groups with a vector
indexed by position, shifted with one memmove per insert or delete, instead
of rehashing two position-keyed hashes. MIN/MAX keep an ordered value
multiset per group, so removing an extreme no longer rescans the source.

### Budgets

All at 1M rows. Where two values are given, they are FastReads / FastUpdates.

| Measure | Budget | Before | After |
|---|---|---:|---:|
| Aggregate middle insert tick | under 2 ms | 91.0 / 93.4 ms | 79.0 / 73.6 µs |
| Aggregate middle delete tick | under 2 ms | 89.6 / 90.4 ms | 77.5 / 72.9 µs |
| Aggregate tail delete tick | under 2 ms | 86.9 / 85.9 ms | 1.04 / 1.04 µs |
| Aggregate tail append tick | under 50 µs | 2,629 / 2,603 µs | 1.17 / 1.12 µs |
| MIN/MAX value edit that misses the extreme | under 5 µs | 0.79 µs | 0.92 µs |
| MIN/MAX value edit that removes the minimum | recorded | 1,250 µs | 0.88 µs |
| Construction, SUM by `region` (String) | at most 1.10× | 330.6 ms | 198.9 ms (0.60×) |
| Construction, SUM by `bucket` (Int32 fast path) | at most 1.10× | 121.5 ms | 62.0 ms (0.51×) |
| Construction, MIN+MAX by `region` | at most 2× | 332.0 ms | 290.1 ms (0.87×) |
| Construction, MIN+MAX by `bucket` | at most 2× | 122.4 ms | 154.4 ms (1.26×) |

- **Where the numbers come from:**
  - The tick rows come from the harness.
  - The value-edit and construction rows come from a throwaway timing
    program, not committed. It used 1M rows, `amount` =
    `(i * 7919) % 100_000`, 8 regions or buckets, the median of 5
    constructions, and 5 warmups plus 31 samples per edit.
- **Every budget is met.** No bulk-build of the MIN/MAX multiset was needed.
- **A middle insert or delete** now grows about 3.6× then 9.3× per 10× rows
  (2.3, 8.5 and 79 µs on FastReads): the memmove of the position vector.
  Tail events stay flat at about 1 µs.

### Downstream

The join + filter + SUM pipeline (`join_chain`) improves wherever the
aggregate's re-indexing had dominated. At 1M rows on FastReads:

| Workload | Before | After |
|---|---:|---:|
| Middle delete | 52.5 ms | 1.4 ms |
| Tail delete | 48.3 ms | 0.9 ms |
| Middle insert | 67.0 ms | 11.6 ms |
| Two-sided, one side at a time | 145 ms | 76 ms |
| Two-sided, ticked together | 1,175 ms | 954 ms |

- **Middle insert:** what remains is the join's own lookup and shifting.
- **Ticked together:** the forced rebuild of the views over the join
  remains.
- **Every other view is unchanged:** its after/before tick ratio is at most
  1.04× for ticks of 1 µs or more.

**What is left.** Of the candidates in the Recommendation:
- The join's key lookups are now the largest per-event cost: 9–68 ms per
  one-sided insert, and 31 ms per customer value edit.
- The two-sided rebuild costs about 0.9 s with views over the join.
- Filter and sort index shifting costs 0.2–1 ms.
- The order between them, and whether to pursue any, is the user's
  decision.

### Environment and noise

- **Machine:** Apple M2, 24 GiB RAM, 8 cores.
- **OS and toolchain:** macOS 27.2 (26B123), rustc 1.91.1 (ed61e7d7e
  2025-11-07), release build.
- **Before run:** commit `a6e860fe07dda39cff588f983f7086a499e14016`.
- **After run:** commit `7c360546b7402ff50494b2bf62d69097045544a2`.
- **Comparability:** the OS differs from the 2026-10-23 run, so compare only
  these two runs with each other.
- **Wall time:** the before run took 201 s and the after run 479 s. The extra
  time was machine load: a timestamped rerun of the after code's 1M sizes
  took 127 s, its usual share, and its tick medians matched.
- **Noisy cells:** a few cells in the before run look noisy, for example
  `join_chain` at 100k on FastReads (middle insert 15.8 ms against 4.2 ms on
  FastUpdates). The aggregate's structural ticks changed by 17× (a tail append
  at 10k) to 80,000× (a tail delete at 1M), far outside that noise.
- **Raw output:**
  [`view_maintenance_2026-10-26-before.jsonl`](../benchmarks/view_maintenance_2026-10-26-before.jsonl)
  and
  [`view_maintenance_2026-10-26-after.jsonl`](../benchmarks/view_maintenance_2026-10-26-after.jsonl).

#### `aggregate` tick median, before → after

| Workload | Rows | FastReads before | after | FastUpdates before | after |
|---|---:|---:|---:|---:|---:|
| value_edit | 10k | 1.04 | 0.79 | 0.75 | 0.67 |
| value_edit | 100k | 0.75 | 0.67 | 1.00 | 0.67 |
| value_edit | 1M | 0.75 | 0.71 | 0.75 | 0.71 |
| middle_insert | 10k | 707 | 2.33 | 655 | 1.71 |
| middle_insert | 100k | 6,972 | 8.50 | 6,940 | 8.08 |
| middle_insert | 1M | 91.0 ms | 79.0 | 93.4 ms | 73.6 |
| middle_delete | 10k | 680 | 2.21 | 649 | 1.58 |
| middle_delete | 100k | 6,780 | 8.42 | 6,786 | 7.96 |
| middle_delete | 1M | 89.6 ms | 77.5 | 90.4 ms | 72.9 |
| tail_append | 10k | 22.5 | 1.29 | 22.4 | 1.12 |
| tail_append | 100k | 168 | 1.12 | 257 | 1.12 |
| tail_append | 1M | 2,629 | 1.17 | 2,603 | 1.12 |
| tail_delete | 10k | 639 | 1.17 | 646 | 1.04 |
| tail_delete | 100k | 6,704 | 1.04 | 8,901 | 1.04 |
| tail_delete | 1M | 86.9 ms | 1.04 | 85.9 ms | 1.04 |

#### `join_chain` tick median, before → after

| Workload | Rows | FastReads before | after | FastUpdates before | after |
|---|---:|---:|---:|---:|---:|
| value_edit | 10k | 1.62 | 1.58 | 1.62 | 1.58 |
| value_edit | 100k | 1.83 | 1.58 | 1.71 | 1.58 |
| value_edit | 1M | 1.92 | 1.58 | 1.75 | 1.75 |
| middle_insert | 10k | 424 | 112 | 418 | 115 |
| middle_insert | 100k | 15.8 ms | 984 | 4,195 | 990 |
| middle_insert | 1M | 67.0 ms | 11.6 ms | 64.7 ms | 11.7 ms |
| middle_delete | 10k | 330 | 14.8 | 322 | 15.1 |
| middle_delete | 100k | 11.9 ms | 121 | 3,205 | 120 |
| middle_delete | 1M | 52.5 ms | 1,377 | 50.7 ms | 1,370 |
| tail_append | 10k | 109 | 103 | 112 | 103 |
| tail_append | 100k | 1,561 | 876 | 980 | 887 |
| tail_append | 1M | 12.9 ms | 9,151 | 14.7 ms | 9,862 |
| tail_delete | 10k | 308 | 10.3 | 311 | 10.2 |
| tail_delete | 100k | 5,289 | 77.3 | 3,245 | 77.1 |
| tail_delete | 1M | 48.3 ms | 903 | 49.9 ms | 946 |
| right_value_edit | 10k | 242 | 245 | 247 | 246 |
| right_value_edit | 100k | 3,724 | 2,484 | 2,790 | 2,489 |
| right_value_edit | 1M | 35.9 ms | 31.3 ms | 35.8 ms | 33.5 ms |
| right_insert | 10k | 535 | 533 | 539 | 546 |
| right_insert | 100k | 16.3 ms | 5,448 | 5,556 | 5,651 |
| right_insert | 1M | 75.7 ms | 62.6 ms | 64.6 ms | 67.7 ms |
| right_delete | 10k | 15.2 | 15.2 | 15.2 | 15.2 |
| right_delete | 100k | 342 | 149 | 152 | 160 |
| right_delete | 1M | 1,966 | 1,816 | 1,876 | 1,842 |
| two_sided_both_then_tick | 10k | 9,427 | 8,897 | 9,486 | 9,065 |
| two_sided_both_then_tick | 100k | 104.0 ms | 90.0 ms | 97.8 ms | 91.7 ms |
| two_sided_both_then_tick | 1M | 1,174.9 ms | 953.7 ms | 1,091.4 ms | 1,028.3 ms |
| two_sided_tick_each_side | 10k | 967 | 642 | 961 | 660 |
| two_sided_tick_each_side | 100k | 10.0 ms | 6,402 | 9,888 | 6,524 |
| two_sided_tick_each_side | 1M | 145.0 ms | 76.0 ms | 130.4 ms | 81.2 ms |


## Method

- **Pipelines.** Each runs on fresh root tables, with exactly one view (or
  none) registered on its own `TickableTable`:
  - `orders` has N rows: `id` = `rank` = i, ten orders per customer, and
    every other row has `amount` 10.0.
  - `customers` has N/10 rows and uses the same storage hint.
  - `join` is `orders` INNER JOIN `customers`.
  - `join_chain` is the join plus a filter (`amount >= 5.0`) and a SUM
    aggregate by `right_name` over it, registered on both roots.
- **Workloads.** Each one restores its starting state within a sample pair:
  - `value_edit`: the middle row's `amount`, 10.0 → 11.0.
  - `middle_insert` / `middle_delete`: parent index N/2, landing mid-sort.
  - `tail_append` / `tail_delete`: landing at the sort's tail.
  - `sort_key_move`: the middle row to rank -1.
  - `right_*`: edits, inserts and deletes on `customers`.
  - `two_sided_*`: a new customer plus its first order.
- **Sampling.** 5 warmups and 31 measured samples per workload. The mutation
  and `tick()` are timed separately, as medians, with a nearest-rank p95 for
  the tick. Construction and the harness's checks are excluded from timing.
- **Built-in checks.** The harness asserts three things:
  - after every sample pair, view lengths have not drifted;
  - after every timed tick, each view kept its history rather than silently
    rebuilding;
  - `both_then_tick` rebuilt the join, while `tick_each_side` kept its
    history.

  Deliberately broken workloads trip each check. The recorded run predates
  the per-tick history check. A reviewer's instrumented copy found no
  rebuilds across 12,960 view-ticks at 1k–100k, and a validation rerun at 1M
  with the check (output discarded) found none either.
- **Timer.** The macOS timer's resolution is about 42 ns, so values under
  0.1 µs are a tick or two of noise.

## Environment

Recorded 2026-10-23 (America/Los_Angeles):
- **Commit:** `e2e7cb8a050d129eed3592fd44b4ad5b9eced602`
- **Machine:** Apple M2, 24 GiB RAM, 8 cores
- **OS:** macOS 27.0.1 (26A434), arm64
- **Toolchain:** rustc 1.91.1 (ed61e7d7e 2025-11-07), release build
- **Load:** no other builds or tests were running. CPU frequency, thermal
  state and scheduling were uncontrolled.
- **Wall time:** 178 s for the full 10k/100k/1M run.

## Reproduce

```bash
# about 2 s
cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 10000
# full run, about 3 minutes
cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 10000 100000 1000000
```

The harness refuses debug builds. Progress goes to stderr and JSON lines to
stdout. Rerunning on another revision or machine is a new measurement.

## Results

Times are µs unless marked ms. "tick" is the median tick, "p95" the
nearest-rank 95th percentile tick, and "mutate" the median root mutation.

#### `none`

| Workload | Rows | FastReads tick | p95 | mutate | FastUpdates tick | p95 | mutate |
|---|---:|---:|---:|---:|---:|---:|---:|
| value_edit | 10k | 0.04 | 0.04 | 0.04 | 0.04 | 0.04 | 0.04 |
| value_edit | 100k | 0.04 | 0.04 | 0.04 | 0.04 | 0.04 | 0.04 |
| value_edit | 1M | 0.04 | 0.04 | 0.04 | 0.04 | 0.04 | 0.04 |
| middle_insert | 10k | 0.17 | 0.17 | 6.33 | 0.08 | 0.08 | 0.88 |
| middle_insert | 100k | 0.08 | 0.12 | 45.5 | 0.08 | 0.08 | 1.83 |
| middle_insert | 1M | 0.25 | 0.50 | 490 | 0.12 | 0.12 | 14.2 |
| middle_delete | 10k | 0.08 | 0.12 | 6.25 | 0.08 | 0.08 | 0.88 |
| middle_delete | 100k | 0.08 | 0.08 | 45.5 | 0.08 | 0.08 | 2.04 |
| middle_delete | 1M | 0.08 | 0.12 | 487 | 0.08 | 0.08 | 14.2 |
| tail_append | 10k | 0.08 | 0.12 | 0.46 | 0.08 | 0.08 | 0.38 |
| tail_append | 100k | 0.08 | 0.08 | 0.33 | 0.08 | 0.08 | 0.38 |
| tail_append | 1M | 0.08 | 0.08 | 0.38 | 0.08 | 0.08 | 0.38 |
| tail_delete | 10k | 0.08 | 0.08 | 0.42 | 0.08 | 0.08 | 0.38 |
| tail_delete | 100k | 0.08 | 0.08 | 0.33 | 0.08 | 0.08 | 0.38 |
| tail_delete | 1M | 0.08 | 0.08 | 0.38 | 0.08 | 0.08 | 0.38 |

#### `filter`

| Workload | Rows | FastReads tick | p95 | mutate | FastUpdates tick | p95 | mutate |
|---|---:|---:|---:|---:|---:|---:|---:|
| value_edit | 10k | 0.42 | 0.50 | 0.04 | 0.38 | 0.38 | 0.04 |
| value_edit | 100k | 0.38 | 0.50 | 0.04 | 0.38 | 0.42 | 0.04 |
| value_edit | 1M | 0.38 | 0.42 | 0.04 | 0.38 | 0.42 | 0.04 |
| middle_insert | 10k | 2.67 | 2.83 | 5.58 | 2.71 | 2.96 | 0.88 |
| middle_insert | 100k | 19.8 | 19.8 | 46.8 | 20.7 | 20.8 | 1.88 |
| middle_insert | 1M | 214 | 236 | 508 | 210 | 214 | 14.5 |
| middle_delete | 10k | 4.12 | 4.21 | 5.50 | 4.00 | 4.08 | 0.88 |
| middle_delete | 100k | 34.0 | 34.3 | 46.8 | 34.6 | 34.6 | 2.00 |
| middle_delete | 1M | 344 | 373 | 504 | 342 | 347 | 14.6 |
| tail_append | 10k | 0.50 | 0.54 | 0.38 | 0.46 | 0.50 | 0.38 |
| tail_append | 100k | 0.50 | 0.54 | 0.38 | 0.54 | 0.58 | 0.38 |
| tail_append | 1M | 0.54 | 0.58 | 0.42 | 0.54 | 0.58 | 0.42 |
| tail_delete | 10k | 3.54 | 3.96 | 0.38 | 3.33 | 3.38 | 0.38 |
| tail_delete | 100k | 29.9 | 30.0 | 0.38 | 29.9 | 30.0 | 0.38 |
| tail_delete | 1M | 294 | 302 | 0.38 | 294 | 297 | 0.38 |

#### `sort`

| Workload | Rows | FastReads tick | p95 | mutate | FastUpdates tick | p95 | mutate |
|---|---:|---:|---:|---:|---:|---:|---:|
| value_edit | 10k | 0.12 | 0.12 | 0.04 | 0.08 | 0.12 | 0.04 |
| value_edit | 100k | 0.12 | 0.12 | 0.04 | 0.12 | 0.12 | 0.04 |
| value_edit | 1M | 0.12 | 0.12 | 0.04 | 0.12 | 0.12 | 0.04 |
| middle_insert | 10k | 9.17 | 9.42 | 5.12 | 10.1 | 10.3 | 1.00 |
| middle_insert | 100k | 88.3 | 91.1 | 47.2 | 89.5 | 92.7 | 1.83 |
| middle_insert | 1M | 930 | 970 | 546 | 907 | 912 | 15.6 |
| middle_delete | 10k | 8.92 | 9.00 | 5.08 | 8.46 | 8.54 | 1.04 |
| middle_delete | 100k | 86.2 | 86.5 | 47.1 | 86.2 | 86.8 | 1.92 |
| middle_delete | 1M | 942 | 991 | 557 | 900 | 912 | 15.7 |
| tail_append | 10k | 0.54 | 0.58 | 0.33 | 0.54 | 0.58 | 0.38 |
| tail_append | 100k | 0.62 | 0.67 | 0.38 | 0.67 | 0.71 | 0.38 |
| tail_append | 1M | 0.75 | 0.83 | 0.42 | 0.75 | 0.79 | 0.42 |
| tail_delete | 10k | 3.88 | 3.92 | 0.38 | 3.88 | 3.96 | 0.38 |
| tail_delete | 100k | 34.6 | 35.3 | 0.38 | 35.6 | 35.8 | 0.38 |
| tail_delete | 1M | 352 | 354 | 0.38 | 352 | 355 | 0.42 |
| sort_key_move | 10k | 4.50 | 6.21 | 0.04 | 4.50 | 4.54 | 0.04 |
| sort_key_move | 100k | 43.8 | 44.1 | 0.04 | 45.2 | 45.7 | 0.04 |
| sort_key_move | 1M | 452 | 455 | 0.04 | 455 | 458 | 0.04 |

#### `aggregate`

| Workload | Rows | FastReads tick | p95 | mutate | FastUpdates tick | p95 | mutate |
|---|---:|---:|---:|---:|---:|---:|---:|
| value_edit | 10k | 0.79 | 0.88 | 0.04 | 0.75 | 0.79 | 0.04 |
| value_edit | 100k | 0.75 | 0.79 | 0.04 | 0.75 | 0.79 | 0.04 |
| value_edit | 1M | 0.79 | 0.83 | 0.04 | 0.75 | 0.83 | 0.04 |
| middle_insert | 10k | 662 | 698 | 5.21 | 665 | 706 | 1.12 |
| middle_insert | 100k | 6,791 | 7,118 | 47.4 | 6,920 | 7,129 | 2.50 |
| middle_insert | 1M | 90.3 ms | 91.1 ms | 654 | 90.3 ms | 91.2 ms | 47.8 |
| middle_delete | 10k | 651 | 691 | 5.17 | 649 | 702 | 1.12 |
| middle_delete | 100k | 6,708 | 6,871 | 47.2 | 6,790 | 7,170 | 2.58 |
| middle_delete | 1M | 88.6 ms | 89.6 ms | 655 | 88.7 ms | 89.6 ms | 50.0 |
| tail_append | 10k | 22.6 | 23.8 | 0.46 | 22.4 | 25.8 | 0.46 |
| tail_append | 100k | 165 | 180 | 0.54 | 169 | 176 | 0.54 |
| tail_append | 1M | 2,594 | 2,619 | 1.92 | 2,596 | 2,618 | 2.29 |
| tail_delete | 10k | 648 | 712 | 0.42 | 654 | 681 | 0.46 |
| tail_delete | 100k | 6,596 | 6,853 | 0.46 | 6,786 | 7,007 | 0.46 |
| tail_delete | 1M | 85.4 ms | 86.3 ms | 1.25 | 84.8 ms | 86.4 ms | 1.33 |

#### `join`

| Workload | Rows | FastReads tick | p95 | mutate | FastUpdates tick | p95 | mutate |
|---|---:|---:|---:|---:|---:|---:|---:|
| value_edit | 10k | 0.21 | 0.21 | 0.04 | 0.17 | 0.21 | 0.04 |
| value_edit | 100k | 0.17 | 0.21 | 0.04 | 0.21 | 0.21 | 0.04 |
| value_edit | 1M | 0.21 | 0.25 | 0.04 | 0.21 | 0.21 | 0.04 |
| middle_insert | 10k | 104 | 107 | 5.17 | 105 | 112 | 1.04 |
| middle_insert | 100k | 958 | 1,019 | 47.9 | 962 | 994 | 1.92 |
| middle_insert | 1M | 11.5 ms | 11.8 ms | 649 | 11.2 ms | 11.5 ms | 32.2 |
| middle_delete | 10k | 9.12 | 9.21 | 5.04 | 9.00 | 9.54 | 1.04 |
| middle_delete | 100k | 80.7 | 84.4 | 47.7 | 80.6 | 83.1 | 2.00 |
| middle_delete | 1M | 1,002 | 1,028 | 627 | 905 | 934 | 39.6 |
| tail_append | 10k | 95.7 | 109 | 0.42 | 96.0 | 113 | 0.38 |
| tail_append | 100k | 880 | 910 | 0.42 | 884 | 920 | 0.42 |
| tail_append | 1M | 9,068 | 9,273 | 0.50 | 9,195 | 9,333 | 0.62 |
| tail_delete | 10k | 5.33 | 5.46 | 0.42 | 5.33 | 5.71 | 0.42 |
| tail_delete | 100k | 46.0 | 48.5 | 0.42 | 46.2 | 49.2 | 0.42 |
| tail_delete | 1M | 591 | 606 | 0.67 | 581 | 602 | 0.71 |
| right_value_edit | 10k | 215 | 272 | 0.08 | 216 | 282 | 0.08 |
| right_value_edit | 100k | 2,690 | 2,859 | 0.08 | 2,600 | 2,798 | 0.08 |
| right_value_edit | 1M | 31.3 ms | 32.0 ms | 0.25 | 31.1 ms | 31.7 ms | 0.38 |
| right_insert | 10k | 519 | 524 | 0.21 | 526 | 549 | 0.17 |
| right_insert | 100k | 5,438 | 5,810 | 0.21 | 5,509 | 5,606 | 0.21 |
| right_insert | 1M | 64.1 ms | 67.5 ms | 0.67 | 64.2 ms | 64.7 ms | 0.75 |
| right_delete | 10k | 14.8 | 14.9 | 0.25 | 14.8 | 15.0 | 0.21 |
| right_delete | 100k | 149 | 176 | 0.25 | 149 | 165 | 0.25 |
| right_delete | 1M | 1,820 | 1,862 | 1.17 | 1,814 | 1,864 | 1.25 |
| two_sided_both_then_tick | 10k | 683 | 705 | 5.33 | 689 | 752 | 1.17 |
| two_sided_both_then_tick | 100k | 7,204 | 7,722 | 48.1 | 7,064 | 7,681 | 2.08 |
| two_sided_both_then_tick | 1M | 82.0 ms | 85.2 ms | 557 | 82.5 ms | 84.2 ms | 34.2 |
| two_sided_tick_each_side | 10k | 625 | 678 | 5.33 | 644 | 679 | 1.21 |
| two_sided_tick_each_side | 100k | 6,429 | 6,788 | 48.2 | 6,458 | 6,633 | 2.04 |
| two_sided_tick_each_side | 1M | 75.9 ms | 76.4 ms | 568 | 75.7 ms | 76.5 ms | 34.4 |

#### `join_chain`

| Workload | Rows | FastReads tick | p95 | mutate | FastUpdates tick | p95 | mutate |
|---|---:|---:|---:|---:|---:|---:|---:|
| value_edit | 10k | 1.67 | 1.75 | 0.04 | 1.67 | 1.75 | 0.04 |
| value_edit | 100k | 1.71 | 1.75 | 0.04 | 1.67 | 1.79 | 0.04 |
| value_edit | 1M | 1.71 | 1.79 | 0.04 | 1.71 | 1.79 | 0.04 |
| middle_insert | 10k | 435 | 497 | 5.17 | 412 | 470 | 1.08 |
| middle_insert | 100k | 4,150 | 4,391 | 48.4 | 4,137 | 4,355 | 2.21 |
| middle_insert | 1M | 56.1 ms | 58.4 ms | 629 | 56.3 ms | 57.5 ms | 43.7 |
| middle_delete | 10k | 323 | 367 | 5.12 | 312 | 344 | 1.12 |
| middle_delete | 100k | 3,168 | 3,444 | 48.2 | 3,180 | 3,394 | 2.38 |
| middle_delete | 1M | 44.7 ms | 46.7 ms | 627 | 44.4 ms | 45.2 ms | 48.7 |
| tail_append | 10k | 108 | 111 | 0.42 | 108 | 110 | 0.42 |
| tail_append | 100k | 975 | 1,007 | 0.50 | 975 | 998 | 0.50 |
| tail_append | 1M | 10.5 ms | 10.9 ms | 1.88 | 10.5 ms | 10.6 ms | 2.12 |
| tail_delete | 10k | 311 | 338 | 0.42 | 313 | 345 | 0.42 |
| tail_delete | 100k | 3,124 | 3,384 | 0.46 | 3,174 | 3,379 | 0.50 |
| tail_delete | 1M | 44.3 ms | 48.3 ms | 1.17 | 44.0 ms | 44.6 ms | 1.38 |
| right_value_edit | 10k | 239 | 287 | 0.08 | 239 | 296 | 0.08 |
| right_value_edit | 100k | 2,527 | 2,861 | 0.08 | 2,566 | 2,848 | 0.08 |
| right_value_edit | 1M | 31.5 ms | 32.3 ms | 0.17 | 31.4 ms | 31.9 ms | 0.17 |
| right_insert | 10k | 520 | 532 | 0.17 | 538 | 557 | 0.21 |
| right_insert | 100k | 5,447 | 5,550 | 0.21 | 5,491 | 5,553 | 0.17 |
| right_insert | 1M | 64.8 ms | 71.6 ms | 0.67 | 65.2 ms | 65.7 ms | 0.75 |
| right_delete | 10k | 14.9 | 15.7 | 0.21 | 15.2 | 15.4 | 0.21 |
| right_delete | 100k | 149 | 153 | 0.21 | 148 | 154 | 0.25 |
| right_delete | 1M | 1,832 | 1,914 | 1.04 | 1,812 | 1,824 | 1.12 |
| two_sided_both_then_tick | 10k | 9,111 | 9,339 | 5.38 | 9,433 | 9,618 | 1.29 |
| two_sided_both_then_tick | 100k | 95.2 ms | 95.7 ms | 53.3 | 96.1 ms | 102.4 ms | 6.92 |
| two_sided_both_then_tick | 1M | 1,027.4 ms | 1,058.8 ms | 559 | 1,031.2 ms | 1,046.6 ms | 46.0 |
| two_sided_tick_each_side | 10k | 946 | 978 | 5.33 | 982 | 1,035 | 1.25 |
| two_sided_tick_each_side | 100k | 9,639 | 9,864 | 48.9 | 9,676 | 9,971 | 2.71 |
| two_sided_tick_each_side | 1M | 121.6 ms | 129.1 ms | 553 | 121.6 ms | 122.3 ms | 44.1 |

#### Two-sided tick split (median µs, orders tick / customers tick)

| View | Variant | Rows | FastReads | FastUpdates |
|---|---|---:|---:|---:|
| join | both_then_tick | 10k | 683 / 0.08 | 689 / 0.08 |
| join | both_then_tick | 100k | 7,204 / 0.12 | 7,064 / 0.08 |
| join | both_then_tick | 1M | 82.0 ms / 0.46 | 82.5 ms / 0.50 |
| join | tick_each_side | 10k | 101 / 524 | 103 / 541 |
| join | tick_each_side | 100k | 933 / 5,494 | 935 / 5,518 |
| join | tick_each_side | 1M | 9,921 / 66.1 ms | 9,783 / 66.0 ms |
| join_chain | both_then_tick | 10k | 9,111 / 0.17 | 9,432 / 0.17 |
| join_chain | both_then_tick | 100k | 95.2 ms / 0.54 | 96.1 ms / 0.62 |
| join_chain | both_then_tick | 1M | 1,027.4 ms / 1.54 | 1,031.2 ms / 1.12 |
| join_chain | tick_each_side | 10k | 101 / 845 | 104 / 878 |
| join_chain | tick_each_side | 100k | 940 / 8,701 | 942 / 8,731 |
| join_chain | tick_each_side | 1M | 9,816 / 111.7 ms | 9,695 / 111.9 ms |

#### Growth: tick median ratio 100k/10k and 1M/100k

| View | Workload | FastReads | FastUpdates |
|---|---|---:|---:|
| none | value_edit | 1.0× / 1.0× | 1.0× / 1.0× |
| none | middle_insert | 0.5× / 3.0× | 1.0× / 1.5× |
| none | middle_delete | 1.0× / 1.0× | 1.0× / 1.0× |
| none | tail_append | 1.0× / 1.0× | 1.0× / 1.0× |
| none | tail_delete | 1.0× / 1.0× | 1.0× / 1.0× |
| filter | value_edit | 0.9× / 1.0× | 1.0× / 1.0× |
| filter | middle_insert | 7.4× / 10.8× | 7.6× / 10.2× |
| filter | middle_delete | 8.2× / 10.1× | 8.6× / 9.9× |
| filter | tail_append | 1.0× / 1.1× | 1.2× / 1.0× |
| filter | tail_delete | 8.4× / 9.8× | 9.0× / 9.8× |
| sort | value_edit | 1.0× / 1.0× | 1.5× / 1.0× |
| sort | middle_insert | 9.6× / 10.5× | 8.8× / 10.1× |
| sort | middle_delete | 9.7× / 10.9× | 10.2× / 10.4× |
| sort | tail_append | 1.2× / 1.2× | 1.2× / 1.1× |
| sort | tail_delete | 8.9× / 10.2× | 9.2× / 9.9× |
| sort | sort_key_move | 9.7× / 10.3× | 10.0× / 10.1× |
| aggregate | value_edit | 0.9× / 1.1× | 1.0× / 1.0× |
| aggregate | middle_insert | 10.3× / 13.3× | 10.4× / 13.0× |
| aggregate | middle_delete | 10.3× / 13.2× | 10.5× / 13.1× |
| aggregate | tail_append | 7.3× / 15.7× | 7.6× / 15.3× |
| aggregate | tail_delete | 10.2× / 12.9× | 10.4× / 12.5× |
| join | value_edit | 0.8× / 1.2× | 1.2× / 1.0× |
| join | middle_insert | 9.2× / 12.1× | 9.2× / 11.6× |
| join | middle_delete | 8.8× / 12.4× | 9.0× / 11.2× |
| join | tail_append | 9.2× / 10.3× | 9.2× / 10.4× |
| join | tail_delete | 8.6× / 12.8× | 8.7× / 12.6× |
| join | right_value_edit | 12.5× / 11.6× | 12.0× / 12.0× |
| join | right_insert | 10.5× / 11.8× | 10.5× / 11.6× |
| join | right_delete | 10.0× / 12.3× | 10.1× / 12.2× |
| join | two_sided_both_then_tick | 10.5× / 11.4× | 10.3× / 11.7× |
| join | two_sided_tick_each_side | 10.3× / 11.8× | 10.0× / 11.7× |
| join_chain | value_edit | 1.0× / 1.0× | 1.0× / 1.0× |
| join_chain | middle_insert | 9.5× / 13.5× | 10.0× / 13.6× |
| join_chain | middle_delete | 9.8× / 14.1× | 10.2× / 14.0× |
| join_chain | tail_append | 9.0× / 10.8× | 9.0× / 10.7× |
| join_chain | tail_delete | 10.1× / 14.2× | 10.1× / 13.9× |
| join_chain | right_value_edit | 10.6× / 12.5× | 10.7× / 12.3× |
| join_chain | right_insert | 10.5× / 11.9× | 10.2× / 11.9× |
| join_chain | right_delete | 10.0× / 12.3× | 9.8× / 12.2× |
| join_chain | two_sided_both_then_tick | 10.4× / 10.8× | 10.2× / 10.7× |
| join_chain | two_sided_tick_each_side | 10.2× / 12.6× | 9.9× / 12.6× |
