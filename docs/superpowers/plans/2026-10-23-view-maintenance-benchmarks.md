# View Maintenance Benchmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A checked-in harness and a recorded report that measure one tick's cost per view type and workload at 10k/100k/1M rows on both storage hints, ending in a ranked recommendation for what to fix next.

**Architecture:** One example program, `impl/examples/view_maintenance_benchmark.rs`, in the style of `sorted_pipeline_benchmark.rs`. It builds fresh roots per pipeline, times the mutation and `tick()` separately, checks its own invariants outside the timing, and prints JSON lines. A report in `docs/` turns one full run into tables and conclusions, and the raw output is committed.

**Tech Stack:** Rust example (`std::time::Instant`, `serde_json`), livetable public API.

**Spec:** `docs/superpowers/specs/2026-10-23-view-maintenance-benchmarks-design.md`

## Global Constraints

- No library changes. No new dependencies. No CI thresholds.
- 5 warmup plus 31 measured samples per workload, unless the 1M run exceeds 15 minutes; then cut only the slowest workloads at 1M, never below 11 measured, and say so in the report.
- JSON fields: `rows`, `storage` (`"fast_reads"`/`"fast_updates"`), `view`, `workload`, `samples`, `mutate_median_us`, `tick_median_us`, `tick_p95_us`. `two_sided` lines add `tick_orders_median_us` and `tick_customers_median_us`. p95 is nearest-rank, as in `sorted_pipeline_benchmark.rs`.
- Timings exclude table and view construction and every invariant check.
- Every workload returns the tables to their starting state within a sample pair.
- Report claims stay within the data ("grows about 10× per 10× rows", not "is O(N)").
- Commit on `master`, ending each message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- A debug build gives meaningless numbers. The harness must refuse to run with `debug_assertions` on (Task 1).
- A view that silently rebuilt, or drifted, would still produce timings. The per-pair `len()` check (Task 1) and the invalidation checks (Task 2) catch these, and each check is proven live by a deliberate failing run.
- `customers` must use the same storage hint as `orders`, or the join's FastUpdates rows measure the wrong thing (Task 2 setup).
- Sub-microsecond ticks at 10k must not round to zero. Print µs with three decimals (Task 1 `report`).
- Stdout must carry only JSON lines, so the committed `.jsonl` is clean; progress goes to stderr (Task 1).

---

### Task 1: Harness with single-root pipelines

**Files:**
- Create: `impl/examples/view_maintenance_benchmark.rs`

**Interfaces:**
- Produces, in that file:
  - `type Row = HashMap<String, ColumnValue>`
  - `fn order(id: i64, rank: i64, cust: i32, amount: f64) -> Row`, with `region` = `format!("region{}", id % 8)`
  - `fn orders(n: usize, hint: StorageHint) -> Rc<RefCell<Table>>`. Row `i` has `id` = `rank` = `i`, `cust` = `i % (n / 10)`, and `amount` 10.0 for even `i`, 0.0 for odd. It calls `clear_changeset()` after seeding.
  - `struct Sample { mutate_us: f64, tick_us: f64, tick_parts: Option<(f64, f64)> }`
  - `fn report(rows: usize, hint: StorageHint, view: &str, workload: &str, samples: &[Sample])`, which prints one JSON line to stdout and skips the first 5 samples as warmups.
  - `const WARMUP: usize = 5; const MEASURED: usize = 31;`

- [ ] **Step 1: Write `main` and the single-root pipelines**
  - `main` parses row counts (default `10000 100000 1000000`; each must be ≥ 1000 and divisible by 10) and panics if `cfg!(debug_assertions)`, telling the user to pass `--release`.
  - Loop over sizes, then `[StorageHint::FastReads, StorageHint::FastUpdates]`, then views `none`, `filter`, `sort`, `aggregate`.
  - Each view gets a fresh `orders(n, hint)`, its own `TickableTable`, and one registered view:
    - `filter`: `amount >= 5.0`
    - `sort`: `SortKey::ascending("rank")`
    - `aggregate`: SUM(`amount`) AS `total`, grouped by `region`
  - Print progress (size, storage, view) to stderr.

- [ ] **Step 2: Implement the workloads** as described in the spec's Workloads table:
  - `value_edit`: middle row's `amount` to 11.0, then back to 10.0. Use row `n / 2` (even, so it passes).
  - `middle_insert` / `middle_delete`: `insert_row(n / 2, order(n as i64, (n / 2) as i64, ((n / 2) % (n / 10)) as i32, 10.0))`, then `delete_row(n / 2)`.
  - `tail_append` / `tail_delete`: `append_row(order(n as i64, n as i64, …, 10.0))`, then `delete_row(n)`.
  - `sort_key_move` (sort only): row `n / 2`'s `rank` to -1, then back to `n / 2`.

  Each iteration of a two-step workload times both steps, as two samples of two workloads. Run `WARMUP + MEASURED` iterations. Time `mutate` and `tick()` with separate `Instant`s, and `black_box` the view's `len()` after the tick.

- [ ] **Step 3: Add the invariant check**

  Capture every registered view's `len()` right after construction. After every iteration (outside the timing), assert each view's `len()` still equals it, with a message naming size, storage, view and workload.

- [ ] **Step 4: Prove the check is live**

  Temporarily change `middle_delete` to delete row `n / 2 + 1`, so the inserted row survives.
  Run: `cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 1000`
  Expected: panic from the invariant check (for example `filter` length off by one). Then revert.

- [ ] **Step 5: Run at small sizes**

  Run: `cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 1000 10000 > /tmp/vm.jsonl` (use the scratchpad path), then `wc -l` and `head -3` on it.
  Expected: exit 0. Each line parses as JSON with the Global Constraints fields. Per size and storage there are 5 workload lines for each of `none`, `filter` and `aggregate`, and 6 for `sort` (21 lines), so 84 lines in total.

- [ ] **Step 6: Lint and commit**

  Run: `cd impl && cargo clippy --all-targets --features server -- -D warnings`. Expected: clean.
  ```bash
  git add impl/examples/view_maintenance_benchmark.rs
  git commit -m "Add view maintenance benchmark harness"
  ```

### Task 2: Join pipelines and the two-sided workload

**Files:**
- Modify: `impl/examples/view_maintenance_benchmark.rs`

**Interfaces:**
- Consumes: Task 1's `order`, `orders`, `Sample`, `report`, `WARMUP`/`MEASURED` and the invariant check.
- Produces: `fn customers(m: usize, hint: StorageHint) -> Rc<RefCell<Table>>` (`cid` Int32 = `i`, `name` String = `format!("c{i}")`, changeset cleared), plus views `join` and `join_chain`.

- [ ] **Step 1: Build the join pipelines**

  Fresh `orders(n, hint)` and `customers(n / 10, hint)`, each with its own `TickableTable`.
  - `join`: INNER `JoinView` on `cust`/`cid`, registered `as_left` on the orders tick and `as_right` on the customers tick.
  - `join_chain`: the same join, plus a `FilterView` over it (`amount >= 5.0`) and an `AggregateView` over the filter (SUM(`amount`) by `right_name`), both registered on both ticks, after the join on each. Otherwise a customers-side tick would update the join but not its children; this matches Python's `register_on_roots`. Derived parents report `usize::MAX` cursors, so double registration never holds back compaction.

  Include both in the invariant check.

- [ ] **Step 2: Join workloads**
  - Run Task 1's left workloads (`value_edit`, `middle_*`, `tail_*`) on both join views.
  - `right_value_edit`: customer `m / 2`'s `name` to `"alt"`, then back to its original value; tick customers.
  - `right_insert` / `right_delete`: append customer `cid` = `m` (no orders), tick customers; then delete it, tick customers.

- [ ] **Step 3: `two_sided`**

  Per iteration, the new customer has `cid` = `m`, and its order is `order(n as i64, (n / 2) as i64, m as i32, 10.0)` inserted at `n / 2`.
  - `both_then_tick`: insert both, then time `orders_tick.tick()` and `customers_tick.tick()` separately; `tick_parts` = (orders, customers), and `tick_us` is their sum.
  - `tick_each_side`: insert the order, time the orders tick; insert the customer, time the customers tick.
  - `mutate_us` is the sum of the inserts.
  - Cleanup, untimed: delete the order, tick orders; delete the customer, tick customers.

- [ ] **Step 4: Invalidation checks**

  Read the join's cursor (`changeset().unwrap().total_len()`) before the inserts. Before each timed tick in `tick_each_side`, read it again: after the join's parents sync, `changeset()` returns Some.
  - After `both_then_tick`'s orders tick, assert `changes_from(cursor)` is None (rebuilt).
  - After each `tick_each_side` tick, assert `changes_from(its pre-tick cursor)` is Some (kept history).

  Messages name size, storage, view and variant.

- [ ] **Step 5: Prove the invalidation check is live**

  Temporarily swap the two variants' expectations.
  Run: `cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 1000`
  Expected: panic in the `both_then_tick` check. Then revert.

- [ ] **Step 6: Run at small sizes**

  Run the 1000 and 10000 command from Task 1, Step 5.
  Expected: exit 0. Per size and storage there are now 21 + 2 × 10 join lines (5 left workloads + `right_value_edit` + 2 right structural + 2 `two_sided` variants), so 41 lines; 164 in total for the two sizes. `two_sided` lines carry both tick fields.

- [ ] **Step 7: Lint and commit**

  Same clippy command as Task 1. Then:
  ```bash
  git add impl/examples/view_maintenance_benchmark.rs
  git commit -m "Benchmark join maintenance and two-sided rebuilds"
  ```

### Task 3: Full run, report, and recommendation

**Files:**
- Create: `benchmarks/view_maintenance_2026-10-23.jsonl`
- Create: `docs/VIEW_MAINTENANCE_BENCHMARK.md`
- Modify: `docs/PERFORMANCE_COMPARISON.md` (benchmark surfaces, incremental pipeline commands), the spec's status header

- [ ] **Step 1: Record the environment**

  `git rev-parse HEAD`, `sysctl -n machdep.cpu.brand_string hw.memsize`, `sw_vers`, `rustc -V`, and a note that no other builds or tests were running.

- [ ] **Step 2: Full run**

  Run: `/usr/bin/time -p cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 10000 100000 1000000 > benchmarks/view_maintenance_2026-10-23.jsonl`, in the background on an idle machine.
  Expected: exit 0, 246 lines (41 × 3 sizes × 2 storages), and the wall time recorded. If it takes over 15 minutes, apply the Global Constraints sample cut and rerun.

- [ ] **Step 3: Write the report**

  Generate the tables with a throwaway script in the scratchpad, not committed. One table per view, with rows = workload × size and columns = median tick, p95 tick and median mutate for each storage hint. Then write:
  - the reproduction section, with the 10k command and its runtime;
  - the environment;
  - answers 1–3 from the spec;
  - a recommendation that ranks every candidate the data shows (two-sided rebuild cascade, filter/sort index shifting, aggregate re-indexing, join lookups), each with the numbers it rests on, stating the trade-offs and leaving the decision to the user.

  Link the report from `PERFORMANCE_COMPARISON.md`, with its run command.

- [ ] **Step 4: Status and commit**

  Set the spec's status to `Implemented 2026-10-23`, linking the plan and report.
  ```bash
  git add benchmarks/view_maintenance_2026-10-23.jsonl docs/VIEW_MAINTENANCE_BENCHMARK.md docs/PERFORMANCE_COMPARISON.md docs/superpowers/specs/2026-10-23-view-maintenance-benchmarks-design.md
  git commit -m "Record view maintenance benchmark results"
  ```
