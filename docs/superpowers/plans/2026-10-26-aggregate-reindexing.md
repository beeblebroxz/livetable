# Aggregate Re-indexing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a single structural event cost `AggregateView` well under 2 ms at 1M rows (from about 90 ms), with no MIN/MAX rescans.

**Architecture:** MIN/MAX move to a per-group ordered multiset, so per-row group membership and parent rescans disappear (Task 2). The position → group map becomes a vector shifted per event in changeset order, replacing both hash indexes and the batch identity remap (Task 3). Baseline and after timings bracket the change (Tasks 1 and 4).

**Tech Stack:** Rust (`std::collections::BTreeMap`, `std::rc::Rc`), the existing aggregate contract tests and fuzz, the view maintenance benchmark harness.

**Spec:** `docs/superpowers/specs/2026-10-26-aggregate-reindexing-design.md`

## Global Constraints

- Output values and output history are unchanged, except that MIN/MAX of {-0.0, 0.0} becomes -0.0/0.0 deterministically.
- Bounds stay as they are: multi-shift batches over 512 events rebuild, and group-key updates in batches over 256 events rebuild.
- Every rebuild fallback happens before the batch changes any state, and before `output_changes.clear()`.
- Group-key updates overwrite `row_groups[row]` in place and never shift.
- Budgets at 1M rows:
  - structural tick under 2 ms (`middle_insert`, `middle_delete`, `tail_delete`) and `tail_append` under 50 µs, on both storage hints;
  - a MIN/MAX value edit that misses the extreme under 5 µs;
  - SUM construction at most 10% slower, and MIN+MAX at most 2× slower, on both build paths.
- No API, Python or output format changes. Commit on `master`, ending each message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- **Two rows tied for a group's MIN:** deleting one keeps the MIN, and deleting both moves it (Task 2 test).
- **Signed zeros:** MIN/MAX over {-0.0, 0.0} in either insertion order give -0.0/0.0, compared by bits (Task 2 test).
- **Median/p90 on a column that also has MIN/MAX** must stay exact without rescans. The fuzz `aggs()` puts all of them on one column, and `debug_assert!`s on removal run in test builds (Task 2).
- **Middle inserts on an Int32 fast-path aggregate:** no test sent any until now (Task 3 fuzz change).
- **A malformed batch** (event index out of range) rebuilds and does not panic (Task 3 test).

---

### Task 1: Baseline timings on unmodified code

**Files:**
- Create (never commit): `impl/examples/agg_budget_timing.rs`
- Output: the scratchpad files `agg_before.jsonl` and `vm_before.jsonl`

**Interfaces:**
- Produces: `agg_budget_timing`, which prints JSON lines `{measure, grouping, aggs, median_us}` for construction and `{measure, workload, median_us, p95_us}` for value edits. Task 4 reruns it unchanged.

- [ ] **Step 1: Write the timing program**
  - **Data:** 1M rows of `id` Int64, `region` String = `format!("region{}", i % 8)`, `bucket` Int32 = `(i % 8)`, and `amount` Float64 = `((i * 7919) % 100_000) as f64`. The minimum, 0, sits in region0, at rows 0, 100000, …
  - **Construction:** the median of 5 `AggregateView::new` calls for each of SUM by `region`, MIN+MAX by `region`, SUM by `bucket`, and MIN+MAX by `bucket`.
  - **Value edits:** a MIN+MAX by `region` view registered on a `TickableTable`, 5 warmups and 31 samples each:
    - `non_extreme_edit`: row 8 to 50 000.5, then back. Time only the first tick.
    - `extreme_edit`: row 0 (value 0, the region0 minimum) to 99 999.5, then back. Time only the first tick.
  - The program refuses debug builds, like the harness.

- [ ] **Step 2: Run the baselines**
  - Run: `cargo run --release --manifest-path impl/Cargo.toml --example agg_budget_timing > <scratchpad>/agg_before.jsonl`
  - Then: `cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 10000 100000 1000000 > <scratchpad>/vm_before.jsonl`
  - Expected: both exit 0. `vm_before.jsonl` has 246 lines.
  - Record the commit (`git rev-parse HEAD`), `sw_vers` and `rustc -V` in the ledger. No commit in this task.

### Task 2: MIN/MAX from an ordered multiset

**Files:**
- Modify: `impl/src/view/aggregate_support.rs` (`ColumnAggState`, `GroupState`)
- Modify: `impl/src/view/aggregate.rs` (delete the rescans and `pending_recalc`)
- Test: `impl/tests/aggregate_pipeline.rs`, `impl/src/view/tests.rs:10-76`
- Docs: `.claude/rules/views-and-propagation.md` (aggregate bullets on rescans and "after deferred MIN/MAX recalcs"), the MIN/MAX recalc comments in the `forward_prop_fuzz.rs` header and batched driver, and CLAUDE.md note 8 if its wording no longer fits

**Interfaces:**
- Produces:
  - `ColumnAggState::new(needs_sorted: bool, needs_extremes: bool) -> Self`
  - `ColumnAggState::remove_value(&mut self, value: f64)`, which no longer returns a recalc flag
  - `GroupState::new(position: usize, percentile_columns: HashSet<String>, extreme_columns: HashSet<String>) -> Self`
  - `GroupState::remove_column_value(&mut self, source_col: &str, value: f64)`
  - `AggregateView::extreme_source_columns(&self) -> HashSet<String>`, which lists the columns with MIN or MAX

- [ ] **Step 1: Write the failing tests** in `impl/tests/aggregate_pipeline.rs`
  - `removing_an_extreme_reads_no_parent_rows`:
    - A read-counting parent wraps `orders(&[("West", 10), ("West", 30), ("West", 25)])` and counts `get_row`, `get_value` and `get_value_by_index`.
    - Delete West's max row (30) and sync: `top` becomes 25.0, with zero reads.
    - Then edit West's max row (now 25) to 5 and sync: `top` becomes 10.0, with zero reads.
  - `tied_extremes_survive_one_deletion`: West has 30, 30 and 10. Deleting one 30 keeps `top` at 30.0 and emits no `top` update. Deleting the other moves it to 10.0.
  - `signed_zero_extremes_are_deterministic`: a MIN+MAX view over a Float64 column. For both insertion orders ([-0.0, 0.0] and [0.0, -0.0]), incrementally and after `refresh()`, MIN's `to_bits()` equals `(-0.0f64).to_bits()` and MAX's equals `0.0f64.to_bits()`.
  - Rename `deleting_the_max_in_a_mixed_batch_emits_the_rescanned_value` to `deleting_the_max_in_a_mixed_batch_emits_the_next_max`, keeping its body.

- [ ] **Step 2: Run them and confirm the failures**
  - Run: `cd impl && cargo test --features server --test aggregate_pipeline`
  - Expected:
    - `removing_an_extreme_reads_no_parent_rows` FAILS, because the reads are not zero.
    - `signed_zero_extremes_are_deterministic` FAILS for at least one order.
    - `tied_extremes_survive_one_deletion` passes. It is a characterization test: it already holds and must keep holding.

- [ ] **Step 3: Implement**
  - `ColumnAggState` replaces `min`/`max` with `extremes: Option<BTreeMap<OrderedF64, usize>>`, where `OrderedF64` is a private newtype ordered by `f64::total_cmp`. MIN and MAX read its first and last keys.
  - `remove_value` decrements the key's count and drops the key at zero, with a `debug_assert!` that it was present. `sorted_values` removal gets the same assertion.
  - Delete `recalculate_min_max`, `recalculate_column_min_max`, `recalculate_group_column_min_max`, `needs_min_max` and every use of `pending_recalc`.
  - Update the `view/tests.rs` constructor calls to `new(true, false)` and `new(false, false)`.

- [ ] **Step 4: Run the suites**
  - Run: `cd impl && cargo test --features server --lib && cargo test --features server --test aggregate_pipeline && cargo test --features server --test forward_prop_fuzz`
  - Expected: all pass, including the three new tests.

- [ ] **Step 5: Update the docs and commit**
  ```bash
  git add impl/src/view/aggregate.rs impl/src/view/aggregate_support.rs impl/src/view/tests.rs impl/tests/aggregate_pipeline.rs impl/tests/forward_prop_fuzz.rs .claude/rules/views-and-propagation.md
  git commit -m "Keep aggregate MIN/MAX in an ordered multiset"
  ```
  Also stage CLAUDE.md if note 8 changed.

### Task 3: Position-indexed row groups

**Files:**
- Modify: `impl/src/view/aggregate.rs` (fields, `rebuild_index`, `add_row_to_aggregates`/`remove_row_from_aggregates` split, `apply_changes`, delete `BatchRowMapping`, `adjust_indices_*`, `ensure_row_to_group_built`, `row_to_group_built`)
- Modify: `impl/src/view/aggregate_support.rs` (`GroupState`)
- Test: `impl/tests/aggregate_pipeline.rs`, `impl/tests/forward_prop_fuzz.rs` (`differential_int_group_by_fuzz`)
- Docs: the rules bullet on temporary row identities, and the `forward_prop_fuzz.rs` int group-by comment on deferred `row_to_group` population

**Interfaces:**
- Consumes: Task 2's `GroupState::new(position, percentile_columns, extreme_columns)`.
- Produces:
  - The field `row_groups: Vec<Option<Rc<GroupKey>>>`.
  - `GroupState { row_count: usize, key: Rc<GroupKey>, … }`, with `GroupState::new(position, percentile_columns, extreme_columns, key: Rc<GroupKey>)`.
  - `fn add_to_group(&mut self, row: &Row, touched: &mut HashMap<GroupKey, Touch>) -> Rc<GroupKey>`.
  - `fn remove_from_group(&mut self, row: &Row, touched: &mut HashMap<GroupKey, Touch>, removed_slots: &mut Vec<usize>)`.
  - `fn indices_in_range(len: usize, changes: &[TableChange]) -> bool`.

- [ ] **Step 1: Write the failing test and the fuzz change**
  - `out_of_range_events_rebuild_instead_of_panicking`, in `aggregate_pipeline.rs`:
    - A test parent wraps a real table but exposes its own `Changeset`, which the test fills by hand.
    - Push a `RowDeleted { index: len + 5, … }` and bump the parent's version, then sync.
    - Assert that sync does not panic, that the view's history is invalidated (`changes_from(pre-sync cursor)` is None), and that the view equals a fresh build over the real table.
  - In `differential_int_group_by_fuzz`, about half the inserts use `insert_row(rng.below(len + 1), row)` instead of `append_row`.

- [ ] **Step 2: Run and confirm**
  - Run: `cd impl && cargo test --features server --test aggregate_pipeline out_of_range && cargo test --features server --test forward_prop_fuzz differential_int_group_by`
  - Expected:
    - The new test FAILS: today the bad delete is tolerated with no rebuild, so history is kept.
    - The int group-by fuzz passes. Middle inserts are correct today, just slow.

- [ ] **Step 3: Implement**
  - **Fields:** `row_groups` replaces `row_to_group`, `row_count` replaces `row_indices`, and each group's `key: Rc<GroupKey>` is created once.
  - **Helpers:** split the add/remove helpers into `add_to_group` and `remove_from_group`, which handle group statistics, the count, emptied groups and touch tracking but never positions.
  - **Events** in `apply_changes`, applied in changeset order:
    - insert: `row_groups.insert(index, Some(add_to_group(data)))`;
    - delete: `remove_from_group(data)`, then `row_groups.remove(index)`;
    - key update: `remove_from_group(old_row)`, then `row_groups[row] = Some(add_to_group(new_row))`;
    - non-key aggregate update: `row_groups[row]` gives the key, with no positional change.
  - **Pre-pass:** `indices_in_range(self.row_groups.len(), changes)` runs before `output_changes.clear()`. It simulates lengths through the batch and rebuilds on a bad index.
  - **`rebuild_index`:**
    - Both paths fill `row_groups` eagerly.
    - The Int32 path creates each group's `Rc` key at group creation and pushes `None` for a row it skips (`_ => continue`).
  - **Deletions:** delete `BatchRowMapping`, the final remap, `adjust_indices_for_insert`/`_delete`, `ensure_row_to_group_built` and `row_to_group_built`. The multi-shift branch keeps only its over-512 rebuild.

- [ ] **Step 4: Run the suites**
  - Run: `cd impl && cargo test --features server --lib && cargo test --features server --test aggregate_pipeline && cargo test --features server --test forward_prop_fuzz && cargo clippy --all-targets --features server -- -D warnings && cargo clippy --all-targets -- -D warnings`
  - Expected: all pass and clippy is clean.

- [ ] **Step 5: Update the docs and commit**
  ```bash
  git add impl/src/view/aggregate.rs impl/src/view/aggregate_support.rs impl/tests/aggregate_pipeline.rs impl/tests/forward_prop_fuzz.rs .claude/rules/views-and-propagation.md
  git commit -m "Index aggregate rows by position instead of rehashing"
  ```

### Task 4: After timings, budgets, and report

**Files:**
- Create: `benchmarks/view_maintenance_2026-10-26-before.jsonl` (copied from Task 1), `benchmarks/view_maintenance_2026-10-26-after.jsonl`
- Modify: `docs/VIEW_MAINTENANCE_BENCHMARK.md` (a new "After the aggregate fix" section), the spec's status
- Delete: `impl/examples/agg_budget_timing.rs` (never committed)

- [ ] **Step 1: Run the after timings**
  - Rebuild the Python extension with the SDK workaround from project memory, so the hook allows pytest later.
  - Rerun both Task 1 commands into `agg_after.jsonl` and `benchmarks/view_maintenance_2026-10-26-after.jsonl`.
  - Expected: both exit 0, and the harness's history checks pass.

- [ ] **Step 2: Check the budgets**
  - From the after JSONL at 1M on both hints: `aggregate` `middle_insert`, `middle_delete` and `tail_delete` ticks under 2000 µs, and `tail_append` under 50 µs.
  - From `agg_after` against `agg_before`:
    - `non_extreme_edit` under 5 µs;
    - SUM construction at most 1.10× before on both groupings;
    - MIN+MAX construction at most 2.0× before.
  - If MIN+MAX exceeds 2×, bulk-build each group's multiset in `rebuild_index` (collect, sort, count), rerun, and record both runs.
  - Any other miss is a finding to diagnose before continuing, not a number to report and move past.

- [ ] **Step 3: Write the report section**
  - Before/after tables for `aggregate` and `join_chain` (every workload, 10k/100k/1M, both hints), taken from today's two runs.
  - The budget table, including `extreme_edit` before and after.
  - The environment (commit, OS, rustc), noting that it differs from the 2026-10-23 run's OS, so only today's two runs are compared with each other.
  - Remove `impl/examples/agg_budget_timing.rs`, and set the spec status to `Implemented 2026-10-26` with links.

- [ ] **Step 4: Full verification and commit**
  - Run: `cd tests && ./run_all.sh`. Expected: all suites pass.
  ```bash
  git add benchmarks/view_maintenance_2026-10-26-before.jsonl benchmarks/view_maintenance_2026-10-26-after.jsonl docs/VIEW_MAINTENANCE_BENCHMARK.md docs/superpowers/specs/2026-10-26-aggregate-reindexing-design.md
  git commit -m "Record aggregate re-indexing results"
  ```
