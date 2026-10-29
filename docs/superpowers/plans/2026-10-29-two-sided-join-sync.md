# Two-sided Join Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Join syncs where both parents are structural apply incrementally, as a delta join, instead of rebuilding, and the obsolete key-update-before-shift rebuild goes away.

**Architecture:**
- Phase 1 applies the left batch against the pre-batch right side: every right read is reconstructed with `row_before_batch`, and an unbuilt right vector is built from live and reversed through the batch.
- Phase 2 is today's right-structural sync against the live post-batch left side.
- A per-side bound of 256 events keeps phase 1's O(B) reads bounded.

**Tech Stack:** Rust, the join contract tests, the collision fuzz, the forward-prop fuzz, the view maintenance harness.

**Spec:** `docs/superpowers/specs/2026-10-29-two-sided-join-sync-design.md`

## Global Constraints

- Output values unchanged. Every batch's history replays exactly. `sync()` returns whether the output changed.
- Phase 1 never reads the right parent live except through `row_before_batch` and the reversed vector. Routing is by side, not by table.
- A two-phase sync rebuilds if either side's batch has more than 256 events. Single-sided syncs have no input bound.
- Budgets at 1M rows, on both storage hints:
  - `two_sided_both_then_tick`: `join` under 10 ms, `join_chain` under 15 ms;
  - every other `join`/`join_chain` workload at most 1.10× before, for ticks of 1 µs or more.
- No API or Python changes. Commit on `master`, ending each message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- **A customer edited and an order of theirs deleted in the same sync as another structural customer change:** the deletion payload shows the old name (Task 3 test).
- **A self-join with inserts, deletes and key updates:** history replays and the rows equal a fresh join (Task 3 test, and the Task 4 fuzz variant).
- **The first two-sided sync after a rebuild**, which builds the right vector in reverse: correct (Task 4's `refresh()` every 10 steps in the collision fuzz).
- **A join over an aggregate**, two-sided on many ticks: history replays (Task 4, `agg_join` added to `join_replayed`).
- **A key update before a shift on the same side:** incremental and exact (Task 2 test, and the batched join fuzz).

---

### Task 1: Baseline

- [ ] **Step 1:** Run the unchanged harness (`-- 10000 100000 1000000`) in the background into the scratchpad file `vm_before.jsonl`, with the machine idle. Record the commit, `sw_vers`, `rustc -V` and the wall time in the ledger.

  Expected: exit 0, 246 lines. `both_then_tick` passes the harness's existing rebuild assertion.

### Task 2: Drop the key-update-before-shift guard

**Files:** Modify `impl/src/view/join.rs` (the guard in `sync`, and `key_update_precedes_row_shift`); Test `impl/tests/join_pipeline.rs`; update the `view/tests.rs` test near line 4044 that pins this rebuild.

- [ ] **Step 1: Write the failing test** `a_key_update_before_an_insert_keeps_history`. For each of INNER, LEFT, RIGHT and FULL:
  - Setup: `orders(&[(1, Some(10), 5), (2, Some(20), 7)])` and `customers(&[(10, "a"), (20, "b")])`.
  - In one sync: `set_value(0, "cust", Int32(20))` on orders, then `insert_row(0, order(3, Some(10), 1))`.
  - Run `replayed`, which must hold, and assert history was kept (`history_kept`).
  - Assert `rows_sorted` equals a fresh join's.

- [ ] **Step 2: Run it.** `cd impl && cargo test --features server --test join_pipeline a_key_update_before`. Expected: FAIL on history kept, because today it rebuilds.

- [ ] **Step 3: Delete the guard.** Remove the guard clause and `key_update_precedes_row_shift`. Update the `view/tests.rs` test (near line 4044) to expect correct incremental output instead of a rebuild, and the guard's comment in `sync`.

- [ ] **Step 4: Run the suites.** `cd impl && cargo test --features server --lib && cargo test --features server --test join_pipeline && cargo test --features server --test forward_prop_fuzz differential_join`. Expected: all pass.

- [ ] **Step 5: Commit** with message "Drop the join's key-update-before-shift rebuild".

### Task 3: Two-phase syncs when both sides are structural

**Files:**
- Modify: `impl/src/filter_changes.rs` (the shared replay helper), `impl/src/view/join.rs`
- Test: `impl/tests/join_pipeline.rs`
- Update: `rebuilds_invalidate_history` (use an over-the-bound shape), the `view/tests.rs` both-sides test near line 3984 and its comment near line 3250
- Docs: the rules join bullet, `docs/JOIN_FEATURE.md` (incremental section), and the `join.rs` comments listed in the spec

**Interfaces:**
- Produces:
  - `pub(crate) fn row_before_batch<E>(changes: &[TableChange], index: usize, get_row: impl FnMut(usize) -> Result<Row, E>) -> Result<Row, E>` in `filter_changes.rs`. It shares its walk with `row_after_update`, which becomes a call into the same helper starting after its event.
  - `matching_rows` gains a key reader (`&dyn Fn(usize) -> Result<Option<JoinKey>, String>`) and an expected live length, in place of its built-in live key read and `rows` check.
  - The phase-1 right routing is the builder's choice (a field such as `right_before: Option<Vec<TableChange>>`, set only during phase 1, read by `output_row`'s `Half::Live(Some(r))` arm), within the spec's constraints.

- [ ] **Step 1: Write the failing tests** (for all four join types unless noted). Each asserts `replayed` holds, history was kept, and `rows_sorted` equals a fresh join's:
  - `a_new_customer_and_first_order_sync_together`: append customer `(30, "c")` and order `(3, Some(30), 9)`, then one sync.
  - `phase_one_payloads_show_the_old_right_rows` (LEFT):
    - In one sync: `customers.set_value(0, "name", name("z"))`, append customer `(30, "c")`, and `orders.delete_row(0)` (customer 10's order).
    - The returned changes include the deletion of that order, whose payload's `right_name` is `name("a")`.
  - `a_self_join_syncs_incrementally`:
    - One table `(id Int32, a Int32, b Int32)` joined to itself on left `a` = right `b`.
    - Three syncs: an insert, a delete, and a `set_value` on `a`.
  - `two_sided_batches_over_the_bound_rebuild`: append 257 orders and 1 customer, sync; history is not kept. It passes today, as a characterization test.

  Also change `rebuilds_invalidate_history`'s both-sides shape to over the bound.

- [ ] **Step 2: Run them.** `cd impl && cargo test --features server --test join_pipeline`. Expected: the first three FAIL on history kept; the bound test passes.

- [ ] **Step 3: Implement.**
  - **The helper:** `row_before_batch` and the shared walk.
  - **`sync`:** when both sides are structural, first rebuild if either batch has more than 256 events. Otherwise run phase 1, `apply_left_changes` with right reads routed to the pre-batch frame, then phase 2, `apply_right_changes(.., true)`.
  - **The right vector in phase 1:**
    - if built, use it as it is;
    - if not, build it from live, after checking the live length equals the simulated post-batch right count;
    - then reverse the right batch into it: an insert is removed; a delete is re-inserted with its payload key's hash; a key update gets the hash of `row_after_update` with the old value restored.
  - **Phase-1 matching** verifies candidates with `row_before_batch` keys.
  - **Phase 2** checks lengths against the live left (`left_rows` after phase 1).
  - **Comments and docs:** update the `join.rs` comments listed in the spec, the rules join bullet (two phases, the frame rule, the per-side bound) and JOIN_FEATURE.

- [ ] **Step 4: Run the suites.** `cd impl && cargo test --features server --lib && cargo test --features server --test join_pipeline && cargo test --features server --test forward_prop_fuzz`, then clippy in both configurations. Expected: all pass and clippy is clean.

- [ ] **Step 5: Commit** with message "Apply two-sided join syncs as a delta join".

### Task 4: Fuzz coverage

**Files:** Modify `impl/src/view/tests.rs` (the collision fuzz) and `impl/tests/forward_prop_fuzz.rs`.

- [ ] **Step 1: Strengthen the collision fuzz.**
  - **History:** before each sync, take a snapshot and cursor. After it, if history was kept, replay it onto the snapshot, check deletion payloads and old values, and require it to equal the live join.
  - **Exact order:** compare exact row order with the fresh join. If that fails on a path this plan doesn't touch, switch to order-free plus replay and record a ruling.
  - **Self-join variant:** the same table as both parents, on `k`.
  - **Refresh:** call `joined.refresh()` every 10 steps.
  - **Comment:** update the comment saying odd trials rebuild.

- [ ] **Step 2: Extend the main fuzz.** Add `agg_join` to `join_replayed()`: strict in the chained driver, `replay_unless_invalidated` in the batched one.

- [ ] **Step 3: Measure and raise the floors.**
  - Print the batched join drivers' kept-history rates once.
  - Raise each floor to about half its rate, as an integer ratio.
  - Remove the print, and record the rates in the ledger.

- [ ] **Step 4: Run the suites.** `cd impl && cargo test --features server --lib && cargo test --features server --test forward_prop_fuzz`, then clippy. Expected: all pass and clippy is clean.

- [ ] **Step 5: Commit** with message "Fuzz two-sided join history and self-joins".

### Task 5: After run and report

**Files:**
- Modify: `impl/examples/view_maintenance_benchmark.rs` (the `both_then_tick` assertion)
- Modify: `docs/VIEW_MAINTENANCE_BENCHMARK.md` (an "After the two-sided join fix" section), the spec's status
- Create: `benchmarks/view_maintenance_2026-10-29-before.jsonl` and `-after.jsonl`

- [ ] **Step 1: Flip the harness check.** `both_then_tick` now asserts history was kept; update its comment. Run at 1000 and 10000: 164 lines.

- [ ] **Step 2: Run "after"** in the background with the machine idle. Expected: 246 lines.

- [ ] **Step 3: Check the budgets** against the before run. A miss is a finding to diagnose.

- [ ] **Step 4: Write the report section.**
  - Contents: the budget table, before/after tables for `join`/`join_chain`, the environment, the harness-check note, and what is left.
  - Set the spec status to `Implemented 2026-10-29`.

- [ ] **Step 5: Verify fully and commit.**
  - Rebuild the Python extension with the SDK workaround, then run `cd tests && ./run_all.sh`. Expected: all suites pass.
  - Commit with message "Record two-sided join sync results".
