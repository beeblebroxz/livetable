# Join Key Lookups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One-sided join syncs stop rebuilding lookups over the other side, and value-only customer batches stop hashing the whole output, within the spec's 1M-row budgets.

**Architecture:**
- Value-only right batches locate their entries with one targeted pass over `join_index` (Task 2).
- Each side gets a lazily built key-hash vector. It is read only while that side has no structural changes, and scanned once per sync for all target keys, with every candidate verified against the live parent (Task 3).
- A benchmark harness warm-up and before/after timings bracket the change (Tasks 1 and 4).

**Tech Stack:** Rust (`std::hash::DefaultHasher`, a small identity `BuildHasher`), the join contract tests and fuzz, the view maintenance harness.

**Spec:** `docs/superpowers/specs/2026-10-26-join-key-lookups-design.md`

## Global Constraints

- Output values, output events, event order and invalidation are unchanged. The frame-mixing guard and value-only-side-first ordering are unchanged.
- A side's key-hash vector is read only when that side has no structural changes in the current sync. Every candidate match is verified against the live parent's key.
- Out-of-range event indices rebuild before any state changes or `output_changes.clear()`.
- Budgets at 1M rows, on both storage hints:
  - `tail_append` under 0.5 ms;
  - `middle_insert` under 5 ms;
  - `right_insert` under 2 ms;
  - `right_value_edit` under 2 ms;
  - `two_sided_tick_each_side` under 10 ms;
  - `value_edit`, `middle_delete`, `tail_delete` and `right_delete` at most 1.25× before, for ticks of 1 µs or more;
  - a 256-row value-only customer batch under 5 ms;
  - the first lookup after a rebuild no slower than one of today's per-sync lookups;
  - construction and `refresh()` at most 1.10× before.
- No API or Python changes. Commit on `master`, ending each message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- **A sync where the left changes structure and the right only edits values** (a join over an aggregate of its own table): value edits land at the right positions, and later matches stay right (Task 2 test).
- **Distinct keys sharing a hash:** joins equal a fresh rebuild (Task 3 collision fuzz under `cfg(test)`).
- **NULL keys on either side** never match through the vectors (Task 3 collision fuzz uses a nullable key).
- **The first insert after a rebuild:** two-sided rebuilds drop the vectors, and the next lookup rebuilds them correctly (the existing batched join fuzz, plus the collision fuzz's batched mode, Task 3).
- **RIGHT/FULL orphans** are claimed and recreated correctly when a customer's only order is deleted and re-added (Task 3 test).

---

### Task 1: Harness warm-up and baselines

**Files:**
- Modify: `impl/examples/view_maintenance_benchmark.rs` (`build`, for `join`/`join_chain`)
- Create (never commit): `impl/examples/join_budget_timing.rs`
- Output: the scratchpad files `jl_before.jsonl` and `vm_before.jsonl`

**Interfaces:**
- Produces: `fn warm_join(p: &Pipeline, n: usize)` in the harness. `join_budget_timing` prints JSON lines `{measure, median_us}`. Task 4 reruns both.

- [ ] **Step 1: Add `warm_join` to the harness**

  Run it right after a join pipeline is built, untimed:
  - append order `order(n as i64, n as i64, ((n / 2) % (n / 10)) as i32, 10.0)`, then tick orders;
  - `delete_row(n)`, then tick orders;
  - append customer `customer(m as i32, "warm")`, then tick customers;
  - `delete_row(m)`, then tick customers.

  Lengths are unchanged afterwards, so the existing checks hold.

- [ ] **Step 2: Run at small sizes**

  Run: `cargo run --release --manifest-path impl/Cargo.toml --example view_maintenance_benchmark -- 1000 10000 | wc -l`
  Expected: 164 lines and exit 0.

  Run clippy (both configurations), then commit:
  ```bash
  git add impl/examples/view_maintenance_benchmark.rs
  git commit -m "Warm join pipelines before measuring"
  ```

- [ ] **Step 3: Write the throwaway timing program**

  Same data as the harness: 1M orders and 100k customers, FastReads, INNER join. The program refuses debug builds. It reports medians:
  - `construction`: `JoinView::new`, over 5 runs;
  - `refresh`: `refresh()`, over 5 runs;
  - `value_batch_256`: set the `name` of customers 0..256 to `"alt"`, then time the customers tick; restore untimed; 2 warmups and 7 samples;
  - `first_lookup_after_rebuild`: `refresh()` the join, then time the customers tick after appending customer `m`; delete it untimed; 5 samples.

- [ ] **Step 4: Run the baselines on the unmodified join**

  - Run `join_budget_timing` into `jl_before.jsonl`.
  - Run the full harness (`-- 10000 100000 1000000`) into `vm_before.jsonl`, in the background, with the machine otherwise idle.
  - Record the commit, `sw_vers` and `rustc -V` in the ledger.

### Task 2: Targeted pass for value-only right batches

**Files:**
- Modify: `impl/src/view/join.rs` (`apply_right_changes`, `insert_left_matches`; delete `right_position_map`)
- Test: `impl/tests/join_pipeline.rs`

**Interfaces:**
- Produces: `fn right_targets_positions(&self, rows: &[usize]) -> HashMap<usize, Vec<usize>>`, one pass over `join_index` that returns ascending positions per target right row.

- [ ] **Step 1: Write the regression test** `value_edits_land_while_the_left_side_shifts`
  - Setup: a LEFT join over `orders(&[(1, Some(10), 5), (2, Some(20), 7)])` and `customers(&[(10, "a"), (20, "b")])`.
  - Sync 1: before syncing, `customers.set_value(1, "name", "z")` and `orders.delete_row(0)`. Use `replayed`, which must reproduce the join, and expect `len() == 1` with `right_name == "z"`.
  - Sync 2: append customer `(20, "c")`, sync with `replayed`, and expect `len() == 2`.
  - This is a characterization test: it passes today and guards Task 3.

- [ ] **Step 2: Run it**
  - Run: `cd impl && cargo test --features server --test join_pipeline value_edits_land`
  - Expected: PASS.

- [ ] **Step 3: Implement**
  - Value-only batch (`structural == false`): collect the distinct target rows from its `CellUpdated` events, call `right_targets_positions` once, and serve each event from the result. Delete `right_position_map`.
  - Orphan claim in `insert_left_matches`: use `find_orphan_insert_position(r)`, and claim only if the entry there is `(None, Some(r))`.

- [ ] **Step 4: Run the suites**
  - Run: `cd impl && cargo test --features server --lib && cargo test --features server --test join_pipeline && cargo test --features server --test forward_prop_fuzz differential_join`
  - Expected: all pass.

- [ ] **Step 5: Commit**
  ```bash
  git add impl/src/view/join.rs impl/tests/join_pipeline.rs
  git commit -m "Locate value-only customer edits with one targeted pass"
  ```

### Task 3: Key-hash vectors, one scan per sync, range checks

**Files:**
- Modify: `impl/src/view/join.rs` (fields, `rebuild_index`, `sync`, `apply_left_changes`, `apply_right_changes`; delete `build_left_lookup`/`build_right_lookup`)
- Test: `impl/tests/join_pipeline.rs`, `impl/src/view/tests.rs`
- Docs: the join bullet in `.claude/rules/views-and-propagation.md`, `docs/JOIN_FEATURE.md` (Incremental behavior)

**Interfaces:**
- Produces:
  - fields `left_key_hashes: Option<Vec<u64>>`, `right_key_hashes: Option<Vec<u64>>`, `left_rows: usize` and `right_rows: usize`;
  - `fn key_hash(key: Option<&JoinKey>) -> u64`, which returns `NO_KEY` (`u64::MAX`) for None, remaps a real hash of `u64::MAX`, and in `cfg(test)` keeps only the low 2 bits of a real hash;
  - `fn indices_in_range(rows: usize, changes: &[TableChange]) -> bool`.

- [ ] **Step 1: Write the failing tests**
  - `one_sided_inserts_read_only_matched_rows`, in `join_pipeline.rs`, using `Counted` parents on both sides:
    - Build an INNER join over 50 orders, two per customer, and 25 customers.
    - Do one order insert and one customer insert, so both vectors get built.
    - Reset both counters. Insert an order for an existing customer and sync; the right parent's reads are at most 4.
    - Insert a customer whose cid matches 2 orders and sync; the left parent's reads are at most 6.
  - `a_customers_only_order_can_be_deleted_and_readded`, for all four join types:
    - Delete a customer's only order, sync, re-append it, sync.
    - `replayed` holds at each step, and the sorted rows equal a fresh join.
  - `out_of_range_events_rebuild_instead_of_panicking`:
    - The left parent is a `Scripted` wrapper, as in `aggregate_pipeline.rs`, with a hand-written changeset over a real 2-row table.
    - Cases: delete at 2, insert at 3, an update of row 2. Each one rebuilds (`changes_from(cursor)` is None), does not panic, and equals a fresh join.
  - `test_join_incremental_matches_rebuild_under_collisions`, in `view/tests.rs`:
    - An LCG over tables `(id Int64, k Int32 nullable)` on each side, with keys 0..6 and about 15% NULL.
    - All four join types, 30 trials of 60 steps.
    - Each step applies 1–3 random ops (insert at a random position, delete, key update, value update) to a random side, then syncs.
    - Compare a sorted multiset of rows against a fresh `JoinView` after each step.

- [ ] **Step 2: Run them and confirm**
  - Run: `cd impl && cargo test --features server --test join_pipeline one_sided_inserts out_of_range a_customers_only && cargo test --features server --lib collisions`
  - Expected:
    - `one_sided_inserts_read_only_matched_rows` FAILS: today's lookups read every row.
    - `out_of_range_events_rebuild_instead_of_panicking` FAILS: today the bad index is tolerated, with no rebuild.
    - The other two pass, as characterization tests.

- [ ] **Step 3: Implement**
  - **`rebuild_index`:** set `left_rows`/`right_rows` and drop both vectors.
  - **`sync`:** after the frame-mixing guard and before `output_changes.clear()`, check `indices_in_range(left_rows, &left_changes)` and `indices_in_range(right_rows, &right_changes)`, and rebuild on failure.
  - **Each `apply_*_changes`:**
    1. **Pre-pass:** compute the target keys (insert payload keys, and the new keys of key updates via `row_after_update`, cached for the apply loop).
    2. **If there are any non-NULL targets:** build the other side's vector from its live parent if it is `None`, then scan it once, collecting ascending candidates per target hash into a map with an identity hasher.
    3. **Matching:** keep only candidates whose live key equals the target key. This replaces `build_*_lookup`.
    4. **Own-side maintenance**, if the vector is built: insert, remove, or overwrite on a key update; update the row counter.
  - **Right inserts at `index == right_rows`:** skip the right shift pass.
  - **Delete:** `build_left_lookup` and `build_right_lookup`.
  - **Docs:** update the rules join bullet (vectors read only when their side is non-structural, lazy build, maintenance, verification, the targeted value-only pass) and JOIN_FEATURE's incremental section.

- [ ] **Step 4: Run the suites**
  - Run: `cd impl && cargo test --features server --lib && cargo test --features server --test join_pipeline && cargo test --features server --test forward_prop_fuzz`
  - Then run clippy in both configurations.
  - Expected: all pass and clippy is clean.

- [ ] **Step 5: Commit**
  ```bash
  git add impl/src/view/join.rs impl/src/view/tests.rs impl/tests/join_pipeline.rs .claude/rules/views-and-propagation.md docs/JOIN_FEATURE.md
  git commit -m "Find join matches from per-side key-hash vectors"
  ```

### Task 4: After timings, budgets, report

**Files:**
- Create: `benchmarks/view_maintenance_2026-10-26-join-before.jsonl` and `-join-after.jsonl`
- Modify: `docs/VIEW_MAINTENANCE_BENCHMARK.md` (a new "After the join lookup fix" section), the spec's status
- Delete: `impl/examples/join_budget_timing.rs`

- [ ] **Step 1: Run the after timings**

  Rerun both Task 1 Step 4 runs on the new code, into `jl_after.jsonl` and the after JSONL, with the machine idle. Expected: exit 0 and 246 harness lines.

- [ ] **Step 2: Check every Global Constraints budget**

  Compare against the before runs. A miss is a finding to diagnose before continuing, not a number to report and move past.

- [ ] **Step 3: Write the report section and clean up**
  - The report section has:
    - the budgets table;
    - before/after tick tables for `join` and `join_chain`;
    - the environment;
    - a note on the first-use build cost;
    - the remaining ranked costs.
  - Remove the throwaway program, and set the spec status to `Implemented 2026-10-26` with links.

- [ ] **Step 4: Full verification and commit**
  - Rebuild the Python extension with the SDK workaround, then run `cd tests && ./run_all.sh`. Expected: all suites pass.
  ```bash
  git add benchmarks/view_maintenance_2026-10-26-join-*.jsonl docs/VIEW_MAINTENANCE_BENCHMARK.md docs/superpowers/specs/2026-10-26-join-key-lookups-design.md
  git commit -m "Record join key lookup results"
  ```
