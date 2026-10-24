# Signed-zero Join Keys and Fuzz Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `-0.0` and `0.0` match as join keys, and close two differential-fuzz blind spots (FastUpdates storage, joins with view parents).

**Architecture:** One semantic fix in the shared join-key builders (`impl/src/view.rs`), reusing GROUP BY's float canonicalization. The rest is test-only work in `impl/tests/forward_prop_fuzz.rs`: per-trial storage hints and new join nodes whose parents are a FilterView and a SortedView.

**Tech Stack:** Rust (cargo test, debug as CI runs it), PyO3 bindings, pytest.

**Spec:** `docs/superpowers/specs/2026-10-23-signed-zero-joins-and-fuzz-coverage-design.md`

## Global Constraints

- No new dependencies. No changes to view behaviour beyond join-key zero folding.
- NaN join keys still never match.
- Joined rows keep each side's stored value: a `-0.0` key shows as `-0.0`.
- `ColumnValue` derives `PartialEq`, so `Float64(-0.0) == Float64(0.0)`. Assert the sign with `is_sign_negative()` wherever it matters.
- Oracles keep building fresh tables with `Table::new` (FastReads).
- Do not cut existing fuzz trial counts. Fuzz wall-time baseline is 128 s (debug `cargo test --features server --test forward_prop_fuzz`, Apple M2, 2026-10-23); the target is at most about 25% more, and any overage is recorded, not trimmed away.
- Commit on `master`, ending each message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- A composite key with a float part of `-0.0` must match `0.0` in that part (Task 1, construction test).
- An unmatched LEFT-join row keyed `-0.0` must have its placeholder filled when a `0.0` partner arrives (Task 1, incremental test uses LEFT).
- A key update from `-0.0` to `0.0` must not duplicate or drop output rows (Task 1, in-place `CellUpdated` test).
- Python `-0.0` literals must join with `0.0` (Task 1, Python test).
- A filter's excluded edit must leave joins over that filter, and their children, coherent and replayable (Task 3 `filter_join_sorted`, Task 5 mutation check).

---

### Task 1: Fold signed zeros in join keys

**Files:**
- Modify: `impl/src/view.rs:36-95` (JoinKeyPart doc, both key builders)
- Modify: `impl/src/view/aggregate_support.rs:210-237` (move `canonical_f32_bits`/`canonical_f64_bits` out; fix `GroupKey` doc)
- Test: `impl/src/view/tests.rs` (next to `test_join_float64_keys`, ~line 3811)
- Test: `impl/tests/join_pipeline.rs`
- Test: `tests/python/test_multi_column_join.py`
- Docs: `docs/JOIN_FEATURE.md:177-178`, `docs/API_GUIDE.md:278`, `.claude/rules/views-and-propagation.md:23`

**Interfaces:**
- Produces: `pub(super) fn canonical_f32_bits(v: f32) -> u32` and `pub(super) fn canonical_f64_bits(v: f64) -> u64` in `impl/src/view.rs`. They are unchanged in behaviour (NaN → canonical NaN bits, ±0 → `+0.0` bits); `aggregate_support.rs` imports them from `super`.

- [ ] **Step 1: Write the failing construction test** `test_join_signed_zero_keys_match` in `impl/src/view/tests.rs`

Tables with `("k", Float64, false), ("label", String, false)`:
- Left `[(-0.0, "L_negzero"), (1.5, "L_one"), (NaN, "L_nan")]`, right `[(0.0, "R_zero"), (NaN, "R_nan")]`.
- INNER: `len() == 1`; `label == "L_negzero"`, `right_label == "R_zero"`; `row["k"]` is `Float64(v)` with `v.is_sign_negative()`, and `row["right_k"]` is not negative.
- FULL: `len() == 4`: the matched zero row, plus `L_one`, `L_nan` and `R_nan` unmatched. NaN still pairs with nothing.
- Repeat INNER with Float32 columns (`-0.0_f32` against `0.0_f32`): `len() == 1`.
- Composite key: `JoinView::new_multi` on `["k", "tag"]` with an Int32 `tag`. Left `(-0.0, 7)` matches right `(0.0, 7)`, and does not match right `(0.0, 8)`.

- [ ] **Step 2: Write the failing incremental tests** in `impl/tests/join_pipeline.rs`

Add a helper `fn float_keyed(name: &str, rows: &[(i32, f64)]) -> Rc<RefCell<Table>>` with columns `("id", Int32, false), ("k", Float64, false)`. It calls `clear_changeset()` after seeding, like `customers`. Join on `"k"`/`"k"` with `JoinView::new`; output columns are `id`, `k`, `right_id`, `right_k`. Use `baseline`/`replayed`:

- `signed_zero_insert_fills_the_placeholder`: LEFT; left `[(1, -0.0)]`, right empty. Append right `(10, 0.0)`. The returned changes are only `CellUpdated` from `Null` on `right_id` and `right_k` at row 0 (a placeholder fill). `len() == 1`.
- `signed_zero_key_update_rematches`: INNER; left `[(1, 0.0)]`, right `[(10, 1.0)]`, so the join starts empty. `set_value(0, "k", Float64(-0.0))` on right. Exactly one `RowInserted` at 0, and `len() == 1`.
- `signed_zero_key_update_in_place`: INNER; left `[(1, 0.0)]`, right `[(10, -0.0)]`. `set_value(0, "k", Float64(0.0))` on right gives exactly `[update(0, "right_k", Float64(-0.0), Float64(0.0))]`, with no delete or insert.

- [ ] **Step 3: Write the failing Python test** `test_signed_zero_float_keys_match` in `tests/python/test_multi_column_join.py`

Left `[("k", FLOAT64, False), ("val", STRING, False)]` with `{"k": -0.0, "val": "neg"}`; right `[("k", FLOAT64, False), ("data", STRING, False)]` with `{"k": 0.0, "data": "pos"}`. An INNER `livetable.JoinView("j", left, right, "k", "k", livetable.JoinType.INNER)` has `len == 1`, `val == "neg"`, `right_data == "pos"`.

- [ ] **Step 4: Run the Rust tests and confirm they fail** (this is the spec's "remove the folding" mutation check)

Run: `cd impl && cargo test --features server --lib signed_zero; cargo test --features server --test join_pipeline signed_zero`
Expected: all four FAIL. The construction and first two incremental tests fail on lengths or events. `signed_zero_key_update_in_place` fails because today the join starts empty and the update emits a `RowInserted`.

- [ ] **Step 5: Implement the fix**

Move the two canonical helpers into `view.rs` beside `JoinKeyPart` as `pub(super)`. In both `column_value_to_join_key_part` and `column_value_into_join_key_part`, keep the NaN `None` arms first and use `canonical_f32_bits`/`canonical_f64_bits` for the bits. Update comments: the `JoinKeyPart` doc says join keys fold `-0.0` into `+0.0` and exclude NaN; the `GroupKey` doc says the policies differ only on NaN.

- [ ] **Step 6: Run the Rust tests and confirm they pass**

Run: `cd impl && cargo test --features server --lib && cargo test --features server --test join_pipeline`
Expected: all pass, including `test_join_float64_keys`.

- [ ] **Step 7: Rebuild the wheel and run the Python test**

Run: `cd impl && ./install.sh && cd ../tests && pytest python/test_multi_column_join.py -v`
Expected: all pass, including `test_signed_zero_float_keys_match`.

- [ ] **Step 8: Update the docs**
  - `JOIN_FEATURE.md`: replace the bitwise bullet with "Float keys compare by value: `-0.0` matches `0.0` (as in sorts, aggregates and SQL), and output rows keep each side's stored value." Note that this changed on 2026-10-23.
  - `API_GUIDE.md:278`: add that `-0.0` and `0.0` keys match.
  - The rule file: replace "Float join keys are bitwise, so -0.0 and +0.0 differ (unlike sorting/grouping)" with "Float join keys fold -0.0 into +0.0 (like sorting/grouping); NaN keys never match."

- [ ] **Step 9: Commit**

```bash
git add impl/src/view.rs impl/src/view/aggregate_support.rs impl/src/view/tests.rs impl/tests/join_pipeline.rs tests/python/test_multi_column_join.py docs/JOIN_FEATURE.md docs/API_GUIDE.md .claude/rules/views-and-propagation.md
git commit -m "Match -0.0 and 0.0 join keys"
```

### Task 2: Run the fuzz on both storage hints

**Files:**
- Modify: `impl/tests/forward_prop_fuzz.rs` (imports, header comment, root construction in all six drivers)

**Interfaces:**
- Produces: `fn hint_for(trial: u64) -> StorageHint` (even → `FastReads`, odd → `FastUpdates`) in `forward_prop_fuzz.rs`.

- [ ] **Step 1: Switch the roots to per-trial hints**

Import `StorageHint`. Build roots with `Table::with_hint(name, schema, hint)`:
- chained, batched, int group-by, computed view: `hint_for(trial)`;
- both join drivers: left `hint_for(trial)`, right `hint_for(trial / 2)`.

Leave `clone_base`, `clone_int_base`, `clone_table` and `region_dim` on `Table::new`. Add a header Coverage bullet: roots alternate FastReads/FastUpdates by trial, and oracles stay FastReads.

- [ ] **Step 2: Run the fuzz**

Run: `cd impl && time cargo test --features server --test forward_prop_fuzz`
Expected: 6 passed, with wall time close to the 128 s baseline. A failure is a real storage bug: diagnose it with superpowers:systematic-debugging and fix it at the root, adding a focused regression test in `impl/src/column/layout_tests.rs` or `impl/src/view/tests.rs`, before continuing.

- [ ] **Step 3: Commit**

```bash
git add impl/tests/forward_prop_fuzz.rs
git commit -m "Fuzz both storage hints"
```

### Task 3: Joins over filter and sort views in the main pipeline

**Files:**
- Modify: `impl/tests/forward_prop_fuzz.rs` (`Pipeline`, `join_replayed`, `build_pipeline`, `assert_pipeline_matches`, header comment)

**Interfaces:**
- Consumes: the existing `f` (`filter`), `sorted_direct`, `dim`, `passes`, `sort_keys()`, `multiset`, `assert_ordered_eq`.
- Produces: `Pipeline` fields `filter_join: Rc<RefCell<JoinView>>`, `filter_join_sorted: Rc<RefCell<SortedView>>`, `sorted_join: Rc<RefCell<JoinView>>`.

- [ ] **Step 1: Build and register the views**
  - `filter_join`: `JoinView::new("fj", filter, dim, "region", "region", Left)`.
  - `filter_join_sorted`: `SortedView::new("fjs", filter_join, sort_keys())`.
  - `sorted_join`: `JoinView::new("sj", dim, sorted_direct, "region", "region", Full)`.

  Register after all existing registrations: `register_join_as_left(&filter_join)`, `register_sorted(&filter_join_sorted)`, `register_join_as_right(&sorted_join)`.

- [ ] **Step 2: Check against oracles** in `assert_pipeline_matches`
  - `ofj` = the same LEFT join of `of` to `p.dim`; compare `filter_join` with `multiset`.
  - `SortedView` over `ofj`; compare `filter_join_sorted` with `assert_ordered_eq` (ids are unique, so the order is total).
  - `osj` = the same FULL join of `p.dim` to `osd`; compare `sorted_join` with `multiset`.

- [ ] **Step 3: Add the three views to `join_replayed()`**

The chained driver keeps strict `assert_replays`, because single-mutation ticks give these joins no legitimate rebuild. The batched driver keeps `replay_unless_invalidated`. Update the header Coverage list.

- [ ] **Step 4: Run the fuzz**

Run: `cd impl && time cargo test --features server --test forward_prop_fuzz`
Expected: 6 passed. A strict-replay failure in the chained driver is a real bug; handle it as in Task 2 Step 2. Note the wall time.

- [ ] **Step 5: Commit**

```bash
git add impl/tests/forward_prop_fuzz.rs
git commit -m "Fuzz joins over filter and sort views"
```

### Task 4: View-parent joins in the join drivers

**Files:**
- Modify: `impl/tests/forward_prop_fuzz.rs` (`differential_join_fuzz`, `differential_join_batched_fuzz`, header comment)

**Interfaces:**
- Produces: `fn low_lval(row: &Row) -> bool` (`lval < 500`, Int32) and `fn right_sort_keys() -> Vec<SortKey>` (`rval` ascending, then `rid` ascending).

- [ ] **Step 1: Add the view join to both drivers**

Per trial, after `joined`, build:
- `lf = FilterView::new("lf", left, low_lval)`;
- `rs = SortedView::new("rs", right, right_sort_keys())`;
- `view_joined = JoinView::new("vj", lf, rs, "k", "k", jt)`.

Each step:
- Take its baseline (`snapshot` and cursor) before the mutations.
- After the mutations, sync `lf`, then `rs`, then `view_joined`, alongside `joined.sync()`.
- Oracle: the same filter, sort and join over `ol`/`or`, compared with `multiset`.
- Replay: strict `assert_replay` in `differential_join_fuzz`; `replay_unless_invalidated` in the batched driver, with its own `(kept, steps)` counters.

- [ ] **Step 2: Set the batched history floor**

Run the batched driver once with a temporary `eprintln!` of the view join's `kept/steps` per join type: `cd impl && cargo test --features server --test forward_prop_fuzz differential_join_batched -- --nocapture`. Assert a floor of about half the lowest observed rate, written as an integer ratio like the existing `kept * 4 >= steps`, then remove the `eprintln!`. Record the observed rates for the Task 5 status note.

- [ ] **Step 3: Run the fuzz**

Run: `cd impl && time cargo test --features server --test forward_prop_fuzz`
Expected: 6 passed. Failures are handled as in Task 2 Step 2. Note the wall time.

- [ ] **Step 4: Commit**

```bash
git add impl/tests/forward_prop_fuzz.rs
git commit -m "Fuzz view-parent joins in the join drivers"
```

### Task 5: Mutation check, full verification, status

**Files:**
- Temporarily modify, then revert: `impl/src/view/join.rs` (the empty-batch branch in `sync()`)
- Modify: `tests/README.md:181`, the spec's status header

- [ ] **Step 1: Mutation check on the empty-batch version record**

In `JoinView::sync()`'s `left_changes.is_empty() && right_changes.is_empty()` branch, delete the assignment to `last_left_parent_version`/`last_right_parent_version` and keep `return false`.
Run: `cd impl && cargo test --features server --test forward_prop_fuzz`
Expected: FAIL in a view-parent join or its child. Restore the line with `git checkout impl/src/view/join.rs`, then confirm with `git diff --stat` (no output). If the fuzz still passes, record that in the status note rather than claiming the gap is closed.

- [ ] **Step 2: Full verification**

Run: `cd tests && ./run_all.sh`, then `cd impl && time cargo test --features server --test forward_prop_fuzz` and `cd impl && cargo clippy --all-targets --features server -- -D warnings`.
Expected: everything passes. Record the fuzz wall time against the 128 s baseline.

- [ ] **Step 3: Docs and status**
  - `tests/README.md:181`: describe `forward_prop_fuzz.rs` as "Differential randomized view propagation on both storage hints, including joins over filter and sort views".
  - Spec header: `**Status:** Implemented 2026-10-23`, plus short notes on the mutation-check outcome, the fuzz wall time before and after, the view join's kept-history rates, and any bug the fuzz found.
  - Run the `feature-docs` skill checklist and update anything else it lists that the change touches.

- [ ] **Step 4: Commit**

```bash
git add tests/README.md docs/superpowers/specs/2026-10-23-signed-zero-joins-and-fuzz-coverage-design.md docs/superpowers/plans/2026-10-23-signed-zero-joins-and-fuzz-coverage.md
git commit -m "Record signed-zero join and fuzz coverage results"
```
