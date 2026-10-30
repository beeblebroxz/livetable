# View Hardening Implementation Plan

**Spec:** `docs/superpowers/specs/2026-10-29-view-hardening-design.md`

## Global Constraints

- Release behaviour changes only for spec item 4, and only for broken
  parents.
- Debug assertions follow the style of `ColumnAggState::remove_value`.

### Task 1: Aggregate checks (spec items 3, 4, 6)

**Files:**
- `impl/src/view/aggregate.rs`;
- `impl/src/view/aggregate_support.rs`;
- `impl/tests/aggregate_pipeline.rs`;
- `impl/tests/forward_prop_fuzz.rs`.

- [ ] Add `a_batch_that_disagrees_with_the_parent_length_rebuilds`.
  - Setup: a `Scripted` parent whose changeset records a `RowInserted` while
    its rows stay the same.
  - Expect: the sync invalidates history, and the view matches a fresh
    build.
  - Confirm it fails first.
- [ ] Replace `indices_in_range` with
  `final_len(len, changes) -> Option<usize>` (`None` when an index is out of
  range). Rebuild unless the result equals the parent's length.
- [ ] Add debug assertions in `remove_from_group` (missing group), in the
  value-update branch of `apply_changes` (missing group) and in
  `remove_column_value` (missing column state).
- [ ] In `differential_int_group_by_fuzz`, apply `1 + rng.below(5)`
  mutations per tick on odd trials.
- [ ] Run the lib tests, `aggregate_pipeline` and the int fuzz. Expect all to
  pass.

### Task 2: Join checks (spec items 1, 2, 5)

**Files:**
- `impl/src/view/join.rs`;
- `impl/src/view/tests.rs`;
- `impl/tests/join_pipeline.rs`;
- `impl/tests/forward_prop_fuzz.rs`.

- [ ] `JoinView::check_bookkeeping(&self) -> Result<(), String>`,
  `#[doc(hidden)] pub`. It checks counters, vectors and `join_index` ranges.
  Call it after every sync in `differential_join_fuzz`,
  `differential_join_batched_fuzz` (both joins) and
  `test_join_incremental_matches_rebuild_under_collisions`.
- [ ] Add fields `left_pending` / `right_pending`:
  - set from the batches in `sync`;
  - each cleared after its side is applied;
  - reset by `rebuild_index`.

  Debug assertions:
  - `matching_rows` (new `pending: bool` parameter) after its early return:
    `!pending || before.is_some()`;
  - `output_row` on a live left read: `!self.left_pending`;
  - `right_row` on a live read: `!self.right_pending`.
- [ ] Add `two_sided_batches_at_the_bound_stay_incremental`:
  - 256 order inserts plus 256 customer inserts: history kept, and the
    replay reproduces the join;
  - one more customer (257): rebuilds.
- [ ] Mutation checks, each restored afterwards:
  - skip `hashes.remove` on a left delete: the fuzz bookkeeping check fails;
  - pass `None` for `before` in phase 1: the read-rule assertion fires;
  - change `>` to `>=` at the bound: the new test fails.

### Task 3: Python sign check and finish (spec item 7)

- [ ] In `test_signed_zero_float_keys_match`, assert
  `math.copysign(1.0, row["k"]) == -1.0` and
  `math.copysign(1.0, row["right_k"]) == 1.0`.
- [ ] Update `.claude/rules/views-and-propagation.md`:
  - the aggregate length check;
  - the join's pending flags and `check_bookkeeping`.
- [ ] Run the fuzz suite, rebuild the extension, then run `run_all.sh`.
  Review, then commit.
