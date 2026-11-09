# Join key types, join output names and aggregate infinities: plan

**Spec:** `docs/superpowers/specs/2026-11-09-join-keys-names-and-aggregate-infinity-design.md`

## Global Constraints

- Write each test first and confirm it fails.
- No join that works today changes its output names or rows.
- A rebuild's aggregate results do not change, except the percentile in spec
  item 4.

### Task 1: Join key types (spec items 1, 2)

**Files:**
- `impl/src/view.rs`;
- `impl/src/view/join.rs`;
- `impl/tests/join_pipeline.rs`;
- `tests/python/test_multi_column_join.py`.

- [ ] Add `int32_and_int64_keys_match_by_value`: build, a right insert and a
  left insert, each compared with a fresh join and `check_bookkeeping`.
- [ ] Add `csv_tables_join_across_inferred_integer_widths`.
- [ ] Add `mismatched_key_types_are_rejected`.
- [ ] Widen `Int32` to `Int64` in `build_composite_key` and
  `build_key_from_indices`.
- [ ] Check key types in `new_multi`, after the existence checks.
- [ ] Python: one test for the widening and one for the rejection.

### Task 2: Aggregate infinities (spec items 3, 4)

**Files:**
- `impl/src/view/aggregate_support.rs`;
- `impl/tests/aggregate_pipeline.rs`.

- [ ] Add `a_removed_infinity_leaves_the_finite_sum`: delete and edit paths.
- [ ] Add `opposite_infinities_sum_to_nan_until_one_leaves`.
- [ ] Add `a_percentile_on_a_value_ignores_an_infinite_neighbour`.
- [ ] Count infinities in `add_to_totals` and `remove_value`; read SUM and
  AVG through one `total()`.
- [ ] Return `sorted[lo]` from `percentile` when the fraction is zero.

### Task 3: Join output names (spec item 5)

**Files:**
- `impl/src/view/join.rs`;
- `impl/tests/join_pipeline.rs`.

- [ ] Add `a_left_column_named_like_a_right_output_stays_readable`.
- [ ] Add `a_join_over_a_join_keeps_every_column`: unique names, `get_value`
  equal to `get_row` for every cell, and replayed history for a right
  insert, a right value edit and a right delete.
- [ ] Add `right_output_names`, computed in `new_multi`.
- [ ] Use it in `prefix_right`, `output_row`, the placeholder fill, both
  `update_cells` callers, `get_value` and `column_names`.

### Task 4: Docs and full suite

- [ ] `docs/JOIN_FEATURE.md`, `docs/PYTHON_BINDINGS_README.md`,
  `docs/API_GUIDE.md`, `.claude/rules/views-and-propagation.md`.
- [ ] Rust tests with the fuzz, the three clippy configurations, then
  `tests/run_all.sh`.
