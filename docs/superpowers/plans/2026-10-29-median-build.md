# Sort-once Percentile Build Implementation Plan

**Spec:** `docs/superpowers/specs/2026-10-29-median-build-design.md`

### Task 1: Sort once per build

**Files:** `impl/src/view/aggregate_support.rs` (`ColumnAggState`,
`GroupState`), `impl/src/view/aggregate.rs` (`rebuild_index`).

**Interfaces:**
- `ColumnAggState::add_value_unsorted(&mut self, value: f64)`: like
  `add_value`, but it appends to `sorted_values` without sorting.
- `ColumnAggState::sort_values(&mut self)`.
- `GroupState::add_column_value_unsorted(&mut self, source_col: &str, value: f64)`.
- `GroupState::finish_build(&mut self)`, which sorts every column's values
  with `f64::total_cmp`.

- [ ] Check in the timing program as
  `impl/examples/aggregate_build_benchmark.rs` (JSON lines, three builds
  and fifteen ticks per cell) and build it at the parent commit.
- [ ] Add `a_build_orders_values_that_arrive_out_of_order` to
  `impl/tests/aggregate_pipeline.rs`: values 5, 1, 4, 2, 3 in one group,
  grouped by `region`, by `note`, and by `note` with a second source
  column. Expect median 3.0 after the build, 2.5 after appending 0, and
  2.0 after deleting the 5. Confirm it fails with the final sort removed.
- [ ] In `rebuild_index`, both build paths call `add_column_value_unsorted`,
  then `finish_build` on every group after the loop.
- [ ] Run `cd impl && cargo test --features server --lib`,
  `--test aggregate_pipeline` and `--test forward_prop_fuzz`, then clippy.
  Expected: all pass.
- [ ] Run the timing program again. Expected: every MEDIAN build within 2×
  of SUM, and the one-insert tick unchanged.
- [ ] Set the spec status, add an "After the percentile build fix" section
  to `docs/VIEW_MAINTENANCE_BENCHMARK.md` and raw output under
  `benchmarks/`, update the aggregate rule and the complexity table in
  `docs/PERFORMANCE_COMPARISON.md`, and commit.
