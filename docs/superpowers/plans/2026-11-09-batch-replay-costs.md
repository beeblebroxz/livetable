# Batch replay costs: plan

**Spec:** `docs/superpowers/specs/2026-11-09-batch-replay-costs-design.md`

This plan covers part 1, the join's work limit. Part 2, one-pass batch replay
in the sorted view, is designed in the spec and needs its own plan.

## Global Constraints

- Record the before numbers on the unchanged join code, with the harness
  added, before touching `join.rs`.
- The limits decide only when a sync gives up. A rebuild's rows are the
  contract, so no test's expected rows change.
- Batches within 256 events per side and appends to the left parent behave
  as before.

### Task 1: Harness and before numbers

**Files:**
- `impl/examples/batch_replay_benchmark.rs`;
- `benchmarks/batch_replay_2026-11-09-before.jsonl`.

- [ ] Time one `sync()` after a batch of 64, 256, 1,024 and 4,096 orders,
  then a `refresh()` of the same view. Joins with orders on the left and on
  the right; appends, scattered inserts and scattered deletes.
- [ ] The same for a sorted view at 64 and 256 events, as the baseline for
  part 2.
- [ ] Run at 100k and 1M rows.

### Task 2: Work limit (spec part 1)

**Files:**
- `impl/src/view/join.rs`;
- `impl/tests/join_pipeline.rs`.

- [ ] Add `expensive_batches_rebuild_past_the_work_limit`: unmatched
  customers deleted from an INNER join emit nothing. 256 keep history;
  1,500 more rebuild.
- [ ] Add `a_batch_that_must_overflow_stops_at_a_rebuilds_worth_of_work`:
  100 orders for one customer replay; 600 more reread every customer key.
- [ ] Confirm both fail on the unchanged code.
- [ ] `JoinOutput`: `work`, `work_limit`, `rebuild_work`; `spend`, `expect`
  and `history_lost`. `record` calls `history_lost` on overflow.
- [ ] `spend` before every O(R) step: `insert_entry`, `remove_left_entries`,
  `remove_right_entries`, `orphan_unmatched`, the orphan claim, both sides'
  renumbering passes and key-hash edits, and the `right_positions` scans.
- [ ] `expected_events` after each side's `matching_rows`; batches over 512
  events add their deleted rows' keys to the targets.
- [ ] `oversized_output_invalidates_history_but_stays_incremental` passes
  unmodified.

### Task 3: After numbers, docs and full suite

- [ ] Run the harness again; compare with before.
- [ ] `docs/VIEW_MAINTENANCE_BENCHMARK.md`, `docs/JOIN_FEATURE.md`,
  `.claude/rules/views-and-propagation.md`.
- [ ] Rust tests with the fuzz, the three clippy configurations, then the
  Python suite on a rebuilt extension.
