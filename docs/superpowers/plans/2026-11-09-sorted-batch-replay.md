# One-pass batch replay in the sorted view: plan

**Spec:** `docs/superpowers/specs/2026-11-09-batch-replay-costs-design.md`,
part 2.

## Global Constraints

- The emitted events do not change. `impl/tests/sorted_pipeline.rs` passes
  unmodified.
- A one-event batch is not slower: it keeps the per-event path unless the
  one-pass path measures within 10% of it.
- Record the before numbers, including 1-, 4- and 16-event batches, on the
  unchanged sorted view.

### Task 1: Harness and before numbers

**Files:**
- `impl/examples/batch_replay_benchmark.rs`;
- `benchmarks/sort_batch_replay_2026-11-09-before.jsonl`.

- [ ] Sort batches of 1, 4, 16, 64 and 256 events; a `--sort-only` flag.
- [ ] Run at 100k and 1M rows.

### Task 2: Resolve and commit

**Files:**
- `impl/src/view/sorted.rs`;
- `impl/src/view/sorted/batch.rs`;
- `impl/src/view/sorted/batch_tests.rs`.

- [ ] Split `apply_changes` into `updated_rows` (the pre-pass), `apply_each`
  (today's loop) and `apply_batch`.
- [ ] `resolve_batch`: a segment list for parent order, tombstones, and
  floating rows in view order; a row's position is its two ranks. It emits
  the events `apply_each` emits.
- [ ] `commit_batch`: final parent rows from the segments; `sort_values` in
  place; `sorted_index` merged from the first touched position;
  `parent_to_view` rewritten.
- [ ] `batch_replay_matches_event_by_event_replay`: random batches of 1 to
  256 events with ties, NULLs, NaN and signed zeros, four key sets, through
  both paths. Same events, same vectors, and a rebuild's vectors.
- [ ] Plant bugs to confirm the test fails.
- [ ] `final_len` check before either path: a parent that disagrees with
  its changeset rebuilds.

### Task 3: Switch-over, contract tests and results

**Files:**
- `impl/tests/sorted_pipeline.rs`;
- `benchmarks/sort_batch_replay_2026-11-09-after.jsonl`.

- [ ] Time 1-, 2- and 3-event batches on both paths; set
  `MIN_BATCH_REPLAY_EVENTS`.
- [ ] Contract tests: a row inserted and deleted in one batch; a row
  re-keyed twice; inserts between equal keys; appends only; a batch that
  deletes every row, then one that refills.
- [ ] Run them on both paths.
- [ ] After numbers; `docs/VIEW_MAINTENANCE_BENCHMARK.md`,
  `docs/INCREMENTAL_SORTED_PIPELINE.md`,
  `.claude/rules/views-and-propagation.md`, `docs/ORIGINAL_VISION.md`.
- [ ] Rust tests with the fuzz, the three clippy configurations, then the
  Python suite on a rebuilt extension.
