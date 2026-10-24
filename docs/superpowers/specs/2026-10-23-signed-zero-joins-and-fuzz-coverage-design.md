# Signed-zero join keys and fuzz coverage

**Status:** Implemented 2026-10-23
Plan: `docs/superpowers/plans/2026-10-23-signed-zero-joins-and-fuzz-coverage.md`.
Current references: the signed-zero tests (`test_join_signed_zero_keys_match`
in `impl/src/view/tests.rs`, `signed_zero_*` in `impl/tests/join_pipeline.rs`)
and `impl/tests/forward_prop_fuzz.rs`.

Implementation notes:

- The fuzz found no FastUpdates or view-parent-join bugs.
- Mutation check: dropping the join's empty-batch version record now fails
  the chained, batched and both join drivers, each in a new view-parent join
  (`filter_join`, `view_join`, `view_join_batched`). Before, only a contract
  test caught it.
- The batched view-parent join kept history in 2071 of 3600 steps (57%) for
  every join type, against 1702 (47%) for the root ⋈ root join. Its floor is
  25%.
- Fuzz wall time (debug `cargo test`, Apple M2): 128.05 s before, 128.19 s
  with storage alternation, 172.40 s with both view-join additions. That is
  35% more, over the ~25% target. The main pipeline's three extra views and
  their oracles cost most of it; kept as this spec directs.

## Why

A review on 2026-10-23 found the core correct (275 Rust tests pass, no
ignored tests, no TODOs) but turned up one semantic bug and two blind spots
in the differential fuzz (`impl/tests/forward_prop_fuzz.rs`):

1. **Joins treat `-0.0` and `+0.0` as different keys.** `JoinKeyPart` stores
   raw IEEE bits (`impl/src/view.rs:69-71`, `87-89`), while sorts treat the
   two zeros as ties and GROUP BY folds them into one group. SQL says
   `-0.0 = 0.0`. A key computed as `-0.0` (for example `0.0 * -1`) silently
   loses its matches.
2. **The fuzz only exercises FastReads storage.** Every fuzz root is
   `Table::new` (FastReads). FastUpdates (tiered sequences, circular NULL bit
   blocks) has column-level unit tests but no end-to-end differential run.
3. **Every fuzzed join has a root table as a parent.** Fuzzed joins are
   root ⋈ root, root ⋈ aggregate, and root ⋈ static table, so every tick
   brings the join changes and its empty-batch branch (a parent version that
   advances without emitting rows, as after a filter's excluded edit) never
   runs. The join output changesets work recorded that the empty-batch
   version record survived its fuzz mutation check, and only a contract test
   caught it. No fuzzed join has a filter or sort parent either.

## Goals

1. Join keys fold `-0.0` into `+0.0` for Float32 and Float64, in
   construction and in every incremental path, Rust and Python alike.
2. Every root-table fuzz driver covers both storage hints, with no runtime
   increase from this item.
3. The fuzz covers joins whose parents are a FilterView and a SortedView,
   on each side, with history replay checks and a child view over such a
   join.

## Non-goals

- NaN join keys keep never matching (like NULL). Postgres matches NaN to NaN
  in joins; changing that is a separate semantic decision.
- Performance work, including the join's two-sided rebuild fallback.
- Python-side fuzzing, and running the server engine differential on
  FastUpdates.
- Changing how output rows carry key values: a joined row still shows each
  side's stored value (`-0.0` on one side, `0.0` on the other).

## Design

### 1. Signed-zero join keys

The two join-key builders in `view.rs` (`column_value_to_join_key_part`,
`column_value_into_join_key_part`) are the only places join keys are built,
and `join.rs` never converts a key back into a value. They keep returning
`None` for NaN, and map zero of either sign to `+0.0` bits. Reuse the
existing canonicalization (`canonical_f32_bits`/`canonical_f64_bits` in
`aggregate_support.rs`) by moving it next to `JoinKeyPart` rather than
writing a second copy; GROUP BY keeps calling it.

A key-column update from `-0.0` to `0.0` needs no special case. Once keys
fold, the before and after keys are equal, so `apply_left_changes` /
`apply_right_changes` take their existing equal-key branch and emit an
in-place `CellUpdated` at each output position. It still counts as
structural for the frame-mixing guard (`has_structural_changes` matches on
the column name), which is conservative and correct.

Comments to fix: the `JoinKeyPart` doc ("Signed zeros remain distinct") and
the `GroupKey` doc, which should then say the two policies differ only on
NaN.

### 2. Both storage hints in the fuzz

Each driver picks the storage hint per trial instead of running every trial
twice, so the mutation sequence for a trial is unchanged and runtime stays
flat:

- Single-root drivers (chained, batched, int group-by, computed view):
  even trials FastReads, odd trials FastUpdates.
- Two-root join drivers: left and right hints vary independently across
  trials so all four combinations occur.

Oracles keep building their fresh copies with `Table::new` (FastReads), so a
FastUpdates trial also checks that results do not depend on layout.

The batched driver (about 150 rows), the int group-by driver (about 72) and
many chained trials (about 60 ± 10) grow past 64 rows, which crosses the
tiered NULL bitmap's first block boundary (`impl/src/column/bitmap.rs`,
64-bit minimum block). The join drivers cap at 24 rows and stay in one
block. That is acceptable for this bundle.

### 3. View parents for fuzzed joins

**Main pipeline** (`build_pipeline`, driven by the chained and batched
drivers). Add:

- `filter_join`: the existing filter `f` LEFT-joined to the static region
  table on `region`, plus a SortedView child over it (`sort_keys()`), so a
  stale version record surfaces in a descendant.
- `sorted_join`: the static region table FULL-joined to `sorted_direct`
  (the sort as the RIGHT parent), so right-side structural changes come from
  a view, including the delete+insert pair a sort emits for a move.

Register both on the base `TickableTable` after their parents (left and
right registration respectively). Compare the joins order-free against
oracles built from the cloned base, compare the sorted child in order, and
add all three to the replay set: strict replay in the chained driver if no
legitimate invalidation is possible there, otherwise
`replay_unless_invalidated`, as for `join_replayed` today.

**Join drivers** (`differential_join_fuzz`, `differential_join_batched_fuzz`).
Alongside the root ⋈ root join, add a join of a FilterView over `left` (a
predicate on `lval` that keeps about half the rows) and a SortedView over
`right` (by `rval`, then `rid`), on `k`, for the same join type. Sync the
two views, then the join, after each step's mutations. Compare order-free
against a join of fresh views over cloned tables.

- `differential_join_fuzz` makes one root mutation per step, so only one
  view parent emits anything: the filter at most one event, the sort at
  most a delete+insert pair, and a `k` update stays in place. This join
  never legitimately rebuilds, so use strict `assert_replay`, like the
  root ⋈ root join there.
- `differential_join_batched_fuzz` can change both parents' structure in
  one sync, so this join sometimes rebuilds; use
  `replay_unless_invalidated`, like the root ⋈ root join there.

## Testing and acceptance

- New join contract tests (in `impl/tests/join_pipeline.rs` or
  `view/tests.rs`, wherever key-semantics tests live): INNER and FULL joins
  match `-0.0` to `0.0` at construction for Float32 and Float64; an
  incremental insert of a `-0.0` key row matches existing `0.0` rows with
  replayable history; a key update from `1.0` to `-0.0` re-matches to the
  `0.0` partners; a key update from `-0.0` to `0.0` emits in-place
  `CellUpdated` events, not delete+insert. NaN still never matches.
- One Python test that a float-keyed join matches `-0.0` to `0.0`.
- Mutation checks, temporarily applied and reverted:
  - Removing the zero folding fails the new contract tests.
  - Removing the join's empty-batch version record
    (`impl/src/view/join.rs`, the `left_changes.is_empty() &&
    right_changes.is_empty()` branch) now fails the fuzz, not only its
    contract test. If it does not, say so in the status note rather than
    claiming the gap is closed.
- Full suite (`tests/run_all.sh`) passes, plus the fuzz binary, which
  `run_all.sh` does not run (CI does). Record the fuzz binary's wall time
  before and after in the same mode CI uses (debug `cargo test`; 128 s on
  2026-10-23, Apple M2). Item 3's extra views should cost no more than
  about 25%. If they cost more, record the measured overhead in the status
  note and keep the coverage; do not cut existing trial counts to fit
  (author's call: coverage is the point of this bundle, and the fuzz is not
  in the local `run_all.sh` loop).

## Docs

Per the `feature-docs` skill: `docs/JOIN_FEATURE.md` key semantics,
`.claude/rules/views-and-propagation.md` (drop "Float join keys are bitwise,
so -0.0 and +0.0 differ"), the join-key notes in
`docs/PYTHON_BINDINGS_README.md` and `docs/API_GUIDE.md` if they describe
join key matching, and the fuzz coverage list at the top of
`forward_prop_fuzz.rs` and in `tests/README.md`.

## Risks

- **Observable behaviour change:** joins that previously left `-0.0` keys
  unmatched now match. This is the fix, but it changes output for anyone
  relying on the old behaviour; note it in `JOIN_FEATURE.md`.
- **The fuzz finds a real bug** in FastUpdates storage or view-parent joins.
  Then that bug is in scope: diagnose and fix it at the root, with a
  focused regression test, before finishing.
