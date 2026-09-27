# Join Output Changesets — Design

**Date:** 2026-09-27
**Status:** Implemented 2026-09-27
**Goal:** Give `JoinView` an output changeset in its own coordinates, so
filters, sorts, aggregates, and joins built over a join replay its history
instead of rebuilding on every tick.

Current references: the contract tests in `impl/tests/join_pipeline.rs` and
the join replay checks in `impl/tests/forward_prop_fuzz.rs`.

Implementation notes (differences from the design below, and measurements):

- LEFT/FULL placeholder checks use the left row's contiguous range (binary
  search) instead of scanning `join_index`, and a key update reuses the
  batch's lookup of the other parent instead of rebuilding it per update.
  The other side cannot change structure during the pass, so one lookup is
  valid for the whole pass.
- The mutation checks all fail the suite. The empty-batch version record is
  caught only by its contract test: no fuzz join has a filter parent.
- The batched join fuzz keeps history in 47% of batches (both parents
  changing structure, or a key update before a shift, rebuild the rest).
- Measured on Apple silicon, release build, 100k orders INNER-joined to 10k
  customers, with a filter and a SUM-by-name aggregate over the join:

  | Edit | Children replay | Children rebuild |
  |---|---|---|
  | One left value edit | 16 µs | 128 ms |
  | One left insert | 195 µs | 127 ms |

  The join's own sync for 256 right value edits (2,560 output updates, so the
  history overflows and invalidates) took 3.2 ms, against 8.2 ms for its
  rebuild.

## Problem

Tables, filters, sorts, and aggregates publish bounded output history.
`JoinView::changeset()` is `None`, so every child of a join takes the
version-checked full rebuild path on each tick, even when the join only saw a
one-cell edit. Consumers are already generic: a filter, sort, aggregate, or
join replays any parent whose `changeset()` is `Some`, and rebuilds otherwise.
All of the work is inside `JoinView`.

Today, `JoinView::sync` mutates `join_index: Vec<(Option<left>, Option<right>)>`
in place (retain/insert/shift) and records nothing. Non-key cell updates do
nothing at all, because output rows are read live from the parents.

## Decisions

1. **Engine only.** No Python or wire changes beyond the `sync()` return value
   (below). Python chaining over joins (`JoinView.filter/sort/group_by`) and
   join nodes in the server pipeline spec are separate follow-ups. Only Rust
   DAGs can put a view over a join today.
2. **Emit as each `join_index` mutation is applied** (approach A below).
3. **Bounded output.** A batch whose output exceeds 512 events invalidates its
   history instead of retaining it.

## Approaches considered

- **A. Emit as each `join_index` mutation is applied (chosen).** Every insert,
  removal, or in-place replacement records an event at the index where it
  happens, with that moment's joined row. A join has no deferred recalculation
  (unlike the aggregate's MIN/MAX), so each mutation is exact when it happens.
- **B. Net diff per batch** (the aggregate's approach). Rejected: join output
  rows have no stable identity to key "before" rows on, since `(left, right)`
  index pairs change under shifts. It would need its own reconciliation pass to
  solve a problem that A does not have.
- **C. Diff `join_index` before and after each sync.** Rejected: O(output) per
  tick even for a one-cell edit, and CLAUDE.md forbids building deltas by
  scanning or diffing full snapshots.

## Output contract

The contract mirrors `SortedView` and `AggregateView`:

- `output_changes: Changeset` holds `TableChange` events in **join
  coordinates** (positions in `join_index`), with sequential step-local
  indices. Right-side columns keep their `right_` prefix in payloads and in
  `CellUpdated.column`, exactly as `get_row` returns them.
- `changeset()` returns it only while **both** parents' `version()` equal the
  versions recorded at the last sync or rebuild.
- Each non-empty input batch applied incrementally clears the history first
  (after the frame-mixing guard, which may still choose a rebuild), so exactly
  one batch is retained. A no-op sync keeps it.
- `rebuild_index()` calls `output_changes.invalidate()`. That one call covers
  `refresh()`, missing or compacted parent history, parents without history,
  the frame-mixing guard, and failed reads during replay.
- **Overflow:** when a batch would record more than 512 events, the join stops
  recording (and stops building payloads). At the end of the batch it
  invalidates the history. `join_index` maintenance continues incrementally.
- **An empty input batch on both sides records both parent versions** and
  keeps history. Today `sync()` returns early without recording them, so an
  excluded edit upstream (for example, a filter parent that emitted nothing)
  would leave `changeset()` as `None` and send children into rebuilds.
- **`sync()` returns `true` when `join_index` changed or any event was
  emitted.** Value-only edits now return `true` (today they return `false`),
  matching `SortedView`, which returns true for applied non-sort cell edits.
  This is visible from Python as `JoinView.sync()`. No existing test asserts
  `false` after a value-only edit.
- Unchanged: join semantics, NULL/NaN key exclusion, bitwise float keys, the
  frame-mixing guard, `join_index` ordering, and `root_changeset_cursor`
  (`usize::MAX`, so a join's children never constrain root compaction).

## Emission

### Processing order

The frame-mixing guard rebuilds whenever both sides have structural changes
(inserts, deletes, or join-key updates), so at most one side is structural.
**The value-only side is processed first**, then the structural side. When the
structural side later reads the other parent live, those rows already match
what the consumer has seen. If neither side is structural, order does not
matter: value updates do not move entries.

Non-key updates never change `join_index`, so this reordering only affects
emission, not the resulting index.

### Payloads

A joined row is the left part plus the right part (`right_`-prefixed), with
Nulls for an unmatched side. The structural side's own row comes from the
change: `data` for inserts and deletes, and reconstructed rows for key updates
(below). The other side is read live, which is coherent by the processing
order above.

### Rules

| Input event | `join_index` mutation | Emitted |
|---|---|---|
| Left delete | Remove the contiguous range of entries for row `l` | `RowDeleted` at the range start, once per entry |
| | RIGHT/FULL: a right row left unmatched becomes an orphan | `RowInserted` at its sorted position in the orphan tail |
| Left insert | RIGHT/FULL: matched orphans leave the tail | `RowDeleted` for each orphan |
| | Insert the matched entries, or the LEFT/FULL placeholder | `RowInserted` for each |
| Right delete | Remove the scattered entries for row `r` | `RowDeleted` in ascending position order, each index minus the number already removed |
| | LEFT/FULL: a left row left unmatched becomes a placeholder | `RowInserted` |
| Right insert | LEFT/FULL: fill the `(l, None)` placeholder in place | `CellUpdated` for each `right_` column, old Null → new value (skipped when the value is still Null) |
| | Otherwise insert the matched entries, or the RIGHT/FULL orphan | `RowInserted` for each |
| Key update, either side | Remove the old entries, insert the new ones (including placeholder fills and orphan moves) | By the delete and insert rules above, with **before** = `row_after_update(t)` with the old value swapped back in, and **after** = `row_after_update(t)` |
| Key update that leaves the composite key unchanged | none | Treated as a non-key update |
| Non-key update | none | `CellUpdated` at every output position referencing the row, using the change's own old and new values |

`row_after_update` (in `filter_changes.rs`) reconstructs the row as of change
`t` by undoing later same-batch updates to it. It replaces today's "read the
live row and swap in the old value", which is wrong when a later update in the
same batch touches the same row. A failed read falls back to a rebuild, as
`AggregateView` does; today it silently skips the row (`continue`).

### Finding positions for value updates

- **Left row:** its entries are contiguous (`join_index` is sorted by left),
  so two `partition_point` searches find the range in O(log output).
- **Right row:** its entries are scattered. When the right side has no
  structural events in the batch, one O(output) pass builds a
  `right row → positions` map for the batch. Otherwise each event scans
  linearly, the same order as the shift loops each structural event already
  runs.

## Costs and bounds

- A non-key update, which today costs the join nothing, costs O(k) for a left
  row with k matches, and one O(output) map pass per batch for right-side
  value updates. In exchange, every child of the join replays instead of
  rebuilding.
- Retained history is at most one batch of 512 events. Payload construction
  reads one joined row per `RowInserted`/`RowDeleted`, which a rebuilding
  child would read anyway.
- Structural paths keep their existing O(output) per-event costs. Making them
  sub-linear is out of scope.

## Testing

**Contract tests** in a new `impl/tests/join_pipeline.rs`, with the aggregate
tests' `replay` helper (which checks deletion payloads and old values) and
`sync_replayed` (old snapshot + history = new snapshot), across the relevant
join types:

- Left insert with and without a match, including an orphan leaving the tail;
  left delete that orphans rights.
- Right insert that fills a placeholder (`CellUpdated` from Null); right delete
  of scattered many-to-many entries (ascending adjusted indices).
- A key update that moves rows, and one that leaves the composite key unchanged
  (fans out as a value update).
- Value-update fan-out for a contiguous left row and a scattered right row,
  with `right_` names.
- Mixed batches: left structural plus right value edits (proves the value-only
  side goes first); a key update followed by a value edit to the same row
  (proves `row_after_update`).
- Rebuild invalidates; overflow invalidates while `join_index` stays correct;
  an empty batch records versions (a join over a filter that excluded an
  edit); history is hidden while a parent is ahead.
- Filter, sort, and aggregate over a join replay, proven with a read-counting
  parent; `sync()` returns `true` for value-only edits.

**Fuzz:** both join fuzzers (`differential_join_fuzz`,
`differential_join_batched_fuzz`) assert on every step that the history
replays the previous snapshot into the current one, or that it was
invalidated. The forward-propagation fuzz gains filter, sort, and aggregate
nodes over a join, compared with rebuilt oracles.

**Mutation checks:** each planted bug must fail the suite: a wrong right-delete
offset, the value-only side processed second, no event on a placeholder fill,
and a missing version record on an empty batch.

**Measurement:** a throwaway probe at 100k joined rows records (1) a child's
sync after a single value edit and a single insert, before (rebuild) and after
(replay), and (2) the join's own added cost for a batch of right-side value
updates. The results go into the implementation notes of this spec.

## Documentation

- CLAUDE.md: the readable-contract line ("Tables and synchronized
  filters/sorts/aggregates expose changesets") and the join line.
- `impl/src/readable.rs` and `impl/src/lib.rs` module docs.
- `docs/JOIN_FEATURE.md` (incremental sync section), `docs/API_GUIDE.md`, and
  the incremental pipeline docs wherever they say join children rebuild.
- `docs/ORIGINAL_VISION.md`: mark joins done under "Output changesets for
  join, projection, and computed views" (projection and computed stay open),
  and update the view-over-view composition line.
- `docs/PYTHON_BINDINGS_README.md`: the `JoinView.sync()` return value.
- `tests/README.md` for the new test target. CI (`.github/workflows/ci.yml`)
  and `tests/run_all.sh` list test targets explicitly, so add `join_pipeline`
  to both, and to the propagation-contract command in CLAUDE.md.

## Non-goals

- Python chaining over joins, and join nodes in the pipeline spec.
- Output changesets for projection and computed views.
- Stable identity for derived rows.
- Relaxing the frame-mixing guard.
- Making today's O(output) structural paths sub-linear.

## Risks

- **Coordinate bugs in emission.** Mitigated by the per-step replay assertion
  in both join fuzzers, and by `replay` checking deletion payloads and old
  values.
- **Payload incoherence across sides.** Mitigated by the processing order and
  the mixed-batch contract tests.
- **Added cost for value updates**, especially right-side batches on large
  joins. Measured by the probe; if it matters, a follow-up can maintain the
  right-row map incrementally.
- **`sync()` return value** changes for value-only edits, which Python callers
  can observe.
