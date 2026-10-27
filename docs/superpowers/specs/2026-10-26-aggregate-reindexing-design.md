# Aggregate re-indexing without rehashing

**Status:** Implemented 2026-10-26
Plan: `docs/superpowers/plans/2026-10-26-aggregate-reindexing.md`.
Results: "After the aggregate fix" in `docs/VIEW_MAINTENANCE_BENCHMARK.md`.

Implementation notes:

- Every budget was met without the bulk-build fallback. At 1M rows:
  - middle insert or delete 73–79 µs, tail events about 1 µs;
  - a MIN/MAX edit 0.9 µs whether or not it removes the extreme;
  - SUM construction 0.51–0.60× of before;
  - MIN+MAX construction 0.87–1.26×.
- The signed-zero and tied-extreme tests also pass on the old code (arm64
  float min/max already order signed zeros), so they are characterization
  tests. The read-counting test is the one that failed first.

## Why

The view maintenance benchmark (`docs/VIEW_MAINTENANCE_BENCHMARK.md`) found
`AggregateView` to be the largest everyday cost: about 90 ms per middle insert
or any delete at 1M rows (7 ms at 100k), and 2.6 ms per tail append, for a SUM
grouped into 8 regions. Filter and sort cost 0.2–0.9 ms for the same events.

The cause is the aggregate's per-row index (`impl/src/view/aggregate.rs`):

- `row_to_group: HashMap<usize, GroupKey>` (parent position → group) and each
  group's `row_indices: HashSet<usize>` (positions) are keyed by position. So
  every insert or delete rebuilds both hashes in full
  (`adjust_indices_for_insert`/`_delete`).
- The append "fast path" still scans every key (`row_to_group.keys().max()`).
- Multi-structural batches avoid per-event rehashing with temporary
  identities (`BatchRowMapping`), but still rehash everything once at the end.

The per-group position sets exist mainly so MIN/MAX can rescan a group's rows
from the parent when its current extreme is removed. That is also why MIN/MAX
recalcs are deferred to batch end, after all shifts.

## Goals

1. A single insert or delete costs the aggregate about what it costs filter
   and sort: well under 2 ms at 1M rows in the benchmark's `aggregate` pipeline,
   down from about 90 ms. A tail append costs microseconds, not 2.6 ms.
2. Output values and output history (group coordinates, net diff, invalidation
   rules) are unchanged, apart from the signed-zero MIN/MAX note below.
3. Value edits and construction stay within the budgets in Acceptance 4.
   MIN/MAX value edits change from O(1), or an O(g) rescan when the extreme
   moves, to O(log g) always; a small fixed cost is accepted for that.

## Non-goals

- Join lookups, filter/sort index shifting, and the two-sided join rebuild
  (separate items in the benchmark report).
- Percentile/median maintenance, which keeps its sorted `Vec`. Its rebuild
  inserts into a sorted vector per row; that cost is not touched here.
- Python API or output changeset format changes.

## Anti-goals

- Moving the cost elsewhere: value edits or MIN/MAX deletes getting much
  slower so that structural events get faster.
- Keeping two ways to do the same thing (an old rehash path kept "just in
  case").

## Design

### Rows map to groups by position, in a vector

Replace `row_to_group: HashMap<usize, GroupKey>` with
`row_groups: Vec<Option<Rc<GroupKey>>>`, indexed by parent position. `None`
marks a row that belongs to no group: the Int32 fast path skips a row whose
group-by value reads as neither Int32 nor NULL (`_ => continue`), and a
positional vector cannot skip an index. `Option<Rc<_>>` is still
pointer-sized.

- **Inserts and deletes:** `Vec::insert` and `Vec::remove`, which is a memmove
  of 8-byte pointers rather than a rehash.
- **Group-key updates overwrite in place.** `row_groups[row]` is replaced, and
  nothing shifts. Today key updates reuse the same add/remove helpers as
  structural events. Those helpers must be split, so that group statistics
  are separate from position bookkeeping and a key update never pays two
  memmoves.
- **Tail appends:** `push`.
- **Shared keys:** each `GroupState` holds the `Rc<GroupKey>` that its rows
  share, created once per group, so per-row work is a reference-count bump
  rather than a key allocation.
- **Construction:** both build paths fill `row_groups` eagerly. The single-Int32
  fast path can do this with no extra lookups, by creating the group's `Rc` key
  when the group is created. The deferred `row_to_group_built` flag goes away.
  Building lazily from the parent at first sync would be wrong, because by
  then the parent already reflects the pending changes.

### Groups keep a row count, not a set of positions

Replace `GroupState::row_indices: HashSet<usize>` with `row_count: usize`.
Its only remaining job is the emptied-group check.

### MIN/MAX from an ordered multiset, so nothing rescans

For each source column that has a MIN or MAX aggregation, `ColumnAggState`
keeps an ordered multiset of its values: a `BTreeMap` from the value, ordered
by `f64::total_cmp`, to a count. MIN and MAX are its first and last keys.
Adding or removing a value is O(log g). NaN never enters, because
`extract_numeric` already excludes it.

Consequences:
- **No parent reads to recompute an extreme.** Deleting a group's minimum, or
  editing it away, needs no parent reads.
- **Deleted code:** `recalculate_group_column_min_max`,
  `recalculate_column_min_max`, `ColumnAggState::recalculate_min_max`, the
  `pending_recalc` deferral, and the batch-end ordering constraint it
  documented.
- **Columns with no MIN/MAX:** they stop tracking min/max at all.

(Author's call: a multiset instead of per-group membership plus rescans. It
removes machinery instead of moving it, makes MIN/MAX deletes O(log g)
instead of an O(g) parent rescan, and is the reason per-row membership is no
longer needed.)

**Signed zeros:** `total_cmp` orders -0.0 below 0.0, so MIN of {-0.0, 0.0} is
-0.0 and MAX is 0.0. Today the result depends on insertion order. The fuzz
compares numerically, so either is accepted; the new rule is deterministic.

### Batches apply events in order

Because `row_groups` is a vector of positions, each event's index is valid
against the vector as already updated by the earlier events in the batch.
That is the changeset's contract, so each event applies its own
insert/remove in order.

- `BatchRowMapping` and the final rehash are deleted.
- The existing bounds stay as they are: multi-shift batches over 512 events
  rebuild, and group-key updates in batches over 256 rebuild.
- **Range checks run first.** Before any state changes, and before
  `output_changes.clear()`, an O(B) pre-pass replays the batch's lengths and
  checks every event index: an insert must be at or before the end, a delete
  or update before it. A bad index rebuilds, so every rebuild fallback still
  happens before the batch touches state. This is a behaviour change for
  malformed batches only: a single-shift batch with a bad index is tolerated
  today, and would now rebuild. Consistent changesets never produce one.

Everything else stays as it is: group-key updates via `row_after_update`,
the `touched` net diff, `remove_group` with batch-end `compact_group_order`,
and `emit_batch_output`.

## Acceptance

1. **The existing suites pass unchanged in meaning:** the `aggregate_pipeline`
   contract tests, the aggregate unit tests in `impl/src/view/tests.rs`, and
   the three fuzz drivers that build aggregates (chained and batched, whose
   pipelines include joins over aggregates, and int group-by).
   - `deleting_the_max_in_a_mixed_batch_emits_the_rescanned_value` keeps its
     expected output. Its name describes the old mechanism, so rename it to
     say what it checks.
   - The `ColumnAggState` unit tests adapt to the new constructor.
2. **New contract tests:**
   - **No parent reads:** deleting a group's MIN and MAX rows, and editing a
     group's MIN value, reads no parent rows. Use a read-counting parent like
     `Counted` in `impl/tests/join_pipeline.rs`. This test fails today,
     because a rescan reads the group's rows.
   - **Duplicate extremes:** with two rows tied for the MIN, deleting one
     keeps the MIN; deleting both moves it.
   - **Signed zeros:** MIN/MAX over {-0.0, 0.0} give -0.0 and 0.0, both
     incrementally and on rebuild. Compare `to_bits()`: `ColumnValue`'s
     `PartialEq` treats the two zeros as equal.
   - **Read counting:** the read-counting parent must count every read the
     aggregate can make (`get_row`, `get_value`, `get_value_by_index`). The
     group must keep at least one row after the delete, or there is nothing
     to rescan.
   - **Middle inserts on the Int32 fast path:** the `differential_int_group_by_fuzz`
     driver inserts at a random position (`insert_row`) for about half its
     inserts. Today every test insert is an append, so no middle insert
     reaches an aggregate built by the Int32 fast path.
3. **Benchmark:** rerun the view maintenance benchmark. At 1M rows, the
   `aggregate` pipeline's `middle_insert`, `middle_delete` and `tail_delete`
   ticks are under 2 ms, and `tail_append` is under 50 µs, on both storage
   hints. Record the run beside the original (new JSONL, and an "after the
   aggregate fix" section in the report), keeping the original as the
   baseline.
4. **Construction and MIN/MAX budgets.** Use a throwaway timing program,
   not committed. Run it on the unmodified code first for the "before"
   numbers, then again after the change.
   - **Data:** 1M rows with realistic distinct amounts (for example
     `(i * 7919) % 100_000`), grouped two ways: by a String `region` (8
     values; the general build path) and by an Int32 `bucket` (8 values; the
     fast path).
   - **Construction:** time `AggregateView::new` for SUM and for MIN+MAX on
     both groupings.
     - SUM must not get more than 10% slower on either path.
     - MIN+MAX may get up to 2× slower. If it is worse, bulk-build each
       group's multiset (collect, sort, count) instead of inserting per row.
   - **MIN/MAX value edits:** for MIN+MAX by region, take the median of 31
     ticks (after 5 warmups) for two edits:
     - an edit that does not touch the extreme must stay under 5 µs (today
       it is O(1));
     - an edit that removes the group's current minimum is recorded (today
       it is an O(g) rescan).
   - Record all of these numbers in the report section.
5. **Lint:** clippy is clean (`--all-targets`, with and without `--features
   server`).

## Docs

- `.claude/rules/views-and-propagation.md`: the aggregate bullets on
  temporary row identities, remapping index hashes, and rescans affecting
  MIN/MAX.
- CLAUDE.md critical note 8, if its wording about `sorted_values` and rescans
  changes.
- `docs/VIEW_MAINTENANCE_BENCHMARK.md` (the after section).
- `forward_prop_fuzz.rs`: the header lines on MIN/MAX recalculation and the
  end-of-batch recalc, the batched driver's comment on it, and the int
  group-by comment on the deferred `row_to_group` population.
- `.claude/rules/views-and-propagation.md`, also line 22 ("after deferred
  MIN/MAX recalcs").
- Anything else the `feature-docs` skill lists that describes aggregate
  internals.

## Risks

- **A removal that misses its value** would leave a stale extreme forever,
  where a rescan used to self-correct. The same goes for percentile
  `sorted_values` on a column that also has MIN/MAX: today each rescan
  rebuilds them from the parent (`recalculate_min_max`), so they are
  periodically resynced, and after this change they are purely incremental,
  like a percentile-only column today. The fuzz aggregates put SUM, AVG,
  MIN, MAX, COUNT, median and p90 on one column, so it checks exactly that
  case against rebuilds.
  - Removal values come from changeset payloads, and from `row_after_update`
    for group-key moves. Percentile `sorted_values` already depend on exact
    removal, and the fuzz checks MIN/MAX/median/p90 against rebuilds.
  - Add a `debug_assert!` that a removed value is present, in both the
    multiset and `sorted_values`.
- **Memory:** about 40 bytes per distinct value per MIN/MAX column, per group.
  This replaces a `HashMap` entry plus a `HashSet` entry per row (about 70
  bytes or more), so the total should fall. Not measured.
- **Wider rebuild cost for MIN/MAX**, covered by Acceptance 4.
