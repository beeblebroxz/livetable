# Batch replay costs in joins and sorted views

**Status:** Both parts implemented 2026-11-09. Part 2's results, and where the
implementation departs from the design, are under
[Part 2 as built](#part-2-as-built).

## Why

Joins and sorted views replay a batch one event at a time, and each
structural event makes several passes over the view's index. A batch of B
events therefore costs O(B × rows). The harness
`impl/examples/batch_replay_benchmark.rs` times one `sync()` beside a rebuild
of the same view (medians of 3, 1M orders, 100k customers;
`benchmarks/batch_replay_2026-11-09-before.jsonl`):

| View and batch | 64 events | 256 | 1,024 | 4,096 | Rebuild |
|---|---:|---:|---:|---:|---:|
| Join, orders on the left, scattered inserts | 56 ms | 224 ms | 938 ms | 3.7 s | 115 ms |
| Join, orders on the left, scattered deletes | 63 ms | 245 ms | 972 ms | 3.8 s | 120 ms |
| Join, orders on the right, appends | 21 ms | 87 ms | 335 ms | 1.4 s | 170 ms |
| Join, orders on the right, scattered inserts | 91 ms | 322 ms | 1.3 s | 5.6 s | 170 ms |
| Join, orders on the right, scattered deletes | 160 ms | 630 ms | 2.6 s | 11.6 s | 175 ms |
| Join, orders on the left, appends | 0.3 ms | 0.6 ms | 1.1 ms | 2.8 ms | 130 ms |
| Sort, scattered inserts | 167 ms | 711 ms | rebuilds | rebuilds | 185 ms |
| Sort, appends | 30 ms | 122 ms | rebuilds | rebuilds | 190 ms |

Two separate problems show here.

1. **The join has no upper bound.** Only a batch with structural changes on
   both sides rebuilds above 256 events. A one-sided batch of any size is
   applied event by event: 4,096 deletes cost 32 to 63 rebuilds. Above 512
   output events the join's history is invalidated anyway, so that work
   buys nothing: every view over the join rebuilds either way.
2. **Inside the bound, a batch costs several rebuilds.** At 256 events the
   join costs 2 to 4 rebuilds and the sorted view about 4. That is the price
   of keeping history, and it is worth paying only when views downstream
   would otherwise rebuild.

Appends at the end of the left parent are the exception: they move nothing,
and stay far cheaper than a rebuild at any batch size.

## Part 1: a work limit in the join (implemented)

The join counts the index maintenance a sync does and gives up when it is no
longer worth it.

1. **Work is counted in entries.** Each O(R) step adds the number of
   join-index or key-hash entries it moves or scans, before it runs:
   renumbering passes, inserts into and removals from the index, the scans
   that find a right row's entries or check for an orphan, and key-hash
   inserts and removals. A step at the end of a vector adds nothing.
2. **While history can be delivered,** the limit is 1,024 passes over the
   join's rows (both parents plus the output, at least 4,096). That is four
   passes for each of 256 events.
3. **Once history is lost,** the limit drops to 64 further passes, about one
   rebuild: in the after run, 64 passes took between 0.5 and 1.5 times the
   rebuild that followed.
   History is lost when the batch records its 513th event, or earlier when
   the batch is certain to.
4. **Certain overflow is estimated before applying.** The pre-pass that
   finds each inserted row's matches already knows how many entries the row
   adds. A batch of more than 512 events also looks up its deleted rows'
   keys, one extra scan of the other side's key hashes, to count the entries
   they remove. If the sum exceeds 512, the batch starts with the smaller
   limit.
5. **Past a limit, the sync returns an error** through the path an
   unreadable row already uses, and `sync` rebuilds.

The estimate and the limits decide only when the join stops replaying. They
never change what it produces: a rebuild gives the same rows.

### What this does not fix

- **Batches inside the bound** cost what they did. Part 2 is the fix.
- **A batch whose overflow cannot be predicted** still pays for its first
  512 output events before the smaller limit applies. Deletes in batches of
  512 events or fewer are not looked up, and key updates are never counted.

## Part 2: one-pass batch replay in the sorted view

The original vision asks for this: "inserting 20 rows at scattered indexes
can be done in one 'slide over' pass rather than 20 separate passes."

### Today

`SortedView` keeps `sorted_index` (view position to parent row),
`parent_to_view` (its inverse) and `sort_values` (the key columns, by parent
row). One insert away from the end makes five linear steps:

1. add one to every `sorted_index` entry at or past the new row;
2. insert the key into each `sort_values` column;
3. insert into `sorted_index`;
4. insert into `parent_to_view`;
5. rewrite `parent_to_view` for every view position past the insert, in
   view order, which is a random write per row.

A delete makes the same five. A batch of B events makes 5 × B of them.

### Design

Split a batch into two phases: resolve every event against the state as it
was before the batch, then change that state once.

**Phase 1: resolve.** Walk the events in order. Nothing in the three vectors
changes. The batch keeps three small structures, each at most B long:

- **Shifts.** The batch's inserts and deletes in parent coordinates, in
  event order. They map an event's row index to a *handle*: a pre-batch
  parent row, or a row the batch inserted. The walk is the one
  `row_before_batch` already does.
- **Tombstones.** Pre-batch rows that have left their slot: deleted, or
  given a new sort key. They stay in `sorted_index` until phase 2; a sorted
  list of their pre-batch view positions counts how many precede a position.
- **Floating rows.** Inserted rows, and pre-batch rows with a new sort key,
  each with its key values, kept in view order.

A row's view position at the time of an event is then a sum of two ranks:

- for a pre-batch row still in its slot at pre-batch position `v`:
  `v`, minus the tombstones before `v`, plus the floating rows ordered
  before it;
- for a floating row: the pre-batch rows ordered before it (a binary search
  over the unchanged `sorted_index` and `sort_values`), minus the tombstones
  before that point, plus its rank among the floating rows.

The binary search stays valid because the vectors it reads are frozen. Ties
follow parent order, as today. Shifts keep the relative order of surviving
rows, so a floating row's place among equal keys is found by comparing
pre-batch parent rows with the pre-batch row it was inserted before.

Each event emits what it emits today, with the same positions: an insert, a
delete, a `CellUpdated` for an edit that stays put, or a delete and insert
for a row that moves. Phase 1 costs O(B log N) comparisons and O(B²)
bookkeeping, about 65,000 steps at the 256-event bound.

**Phase 2: commit.** One pass over each vector:

- **`sort_values`:** copy forward, dropping deleted rows, placing inserted
  keys and overwriting changed ones. The same pass fills a table from each
  pre-batch parent row to its final row.
- **`sorted_index`:** merge the surviving entries, renumbered through that
  table, with the floating rows.
- **`parent_to_view`:** rewritten from the new `sorted_index`.

Each pass starts at the first position the batch touched, so a batch of
appends rewrites only what lies after its first insert.

### Expected cost and budgets

The batch costs one event's worth of passes, not B. These are predictions to
measure with `batch_replay_benchmark`, not results:

| Measure at 1M rows | Before | Budget |
|---|---:|---:|
| 256 scattered inserts | 711 ms | under 20 ms |
| 256 scattered deletes | 749 ms | under 20 ms |
| 256 appends | 122 ms | under 10 ms |
| 1 scattered insert (`view_maintenance_benchmark`) | 0.93 ms | at most 1.10× |

A single event must not get slower: the server ticks once per mutation.
Batches of one event keep today's path if the two-phase path measures slower
for them.

### Verification

- The emitted events do not change, so `impl/tests/sorted_pipeline.rs` must
  pass unmodified.
- `forward_prop_fuzz` compares every sync with a rebuild and replays the
  history. Add trials with 2 to 256 mutations per tick; today's drivers
  mostly apply one.
- New contract tests: a row inserted and deleted in one batch; a row
  re-keyed twice; an insert between rows of equal key; a batch of appends
  only; a batch that deletes every row.

### Part 2 as built

Code: `impl/src/view/sorted/batch.rs`. Plan:
`docs/superpowers/plans/2026-11-09-sorted-batch-replay.md`.

Medians of 3 at 1M rows
(`benchmarks/sort_batch_replay_2026-11-09-{before,after}.jsonl`):

| Batch | Before | After | Budget |
|---|---:|---:|---:|
| 256 scattered inserts | 703 ms | 7.2 ms | under 20 ms |
| 256 scattered deletes | 718 ms | 6.2 ms | under 20 ms |
| 256 appends | 125 ms | 2.4 ms | under 10 ms |
| 1 scattered insert | 3.75 ms | 3.77 ms | at most 1.10× |

Every budget is met. The before column differs slightly from the table above,
which came from the earlier run of the same harness.

Departures from the design:

- **Batches of one or two events keep the per-event path.** One pass costs
  about two events' worth: 1.9 to 2.3 times one event, level at two, ahead
  from three. So the switch is at three events
  (`MIN_BATCH_REPLAY_EVENTS`), and both paths stay.
- **The two paths are checked against each other,** not by more fuzz trials.
  A unit test applies random batches of 1 to 256 events through both and
  requires the same events, the same three vectors, and the vectors a
  rebuild gives. `forward_prop_fuzz` already runs batches of up to 6 events
  through whole pipelines, which now takes the one-pass path from 3.
- **Parent order is a segment list,** not a walk back through the events:
  runs of pre-batch rows that are still consecutive, and inserted rows.
  It gives each event's row directly, and its final form is the parent
  order the commit needs.
- **`sort_values` is rewritten in place,** by one `retain` for deleted rows
  and one rotation per inserted row from the end, so no second copy of the
  key columns is allocated.
- **A parent whose rows disagree with its changeset now rebuilds,** in both
  paths. The sorted view used to index out of range there; the aggregate
  and the join already rebuilt.

### The same split in the join

The join's batch has the same shape, and the same split applies: resolve
against a frozen `join_index` with tombstoned ranges and floating entries,
then drop, splice and renumber in one pass. It is the larger instance:

- left rows own a contiguous range of entries, but right rows' entries are
  scattered, so resolving a right-side batch needs one pass to find them;
- RIGHT and FULL joins move entries to and from the unmatched tail;
- the two-sided path runs the left batch against the right parent as it was.

Resolving first has one more benefit there: the batch's output is known
before anything is moved, so a batch that overflows the history can rebuild
at once, and the estimate in part 1 goes away.

Design it in its own spec after the sorted view's version is measured. The
sorted view has one parent and no fan-out, so it is the smaller place to
prove the technique.

## Not included

- **Lowering the join's 256-event bounds.** The work limit is sized so that
  batches within them are not affected.
- **Per-event savings in the join** that need no batching: renumbering only
  the entries after a left row's range, and merging the three passes of a
  right delete into one.
- **The filter view.** Its per-event cost is one shift of one vector, and a
  256-event batch stays below its rebuild.

## Acceptance (part 2)

- The existing tests in `impl/tests/sorted_pipeline.rs` pass unmodified, and
  the five new batch tests pass on both paths.
- The differential unit test fails under each of four planted bugs: an
  off-by-one in the floating-row count, a wrong tie rule against frozen
  rows, reversed order between two inserted rows with one anchor, and a
  short rotation in the commit.
- The budgets above.
- **Full suite:** fuzz, clippy and the Python suite all pass.

## Acceptance (part 1)

- **Each new test fails before the change:**
  - a batch of expensive events that emit nothing rebuilds past the limit,
    and 256 such events do not;
  - a batch certain to overflow that costs more than a rebuild rebuilds,
    and one that fits the history is replayed.
- **Appends stay incremental:**
  `oversized_output_invalidates_history_but_stays_incremental` passes
  unmodified.
- **Benchmark:** at 1,024 and 4,096 events, no join workload costs more than
  3 rebuilds. Rows at 64 and 256 events and the append rows stay within
  noise of before.
  - Result: 1.5 to 1.8 rebuilds at 1M and 1.9 to 2.5 at 100k, against up to
    63 and 90 before. See
    [the benchmark](../../VIEW_MAINTENANCE_BENCHMARK.md#after-the-join-work-limit-2026-11-09)
    for the noise in the small-batch rows.
- **Full suite:** fuzz, clippy and `run_all.sh` all pass.
