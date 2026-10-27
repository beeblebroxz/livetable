# Join key lookups without rebuilding the other side

**Status:** Implemented 2026-10-26
Plan: `docs/superpowers/plans/2026-10-26-join-key-lookups.md`.
Results: "After the join lookup fix" in `docs/VIEW_MAINTENANCE_BENCHMARK.md`.

Implementation notes:

- Every budget was met. At 1M rows:
  - order append 90 µs;
  - order middle insert 1.1 ms;
  - customer insert 0.89 ms;
  - customer value edit 0.6 ms;
  - one-side-at-a-time two-sided updates 2 ms;
  - deletes 0.82–1.14× of before;
  - construction 0.96× and `refresh()` 1.04× of before.
- A 256-customer value-only batch takes 0.71 ms (from 33.4 ms). The first
  customer insert after a rebuild takes 29 ms (from 64 ms), because it
  builds the order-side vector.
- The candidate scan compares up to four target hashes directly per row and
  uses an identity-hashed set beyond that.
- `join.rs` was not rustfmt-clean before this change and was left as it
  was.

## Why

After the aggregate fix, the join's per-event lookups are the largest cost in
the view maintenance benchmark (`docs/VIEW_MAINTENANCE_BENCHMARK.md`). The
figures are for 1M orders INNER JOIN 100k customers on FastReads:

| Event | Join tick | Cause (`impl/src/view/join.rs`) |
|---|---:|---|
| Order append | 9.1 ms | `build_right_lookup`: a `HashMap<JoinKey, Vec<usize>>` over every customer, built per sync |
| Order middle insert | 11.5 ms | the same lookup, plus about 2.4 ms shifting `join_index` |
| Customer insert | 64 ms | `build_left_lookup` over every order |
| Customer value edit | 31 ms | `right_position_map`: right row → join positions, over the whole join output |
| Two-sided, one side at a time | 76 ms | the two lookups above |

Each of these builds is O(other side), or O(output), from scratch on every
sync. Each one reads every row's key columns and allocates a `JoinKey` per
row. Nothing from one sync is reused by the next.

## Goals

At 1M rows, on both storage hints, in the benchmark's `join` pipeline:

| Workload | Before | Budget |
|---|---:|---:|
| `tail_append` (order) | 9.1 ms | under 0.5 ms |
| `middle_insert` (order) | 11.5 ms | under 5 ms |
| `right_insert` (customer) | 64 ms | under 2 ms |
| `right_value_edit` | 31 ms | under 2 ms |
| `two_sided_tick_each_side` | 76 ms | under 10 ms |
| `value_edit`, `middle_delete`, `tail_delete`, `right_delete` | as measured | at most 1.25× before, for ticks of 1 µs or more |

Further budgets, measured with a throwaway program at 1M rows:
- **A 256-row value-only customer batch:** under 5 ms (today's map costs
  31 ms).
- **The first lookup after a rebuild,** which builds a key vector: at most the
  cost of one of today's per-sync lookups.

Join construction and `refresh()` take at most 1.10× before. Output values and
output history (events, order, invalidation) are unchanged.

## Non-goals

- **The two-sided rebuild:** both parents structural in one sync. It keeps its
  rebuild.
- **Shrinking `join_index` entries** (32 bytes today) or removing its
  O(output) passes. Right deletes and right key updates keep their
  O(output) `right_positions` scan and `retain`.
- **The RIGHT/FULL "still matched" check in `orphan_unmatched`:** an O(output)
  scan, unchanged.
- **Python or API changes.**

## Anti-goals

- **Moving the cost elsewhere:** construction, rebuilds, deletes or
  multi-key batches getting noticeably slower to pay for faster single
  inserts. The budgets above bound this.
- **Joins that are wrong under hash collisions.** A key's hash is never
  trusted as a match on its own.
- **A key vector that disagrees with the view's rows.** The vectors are only
  read when that side has no structural changes in the current sync.

## Design

### Where the time goes, and what changes

Two different jobs share today's slow paths:

1. **Finding matches** for a new row or a changed key: which rows on the
   other side have this key? Today a lookup over the whole other side is
   built per sync (`build_left_lookup`/`build_right_lookup`). This changes:
   the other side keeps a key-hash vector.
2. **Locating existing output rows** of a right row, for right deletes, key
   updates and value edits. Today this is an O(output) scan per event
   (`right_positions`), or for value-only right batches a map over the whole
   output (`right_position_map`).
   - Value-only right batches change to one targeted pass.
   - Structural right batches keep the scan, because their `retain` already
     costs O(output).

### Key-hash vectors, used only for finding matches

Each side gets `Option<Vec<u64>>`: the hash of each row's join key (a sentinel
for NULL or NaN keys), indexed by that parent's row position. `None` means
not built.

- **When they are read.** Only to find matches for:
  - left inserts and left key updates with a new key, which read the right
    vector;
  - right inserts and right key updates with a new key, which read the left
    vector.

  A side structural in the current sync is never read. When the left is
  structural, the right has no structural changes (the frame-mixing guard
  rebuilds otherwise), and the reverse also holds. So the read side's live
  rows match the view's state, apart from value edits, which don't change
  keys.
- **Lazy build.** A vector is built from the live parent the first time a
  read needs it, which by the rule above is always valid.
- **Maintenance.** Once built, the vector is updated by every structural event
  on its side, in changeset order:
  - an insert is `Vec::insert` (`push` at the tail);
  - a delete is `Vec::remove`;
  - a key update overwrites its entry.

  An unbuilt vector stays `None`. `rebuild_index` drops both.
- **Why lazily.** Construction and rebuilds get no extra work, which matters
  for the two-sided cascade. A side never read never pays for a vector.
  (Author's call.)
- **Hash.** `std`'s `DefaultHasher` over the `JoinKey`, deterministic within a
  process. A real hash that equals the NULL sentinel is remapped. In
  `cfg(test)` builds the hash keeps only its low 2 bits, so the crate's unit
  tests run under constant collisions.

### One scan per sync for all keys it needs

Before applying a structural batch, a pre-pass computes the target keys:
- the payload key of each insert;
- the new key of each key update, from `row_after_update`, cached for the
  apply pass.

One scan of the other side's vector collects candidate positions for every
target hash, using an identity-hashed map since the values are already
hashes. Candidate lists stay ascending.

During the apply pass, each candidate is verified by reading its key from
the live (non-structural) parent, and kept only if the keys are equal. NULL
and NaN keys skip the lookup: they never match. A batch with no targets
scans nothing.

### Value-only right batches: one targeted pass

When the right batch only edits values, its rows don't move. The pass:
1. collects the set of right rows its events touch;
2. walks `join_index` once, collecting each target row's positions in
   ascending order;
3. serves every event from that.

This replaces `right_position_map`, which hashed every entry. It is valid in
every ordering, including value-only-side-first while the left is
structural, because it reads only `join_index`, which is in the coordinates
the events use.

### Smaller changes

- **Orphan claim:** the orphan claim in `insert_left_matches` becomes a
  binary search in the orphan tail (`find_orphan_insert_position` plus an
  equality check), replacing its O(output) `position` scan.
- **Right tail inserts skip the shift pass.** A per-side row counter
  (`left_rows`, `right_rows`) is set at rebuild and updated per structural
  event. A right insert at `index == right_rows` skips the right shift pass;
  today it always shifts.
- **Left inserts keep their existing skip** (`max_existing_left`). It covers
  more cases than a counter, such as inserts after the last *matched* left
  row in INNER/RIGHT joins.
- **Range checks run first.** Before applying a batch, a pre-pass checks
  every event index against the simulated row counts (insert ≤ rows,
  delete and update < rows). A bad index rebuilds before any state changes,
  so the vectors' `insert`/`remove` can never panic. This follows the
  aggregate's rule.

### Unchanged

`join_index` and its ordering invariants, the frame-mixing guard,
value-only-side-first ordering, output events, the 512-event overflow
invalidation, `rebuild_index`'s matching algorithm, and `right_positions`
for structural right batches. `build_left_lookup`, `build_right_lookup` and
`right_position_map` are deleted.

## Testing and acceptance

1. **Existing suites pass:**
   - the join unit tests in `impl/src/view/tests.rs`, now under forced hash
     collisions;
   - `impl/tests/join_pipeline.rs`;
   - every fuzz driver with joins, including `agg_join`, which exercises a
     structural left batch alongside a value-only right batch.
2. **New contract tests in `join_pipeline.rs`:**
   - **Read counting:** after a first sync builds the vectors, a customer
     insert and an order insert each read only matched rows of the other
     parent. This fails today, because the lookups read every row.
   - **The case the review found:** a LEFT join where one sync deletes an
     order and edits a customer's name. The value edit lands at the right
     position, and a later customer insert still matches the remaining
     order.
   - **Delete and re-add:** deleting and re-adding the only order of a
     customer works for INNER, LEFT, RIGHT and FULL.
   - **Out-of-range indices** rebuild instead of panicking.
3. **Collision fuzz:** a small randomized differential test in
   `view/tests.rs`, so it runs under the forced collisions. It makes random
   inserts, deletes, key updates and value edits on both sides, for all four
   join types and both single and batched syncs, and compares each step with
   a fresh join.
4. **Benchmark:**
   - The harness adds an untimed customer insert and delete right after
     building each join pipeline, so both vectors exist before anything is
     measured. Left-side events then include vector maintenance.
   - Rerun before (on unmodified code with the changed harness) and after,
     on the same day.
   - Budgets as in Goals.
   - The throwaway program measures the 256-row customer batch, the first
     lookup after a rebuild, construction, and `refresh()`.
5. **Lint:** clippy is clean.

## Docs

- `.claude/rules/views-and-propagation.md`, the join bullet: the per-side
  key-hash vectors, read only when their side is non-structural, the lazy
  build and maintenance, verification, and the targeted pass for value-only
  right batches.
- `docs/JOIN_FEATURE.md`, the incremental behaviour section.
- The benchmark report: an "After the join lookup fix" section.

## Risks

- **A vector out of step with its parent** would silently produce wrong
  matches. Mitigations:
  - maintenance happens per event, in the same places that already shift
    `join_index`;
  - rebuilds drop the vectors;
  - the fuzz drivers compare against fresh joins on every step, and the
    collision build tests the verification path.
- **Memory:** 8 bytes per row on each side that has been read.
- **The read rule is load-bearing.** If a future change reads a vector while
  its side is structural, matches silently go wrong. The rules file states
  the rule, and the review-found test pins the known trap.
- **First query after a rebuild** pays one O(rows) build of the vector
  (about what one lookup costs today). The report will say so.
