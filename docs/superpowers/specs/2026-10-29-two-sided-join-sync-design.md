# Two-sided join syncs without rebuilding

**Status:** Draft 2026-10-29

## Why

After the aggregate and join lookup fixes, the largest remaining cost in the
view maintenance benchmark (`docs/VIEW_MAINTENANCE_BENCHMARK.md`) is the
join's frame-mixing rebuild. At 1M orders, when both joined tables change
before either is ticked:

| Pipeline | Ticked together (rebuild) | One side at a time |
|---|---:|---:|
| join | 81–92 ms | 2 ms |
| join + filter + SUM over it | about 1 s | 2.3 ms |

The rebuild also invalidates the join's history, so every view over the join
rebuilds too; that is most of the 1 s. Loading new customers and their
orders and then ticking is enough to trigger it, as is any self-join.

`JoinView::sync` (`impl/src/view/join.rs`) rebuilds in two cases:

1. **Both parents structural** (insert, delete, or join-key update on both
   sides). The handlers read the other parent live, which is post-batch,
   while `join_index` is still pre-batch for that side.
2. **A key update before an insert or delete on the same side.** This was
   added in June (`733122e`), when the key-update handler read the row from
   the live parent by its recorded index. Since September (`6046003`) the
   handler reconstructs the row with `row_after_update`, which accounts for
   later shifts and deletes. Nothing else on that side reads the live parent
   by index, so this case looks obsolete.

## Goals

1. Syncs where both parents are structural apply incrementally and keep
   their history, when each side's batch has at most 256 events. For a
   self-join both changesets are the same events, so the bound is 256
   events. (Author's call: a per-side bound keeps phase 1's O(B) right reads
   bounded, and single-sided syncs keep having no input bound, as today.)
2. Drop the key-update-before-shift rebuild.
3. Budgets at 1M rows, on both storage hints:
   - `two_sided_both_then_tick`: `join` under 10 ms (from 81–92 ms) and
     `join_chain` under 15 ms (from about 1 s);
   - every other `join`/`join_chain` workload at most 1.10× before, for ticks
     of 1 µs or more.

   `rebuild_index` doesn't change, so there is no construction or
   `refresh()` budget.
4. Output values unchanged. Output history becomes available where it was
   invalidated, and every batch must still replay exactly.
   - `sync()` keeps its meaning: it returns whether the output changed.
   - A two-sided batch that changes nothing now returns false. The rebuild
     always returned true; no caller in the repo uses the result.

## Non-goals

- Two-sided batches with more than 256 events on either side: they still
  rebuild.
- O(output) passes over `join_index` (shifts, `retain`, the RIGHT/FULL
  still-matched scan), and filter/sort index shifting.
- Python or API changes.

## Anti-goals

- **History that doesn't replay.** Every phase-1 payload must show the right
  side as consumers last saw it (pre-batch), not the live post-batch rows.
- **A key-hash vector in the wrong frame.**
- **Slowing single-sided syncs to pay for two-sided ones.**

## Design

### Two phases, each in a consistent frame

A two-sided batch is applied as if every left change happened first, then
every right change. This is the usual delta join,
ΔL ⋈ R_old + L_new ⋈ ΔR:

1. **Phase 1:** apply the left batch against the pre-batch right side,
   R_old.
2. **Phase 2:** apply the right batch against the post-batch left side,
   L_new. After phase 1 the join's left state is the live left parent, so
   phase 2 is exactly today's right-structural sync.

Output history is phase 1's events followed by phase 2's, a valid ordering
that replays from the old output to the new one.

### Reading R_old in phase 1

Phase 1 reads the right side in three places:
- the payloads of right halves (`output_row` with `Half::Live(Some(r))`);
- the keys of candidate rows when matching left inserts and key updates;
- the right key-hash vector.

During phase 1 these go through the pre-batch frame:

- **Rows.** `row_before_batch(changes, r, get_row)` returns right row `r`
  as it was before the batch. It walks the right batch forward from position
  `r`:
  - tracking the row's position through inserts and deletes;
  - taking the delete payload if the row was deleted;
  - otherwise reading the live row at its final position;
  - undoing every value change to that row, earliest old value winning.

  This is `row_after_update`'s walk, started before the batch rather than
  after an event, so both should share one helper. It costs O(batch) per
  read; batches are bounded at 256 events.
- **Matching.** `matching_rows` takes a key reader instead of always reading
  live keys. Phase 1 passes one that verifies candidates with
  `row_before_batch`.
- **The right vector.** If it is already built, it is in the pre-batch
  frame, since phase 2 hasn't applied yet, and is used as it is. If not, it
  is built from the live parent and then converted to the pre-batch frame
  by applying the right batch in reverse:
  - an insert is removed;
  - a delete is re-inserted with the hash of its payload's key;
  - a key update gets the hash of its before-key, from `row_after_update`
    with the old value restored.

  Phase 2 then maintains it forward as usual. The length check compares the
  live length with the right row counter after the batch, simulated as the
  range check already does.

The rule this replaces ("read a side's vector only when that side has no
structural changes") becomes: **a side's vector, and every read of that
side, must be in the frame the join has applied so far for that side.**
Building from the live parent is valid only when that side has no
unapplied structural changes. Otherwise the reads use the reverse
application above.

**Every phase-1 read of the right side** reaches the parent through one of
two points:
- `matching_rows`: its length check, building the vector from live, and
  candidate verification;
- `output_row`'s `Half::Live(Some(r))` arm, reached from
  `remove_left_entries`, `orphan_unmatched` and `insert_left_matches` (both
  the unmatched-row claim and match inserts).

How phase 1 routes these (a mode set only for phase 1, or a parameter) is
left to the builder, with three constraints:
- `get_row` also uses `output_row` and must stay live, so it is a phase-1
  mode, not a permanent change;
- routing is **by side, not by table**: in a self-join the phase-1 left
  reads (`row_after_update` on the left) hit the same table and must stay
  live, so routing keyed on `Rc::ptr_eq` would be wrong;
- the two hash vectors stay separate even when a self-join uses the same key
  columns, since during phase 1 they are in different frames.

**Length check in phase 1.** `matching_rows` compares the live right length
with the right row count *after* the batch, as the range check simulates
it, not with `right_rows`. If this isn't adapted, every two-sided sync with
left inserts errors and quietly rebuilds; only history-kept assertions
catch that.

### Guards after the change

- A two-phase sync rebuilds when either side's batch has more than 256
  events. Single-sided syncs have no input bound, including ones with a key
  update before a shift.
- The range checks, the unreadable-row error rebuild and the 512-event output
  overflow are unchanged.
- The key-update-before-shift guard and its helper are deleted.
- `has_structural_changes` still decides the phase order: value-only side
  first, then the structural side, or left then right when both are
  structural.

## Testing and acceptance

1. **Existing suites pass:**
   - the join unit tests, including the collision fuzz;
   - `impl/tests/join_pipeline.rs`, where tests that pinned a rebuild for
     these shapes change to expect kept history, each change noted;
   - all fuzz drivers.
2. **New contract tests in `join_pipeline.rs`**, for INNER, LEFT, RIGHT and
   FULL. Each:
   - uses `replayed`, which checks deletion payloads and old values against
     the view's own snapshot;
   - compares `rows_sorted` with a fresh join built in the test;
   - asserts history was kept.

   The first four fail today, because they rebuild:
   - **A new customer and its first order,** ticked together.
   - **Old values in payloads:** in one sync, edit a customer's name, insert
     another customer (so the right side is structural and phase 1 runs),
     and delete an order matched to the edited customer. The order's
     deletion payload must carry the **old** name.
   - **A key update before an insert** on the same side.
   - **A self-join** (one table as both parents, joined on two different
     columns) with inserts, deletes and key updates.

   And one characterization test, which passes today:
   - **Over the bound:** a two-sided batch with more than 256 events on one
     side still rebuilds.
3. **Fuzz:**
   - **The collision fuzz** (`impl/src/view/tests.rs`):
     - It adds history replay, and an exact-order comparison with a fresh
       join for root-table parents. This was deferred from the last review
       and matters more now. If exact order fails on paths this change
       doesn't touch, that is pre-existing: fall back to an order-free
       comparison plus replay, and record it rather than make order a
       contract.
     - It adds a self-join variant.
     - It calls `refresh()` every 10 steps, so the next two-sided sync
       rebuilds the right vector through the reverse-application path, which
       otherwise only runs on the first two-sided sync after a rebuild.
   - **The main fuzz pipeline** adds `agg_join` (base FULL JOIN an aggregate,
     two-sided on many ticks) to `join_replayed()`, so its history is
     replay-checked.
   - **The batched join fuzz** records its new kept-history rates, and its
     floors rise to about half of them. Kept history should approach 100%.
4. **Benchmark:**
   - Run the "before" harness unchanged, since it asserts that
     `both_then_tick` rebuilds.
   - Then change that check to expect kept history, and run "after".
   - Budgets as in Goals.
5. **Lint:** clippy is clean.

## Comments and tests that describe the rebuild

- `join.rs`: the frame-mixing guard comment, `sync()`'s doc comment, and
  the `matching_rows` and pre-pass comments that state the old read rule.
- `forward_prop_fuzz.rs`: the `join_replayed` and `multiset` comments that
  mention the rebuild.
- `view/tests.rs`: tests and comments that expect a both-sides rebuild, and
  the collision fuzz's comment about odd trials rebuilding.
- `join_pipeline.rs`: `rebuilds_invalidate_history`, which must use a shape
  that still rebuilds, such as one over the bound, and
  `self_join_value_edits_update_both_halves`, if it pins a rebuild.

## Docs

- `.claude/rules/views-and-propagation.md`, the join bullet: the two phases,
  R_old reads, the frame rule for vectors, the 256-event bound, and no
  key-update-before-shift guard.
- `docs/JOIN_FEATURE.md`, the incremental behaviour section.
- The benchmark report: an "After the two-sided join fix" section.

## Risks

- **A phase-1 right read that bypasses reconstruction** gives payloads or
  matches in the wrong frame. Mitigations:
  - one routing point for right reads;
  - `replayed`'s payload checks;
  - the fuzz's replay checks, now in the collision fuzz too.
- **The deleted guard was still needed** for a reason not found here. The
  batched join fuzz exercises key updates before shifts on both sides, and
  the new contract test pins the shape.
- **Cost** is O(batch) per phase-1 right read, so O(B²) in the worst case
  with B ≤ 256, all in-memory.
