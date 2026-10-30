# View hardening: checks and tests from deferred review findings

**Status:** Implemented 2026-10-29

## Why

The reviews of the 2026-10-26 to 2026-10-29 join and aggregate work deferred
a set of minor findings. None is a known bug. Each is a check or test that
would catch a future one sooner, before the server-joins work builds on these
views.

## What

1. **Join bookkeeping check.** `JoinView::check_bookkeeping() -> Result<(),
   String>`, `#[doc(hidden)] pub`, for tests. It confirms three things:
   - each side's row counter equals its parent's length;
   - every built key-hash vector equals one recomputed from the live parent;
   - every `join_index` entry is in range.

   The join fuzz drivers, the pipeline fuzz and the collision fuzz call it
   after every sync. It is not run inside `sync` itself: it costs O(N), and
   debug-built servers with large tables would pay it on every tick.
2. **Join read rule, asserted.** Each side records whether its batch is still
   unapplied during a sync. Debug assertions fire on a live read of a side
   with an unapplied batch: a row read, or a key-hash vector built from the
   live parent. Phase 1 reads the right through `right_before`, which is
   allowed.
3. **Aggregate removal checks.** Three paths silently skip a missing piece of
   state:
   - removing a row from a group that doesn't exist;
   - a value update for a row whose group doesn't exist;
   - removing a value from a column with no state.

   Each gets a debug assertion, like the existing ones in `remove_value`.
4. **Aggregate batch-length check.** Before applying a batch, compute the
   length it leaves. If that disagrees with the parent's length, rebuild.
   - This matches the join's handling of a parent whose changeset disagrees
     with its rows. Today the aggregate would silently drift.
   - It changes release behaviour only for such broken parents.
5. **Join 256-event boundary pinned.** A two-sided batch with exactly 256
   events on each side keeps history. 257 on the right side rebuilds; 257 on
   the left is already tested.
6. **Batched Int32 group-by fuzz.** Odd trials apply 1–5 mutations per tick,
   so multi-event incremental batches run over the state the Int32 fast path
   built. The fast path itself runs only in rebuilds, here at construction.
   Even trials keep one mutation per tick.
7. **Python signed-zero join test checks signs.** The joined row keeps the
   left's -0.0 and the right's 0.0.

## Not included

- **The Int32 fast-path special case** for rows whose group value isn't
  Int32. Today's parents can't produce those rows.
- **View parents on both sides of a fuzzed join.** Already covered:
  `view_parent_join` puts a filter on the left and a sort on the right.
- **The small performance items** from the same reviews.

## Acceptance

- **Each new check fails under a deliberate bug:**
  - (1) skipping a key-hash vector update;
  - (2) dropping `right_before` in phase 1;
  - (4) a parent whose changeset disagrees with its rows;
  - (5) moving the bound by one.
- **Full suite:** fuzz, clippy and `run_all.sh` all pass.
