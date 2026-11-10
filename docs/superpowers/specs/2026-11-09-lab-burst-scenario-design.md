# Orders Lab: a burst scenario with engine timing

**Status:** Implemented 2026-11-09, with the three decisions as proposed. See
[As built](#as-built).

## Why

The Lab shows what travels over the wire. It cannot show how much work the
engine did, and it never sends the engine a batch large enough to matter:

- **A step is three mutations.** One amount edit among the first 512 rows,
  one delete at the tail and one append. The sorted view's one-pass replay
  starts at three events, and its gain grows with the batch: at 100k rows,
  256 scattered inserts fell from 67 ms to 0.7 ms.
- **The reply carries no time.** `LabComplete` returns the row count, the
  step and the mutation count. The only time on screen is "send to
  client-state completion", which the page itself labels as not an
  engine-only benchmark.

So the batch work from 2026-11-09 has no visible form outside the benchmark
harness.

## What

A sixth guided scenario, "Burst", plus the server support it needs.

### Server

1. **`LabAction::Burst { size }`.** `size` is 16, 64 or 256 mutations,
   applied before one tick. 256 is the filter's replay bound, so every node
   stays incremental and each delivery fits one `ViewDelta`.
   - Mutations cycle through four kinds: an amount edit, another amount
     edit, a delete, and an append. Row count stays steady.
   - Edited and deleted rows are spread over the whole table, not the first
     512. Positions and amounts are deterministic from the lab's step
     counter, as today's step is.
   - Edits that cross a client's threshold reach its filter as inserts and
     deletes in the middle, which reach its sort the same way. That is the
     case the batch work made cheap. (The server appends only, so base
     inserts are never in the middle; base ids must stay ascending.)
2. **Per-view sync time.** After the mutations, the engine syncs the
   requester's pipeline nodes in order, timing each `sync()`, then runs the
   usual tick for everyone else. Each node keeps a typed sync handle for
   this; today a node is only a `dyn ReadableTable`.
3. **Per-view rebuild time, for a burst only.** The engine builds a fresh
   view of the same kind over the same parent, times that, and drops it. It
   is the work `refresh()` does, measured without disturbing the live view.
   Steps skip it: it costs one pass over the table per node, too much for a
   10-per-second stream.
4. **`LabComplete` gains an optional `timing`:**

   ```json
   "timing": { "views": [
     { "node_id": "high-value", "sync_us": 0.0, "rebuild_us": 0.0 },
     { "node_id": "ranked",     "sync_us": 0.0, "rebuild_us": 0.0 },
     { "node_id": "regions",    "sync_us": 0.0, "rebuild_us": 0.0 }
   ] }
   ```

   `rebuild_us` is present for a burst only. `timing` is absent after a
   reset, and when the requester has no pipeline on the lab table.

### Frontend

5. **Scenario 06, "Batched replay".** Runs a 256-change burst. Expected
   path: three deltas, no snapshot, and engine time beside a rebuild. The
   tour's "05" labels become "06". Explore mode gets a burst-size selector
   and a "Run burst" button beside "Single step".
6. **An engine-time panel** under the result line. One row per view: its
   sync time, its rebuild time when present, and a bar showing the ratio.
   After a step it shows sync times only, so streaming has a live readout.
7. **The panel says what the numbers are:**
   - sync time is measured on the server around each view's `sync()`, for
     this client's views only;
   - it excludes applying the mutations, JSON, the network and rendering;
   - rebuild time is a fresh build of the same view over the same rows, in
     the same command; a real rebuild would also resend the whole view as a
     snapshot.

## What the scenario does and does not show

- **Shows:** a 256-change tick stays incremental end to end, and what each
  view's sync costs beside rebuilding it, on the viewer's own machine.
- **Does not show:** the before and after of the sorted view's change. The
  slow path no longer runs for batches of three or more events, so there is
  nothing live to compare with. The page must not print a "before" number.
- **Does not show:** anything about joins. The server builds no join nodes.

Predicted at 100k rows, to be replaced by measurements: the sort syncs in
under 1 ms against a rebuild near 8 ms; the filter is likely the largest
sync, a few ms, because it still applies events one at a time.

## As built

Plan: `docs/superpowers/plans/2026-11-09-lab-burst-scenario.md`.

- **The predictions held at 100,000 rows.** A 256-change burst synced the
  sort in 0.81 ms against an 8.4 ms rebuild, the filter in 4.3 ms against
  30.9 ms, and the group in 0.74 ms against 18.6 ms (median of 7, release
  build; the table is in `docs/ORDERS_LAB.md`).
- **On 1,000 rows, the default dataset, the sort's rebuild is cheaper** than
  replaying 256 events: 0.11 ms against 0.35 ms. The scenario text tells the
  viewer to load 100,000 orders first, and the panel shows the ratio either
  way ("3.2× a rebuild", with the bar in the accent colour).
- **A single edit is timed too,** not only steps and bursts. Only a reset
  has no timing.
- **Two captions were stale and are corrected:** the pipeline graph and the
  snapshot counter said regional totals receive snapshots. They have
  received deltas since protocol v4, which scenario 06 relies on.
- **The verification limit did not apply in the end.** A Node 22 build was
  unpacked into a scratch directory, so lint, the type check, the unit tests,
  the build and the browser tests all ran locally.

## Decisions to confirm

1. **Protocol version stays 4.** Both wire changes are additive: a new
   action, and an optional field old clients ignore. The rule is to bump on
   breaking changes. The alternative is 5, which makes old pages warn.
2. **Burst sizes are fixed at 16, 64 and 256,** like the dataset sizes, not
   a free number.
3. **Rebuild timing runs on every burst.** It adds roughly one rebuild of
   each view to the command, tens of ms at 100k rows. The alternative is a
   separate "measure rebuild" control.

## Not included

- **A switch that forces the per-event path** so the page could show a live
  "before". It would put a demo-only branch in the sorted view.
- **Timing the mutations themselves,** or a total "engine time". Applying a
  mutation on the server includes JSON conversion and the delivery journal,
  so the number would not be the table's.
- **Bursts above 256,** which rebuild the filter by design.
- **Server-side joins.**

## Acceptance

- **Rust:**
  - a 256 burst leaves the row count unchanged and delivers a `ViewDelta`,
    not a snapshot, for the filter, the sort and the group;
  - after several bursts each node's `QueryView` snapshot equals a freshly
    built pipeline's;
  - `timing` has one entry per requester node, with `rebuild_us` for a
    burst and without it for a step;
  - a second connection's pipeline stays correct when the first bursts;
  - sizes other than 16, 64 and 256 are rejected.
- **Frontend:** unit tests for the new action and for the hook keeping
  `timing`; the browser test runs scenario 06 and sees three deltas, no
  snapshot and the panel.
- **Docs:** `docs/ORDERS_LAB.md`, `docs/WEBSOCKET_PROTOCOL.md` and the
  server rule file.
- **Full suite:** Rust tests with the fuzz, clippy, the Python suite, and
  the frontend lint, unit tests and build.

## Verification limit

Node.js is not installed on the machine this was written on. The frontend
change can be written there, but its unit and browser tests cannot be run
there. CI runs the frontend job on push; nothing runs the page in a browser
until someone does so by hand or installs Node.
