# Orders Lab burst scenario: plan

**Spec:** `docs/superpowers/specs/2026-11-09-lab-burst-scenario-design.md`

## Global Constraints

- Both wire changes are additive; the protocol version stays 4.
- Existing scenarios and the step action behave as before.
- The page shows only times the server measured. No "before" figures.

### Task 1: Server

**Files:**
- `impl/src/messages.rs`;
- `impl/src/engine.rs`;
- `impl/src/engine/lab.rs`;
- `impl/src/engine/delivery.rs`;
- `impl/src/websocket.rs`.

- [ ] `LabAction::Burst { size }`; `ViewTiming`, `LabTiming`;
  `LabComplete.timing`, omitted when absent.
- [ ] `ViewNode` keeps a `sync` handle and a `rebuild` handle, set in
  `set_pipeline` from the typed view and its spec.
- [ ] `lab_command`: a burst of 16, 64 or 256 mutations, two edits, a delete
  and an append repeated, on rows spread over the table. Other sizes are
  rejected before any mutation.
- [ ] `lab_timing(conn, action)`: sync the requester's nodes in order, timing
  each; for a burst, time a fresh build of each. None after a reset or
  without a pipeline.
- [ ] The actor passes the connection ID, calls `lab_timing` before
  `propagate_views`, and returns the timing in `LabComplete`.
- [ ] Tests:
  - six 256 bursts with two connections: row count steady, a delta and no
    snapshot for every node on both, and each node equal to a fresh
    pipeline's at the end;
  - timing shape for a burst, a step, a reset and a connection with no
    pipeline; invalid sizes;
  - the JSON of `LabComplete` with and without timing;
  - an actor test: the requester's reply has timing and follows its deltas.

### Task 2: Frontend

**Files:**
- `frontend/src/types.ts`;
- `frontend/src/lib/lab.ts`, `lab.test.ts`;
- `frontend/src/hooks/useLab.ts`, `useLab.test.tsx`;
- `frontend/src/pages/OrdersLab.tsx`, `OrdersLab.css`;
- `frontend/e2e/orders-lab.spec.ts`.

- [ ] Scenario 06; `BURST_SIZES`; `formatMicros` and `formatShare`.
- [ ] The hook keeps the timing of its own latest command, drops it on a
  reply without one, on clear and on a new pipeline generation.
- [ ] The engine-time panel; the burst controls in Explore; the tour's
  hard-coded "05" and five-column grid.
- [ ] Unit tests for the action, the formatters and the hook.
- [ ] Browser test: scenario 06 at 10,000 rows shows four deltas, no
  snapshot and three timed rows with a rebuild; a step shows sync only; a
  burst of 64 from Explore shows the rebuild again.

### Task 3: Measure, docs and full suite

- [ ] Time bursts and steps at each dataset size with a throwaway program.
- [ ] `docs/ORDERS_LAB.md`, `docs/WEBSOCKET_PROTOCOL.md`, `README.md`,
  `.claude/rules/server-and-protocol.md`.
- [ ] Rust tests with the fuzz, the three clippy configurations, the Python
  suite, and the frontend lint, unit tests, build and browser tests.
