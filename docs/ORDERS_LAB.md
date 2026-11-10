# LiveTable Orders Lab

A local demonstration of the real Rust engine and protocol-v4 client. It replaces
the Forward Prop Demo; the redesigned editor lives at `/#editor` on the separate
`demo` table. No user data is needed.

## Start locally

The one-command launcher builds the release server, starts both services on
loopback, and stops its own children on Ctrl+C:

```bash
cd frontend
npm install
npm run lab
```

It refuses occupied ports rather than stopping or reusing another server.
Override `LAB_PORT` (8080) and `LAB_UI_PORT` (5173) if needed. The frontend
WebSocket URL follows the selected backend port automatically.

For separate terminals:

Use two terminals, each starting at the repository root. Rust and Node.js 20+
(22+ recommended for the browser-test toolchain) are required.

```bash
# Terminal 1: release mode matters for meaningful timing
cargo run --release --manifest-path impl/Cargo.toml --features server --bin livetable-server -- --lab
```

```bash
# Terminal 2
cd frontend
npm install
npm run dev -- --host 127.0.0.1
```

Open `http://127.0.0.1:5173`. WebSockets default to port 8080 on the page's
hostname. Override `PORT` for the backend and `VITE_LIVETABLE_WS_URL` for Vite
when using another endpoint. The `--lab` CLI refuses non-loopback `HOST` values.
Without `--lab`, the original two-row `demo` service is unchanged and lab
commands return an explicit disabled error.

The lab starts with 1,000 deterministic synthetic orders: `order` and `quantity`
are INT32, `amount` is FLOAT64, and `region` and `product` are interned STRING
columns. All are non-nullable and use fast-reads storage. Displayed order numbers
are business data; only base snapshot `row_id` values authorize mutations.

## A two-minute tour

Start at the default $1,000 threshold and run each numbered scenario:

1. **Selective propagation:** edit a below-threshold order. Observe a base delta
   and no filter, sort, or group delivery.
2. **Incremental membership:** promote an order above the threshold. Filter and
   ranked results receive insertions; regional totals receive a group delta.
3. **Incremental ordering:** move a qualifying order to rank one. Inspect the
   ranked delta's delete and insert operations.
4. **Independent clients:** open client B, defaulting to a $2,500 threshold. Run
   a shared mixed batch or edit a source order. Both connections share the base
   but have independent pipeline definitions and delivery baselines.
5. **Snapshot recovery:** discard one incoming filter delta *in this client*,
   before reconciliation. The normal `PipelineStatus` watermark detects the
   missing final delivery, the hook sends `QueryView`, and a snapshot repairs
   the baseline. There is no fake recovery timer or mocked server.
6. **Batched replay:** apply 256 changes in one tick. Every view receives one
   delta and no snapshot, and the engine-time panel shows what each view's
   sync cost beside a rebuild of it. Load 100,000 orders first: on a small
   table a rebuild is the cheaper path (see [Engine time](#engine-time)).

The graph branches from high-value orders into ranked orders and regional
totals; grouping does not depend on sorting. Select a node to inspect its entire
result through a virtualized table. In All orders, select an order ID to edit
its amount. Derived rows remain read-only.

Each guided run clears client counters and the trace first. “No delivery” means
no message for that node since clearing, not proof of zero engine work. Sequence
numbers are delivery baselines, not engine tick counts.

## Explore and stream

Reset/load accepts exactly 1,000, 10,000 or 100,000 rows. UI confirmation names
the shared synthetic table: reset affects all lab clients, never the editor's
`demo` data. The source table, row-ID allocator and change clock remain intact;
installed views receive ordinary fallback snapshots without reconnecting. Reset
restores deterministic business values, not old wire IDs or sequence numbers.

Single step performs update + delete + insert before one engine tick. Streaming
uses the same mixed batch at a target ceiling of 1, 2, 5 or 10 batches/second.
Only one command is in flight per streaming client; slow delivery lowers the
achieved rate instead of growing a command queue. Pause stops scheduling after
the current batch. Row count stays steady. Runs cap at one million batches
until reset. Other clients can stream independently.

**Run burst** applies 16, 64 or 256 mutations before one engine tick: two
amount edits, a delete and an append, repeated, so row count stays steady.
Edited and deleted rows are spread over the whole table. An edit that crosses a
client's threshold reaches its filter, and the views over it, as an insert or
delete in the middle. The server only appends to the base table, so base
inserts are never in the middle. 256 is the filter's replay bound: a larger
batch would rebuild it.

Generic protocol edits can also change the synthetic table, so reset if a guided
scenario has no suitable row. Concurrent clients can change a chosen row between
selection and execution; use one active publisher for repeatable demonstrations.

## Table editor

Open **Table editor** in the navigation, or visit `/#editor`. It shares the lab's
visual style but edits the separate `demo` base table; lab resets never affect it.
Use **Open another editor** to watch confirmed changes arrive in a second tab.

- Click a cell to edit. **Enter** or blur submits; **Escape** discards the current
  draft; **Tab** submits and moves focus. The input returns to its confirmed value
  until the server reflects the change. Rejected values produce a visible error.
- Search and column-header sorting are local to each browser, not server pipeline
  operations. Search covers all loaded rows; pagination renders at most 25 at a
  time and stays on the current page across updates. Mutations use stable wire
  row IDs, never a display position or an editable business ID.
- Select a row number or cell for its server-confirmed record inspector.
  **Add row** opens a draft form; only **Create row** sends an insertion.
  Deletion requires explicit confirmation and has no undo.
- Editing waits for the initial/reconnected snapshot. One mutation is allowed
  in flight per editor. Generic writes have no correlated request IDs: the status
  confirms a matching server state, not a transactional receipt or durability.
  On disconnect or a 10-second confirmation timeout, inspect the refreshed table
  before retrying; mutations are never automatically replayed.

The flat snapshot includes column names and values, not schema types/nullability.
Input types are inferred from observed non-null values and retained across nulls;
the known `demo.amount` field also works when the table is empty. All-null columns
without an observed type default to text. Finite-number and boolean syntax checks
run locally; the server still validates the actual schema. This is not a schema
editor. Data is in memory and is lost when the server restarts.

## Measurement boundaries

- **Received JSON:** UTF-8 bytes of valid current-table/current-generation
  messages seen by this client. Includes initial/reset/repair snapshots,
  intentionally discarded deliveries, flat echoes, watermarks and command
  replies. Excludes WebSocket/TCP framing and outgoing requests.
- **Deltas/snapshots:** received `ViewDelta`/`ViewData` counts, not engine events.
- **Recoveries:** applied repair snapshots and requested repairs, including retries.
- **Completion:** client `performance.now()` from command send until its
  `LabComplete` reply arrives after queued view deliveries; the recovery scenario
  additionally waits for the filter repair snapshot. Includes parsing and
  reconciliation, not React commit, browser layout or paint. Not an engine-only
  latency or cross-machine timestamp subtraction.
- **Trace:** only the latest 80 summaries; raw snapshot payloads are not retained.

Initial/repair snapshots remain O(N), unchunked transfers. Derived rows,
including groups, have no stable row identity. Client deltas shallow-copy row arrays. Virtualization bounds DOM work,
not all client-state work. The storage panel points to the separate measured
[column-layout benchmark](TYPED_COLUMN_STORAGE.md), not a fabricated memory gauge.
Use the [delivery benchmark](PIPELINE_DELIVERY.md) for controlled comparisons;
its workload and measurement scope differ. Neither is a latency guarantee.

## Engine time

After a step, a burst or an edit, the reply carries server-measured times for
this client's three views, shown under the result line:

- **Sync** is each view's own `sync()` for the batch. The server syncs the
  requester's views one at a time, in pipeline order, and times each call;
  other clients' views are synced afterwards in the ordinary tick.
- **Rebuild**, after a burst only, is a fresh view of the same kind built over
  the same parent in the same command, then dropped. It is the work a rebuild
  does. A real rebuild would also invalidate the view's history and resend
  the whole view as a snapshot, which this number does not include.
- Neither includes applying the mutations, JSON conversion, the network or
  rendering. The panel is not a "before and after" of any engine change: it
  compares today's incremental path with rebuilding.

One machine's numbers, to set expectations (Apple M1 Pro, release build,
median of 7 bursts per row, threshold $1,000; from a temporary program that
called the engine directly, not committed):

| Orders | Burst | Filter sync / rebuild | Sort sync / rebuild | Group sync / rebuild |
|---:|---:|---:|---:|---:|
| 100,000 | 256 | 4.3 ms / 30.9 ms | 0.81 ms / 8.4 ms | 0.74 ms / 18.6 ms |
| 100,000 | 64 | 1.1 ms / 31.0 ms | 0.49 ms / 7.9 ms | 0.20 ms / 18.7 ms |
| 10,000 | 256 | 0.52 ms / 3.1 ms | 0.30 ms / 0.93 ms | 0.17 ms / 1.9 ms |
| 1,000 | 256 | 0.26 ms / 0.45 ms | 0.35 ms / 0.11 ms | 0.17 ms / 0.30 ms |

- **The filter is the largest sync** at 100,000 rows. It still applies a batch
  one event at a time; the sort applies it in one pass.
- **On 1,000 rows the sort's rebuild is cheaper than replaying 256 events.**
  Replaying costs a fixed amount per event, and a rebuild of a small view
  costs less than that. The panel shows this as it is, with the bar in the
  accent colour and "3.2× a rebuild".

## Demo control extension

The additive `LabCommand` extension (since protocol v3) cannot select a table: it always
targets the opt-in `lab`. Example requests:

```json
{"type":"LabCommand","request_id":1,"action":{"kind":"reset","rows":1000}}
{"type":"LabCommand","request_id":2,"action":{"kind":"step"}}
{"type":"LabCommand","request_id":3,"action":{"kind":"update","row_id":1001,"amount":1250}}
{"type":"LabCommand","request_id":4,"action":{"kind":"burst","size":256}}
```

The example row ID is illustrative; use a real current base ID. Amounts must be
finite, between 0 and 1,000,000. A burst's `size` must be 16, 64 or 256. Invalid
sizes are rejected before mutation.
Replies go only to the requester:

```json
{"type":"LabComplete","request_id":2,"rows":1000,"step":1,"mutations":3,
 "timing":{"views":[{"node_id":"high-value","sync_us":2.6},{"node_id":"ranked","sync_us":6.0}]}}
{"type":"LabComplete","request_id":4,"rows":1000,"step":2,"mutations":256,
 "timing":{"views":[{"node_id":"high-value","sync_us":262.0,"rebuild_us":449.9}]}}
{"type":"LabError","request_id":3,"message":"Row '1001' not found"}
```

`timing` (added 2026-11-09, protocol version unchanged) lists the requester's
pipeline nodes in pipeline order with server-measured microseconds; see
[Engine time](#engine-time). `rebuild_us` is present after a burst only.
`timing` is absent after a reset, and when the requester has no pipeline on
the lab table. Clients that predate it ignore the field.

Request IDs correlate replies; they are not deduplication keys or durable acks.
A disconnect or 30-second timeout does not prove a mutation was rejected. The
client stops streaming and never automatically retries mutations. Reset also
broadcasts a monotonic `TableData` baseline to flat subscribers before pipeline
delivery. Existing non-lab protocol contracts are unchanged. This is an in-memory
local demo, not an authenticated hosted service.

## Verification

```bash
cargo test --manifest-path impl/Cargo.toml --features server --lib
cd frontend
npm test -- --silent
npm run lint
npm run build
npm run test:e2e
```

End-to-end tests use installed Google Chrome in an isolated headless profile,
start their own Rust lab on 8087 and Vite on 5180, and stop both afterward. They
refuse occupied ports and do not touch a lab on 8080. Release builds and
localhost/browser launch permissions are required. Screenshots/failure traces
are ignored artifacts in `frontend/test-results/`. Coverage includes guided
scenarios, a 256-change burst with its engine-time panel, independent clients,
shared reset, 100k scrolling, bounded streaming,
mobile layout, reset cancellation, and the redesigned editor's keyboard actions,
independent local search/sort, two-client edits, server rejection, draft creation
and confirmed deletion. Unit tests additionally cover pagination, stable row IDs,
confirmation timeouts, disconnects and stale-socket isolation.
