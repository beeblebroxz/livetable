# LiveTable Orders Lab

A local demonstration of the real Rust engine and protocol-v3 client. It replaces
the Forward Prop Demo; the original editor remains at `/#editor` on the separate
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
   and no filter/sort delivery. A group snapshot can still be sent.
2. **Incremental membership:** promote an order above the threshold. Filter and
   ranked results receive insertions.
3. **Incremental ordering:** move a qualifying order to rank one. Inspect the
   ranked delta's delete and insert operations.
4. **Independent clients:** open client B, defaulting to a $2,500 threshold. Run
   a shared mixed batch or edit a source order. Both connections share the base
   but have independent pipeline definitions and delivery baselines.
5. **Snapshot recovery:** discard one incoming filter delta *in this client*,
   before reconciliation. The normal `PipelineStatus` watermark detects the
   missing final delivery, the hook sends `QueryView`, and a snapshot repairs
   the baseline. There is no fake recovery timer or mocked server.

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

Generic protocol edits can also change the synthetic table, so reset if a guided
scenario has no suitable row. Concurrent clients can change a chosen row between
selection and execution; use one active publisher for repeatable demonstrations.

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

Initial/repair snapshots remain O(N), unchunked transfers; groups still send
snapshots. Client deltas shallow-copy row arrays. Virtualization bounds DOM work,
not all client-state work. The storage panel points to the separate measured
[column-layout benchmark](TYPED_COLUMN_STORAGE.md), not a fabricated memory gauge.
Use the [delivery benchmark](PIPELINE_DELIVERY.md) for controlled comparisons;
its workload and measurement scope differ. Neither is a latency guarantee.

## Demo control extension

The additive protocol-v3 `LabCommand` extension cannot select a table: it always
targets the opt-in `lab`. Example requests:

```json
{"type":"LabCommand","request_id":1,"action":{"kind":"reset","rows":1000}}
{"type":"LabCommand","request_id":2,"action":{"kind":"step"}}
{"type":"LabCommand","request_id":3,"action":{"kind":"update","row_id":1001,"amount":1250}}
```

The example row ID is illustrative; use a real current base ID. Amounts must be
finite, between 0 and 1,000,000. Invalid sizes are rejected before mutation.
Replies go only to the requester:

```json
{"type":"LabComplete","request_id":2,"rows":1000,"step":1,"mutations":3}
{"type":"LabError","request_id":3,"message":"Row '1001' not found"}
```

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
scenarios, independent clients, shared reset, 100k scrolling, bounded streaming,
mobile layout, reset cancellation and the preserved editor.
