# Incremental pipeline delivery

Protocol v3 extends the engine's incremental filter/sort work across the
WebSocket boundary. Base/filter/sort nodes send ordered changes after an initial
snapshot, with safe snapshot recovery. Aggregate nodes retain snapshots.
Rust/Python table and view APIs are unchanged; the optional server delivery API
and pipeline wire sequencing have changed. Upgrade server and browser together.

The [protocol reference](WEBSOCKET_PROTOCOL.md) is canonical for message shapes,
sequence rules, recovery, and limits.

## Delivery design

- Every connection/table/generation/node has its own delivery sequence, starting
  at zero. View versions decide when to inspect a node; changeset cursors decide
  whether its history covers the previous baseline. Neither is the wire sequence.
- The base journal records step-local coordinates and inserted row IDs at mutation
  time, before tick compacts root history. It retains at most 512 operations and
  is drained after collection. A consumer outside that window receives a snapshot.
- Filter/sort deltas serialize retained node-coordinate changesets directly.
  There is no full snapshot comparison or base-ID-vector clone on the delta path.
  Empty output sends no node payload and leaves its delivery sequence unchanged.
- Batches contain 1–512 ordered insert/delete/update operations. A sort move is
  delete + insert; an operation's index refers to that step, not the final layout.
  Missing/invalidated history and oversized batches use a snapshot.
- Groups retain inherited-version-triggered snapshots, including after some
  excluded edits. Their filter/sort descendants may rebuild and use snapshots too.
- Snapshot failures report a node error without advancing the delivery baseline.
  The client atomically validates and applies batches; invalid operations leave
  its previous coherent rows intact.
- Generation-scoped `QueryView` repairs one node without replacing the pipeline.
  It advances only that connection/node's sequence, even if content is unchanged.
  One-second `PipelineStatus` watermarks reveal lost final/initial deliveries.
- The hook issues at most one outstanding repair per node, checks retries at
  three-second intervals, and holds no pending delta queue. A newer snapshot
  rebaselines it. New generations clear old snapshots/repairs; stale socket,
  table, generation, and unknown-node messages cannot alter current state.

Derived rows still have null wire IDs: ordered, baseline-checked coordinates
make replay possible without inventing stable identity. Only real base IDs
authorize base-row mutations. Delivery is atomic per node/batch, not a
simultaneous multi-node UI transaction.

## Verification

- Engine reconstruction tests compare 640 seeded mixed batches with freshly
  built pipelines, in addition to the existing 500 single-tick shadow-model cases.
- Targeted tests cover excluded-output suppression, historical coordinates,
  transient inserted/deleted IDs, 257/513-event fallback, independent connection
  baselines, mid-batch subscription, generation rejection, and group descendants.
- A read-counting probe verifies the delta collector reads no complete source
  rows; injected snapshot failure verifies the cursor/sequence remain retryable.
- The actual TCP/WebSocket integration test deliberately drops a final filter
  delivery, detects it with a checkpoint, and queries a new baseline. It also
  checks mutations, replacement generations, and independent connections.
- Frontend tests cover atomic rollback, duplicates, gaps, pre-snapshot deltas,
  malformed rows/snapshots, lost repair retries, stale callbacks, removed nodes,
  prototype-named IDs, sorted moves, and delta-driven React rendering.

From the repository root:

```bash
cargo test --manifest-path impl/Cargo.toml --lib --features server
cargo test --manifest-path impl/Cargo.toml --features server --test protocol_v3_websocket
(cd frontend && npm run test && npm run lint && npm run build)
```

See [the test guide](../tests/README.md) for core propagation and Python checks.
Final validation passed 715 project tests (including 434 Python tests against a
fresh isolated wheel), 16 Rust doctests (one existing ignored), all three Clippy
configurations, frontend lint/build, and 111 local documentation links plus
17 executable documentation blocks and the quickstart tutorial.

## Reproducible measurement

Server harness: [pipeline_delivery_server.rs](../impl/examples/pipeline_delivery_server.rs).
Client harness: [pipeline_delivery.mjs](../benchmarks/pipeline_delivery.mjs).

```bash
cargo build --manifest-path impl/Cargo.toml --release --features server --example pipeline_delivery_server
node benchmarks/pipeline_delivery.mjs "$PWD/impl/target/release/examples/pipeline_delivery_server" 10000 100000
```

Requires Node 24+ (native WebSocket and TypeScript stripping) and localhost
binding permission. Each size starts a new ephemeral Actix server with the same
seed rows; the harness closes its socket and terminates only that child server.
One connection subscribes to the base plus filter → descending sort → SUM(group)
pipeline. About half the rows qualify. The seed has repeated region labels,
numeric amounts, and unique product labels.
Excluded edits target base ID 1; included product edits and sort-key moves
target ID 2. These early IDs keep the server's linear ID lookup near its best
case; the benchmark is not a random-row-access measurement.

Each sample measures sending an update through the real server and receiving,
parsing, and reconstructing client state through the final group snapshot.
The client uses the production delta reducer. It accepts the old snapshot-only
protocol too, making the client workload identical in before/after runs.
Five warmups precede 31 samples for each workload; p95 is nearest-rank.
After each workload, independent filter/sort/group recomputation checks the
reconstructed state outside the timed interval.

Included: request encoding, localhost transport, engine mutation/tick, server
row conversion/JSON serialization, client JSON parsing, and delta row-array
copy/application. Downstream byte counts include the flat-table update echo and
pipeline messages, but exclude WebSocket/TCP framing, upstream requests, and
periodic checkpoints (reported separately by the harness).

Excluded: seeding, initial snapshots, independent QA recomputation, React
rendering, browser layout/paint, and the hook's runtime message/schema validation.
This is end-to-end **localhost client-state delivery**, not browser paint latency.
It does not measure recovery cost, multi-client load, process memory, or WAN
behavior. No performance threshold is enforced in CI.

## Recorded comparison

Recorded September 5, 2026 (America/Los_Angeles), Apple M2, 24 GiB RAM,
macOS 26.5.2 (25F84), arm64, rustc 1.91.1 (ed61e7d7e 2025-11-07), release,
Node v24.11.1. Before: `ef3ab0ee95037d45cb5aff5021c3838d8da2c48c`, exported into
a clean temporary directory, with only the identical server harness and
pre-seeded-server startup hooks added. Its snapshot delivery implementation
was unchanged. After: the protocol-v3 implementation accompanying this report.

Two alternating before/after pairs used 10,000 and 100,000 rows. The first pair
primed the workload; the second pair is reported below. There were no concurrent
agent-started builds or tests during measurement. CPU scheduling, frequency,
thermal state, and Node garbage collection were uncontrolled.

| Rows | Workload | Median downstream JSON bytes, before → after | Latency median / p95 ms, before → after |
|------|----------|--------------------------------------------:|--------------------------------------:|
| 10,000 | Excluded amount update | 1,637,720 → 726 | 17.548 / 18.135 → 0.127 / 0.175 |
| 10,000 | Included product update | 1,637,733 → 1,111 | 18.818 / 19.262 → 0.133 / 0.525 |
| 10,000 | Sort-key move | 1,637,728 → 1,176 | 17.833 / 18.565 → 0.364 / 0.397 |
| 100,000 | Excluded amount update | 16,667,730 → 731 | 187.698 / 192.577 → 0.233 / 0.315 |
| 100,000 | Included product update | 16,667,743 → 1,117 | 188.476 / 192.663 → 0.338 / 0.423 |
| 100,000 | Sort-key move | 16,667,738 → 1,182 | 192.746 / 194.789 → 3.049 / 4.723 |

At 100,000 rows, the measured payload reduction exceeds 99.99% in all three
workloads. Excluded edits now send three messages (flat-table echo, base delta,
group snapshot), versus five before. Included edits still send five messages,
but three are compact node deltas. These short v3 runs completed before the
first periodic checkpoint; zero checkpoint bytes observed in those runs does
**not** mean zero ongoing checkpoint overhead for an idle connected client.

The first pair had 100,000-row v3 medians of 0.202, 0.324, and 2.732 ms in the
same workload order. These variations reinforce that timings are measurements,
not latency guarantees. Sorted moves still do index maintenance and client
array shifting; stationary deltas still shallow-copy row arrays. Large initial
and repair snapshots remain O(N) row serialization/transfer and are unchunked.
High-cardinality aggregate snapshots can still dominate delivery cost.
Recovery assumes traffic eventually flows again; this milestone does not bound
all transport queues or add durable acknowledgements/backpressure control.

Final SHA-256 source fingerprints (test-only additions do not change measured paths):

```text
7316342fd9cec2d9f47e6086c9754ee0d6519d69c7990805363bd503b48f695e  impl/src/engine.rs
2e8592a0ba561fc4bb42bf39593480ca019ee203d4308b4d3e4b003f39d49380  impl/src/engine/delivery.rs
7366d35e73139e7cf79169094528550bca0a68cdea556361f701c54803603be0  impl/src/messages.rs
fc4ef8716130f9d29aab19e9a0829c0fff133bb5d7ce46065cb3956ea2708f7d  impl/src/websocket.rs
1dc1bfc645fa44533b65ed97cb1080452514c3b4114ae8a87d5e3d226d14170c  impl/examples/pipeline_delivery_server.rs
49e5284f18d9e6aceedf31e91e94a56db0568f364b5937339abdd84c81839358  frontend/src/lib/pipelineReconciliation.ts
9a4325f3982ea5e6645834ee132554e6b9239f8d4703a22b525513c5f58173b0  benchmarks/pipeline_delivery.mjs
```
