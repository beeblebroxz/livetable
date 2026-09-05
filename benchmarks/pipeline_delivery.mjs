// Node 24+; native WebSocket and TypeScript stripping, no extra dependencies.
// Usage: node benchmarks/pipeline_delivery.mjs /absolute/server/binary 10000 100000
// Measures request -> Rust actor/engine -> JSON/WebSocket -> reconstructed client
// state. Uses the production delta reducer; excludes React render and hook/parser
// validation. The identical client also accepts protocol-v2 snapshot-only servers.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { applyViewDelta } from '../frontend/src/lib/pipelineReconciliation.ts';

const [binary, ...rowArguments] = process.argv.slice(2);
assert(binary, 'server binary required');
const sizes = rowArguments.length ? rowArguments.map(Number) : [10000, 100000];
const nodes = [
  { id: 'f', source_id: 'base', kind: 'filter', predicate: 'amount >= 150' },
  { id: 's', source_id: 'f', kind: 'sort', keys: [{ column: 'amount', descending: true }] },
  { id: 'g', source_id: 's', kind: 'group', group_by: ['region'], aggs: [{ alias: 'total', column: 'amount', op: 'sum' }] },
];
const quantile = (values, percentile) => [...values].sort((a, b) => a - b)[Math.ceil(values.length * percentile) - 1];

async function run(size) {
  assert(Number.isSafeInteger(size) && size >= 100);
  const server = spawn(binary, [String(size)], { stdio: ['ignore', 'pipe', 'inherit'] });
  const lines = createInterface({ input: server.stdout });
  let socket;
  try {
    const [line] = await Promise.race([
      once(lines, 'line'),
      once(server, 'exit').then(([code]) => { throw new Error(`server exited before readiness: ${code}`); }),
    ]);
    const { address } = JSON.parse(line);
    socket = new WebSocket(`ws://${address}/ws`);
    const snapshots = new Map();
    let awaiting;
    let bytes = 0;
    let messages = 0;
    let protocol;
    let checkpointBytes = 0;
    socket.addEventListener('message', ({ data }) => {
      try {
        const message = JSON.parse(data);
        if (message.type === 'PipelineStatus') {
          checkpointBytes += Buffer.byteLength(data);
          return;
        }
        bytes += Buffer.byteLength(data);
        messages += 1;
        if (message.type === 'Subscribed') protocol = message.protocol_version;
        if (message.type === 'Error' || message.type === 'ViewError') throw new Error(data);
        if (message.type === 'ViewData') {
          snapshots.set(message.node_id, { generation: message.pipeline_generation,
            nodeId: message.node_id, sourceId: message.source_id, kind: message.kind,
            seq: message.seq, columns: message.columns, rows: message.rows });
        } else if (message.type === 'ViewDelta') {
          const next = applyViewDelta(snapshots.get(message.node_id), message);
          assert(next, 'delta must apply to exactly the current baseline');
          snapshots.set(message.node_id, next);
        }
        if (message.type === 'ViewData' && message.node_id === 'g') awaiting?.resolve();
      } catch (error) { awaiting?.reject(error); }
    });
    await new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve, { once: true });
      socket.addEventListener('error', reject, { once: true });
    });
    const roundTrip = async (message) => {
      const done = new Promise((resolve, reject) => { awaiting = { resolve, reject }; });
      const timeout = setTimeout(() => awaiting?.reject(new Error('delivery timed out')), 30000);
      try {
        socket.send(JSON.stringify(message));
        await done;
      } finally { clearTimeout(timeout); awaiting = undefined; }
    };
    socket.send(JSON.stringify({ type: 'Subscribe', table_name: 'demo' }));
    await roundTrip({ type: 'SetPipeline', table_name: 'demo', pipeline_generation: 1, nodes });
    assert.equal(snapshots.get('base').rows.length, size);
    for (const workload of ['excluded_update', 'non_sort_update', 'sort_key_move']) {
      const timings = [], payloadBytes = [], frameCounts = [];
      for (let sample = 0; sample < 36; sample++) {
        const message = { type: 'UpdateCell', table_name: 'demo', row_id: workload === 'excluded_update' ? 1 : 2,
          column: workload === 'non_sort_update' ? 'product' : 'amount',
          value: workload === 'excluded_update' ? 50 + (sample % 2) * 25
            : workload === 'non_sort_update' ? `changed${sample % 2}` : (sample % 2 ? 200.75 : 1200.75),
        };
        bytes = 0; messages = 0;
        const start = performance.now();
        await roundTrip(message);
        const elapsed = performance.now() - start;
        if (sample >= 5) { timings.push(elapsed); payloadBytes.push(bytes); frameCounts.push(messages); }
      }
      // Independent recomputation, outside the measured interval.
      const base = snapshots.get('base').rows;
      const expectedFilter = base.filter(({ row }) => row.amount >= 150).map(({ row }) => ({ row_id: null, row }));
      assert.deepEqual(snapshots.get('f').rows, expectedFilter);
      assert.deepEqual(snapshots.get('s').rows, [...expectedFilter].sort((a, b) => b.row.amount - a.row.amount));
      const expectedGroups = new Map();
      for (const { row } of expectedFilter) expectedGroups.set(row.region, (expectedGroups.get(row.region) ?? 0) + row.amount);
      assert.deepEqual(new Map(snapshots.get('g').rows.map(({ row }) => [row.region, row.total])), expectedGroups);
      console.log(JSON.stringify({ rows: size, workload, protocol, samples: timings.length,
        median_ms: quantile(timings, 0.5), p95_ms: quantile(timings, 0.95),
        median_json_bytes: quantile(payloadBytes, 0.5), median_messages: quantile(frameCounts, 0.5) }));
    }
    console.log(JSON.stringify({ rows: size, checkpoint_json_bytes_during_run: checkpointBytes }));
  } finally {
    socket?.close();
    lines.close();
    if (server.pid && server.exitCode === null && server.signalCode === null) {
      const exited = once(server, 'exit');
      server.kill('SIGTERM');
      await exited;
    }
  }
}
for (const size of sizes) await run(size);
