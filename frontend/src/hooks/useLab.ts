import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { LabAction } from '../types';
import { usePipeline, type PipelineEvent } from './usePipeline';
import { labPipeline } from '../lib/lab';

export interface TraceEntry { id: number; at: number; node: string; kind: string; bytes: number; detail: string }
export interface NodeDelivery { kind: string; bytes: number; operations: number; seq: number; indices: number[] }
export interface LabStats {
  bytes: number; snapshots: number; deltas: number; statuses: number; repairs: number; recovered: number;
  events: TraceEntry[]; nodes: Record<string, NodeDelivery>;
}
const emptyStats = (): LabStats => ({ bytes: 0, snapshots: 0, deltas: 0, statuses: 0, repairs: 0, recovered: 0, events: [], nodes: {} });
interface Pending {
  id: number; label: string; start: number; ack: boolean; recovery: string | null;
  timer: number; resolve: () => void; reject: (error: Error) => void;
}

export function useLab(threshold: number) {
  const [stats, setStats] = useState(emptyStats);
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState('');
  const [result, setResult] = useState<{ label: string; ms: number } | null>(null);
  const pendingRef = useRef<Pending | null>(null);
  const counterRef = useRef(0);
  const eventIdRef = useRef(0);

  const finish = useCallback((error?: string) => {
    const pending = pendingRef.current;
    if (!pending) return;
    window.clearTimeout(pending.timer);
    pendingRef.current = null;
    setBusy(false);
    if (error) { setProblem(error); pending.reject(new Error(error)); }
    else { setResult({ label: pending.label, ms: performance.now() - pending.start }); pending.resolve(); }
  }, []);

  const onEvent = useCallback((event: PipelineEvent) => {
    if (event.kind === 'generation') {
      finish('Pipeline replaced. Run the scenario again after it loads.');
      setStats(emptyStats()); setResult(null);
      return;
    }
    if (event.kind === 'disconnected') { finish('Connection lost. The command may have been applied; inspect the reconnected state before retrying.'); return; }
    if (event.kind === 'applied') return;
    let entry: TraceEntry | undefined;
    let node: NodeDelivery | undefined;
    const increments = { bytes: 0, snapshots: 0, deltas: 0, statuses: 0, repairs: 0, recovered: 0 };
    if (event.kind === 'received') {
      const message = event.message;
      increments.bytes = event.bytes;
      const nodeId = 'node_id' in message ? message.node_id : 'transport';
      if (message.type === 'ViewData' || message.type === 'ViewDelta') {
        const snapshot = message.type === 'ViewData';
        const operations = snapshot ? message.rows.length : message.changes.length;
        increments[snapshot ? 'snapshots' : 'deltas'] = 1;
        const detail = snapshot ? `${operations.toLocaleString()} rows · seq ${message.seq}`
          : message.changes.slice(0, 6).map(change => `${change.type === 'CellUpdated' ? `update ${change.column}` : change.type === 'RowInserted' ? 'insert' : 'delete'} @ ${change.index}`).join(' · ');
        entry = { id: ++eventIdRef.current, at: event.at, node: nodeId, kind: snapshot ? 'snapshot' : 'delta', bytes: event.bytes, detail };
        node = { kind: entry.kind, bytes: event.bytes, operations, seq: message.seq,
          indices: snapshot ? [] : message.changes.filter(change => change.type !== 'RowDeleted').slice(0, 20).map(change => change.index) };
      } else if (message.type === 'PipelineStatus') {
        increments.statuses = 1;
        entry = { id: ++eventIdRef.current, at: event.at, node: 'transport', kind: 'watermark', bytes: event.bytes, detail: 'Per-node delivery sequences; no row payload' };
      } else if (message.type === 'LabComplete') {
        if (pendingRef.current?.id === message.request_id) {
          pendingRef.current.ack = true;
          if (!pendingRef.current.recovery) finish();
        }
        entry = { id: ++eventIdRef.current, at: event.at, node: 'server', kind: 'complete', bytes: event.bytes, detail: `${message.mutations.toLocaleString()} mutations · ${message.rows.toLocaleString()} rows · batch ${message.step}` };
      } else if (message.type === 'LabError') {
        if (pendingRef.current?.id === message.request_id) finish(message.message);
        entry = { id: ++eventIdRef.current, at: event.at, node: 'server', kind: 'error', bytes: event.bytes, detail: message.message };
      }
    } else if ('nodeId' in event) {
      if (event.kind === 'repair') increments.repairs = 1;
      if (event.kind === 'recovered') {
        increments.recovered = 1;
        if (pendingRef.current?.recovery === event.nodeId) {
          pendingRef.current.recovery = null;
          if (pendingRef.current.ack) finish();
        }
      }
      entry = { id: ++eventIdRef.current, at: event.at, node: event.nodeId, kind: event.kind, bytes: 0,
        detail: event.kind === 'dropped' ? 'Client intentionally discarded this delta before reconciliation'
          : event.kind === 'repair' ? 'QueryView requested a coherent baseline' : 'Repair snapshot applied; client is coherent again' };
    }
    const traceEntry = entry;
    const delivery = node;
    setStats(previous => ({
      bytes: previous.bytes + increments.bytes, snapshots: previous.snapshots + increments.snapshots,
      deltas: previous.deltas + increments.deltas, statuses: previous.statuses + increments.statuses,
      repairs: previous.repairs + increments.repairs, recovered: previous.recovered + increments.recovered,
      events: traceEntry ? [traceEntry, ...previous.events].slice(0, 80) : previous.events,
      nodes: traceEntry && delivery ? { ...previous.nodes, [traceEntry.node]: delivery } : previous.nodes,
    }));
  }, [finish]);

  const nodes = useMemo(() => labPipeline(threshold), [threshold]);
  const pipeline = usePipeline('lab', nodes, undefined, { onEvent, allowFaultInjection: true });
  const pipelineRef = useRef(pipeline);
  pipelineRef.current = pipeline;

  const run = useCallback((action: LabAction, label: string, recovery = false): Promise<void> => {
    if (pendingRef.current) return Promise.reject(new Error('Wait for the current command to finish.'));
    const current = pipelineRef.current;
    if (!current.connected) return Promise.reject(new Error('Connect the local lab server first.'));
    if (recovery && !current.dropNextDelta('high-value')) return Promise.reject(new Error('Wait for the filter baseline before testing recovery.'));
    setBusy(true); setProblem(''); setResult(null);
    return new Promise<void>((resolve, reject) => {
      const id = ++counterRef.current;
      pendingRef.current = { id, label, start: performance.now(), ack: false, recovery: recovery ? 'high-value' : null, resolve, reject,
        timer: window.setTimeout(() => {
          current.cancelDrop();
          finish('Command timed out. It may have been applied; inspect the connection before retrying.');
        }, 30000),
      };
      if (!current.send({ type: 'LabCommand', request_id: id, action })) finish('Connection closed before the command was sent.');
    }).finally(() => current.cancelDrop());
  }, [finish]);

  useEffect(() => () => {
    const pending = pendingRef.current;
    if (pending) { window.clearTimeout(pending.timer); pending.reject(new Error('Lab closed')); pendingRef.current = null; }
  }, []);

  return { ...pipeline, stats, busy, problem, result, run,
    clear: () => { setStats(emptyStats()); setResult(null); setProblem(''); },
  };
}
