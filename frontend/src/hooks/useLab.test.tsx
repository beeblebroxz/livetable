import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useLab } from './useLab';
import { FakeWebSocket } from '../test/fakeWebSocket';
import type { ServerMessage } from '../types';

function seed(socket: FakeWebSocket) {
  socket.open();
  for (const [id, kind, source] of [['base', 'base', 'base'], ['high-value', 'filter', 'base'], ['ranked', 'sort', 'high-value'], ['regions', 'group', 'high-value']] as const) {
    socket.receive({ type: 'ViewData', table_name: 'lab', pipeline_generation: 1, node_id: id, source_id: source, kind, seq: 0,
      columns: ['amount'], rows: [{ row_id: id === 'base' ? 1 : null, row: { amount: 1480 } }] });
  }
}
const delta = (): ServerMessage => ({ type: 'ViewDelta', table_name: 'lab', pipeline_generation: 1,
  node_id: 'high-value', from_seq: 0, seq: 1, changes: [{ type: 'CellUpdated', index: 0, column: 'amount', value: 1481 }] });

describe('Orders lab controller', () => {
  beforeEach(() => { FakeWebSocket.reset(); vi.useFakeTimers(); vi.stubGlobal('WebSocket', FakeWebSocket); });
  afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

  it('counts UTF-8 received JSON without retaining row payloads and bounds the trace', async () => {
    const { result } = renderHook(() => useLab(1000));
    const socket = FakeWebSocket.instances[0];
    await act(async () => seed(socket));
    act(() => result.current.clear());
    const message: ServerMessage = { type: 'LabError', request_id: 999, message: 'échec' };
    await act(async () => { for (let i = 0; i < 100; i++) socket.receive(message); });
    expect(result.current.stats.bytes).toBe(new TextEncoder().encode(JSON.stringify(message)).length * 100);
    expect(result.current.stats.events).toHaveLength(80);
    expect(result.current.stats.events[0]).not.toHaveProperty('message');
    expect(result.current.problem).toBe(''); // Unrelated command IDs never finish this client's command.
  });

  it('holds a single command in flight and completes only its matching acknowledgement', async () => {
    const { result } = renderHook(() => useLab(1000));
    const socket = FakeWebSocket.instances[0];
    await act(async () => seed(socket));
    let command: Promise<void>;
    act(() => { command = result.current.run({ kind: 'step' }, 'Batch'); });
    expect(result.current.busy).toBe(true);
    await expect(result.current.run({ kind: 'step' }, 'Duplicate')).rejects.toThrow('Wait');
    await act(async () => socket.receive({ type: 'LabComplete', request_id: 200, rows: 1000, mutations: 3, step: 1 }));
    expect(result.current.busy).toBe(true);
    await act(async () => { socket.receive({ type: 'LabComplete', request_id: 1, rows: 1000, mutations: 3, step: 1 }); await command; });
    expect(result.current.busy).toBe(false);
    expect(result.current.result?.label).toBe('Batch');
  });

  it('keeps the server view timing of its own latest command only', async () => {
    const { result } = renderHook(() => useLab(1000));
    const socket = FakeWebSocket.instances[0];
    await act(async () => seed(socket));
    const timing = { views: [{ node_id: 'high-value', sync_us: 4286.9, rebuild_us: 30877 }, { node_id: 'ranked', sync_us: 806.8, rebuild_us: 8442.8 }] };
    let command: Promise<void>;
    act(() => { command = result.current.run({ kind: 'burst', size: 256 }, 'Burst'); });
    expect(socket.sentMessages[socket.sentMessages.length - 1]).toEqual({ type: 'LabCommand', request_id: 1, action: { kind: 'burst', size: 256 } });
    // Another client's reply carries its own pipeline's times, not ours.
    await act(async () => socket.receive({ type: 'LabComplete', request_id: 200, rows: 1000, mutations: 256, step: 1, timing: { views: [{ node_id: 'ranked', sync_us: 1 }] } }));
    expect(result.current.timing).toBeNull();
    await act(async () => { socket.receive({ type: 'LabComplete', request_id: 1, rows: 1000, mutations: 256, step: 1, timing }); await command; });
    expect(result.current.timing).toEqual(timing);
    expect(result.current.stats.events[0].detail).toContain('views synced in 5.09 ms on the server');
    // A later command without timing (a reset) replaces it; clearing the trace does too.
    act(() => { command = result.current.run({ kind: 'reset', rows: 1000 }, 'Reset'); });
    await act(async () => { socket.receive({ type: 'LabComplete', request_id: 2, rows: 1000, mutations: 2000, step: 0 }); await command; });
    expect(result.current.timing).toBeNull();
    act(() => { command = result.current.run({ kind: 'step' }, 'Step'); });
    await act(async () => { socket.receive({ type: 'LabComplete', request_id: 3, rows: 1000, mutations: 3, step: 1, timing: { views: [{ node_id: 'ranked', sync_us: 6 }] } }); await command; });
    expect(result.current.timing?.views).toHaveLength(1);
    act(() => result.current.clear());
    expect(result.current.timing).toBeNull();
  });

  it('discards one delta, waits past the ack, and completes after ordinary watermark repair', async () => {
    const { result } = renderHook(() => useLab(1000));
    const socket = FakeWebSocket.instances[0];
    await act(async () => seed(socket));
    let command: Promise<void>;
    act(() => { command = result.current.run({ kind: 'update', row_id: 1, amount: 1481 }, 'Recovery', true); });
    await act(async () => {
      socket.receive(delta());
      socket.receive({ type: 'LabComplete', request_id: 1, rows: 1000, mutations: 1, step: 0 });
    });
    expect(result.current.snapshots['high-value'].rows[0].row.amount).toBe(1480);
    expect(result.current.busy).toBe(true);
    await act(async () => socket.receive({ type: 'PipelineStatus', table_name: 'lab', pipeline_generation: 1, sequences: { 'high-value': 1 } }));
    expect(socket.sentMessages[socket.sentMessages.length - 1]).toEqual({ type: 'QueryView', table_name: 'lab', pipeline_generation: 1, node_id: 'high-value' });
    await act(async () => {
      socket.receive({ type: 'ViewData', table_name: 'lab', pipeline_generation: 1, node_id: 'high-value', source_id: 'base', kind: 'filter', seq: 2,
        columns: ['amount'], rows: [{ row_id: null, row: { amount: 1481 } }] });
      await command;
    });
    expect(result.current.snapshots['high-value'].seq).toBe(2);
    expect(result.current.busy).toBe(false);
    expect(result.current.stats.repairs).toBe(1);
    expect(result.current.stats.recovered).toBe(1);
    expect(result.current.stats.events.map(event => event.kind)).toContain('dropped');
  });

  it('reports command rejection and cancels fault injection before the next normal delivery', async () => {
    const { result } = renderHook(() => useLab(1000));
    const socket = FakeWebSocket.instances[0];
    await act(async () => seed(socket));
    let command: Promise<unknown>;
    act(() => { command = result.current.run({ kind: 'update', row_id: 999, amount: 1481 }, 'Bad edit', true).catch(error => error); });
    await act(async () => { socket.receive({ type: 'LabError', request_id: 1, message: 'Row not found' }); await command; });
    expect(result.current.problem).toBe('Row not found');
    expect(result.current.busy).toBe(false);
    await act(async () => socket.receive(delta()));
    expect(result.current.snapshots['high-value'].seq).toBe(1);
  });

  it('times out without automatic command retries and rejects on disconnect', async () => {
    const { result } = renderHook(() => useLab(1000));
    const socket = FakeWebSocket.instances[0];
    await act(async () => seed(socket));
    let command: Promise<unknown>;
    act(() => { command = result.current.run({ kind: 'step' }, 'Timeout').catch(error => error); });
    await act(async () => { vi.advanceTimersByTime(30000); await command; });
    expect(result.current.problem).toContain('may have been applied');
    expect(socket.sentMessages.filter(message => message.type === 'LabCommand')).toHaveLength(1);
    act(() => { command = result.current.run({ kind: 'step' }, 'Disconnect').catch(error => error); });
    await act(async () => { socket.close(); await command; });
    expect(result.current.problem).toContain('Connection lost');
    expect(result.current.busy).toBe(false);
  });
});
