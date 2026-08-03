import { act, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ViewNodeSpec } from '../types';
import { FakeWebSocket } from '../test/fakeWebSocket';
import { PIPELINE_DEBOUNCE_MS, usePipeline } from './usePipeline';

const FILTER: ViewNodeSpec[] = [
  {
    id: 'filtered',
    source_id: 'base',
    kind: 'filter',
    predicate: 'amount >= 150',
  },
];

function Harness({ nodes = FILTER }: { nodes?: ViewNodeSpec[] }) {
  const pipeline = usePipeline('demo', nodes, 'ws://localhost:8080/ws');
  return (
    <div>
      <div data-testid="connected">{String(pipeline.connected)}</div>
      <div data-testid="generation">{pipeline.generation}</div>
      <div data-testid="snapshots">{JSON.stringify(pipeline.snapshots)}</div>
      <div data-testid="errors">{JSON.stringify(pipeline.errors)}</div>
      <button onClick={() => pipeline.insertRow({ region: 'West', product: 'New', amount: 300 })}>
        Insert
      </button>
      <button onClick={() => pipeline.updateCell(7, 'amount', 550)}>Update</button>
      <button onClick={() => pipeline.deleteRow(7)}>Delete</button>
    </div>
  );
}

describe('usePipeline', () => {
  beforeEach(() => {
    FakeWebSocket.reset();
    vi.useFakeTimers();
    vi.stubGlobal('WebSocket', FakeWebSocket);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('subscribes and sends generation one on open', async () => {
    render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => socket.open());

    expect(socket.sentMessages).toEqual([
      { type: 'Subscribe', table_name: 'demo' },
      {
        type: 'SetPipeline',
        table_name: 'demo',
        pipeline_generation: 1,
        nodes: FILTER,
      },
    ]);
    expect(screen.getByTestId('connected').textContent).toBe('true');
    expect(screen.getByTestId('generation').textContent).toBe('1');
  });

  it('keeps only current-generation snapshots with increasing node seq', async () => {
    render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({
        type: 'ViewData',
        table_name: 'demo',
        pipeline_generation: 1,
        node_id: 'filtered',
        source_id: 'base',
        kind: 'filter',
        seq: 8,
        columns: ['amount'],
        rows: [{ row_id: null, row: { amount: 200 } }],
      });
      socket.receive({
        type: 'ViewData',
        table_name: 'demo',
        pipeline_generation: 1,
        node_id: 'filtered',
        source_id: 'base',
        kind: 'filter',
        seq: 7,
        columns: ['amount'],
        rows: [{ row_id: null, row: { amount: 999 } }],
      });
      socket.receive({
        type: 'ViewData',
        table_name: 'demo',
        pipeline_generation: 0,
        node_id: 'filtered',
        source_id: 'base',
        kind: 'filter',
        seq: 100,
        columns: ['amount'],
        rows: [{ row_id: null, row: { amount: 1000 } }],
      });
    });

    const snapshots = screen.getByTestId('snapshots').textContent ?? '';
    expect(snapshots).toContain('"seq":8');
    expect(snapshots).toContain('"amount":200');
    expect(snapshots).not.toContain('999');
    expect(snapshots).not.toContain('1000');
  });

  it('debounces edits into a new generation and ignores old errors', async () => {
    const { rerender } = render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => socket.open());

    const changed: ViewNodeSpec[] = [{
      id: 'filtered',
      source_id: 'base',
      kind: 'filter',
      predicate: 'amount >= 500',
    }];
    rerender(<Harness nodes={changed} />);
    expect(socket.sentMessages).toHaveLength(2);
    await act(async () => vi.advanceTimersByTime(PIPELINE_DEBOUNCE_MS));
    expect(socket.sentMessages[2]).toEqual({
      type: 'SetPipeline',
      table_name: 'demo',
      pipeline_generation: 2,
      nodes: changed,
    });

    await act(async () => {
      socket.receive({
        type: 'ViewError',
        table_name: 'demo',
        pipeline_generation: 1,
        node_id: 'filtered',
        message: 'stale',
      });
      socket.receive({
        type: 'ViewError',
        table_name: 'demo',
        pipeline_generation: 2,
        node_id: 'filtered',
        message: 'current',
      });
    });
    const errors = screen.getByTestId('errors').textContent ?? '';
    expect(errors).toContain('current');
    expect(errors).not.toContain('stale');
  });

  it('uses a fresh generation on reconnect and exposes mutations', async () => {
    render(<Harness />);
    const first = FakeWebSocket.instances[0];
    await act(async () => first.open());
    await act(async () => first.close());
    await act(async () => vi.advanceTimersByTime(250));
    const second = FakeWebSocket.instances[1];
    await act(async () => second.open());
    expect(second.sentMessages[1]).toMatchObject({
      type: 'SetPipeline',
      pipeline_generation: 2,
    });

    await act(async () => screen.getByRole('button', { name: 'Insert' }).click());
    expect(second.sentMessages[2]).toEqual({
      type: 'InsertRow',
      table_name: 'demo',
      row: { region: 'West', product: 'New', amount: 300 },
    });
    await act(async () => screen.getByRole('button', { name: 'Update' }).click());
    await act(async () => screen.getByRole('button', { name: 'Delete' }).click());
    expect(second.sentMessages.slice(3)).toEqual([
      {
        type: 'UpdateCell',
        table_name: 'demo',
        row_id: 7,
        column: 'amount',
        value: 550,
      },
      { type: 'DeleteRow', table_name: 'demo', row_id: 7 },
    ]);
  });
});
