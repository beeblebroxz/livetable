import { act, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { PipelineSnapshot, ServerMessage, ViewChange, ViewNodeSpec } from '../types';
import { FakeWebSocket } from '../test/fakeWebSocket';
import { PIPELINE_DEBOUNCE_MS, PIPELINE_RESYNC_RETRY_MS, usePipeline } from './usePipeline';
import { MAX_VIEW_DELTA_CHANGES, parseServerMessage } from './useTableWebSocket';

function snapshot(seq = 0, amounts: (number | null)[] = [200, 500]): Extract<ServerMessage, { type: 'ViewData' }> {
  return { type: 'ViewData', table_name: 'demo', pipeline_generation: 1,
    node_id: 'filtered', source_id: 'base', kind: 'filter', seq, columns: ['amount'],
    rows: amounts.map((amount) => ({ row_id: null, row: { amount } })),
  };
}

function delta(fromSeq = 0, changes: ViewChange[] = [{ type: 'CellUpdated', index: 0, column: 'amount', value: 250 }]): Extract<ServerMessage, { type: 'ViewDelta' }> {
  return { type: 'ViewDelta', table_name: 'demo', pipeline_generation: 1,
    node_id: 'filtered', from_seq: fromSeq, seq: fromSeq + 1, changes,
  };
}

const currentSnapshots = (): Record<string, PipelineSnapshot> => JSON.parse(screen.getByTestId('snapshots').textContent ?? '{}');
const repairs = (socket: FakeWebSocket) => socket.sentMessages.filter((message) => message.type === 'QueryView');

const FILTER: ViewNodeSpec[] = [
  {
    id: 'filtered',
    source_id: 'base',
    kind: 'filter',
    predicate: 'amount >= 150',
  },
];

function Harness({ nodes = FILTER, tableName = 'demo' }: { nodes?: ViewNodeSpec[]; tableName?: string }) {
  const pipeline = usePipeline(tableName, nodes, 'ws://localhost:8080/ws');
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

  it('applies mixed coordinate edits atomically and ignores duplicates', async () => {
    render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    const batch = delta(0, [
      { type: 'RowDeleted', index: 0 },
      { type: 'RowInserted', index: 1, row: { row_id: null, row: { amount: null } } },
      { type: 'CellUpdated', index: 0, column: 'amount', value: 700 },
    ]);
    await act(async () => { socket.open(); socket.receive(snapshot()); socket.receive(batch); socket.receive(batch); });
    expect(currentSnapshots().filtered.rows.map((row) => row.row.amount)).toEqual([700, null]);
    expect(currentSnapshots().filtered.seq).toBe(1);
    expect(repairs(socket)).toHaveLength(0);
  });

  it('repairs a gap once, ignores intervening deltas, and resumes after snapshot', async () => {
    render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open(); socket.receive(snapshot());
      socket.receive(delta(1)); socket.receive(delta(2)); socket.receive(delta(0));
    });
    expect(currentSnapshots().filtered.seq).toBe(0);
    expect(repairs(socket)).toEqual([{ type: 'QueryView', table_name: 'demo', pipeline_generation: 1, node_id: 'filtered' }]);
    await act(async () => {
      socket.receive(snapshot(4, [900])); socket.receive(delta(2));
      socket.receive(delta(4, [{ type: 'RowInserted', index: 0, row: { row_id: null, row: { amount: 100 } } }]));
    });
    expect(currentSnapshots().filtered.rows.map((row) => row.row.amount)).toEqual([100, 900]);
    expect(currentSnapshots().filtered.seq).toBe(5);
  });

  it('requests a baseline for a pre-snapshot delta and retries lost repair responses', async () => {
    const { unmount } = render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => { socket.open(); socket.receive(delta()); });
    expect(currentSnapshots()).toEqual({});
    expect(repairs(socket)).toHaveLength(1);
    await act(async () => vi.advanceTimersByTime(PIPELINE_RESYNC_RETRY_MS));
    expect(repairs(socket)).toHaveLength(2);
    await act(async () => socket.receive(snapshot(2)));
    await act(async () => vi.advanceTimersByTime(PIPELINE_RESYNC_RETRY_MS * 2));
    expect(repairs(socket)).toHaveLength(2);
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('uses watermarks to recover lost final updates and initial snapshots', async () => {
    render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    const status: ServerMessage = { type: 'PipelineStatus', table_name: 'demo', pipeline_generation: 1,
      sequences: { base: 0, filtered: 1, unexpected: 10 },
    };
    await act(async () => { socket.open(); socket.receive(snapshot()); socket.receive(status); socket.receive(status); });
    expect(repairs(socket).map((message) => message.node_id)).toEqual(['base', 'filtered']);
    await act(async () => {
      socket.receive(snapshot(2, [800]));
      socket.receive({ ...snapshot(1), node_id: 'base', kind: 'base', rows: [] });
      socket.receive(status);
    });
    expect(repairs(socket)).toHaveLength(2);
  });

  it.each([
    [{ type: 'RowDeleted', index: 50 }],
    [{ type: 'RowInserted', index: 50, row: { row_id: null, row: { amount: 1 } } }],
    [{ type: 'CellUpdated', index: 0, column: 'missing', value: 1 }],
    [{ type: 'RowInserted', index: 0, row: { row_id: 8, row: { amount: 1 } } }],
    [{ type: 'RowInserted', index: 0, row: { row_id: null, row: { missing: 1 } } }],
  ] satisfies ViewChange[][])('does not partially apply a batch with invalid operations: %j', async (...invalid) => {
    render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open(); socket.receive(snapshot());
      socket.receive(delta(0, [{ type: 'CellUpdated', index: 0, column: 'amount', value: 999 }, ...invalid]));
    });
    expect(currentSnapshots().filtered.rows[0].row.amount).toBe(200);
    expect(currentSnapshots().filtered.seq).toBe(0);
    expect(repairs(socket)).toHaveLength(1);
  });

  it('rejects malformed or oversized deltas at the wire boundary', () => {
    for (const changes of [[], Array(MAX_VIEW_DELTA_CHANGES + 1).fill(delta().changes[0]),
      [{ type: 'RowDeleted', index: -1 }], [{ type: 'CellUpdated', index: 0, column: 'amount', value: {} }],
      [{ type: 'RowInserted', index: 0, row: { row_id: Number.MAX_SAFE_INTEGER + 1, row: {} } }],
    ]) expect(parseServerMessage(JSON.stringify({ ...delta(), changes }))).toBeNull();
    expect(parseServerMessage(JSON.stringify({ ...delta(), seq: 4 }))).toBeNull();
    expect(parseServerMessage(JSON.stringify({ ...delta(), from_seq: -1 }))).toBeNull();
    expect(parseServerMessage(JSON.stringify(delta()))).toEqual(delta());
  });

  it('clears removed-node baselines and repair state on a new generation', async () => {
    const { rerender } = render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => { socket.open(); socket.receive(snapshot()); socket.receive(delta(4)); });
    const changed: ViewNodeSpec[] = [{ ...FILTER[0], id: 'new-filter' }];
    rerender(<Harness nodes={changed} />);
    await act(async () => vi.advanceTimersByTime(PIPELINE_DEBOUNCE_MS));
    expect(currentSnapshots()).toEqual({});
    await act(async () => {
      socket.receive(snapshot(20)); socket.receive(delta(20));
      socket.receive({ type: 'PipelineStatus', table_name: 'demo', pipeline_generation: 1, sequences: { filtered: 20 } });
      socket.receive({ ...snapshot(), pipeline_generation: 2 }); // removed id, even in current generation
      vi.advanceTimersByTime(PIPELINE_RESYNC_RETRY_MS);
    });
    expect(currentSnapshots()).toEqual({});
    expect(repairs(socket)).toHaveLength(1);
  });

  it('ignores wrong tables and callbacks from an obsolete socket', async () => {
    render(<Harness />);
    const first = FakeWebSocket.instances[0];
    await act(async () => { first.open(); first.receive({ ...snapshot(), table_name: 'other' }); });
    expect(currentSnapshots()).toEqual({});
    await act(async () => { first.close(); vi.advanceTimersByTime(250); });
    const second = FakeWebSocket.instances[1];
    await act(async () => {
      second.open();
      first.receive({ ...snapshot(100), pipeline_generation: 2 });
      first.emitError(); first.close();
    });
    expect(currentSnapshots()).toEqual({});
    expect(screen.getByTestId('connected').textContent).toBe('true');
    expect(screen.getByTestId('errors').textContent).toBe('{}');
  });

  it('supports node IDs that overlap Object prototype properties', async () => {
    const nodes: ViewNodeSpec[] = [{ ...FILTER[0], id: '__proto__' }, { ...FILTER[0], id: 'constructor' }];
    render(<Harness nodes={nodes} />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({ type: 'PipelineStatus', table_name: 'demo', pipeline_generation: 1,
        sequences: JSON.parse('{"__proto__":0,"constructor":0}'),
      });
    });
    expect(repairs(socket).map((message) => message.node_id)).toEqual(['__proto__', 'constructor']);
    await act(async () => {
      socket.receive({ ...snapshot(1), node_id: '__proto__' });
      socket.receive({ ...delta(1), node_id: '__proto__' });
    });
    expect(currentSnapshots()['__proto__'].seq).toBe(2);
    expect(currentSnapshots()['__proto__'].rows[0].row.amount).toBe(250);
  });

  it('rejects malformed snapshot schemas and duplicate base identities', async () => {
    render(<Harness />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({ ...snapshot(), columns: ['missing'] });
      socket.receive({ ...snapshot(), node_id: 'base', kind: 'base',
        rows: [{ row_id: 1, row: { amount: 100 } }, { row_id: 1, row: { amount: 200 } }],
      });
    });
    expect(currentSnapshots()).toEqual({});
    expect(repairs(socket).map((message) => message.node_id)).toEqual(['filtered', 'base']);
    await act(async () => {
      socket.receive(snapshot(1));
      socket.receive({ ...snapshot(1), node_id: 'base', kind: 'base', rows: [{ row_id: 1, row: { amount: 100 } }] });
      socket.receive({ ...delta(1, [
        { type: 'RowInserted', index: 1, row: { row_id: 8, row: { amount: 800 } } },
        { type: 'RowDeleted', index: 0 },
      ]), node_id: 'base' });
    });
    expect(currentSnapshots().base.rows).toEqual([{ row_id: 8, row: { amount: 800 } }]);
  });

  it('applies sorted row moves without altering retained row values', async () => {
    const nodes: ViewNodeSpec[] = [{ id: 'ranked', source_id: 'base', kind: 'sort', keys: [{ column: 'amount', descending: true }] }];
    render(<Harness nodes={nodes} />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open(); socket.receive({ ...snapshot(0, [500, 200]), node_id: 'ranked', kind: 'sort' });
      socket.receive({ ...delta(0, [
        { type: 'RowDeleted', index: 1 },
        { type: 'RowInserted', index: 0, row: { row_id: null, row: { amount: 700 } } },
      ]), node_id: 'ranked' });
    });
    expect(currentSnapshots().ranked.rows.map((row) => row.row.amount)).toEqual([700, 500]);
    expect(repairs(socket)).toHaveLength(0);
  });

  it('clears another table’s data immediately and ignores unknown-node errors', async () => {
    const { rerender } = render(<Harness />);
    const first = FakeWebSocket.instances[0];
    await act(async () => { first.open(); first.receive(snapshot()); });
    rerender(<Harness tableName="other" />);
    expect(currentSnapshots()).toEqual({});
    expect(screen.getByTestId('connected').textContent).toBe('false');
    const second = FakeWebSocket.instances[1];
    await act(async () => {
      second.open();
      second.receive({ type: 'ViewError', table_name: 'other', pipeline_generation: 2, node_id: 'unknown', message: 'ignore me' });
      second.receive({ ...snapshot(), table_name: 'other', pipeline_generation: 2 });
    });
    expect(currentSnapshots().filtered.seq).toBe(0);
    expect(screen.getByTestId('errors').textContent).toBe('{}');
  });
});
