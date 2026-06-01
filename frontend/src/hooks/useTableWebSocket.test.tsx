import { act, render, screen } from '@testing-library/react';
import { describe, expect, it, beforeEach, afterEach, vi } from 'vitest';
import { useTableWebSocket } from './useTableWebSocket';
import { FakeWebSocket } from '../test/fakeWebSocket';

function HookHarness({ label, wsUrl = 'ws://localhost:8080/ws' }: { label: string; wsUrl?: string }) {
  const { connected, data } = useTableWebSocket('demo', wsUrl);

  return (
    <div>
      <div data-testid={`${label}-connected`}>{connected ? 'connected' : 'disconnected'}</div>
      <div data-testid={`${label}-rows`}>{JSON.stringify(data)}</div>
    </div>
  );
}

describe('useTableWebSocket', () => {
  beforeEach(() => {
    FakeWebSocket.reset();
    vi.useFakeTimers();
    vi.stubGlobal('WebSocket', FakeWebSocket);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('keeps multiple clients in sync using stable row ids', async () => {
    render(
      <>
        <HookHarness label="client-a" />
        <HookHarness label="client-b" />
      </>
    );

    expect(FakeWebSocket.instances).toHaveLength(2);
    const [clientA, clientB] = FakeWebSocket.instances;

    await act(async () => {
      clientA.open();
      clientB.open();
    });

    expect(clientA.sentMessages).toEqual([
      { type: 'Subscribe', table_name: 'demo' },
      { type: 'Query', table_name: 'demo' },
    ]);
    expect(clientB.sentMessages).toEqual(clientA.sentMessages);

    const initialTable = {
      type: 'TableData' as const,
      table_name: 'demo',
      seq: 5,
      columns: ['id', 'name', 'value'],
      rows: [
        { row_id: 11, row: { id: 4, name: 'Alice', value: 100 } },
        { row_id: 42, row: { id: 9, name: 'Bob', value: 200 } },
      ],
    };
    await act(async () => {
      clientA.receive(initialTable);
      clientB.receive(initialTable);
    });
    expect(screen.getByTestId('client-a-rows').textContent).toContain('"rowId":11');
    expect(screen.getByTestId('client-b-rows').textContent).toContain('"rowId":42');

    const deletedRow = {
      type: 'RowDeleted' as const,
      table_name: 'demo',
      seq: 6,
      row_id: 11,
    };
    const updatedRow = {
      type: 'CellUpdated' as const,
      table_name: 'demo',
      seq: 7,
      row_id: 42,
      column: 'value',
      value: 250,
    };
    await act(async () => {
      clientA.receive(deletedRow);
      clientB.receive(deletedRow);
      clientA.receive(updatedRow);
      clientB.receive(updatedRow);
    });
    const clientARows = screen.getByTestId('client-a-rows').textContent ?? '';
    const clientBRows = screen.getByTestId('client-b-rows').textContent ?? '';
    expect(clientARows).not.toContain('"rowId":11');
    expect(clientBRows).not.toContain('"rowId":11');
    expect(clientARows).toContain('"value":250');
    expect(clientBRows).toContain('"value":250');
  });

  it('reconnects and re-subscribes after the socket closes', async () => {
    render(<HookHarness label="client" />);

    expect(FakeWebSocket.instances).toHaveLength(1);
    const firstSocket = FakeWebSocket.instances[0];
    await act(async () => {
      firstSocket.open();
    });

    expect(screen.getByTestId('client-connected').textContent).toBe('connected');

    await act(async () => {
      firstSocket.close();
    });

    expect(screen.getByTestId('client-connected').textContent).toBe('disconnected');

    await act(async () => {
      vi.advanceTimersByTime(250);
    });

    expect(FakeWebSocket.instances).toHaveLength(2);
    const secondSocket = FakeWebSocket.instances[1];
    await act(async () => {
      secondSocket.open();
    });

    expect(screen.getByTestId('client-connected').textContent).toBe('connected');

    expect(secondSocket.sentMessages).toEqual([
      { type: 'Subscribe', table_name: 'demo' },
      { type: 'Query', table_name: 'demo' },
    ]);
  });

  // Reproduction: snapshot/delta consistency hazard.
  //
  // On (re)connect the client subscribes BEFORE it queries (see `onopen`). That
  // correctly avoids missing updates, but it opens an overlap window: a row
  // inserted by another client AFTER we subscribe but BEFORE the server takes
  // our snapshot appears in BOTH the broadcast `RowInserted` (already queued to
  // us) AND the `TableData` snapshot. The wire protocol carries no
  // sequence/generation tag, so the client cannot tell the delta is already
  // reflected in the snapshot.
  //
  // Actix does not guarantee FIFO ordering between a stream frame (our Query ->
  // TableData) and a mailbox message (the broadcast RowInserted). When the
  // snapshot is delivered first, the client double-applies the insert.
  //
  // This pins the client merge invariant: a row_id must appear at most once.
  it('does not duplicate a row already reflected in the snapshot', async () => {
    render(<HookHarness label="client" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
    });

    // A concurrent writer inserted row_id 99 after we subscribed but before our
    // snapshot was taken, so the snapshot already contains it...
    const snapshotIncludingInsert = {
      type: 'TableData' as const,
      table_name: 'demo',
      seq: 7,
      columns: ['id', 'name', 'value'],
      rows: [
        { row_id: 11, row: { id: 4, name: 'Alice', value: 100 } },
        { row_id: 42, row: { id: 9, name: 'Bob', value: 200 } },
        { row_id: 99, row: { id: 7, name: 'Carol', value: 300 } },
      ],
    };
    // ...and the broadcast for that same insert (its seq is <= the snapshot's,
    // so it is already reflected) is still in flight, delivered AFTER the
    // snapshot because of stream-vs-mailbox ordering.
    const concurrentInsert = {
      type: 'RowInserted' as const,
      table_name: 'demo',
      seq: 7,
      index: 2,
      row_id: 99,
      row: { id: 7, name: 'Carol', value: 300 },
    };

    await act(async () => {
      socket.receive(snapshotIncludingInsert);
      socket.receive(concurrentInsert);
    });

    const rendered = screen.getByTestId('client-rows').textContent ?? '[]';
    const rows = JSON.parse(rendered) as { rowId: number }[];
    const duplicatesOf99 = rows.filter((row) => row.rowId === 99);

    expect(duplicatesOf99).toHaveLength(1);
  });

  // Deltas can also win the race and arrive BEFORE the snapshot. They are
  // buffered until the snapshot defines the seq cutoff, then replayed: those
  // already reflected in the snapshot are dropped, newer ones are applied.
  it('buffers pre-snapshot deltas and replays only newer ones', async () => {
    render(<HookHarness label="client" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
    });

    // Two inserts arrive before our snapshot: row 55 (seq 5, will be reflected
    // in the snapshot) and row 77 (seq 8, newer than the snapshot).
    await act(async () => {
      socket.receive({
        type: 'RowInserted',
        table_name: 'demo',
        seq: 5,
        index: 2,
        row_id: 55,
        row: { id: 5, name: 'Dave', value: 500 },
      });
      socket.receive({
        type: 'RowInserted',
        table_name: 'demo',
        seq: 8,
        index: 3,
        row_id: 77,
        row: { id: 7, name: 'Erin', value: 700 },
      });
    });

    // Snapshot at seq 7 already contains row 55 but predates row 77.
    await act(async () => {
      socket.receive({
        type: 'TableData',
        table_name: 'demo',
        seq: 7,
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 11, row: { id: 4, name: 'Alice', value: 100 } },
          { row_id: 42, row: { id: 9, name: 'Bob', value: 200 } },
          { row_id: 55, row: { id: 5, name: 'Dave', value: 500 } },
        ],
      });
    });

    const rendered = screen.getByTestId('client-rows').textContent ?? '[]';
    const rows = JSON.parse(rendered) as { rowId: number }[];

    expect(rows.filter((row) => row.rowId === 55)).toHaveLength(1); // reflected, dropped
    expect(rows.filter((row) => row.rowId === 77)).toHaveLength(1); // newer, replayed
    expect(rows).toHaveLength(4);
  });

  it('drops duplicate deltas after a snapshot', async () => {
    render(<HookHarness label="client" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({
        type: 'TableData',
        table_name: 'demo',
        seq: 10,
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 11, row: { id: 1, name: 'Alice', value: 100 } },
        ],
      });
    });

    const insert = {
      type: 'RowInserted' as const,
      table_name: 'demo',
      seq: 11,
      index: 1,
      row_id: 22,
      row: { id: 2, name: 'Bob', value: 200 },
    };

    await act(async () => {
      socket.receive(insert);
      socket.receive(insert);
    });

    const rendered = screen.getByTestId('client-rows').textContent ?? '[]';
    const rows = JSON.parse(rendered) as { rowId: number }[];

    expect(rows.filter((row) => row.rowId === 22)).toHaveLength(1);
    expect(rows).toHaveLength(2);
  });

  it('buffers out-of-order post-snapshot deltas until contiguous', async () => {
    render(<HookHarness label="client" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({
        type: 'TableData',
        table_name: 'demo',
        seq: 20,
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 11, row: { id: 1, name: 'Alice', value: 100 } },
        ],
      });
    });

    await act(async () => {
      socket.receive({
        type: 'RowInserted',
        table_name: 'demo',
        seq: 22,
        index: 2,
        row_id: 33,
        row: { id: 3, name: 'Carol', value: 300 },
      });
    });

    let rendered = screen.getByTestId('client-rows').textContent ?? '[]';
    let rows = JSON.parse(rendered) as { rowId: number }[];
    expect(rows.map((row) => row.rowId)).toEqual([11]);

    await act(async () => {
      socket.receive({
        type: 'RowInserted',
        table_name: 'demo',
        seq: 21,
        index: 1,
        row_id: 22,
        row: { id: 2, name: 'Bob', value: 200 },
      });
    });

    rendered = screen.getByTestId('client-rows').textContent ?? '[]';
    rows = JSON.parse(rendered) as { rowId: number }[];
    expect(rows.map((row) => row.rowId)).toEqual([11, 22, 33]);
  });
});
