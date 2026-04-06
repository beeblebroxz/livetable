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
      row_id: 11,
    };
    const updatedRow = {
      type: 'CellUpdated' as const,
      table_name: 'demo',
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
});
