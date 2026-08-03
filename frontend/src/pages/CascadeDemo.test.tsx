import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket } from '../test/fakeWebSocket';
import { PIPELINE_DEBOUNCE_MS } from '../hooks/usePipeline';
import { CascadeDemo } from './CascadeDemo';

describe('CascadeDemo protocol-v2 integration', () => {
  beforeEach(() => {
    FakeWebSocket.reset();
    vi.useFakeTimers();
    vi.stubGlobal('WebSocket', FakeWebSocket);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('renders server snapshots, sends base mutations, and rebuilds edited pipelines', async () => {
    render(<CascadeDemo onBack={vi.fn()} />);
    expect(FakeWebSocket.instances).toHaveLength(1);
    const socket = FakeWebSocket.instances[0];

    await act(async () => socket.open());
    expect(socket.sentMessages).toEqual([
      { type: 'Subscribe', table_name: 'demo' },
      {
        type: 'SetPipeline',
        table_name: 'demo',
        pipeline_generation: 1,
        nodes: [
          {
            id: 'high-value',
            source_id: 'base',
            kind: 'filter',
            predicate: 'amount >= 500',
          },
          {
            id: 'ranked',
            source_id: 'high-value',
            kind: 'sort',
            keys: [{ column: 'amount', descending: true }],
          },
          {
            id: 'regional-totals',
            source_id: 'ranked',
            kind: 'group',
            group_by: ['region'],
            aggs: [
              { alias: 'total', op: 'sum', column: 'amount' },
              { alias: 'average', op: 'avg', column: 'amount' },
              { alias: 'count', op: 'count', column: 'amount' },
            ],
          },
        ],
      },
    ]);

    await act(async () => {
      socket.receive({
        type: 'Subscribed',
        table_name: 'demo',
        protocol_version: 2,
      });
      socket.receive({
        type: 'ViewData',
        table_name: 'demo',
        pipeline_generation: 1,
        node_id: 'base',
        source_id: 'base',
        kind: 'base',
        seq: 3,
        columns: ['region', 'product', 'amount'],
        rows: [
          { row_id: 1, row: { region: 'West', product: 'Widget', amount: 100.5 } },
          { row_id: 2, row: { region: 'East', product: 'Gadget', amount: 200.75 } },
          { row_id: 3, row: { region: 'West', product: 'Premium', amount: 700 } },
        ],
      });
      socket.receive({
        type: 'ViewData',
        table_name: 'demo',
        pipeline_generation: 1,
        node_id: 'high-value',
        source_id: 'base',
        kind: 'filter',
        seq: 4,
        columns: ['region', 'product', 'amount'],
        rows: [{ row_id: null, row: { region: 'West', product: 'Premium', amount: 700 } }],
      });
      socket.receive({
        type: 'ViewData',
        table_name: 'demo',
        pipeline_generation: 1,
        node_id: 'ranked',
        source_id: 'high-value',
        kind: 'sort',
        seq: 5,
        columns: ['region', 'product', 'amount'],
        rows: [{ row_id: null, row: { region: 'West', product: 'Premium', amount: 700 } }],
      });
      socket.receive({
        type: 'ViewData',
        table_name: 'demo',
        pipeline_generation: 1,
        node_id: 'regional-totals',
        source_id: 'ranked',
        kind: 'group',
        seq: 6,
        columns: ['region', 'total', 'average', 'count'],
        rows: [{ row_id: null, row: { region: 'West', total: 700, average: 700, count: 1 } }],
      });
    });

    expect(screen.getAllByText('Base Sales').length).toBeGreaterThan(0);
    expect(screen.getAllByText('High Value Filter').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Regional Totals').length).toBeGreaterThan(0);
    expect(screen.getByText('$1,001')).toBeTruthy();
    expect(screen.getAllByText('$700').length).toBeGreaterThan(0);

    const amountInput = screen.getByLabelText('Edit amount for row 1');
    fireEvent.change(amountInput, { target: { value: '600' } });
    fireEvent.blur(amountInput);
    expect(socket.sentMessages[socket.sentMessages.length - 1]).toEqual({
      type: 'UpdateCell',
      table_name: 'demo',
      row_id: 1,
      column: 'amount',
      value: 600,
    });

    fireEvent.click(screen.getAllByRole('button', { name: 'Delete' })[1]);
    expect(socket.sentMessages[socket.sentMessages.length - 1]).toEqual({
      type: 'DeleteRow',
      table_name: 'demo',
      row_id: 2,
    });

    fireEvent.click(screen.getByRole('button', { name: 'Insert sale' }));
    expect(socket.sentMessages[socket.sentMessages.length - 1]).toMatchObject({
      type: 'InsertRow',
      table_name: 'demo',
    });

    fireEvent.change(screen.getByLabelText('filter expression'), {
      target: { value: 'amount >= 900' },
    });
    await act(async () => vi.advanceTimersByTime(PIPELINE_DEBOUNCE_MS));
    const rebuilt = socket.sentMessages[socket.sentMessages.length - 1];
    expect(rebuilt).toMatchObject({
      type: 'SetPipeline',
      table_name: 'demo',
      pipeline_generation: 2,
    });
    expect(rebuilt?.type).toBe('SetPipeline');
    if (rebuilt?.type !== 'SetPipeline') {
      throw new Error('expected SetPipeline');
    }
    expect(rebuilt.nodes).toHaveLength(3);
    expect(rebuilt.nodes[0]).toMatchObject({
      id: 'high-value',
      kind: 'filter',
      predicate: 'amount >= 900',
    });

    await act(async () => {
      socket.receive({
        type: 'ViewError',
        table_name: 'demo',
        pipeline_generation: 1,
        node_id: 'high-value',
        message: 'stale error',
      });
    });
    expect(screen.queryByText('stale error')).toBeNull();

    await act(async () => {
      socket.receive({
        type: 'ViewError',
        table_name: 'demo',
        pipeline_generation: 2,
        node_id: 'high-value',
        message: 'current error',
      });
    });
    expect(screen.getByText('current error')).toBeTruthy();
  });
});
