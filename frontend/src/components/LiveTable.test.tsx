import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, beforeEach, afterEach, vi } from 'vitest';
import { LiveTable } from './LiveTable';
import { FakeWebSocket } from '../test/fakeWebSocket';

describe('LiveTable', () => {
  beforeEach(() => {
    FakeWebSocket.reset();
    vi.stubGlobal('WebSocket', FakeWebSocket);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('uses the max current numeric id when adding a new row', async () => {
    render(<LiveTable tableName="demo" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({
        type: 'TableData',
        table_name: 'demo',
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 100, row: { id: 1, name: 'Alice', value: 100 } },
          { row_id: 101, row: { id: 4, name: 'Bob', value: 200 } },
        ],
      });
    });

    expect(screen.getByText('Total rows:')).toBeTruthy();
    expect(screen.getByText('2')).toBeTruthy();

    await userEvent.click(screen.getByRole('button', { name: '+ Add Row' }));

    const insertMessage = socket.sentMessages[socket.sentMessages.length - 1];
    expect(insertMessage).toEqual({
      type: 'InsertRow',
      table_name: 'demo',
      row: {
        id: 5,
        name: 'New Item 3',
        value: 0,
      },
    });
  });

  it('sends row-id based updates when editing a cell', async () => {
    render(<LiveTable tableName="demo" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({
        type: 'TableData',
        table_name: 'demo',
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 900, row: { id: 12, name: 'Alice', value: 100 } },
        ],
      });
    });

    const valueInput = await screen.findByDisplayValue('100');
    await userEvent.clear(valueInput);
    await userEvent.type(valueInput, '250');
    valueInput.blur();

    const updateMessage = socket.sentMessages[socket.sentMessages.length - 1];
    expect(updateMessage).toEqual({
      type: 'UpdateCell',
      table_name: 'demo',
      row_id: 900,
      column: 'value',
      value: 250,
    });
  });
});
