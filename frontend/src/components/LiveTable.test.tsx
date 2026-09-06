import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, beforeEach, afterEach, vi } from 'vitest';
import { LiveTable } from './LiveTable';
import { FakeWebSocket } from '../test/fakeWebSocket';

async function loadEditor(count = 2) {
  render(<LiveTable tableName="demo" />);
  const socket = FakeWebSocket.instances[0];
  await act(async () => {
    socket.open();
    socket.receive({ type: 'TableData', table_name: 'demo', seq: 1, columns: ['id', 'name', 'value'],
      rows: Array.from({ length: count }, (_, index) => ({ row_id: 900 + index, row: { id: index + 1, name: `Item ${index + 1}`, value: (index + 1) * 100 } })),
    });
  });
  return socket;
}

const inputValue = (label: string) => (screen.getByLabelText(label) as HTMLInputElement).value;

describe('LiveTable', () => {
  beforeEach(() => {
    FakeWebSocket.reset();
    vi.stubGlobal('WebSocket', FakeWebSocket);
  });

  afterEach(() => {
    vi.useRealTimers();
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
        seq: 2,
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 100, row: { id: 1, name: 'Alice', value: 100 } },
          { row_id: 101, row: { id: 4, name: 'Bob', value: 200 } },
        ],
      });
    });

    expect(screen.getByText(/Total rows:/).textContent).toContain('Total rows: 2');

    await userEvent.click(screen.getByRole('button', { name: 'Add row' }));
    expect((screen.getByLabelText('New id') as HTMLInputElement).value).toBe('5');
    expect(socket.sentMessages.some(message => message.type === 'InsertRow')).toBe(false);
    await userEvent.click(screen.getByRole('button', { name: 'Create row' }));

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
        seq: 1,
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

  // Clearing a numeric cell sends null. If the column is non-nullable the
  // server rejects it with an Error (no CellUpdated echo), so the input must
  // not keep showing the cleared value — it would silently disagree with the
  // table until the next snapshot. The cell snaps back to the last confirmed
  // value and only moves when the server echoes the change.
  it('reverts a cleared numeric cell when the server rejects the update', async () => {
    render(<LiveTable tableName="demo" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({
        type: 'TableData',
        table_name: 'demo',
        seq: 1,
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 900, row: { id: 12, name: 'Alice', value: 100 } },
        ],
      });
    });

    const valueInput = await screen.findByDisplayValue('100');
    await userEvent.clear(valueInput);
    await act(async () => {
      valueInput.blur();
    });

    // The attempt is still sent — the server is the one who knows nullability.
    const updateMessage = socket.sentMessages[socket.sentMessages.length - 1];
    expect(updateMessage).toEqual({
      type: 'UpdateCell',
      table_name: 'demo',
      row_id: 900,
      column: 'value',
      value: null,
    });

    // No echo came back, so the cell shows the last confirmed value.
    expect((valueInput as HTMLInputElement).value).toBe('100');
    await act(async () => {
      socket.receive({ type: 'Error', message: "Column 'value' is not nullable" });
    });
    expect((valueInput as HTMLInputElement).value).toBe('100');
    expect(screen.getByRole('alert').textContent).toContain("Column 'value' is not nullable");
  });

  it('clears a numeric cell when the server confirms the update', async () => {
    render(<LiveTable tableName="demo" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({
        type: 'TableData',
        table_name: 'demo',
        seq: 1,
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 900, row: { id: 12, name: 'Alice', value: 100 } },
        ],
      });
    });

    const valueInput = await screen.findByDisplayValue('100');
    await userEvent.clear(valueInput);
    await act(async () => {
      valueInput.blur();
    });

    // Nullable column: the server accepts and echoes the change.
    await act(async () => {
      socket.receive({
        type: 'CellUpdated',
        table_name: 'demo',
        seq: 2,
        row_id: 900,
        column: 'value',
        value: null,
      });
    });
    expect((valueInput as HTMLInputElement).value).toBe('');
    expect(screen.getByRole('status').textContent).toContain('Change confirmed by the server.');
  });

  it('reverts non-numeric text typed into a numeric cell', async () => {
    render(<LiveTable tableName="demo" />);

    const socket = FakeWebSocket.instances[0];
    await act(async () => {
      socket.open();
      socket.receive({
        type: 'TableData',
        table_name: 'demo',
        seq: 1,
        columns: ['id', 'name', 'value'],
        rows: [
          { row_id: 900, row: { id: 12, name: 'Alice', value: 100 } },
        ],
      });
    });

    const valueInput = await screen.findByDisplayValue('100');
    await userEvent.clear(valueInput);
    await userEvent.type(valueInput, 'abc');
    const messagesBefore = socket.sentMessages.length;
    await act(async () => {
      valueInput.blur();
    });

    // Unparseable input: nothing is sent and the cell snaps back.
    expect(socket.sentMessages.length).toBe(messagesBefore);
    expect((valueInput as HTMLInputElement).value).toBe('100');
    expect(screen.getByRole('alert').textContent).toContain('Enter a finite number.');
  });

  it('discards with Escape and waits for confirmation after Enter', async () => {
    const socket = await loadEditor();
    const cell = screen.getByLabelText('Edit value for row 900');
    await userEvent.clear(cell);
    await userEvent.type(cell, '123{Escape}');
    expect(inputValue('Edit value for row 900')).toBe('100');
    expect(socket.sentMessages).toHaveLength(2);
    await userEvent.clear(cell);
    await userEvent.type(cell, '123{Enter}');
    expect(screen.getByRole('status').textContent).toContain('Waiting for server confirmation');
    expect(inputValue('Edit value for row 900')).toBe('100');
    await act(async () => socket.receive({ type: 'CellUpdated', table_name: 'demo', seq: 2, row_id: 900, column: 'value', value: 123 }));
    expect(inputValue('Edit value for row 900')).toBe('123');
    expect(screen.getByRole('status').textContent).toContain('Change confirmed');
  });

  it('keeps focus on the next cell when Tab submits a change', async () => {
    const socket = await loadEditor();
    const cell = screen.getByLabelText('Edit name for row 900');
    await userEvent.clear(cell);
    await userEvent.type(cell, 'Updated');
    await userEvent.tab();
    const next = screen.getByLabelText('Edit value for row 900');
    expect(document.activeElement).toBe(next);
    await act(async () => socket.receive({ type: 'CellUpdated', table_name: 'demo', seq: 2, row_id: 900, column: 'name', value: 'Updated' }));
    expect(document.activeElement).toBe(next);
    expect((next as HTMLInputElement).readOnly).toBe(false);
  });

  it('searches and sorts locally while mutations keep stable server row IDs', async () => {
    const socket = await loadEditor();
    await userEvent.click(screen.getByRole('button', { name: 'Sort by value' }));
    expect(screen.getAllByRole('button', { name: /^Select row/ })[0].getAttribute('aria-label')).toBe('Select row 901');
    await userEvent.type(screen.getByLabelText('Search table'), 'Item 2');
    expect(screen.queryByLabelText('Edit name for row 900')).toBeNull();
    expect(socket.sentMessages).toHaveLength(2);
    await userEvent.clear(screen.getByLabelText('Edit value for row 901'));
    await userEvent.type(screen.getByLabelText('Edit value for row 901'), '250{Enter}');
    expect(socket.sentMessages.slice(-1)[0]).toEqual({ type: 'UpdateCell', table_name: 'demo', row_id: 901, column: 'value', value: 250 });
  });

  it('requires confirmation to delete and waits for the server before removing a row', async () => {
    const socket = await loadEditor();
    await userEvent.click(screen.getByRole('button', { name: 'Select row 901' }));
    await userEvent.click(screen.getByRole('button', { name: 'Delete selected row' }));
    expect(screen.getByRole('alertdialog').textContent).toContain('There is no undo');
    await userEvent.click(screen.getByRole('button', { name: 'Keep row' }));
    expect(socket.sentMessages).toHaveLength(2);
    await userEvent.click(screen.getByRole('button', { name: 'Delete selected row' }));
    await userEvent.click(screen.getByRole('button', { name: 'Delete permanently' }));
    expect(socket.sentMessages.slice(-1)[0]).toEqual({ type: 'DeleteRow', table_name: 'demo', row_id: 901 });
    expect(screen.queryByLabelText('Edit name for row 901')).not.toBeNull();
    await act(async () => socket.receive({ type: 'RowDeleted', table_name: 'demo', seq: 2, row_id: 901 }));
    expect(screen.queryByLabelText('Edit name for row 901')).toBeNull();
    expect(screen.getByRole('status').textContent).toContain('Row removed');
  });

  it('keeps cancelled drafts local and selects a server-confirmed new row', async () => {
    const socket = await loadEditor();
    await userEvent.click(screen.getByRole('button', { name: 'Add row' }));
    await userEvent.click(screen.getByRole('button', { name: 'Cancel new row' }));
    expect(socket.sentMessages).toHaveLength(2);
    await userEvent.click(screen.getByRole('button', { name: 'Add row' }));
    await userEvent.click(screen.getByRole('button', { name: 'Create row' }));
    expect(screen.queryByLabelText('Edit id for row 999')).toBeNull();
    await act(async () => socket.receive({ type: 'RowInserted', table_name: 'demo', seq: 2, index: 2, row_id: 999, row: { id: 3, name: 'New Item 3', value: 0 } }));
    expect(screen.queryByLabelText('New id')).toBeNull();
    expect(screen.getByRole('heading', { name: 'Row 999' })).toBeTruthy();
    expect(screen.getByRole('status').textContent).toContain('New row confirmed');
  });

  it('bounds rendered rows and searches across all pages', async () => {
    const socket = await loadEditor(60);
    expect(screen.getAllByRole('button', { name: /^Select row/ })).toHaveLength(25);
    await userEvent.click(screen.getByRole('button', { name: 'Next page' }));
    expect(screen.getAllByRole('button', { name: /^Select row/ })[0].getAttribute('aria-label')).toBe('Select row 925');
    await userEvent.type(screen.getByLabelText('Search table'), 'Item 60');
    expect(screen.getAllByRole('button', { name: /^Select row/ })).toHaveLength(1);
    expect(screen.getByLabelText('Edit name for row 959')).toBeTruthy();
    expect(socket.sentMessages).toHaveLength(2);
  });

  it('locks mutations until the new connection has a snapshot', async () => {
    render(<LiveTable tableName="demo" />);
    const socket = FakeWebSocket.instances[0];
    await act(async () => socket.open());
    expect((screen.getByRole('button', { name: 'Add row' }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText('Loading snapshot')).toBeTruthy();
    await act(async () => socket.receive({ type: 'TableData', table_name: 'demo', seq: 0, columns: ['product', 'amount'], rows: [] }));
    await userEvent.click(screen.getByRole('button', { name: 'Add row' }));
    expect(inputValue('New amount')).toBe('0');
    await userEvent.click(screen.getByRole('button', { name: 'Create row' }));
    expect(socket.sentMessages.slice(-1)[0]).toEqual({ type: 'InsertRow', table_name: 'demo', row: { product: '', amount: 0 } });
  });

  it('retains numeric editing after a column becomes entirely null', async () => {
    const socket = await loadEditor(1);
    await act(async () => socket.receive({ type: 'CellUpdated', table_name: 'demo', seq: 2, row_id: 900, column: 'value', value: null }));
    await userEvent.type(screen.getByLabelText('Edit value for row 900'), '42{Enter}');
    expect(socket.sentMessages.slice(-1)[0]).toEqual({ type: 'UpdateCell', table_name: 'demo', row_id: 900, column: 'value', value: 42 });
  });

  it('warns about unconfirmed changes on disconnect without retrying them', async () => {
    const socket = await loadEditor();
    await userEvent.clear(screen.getByLabelText('Edit value for row 900'));
    await userEvent.type(screen.getByLabelText('Edit value for row 900'), '123{Enter}');
    await act(async () => socket.close());
    expect(screen.getByRole('alert').textContent).toContain('Check the refreshed table before retrying');
    expect(inputValue('Edit value for row 900')).toBe('100');
    expect(socket.sentMessages.filter(message => message.type === 'UpdateCell')).toHaveLength(1);
  });

  it('keeps the current page across edits and clamps it when rows disappear', async () => {
    const socket = await loadEditor(26);
    await userEvent.click(screen.getByRole('button', { name: 'Next page' }));
    await userEvent.clear(screen.getByLabelText('Edit value for row 925'));
    await userEvent.type(screen.getByLabelText('Edit value for row 925'), '42{Enter}');
    await act(async () => socket.receive({ type: 'CellUpdated', table_name: 'demo', seq: 2, row_id: 925, column: 'value', value: 42 }));
    expect(inputValue('Edit value for row 925')).toBe('42');
    expect(screen.getAllByRole('button', { name: /^Select row/ })).toHaveLength(1);
    await act(async () => socket.receive({ type: 'RowDeleted', table_name: 'demo', seq: 3, row_id: 925 }));
    expect(screen.getAllByRole('button', { name: /^Select row/ })).toHaveLength(25);
    expect(screen.getByLabelText('Edit value for row 900')).toBeTruthy();
  });

  it('releases a timed-out edit without claiming success or automatically retrying', async () => {
    const socket = await loadEditor();
    const cell = screen.getByLabelText('Edit value for row 900');
    await userEvent.clear(cell);
    await userEvent.type(cell, '42');
    vi.useFakeTimers();
    await act(async () => cell.blur());
    await act(async () => vi.advanceTimersByTime(10001));
    expect(screen.getByRole('alert').textContent).toContain('the change may have applied');
    expect(screen.getByRole('status').textContent).not.toContain('Change confirmed');
    expect(inputValue('Edit value for row 900')).toBe('100');
    expect((cell as HTMLInputElement).readOnly).toBe(false);
    expect(socket.sentMessages.filter(message => message.type === 'UpdateCell')).toHaveLength(1);
  });
});
