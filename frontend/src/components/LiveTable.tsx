import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from 'react';
import { useReactTable, getCoreRowModel, getSortedRowModel, getPaginationRowModel, flexRender, type CellContext, type ColumnDef, type SortingState } from '@tanstack/react-table';
import { useTableWebSocket } from '../hooks/useTableWebSocket';
import { buildDraftRow } from '../lib/liveTableDraftRow';
import type { ScalarValue, TableRecord, TableRow } from '../types';
import '../pages/OrdersLab.css';
import './LiveTable.css';

type ValueKind = 'string' | 'number' | 'boolean';
type PendingEdit =
  | { kind: 'update'; rowId: number; column: string; value: ScalarValue }
  | { kind: 'insert'; row: TableRow; existingIds: Set<number> }
  | { kind: 'delete'; rowId: number };

interface EditorMeta {
  kinds: Record<string, ValueKind>;
  locked: boolean;
  saveCell: (rowId: number, column: string, value: ScalarValue) => void;
  selectRow: (rowId: number) => void;
  reportError: (message: string) => void;
}

// Keep the renderer's component identity stable across connection/pending state
// changes so a save does not remount inputs and swallow keyboard focus.
function TableCell({ row, column, table }: CellContext<TableRecord, unknown>) {
  const meta = table.options.meta as EditorMeta;
  return <EditableCell initialValue={row.original.values[column.id] ?? null} rowId={row.original.rowId}
    columnId={column.id} kind={meta.kinds[column.id]} locked={meta.locked} updateCell={meta.saveCell}
    selectRow={meta.selectRow} reportError={meta.reportError} />;
}

function coerceValue(raw: string, kind: ValueKind): ScalarValue {
  if (kind === 'number') {
    if (!raw.trim()) return null;
    const value = Number(raw);
    if (!Number.isFinite(value)) throw new Error('Enter a finite number.');
    return value;
  }
  if (kind === 'boolean') {
    if (!raw.trim()) return null;
    if (/^(true|false)$/i.test(raw.trim())) return raw.trim().toLowerCase() === 'true';
    throw new Error('Enter true or false.');
  }
  return raw;
}

function EditableCell({ initialValue, rowId, columnId, kind, locked, updateCell, selectRow, reportError }: {
  initialValue: ScalarValue;
  rowId: number;
  columnId: string;
  kind: ValueKind;
  locked: boolean;
  updateCell: (rowId: number, column: string, value: ScalarValue) => void;
  selectRow: (rowId: number) => void;
  reportError: (message: string) => void;
}) {
  const [value, setValue] = useState(String(initialValue ?? ''));
  const cancelled = useRef(false);
  useEffect(() => setValue(String(initialValue ?? '')), [initialValue]);

  const save = () => {
    // Only the server's echo changes the displayed, committed value.
    setValue(String(initialValue ?? ''));
    if (cancelled.current) { cancelled.current = false; return; }
    if (locked) return;
    try {
      const next = value === '' && initialValue === null ? null : coerceValue(value, kind);
      if (next !== initialValue) updateCell(rowId, columnId, next);
    } catch (error) {
      reportError(`${columnId}: ${(error as Error).message}`);
    }
  };

  return <input
    aria-label={`Edit ${columnId} for row ${rowId}`}
    className={`editor-cell ${kind === 'number' ? 'number-cell' : ''}`}
    value={value}
    placeholder={initialValue === null ? 'NULL' : 'Empty'}
    inputMode={kind === 'number' ? 'decimal' : 'text'}
    readOnly={locked}
    onFocus={() => selectRow(rowId)}
    onChange={event => setValue(event.target.value)}
    onBlur={save}
    onKeyDown={event => {
      if (event.nativeEvent.isComposing) return;
      if (event.key === 'Enter') { event.preventDefault(); event.currentTarget.blur(); }
      if (event.key === 'Escape') {
        event.preventDefault();
        cancelled.current = true;
        setValue(String(initialValue ?? ''));
        event.currentTarget.blur();
      }
    }}
  />;
}

export function LiveTable({ tableName }: { tableName: string }) {
  const { data, columns: columnNames, connected, ready, error, errorRevision, clearError, sequence, insertRow, updateCell, deleteRow } = useTableWebSocket(tableName);
  const [search, setSearch] = useState('');
  const [sorting, setSorting] = useState<SortingState>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [draft, setDraft] = useState<Record<string, string> | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [notice, setNotice] = useState('');
  const [localError, setLocalError] = useState('');
  const [pending, setPending] = useState<PendingEdit | null>(null);
  const pendingRef = useRef(pending);
  pendingRef.current = pending;
  const previousError = useRef(errorRevision);
  // The flat protocol has values, not schema types. Retain observed types when
  // a column temporarily contains only nulls; server validation is authoritative.
  const knownKinds = useRef<{ table: string; kinds: Record<string, ValueKind> }>({ table: tableName, kinds: {} });
  const kinds = useMemo(() => Object.fromEntries(columnNames.map(column => {
    const sample = data.find(row => row.values[column] != null)?.values[column];
    const remembered = knownKinds.current.table === tableName ? knownKinds.current.kinds[column] : undefined;
    const kind: ValueKind = typeof sample === 'number' ? 'number' : typeof sample === 'boolean' ? 'boolean'
      : typeof sample === 'string' ? 'string' : remembered ?? (tableName === 'demo' && column === 'amount' ? 'number' : 'string');
    return [column, kind];
  })), [columnNames, data, tableName]);
  useEffect(() => { knownKinds.current = { table: tableName, kinds }; }, [tableName, kinds]);
  const selected = data.find(row => row.rowId === selectedId);
  const locked = !connected || !ready || pending !== null;

  useEffect(() => {
    if (!pending || !ready) return;
    // Generic writes have no request ID. Match the resulting server state;
    // this is confirmation of visible data, not a transactional receipt.
    let confirmed = false;
    if (pending.kind === 'update') confirmed = data.some(row => row.rowId === pending.rowId && row.values[pending.column] === pending.value);
    if (pending.kind === 'delete') confirmed = !data.some(row => row.rowId === pending.rowId);
    if (pending.kind === 'insert') {
      const inserted = data.find(row => !pending.existingIds.has(row.rowId) && Object.entries(pending.row).every(([key, value]) => row.values[key] === value));
      confirmed = Boolean(inserted);
      if (inserted) { setSelectedId(inserted.rowId); setDraft(null); }
    }
    if (confirmed) {
      setNotice(pending.kind === 'delete' ? 'Row removed from the shared table.' : pending.kind === 'insert' ? 'New row confirmed by the server.' : 'Change confirmed by the server.');
      setPending(null);
      setConfirmDelete(false);
    }
  }, [data, pending, ready]);

  useEffect(() => {
    if (previousError.current === errorRevision) return;
    previousError.current = errorRevision;
    setPending(null);
    setNotice('');
  }, [errorRevision]);

  useEffect(() => {
    if (!connected && pendingRef.current) {
      setPending(null);
      setNotice('');
      setLocalError('Connection lost during a change. Check the refreshed table before retrying.');
    }
  }, [connected]);

  useEffect(() => {
    if (!pending) return;
    const timer = window.setTimeout(() => {
      setPending(null);
      setLocalError('No matching server confirmation yet. Check the table before retrying; the change may have applied.');
    }, 10000);
    return () => window.clearTimeout(timer);
  }, [pending]);

  const begin = useCallback((edit: PendingEdit, send: () => boolean) => {
    if (pendingRef.current) return;
    setLocalError(''); setNotice(''); clearError();
    if (!send()) { setLocalError('Not connected. Wait for the table to reconnect.'); return; }
    pendingRef.current = edit;
    setPending(edit);
  }, [clearError]);
  const saveCell = useCallback((rowId: number, column: string, value: ScalarValue) => {
    begin({ kind: 'update', rowId, column, value }, () => updateCell(rowId, column, value));
  }, [begin, updateCell]);
  const selectRow = useCallback((rowId: number) => {
    setSelectedId(rowId); setDraft(null); setConfirmDelete(false);
  }, []);
  const columns = useMemo<ColumnDef<TableRecord>[]>(() => columnNames.map(column => ({
    id: column,
    accessorFn: record => record.values[column] ?? null,
    header: column,
    cell: TableCell,
  })), [columnNames]);
  const filtered = useMemo(() => {
    const query = search.trim().toLocaleLowerCase();
    return query ? data.filter(row => columnNames.some(column => String(row.values[column] ?? '').toLocaleLowerCase().includes(query))) : data;
  }, [data, search, columnNames]);
  const table = useReactTable({
    data: filtered, columns, getRowId: row => String(row.rowId), state: { sorting }, onSortingChange: setSorting,
    meta: { kinds, locked, saveCell, selectRow, reportError: setLocalError } satisfies EditorMeta,
    getCoreRowModel: getCoreRowModel(), getSortedRowModel: getSortedRowModel(), getPaginationRowModel: getPaginationRowModel(),
    autoResetPageIndex: false,
    initialState: { pagination: { pageSize: 25 } },
  });

  const openDraft = () => {
    const row = buildDraftRow(columnNames, data);
    for (const column of columnNames) if (row[column] === '' && kinds[column] === 'number') row[column] = 0;
    setDraft(Object.fromEntries(Object.entries(row).map(([key, value]) => [key, String(value ?? '')])));
    setSelectedId(null); setConfirmDelete(false); setLocalError(''); clearError();
  };
  const createRow = (event: FormEvent) => {
    event.preventDefault();
    if (!draft || locked) return;
    try {
      const row = Object.fromEntries(columnNames.map(column => [column, coerceValue(draft[column], kinds[column])]));
      begin({ kind: 'insert', row, existingIds: new Set(data.map(record => record.rowId)) }, () => insertRow(row));
    } catch (error) { setLocalError((error as Error).message); }
  };
  const pageIndex = table.getState().pagination.pageIndex;
  const pageCount = Math.max(1, table.getPageCount());
  useEffect(() => {
    if (pageIndex >= pageCount) table.setPageIndex(pageCount - 1);
  }, [pageIndex, pageCount, table]);

  return <div className="orders-lab table-editor">
    <header className="lab-header">
      <a className="lab-brand" href="#lab" aria-label="LiveTable lab home"><span className="brand-symbol" aria-hidden="true"><i /><i /><i /></span>livetable<span className="brand-divider" /><span className="brand-lab">LAB</span></a>
      <nav aria-label="Demo navigation"><a href="#lab">Orders Lab</a><a href="#editor" className="nav-current" aria-current="page">Table editor</a></nav>
      <span className={`connection-pill ${connected && ready ? 'connected' : ''}`}><i />{!connected ? 'Reconnecting' : ready ? 'Live connection' : 'Loading snapshot'}</span>
    </header>
    <main className="lab-main editor-main">
      <section className="lab-hero editor-hero">
        <div><div className="eyebrow"><span className="small-cross">+</span>THE SHARED SOURCE</div><h1>LiveTable <em>Editor.</em></h1><p>Make a change here. Every connected client follows.</p></div>
        <a className="secondary-button" href={`${window.location.pathname}${window.location.search}#editor`} target="_blank" rel="noreferrer">Open another editor <span aria-hidden="true">↗</span></a>
      </section>
      <div className="editor-workspace">
        <aside className="editor-sidebar" aria-label="Table information">
          <span className="eyebrow">WORKSPACE</span>
          <div className="editor-table-link"><span aria-hidden="true">▦</span><strong>{tableName}</strong><span>{data.length}</span></div>
          <span className="eyebrow editor-fields-heading">FIELDS</span>
          <ul>{columnNames.map(column => <li key={column}><span className="field-type">{kinds[column] === 'number' ? '#' : kinds[column] === 'boolean' ? '◐' : 'Aa'}</span>{column}</li>)}</ul>
          <p className="editor-sidebar-note">A shared, editable base table.<br />Separate from the Orders Lab dataset.</p>
        </aside>
        <section className="editor-grid-panel" aria-label={`${tableName} table editor`}>
          <div className="editor-grid-heading"><h2>{tableName}<span className="eyebrow">BASE TABLE</span></h2><span className="mono">seq {sequence ?? '—'}</span></div>
          <div className="editor-toolbar">
            <label className="editor-search"><svg width="16" height="16" viewBox="0 0 20 20" fill="none" aria-hidden="true"><circle cx="8" cy="8" r="5.5" stroke="currentColor" strokeWidth="1.5" /><path d="m12 12 5 5" stroke="currentColor" strokeWidth="1.5" /></svg><input aria-label="Search table" placeholder="Find in this table…" value={search} onChange={event => { setSearch(event.target.value); table.setPageIndex(0); }} /></label>
            <button className="primary-button" onClick={openDraft} disabled={locked}><span aria-hidden="true">+</span> Add row</button>
          </div>
          <div className="editor-grid-scroll" role="region" aria-label="Editable rows" tabIndex={0}>
            <table className="editor-grid">
              <thead>{table.getHeaderGroups().map(group => <tr key={group.id}><th scope="col" className="row-gutter"><span className="sr-only">Select row</span>#</th>{group.headers.map(header => <th key={header.id} scope="col" aria-sort={header.column.getIsSorted() === 'asc' ? 'ascending' : header.column.getIsSorted() === 'desc' ? 'descending' : 'none'}>
                <button aria-label={`Sort by ${header.id}`} onClick={header.column.getToggleSortingHandler()}><span className="field-type">{kinds[header.id] === 'number' ? '#' : kinds[header.id] === 'boolean' ? '◐' : 'Aa'}</span>{header.id}<span className="sort-direction" aria-hidden="true">{header.column.getIsSorted() === 'asc' ? '↑' : header.column.getIsSorted() === 'desc' ? '↓' : '↕'}</span></button>
              </th>)}</tr>)}</thead>
              <tbody>{table.getRowModel().rows.map((row, index) => <tr key={row.id} className={selectedId === row.original.rowId ? 'row-selected' : ''}>
                <td className="row-gutter"><button aria-label={`Select row ${row.original.rowId}`} aria-pressed={selectedId === row.original.rowId} onClick={() => selectRow(row.original.rowId)}>{pageIndex * 25 + index + 1}</button></td>
                {row.getVisibleCells().map(cell => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
              </tr>)}</tbody>
            </table>
            {!filtered.length && <div className="editor-empty"><span aria-hidden="true">▦</span><h3>{!ready ? 'Waiting for your table.' : search ? 'No matching rows.' : 'A clean slate.'}</h3><p>{!ready ? 'The server snapshot will appear here.' : search ? 'Try a different search or clear it to see every row.' : 'Create your first row to start collaborating.'}</p>{ready && <button className="secondary-button" onClick={search ? () => setSearch('') : openDraft} disabled={locked}>{search ? 'Clear search' : 'Create a row'}</button>}</div>}
          </div>
          <div className="editor-grid-footer"><span>{search ? `${filtered.length} matching · ` : ''}Total rows: <strong>{data.length}</strong></span><div><button className="editor-page-button" aria-label="Previous page" disabled={!table.getCanPreviousPage()} onClick={() => table.previousPage()}>←</button><span>{pageIndex + 1} / {pageCount}</span><button className="editor-page-button" aria-label="Next page" disabled={!table.getCanNextPage()} onClick={() => table.nextPage()}>→</button></div></div>
          <div className="editor-feedback" role="status"><span>{pending ? 'Waiting for server confirmation…' : notice || 'Edits apply after server confirmation.'}</span><span>Search & sort are local</span></div>
        </section>
        <aside className="editor-inspector" aria-label="Row inspector">
          <div className="eyebrow">{draft ? 'NEW RECORD' : selected ? 'SELECTED RECORD' : 'ROW INSPECTOR'}</div>
          {draft ? <form onSubmit={createRow}>
            <h2>Add a row.</h2><p>Review the values, then add them to the shared table.</p>
            <div className="editor-draft-fields">{columnNames.map((column, index) => <label key={column}>{column}<small>{kinds[column]}</small><input autoFocus={index === 0} aria-label={`New ${column}`} value={draft[column]} inputMode={kinds[column] === 'number' ? 'decimal' : 'text'} disabled={locked} onChange={event => setDraft({ ...draft, [column]: event.target.value })} /></label>)}</div>
            <button type="submit" className="primary-button" disabled={locked}>Create row</button><button type="button" className="quiet-button" disabled={pending !== null} onClick={() => setDraft(null)}>Cancel new row</button>
          </form> : selected ? <>
            <h2>Row {selected.rowId}<span className="record-marker" /></h2><p>Server-confirmed values.<br />Select a cell in the grid to edit.</p>
            <dl className="editor-record">{columnNames.map(column => <div key={column}><dt>{column}</dt><dd className={kinds[column] === 'number' ? 'mono' : ''}>{selected.values[column] === null ? <span className="null-value">NULL</span> : String(selected.values[column] ?? '') || <span className="null-value">Empty</span>}</dd></div>)}</dl>
            {confirmDelete ? <div className="editor-delete-confirm" role="alertdialog" aria-labelledby="delete-row-title" aria-describedby="delete-row-description"><h3 id="delete-row-title">Delete row {selected.rowId}?</h3><p id="delete-row-description">This removes it for every editor. There is no undo.</p><button className="editor-danger-button" disabled={locked} onClick={() => begin({ kind: 'delete', rowId: selected.rowId }, () => deleteRow(selected.rowId))}>Delete permanently</button><button className="quiet-button" disabled={pending !== null} onClick={() => setConfirmDelete(false)}>Keep row</button></div>
              : <button className="editor-delete-link" disabled={locked} onClick={() => setConfirmDelete(true)}>Delete selected row <span aria-hidden="true">↗</span></button>}
          </> : <><div className="inspector-illustration" aria-hidden="true"><i /><i /><i /><span>↖</span></div><h2>A closer look.</h2><p>Select any cell or row number to inspect a record. Changes are shared with every connected editor.</p></>}
          <div className="editor-keyboard"><span className="eyebrow">AT YOUR FINGERTIPS</span><p><kbd>Enter</kbd> Confirm a cell</p><p><kbd>Esc</kbd> Discard an edit</p><p><kbd>Tab</kbd> Save & move on</p></div>
        </aside>
      </div>
      {(localError || error) && <div className="editor-error" role="alert"><span>{localError || error}</span><button className="quiet-button" onClick={() => { setLocalError(''); clearError(); }}>Dismiss</button></div>}
      <footer className="editor-footer"><span><i /> One table. Every client in sync.</span><span>In-memory demo · changes last until the server restarts.</span></footer>
    </main>
  </div>;
}
