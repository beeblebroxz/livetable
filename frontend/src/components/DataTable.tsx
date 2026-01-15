import { useMemo, useState } from 'react';
import {
  ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  SortingState,
  useReactTable,
} from '@tanstack/react-table';
import type { ScalarValue, TableRow } from '../types';

interface DataTableProps {
  columns: string[];
  data: TableRow[];
}

const formatValue = (value: ScalarValue): string => {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'number') {
    return Number.isInteger(value) ? value.toString() : value.toFixed(2);
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  return String(value);
};

export function DataTable({ columns, data }: DataTableProps) {
  const [sorting, setSorting] = useState<SortingState>([]);

  const columnDefs = useMemo<ColumnDef<TableRow>[]>(
    () =>
      columns.map((column) => ({
        accessorKey: column,
        header: column,
        cell: ({ getValue }) => <span>{formatValue(getValue() as ScalarValue)}</span>,
      })),
    [columns]
  );

  const table = useReactTable({
    data,
    columns: columnDefs,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  if (!columns.length) {
    return <p className="muted">Waiting for schema…</p>;
  }

  return (
    <div className="table-wrapper">
      <table>
        <thead>
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th
                  key={header.id}
                  onClick={header.column.getToggleSortingHandler()}
                  className="sortable"
                >
                  {flexRender(header.column.columnDef.header, header.getContext())}
                  {{ asc: ' ↑', desc: ' ↓' }[header.column.getIsSorted() as string] ?? ''}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {!data.length && <p className="muted">No rows yet.</p>}
    </div>
  );
}
