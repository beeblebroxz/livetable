import React, { useMemo } from 'react';
import { useReactTable, getCoreRowModel, flexRender, ColumnDef } from '@tanstack/react-table';
import { useTableWebSocket, TableRow } from '../hooks/useTableWebSocket';

interface LiveTableProps {
  tableName: string;
}

export function LiveTable({ tableName }: LiveTableProps) {
  const { data, columns: columnNames, connected, insertRow, updateCell, deleteRow } = useTableWebSocket(tableName);

  const columns = useMemo<ColumnDef<TableRow>[]>(() => {
    if (columnNames.length === 0) return [];

    return columnNames.map((colName) => ({
      id: colName,
      accessorKey: colName,
      header: colName.charAt(0).toUpperCase() + colName.slice(1),
      cell: ({ getValue, row, column }: any) => {
        const initialValue = getValue();
        const [value, setValue] = React.useState(initialValue);

        React.useEffect(() => {
          setValue(initialValue);
        }, [initialValue]);

        const onBlur = () => {
          if (value !== initialValue) {
            let convertedValue = value;
            if (colName === 'id') {
              convertedValue = parseInt(value as string);
            } else if (colName === 'value') {
              convertedValue = parseFloat(value as string);
            }
            updateCell(row.index, column.id, convertedValue);
          }
        };

        return (
          <input
            value={value as string ?? ''}
            onChange={(e) => setValue(e.target.value)}
            onBlur={onBlur}
            className="w-full px-2 py-1 border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        );
      },
    }));
  }, [columnNames, updateCell]);

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  const addRow = () => {
    const newRow: TableRow = {};
    columnNames.forEach((col) => {
      if (col === 'id') {
        newRow[col] = data.length + 1;
      } else if (col === 'name') {
        newRow[col] = `New Item ${data.length + 1}`;
      } else if (col === 'value') {
        newRow[col] = Math.random() * 100;
      } else {
        newRow[col] = '';
      }
    });
    insertRow(newRow);
  };

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Table: {tableName}</h2>
          <p className="text-sm text-gray-600 mt-1">
            {connected ? (
              <span className="text-green-600 font-semibold">● Connected</span>
            ) : (
              <span className="text-red-600 font-semibold">● Disconnected</span>
            )}
          </p>
        </div>
        <button
          onClick={addRow}
          disabled={!connected}
          className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition disabled:bg-gray-400 disabled:cursor-not-allowed font-semibold shadow-md"
        >
          + Add Row
        </button>
      </div>

      <div className="overflow-x-auto shadow-lg rounded-lg border border-gray-200">
        <table className="w-full border-collapse bg-white">
          <thead>
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id} className="bg-gradient-to-r from-blue-600 to-blue-700">
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    className="border-b border-blue-800 p-4 text-left font-bold text-white"
                  >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
                <th className="border-b border-blue-800 p-4 text-left font-bold text-white">
                  Actions
                </th>
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.length === 0 ? (
              <tr>
                <td
                  colSpan={columnNames.length + 1}
                  className="p-8 text-center text-gray-500"
                >
                  No data available. Click "Add Row" to insert data.
                </td>
              </tr>
            ) : (
              table.getRowModel().rows.map((row, idx) => (
                <tr
                  key={row.id}
                  className={`hover:bg-blue-50 transition ${
                    idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'
                  }`}
                >
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="border-b border-gray-200 p-3">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                  <td className="border-b border-gray-200 p-3">
                    <button
                      onClick={() => deleteRow(row.index)}
                      className="px-4 py-2 bg-red-500 text-white rounded hover:bg-red-600 transition text-sm font-semibold"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="mt-4 text-sm text-gray-600 flex justify-between items-center">
        <span>
          Total rows: <strong>{data.length}</strong>
        </span>
        <span className="text-xs text-gray-500">
          Real-time updates powered by Rust + WebSocket
        </span>
      </div>
    </div>
  );
}
