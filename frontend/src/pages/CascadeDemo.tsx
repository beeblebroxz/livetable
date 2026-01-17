import { useState, useMemo, useEffect, useRef } from 'react';
import { useTableWebSocket, TableRow } from '../hooks/useTableWebSocket';

// Types
interface SaleRow extends TableRow {
  region: string;
  product: string;
  amount: number;
}

interface AggRow {
  region: string;
  total: number;
  count: number;
}

// Utility
const formatMoney = (amount: number) => `$${amount.toLocaleString()}`;

// Connection status badge
function ConnectionBadge({ connected }: { connected: boolean }) {
  return (
    <div className={`flex items-center gap-2 px-3 py-1 rounded-full text-sm ${
      connected ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
    }`}>
      <span className={`w-2 h-2 rounded-full ${connected ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
      {connected ? 'Connected' : 'Disconnected'}
    </div>
  );
}

// Table card component
function TableCard({
  title,
  subtitle,
  color,
  count,
  children,
  highlight
}: {
  title: string;
  subtitle: string;
  color: 'blue' | 'green' | 'purple' | 'orange';
  count: number;
  children: React.ReactNode;
  highlight?: boolean;
}) {
  const colorClasses = {
    blue: 'border-blue-200 bg-blue-50',
    green: 'border-green-200 bg-green-50',
    purple: 'border-purple-200 bg-purple-50',
    orange: 'border-orange-200 bg-orange-50',
  };

  const headerClasses = {
    blue: 'bg-blue-100 text-blue-800',
    green: 'bg-green-100 text-green-800',
    purple: 'bg-purple-100 text-purple-800',
    orange: 'bg-orange-100 text-orange-800',
  };

  return (
    <div className={`rounded-lg border-2 transition-all duration-300 ${colorClasses[color]} ${highlight ? 'ring-2 ring-offset-2 ring-yellow-400 shadow-lg' : ''}`}>
      <div className={`px-4 py-2 border-b flex justify-between items-center ${headerClasses[color]}`}>
        <div>
          <h3 className="font-bold">{title}</h3>
          <p className="text-xs opacity-75">{subtitle}</p>
        </div>
        <span className="text-sm font-mono bg-white/50 px-2 py-0.5 rounded">
          {count} rows
        </span>
      </div>
      <div className="p-3 max-h-64 overflow-y-auto">{children}</div>
    </div>
  );
}

// Simple table display with highlight for new rows
function SimpleTable({
  headers,
  rows,
  highlightIndices,
  formatters = {}
}: {
  headers: string[];
  rows: TableRow[];
  highlightIndices?: Set<number>;
  formatters?: Record<string, (v: number) => string>;
}) {
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b">
          {headers.map(h => (
            <th key={h} className="text-left py-1 px-2 font-semibold text-gray-600">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, idx) => (
          <tr
            key={idx}
            className={`border-b border-gray-100 transition-all duration-700 ${
              highlightIndices?.has(idx)
                ? 'bg-yellow-200 animate-pulse'
                : ''
            }`}
          >
            {headers.map(h => {
              const value = row[h];
              const formatted = formatters[h] && typeof value === 'number'
                ? formatters[h](value)
                : value;
              return (
                <td key={h} className="py-1 px-2">
                  {formatted as string | number}
                </td>
              );
            })}
          </tr>
        ))}
        {rows.length === 0 && (
          <tr>
            <td colSpan={headers.length} className="py-4 px-2 text-gray-400 text-center">
              Waiting for data...
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}

// Stats card
function StatCard({ label, value, color }: { label: string; value: string | number; color: string }) {
  return (
    <div className={`bg-${color}-50 border border-${color}-200 rounded-lg px-4 py-2`}>
      <div className="text-xs text-gray-500">{label}</div>
      <div className="text-xl font-bold">{value}</div>
    </div>
  );
}

// Main demo component
interface CascadeDemoProps {
  onBack: () => void;
}

export function CascadeDemo({ onBack }: CascadeDemoProps) {
  // Connect to WebSocket
  const { data, connected } = useTableWebSocket('demo');

  // Track recently added rows for highlighting
  const [recentIndices, setRecentIndices] = useState<Set<number>>(new Set());
  const prevLengthRef = useRef(0);

  // When new rows arrive, highlight them briefly
  useEffect(() => {
    if (data.length > prevLengthRef.current) {
      const newIndices = new Set<number>();
      for (let i = prevLengthRef.current; i < data.length; i++) {
        newIndices.add(i);
      }
      setRecentIndices(newIndices);

      // Clear highlights after animation
      const timer = setTimeout(() => {
        setRecentIndices(new Set());
      }, 1500);

      prevLengthRef.current = data.length;
      return () => clearTimeout(timer);
    }
    prevLengthRef.current = data.length;
  }, [data.length]);

  // Cast data to SaleRow type
  const sales = data as SaleRow[];

  // Derived views (computed from real data)
  const filteredView = useMemo(() =>
    sales.filter(row => row.amount > 500),
    [sales]
  );

  const sortedView = useMemo(() =>
    [...sales].sort((a, b) => b.amount - a.amount),
    [sales]
  );

  const aggregateView = useMemo(() => {
    const groups: Record<string, AggRow> = {};
    for (const row of sales) {
      if (!row.region) continue;
      if (!groups[row.region]) {
        groups[row.region] = { region: row.region, total: 0, count: 0 };
      }
      groups[row.region].total += row.amount || 0;
      groups[row.region].count += 1;
    }
    return Object.values(groups).sort((a, b) => b.total - a.total);
  }, [sales]);

  // Stats
  const totalSales = sales.reduce((sum, row) => sum + (row.amount || 0), 0);
  const avgSale = sales.length > 0 ? totalSales / sales.length : 0;
  const highValueCount = filteredView.length;

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
      {/* Header */}
      <header className="bg-white shadow-md">
        <div className="max-w-7xl mx-auto py-4 px-4 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
              Real-Time Reactive Views
              <ConnectionBadge connected={connected} />
            </h1>
            <p className="text-gray-600 text-sm">
              Watch cascading view updates from live WebSocket stream
            </p>
          </div>
          <button
            onClick={onBack}
            className="px-4 py-2 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition"
          >
            &larr; Back to Editor
          </button>
        </div>
      </header>

      {/* Main content */}
      <main className="max-w-7xl mx-auto py-6 px-4">
        {/* Stats bar */}
        <div className="bg-white rounded-lg shadow-md p-4 mb-6">
          <div className="flex flex-wrap items-center gap-6">
            <div className="bg-blue-50 border border-blue-200 rounded-lg px-4 py-2">
              <div className="text-xs text-gray-500">Total Rows</div>
              <div className="text-xl font-bold">{sales.length}</div>
            </div>
            <div className="bg-green-50 border border-green-200 rounded-lg px-4 py-2">
              <div className="text-xs text-gray-500">High-Value (&gt;$500)</div>
              <div className="text-xl font-bold">{highValueCount}</div>
            </div>
            <div className="bg-purple-50 border border-purple-200 rounded-lg px-4 py-2">
              <div className="text-xs text-gray-500">Total Revenue</div>
              <div className="text-xl font-bold">{formatMoney(totalSales)}</div>
            </div>
            <div className="bg-orange-50 border border-orange-200 rounded-lg px-4 py-2">
              <div className="text-xs text-gray-500">Avg Sale</div>
              <div className="text-xl font-bold">{formatMoney(Math.round(avgSale))}</div>
            </div>
            <div className="bg-gray-50 border border-gray-200 rounded-lg px-4 py-2">
              <div className="text-xs text-gray-500">Regions</div>
              <div className="text-xl font-bold">{aggregateView.length}</div>
            </div>
          </div>
        </div>

        {/* Architecture diagram */}
        <div className="bg-white rounded-lg shadow-md p-4 mb-6">
          <div className="flex items-center justify-center gap-2 text-sm text-gray-600 font-mono">
            <span className="px-3 py-1 bg-blue-100 rounded text-blue-700">WebSocket Stream</span>
            <span>&rarr;</span>
            <span className="px-3 py-1 bg-blue-100 rounded text-blue-700">Base Table</span>
            <span>──┬─&gt;</span>
            <span className="px-3 py-1 bg-green-100 rounded text-green-700">FilterView (&gt;$500)</span>
          </div>
          <div className="flex items-center justify-center gap-2 text-sm text-gray-600 font-mono mt-1">
            <span className="w-64"></span>
            <span className="ml-6">├─&gt;</span>
            <span className="px-3 py-1 bg-purple-100 rounded text-purple-700">SortedView (by amount)</span>
          </div>
          <div className="flex items-center justify-center gap-2 text-sm text-gray-600 font-mono mt-1">
            <span className="w-64"></span>
            <span className="ml-6">└─&gt;</span>
            <span className="px-3 py-1 bg-orange-100 rounded text-orange-700">AggregateView (by region)</span>
          </div>
        </div>

        {/* Tables grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Base Table */}
          <TableCard
            title="[1] Base Table"
            subtitle="All incoming sales from WebSocket"
            color="blue"
            count={sales.length}
            highlight={recentIndices.size > 0}
          >
            <SimpleTable
              headers={['region', 'product', 'amount']}
              rows={sales.slice(-20).reverse()}
              highlightIndices={new Set([0])}
              formatters={{ amount: formatMoney }}
            />
            {sales.length > 20 && (
              <div className="text-xs text-gray-400 mt-2 text-center">
                Showing last 20 of {sales.length} rows
              </div>
            )}
          </TableCard>

          {/* Filter View */}
          <TableCard
            title="[2] FilterView"
            subtitle="amount > $500 only"
            color="green"
            count={filteredView.length}
          >
            <SimpleTable
              headers={['region', 'product', 'amount']}
              rows={filteredView.slice(-15).reverse()}
              formatters={{ amount: formatMoney }}
            />
            <div className="mt-2 text-xs text-gray-500">
              {sales.length - filteredView.length} rows filtered out
            </div>
          </TableCard>

          {/* Sorted View */}
          <TableCard
            title="[3] SortedView"
            subtitle="Ordered by amount descending"
            color="purple"
            count={sortedView.length}
          >
            <SimpleTable
              headers={['region', 'product', 'amount']}
              rows={sortedView.slice(0, 15)}
              formatters={{ amount: formatMoney }}
            />
            {sortedView.length > 0 && (
              <div className="mt-2 text-xs text-gray-500">
                Top sale: {formatMoney(sortedView[0]?.amount || 0)}
              </div>
            )}
          </TableCard>

          {/* Aggregate View */}
          <TableCard
            title="[4] AggregateView"
            subtitle="Grouped by region"
            color="orange"
            count={aggregateView.length}
          >
            <SimpleTable
              headers={['region', 'total', 'count']}
              rows={aggregateView}
              formatters={{ total: formatMoney }}
            />
          </TableCard>
        </div>

        {/* Instructions */}
        <div className="mt-6 bg-gray-50 rounded-lg p-4 text-sm text-gray-600">
          <h3 className="font-semibold text-gray-700 mb-2">How to use:</h3>
          <ol className="list-decimal list-inside space-y-1">
            <li>Start the backend: <code className="bg-gray-200 px-1 rounded">cd impl && cargo run --bin livetable-server --features server</code></li>
            <li>Start the publisher: <code className="bg-gray-200 px-1 rounded">cd examples && python3 streaming_publisher.py</code></li>
            <li>Watch the tables update in real-time as new sales stream in</li>
          </ol>
          <p className="mt-3 text-gray-500">
            Publisher options: <code className="bg-gray-200 px-1 rounded">--fast</code> (0.5s) or <code className="bg-gray-200 px-1 rounded">--slow</code> (5s)
          </p>
        </div>
      </main>
    </div>
  );
}
