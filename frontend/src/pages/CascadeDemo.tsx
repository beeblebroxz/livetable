import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { usePipeline } from '../hooks/usePipeline';
import type {
  PipelineSnapshot,
  ScalarValue,
  TableRow,
  ViewNodeSpec,
} from '../types';

type ViewKind = 'filter' | 'sort' | 'group';
type NodeKind = 'base' | ViewKind;
type Accent = 'base' | 'filter' | 'sort' | 'group';

interface ViewDefinition {
  id: string;
  title: string;
  sourceId: string;
  kind: ViewKind;
  expression: string;
  defaultExpression: string;
}

interface EvaluatedNode {
  id: string;
  title: string;
  kind: NodeKind;
  sourceId?: string;
  sourceTitle?: string;
  expression?: string;
  rows: TableRow[];
  rowIds?: (number | null)[];
  columns: string[];
  tickKey: string;
  error?: string;
}

interface CascadeDemoProps {
  onBack: () => void;
}

const DEFAULT_DEFINITIONS: ViewDefinition[] = [
  {
    id: 'high-value',
    title: 'High Value Filter',
    sourceId: 'base',
    kind: 'filter',
    expression: 'amount >= 500',
    defaultExpression: 'amount >= 500',
  },
  {
    id: 'ranked',
    title: 'Ranked Sales',
    sourceId: 'high-value',
    kind: 'sort',
    expression: 'amount desc',
    defaultExpression: 'amount desc',
  },
  {
    id: 'regional-totals',
    title: 'Regional Totals',
    sourceId: 'ranked',
    kind: 'group',
    expression: 'region | total=sum(amount), average=avg(amount), count=count(amount)',
    defaultExpression: 'region | total=sum(amount), average=avg(amount), count=count(amount)',
  },
];

const SAMPLE_REGIONS = ['West', 'East', 'North', 'South', 'Central'];
const SAMPLE_PRODUCTS = ['Widget', 'Gadget', 'Premium', 'Basic', 'Deluxe', 'Ultra', 'Pro', 'Lite'];
const DEFAULT_BASE_COLUMNS = ['region', 'product', 'amount'];

const accentStyles: Record<Accent, {
  border: string;
  dot: string;
  badge: string;
  active: string;
}> = {
  base: {
    border: 'border-sky-200',
    dot: 'bg-sky-500',
    badge: 'border-sky-200 bg-sky-50 text-sky-800',
    active: 'ring-sky-300',
  },
  filter: {
    border: 'border-emerald-200',
    dot: 'bg-emerald-500',
    badge: 'border-emerald-200 bg-emerald-50 text-emerald-800',
    active: 'ring-emerald-300',
  },
  sort: {
    border: 'border-indigo-200',
    dot: 'bg-indigo-500',
    badge: 'border-indigo-200 bg-indigo-50 text-indigo-800',
    active: 'ring-indigo-300',
  },
  group: {
    border: 'border-amber-200',
    dot: 'bg-amber-500',
    badge: 'border-amber-200 bg-amber-50 text-amber-900',
    active: 'ring-amber-300',
  },
};

const isNumber = (value: ScalarValue): value is number =>
  typeof value === 'number' && Number.isFinite(value);

const formatMoney = (amount: number) =>
  `$${amount.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;

const formatCellValue = (column: string, value: ScalarValue): string => {
  if (value === null || value === undefined) {
    return '-';
  }

  if (typeof value === 'number') {
    const lowerColumn = column.toLowerCase();
    if (
      lowerColumn.includes('amount') ||
      lowerColumn.includes('total') ||
      lowerColumn.includes('average')
    ) {
      return formatMoney(value);
    }

    return Number.isInteger(value) ? String(value) : value.toFixed(2);
  }

  return String(value);
};

const parseSortExpression = (expression: string) => {
  const keys = expression
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const [field, direction = 'asc'] = part.split(/\s+/);
      const normalizedDirection = direction.toLowerCase();
      if (!field || !/^[A-Za-z_][\w]*$/.test(field)) {
        throw new Error(`Invalid sort field in "${part}"`);
      }
      if (normalizedDirection !== 'asc' && normalizedDirection !== 'desc') {
        throw new Error(`Sort direction must be asc or desc in "${part}"`);
      }
      return { column: field, descending: normalizedDirection === 'desc' };
    });
  if (keys.length === 0) {
    throw new Error('Sort expression needs at least one column');
  }
  return keys;
};

const parseGroupExpression = (expression: string) => {
  const parts = expression.split('|');
  if (parts.length !== 2) {
    throw new Error('Group expression must be "column | alias=op(column)"');
  }
  const groupBy = parts[0]
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);
  if (groupBy.length === 0 || groupBy.some((field) => !/^[A-Za-z_][\w]*$/.test(field))) {
    throw new Error('Group expression must start with a column name');
  }
  const aggs = parts[1]
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const parsed = part.match(/^([A-Za-z_][\w]*)\s*=\s*(.+)\(\s*([A-Za-z_][\w]*)\s*\)$/);
      if (!parsed) {
        throw new Error(`Could not parse aggregate "${part}"`);
      }
      const [, alias, op, column] = parsed;
      return { alias, op: op.trim(), column };
    });
  if (aggs.length === 0) {
    throw new Error('Group expression needs at least one aggregate');
  }
  return { group_by: groupBy, aggs };
};

const compilePipeline = (
  definitions: ViewDefinition[]
): { specs: ViewNodeSpec[]; errors: Record<string, string> } => {
  const specs: ViewNodeSpec[] = [];
  const errors: Record<string, string> = {};
  for (const definition of definitions) {
    try {
      if (definition.kind === 'filter') {
        specs.push({
          id: definition.id,
          source_id: definition.sourceId,
          kind: 'filter',
          predicate: definition.expression,
        });
      } else if (definition.kind === 'sort') {
        specs.push({
          id: definition.id,
          source_id: definition.sourceId,
          kind: 'sort',
          keys: parseSortExpression(definition.expression),
        });
      } else {
        specs.push({
          id: definition.id,
          source_id: definition.sourceId,
          kind: 'group',
          ...parseGroupExpression(definition.expression),
        });
      }
    } catch (error) {
      errors[definition.id] = error instanceof Error ? error.message : 'Expression failed';
      break;
    }
  }
  return { specs, errors };
};

const snapshotRows = (snapshot?: PipelineSnapshot): TableRow[] =>
  snapshot?.rows.map((record) => record.row) ?? [];

const parseEditedValue = (raw: string, previous: ScalarValue): ScalarValue => {
  if (typeof previous === 'number') {
    if (raw.trim() === '') {
      return previous;
    }
    const value = Number(raw);
    return Number.isFinite(value) ? value : previous;
  }
  if (typeof previous === 'boolean') {
    return raw.toLowerCase() === 'true';
  }
  if (previous === null) {
    return raw === '' ? null : raw;
  }
  return raw;
};

const createRandomSale = (): TableRow => ({
  region: SAMPLE_REGIONS[Math.floor(Math.random() * SAMPLE_REGIONS.length)],
  product: SAMPLE_PRODUCTS[Math.floor(Math.random() * SAMPLE_PRODUCTS.length)],
  amount: Math.round(100 + Math.random() * 2400),
});

function ConnectionBadge({ connected }: { connected: boolean }) {
  return (
    <span
      className={`inline-flex items-center gap-2 rounded-md border px-2.5 py-1 text-sm font-medium ${
        connected
          ? 'border-emerald-200 bg-emerald-50 text-emerald-800'
          : 'border-rose-200 bg-rose-50 text-rose-800'
      }`}
    >
      <span className={`h-2 w-2 rounded-full ${connected ? 'bg-emerald-500' : 'bg-rose-500'}`} />
      {connected ? 'Connected' : 'Disconnected'}
    </span>
  );
}

function FlowStrip({
  nodes,
  tickCounts,
  activeTicks,
}: {
  nodes: EvaluatedNode[];
  tickCounts: Record<string, number>;
  activeTicks: Set<string>;
}) {
  return (
    <div className="overflow-x-auto rounded-md border border-gray-200 bg-white">
      <div className="flex min-w-max items-center gap-2 px-4 py-3">
        {nodes.map((node, index) => {
          const accent = accentStyles[node.kind === 'base' ? 'base' : node.kind];
          return (
            <div key={node.id} className="flex items-center gap-2">
              {index > 0 && <span className="text-gray-300">-&gt;</span>}
              <div
                className={`rounded-md border px-3 py-2 text-sm transition ${
                  accent.badge
                } ${
                  activeTicks.has(node.id) ? `ring-2 ring-offset-2 ${accent.active}` : ''
                }`}
              >
                <div className="flex items-center gap-2 font-semibold">
                  <span className={`h-2 w-2 rounded-full ${accent.dot}`} />
                  {node.title}
                </div>
                <div className="mt-1 text-xs opacity-75">
                  {node.rows.length} rows · tick {tickCounts[node.id] ?? 0}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function DataPreview({
  columns,
  rows,
  rowIds,
  onUpdateCell,
  onDeleteRow,
  maxRows = 8,
}: {
  columns: string[];
  rows: TableRow[];
  rowIds?: (number | null)[];
  onUpdateCell?: (rowId: number, column: string, value: ScalarValue) => void;
  onDeleteRow?: (rowId: number) => void;
  maxRows?: number;
}) {
  const visibleRows = rows.slice(0, maxRows);

  return (
    <div className="overflow-x-auto">
      <table className="w-full table-fixed border-collapse text-sm">
        <thead>
          <tr className="border-b border-gray-200 text-left text-xs uppercase tracking-wide text-gray-500">
            {columns.map((column) => (
              <th key={column} className="px-3 py-2 font-semibold">
                {column}
              </th>
            ))}
            {onDeleteRow && <th className="w-20 px-3 py-2 font-semibold">Actions</th>}
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((row, rowIndex) => {
            const rowId = rowIds?.[rowIndex] ?? null;
            return (
              <tr key={rowId ?? `${rowIndex}-${JSON.stringify(row)}`} className="border-b border-gray-100">
                {columns.map((column) => {
                  const value = row[column] ?? null;
                  return (
                    <td key={column} className="truncate px-3 py-2 text-gray-800">
                      {rowId !== null && onUpdateCell ? (
                        <input
                          key={`${rowId}:${column}:${String(value)}`}
                          aria-label={`Edit ${column} for row ${rowId}`}
                          defaultValue={value === null ? '' : String(value)}
                          onBlur={(event) => {
                            const next = parseEditedValue(event.target.value, value);
                            if (next !== value) {
                              onUpdateCell(rowId, column, next);
                            }
                          }}
                          className="w-full rounded border border-transparent bg-transparent px-1 py-0.5 outline-none hover:border-gray-200 focus:border-sky-400 focus:bg-white"
                        />
                      ) : (
                        formatCellValue(column, value)
                      )}
                    </td>
                  );
                })}
                {onDeleteRow && (
                  <td className="px-3 py-2">
                    <button
                      type="button"
                      disabled={rowId === null}
                      onClick={() => rowId !== null && onDeleteRow(rowId)}
                      className="text-xs font-medium text-rose-700 hover:text-rose-900 disabled:text-gray-300"
                    >
                      Delete
                    </button>
                  </td>
                )}
              </tr>
            );
          })}
          {visibleRows.length === 0 && (
            <tr>
              <td colSpan={columns.length + (onDeleteRow ? 1 : 0)} className="px-3 py-8 text-center text-gray-400">
                No matching rows
              </td>
            </tr>
          )}
        </tbody>
      </table>
      {rows.length > maxRows && (
        <div className="border-t border-gray-100 px-3 py-2 text-xs text-gray-500">
          Showing {maxRows} of {rows.length} rows
        </div>
      )}
    </div>
  );
}

function NodePanel({
  node,
  definition,
  tickCount,
  isActive,
  onExpressionChange,
  onReset,
  onUpdateCell,
  onDeleteRow,
}: {
  node: EvaluatedNode;
  definition?: ViewDefinition;
  tickCount: number;
  isActive: boolean;
  onExpressionChange?: (expression: string) => void;
  onReset?: () => void;
  onUpdateCell?: (rowId: number, column: string, value: ScalarValue) => void;
  onDeleteRow?: (rowId: number) => void;
}) {
  const accent = accentStyles[node.kind === 'base' ? 'base' : node.kind];
  const expressionRows = node.kind === 'group' ? 3 : 2;

  return (
    <section
      className={`rounded-md border bg-white shadow-sm transition ${accent.border} ${
        isActive ? `ring-2 ring-offset-2 ${accent.active}` : ''
      }`}
    >
      <div className="flex items-start justify-between gap-4 border-b border-gray-100 px-4 py-3">
        <div>
          <div className="flex items-center gap-2">
            <span className={`h-2.5 w-2.5 rounded-full ${accent.dot}`} />
            <h2 className="text-base font-semibold text-gray-950">{node.title}</h2>
          </div>
          <p className="mt-1 text-xs text-gray-500">
            {node.sourceTitle ? `${node.sourceTitle} -> ${node.kind}` : 'WebSocket source'}
          </p>
        </div>
        <div className="shrink-0 text-right">
          <div className="text-lg font-semibold text-gray-950">{node.rows.length}</div>
          <div className="text-xs text-gray-500">rows · tick {tickCount}</div>
        </div>
      </div>

      {definition && onExpressionChange && onReset && (
        <div className="border-b border-gray-100 px-4 py-3">
          <div className="mb-2 flex items-center justify-between gap-3">
            <label htmlFor={`${node.id}-expression`} className="text-xs font-semibold uppercase text-gray-500">
              {definition.kind} expression
            </label>
            <button
              type="button"
              onClick={onReset}
              className="rounded-md border border-gray-200 px-2 py-1 text-xs font-medium text-gray-600 hover:border-gray-300 hover:bg-gray-50"
            >
              Reset
            </button>
          </div>
          <textarea
            id={`${node.id}-expression`}
            value={definition.expression}
            rows={expressionRows}
            onChange={(event) => onExpressionChange(event.target.value)}
            spellCheck={false}
            className="block w-full resize-none rounded-md border border-gray-300 bg-gray-50 px-3 py-2 font-mono text-sm text-gray-900 outline-none focus:border-sky-400 focus:bg-white focus:ring-2 focus:ring-sky-100"
          />
          {node.error && (
            <div className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-800">
              {node.error}
            </div>
          )}
        </div>
      )}

      {!definition && node.error && (
        <div className="border-b border-gray-100 px-4 py-3">
          <div className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-800">
            {node.error}
          </div>
        </div>
      )}

      <DataPreview
        columns={node.columns}
        rows={node.rows}
        rowIds={node.rowIds}
        onUpdateCell={onUpdateCell}
        onDeleteRow={onDeleteRow}
      />
    </section>
  );
}

export function CascadeDemo({ onBack }: CascadeDemoProps) {
  const [definitions, setDefinitions] = useState<ViewDefinition[]>(DEFAULT_DEFINITIONS);
  const compiled = useMemo(() => compilePipeline(definitions), [definitions]);
  const {
    connected,
    generation,
    snapshots,
    errors: serverErrors,
    insertRow,
    updateCell,
    deleteRow,
  } = usePipeline('demo', compiled.specs);
  const [autoStream, setAutoStream] = useState(false);
  const [tickCounts, setTickCounts] = useState<Record<string, number>>({});
  const [activeTicks, setActiveTicks] = useState<Set<string>>(new Set());
  const lastTickKeysRef = useRef<Record<string, string>>({});

  const nodes = useMemo(() => {
    const currentSnapshot = (id: string) => {
      const snapshot = snapshots[id];
      return snapshot?.generation === generation ? snapshot : undefined;
    };
    const baseSnapshot = currentSnapshot('base');
    const baseNode: EvaluatedNode = {
      id: 'base',
      title: 'Base Sales',
      kind: 'base',
      rows: snapshotRows(baseSnapshot),
      rowIds: baseSnapshot?.rows.map((record) => record.row_id),
      columns: baseSnapshot?.columns ?? DEFAULT_BASE_COLUMNS,
      tickKey: baseSnapshot ? `${baseSnapshot.generation}:${baseSnapshot.seq}` : '',
      error: serverErrors.base ?? serverErrors.pipeline ?? serverErrors.connection,
    };
    const byId = new Map<string, EvaluatedNode>([['base', baseNode]]);
    const materialized = [baseNode];
    for (const definition of definitions) {
      const source = byId.get(definition.sourceId) ?? baseNode;
      const snapshot = currentSnapshot(definition.id);
      const node: EvaluatedNode = {
        id: definition.id,
        title: definition.title,
        kind: definition.kind,
        sourceId: definition.sourceId,
        sourceTitle: source.title,
        expression: definition.expression,
        rows: snapshotRows(snapshot),
        columns: snapshot?.columns ?? source.columns,
        tickKey: snapshot ? `${snapshot.generation}:${snapshot.seq}` : '',
        error: compiled.errors[definition.id] ?? serverErrors[definition.id],
      };
      byId.set(node.id, node);
      materialized.push(node);
    }
    return materialized;
  }, [compiled.errors, definitions, generation, serverErrors, snapshots]);
  const baseRows = nodes[0].rows;
  const tickSignature = nodes.map((node) => `${node.id}:${node.tickKey}`).join('\n');
  const definitionById = useMemo(
    () => new Map(definitions.map((definition) => [definition.id, definition])),
    [definitions]
  );

  const pushRandomSale = useCallback(() => {
    insertRow(createRandomSale());
  }, [insertRow]);

  useEffect(() => {
    if (!autoStream || !connected) {
      return undefined;
    }

    const timer = window.setInterval(pushRandomSale, 1600);
    return () => window.clearInterval(timer);
  }, [autoStream, connected, pushRandomSale]);

  useEffect(() => {
    const changedIds = nodes
      .filter((node) => node.tickKey && lastTickKeysRef.current[node.id] !== node.tickKey)
      .map((node) => node.id);

    if (changedIds.length === 0) {
      return undefined;
    }

    lastTickKeysRef.current = Object.fromEntries(
      nodes.filter((node) => node.tickKey).map((node) => [node.id, node.tickKey])
    );
    setTickCounts((previous) => {
      const next = { ...previous };
      for (const id of changedIds) {
        next[id] = (next[id] ?? 0) + 1;
      }
      return next;
    });
    setActiveTicks(new Set(changedIds));

    const timer = window.setTimeout(() => setActiveTicks(new Set()), 750);
    return () => window.clearTimeout(timer);
  }, [nodes, tickSignature]);

  const updateExpression = (id: string, expression: string) => {
    setDefinitions((current) =>
      current.map((definition) =>
        definition.id === id ? { ...definition, expression } : definition
      )
    );
  };

  const resetExpression = (id: string) => {
    setDefinitions((current) =>
      current.map((definition) =>
        definition.id === id
          ? { ...definition, expression: definition.defaultExpression }
          : definition
      )
    );
  };

  const totalSales = baseRows.reduce((sum, row) => {
    const amount = row.amount;
    return isNumber(amount) ? sum + amount : sum;
  }, 0);
  const pipelineTicks = Object.values(tickCounts).reduce((sum, count) => sum + count, 0);

  return (
    <div className="min-h-screen bg-gray-100">
      <header className="border-b border-gray-200 bg-white">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-4 px-4 py-4">
          <div>
            <div className="flex flex-wrap items-center gap-3">
              <h1 className="text-2xl font-semibold text-gray-950">Forward Propagation Demo</h1>
              <ConnectionBadge connected={connected} />
            </div>
            <p className="mt-1 text-sm text-gray-600">
              Live base rows flow through editable derived tables on every tick.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={pushRandomSale}
              disabled={!connected}
              className="rounded-md bg-gray-950 px-3 py-2 text-sm font-semibold text-white hover:bg-gray-800 disabled:cursor-not-allowed disabled:bg-gray-300"
            >
              Insert sale
            </button>
            <button
              type="button"
              onClick={() => setAutoStream((current) => !current)}
              disabled={!connected}
              className={`rounded-md border px-3 py-2 text-sm font-semibold disabled:cursor-not-allowed disabled:border-gray-200 disabled:text-gray-400 ${
                autoStream
                  ? 'border-emerald-300 bg-emerald-50 text-emerald-800'
                  : 'border-gray-300 bg-white text-gray-700 hover:bg-gray-50'
              }`}
            >
              {autoStream ? 'Streaming on' : 'Auto stream'}
            </button>
            <button
              type="button"
              onClick={onBack}
              className="rounded-md border border-gray-300 bg-white px-3 py-2 text-sm font-semibold text-gray-700 hover:bg-gray-50"
            >
              Editor
            </button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-5">
        <div className="mb-5 grid grid-cols-2 gap-px overflow-hidden rounded-md border border-gray-200 bg-gray-200 md:grid-cols-4">
          <div className="bg-white px-4 py-3">
            <div className="text-xs font-medium uppercase text-gray-500">Base rows</div>
            <div className="mt-1 text-2xl font-semibold text-gray-950">{baseRows.length}</div>
          </div>
          <div className="bg-white px-4 py-3">
            <div className="text-xs font-medium uppercase text-gray-500">Revenue</div>
            <div className="mt-1 text-2xl font-semibold text-gray-950">{formatMoney(totalSales)}</div>
          </div>
          <div className="bg-white px-4 py-3">
            <div className="text-xs font-medium uppercase text-gray-500">Derived tables</div>
            <div className="mt-1 text-2xl font-semibold text-gray-950">{definitions.length}</div>
          </div>
          <div className="bg-white px-4 py-3">
            <div className="text-xs font-medium uppercase text-gray-500">Ticks observed</div>
            <div className="mt-1 text-2xl font-semibold text-gray-950">{pipelineTicks}</div>
          </div>
        </div>

        <div className="mb-5">
          <FlowStrip nodes={nodes} tickCounts={tickCounts} activeTicks={activeTicks} />
        </div>

        <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
          {nodes.map((node) => {
            const definition = definitionById.get(node.id);
            return (
              <NodePanel
                key={node.id}
                node={node}
                definition={definition}
                tickCount={tickCounts[node.id] ?? 0}
                isActive={activeTicks.has(node.id)}
                onExpressionChange={
                  definition ? (expression) => updateExpression(definition.id, expression) : undefined
                }
                onReset={definition ? () => resetExpression(definition.id) : undefined}
                onUpdateCell={node.kind === 'base' ? updateCell : undefined}
                onDeleteRow={node.kind === 'base' ? deleteRow : undefined}
              />
            );
          })}
        </div>
      </main>
    </div>
  );
}
