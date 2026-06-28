import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTableWebSocket } from '../hooks/useTableWebSocket';
import type { ScalarValue, TableRow } from '../types';

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
  columns: string[];
  tickKey: string;
  error?: string;
}

interface AggregateSpec {
  alias: string;
  op: 'sum' | 'avg' | 'min' | 'max' | 'count';
  field: string | null;
}

interface GroupAccumulator {
  row: TableRow;
  count: number;
  values: Record<string, number[]>;
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
    expression: 'region | total=sum(amount), average=avg(amount), count=count()',
    defaultExpression: 'region | total=sum(amount), average=avg(amount), count=count()',
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

const inferColumns = (rows: TableRow[], fallback: string[] = DEFAULT_BASE_COLUMNS): string[] => {
  const columns = new Set<string>();
  for (const row of rows) {
    Object.keys(row).forEach((column) => columns.add(column));
  }
  return columns.size > 0 ? Array.from(columns) : fallback;
};

const rowsSignature = (rows: TableRow[]): string =>
  rows.map((row) => JSON.stringify(row)).join('|');

const parseLiteral = (rawValue: string): ScalarValue => {
  const value = rawValue.trim();
  if (!value) {
    return '';
  }

  const quoted = value.match(/^["'](.*)["']$/);
  if (quoted) {
    return quoted[1];
  }

  if (/^-?\d+(\.\d+)?$/.test(value)) {
    return Number(value);
  }

  if (/^true$/i.test(value)) {
    return true;
  }

  if (/^false$/i.test(value)) {
    return false;
  }

  if (/^null$/i.test(value)) {
    return null;
  }

  return value;
};

const compareValues = (
  left: ScalarValue,
  operator: string,
  right: ScalarValue
): boolean => {
  if (operator === 'contains' || operator === 'startsWith' || operator === 'endsWith') {
    const leftText = String(left ?? '');
    const rightText = String(right ?? '');
    if (operator === 'contains') {
      return leftText.includes(rightText);
    }
    if (operator === 'startsWith') {
      return leftText.startsWith(rightText);
    }
    return leftText.endsWith(rightText);
  }

  if (operator === '=' || operator === '==') {
    return left === right || String(left) === String(right);
  }

  if (operator === '!=') {
    return left !== right && String(left) !== String(right);
  }

  const leftComparable = isNumber(left) && isNumber(right) ? left : String(left ?? '');
  const rightComparable = isNumber(left) && isNumber(right) ? right : String(right ?? '');

  switch (operator) {
    case '>':
      return leftComparable > rightComparable;
    case '>=':
      return leftComparable >= rightComparable;
    case '<':
      return leftComparable < rightComparable;
    case '<=':
      return leftComparable <= rightComparable;
    default:
      throw new Error(`Unsupported operator "${operator}"`);
  }
};

const evaluateCondition = (row: TableRow, condition: string): boolean => {
  const parsed = condition
    .trim()
    .match(/^([A-Za-z_][\w]*)\s*(contains|startsWith|endsWith|==|=|!=|>=|<=|>|<)\s*(.+)$/i);

  if (!parsed) {
    throw new Error(`Could not parse condition "${condition.trim()}"`);
  }

  const [, field, operator, rawLiteral] = parsed;
  return compareValues(row[field] ?? null, operator, parseLiteral(rawLiteral));
};

const applyFilter = (rows: TableRow[], expression: string): TableRow[] => {
  const trimmed = expression.trim();
  if (!trimmed) {
    return rows;
  }

  return rows.filter((row) => {
    const parts = trimmed.split(/\s+(AND|OR)\s+/i);
    let result = evaluateCondition(row, parts[0]);

    for (let index = 1; index < parts.length; index += 2) {
      const connector = parts[index].toUpperCase();
      const nextResult = evaluateCondition(row, parts[index + 1]);
      result = connector === 'AND' ? result && nextResult : result || nextResult;
    }

    return result;
  });
};

const compareForSort = (left: ScalarValue, right: ScalarValue): number => {
  if (left === right) {
    return 0;
  }
  if (left === null || left === undefined) {
    return 1;
  }
  if (right === null || right === undefined) {
    return -1;
  }
  if (isNumber(left) && isNumber(right)) {
    return left - right;
  }
  return String(left).localeCompare(String(right));
};

const applySort = (rows: TableRow[], expression: string): TableRow[] => {
  const sortParts = expression
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
      return { field, descending: normalizedDirection === 'desc' };
    });

  if (sortParts.length === 0) {
    return rows;
  }

  return [...rows].sort((left, right) => {
    for (const sortPart of sortParts) {
      const comparison = compareForSort(left[sortPart.field] ?? null, right[sortPart.field] ?? null);
      if (comparison !== 0) {
        return sortPart.descending ? -comparison : comparison;
      }
    }
    return 0;
  });
};

const parseGroupExpression = (expression: string): {
  groupField: string;
  specs: AggregateSpec[];
} => {
  const [rawGroupField, rawAggs = 'count=count()'] = expression.split('|').map((part) => part.trim());
  if (!rawGroupField || !/^[A-Za-z_][\w]*$/.test(rawGroupField)) {
    throw new Error('Group expression must start with a column name');
  }

  const specs = rawAggs
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const parsed = part.match(/^([A-Za-z_][\w]*)\s*=\s*(sum|avg|min|max|count)\(([^)]*)\)$/i);
      if (!parsed) {
        throw new Error(`Could not parse aggregate "${part}"`);
      }

      const [, alias, rawOp, rawField] = parsed;
      const op = rawOp.toLowerCase() as AggregateSpec['op'];
      const field = rawField.trim();
      if (op !== 'count' && !/^[A-Za-z_][\w]*$/.test(field)) {
        throw new Error(`${op}() needs a column name`);
      }
      if (op === 'count' && field && field !== '*' && !/^[A-Za-z_][\w]*$/.test(field)) {
        throw new Error('count() must be empty, *, or a column name');
      }

      return {
        alias,
        op,
        field: field && field !== '*' ? field : null,
      };
    });

  return {
    groupField: rawGroupField,
    specs: specs.length > 0 ? specs : [{ alias: 'count', op: 'count', field: null }],
  };
};

const applyGroup = (rows: TableRow[], expression: string): {
  rows: TableRow[];
  columns: string[];
} => {
  const { groupField, specs } = parseGroupExpression(expression);
  const groups = new Map<string, GroupAccumulator>();

  for (const sourceRow of rows) {
    const keyValue = sourceRow[groupField] ?? '-';
    const key = String(keyValue);
    const group = groups.get(key) ?? {
      row: { [groupField]: keyValue },
      count: 0,
      values: {},
    };

    group.count += 1;
    for (const spec of specs) {
      if (spec.op === 'count') {
        if (spec.field && sourceRow[spec.field] !== null && sourceRow[spec.field] !== undefined) {
          group.values[spec.alias] = [...(group.values[spec.alias] ?? []), 1];
        }
        continue;
      }

      if (!spec.field) {
        continue;
      }

      const value = sourceRow[spec.field];
      if (isNumber(value)) {
        group.values[spec.alias] = [...(group.values[spec.alias] ?? []), value];
      }
    }

    groups.set(key, group);
  }

  const resultRows = Array.from(groups.values()).map((group) => {
    const output: TableRow = { ...group.row };
    for (const spec of specs) {
      if (spec.op === 'count') {
        output[spec.alias] = spec.field
          ? group.values[spec.alias]?.length ?? 0
          : group.count;
        continue;
      }

      const values = group.values[spec.alias] ?? [];
      if (values.length === 0) {
        output[spec.alias] = 0;
        continue;
      }

      if (spec.op === 'sum') {
        output[spec.alias] = values.reduce((sum, value) => sum + value, 0);
      } else if (spec.op === 'avg') {
        output[spec.alias] = values.reduce((sum, value) => sum + value, 0) / values.length;
      } else if (spec.op === 'min') {
        output[spec.alias] = Math.min(...values);
      } else {
        output[spec.alias] = Math.max(...values);
      }
    }
    return output;
  });

  resultRows.sort((left, right) => String(left[groupField]).localeCompare(String(right[groupField])));

  return {
    rows: resultRows,
    columns: [groupField, ...specs.map((spec) => spec.alias)],
  };
};

const evaluateView = (
  definition: ViewDefinition,
  source: EvaluatedNode
): Pick<EvaluatedNode, 'rows' | 'columns' | 'error'> => {
  try {
    if (definition.kind === 'filter') {
      return {
        rows: applyFilter(source.rows, definition.expression),
        columns: source.columns,
      };
    }

    if (definition.kind === 'sort') {
      return {
        rows: applySort(source.rows, definition.expression),
        columns: source.columns,
      };
    }

    return applyGroup(source.rows, definition.expression);
  } catch (error) {
    return {
      rows: [],
      columns: source.columns,
      error: error instanceof Error ? error.message : 'Expression failed',
    };
  }
};

const evaluatePipeline = (
  baseRows: TableRow[],
  baseColumns: string[],
  definitions: ViewDefinition[]
): EvaluatedNode[] => {
  const nodesById = new Map<string, EvaluatedNode>();
  const baseNode: EvaluatedNode = {
    id: 'base',
    title: 'Base Sales',
    kind: 'base',
    rows: baseRows,
    columns: baseColumns,
    tickKey: rowsSignature(baseRows),
  };

  nodesById.set(baseNode.id, baseNode);
  const nodes = [baseNode];

  for (const definition of definitions) {
    const source = nodesById.get(definition.sourceId) ?? baseNode;
    const evaluated = evaluateView(definition, source);
    const node: EvaluatedNode = {
      id: definition.id,
      title: definition.title,
      kind: definition.kind,
      sourceId: source.id,
      sourceTitle: source.title,
      expression: definition.expression,
      rows: evaluated.rows,
      columns: evaluated.columns,
      error: evaluated.error,
      tickKey: [
        source.tickKey,
        definition.kind,
        definition.expression,
        evaluated.error ?? 'ok',
      ].join('::'),
    };
    nodesById.set(node.id, node);
    nodes.push(node);
  }

  return nodes;
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
  maxRows = 8,
}: {
  columns: string[];
  rows: TableRow[];
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
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((row, rowIndex) => (
            <tr key={`${rowIndex}-${JSON.stringify(row)}`} className="border-b border-gray-100">
              {columns.map((column) => (
                <td key={column} className="truncate px-3 py-2 text-gray-800">
                  {formatCellValue(column, row[column] ?? null)}
                </td>
              ))}
            </tr>
          ))}
          {visibleRows.length === 0 && (
            <tr>
              <td colSpan={columns.length} className="px-3 py-8 text-center text-gray-400">
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
}: {
  node: EvaluatedNode;
  definition?: ViewDefinition;
  tickCount: number;
  isActive: boolean;
  onExpressionChange?: (expression: string) => void;
  onReset?: () => void;
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

      <DataPreview columns={node.columns} rows={node.rows} />
    </section>
  );
}

export function CascadeDemo({ onBack }: CascadeDemoProps) {
  const {
    data: records,
    columns: serverColumns,
    connected,
    insertRow,
  } = useTableWebSocket('demo');
  const [definitions, setDefinitions] = useState<ViewDefinition[]>(DEFAULT_DEFINITIONS);
  const [autoStream, setAutoStream] = useState(false);
  const [tickCounts, setTickCounts] = useState<Record<string, number>>({});
  const [activeTicks, setActiveTicks] = useState<Set<string>>(new Set());
  const lastTickKeysRef = useRef<Record<string, string>>({});

  const baseRows = useMemo(() => records.map((record) => record.values), [records]);
  const baseColumns = useMemo(
    () => (serverColumns.length > 0 ? serverColumns : inferColumns(baseRows)),
    [baseRows, serverColumns]
  );

  const nodes = useMemo(
    () => evaluatePipeline(baseRows, baseColumns, definitions),
    [baseColumns, baseRows, definitions]
  );
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
      .filter((node) => lastTickKeysRef.current[node.id] !== node.tickKey)
      .map((node) => node.id);

    if (changedIds.length === 0) {
      return undefined;
    }

    lastTickKeysRef.current = Object.fromEntries(nodes.map((node) => [node.id, node.tickKey]));
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
              />
            );
          })}
        </div>
      </main>
    </div>
  );
}
