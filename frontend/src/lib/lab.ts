import type { LabAction, PipelineSnapshot, ViewNodeSpec } from '../types';

export const NODE_LABELS: Record<string, string> = {
  base: 'All orders', 'high-value': 'High-value orders', ranked: 'Ranked orders', regions: 'Regional totals',
};

export function labPipeline(threshold: number): ViewNodeSpec[] {
  return [
    { id: 'high-value', source_id: 'base', kind: 'filter', predicate: `amount >= ${threshold}` },
    { id: 'ranked', source_id: 'high-value', kind: 'sort', keys: [{ column: 'amount', descending: true }] },
    { id: 'regions', source_id: 'high-value', kind: 'group', group_by: ['region'], aggs: [
      { alias: 'total', op: 'sum', column: 'amount' },
      { alias: 'orders', op: 'count', column: 'amount' },
      { alias: 'average', op: 'avg', column: 'amount' },
    ] },
  ];
}

export const SCENARIOS = [
  { id: 'excluded', number: '01', title: 'Change less. Send less.', tag: 'Selective propagation',
    description: 'Edit an order below your threshold. The base changes, but the filter and ranked view need no delivery.',
    action: 'Run excluded edit', expected: 'Base delta · no filter or sort delivery · group may snapshot' },
  { id: 'crossing', number: '02', title: 'Cross the threshold.', tag: 'Incremental membership',
    description: 'Promote a small order into the high-value queue. Follow its insertion into the filter and ranked branch.',
    action: 'Promote an order', expected: 'Base update · filter insertion · ranked insertion · group snapshot' },
  { id: 'ranked', number: '03', title: 'Make a move.', tag: 'Incremental ordering',
    description: 'Raise a qualifying order to the top. The ranked view delivers a delete and an insert, not the entire table.',
    action: 'Move an order to #1', expected: 'Base update · filter update · sorted move · group snapshot' },
  { id: 'clients', number: '04', title: 'One source. Two perspectives.', tag: 'Independent clients',
    description: 'Open a second client with a different threshold. Run a mixed batch here and watch both independent pipelines respond.',
    action: 'Run shared batch', expected: 'Shared source data · connection-local views and delivery sequences' },
  { id: 'recovery', number: '05', title: 'Miss a message. Catch up.', tag: 'Snapshot recovery',
    description: 'Discard one incoming filter delta in this client. The next server watermark detects the gap and triggers a repair snapshot.',
    action: 'Drop a delta & recover', expected: 'Dropped delta → watermark → QueryView → coherent snapshot' },
] as const;
export type ScenarioId = typeof SCENARIOS[number]['id'];

export function scenarioAction(id: ScenarioId, base: PipelineSnapshot, threshold: number, ranked?: PipelineSnapshot): LabAction {
  if (id === 'clients') return { kind: 'step' };
  const excluded = id === 'excluded' || id === 'crossing';
  // Guided commands inspect only as far as the first suitable row, not a sort
  // or full-table recomputation. Default seeds guarantee both memberships.
  const record = base.rows.find(({ row }) => excluded ? Number(row.amount) < threshold : Number(row.amount) >= threshold && Number(row.order) !== Number(ranked?.rows[0]?.row.order));
  if (!record || record.row_id === null) throw new Error('No suitable order. Reset the dataset and try again.');
  const current = Number(record.row.amount);
  const amount = id === 'excluded' ? (current === 120 ? 240 : 120)
    : id === 'crossing' ? threshold + 250
    : id === 'ranked' ? Math.min(1_000_000, Number(ranked?.rows[0]?.row.amount ?? 5000) + 100)
    : current + 1;
  if (amount === current || (id === 'ranked' && amount <= Number(ranked?.rows[0]?.row.amount))) {
    throw new Error('Scenario range exhausted. Reset the dataset to repeat it.');
  }
  return { kind: 'update', row_id: record.row_id, amount };
}

export function formatBytes(bytes: number) {
  return bytes < 1000 ? `${bytes} B` : bytes < 1_000_000 ? `${(bytes / 1000).toFixed(1)} KB` : `${(bytes / 1_000_000).toFixed(2)} MB`;
}

export function formatCell(column: string, value: unknown): string {
  if (value === null || value === undefined) return '—';
  if (column === 'order') return `ORD-${String(value).padStart(5, '0')}`;
  if (['amount', 'total', 'average'].includes(column) && typeof value === 'number') {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(value);
  }
  return typeof value === 'number' ? value.toLocaleString('en-US') : String(value);
}
