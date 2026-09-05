import type { PipelineSnapshot, ServerMessage, WireViewRecord } from '../types';

type ViewDelta = Extract<ServerMessage, { type: 'ViewDelta' }>;
type ViewData = Extract<ServerMessage, { type: 'ViewData' }>;

const validRow = (record: WireViewRecord, columns: string[], kind: string) =>
  (kind === 'base' ? record.row_id !== null : record.row_id === null) &&
  Object.keys(record.row).length === columns.length &&
  columns.every((column) => Object.prototype.hasOwnProperty.call(record.row, column));

export function isValidViewSnapshot(message: ViewData): boolean {
  if (new Set(message.columns).size !== message.columns.length ||
      !message.rows.every((row) => validRow(row, message.columns, message.kind))) return false;
  return message.kind !== 'base' || new Set(message.rows.map((row) => row.row_id)).size === message.rows.length;
}

// No in-place mutations: a bad operation halfway through a batch must not
// partially change the visible baseline. Row coordinates are step-local.
export function applyViewDelta(previous: PipelineSnapshot, delta: ViewDelta): PipelineSnapshot | null {
  if (previous.generation !== delta.pipeline_generation || previous.nodeId !== delta.node_id ||
      previous.seq !== delta.from_seq || previous.kind === 'group') return null;
  const rows = previous.rows.slice();
  for (const change of delta.changes) {
    if (change.type === 'RowInserted') {
      if (change.index > rows.length || !validRow(change.row, previous.columns, previous.kind)) return null;
      rows.splice(change.index, 0, change.row);
    } else {
      if (change.index >= rows.length) return null;
      if (change.type === 'RowDeleted') {
        rows.splice(change.index, 1);
      } else {
        if (!previous.columns.includes(change.column)) return null;
        const previousRow = rows[change.index];
        rows[change.index] = {
          ...previousRow,
          row: { ...previousRow.row, [change.column]: change.value },
        };
      }
    }
  }
  return { ...previous, seq: delta.seq, rows };
}
