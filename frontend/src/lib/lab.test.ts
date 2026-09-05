import { describe, expect, it } from 'vitest';
import { formatBytes, labPipeline, scenarioAction } from './lab';
import type { PipelineSnapshot } from '../types';

const base: PipelineSnapshot = { generation: 1, nodeId: 'base', sourceId: 'base', kind: 'base', seq: 0, columns: ['order', 'amount'], rows: [
  { row_id: 1, row: { order: 10001, amount: 240 } }, { row_id: 2, row: { order: 10002, amount: 1480 } },
  { row_id: 3, row: { order: 10003, amount: 4900 } },
] };
const ranked = { ...base, rows: [base.rows[2], base.rows[1]] };

describe('guided scenarios', () => {
  it('branches groups off the filter, not the sort', () => {
    const nodes = labPipeline(2500);
    expect(nodes[0]).toHaveProperty('predicate', 'amount >= 2500');
    expect(nodes[1].source_id).toBe('high-value');
    expect(nodes[2].source_id).toBe('high-value');
  });
  it('chooses real base IDs and guarantees an excluded edit, crossing and sorted move', () => {
    expect(scenarioAction('excluded', base, 1000, ranked)).toEqual({ kind: 'update', row_id: 1, amount: 120 });
    expect(scenarioAction('crossing', base, 1000, ranked)).toEqual({ kind: 'update', row_id: 1, amount: 1250 });
    expect(scenarioAction('ranked', base, 1000, ranked)).toEqual({ kind: 'update', row_id: 2, amount: 5000 });
    expect(scenarioAction('recovery', base, 1000, ranked)).toEqual({ kind: 'update', row_id: 2, amount: 1481 });
    expect(scenarioAction('clients', base, 1000, ranked)).toEqual({ kind: 'step' });
  });
  it('explains when reset is needed instead of inventing a row ID', () => {
    expect(() => scenarioAction('excluded', { ...base, rows: [] }, 1000)).toThrow('Reset');
  });
  it('formats decimal UTF-8 payload byte units', () => {
    expect(formatBytes(731)).toBe('731 B');
    expect(formatBytes(1182)).toBe('1.2 KB');
    expect(formatBytes(16_667_730)).toBe('16.67 MB');
  });
});
