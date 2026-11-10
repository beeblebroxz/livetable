import { describe, expect, it } from 'vitest';
import { BURST_SIZES, formatBytes, formatMicros, formatShare, labPipeline, SCENARIOS, scenarioAction } from './lab';
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
  it('runs the burst scenario at the largest size the server accepts, without choosing a row', () => {
    expect(SCENARIOS[SCENARIOS.length - 1].id).toBe('burst');
    expect(scenarioAction('burst', { ...base, rows: [] }, 1000)).toEqual({ kind: 'burst', size: 256 });
    expect(Math.max(...BURST_SIZES)).toBe(256);
  });
  it('formats server microseconds and a sync time beside its rebuild', () => {
    expect(formatMicros(2.64)).toBe('2.6 µs');
    expect(formatMicros(806.8)).toBe('807 µs');
    expect(formatMicros(4286.9)).toBe('4.29 ms');
    expect(formatMicros(30877)).toBe('30.9 ms');
    expect(formatShare(0.0956)).toBe('9.6% of a rebuild');
    expect(formatShare(0.32)).toBe('32% of a rebuild');
    expect(formatShare(3.17)).toBe('3.2× a rebuild'); // a small table: the rebuild was cheaper
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
