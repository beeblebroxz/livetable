import type { TableRecord, TableRow } from '../types';

const getNextNumericId = (records: TableRecord[]): number | null => {
  if (records.length === 0) {
    return 1;
  }

  let maxId = Number.NEGATIVE_INFINITY;
  for (const record of records) {
    const currentId = record.values.id;
    if (typeof currentId !== 'number' || !Number.isFinite(currentId)) {
      return null;
    }
    maxId = Math.max(maxId, currentId);
  }

  return maxId + 1;
};

export const buildDraftRow = (
  columnNames: string[],
  records: TableRecord[]
): TableRow => {
  const draftRow: TableRow = {};
  const templateRow = records[0]?.values;
  const nextNumericId = columnNames.includes('id') ? getNextNumericId(records) : null;

  columnNames.forEach((columnName) => {
    if (columnName === 'id' && nextNumericId !== null) {
      draftRow[columnName] = nextNumericId;
    } else if (columnName === 'name') {
      draftRow[columnName] = `New Item ${records.length + 1}`;
    } else if (templateRow && typeof templateRow[columnName] === 'number') {
      draftRow[columnName] = 0;
    } else if (templateRow && typeof templateRow[columnName] === 'boolean') {
      draftRow[columnName] = false;
    } else {
      draftRow[columnName] = '';
    }
  });

  return draftRow;
};
