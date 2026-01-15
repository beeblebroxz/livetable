export type ScalarValue = string | number | boolean | null;

export type TableRow = Record<string, ScalarValue>;

export type ConnectionState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'closed'
  | 'error';

export type ClientMessage =
  | { type: 'Subscribe'; table_name: string }
  | { type: 'Query'; table_name: string }
  | { type: 'InsertRow'; table_name: string; row: TableRow }
  | { type: 'UpdateCell'; table_name: string; row_index: number; column: string; value: ScalarValue }
  | { type: 'DeleteRow'; table_name: string; row_index: number };

export type ServerMessage =
  | { type: 'Subscribed'; table_name: string }
  | { type: 'TableData'; table_name: string; columns: string[]; rows: TableRow[] }
  | { type: 'RowInserted'; table_name: string; index: number; row: TableRow }
  | { type: 'CellUpdated'; table_name: string; row_index: number; column: string; value: ScalarValue }
  | { type: 'RowDeleted'; table_name: string; index: number }
  | { type: 'Error'; message: string };
