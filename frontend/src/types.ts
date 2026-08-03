export type ScalarValue = string | number | boolean | null;

export type TableRow = Record<string, ScalarValue>;

export interface TableRecord {
  rowId: number;
  values: TableRow;
}

export interface WireTableRecord {
  row_id: number;
  row: TableRow;
}

export interface WireViewRecord {
  row_id: number | null;
  row: TableRow;
}

export interface PipelineSnapshot {
  generation: number;
  nodeId: string;
  sourceId: string;
  kind: 'base' | 'filter' | 'sort' | 'group';
  seq: number;
  columns: string[];
  rows: WireViewRecord[];
}

export interface SortKeySpec {
  column: string;
  descending: boolean;
}

export interface AggSpec {
  alias: string;
  op: string;
  column: string;
}

export type ViewNodeSpec = {
  id: string;
  source_id: string;
} & (
  | { kind: 'filter'; predicate: string }
  | { kind: 'sort'; keys: SortKeySpec[] }
  | { kind: 'group'; group_by: string[]; aggs: AggSpec[] }
);

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
  | { type: 'UpdateCell'; table_name: string; row_id: number; column: string; value: ScalarValue }
  | { type: 'DeleteRow'; table_name: string; row_id: number }
  | {
      type: 'SetPipeline';
      table_name: string;
      pipeline_generation: number;
      nodes: ViewNodeSpec[];
    };

// `seq` is the server's monotonic change count. `TableData` reports the count
// its snapshot was taken at; each delta reports the count after it was applied.
// Clients drop any delta whose `seq` is <= the snapshot's `seq` (already
// reflected) and apply the rest. See ServerMessage in impl/src/messages.rs.
export type ServerMessage =
  | { type: 'Subscribed'; table_name: string; protocol_version?: number }
  | { type: 'TableData'; table_name: string; seq: number; columns: string[]; rows: WireTableRecord[] }
  | { type: 'RowInserted'; table_name: string; seq: number; index: number; row_id: number; row: TableRow }
  | { type: 'CellUpdated'; table_name: string; seq: number; row_id: number; column: string; value: ScalarValue }
  | { type: 'RowDeleted'; table_name: string; seq: number; row_id: number }
  | {
      type: 'ViewData';
      table_name: string;
      pipeline_generation: number;
      node_id: string;
      source_id: string;
      kind: 'base' | 'filter' | 'sort' | 'group';
      seq: number;
      columns: string[];
      rows: WireViewRecord[];
    }
  | {
      type: 'ViewError';
      table_name: string;
      pipeline_generation: number;
      node_id: string;
      message: string;
    }
  | { type: 'Error'; message: string };
