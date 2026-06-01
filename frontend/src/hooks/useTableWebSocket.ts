import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  ClientMessage,
  ScalarValue,
  ServerMessage,
  TableRecord,
  TableRow,
  WireTableRecord,
} from '../types';

type DeltaMessage = Extract<
  ServerMessage,
  { type: 'RowInserted' | 'CellUpdated' | 'RowDeleted' }
>;

const isDev = import.meta.env.DEV;
const configuredWsUrl = import.meta.env.VITE_LIVETABLE_WS_URL;

const getDefaultWebSocketUrl = () => {
  if (configuredWsUrl) {
    return configuredWsUrl;
  }

  if (typeof window === 'undefined') {
    return 'ws://localhost:8080/ws';
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const hostname = window.location.hostname || 'localhost';
  return `${protocol}//${hostname}:8080/ws`;
};

const logDebug = (...args: unknown[]) => {
  if (isDev) {
    console.log(...args);
  }
};

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isScalarValue = (value: unknown): value is ScalarValue =>
  value === null ||
  typeof value === 'string' ||
  typeof value === 'number' ||
  typeof value === 'boolean';

const isSeq = (value: unknown): value is number =>
  typeof value === 'number' && Number.isInteger(value) && value >= 0;

const isTableRow = (value: unknown): value is TableRow =>
  isObject(value) && Object.values(value).every(isScalarValue);

const isWireTableRecord = (value: unknown): value is WireTableRecord =>
  isObject(value) &&
  typeof value.row_id === 'number' &&
  Number.isInteger(value.row_id) &&
  value.row_id >= 0 &&
  isTableRow(value.row);

const toTableRecord = (record: WireTableRecord): TableRecord => ({
  rowId: record.row_id,
  values: record.row,
});

const parseServerMessage = (payload: unknown): ServerMessage | null => {
  if (typeof payload !== 'string') {
    return null;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return null;
  }

  if (!isObject(parsed) || typeof parsed.type !== 'string') {
    return null;
  }

  switch (parsed.type) {
    case 'Subscribed':
      return typeof parsed.table_name === 'string' ? parsed as ServerMessage : null;
    case 'TableData':
      return typeof parsed.table_name === 'string' &&
        isSeq(parsed.seq) &&
        Array.isArray(parsed.columns) &&
        parsed.columns.every((column) => typeof column === 'string') &&
        Array.isArray(parsed.rows) &&
        parsed.rows.every(isWireTableRecord)
        ? parsed as ServerMessage
        : null;
    case 'RowInserted':
      return typeof parsed.table_name === 'string' &&
        isSeq(parsed.seq) &&
        typeof parsed.index === 'number' &&
        Number.isInteger(parsed.index) &&
        parsed.index >= 0 &&
        typeof parsed.row_id === 'number' &&
        Number.isInteger(parsed.row_id) &&
        parsed.row_id >= 0 &&
        isTableRow(parsed.row)
        ? parsed as ServerMessage
        : null;
    case 'CellUpdated':
      return typeof parsed.table_name === 'string' &&
        isSeq(parsed.seq) &&
        typeof parsed.row_id === 'number' &&
        Number.isInteger(parsed.row_id) &&
        parsed.row_id >= 0 &&
        typeof parsed.column === 'string' &&
        isScalarValue(parsed.value)
        ? parsed as ServerMessage
        : null;
    case 'RowDeleted':
      return typeof parsed.table_name === 'string' &&
        isSeq(parsed.seq) &&
        typeof parsed.row_id === 'number' &&
        Number.isInteger(parsed.row_id) &&
        parsed.row_id >= 0
        ? parsed as ServerMessage
        : null;
    case 'Error':
      return typeof parsed.message === 'string' ? parsed as ServerMessage : null;
    default:
      return null;
  }
};

const sendSocketMessage = (socket: WebSocket, message: ClientMessage) => {
  socket.send(JSON.stringify(message));
};

const applyServerMessage = (
  previousRows: TableRecord[],
  message: ServerMessage
): TableRecord[] => {
  switch (message.type) {
    case 'TableData':
      return message.rows.map(toTableRecord);
    case 'RowInserted': {
      const nextRows = previousRows.slice();
      const insertIndex =
        message.index >= 0 && message.index <= nextRows.length
          ? message.index
          : nextRows.length;
      nextRows.splice(insertIndex, 0, {
        rowId: message.row_id,
        values: message.row,
      });
      return nextRows;
    }
    case 'CellUpdated': {
      const rowIndex = previousRows.findIndex(
        (row) => row.rowId === message.row_id
      );
      if (rowIndex === -1) {
        return previousRows;
      }

      const nextRows = previousRows.slice();
      nextRows[rowIndex] = {
        ...nextRows[rowIndex],
        values: {
          ...nextRows[rowIndex].values,
          [message.column]: message.value,
        },
      };
      return nextRows;
    }
    case 'RowDeleted':
      return previousRows.filter((row) => row.rowId !== message.row_id);
    default:
      return previousRows;
  }
};

const applyBufferedDeltasInSequence = (
  previousRows: TableRecord[],
  deltas: DeltaMessage[],
  lastAppliedSeq: number
): {
  rows: TableRecord[];
  pendingDeltas: DeltaMessage[];
  lastAppliedSeq: number;
} => {
  let rows = previousRows;
  let appliedSeq = lastAppliedSeq;
  const pendingDeltas: DeltaMessage[] = [];

  const sortedDeltas = [...deltas].sort((a, b) => a.seq - b.seq);
  for (const delta of sortedDeltas) {
    if (delta.seq <= appliedSeq) {
      continue;
    }

    if (delta.seq === appliedSeq + 1) {
      rows = applyServerMessage(rows, delta);
      appliedSeq = delta.seq;
    } else {
      pendingDeltas.push(delta);
    }
  }

  return { rows, pendingDeltas, lastAppliedSeq: appliedSeq };
};

export function useTableWebSocket(
  tableName: string,
  wsUrl: string = getDefaultWebSocketUrl()
) {
  const [data, setData] = useState<TableRecord[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const dataRef = useRef<TableRecord[]>([]);
  // Last server sequence reflected in `data` (null until first TableData on
  // this connection), plus deltas that arrived before a contiguous baseline.
  const lastAppliedSeqRef = useRef<number | null>(null);
  const pendingDeltasRef = useRef<DeltaMessage[]>([]);

  useEffect(() => {
    let disposed = false;
    let reconnectAttempts = 0;

    const clearReconnectTimer = () => {
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const connect = () => {
      if (disposed) {
        return;
      }

      clearReconnectTimer();
      const socket = new WebSocket(wsUrl);
      wsRef.current = socket;

      socket.onopen = () => {
        logDebug('WebSocket connected');
        reconnectAttempts = 0;
        setConnected(true);

        // A fresh connection means a fresh snapshot is on the way; discard the
        // seq cutoff and any buffered deltas from the previous connection.
        lastAppliedSeqRef.current = null;
        pendingDeltasRef.current = [];

        sendSocketMessage(socket, { type: 'Subscribe', table_name: tableName });
        sendSocketMessage(socket, { type: 'Query', table_name: tableName });
      };

      socket.onmessage = (event) => {
        const message = parseServerMessage(event.data);
        if (!message) {
          console.error('Invalid server message payload:', event.data);
          return;
        }

        logDebug('Received:', message);

        switch (message.type) {
          case 'TableData': {
            setColumns(message.columns);
            // Replay deltas that raced ahead of the snapshot, keeping only a
            // contiguous sequence newer than it. Older/duplicate deltas are
            // already reflected in the snapshot and are dropped.
            const buffered = pendingDeltasRef.current;
            pendingDeltasRef.current = [];
            const replay = applyBufferedDeltasInSequence(
              message.rows.map(toTableRecord),
              buffered,
              message.seq
            );
            pendingDeltasRef.current = replay.pendingDeltas;
            lastAppliedSeqRef.current = replay.lastAppliedSeq;
            dataRef.current = replay.rows;
            setData(replay.rows);
            break;
          }
          case 'RowInserted':
          case 'CellUpdated':
          case 'RowDeleted': {
            const lastAppliedSeq = lastAppliedSeqRef.current;
            if (lastAppliedSeq === null) {
              // No baseline yet — buffer until the snapshot defines the cutoff.
              pendingDeltasRef.current.push(message);
            } else {
              pendingDeltasRef.current.push(message);
              const replay = applyBufferedDeltasInSequence(
                dataRef.current,
                pendingDeltasRef.current,
                lastAppliedSeq
              );
              pendingDeltasRef.current = replay.pendingDeltas;
              lastAppliedSeqRef.current = replay.lastAppliedSeq;
              dataRef.current = replay.rows;
              setData(replay.rows);
            }
            break;
          }
          case 'Subscribed':
            logDebug('Subscribed to', message.table_name);
            break;
          case 'Error':
            console.error('Server error:', message.message);
            break;
        }
      };

      socket.onerror = () => console.error('WebSocket error');
      socket.onclose = () => {
        logDebug('Disconnected');
        if (wsRef.current === socket) {
          wsRef.current = null;
        }
        setConnected(false);

        if (disposed) {
          return;
        }

        const reconnectDelay = Math.min(250 * 2 ** reconnectAttempts, 2000);
        reconnectAttempts += 1;
        reconnectTimerRef.current = window.setTimeout(connect, reconnectDelay);
      };
    };

    connect();

    return () => {
      disposed = true;
      clearReconnectTimer();
      const activeSocket = wsRef.current;
      wsRef.current = null;
      activeSocket?.close();
    };
  }, [tableName, wsUrl]);

  const sendMessage = useCallback((message: ClientMessage) => {
    const socket = wsRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }

    sendSocketMessage(socket, message);
    return true;
  }, []);

  const insertRow = useCallback((row: TableRow) => {
    sendMessage({ type: 'InsertRow', table_name: tableName, row });
  }, [sendMessage, tableName]);

  const updateCell = useCallback((
    rowId: number,
    column: string,
    value: ScalarValue
  ) => {
    sendMessage({
      type: 'UpdateCell',
      table_name: tableName,
      row_id: rowId,
      column,
      value,
    });
  }, [sendMessage, tableName]);

  const deleteRow = useCallback((rowId: number) => {
    sendMessage({
      type: 'DeleteRow',
      table_name: tableName,
      row_id: rowId,
    });
  }, [sendMessage, tableName]);

  return { data, columns, connected, insertRow, updateCell, deleteRow };
}
