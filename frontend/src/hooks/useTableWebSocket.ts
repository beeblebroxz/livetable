import { useCallback, useEffect, useRef, useState } from 'react';

export interface TableRow {
  [key: string]: string | number | boolean | null;
}

export interface ServerMessage {
  type: string;
  [key: string]: any;
}

const isDev = import.meta.env.DEV;

const logDebug = (...args: unknown[]) => {
  if (isDev) {
    console.log(...args);
  }
};

const applyServerMessage = (
  prev: TableRow[],
  message: ServerMessage
): TableRow[] => {
  switch (message.type) {
    case 'TableData':
      return message.rows || [];
    case 'RowInserted': {
      const next = prev.slice();
      const insertIndex =
        typeof message.index === 'number' ? message.index : next.length;
      next.splice(insertIndex, 0, message.row);
      return next;
    }
    case 'CellUpdated': {
      if (
        typeof message.row_index !== 'number' ||
        message.row_index < 0 ||
        message.row_index >= prev.length
      ) {
        return prev;
      }

      const next = prev.slice();
      next[message.row_index] = {
        ...next[message.row_index],
        [message.column]: message.value,
      };
      return next;
    }
    case 'RowDeleted': {
      if (
        typeof message.index !== 'number' ||
        message.index < 0 ||
        message.index >= prev.length
      ) {
        return prev;
      }

      const next = prev.slice();
      next.splice(message.index, 1);
      return next;
    }
    default:
      return prev;
  }
};

export function useTableWebSocket(
  tableName: string,
  wsUrl: string = 'ws://localhost:8080/ws'
) {
  const [data, setData] = useState<TableRow[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    const socket = new WebSocket(wsUrl);
    wsRef.current = socket;

    socket.onopen = () => {
      logDebug('WebSocket connected');
      setConnected(true);

      socket.send(JSON.stringify({ type: 'Subscribe', table_name: tableName }));
      socket.send(JSON.stringify({ type: 'Query', table_name: tableName }));
    };

    socket.onmessage = (event) => {
      const message: ServerMessage = JSON.parse(event.data);
      logDebug('Received:', message);

      switch (message.type) {
        case 'TableData':
          setColumns(message.columns || []);
          setData((prev) => applyServerMessage(prev, message));
          break;
        case 'RowInserted':
        case 'CellUpdated':
        case 'RowDeleted':
          setData((prev) => applyServerMessage(prev, message));
          break;
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
    };

    return () => {
      if (wsRef.current === socket) {
        wsRef.current = null;
      }
      socket.close();
    };
  }, [tableName, wsUrl]);

  const insertRow = useCallback((row: TableRow) => {
    wsRef.current?.send(
      JSON.stringify({ type: 'InsertRow', table_name: tableName, row })
    );
  }, [tableName]);

  const updateCell = useCallback((rowIndex: number, column: string, value: unknown) => {
    wsRef.current?.send(
      JSON.stringify({
        type: 'UpdateCell',
        table_name: tableName,
        row_index: rowIndex,
        column,
        value,
      })
    );
  }, [tableName]);

  const deleteRow = useCallback((rowIndex: number) => {
    wsRef.current?.send(
      JSON.stringify({ type: 'DeleteRow', table_name: tableName, row_index: rowIndex })
    );
  }, [tableName]);

  return { data, columns, connected, insertRow, updateCell, deleteRow };
}
