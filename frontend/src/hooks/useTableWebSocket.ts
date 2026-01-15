import { useEffect, useState } from 'react';

export interface TableRow {
  [key: string]: string | number | boolean | null;
}

export interface ServerMessage {
  type: string;
  [key: string]: any;
}

export function useTableWebSocket(
  tableName: string,
  wsUrl: string = 'ws://localhost:8080/ws'
) {
  const [data, setData] = useState<TableRow[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [ws, setWs] = useState<WebSocket | null>(null);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const socket = new WebSocket(wsUrl);

    socket.onopen = () => {
      console.log('WebSocket connected');
      setConnected(true);

      socket.send(JSON.stringify({ type: 'Subscribe', table_name: tableName }));
      socket.send(JSON.stringify({ type: 'Query', table_name: tableName }));
    };

    socket.onmessage = (event) => {
      const message: ServerMessage = JSON.parse(event.data);
      console.log('Received:', message);

      switch (message.type) {
        case 'TableData':
          setColumns(message.columns || []);
          setData(message.rows || []);
          break;
        case 'RowInserted':
          setData((prev) => [...prev, message.row]);
          break;
        case 'CellUpdated':
          setData((prev) =>
            prev.map((row, idx) =>
              idx === message.row_index
                ? { ...row, [message.column]: message.value }
                : row
            )
          );
          break;
        case 'RowDeleted':
          setData((prev) => prev.filter((_, idx) => idx !== message.index));
          break;
        case 'Subscribed':
          console.log('Subscribed to', message.table_name);
          break;
        case 'Error':
          console.error('Server error:', message.message);
          break;
      }
    };

    socket.onerror = () => console.error('WebSocket error');
    socket.onclose = () => { console.log('Disconnected'); setConnected(false); };

    setWs(socket);
    return () => socket.close();
  }, [tableName, wsUrl]);

  const insertRow = (row: TableRow) => {
    ws?.send(JSON.stringify({ type: 'InsertRow', table_name: tableName, row }));
  };

  const updateCell = (rowIndex: number, column: string, value: any) => {
    ws?.send(JSON.stringify({ type: 'UpdateCell', table_name: tableName, row_index: rowIndex, column, value }));
  };

  const deleteRow = (rowIndex: number) => {
    ws?.send(JSON.stringify({ type: 'DeleteRow', table_name: tableName, row_index: rowIndex }));
  };

  return { data, columns, connected, insertRow, updateCell, deleteRow };
}
