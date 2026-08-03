import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  ClientMessage,
  PipelineSnapshot,
  ScalarValue,
  TableRow,
  ViewNodeSpec,
} from '../types';
import {
  getDefaultWebSocketUrl,
  parseServerMessage,
  SUPPORTED_PROTOCOL_VERSION,
} from './useTableWebSocket';

export const PIPELINE_DEBOUNCE_MS = 250;

const sendMessage = (socket: WebSocket, message: ClientMessage) => {
  socket.send(JSON.stringify(message));
};

export function usePipeline(
  tableName: string,
  nodes: ViewNodeSpec[],
  wsUrl: string = getDefaultWebSocketUrl()
) {
  const [snapshots, setSnapshots] = useState<Record<string, PipelineSnapshot>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [connected, setConnected] = useState(false);
  const [generation, setGeneration] = useState(0);
  const socketRef = useRef<WebSocket | null>(null);
  const nodesRef = useRef(nodes);
  const generationRef = useRef(0);
  const reconnectTimerRef = useRef<number | null>(null);
  const pipelineTimerRef = useRef<number | null>(null);

  nodesRef.current = nodes;

  const sendPipeline = useCallback((socket: WebSocket) => {
    if (socket.readyState !== WebSocket.OPEN) {
      return;
    }
    generationRef.current += 1;
    const nextGeneration = generationRef.current;
    setGeneration(nextGeneration);
    setErrors({});
    sendMessage(socket, {
      type: 'SetPipeline',
      table_name: tableName,
      pipeline_generation: nextGeneration,
      nodes: nodesRef.current,
    });
  }, [tableName]);

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
      socketRef.current = socket;

      socket.onopen = () => {
        reconnectAttempts = 0;
        setConnected(true);
        sendMessage(socket, { type: 'Subscribe', table_name: tableName });
        sendPipeline(socket);
      };

      socket.onmessage = (event) => {
        const message = parseServerMessage(event.data);
        if (!message) {
          console.error('Invalid server message payload:', event.data);
          return;
        }

        switch (message.type) {
          case 'Subscribed':
            if (
              message.protocol_version !== undefined &&
              message.protocol_version !== SUPPORTED_PROTOCOL_VERSION
            ) {
              setErrors((current) => ({
                ...current,
                pipeline:
                  `Server protocol ${message.protocol_version} is incompatible with ` +
                  `client protocol ${SUPPORTED_PROTOCOL_VERSION}`,
              }));
            }
            break;
          case 'ViewData':
            if (message.pipeline_generation !== generationRef.current) {
              return;
            }
            setSnapshots((current) => {
              const previous = current[message.node_id];
              if (
                previous?.generation === message.pipeline_generation &&
                previous.seq >= message.seq
              ) {
                return current;
              }
              return {
                ...current,
                [message.node_id]: {
                  generation: message.pipeline_generation,
                  nodeId: message.node_id,
                  sourceId: message.source_id,
                  kind: message.kind,
                  seq: message.seq,
                  columns: message.columns,
                  rows: message.rows,
                },
              };
            });
            setErrors((current) => {
              if (!(message.node_id in current)) {
                return current;
              }
              const next = { ...current };
              delete next[message.node_id];
              return next;
            });
            break;
          case 'ViewError':
            if (message.pipeline_generation === generationRef.current) {
              setErrors((current) => ({
                ...current,
                [message.node_id]: message.message,
              }));
            }
            break;
          case 'Error':
            setErrors((current) => ({ ...current, pipeline: message.message }));
            break;
          default:
            break;
        }
      };

      socket.onerror = () => {
        setErrors((current) => ({ ...current, connection: 'WebSocket error' }));
      };
      socket.onclose = () => {
        if (socketRef.current === socket) {
          socketRef.current = null;
        }
        setConnected(false);
        if (disposed) {
          return;
        }
        const delay = Math.min(250 * 2 ** reconnectAttempts, 2000);
        reconnectAttempts += 1;
        reconnectTimerRef.current = window.setTimeout(connect, delay);
      };
    };

    connect();
    return () => {
      disposed = true;
      clearReconnectTimer();
      const socket = socketRef.current;
      socketRef.current = null;
      socket?.close();
    };
  }, [sendPipeline, tableName, wsUrl]);

  useEffect(() => {
    if (pipelineTimerRef.current !== null) {
      window.clearTimeout(pipelineTimerRef.current);
    }
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return;
    }
    pipelineTimerRef.current = window.setTimeout(() => {
      pipelineTimerRef.current = null;
      if (socketRef.current === socket) {
        sendPipeline(socket);
      }
    }, PIPELINE_DEBOUNCE_MS);

    return () => {
      if (pipelineTimerRef.current !== null) {
        window.clearTimeout(pipelineTimerRef.current);
        pipelineTimerRef.current = null;
      }
    };
  }, [nodes, sendPipeline]);

  const send = useCallback((message: ClientMessage) => {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    sendMessage(socket, message);
    return true;
  }, []);

  const insertRow = useCallback((row: TableRow) => {
    return send({ type: 'InsertRow', table_name: tableName, row });
  }, [send, tableName]);

  const updateCell = useCallback((rowId: number, column: string, value: ScalarValue) => {
    return send({
      type: 'UpdateCell',
      table_name: tableName,
      row_id: rowId,
      column,
      value,
    });
  }, [send, tableName]);

  const deleteRow = useCallback((rowId: number) => {
    return send({ type: 'DeleteRow', table_name: tableName, row_id: rowId });
  }, [send, tableName]);

  return {
    connected,
    generation,
    snapshots,
    errors,
    insertRow,
    updateCell,
    deleteRow,
  };
}
