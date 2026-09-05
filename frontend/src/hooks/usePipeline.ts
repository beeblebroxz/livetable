import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  ClientMessage,
  PipelineSnapshot,
  ScalarValue,
  ServerMessage,
  TableRow,
  ViewNodeSpec,
} from '../types';
import {
  getDefaultWebSocketUrl,
  parseServerMessage,
  SUPPORTED_PROTOCOL_VERSION,
} from './useTableWebSocket';
import { applyViewDelta, isValidViewSnapshot } from '../lib/pipelineReconciliation';

export const PIPELINE_DEBOUNCE_MS = 250;
export const PIPELINE_RESYNC_RETRY_MS = 3000;

export type PipelineEvent =
  | { kind: 'received'; at: number; bytes: number; message: ServerMessage }
  | { kind: 'applied'; at: number; nodeId: string; format: 'snapshot' | 'delta'; seq: number; operations: number }
  | { kind: 'repair' | 'recovered' | 'dropped'; at: number; nodeId: string }
  | { kind: 'generation' | 'disconnected'; at: number };

export interface PipelineOptions {
  onEvent?: (event: PipelineEvent) => void;
  /** Opt-in client-side fault injection; never drops traffic for other clients. */
  allowFaultInjection?: boolean;
}

const sendMessage = (socket: WebSocket, message: ClientMessage) => {
  socket.send(JSON.stringify(message));
};

export function usePipeline(
  tableName: string,
  nodes: ViewNodeSpec[],
  wsUrl: string = getDefaultWebSocketUrl(),
  options: PipelineOptions = {}
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
  const snapshotsRef = useRef<Record<string, PipelineSnapshot>>({});
  const expectedNodesRef = useRef(new Map<string, { sourceId: string; kind: string }>());
  // At most one outstanding repair per installed node; no unbounded delta queue.
  const resyncRef = useRef(new Map<string, number>());
  const optionsRef = useRef(options);
  const droppedNodeRef = useRef<string | null>(null);
  optionsRef.current = options;

  const observe = useCallback((event: PipelineEvent) => {
    // Diagnostics cannot interfere with reconciliation or retain message queues.
    try { optionsRef.current.onEvent?.(event); } catch (error) { console.error('Pipeline observer failed', error); }
  }, []);

  nodesRef.current = nodes;

  const sendPipeline = useCallback((socket: WebSocket) => {
    if (socket.readyState !== WebSocket.OPEN) {
      return;
    }
    generationRef.current += 1;
    const nextGeneration = generationRef.current;
    setGeneration(nextGeneration);
    setErrors({});
    snapshotsRef.current = {};
    setSnapshots({});
    resyncRef.current.clear();
    droppedNodeRef.current = null;
    observe({ kind: 'generation', at: performance.now() });
    expectedNodesRef.current = new Map([
      ['base', { sourceId: 'base', kind: 'base' }],
      ...nodesRef.current.map((node): [string, { sourceId: string; kind: string }] =>
        [node.id, { sourceId: node.source_id, kind: node.kind }]),
    ]);
    sendMessage(socket, {
      type: 'SetPipeline',
      table_name: tableName,
      pipeline_generation: nextGeneration,
      nodes: nodesRef.current,
    });
  }, [observe, tableName]);

  useEffect(() => {
    let disposed = false;
    let reconnectAttempts = 0;
    const pendingResyncs = resyncRef.current;
    // A different table or endpoint cannot borrow the preceding connection's
    // baseline, even while the replacement socket is still connecting.
    snapshotsRef.current = {};
    setSnapshots({});
    setConnected(false);

    const clearReconnectTimer = () => {
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const requestSnapshot = (socket: WebSocket, nodeId: string, retry = false) => {
      if (!expectedNodesRef.current.has(nodeId) || socket.readyState !== WebSocket.OPEN ||
          socketRef.current !== socket || (!retry && resyncRef.current.has(nodeId))) return;
      resyncRef.current.set(nodeId, Date.now());
      observe({ kind: 'repair', nodeId, at: performance.now() });
      sendMessage(socket, {
        type: 'QueryView', table_name: tableName,
        pipeline_generation: generationRef.current, node_id: nodeId,
      });
    };

    const clearNodeError = (nodeId: string) => {
      setErrors((current) => {
        if (!Object.prototype.hasOwnProperty.call(current, nodeId)) return current;
        const next = { ...current };
        delete next[nodeId];
        return next;
      });
    };

    const getSnapshot = (nodeId: string) => Object.prototype.hasOwnProperty.call(snapshotsRef.current, nodeId)
      ? snapshotsRef.current[nodeId] : undefined;

    const retryTimer = window.setInterval(() => {
      const socket = socketRef.current;
      if (!socket) return;
      for (const [nodeId, requestedAt] of resyncRef.current) {
        if (Date.now() - requestedAt >= PIPELINE_RESYNC_RETRY_MS) requestSnapshot(socket, nodeId, true);
      }
    }, PIPELINE_RESYNC_RETRY_MS);

    const connect = () => {
      if (disposed) {
        return;
      }
      clearReconnectTimer();
      const socket = new WebSocket(wsUrl);
      socketRef.current = socket;

      socket.onopen = () => {
        if (disposed || socketRef.current !== socket) return;
        reconnectAttempts = 0;
        setConnected(true);
        sendMessage(socket, { type: 'Subscribe', table_name: tableName });
        sendPipeline(socket);
      };

      socket.onmessage = (event) => {
        if (disposed || socketRef.current !== socket) return;
        const message = parseServerMessage(event.data);
        if (!message) {
          console.error('Invalid server message payload:', event.data);
          return;
        }
        if ('table_name' in message && message.table_name !== tableName) return;
        if ('pipeline_generation' in message && message.pipeline_generation !== generationRef.current) return;
        if (optionsRef.current.onEvent) {
          observe({ kind: 'received', at: performance.now(), bytes: new TextEncoder().encode(event.data).byteLength, message });
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
          case 'ViewData': {
            const expected = expectedNodesRef.current.get(message.node_id);
            if (message.pipeline_generation !== generationRef.current || !expected ||
                message.kind !== expected.kind || message.source_id !== expected.sourceId) return;
            const previous = getSnapshot(message.node_id);
            if (previous && previous.seq >= message.seq) return;
            if (!isValidViewSnapshot(message)) {
              requestSnapshot(socket, message.node_id);
              return;
            }
            snapshotsRef.current = {
              ...snapshotsRef.current,
              [message.node_id]: {
                generation: message.pipeline_generation, nodeId: message.node_id,
                sourceId: message.source_id, kind: message.kind, seq: message.seq,
                columns: message.columns, rows: message.rows,
              },
            };
            setSnapshots(snapshotsRef.current);
            observe({ kind: 'applied', at: performance.now(), nodeId: message.node_id, format: 'snapshot', seq: message.seq, operations: message.rows.length });
            if (resyncRef.current.has(message.node_id)) observe({ kind: 'recovered', at: performance.now(), nodeId: message.node_id });
            resyncRef.current.delete(message.node_id);
            clearNodeError(message.node_id);
            break;
          }
          case 'ViewDelta': {
            if (message.pipeline_generation !== generationRef.current ||
                !expectedNodesRef.current.has(message.node_id)) return;
            const previous = getSnapshot(message.node_id);
            if (previous && message.seq <= previous.seq) return;
            // While repairing, only a snapshot can establish a new baseline.
            if (resyncRef.current.has(message.node_id)) return;
            if (optionsRef.current.allowFaultInjection && droppedNodeRef.current === message.node_id) {
              droppedNodeRef.current = null;
              observe({ kind: 'dropped', at: performance.now(), nodeId: message.node_id });
              return;
            }
            const next = previous ? applyViewDelta(previous, message) : null;
            if (!next) {
              requestSnapshot(socket, message.node_id);
              return;
            }
            snapshotsRef.current = { ...snapshotsRef.current, [message.node_id]: next };
            setSnapshots(snapshotsRef.current);
            observe({ kind: 'applied', at: performance.now(), nodeId: message.node_id, format: 'delta', seq: message.seq, operations: message.changes.length });
            clearNodeError(message.node_id);
            break;
          }
          case 'PipelineStatus':
            if (message.pipeline_generation !== generationRef.current) return;
            for (const [nodeId, seq] of Object.entries(message.sequences)) {
              const previous = getSnapshot(nodeId);
              if (!previous || previous.seq < seq) requestSnapshot(socket, nodeId);
            }
            break;
          case 'ViewError':
            if (message.pipeline_generation === generationRef.current &&
                (message.node_id === 'pipeline' || expectedNodesRef.current.has(message.node_id))) {
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
        if (disposed || socketRef.current !== socket) return;
        setErrors((current) => ({ ...current, connection: 'WebSocket error' }));
      };
      socket.onclose = () => {
        if (disposed || socketRef.current !== socket) return;
        socketRef.current = null;
        resyncRef.current.clear();
        droppedNodeRef.current = null;
        observe({ kind: 'disconnected', at: performance.now() });
        setConnected(false);
        const delay = Math.min(250 * 2 ** reconnectAttempts, 2000);
        reconnectAttempts += 1;
        reconnectTimerRef.current = window.setTimeout(connect, delay);
      };
    };

    connect();
    return () => {
      disposed = true;
      clearReconnectTimer();
      window.clearInterval(retryTimer);
      pendingResyncs.clear();
      const socket = socketRef.current;
      socketRef.current = null;
      socket?.close();
    };
  }, [observe, sendPipeline, tableName, wsUrl]);

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
    send,
    dropNextDelta: (nodeId: string) => {
      if (!optionsRef.current.allowFaultInjection || !connected ||
          !Object.prototype.hasOwnProperty.call(snapshotsRef.current, nodeId) || resyncRef.current.has(nodeId)) return false;
      droppedNodeRef.current = nodeId;
      return true;
    },
    cancelDrop: () => { droppedNodeRef.current = null; },
  };
}
