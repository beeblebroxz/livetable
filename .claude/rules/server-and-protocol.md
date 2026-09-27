---
paths:
  - "impl/src/websocket.rs"
  - "impl/src/messages.rs"
  - "impl/src/server.rs"
  - "impl/src/engine.rs"
  - "impl/src/engine/**"
  - "impl/src/pipeline_spec.rs"
  - "impl/src/bin/**"
  - "impl/tests/protocol_*.rs"
  - "frontend/src/**"
  - "benchmarks/**"
  - "docs/WEBSOCKET_PROTOCOL.md"
  - "docs/PIPELINE_DELIVERY.md"
  - "docs/ORDERS_LAB.md"
---

# Server, wire protocol and pipeline delivery

- WebSocket base protocol: `Subscribe`, `Query`, `InsertRow`, `UpdateCell`, and `DeleteRow`; successful mutations broadcast `RowInserted`, `CellUpdated`, and `RowDeleted` to base subscribers
- Flat-table WebSocket reconciliation: every `TableData` and flat-table delta carries a monotonic `seq` (the table's `Changeset::total_len`, captured in the same serialized engine operation as the snapshot/mutation). The client drops any delta with `seq <= snapshot_seq` (already reflected) and buffers deltas that arrive before the snapshot. Pipeline delivery uses a separate sequence domain.
- WebSocket gap recovery: deltas only apply contiguously (`seq == applied + 1`); if a gap persists for `SEQ_GAP_REQUERY_MS` the client re-sends `Query` and re-baselines from the fresh snapshot (`useTableWebSocket.ts`). The pending-delta buffer is capped at `MAX_PENDING_DELTAS` (oldest evicted; the resulting gap heals via re-query). Cell edits are server-authoritative: on blur the input snaps back to the last confirmed value and only the `CellUpdated` broadcast echo moves it forward, so rejected updates (e.g. null into a non-nullable column) self-heal.
- WebSocket `Subscribed` carries `protocol_version` (`messages::PROTOCOL_VERSION`); the client warns on mismatch with `SUPPORTED_PROTOCOL_VERSION`. Bump both on breaking wire changes.
- Pipeline state is scoped by connection/table/generation/node. `pipeline_generation` must increase when replacing an installed pipeline. Each node's delivery `seq` starts at zero and advances once per emitted delta or later snapshot, independently of view versions and changeset cursors. `usePipeline` clears old baselines and repairs on new generations, ignores wrong tables/unknown nodes/stale sockets, and uses own-property-safe node maps.
- `ViewData` uses `WireViewRow { row_id: Option<u64>, row }`; derived rows without stable identity serialize `row_id: null`. Never use `u64::MAX` as a JavaScript sentinel. Pipeline `count` is `COUNT(column)` and every aggregate spec therefore requires a source `column`.
- `ViewDelta` is an atomic ordered batch of 1–512 node-coordinate operations, accepted only on exactly `from_seq` (`seq == from_seq + 1`). Filters/sorts/groups emit their retained output history; empty output sends nothing and keeps the delivery sequence unchanged. Missing/invalidated history (and >512 operations) falls back to snapshots. Protocol v4 added group deltas; v3 clients reject them. The base journal captures IDs and coordinates at mutation time, before root compaction; it is bounded at 512 events and drained on collection. Never scan/diff full node snapshots to build a delta or clone all base row IDs on every tick.
- `QueryView` returns a fresh snapshot and advances only that connection/node's sequence, even without a data change. One-second `PipelineStatus` checkpoints detect lost final/initial deliveries. The client requests repair on gaps, invalid operations, or missing baselines; it retains coherent rows, ignores further deltas while repairing, and retries at 3-second checks without buffering delta history. Do not advance snapshot baselines on failed reads. See `docs/PIPELINE_DELIVERY.md` and `docs/WEBSOCKET_PROTOCOL.md`.
- Run `pipeline_spec::validate_pipeline_spec()` before allocating views. It enforces ordered/acyclic sources, unique non-reserved node IDs, required node fields, and protocol resource limits.
- The canonical wire reference is `docs/WEBSOCKET_PROTOCOL.md`; update it whenever message shapes, limits, generation rules, or sequence semantics change.
- `pages/OrdersLab.tsx` + `hooks/useLab.ts` - Default synthetic Orders Lab, five guided scenarios, virtualized results, actual delivery counters and bounded mixed streaming. `/#editor` opens the redesigned flat-table workspace (separate `demo` data), with local search/sort, 25-row pagination, row inspector and keyboard edits. Generic writes remain server-authoritative; confirmation matches server state, not request IDs. See `docs/ORDERS_LAB.md`.
- `engine/lab.rs` - Opt-in `lab` table and deterministic reset/step/update controls; never target `demo`. Reset preserves source identity, monotonically increasing IDs and delivery baselines. `LabComplete` is a command/delivery reply, not a browser-paint acknowledgement.
