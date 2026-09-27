# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LiveTable is a columnar table system written in Rust with Python bindings via PyO3. It focuses on typed row operations, shared-source view DAGs, and incremental change propagation; benchmark claims must be tied to the checked-in harnesses and a recorded environment. Views keep indices, cached keys, or aggregate state; reads and event payloads clone values/rows, so do not claim blanket zero-copy behavior.

## Working Style

The Working Style in `~/projects/CLAUDE.md` applies. Keep writing specs and plans to `docs/superpowers/`; they are how the next session sees what is done and what is next.

## Original Design Vision

Before adding new features, consult the original design document:

**[docs/ORIGINAL_VISION.md](docs/ORIGINAL_VISION.md)**

Key principles from the original vision:
- Operations should be as fast as hand-coded C++
- Large table graphs can "tick" in real time
- Algorithms don't care about physical data layout
- Two storage backends via `StorageHint`: Array (O(1) access, default) and TieredVector (O(1) access, O(√N) insert)
- Root tables own data; Views derive from parents via DAG
- Incremental change propagation through changesets
- String interning for memory efficiency

## Documentation

When a feature or API change is done, use the `feature-docs` skill before committing; it lists every doc to update. Plans/specs under `docs/superpowers/` are historical records, not current task instructions.


## Build Commands

Each command group starts at the repository root. Prefer an isolated virtualenv
for Python builds; see the Python reference. Rebuild/reinstall after Rust changes:
the test runner checks importability, not whether an installed extension is current.

```bash
# Build and install Python package (recommended)
cd impl && ./install.sh

# Manual build with ABI3 compatibility (required for PyO3)
cd impl
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release
pip3 install target/wheels/livetable-*.whl --force-reinstall

# Build WebSocket server
cd impl && cargo build --bin livetable-server --features server
```

## Test Commands

```bash
# Run standard Rust/Python/frontend checks
cd tests && ./run_all.sh

# Rust tests only
cd impl && cargo test --lib --features server

# Propagation contracts and randomized differential tests
cd impl && cargo test --features server --test filter_pipeline --test sorted_pipeline --test aggregate_pipeline --test join_pipeline --test forward_prop_fuzz

# Real WebSocket protocol integration test
cd impl && cargo test --features server --test protocol_v3_websocket

# Python tests only
cd tests && pytest python/ -v

# Integration tests only
cd tests && pytest integration/ -v

# Rust benchmarks
cd impl && cargo bench
```

## Frontend Commands

```bash
# Start WebSocket server first
cd impl && cargo run --release --bin livetable-server --features server -- --lab

# Then start frontend (Node.js 20+; 22+ for browser tests)
cd frontend && npm install && npm run dev
```

## Architecture

### Layered Design (impl/src/)
- **sequence.rs** - Storage backends: `ArraySequence` (contiguous, default), `TieredVectorSequence` (O(√N) inserts, backed by [tiered-vector](https://crates.io/crates/tiered-vector) crate)
- **column.rs** + **column/** - Public ColumnValue API over native-width typed buffers, packed array/tiered NULL masks, and ID-only interned strings; storage/bitmap model tests live alongside the implementation
- **table.rs** - Row-level CRUD operations on column collections
- **readable.rs** - `ReadableTable` trait: the read surface shared by `Table` and every view, enabling views-over-views (DAG composition)
- **changeset.rs** - `TableChange` events and the `Changeset` history that every incremental view consumes and publishes
- **view.rs** + **view/** - Shared-source views, one file per type: `FilterView`, `ProjectionView`, `ComputedView`, `JoinView` (LEFT/INNER/RIGHT/FULL), `SortedView`, `AggregateView`, `TickableTable`; view.rs holds shared typed join-key machinery; tests in view/tests.rs
- **filter_changes.rs** - Shared historical-row reconstruction and filter replay for Rust/Python; also used by sort/aggregate consumers
- **python_bindings.rs** - PyO3 bindings exposing Rust types as Python classes
- **engine.rs** + **engine/delivery.rs** + **pipeline_spec.rs** - Single-threaded server table owner, bounded wire-spec → real-view construction, and cursor-checked pipeline delivery
- **websocket.rs** + **messages.rs** - Actix transport and protocol-v4 real-time sync

### Invariants

Subsystem invariants live in `.claude/rules/` and load automatically when you read or edit matching files:
- `tables-and-columns.md` - column storage, NULL masks, string interning, mutation safety, type inference, expressions
- `views-and-propagation.md` - view DAG, changesets and cursors, filter/sort/aggregate/join incremental contracts, iterator guards
- `python-bindings.md` - Python callbacks, chaining and tick registration, constructor options
- `server-and-protocol.md` - WebSocket protocol, sequencing and gap recovery, pipeline delivery, Orders Lab

When a change alters an invariant, update its rule file.

### Frontend (frontend/src/)
- React 18 + TypeScript + Vite + Tailwind CSS
- `hooks/useTableWebSocket.ts` - Flat-table WebSocket connection management
- `hooks/usePipeline.ts` + `lib/pipelineReconciliation.ts` - Generation-aware pipeline connection management, atomic deltas, and snapshot recovery
- `components/LiveTable.tsx` - Real-time table rendering with TanStack Table
- `pages/OrdersLab.tsx` + `hooks/useLab.ts` - Default synthetic Orders Lab; `/#editor` opens the flat-table workspace. See `docs/ORDERS_LAB.md`.
- `npm run test:e2e` - Isolated real Rust server + installed headless Chrome tests on ports 8087/5180. Browser outputs are ignored under `frontend/test-results/`.

## Critical Notes

1. **Always use ABI3 forward compatibility** when building Python bindings:
   ```bash
   env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release
   ```

2. **Python package must be installed** before running Python tests - if imports fail, run `cd impl && ./install.sh`

3. **Port usage**: Backend WebSocket server runs on port 8080, frontend dev server on port 5173

4. **RefCell borrow safety**: When holding `Rc<RefCell<Table>>` borrows in PyO3 code, release the borrow before making Python calls (e.g., pandas DataFrame construction) to avoid panics from GC reentrancy

5. **Mutation safety**: Always validate all values before mutating any column — partial mutations on type errors corrupt column lengths irrecoverably. Use `Column::check_value_type()` for pre-validation. `append_row` rolls back via `truncate_to` and `delete_row` re-inserts already-deleted values on partial failure; keep two-phase commit semantics when adding row-level mutations.

6. **Float serialization**: `serde_json::Number::from_f64()` returns `None` for NaN/Infinity — always handle with `.unwrap_or(JsonValue::Null)`, never `.unwrap()`

7. **Pre-epoch datetime**: Use `div_euclid`/`rem_euclid` for ms→days decomposition, not `/`/`%` (truncating division gives wrong results for negative timestamps)

8. **NaN in incremental aggregates**: `AggregateView` excludes NaN values like NULL (`extract_numeric` guard) — NaN would permanently poison running SUM/AVG and can never be binary-searched out of `sorted_values`. Group-by keys canonicalize NaN (one group, Postgres-style) and fold -0.0 into +0.0. Keep these guards when adding aggregate paths.

9. **Timezone-aware datetimes are rejected**: `datetime_to_ms_since_epoch` raises `ValueError` for aware datetimes (storage is naive ms-since-epoch; reading wall-clock fields would silently drop the offset). The pandas path falls back to `.timestamp()`, which converts aware values correctly.

## Python API

The full reference with examples is [docs/PYTHON_BINDINGS_README.md](docs/PYTHON_BINDINGS_README.md); a complete runnable chain is in [Getting Started](docs/GETTING_STARTED.md).
