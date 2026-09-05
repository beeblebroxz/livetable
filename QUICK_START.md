# LiveTable Quick Start

Run commands below from the repository root unless a step changes directory.

## Python API

Build and install the PyO3 extension:

```bash
cd impl
./install.sh
```

Then run the guided example:

```bash
cd ../examples
python3 quickstart.py
```

Other useful examples:

```bash
python3 demo_reactive_propagation.py --tick
python3 playground.py
python3 scratch.py
./run.sh
```

Minimal Python example:

Columns use native-width buffers and packed NULL masks internally; no special
API is required. Both storage hints support the compact layout.

```python
import livetable

schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    ("age", livetable.ColumnType.INT32, True),
])
table = livetable.Table("users", schema)

table.append_row({"id": 1, "name": "Alice", "age": 30})
table.append_row({"id": 2, "name": "Bob", "age": None})

adults = table.filter(lambda row: row["age"] is not None and row["age"] >= 18)
print(adults[0])
```

Views with cached indices update when synchronized. Views created through the
simplified API are registered automatically:

```python
ranked = adults.sort("age", descending=True)
totals = ranked.group_by("name", agg=[("total_age", "age", "sum")])
table.append_row({"id": 3, "name": "Carol", "age": 41})
table.tick()
assert ranked[0]["name"] == "Carol"
assert {r["name"]: r["total_age"] for r in totals} == {"Alice": 30, "Carol": 41}
```

This is an incremental table → filter → sort → group chain for small batches.
Filters/sorts rebuild when more than 256 input events are pending or their
history is unavailable. Sync parents before children; after a manual refresh
without a root mutation, call the child's `sync()` directly. See the
[propagation contract](docs/INCREMENTAL_SORTED_PIPELINE.md) for limits.

## Rust API

Run the checked-in examples directly:

```bash
cd impl
cargo run --example basic_table
cargo run --example views
cargo run --example joins
```

See [docs/API_GUIDE.md](docs/API_GUIDE.md) for the public Rust surface.

## WebSocket server and React demo

Terminal 1:

```bash
cd impl
cargo run --bin livetable-server --features server
```

Terminal 2:

```bash
cd frontend
npm install
npm run dev
```

Open the URL printed by Vite. The editor uses base-table synchronization; the
Forward Prop Demo sends a protocol-v2 filter/sort/group pipeline to the Rust
server. Its engine updates incrementally, but pipeline transport still sends
full snapshots. See [docs/WEBSOCKET_PROTOCOL.md](docs/WEBSOCKET_PROTOCOL.md).

## Verification

```bash
cd tests
./run_all.sh
```

From the repository root, run the propagation and real-server tests directly:

```bash
cargo test --manifest-path impl/Cargo.toml --features server --test filter_pipeline --test sorted_pipeline --test forward_prop_fuzz
cargo test --manifest-path impl/Cargo.toml --features server --test protocol_v2_websocket
```

## Documentation map

- [README.md](README.md): project overview and primary examples
- [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md): Python walkthrough
- [docs/PYTHON_BINDINGS_README.md](docs/PYTHON_BINDINGS_README.md): Python reference
- [docs/API_GUIDE.md](docs/API_GUIDE.md): Rust reference
- [docs/TYPED_COLUMN_STORAGE.md](docs/TYPED_COLUMN_STORAGE.md): memory layout and measurements
- [docs/INCREMENTAL_SORTED_PIPELINE.md](docs/INCREMENTAL_SORTED_PIPELINE.md): latest propagation contract and measured benchmarks
- [docs/JOIN_FEATURE.md](docs/JOIN_FEATURE.md): join semantics
- [docs/WEBSOCKET_PROTOCOL.md](docs/WEBSOCKET_PROTOCOL.md): server protocol
- [tests/README.md](tests/README.md): test matrix
