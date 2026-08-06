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
ranked = table.sort("age", descending=True)
table.append_row({"id": 3, "name": "Carol", "age": 41})
table.tick()
```

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
server. See [docs/WEBSOCKET_PROTOCOL.md](docs/WEBSOCKET_PROTOCOL.md).

## Verification

```bash
cd tests
./run_all.sh
```

The real-server protocol test can also be run directly:

```bash
cd impl
cargo test --features server --test protocol_v2_websocket
```

## Documentation map

- [README.md](README.md): project overview and primary examples
- [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md): Python walkthrough
- [docs/PYTHON_BINDINGS_README.md](docs/PYTHON_BINDINGS_README.md): Python reference
- [docs/API_GUIDE.md](docs/API_GUIDE.md): Rust reference
- [docs/JOIN_FEATURE.md](docs/JOIN_FEATURE.md): join semantics
- [docs/WEBSOCKET_PROTOCOL.md](docs/WEBSOCKET_PROTOCOL.md): server protocol
- [tests/README.md](tests/README.md): test matrix
