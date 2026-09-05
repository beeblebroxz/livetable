# Start Here

LiveTable has three supported entry points:

1. A Rust table/view library.
2. Python bindings built with PyO3.
3. An optional Actix WebSocket server with a React demo.

The package is not assumed to be preinstalled. Begin from the repository root.

## Fastest Python path

```bash
cd impl
./install.sh

cd ../examples
python3 quickstart.py
```

The tutorial covers typed schemas, CRUD, filters, projection, computed columns,
joins, sorting, grouping, and `tick()` propagation.

To explore interactively:

```bash
# From the repository root (or run ./run.sh if already in examples/)
cd examples
./run.sh
```

## Run the browser demo

```bash
# Terminal 1
cd impl
cargo run --bin livetable-server --features server

# Terminal 2
cd frontend
npm install
npm run dev
```

The default server endpoint is `ws://127.0.0.1:8080/ws`. The Forward Prop Demo
uses real server-owned filter, sort, and group views. Small batches now propagate
incrementally through that chain in Rust and Python, including
`ranked.group_by(...)`. The pipeline wire protocol still sends full snapshots;
delta delivery is a planned next milestone.

## Choose the right guide

- [Getting Started](GETTING_STARTED.md): Python walkthrough and core concepts
- [Python API reference](PYTHON_BINDINGS_README.md): complete binding surface
- [Rust API guide](API_GUIDE.md): native Rust usage
- [Typed column storage](TYPED_COLUMN_STORAGE.md): compact buffers, NULL masks, and measurements
- [Filter propagation](INCREMENTAL_FILTER_PIPELINE.md): shared Rust/Python replay
- [Sorted pipelines](INCREMENTAL_SORTED_PIPELINE.md): latest contract, limits, and benchmarks
- [Join operations](JOIN_FEATURE.md): join types and incremental behavior
- [WebSocket protocol v2](WEBSOCKET_PROTOCOL.md): base and pipeline messages
- [Original design vision](ORIGINAL_VISION.md): architecture goals and status
- [Performance](PERFORMANCE_COMPARISON.md): benchmark methodology
- [Test suite](../tests/README.md): local and CI verification

The implementation plans in `docs/superpowers/` are historical engineering
records. They are useful for design rationale, but the guides above are the
canonical current documentation.
