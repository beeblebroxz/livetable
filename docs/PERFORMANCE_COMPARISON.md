# Performance and Benchmarking

LiveTable is designed for typed row operations, reactive views, and incremental
updates. Performance depends heavily on data shape, storage backend, Python/Rust
boundary costs, and whether an operation can use an incremental path.

The [filter-to-aggregate benchmark](INCREMENTAL_FILTER_PIPELINE.md) and
[filter-to-sort-to-aggregate benchmark](INCREMENTAL_SORTED_PIPELINE.md) record
focused before/after baselines with their harnesses and environments. There is no
library-wide baseline or performance threshold enforced in CI. Treat results
as measurements of the recorded machine, commit, and workload, not guarantees.

## Benchmark surfaces

### Pipeline WebSocket delivery

The [delivery report](PIPELINE_DELIVERY.md) compares snapshot-only protocol v2
with protocol-v3 deltas at the actual localhost WebSocket boundary. The Node
client uses the production delta reducer; timing ends at reconstructed client
state, not React rendering. It reports JSON payload bytes and latency separately
from periodic checkpoint traffic, with an identical seed/client harness on both
implementations. Initial and recovery snapshots remain full-sized.

```bash
cargo build --manifest-path impl/Cargo.toml --release --features server --example pipeline_delivery_server
node benchmarks/pipeline_delivery.mjs "$PWD/impl/target/release/examples/pipeline_delivery_server" 10000 100000
```

The harness needs Node 24+ and permission to bind an ephemeral localhost server.

### Column memory layout

The [typed storage report](TYPED_COLUMN_STORAGE.md) explains native-width
buffers and packed NULL masks, with a dedicated allocator-counting and scan/edit
harness. Its retained-column heap measurements exclude table changesets and
view caches; do not treat them as whole-application memory savings.

```bash
cargo run --manifest-path impl/Cargo.toml --release --example column_layout_benchmark -- 10000 100000
```

### Rust microbenchmarks

`impl/benches/livetable_benchmarks.rs` uses Criterion to measure the Rust core:

- Array and tiered-vector append
- Random access
- Middle insertion
- Table append and row access
- Column operations

Run from the repository root:

```bash
cd impl
cargo bench
```

Criterion performs warm-up, repeated sampling, statistical analysis, and
`black_box` protection. Results are written beneath `impl/target/criterion/`.

### LiveTable versus pandas

`benchmarks/benchmark_vs_pandas.py` compares the installed PyO3 extension with
pandas for row insertion, bulk insertion, filtering, aggregation, joins,
sorting, iteration, and random access.

```bash
cd impl
./install.sh

cd ../benchmarks
python3 -m pip install pandas numpy
python3 benchmark_vs_pandas.py
```

This comparison includes Python binding overhead and is the relevant benchmark
for Python callers. It is not equivalent to the Rust Criterion microbenchmarks.

### Incremental pipeline updates

From the repository root:

```bash
cargo run --manifest-path impl/Cargo.toml --release --example filter_pipeline_benchmark -- 10000 100000
cargo run --manifest-path impl/Cargo.toml --release --example sorted_pipeline_benchmark -- 10000 100000
```

These harnesses measure Rust mutation batches plus `tick()`, excluding initial
construction, Python callbacks, serialization, WebSocket delivery, and rendering.
The linked reports above preserve their before/after commits and measured
environments; rerunning on a different revision creates a new measurement.

### Filter comparison

`benchmarks/filter_vs_pandas.py` compares:

- pandas vectorized filtering
- LiveTable's Rust `filter_expr`
- LiveTable's Python-callback `filter`

```bash
cd benchmarks
python3 filter_vs_pandas.py
```

Vectorized pandas operations may outperform row-oriented APIs on large bulk
workloads. LiveTable's strengths are stable typed storage, row-level access,
and incremental view maintenance rather than replacing every vectorized pandas
operation.

## Complexity expectations

| Operation | Expected complexity |
|-----------|---------------------|
| Array random access | O(1) |
| Array middle insert/delete | O(N) |
| Tiered-vector random access | O(1) |
| Tiered-vector middle insert/delete | O(√N) |
| Hash join construction | O(N + M + R) |
| Filter full rebuild | O(N) |
| Sort full rebuild | O(N log N) |
| Group full rebuild | O(N) plus percentile bookkeeping |
| Bounded-batch view sync | Depends on view and event types; incremental when history/bounds permit |

`R` is join output size. Many-to-many joins may produce an output much larger
than either input.

Filters/sorts replay at most 256 input events; sorting can emit up to 512 events
because a move is a delete/insert pair. Non-sort scalar updates can avoid source
scans, but structural updates still shift indices and can cost O(BN) for B events.
Aggregate structural remapping uses O(N+B) temporary index storage for bounded
batches. Sorts retain O(NK+N) indices and cached values for K sort keys, plus
bounded full-row event payloads. Shared source ownership is not zero memory
overhead, and faster source insertion does not remove downstream index costs.

## Storage selection

Use `storage="fast_reads"` (the default ArraySequence) for append/read-heavy
tables. Use `storage="fast_updates"` (TieredVectorSequence) when frequent
middle inserts/deletes justify additional indirection.

Benchmark both choices with representative data. Big-O behavior does not
predict the crossover point for a particular workload.

## Reporting a benchmark

A useful result should record:

1. Git commit.
2. CPU, memory, operating system, Rust toolchain, and Python version.
3. Dataset size, schema, null/cardinality distribution, and storage hint.
4. Warm-up and iteration counts.
5. Whether construction, conversion, and materialization are included.
6. Median plus a dispersion measure, not a single timing.

For Python comparisons, pin and report pandas/numpy versions. For view tests,
separate initial construction from incremental mutation-plus-`tick()` timing.

## Historical results

An earlier version of this document contained large speedup claims assembled
from different benchmark units and harnesses, including sub-nanosecond access
figures. Those numbers were removed because the repository did not retain the
raw output, environment, or a reproducible calculation tying them to the
current implementation. Re-run the checked-in benchmarks when current numbers
are needed.
