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
| Single-change view sync | Depends on view; avoids a full rebuild when supported |

`R` is join output size. Many-to-many joins may produce an output much larger
than either input.

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
