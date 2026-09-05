# LiveTable

A high-performance columnar table system written in Rust with Python bindings. Get the performance of Rust with the ease of Python.

## Quick Start

```bash
# Build and install
cd impl
./install.sh

# Try the examples
cd ../examples
python3 quickstart.py                # 5-minute tutorial
python3 playground.py                # Interactive examples
python3 demo_reactive_propagation.py # Watch changes flow through views
```

## Why LiveTable?

LiveTable is designed for **typed row-level operations** and **reactive
views**. Performance is workload- and environment-dependent; use the checked-in
benchmarks for current measurements rather than treating historical numbers as
guarantees. See [Performance and Benchmarking](docs/PERFORMANCE_COMPARISON.md).

**Key advantages:**
- **Compact typed storage** - Native-width column buffers, packed NULL masks, and ID-only interned strings
- **Shared-source views** - Views read source rows while caching indices, sort keys, or aggregate state as needed
- **Reactive updates** - `tick()` incrementally synchronizes registered views
- **Incremental sorted pipelines** - Small batches propagate through filter → sort → group
- **Type safety** - Schema-enforced types catch errors early
- **Pythonic API** - Natural Python syntax with indexing, slicing, and iteration

**When to use pandas instead:** Bulk vectorized operations on large datasets where numpy's optimized C code excels.

## Design Philosophy

LiveTable makes some unusual design choices to optimize for specific workloads:

### Selectable Storage Backends

Most table libraries use a single storage strategy. LiveTable lets you choose at table creation:

```python
# Default: optimized for analytics, batch processing, read-heavy workloads
logs = livetable.Table("logs", schema)  # ArraySequence

# For order books, streaming inserts, time-series with frequent updates
orderbook = livetable.Table("orderbook", schema, storage="fast_updates")  # TieredVectorSequence
```

**Why this matters:** A contiguous array gives you cache-friendly iteration and O(1) access, but inserting in the middle requires shifting subsequent elements (O(N)). A [tiered vector](https://crates.io/crates/tiered-vector) maintains O(1) access while reducing storage-level insert/delete to O(√N). These are asymptotic storage costs, not measured operation counts; view index maintenance can still be O(N).

### True O(1) Tiered Vector Access

Many "tiered vector" implementations use sqrt-decomposition with binary search, giving O(log √N) access time. LiveTable uses the [tiered-vector](https://crates.io/crates/tiered-vector) crate which employs **circular buffers** to compute indices directly - genuine constant-time access regardless of table size.

### Shared-Source View DAG

Views reference their parents instead of maintaining full copies of source rows.
Filters and joins cache row mappings; sorts also cache sort-key columns;
aggregates materialize group state. Reads and retained change payloads may copy
values, so "zero-copy" is not an allocation-free guarantee:

```
┌─────────┐
│  Table  │──┬──► FilterView ──► SortedView ──► AggregateView
└─────────┘  │
             ├──► JoinView
             │
             └──► AggregateView
```

When the source table changes, views receive **changesets** describing what
changed. Calling `tick()` synchronizes registered views in topological order.
Stateful views such as `AggregateView` update incrementally when the change can
be replayed safely and fall back to a rebuild when needed.

### String Interning

For columns with repeated values (status codes, categories, country names), enable string interning:

```python
table = livetable.Table("events", schema, use_string_interning=True)
```

Each unique string is stored once; the column holds 4-byte IDs instead of full
strings. This can reduce memory for low-cardinality or otherwise highly
repetitive text.

### Native-width column buffers

INT32 values occupy 4-byte slots, FLOAT64 values 8-byte slots; the type is
selected once per column. Nullable columns use packed masks, and interned
strings store only 4-byte IDs without a duplicate placeholder buffer. Both
array and tiered storage are supported without changing the public API.
Read APIs and changesets still use `ColumnValue`/row objects. See the
[layout, guarantees, and measurements](docs/TYPED_COLUMN_STORAGE.md).

## Data Types

| Type | Python Type | Description |
|------|-------------|-------------|
| `INT32` | `int` | 32-bit signed integer |
| `INT64` | `int` | 64-bit signed integer |
| `FLOAT32` | `float` | 32-bit floating point |
| `FLOAT64` | `float` | 64-bit floating point |
| `STRING` | `str` | UTF-8 text (with optional interning) |
| `BOOL` | `bool` | Boolean true/false |
| `DATE` | `datetime.date` | Calendar date |
| `DATETIME` | `datetime.datetime` | Date with time |

Any column can be marked **nullable** to support `None` values.

## Features

API fragments below assume tables with the named columns and matching schemas.
The composition example and [complete example](#example) define their own data.

### Tables & CRUD
- Typed Rust values and runtime schema validation in both Rust and Python
- Row operations: `append_row()`, `get_row()`, `set_value()`, `delete_row()`
- Bulk insert: `append_rows([...])` for efficient multi-row operations
- NULL value support for nullable columns
- Storage hints: `storage="fast_reads"` (default) or `storage="fast_updates"` for insert-heavy workloads

### Pythonic API
```python
table[0]              # First row
table[-1]             # Last row (negative indexing)
table[1:5]            # Slice rows 1-4
table["name"]         # All values from "name" column
for row in table:     # Iteration
    print(row)
```

### Views
| View | Method | Description |
|------|--------|-------------|
| `FilterView` | `table.filter()` | Filter rows with Python lambdas |
| `ProjectionView` | `table.select()` | Select specific columns |
| `ComputedView` | `table.add_computed_column()` | Add calculated columns |
| `JoinView` | `table.join()` | LEFT/INNER/RIGHT/FULL joins (single or composite keys) |
| `SortedView` | `table.sort()` | Multi-column sorting |
| `AggregateView` | `table.group_by()` | GROUP BY with SUM, AVG, MIN, MAX, COUNT, MEDIAN, PERCENTILE |

Stateful views created with the simplified API are auto-registered. Call
`table.tick()` after mutations to synchronize them. Projection and computed
views read through to their parent and need no registration.

### View Composition (Views over Views)
Views can derive from other views, forming a DAG over root tables:

```python
import livetable

sales = livetable.Table("sales", livetable.Schema([
    ("region", livetable.ColumnType.STRING, False),
    ("amount", livetable.ColumnType.FLOAT64, False),
]))
sales.append_rows([
    {"region": "N", "amount": 50.0},
    {"region": "S", "amount": 150.0},
])
big = sales.filter(lambda row: row["amount"] >= 100)
ranked = big.sort("amount", descending=True)          # sort the filtered rows
by_region = ranked.group_by("region", agg=[("total", "amount", "sum")])

sales.append_row({"region": "S", "amount": 900.0})
sales.tick()   # root -> filter -> sort -> group, all updated in one call
assert by_region[0]["total"] == 1050.0
```

In Rust, any view can parent any other view — every view implements the
`ReadableTable` trait (`FilterView`, `SortedView`, `AggregateView`, `JoinView`,
`ProjectionView`, `ComputedView`, and `Table` itself).

Filters and sorted views publish changesets, allowing
`table -> filter -> sort -> group_by` to update incrementally in Rust and Python.
Small batches evaluate only changed rows; an edit that stays outside the filter
produces no downstream changes in these engine stages (wire snapshots are separate).
Each filter/sort retains one batch of history and rebuilds for more than 256
input changes or unavailable history. Sorts cache only sort-key columns and
index mappings, not complete source rows. Non-sort edits forward without a
scan; row moves can still shift linear-time bookkeeping. Children of other
view types use version-checked rebuilds. See the
[filter contract](docs/INCREMENTAL_FILTER_PIPELINE.md) and
[sorted pipeline contract and benchmarks](docs/INCREMENTAL_SORTED_PIPELINE.md).

### Filtering
```python
# Lambda filter
high_scorers = table.filter(lambda row: row["score"] is not None and row["score"] >= 90)

# Expression filter (evaluated in Rust without a Python callback)
indices = table.filter_expr("score >= 90 AND name != 'Test'")
# Supports: =, !=, <, >, <=, >=, AND, OR, NOT, IS NULL, IS NOT NULL
```

### Sorting
```python
# Single column
sorted_table = table.sort("score")                    # Ascending (default)
sorted_table = table.sort("score", descending=True)   # Descending

# Multiple columns with mixed order
sorted_table = table.sort(["score", "name"], descending=[True, False])
```

Python's simplified `.sort()` puts NULL first in either direction. Use explicit
`SortKey` objects for NULL-last ordering. Equal keys preserve parent order;
NaNs tie after numbers ascending and before numbers descending.

### Joining
```python
# Join on same-named column
joined = students.join(grades, on="id")

# Join on different column names
joined = students.join(enrollments, left_on="id", right_on="student_id")

# Inner join (default is left)
joined = students.join(enrollments, left_on="id", right_on="student_id", how="inner")

# Right join - all rows from right table
joined = students.join(grades, on="id", how="right")

# Full outer join - all rows from both tables
joined = students.join(grades, on="id", how="full")   # also: "outer", "full_outer"

# Multi-column join
joined = sales.join(targets, on=["year", "month"])
```

### Aggregations
```python
# Simple aggregations
table.sum("score")
table.avg("score")
table.min("score")
table.max("score")
table.count_non_null("score")

# GROUP BY
grouped = table.group_by("department", agg=[
    ("total", "salary", "sum"),
    ("average", "salary", "avg"),
])
```

### Serialization
```python
# Export
csv_string = table.to_csv()
json_string = table.to_json()

# Import (types auto-inferred)
table = livetable.Table.from_csv("name", csv_string)
table = livetable.Table.from_json("name", json_string)
```

### Pandas Integration
```python
import pandas as pd

df = table.to_pandas()                              # Table -> DataFrame
table = livetable.Table.from_pandas("name", df)    # DataFrame -> Table
```

## Example

```python
import livetable
from datetime import date

# Define schema
schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    ("department", livetable.ColumnType.STRING, False),
    ("score", livetable.ColumnType.FLOAT64, True),  # Nullable
    ("joined", livetable.ColumnType.DATE, False),
])
table = livetable.Table("students", schema)

# For insert-heavy workloads, use fast_updates storage:
# table = livetable.Table("orderbook", schema, storage="fast_updates")

# Add data
table.append_row({"id": 1, "name": "Alice", "department": "Science", "score": 95.5, "joined": date(2024, 9, 1)})
table.append_rows([
    {"id": 2, "name": "Bob", "department": "Arts", "score": 87.0, "joined": date(2024, 9, 1)},
    {"id": 3, "name": "Charlie", "department": "Science", "score": None, "joined": date(2024, 9, 15)},
])

# Query with Pythonic syntax
for row in table:
    print(f"{row['name']}: {row['score']}")

names = [row["name"] for row in table if row["score"] and row["score"] >= 90]

# Create filtered view
high_scorers = table.filter(lambda row: row["score"] is not None and row["score"] >= 90)
print(f"High scorers: {len(high_scorers)}")

# Sort the non-NULL high scorers; simplified sort otherwise puts NULL first
sorted_table = high_scorers.sort("score", descending=True)
print(f"Top student: {sorted_table[0]['name']}")

# Join tables
enrollment_schema = livetable.Schema([
    ("student_id", livetable.ColumnType.INT32, False),
    ("course", livetable.ColumnType.STRING, False),
])
enrollments = livetable.Table("enrollments", enrollment_schema)
joined = table.join(enrollments, left_on="id", right_on="student_id")

# Group the filtered, ranked students
by_dept = sorted_table.group_by("department", agg=[
    ("total", "score", "sum"),
    ("avg", "score", "avg"),
])

# Export
with open("students.csv", "w") as f:
    f.write(table.to_csv())
```

## Building

### Simple Way (Recommended)
```bash
cd impl
./install.sh
```

### Manual Build
```bash
cd impl
pip install maturin
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release
pip install target/wheels/livetable-*.whl
```

## Testing

Run from the repository root. Rebuild/reinstall the Python extension after
source changes; the convenience runner only installs it if import fails.

```bash
# Run the standard local checks
(cd tests && ./run_all.sh)

# Individual verification steps
cargo clippy --manifest-path impl/Cargo.toml --all-targets -- -D warnings
cargo clippy --manifest-path impl/Cargo.toml --all-targets --features server -- -D warnings
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo clippy --manifest-path impl/Cargo.toml --all-targets --features python -- -D warnings
cargo test --manifest-path impl/Cargo.toml --features server --lib --test filter_pipeline --test sorted_pipeline --test forward_prop_fuzz
cargo test --manifest-path impl/Cargo.toml --features server --test protocol_v2_websocket
python3 -m pytest -c tests/pytest.ini
(cd frontend && npm run lint && npm run test && npm run build)
```

See [tests/README.md](tests/README.md) for the full test matrix and toolchain split.

## React Frontend

Real-time table editor with WebSocket sync. Protocol v2 also supports
connection-local server-computed pipelines: the forward-propagation demo sends
`SetPipeline` definitions and renders Rust-engine `ViewData` snapshots for the
base, filter, sort, and group nodes. Expression rebuilds are debounced and
generation-scoped so stale responses cannot overwrite newer definitions.

```bash
# Terminal 1: Start backend
cd impl
cargo run --bin livetable-server --features server

# Terminal 2: Start frontend (Node.js 18+)
cd frontend
npm install && npm run dev
```

The current demo client connects to `ws://<current-host>:8080/ws` by default. Set
`VITE_LIVETABLE_WS_URL=ws://host:port/ws` when starting Vite to override it.
See [WebSocket Protocol v2](docs/WEBSOCKET_PROTOCOL.md) for message schemas and
reconciliation rules.

Pipeline nodes still send full snapshots, including the pipeline's base node;
internal filter/sort changesets are not transmitted as deltas. Pipeline delta
delivery with resynchronization is planned, not implemented.

## Project Structure

```
livetable/
├── impl/                       # Rust implementation + Python bindings
│   ├── src/
│   │   ├── lib.rs              # Library root
│   │   ├── table.rs            # Table, Schema, storage hints
│   │   ├── column.rs           # Column API, types, and conversions
│   │   ├── column/             # Typed buffers, packed NULL masks, and tests
│   │   ├── sequence.rs         # Storage backends (Array / TieredVector)
│   │   ├── readable.rs         # ReadableTable trait (view composition)
│   │   ├── view.rs             # View module root + shared join-key types
│   │   ├── view/               # One file per view type + tests
│   │   ├── changeset.rs        # Incremental change tracking
│   │   ├── filter_changes.rs   # Shared bounded historical-row replay
│   │   ├── expr.rs             # Expression parser for filter_expr()
│   │   ├── interner.rs         # String interning engine
│   │   ├── engine.rs           # Server table owner + per-connection pipelines
│   │   ├── pipeline_spec.rs    # Bounded protocol spec validation/building
│   │   ├── messages.rs         # WebSocket wire protocol types
│   │   ├── websocket.rs        # WebSocket server (actix)
│   │   ├── server.rs           # HTTP server setup
│   │   ├── bin/                # Server binary entry point
│   │   ├── python_bindings.rs  # PyO3 bindings
│   │   └── python_bindings/    # Conversions and iterator types
│   └── install.sh              # Build + install script
│
├── examples/                   # Python examples
├── tests/                      # Python + integration test suites
├── frontend/                   # React real-time editor + Vitest/ESLint
├── .github/workflows/          # CI pipeline
├── docs/                       # Additional documentation
└── benchmarks/                 # Performance comparisons
```

## Architecture

- **Language**: Rust core with PyO3 Python bindings
- **Storage**: Pluggable backends (ArraySequence / TieredVectorSequence) - see [Design Philosophy](#design-philosophy)
- **Views**: Shared-source DAG with bounded incremental propagation and rebuild fallbacks
- **Type System**: Strongly typed columns with NULL support
- **Memory**: Optional string interning for categorical data

## Documentation

- [Quick start](QUICK_START.md)
- [Python API reference](docs/PYTHON_BINDINGS_README.md)
- [Rust API guide](docs/API_GUIDE.md)
- [Filter propagation contract](docs/INCREMENTAL_FILTER_PIPELINE.md)
- [Sorted pipeline contract and benchmarks](docs/INCREMENTAL_SORTED_PIPELINE.md)
- [Join semantics](docs/JOIN_FEATURE.md)
- [WebSocket protocol v2](docs/WEBSOCKET_PROTOCOL.md)
- [Test matrix](tests/README.md)

## License

MIT
