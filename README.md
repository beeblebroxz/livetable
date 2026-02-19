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

LiveTable excels at **row-level operations** and **reactive views** - areas where pandas struggles:

| Operation | LiveTable | Pandas | Speedup |
|-----------|-----------|--------|---------|
| Row iteration (`for row in table`) | 2.8ms | 68ms | **25x faster** |
| Random access (`get_row(i)`) | 0.3ms | 10ms | **33x faster** |
| Aggregations (small data) | 0.02ms | 0.03ms | 1.5x faster |

*Benchmarks on 10,000 rows. See `benchmarks/benchmark_vs_pandas.py`*

**Key advantages:**
- **Zero-copy views** - FilterView, JoinView, etc. don't duplicate data
- **Reactive updates** - Views auto-update when source table changes
- **Type safety** - Schema-enforced types catch errors early
- **Pythonic API** - Natural Python syntax with indexing, slicing, and iteration

**When to use pandas instead:** Bulk vectorized operations on large datasets where numpy's optimized C code excels.

## Design Philosophy

LiveTable makes some unusual design choices to optimize for specific workloads:

### Adaptive Storage Backends

Most table libraries use a single storage strategy. LiveTable lets you choose at table creation:

```python
# Default: optimized for analytics, batch processing, read-heavy workloads
logs = livetable.Table("logs", schema)  # ArraySequence

# For order books, streaming inserts, time-series with frequent updates
orderbook = livetable.Table("orderbook", schema, storage="fast_updates")  # TieredVectorSequence
```

**Why this matters:** A contiguous array gives you cache-friendly iteration and O(1) access, but inserting in the middle requires shifting all subsequent elements (O(N)). A [tiered vector](https://crates.io/crates/tiered-vector) maintains O(1) access while reducing insert/delete to O(√N) - the difference between 1 million operations and 1,000.

### True O(1) Tiered Vector Access

Many "tiered vector" implementations use sqrt-decomposition with binary search, giving O(log √N) access time. LiveTable uses the [tiered-vector](https://crates.io/crates/tiered-vector) crate which employs **circular buffers** to compute indices directly - genuine constant-time access regardless of table size.

### Zero-Copy View DAG

Views don't copy data - they reference the source table and compute on demand:

```
┌─────────┐
│  Table  │──┬──► FilterView ──► SortedView
└─────────┘  │
             ├──► JoinView
             │
             └──► AggregateView
```

When the source table changes, views receive **changesets** describing what changed. Smart views (like AggregateView) update incrementally - if you add one row, it adjusts the running totals rather than re-scanning everything.

### String Interning

For columns with repeated values (status codes, categories, country names), enable string interning:

```python
table = livetable.Table("events", schema, use_string_interning=True)
```

Each unique string is stored once; the column holds 4-byte IDs instead of full strings. This can dramatically reduce memory for high-cardinality categorical data.

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

### Tables & CRUD
- Strongly-typed schemas with compile-time (Rust) and runtime (Python) validation
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

### Views (Zero-Copy)
| View | Method | Description |
|------|--------|-------------|
| `FilterView` | `table.filter()` | Filter rows with Python lambdas |
| `ProjectionView` | `table.select()` | Select specific columns |
| `ComputedView` | `table.add_computed_column()` | Add calculated columns |
| `JoinView` | `table.join()` | LEFT/INNER joins (single or composite keys) |
| `SortedView` | `table.sort()` | Multi-column sorting |
| `AggregateView` | `table.group_by()` | GROUP BY with SUM, AVG, MIN, MAX, COUNT, MEDIAN, PERCENTILE |

Views created with simplified API are auto-registered. Call `table.tick()` to propagate changes to all registered views at once.

### Filtering
```python
# Lambda filter
high_scorers = table.filter(lambda row: row["score"] >= 90)

# Expression filter (2x faster)
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

### Joining
```python
# Join on same-named column
joined = students.join(grades, on="id")

# Join on different column names
joined = students.join(enrollments, left_on="id", right_on="student_id")

# Inner join (default is left)
joined = students.join(enrollments, left_on="id", right_on="student_id", how="inner")

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
    ("score", livetable.ColumnType.FLOAT64, True),  # Nullable
    ("joined", livetable.ColumnType.DATE, False),
])
table = livetable.Table("students", schema)

# For insert-heavy workloads, use fast_updates storage:
# table = livetable.Table("orderbook", schema, storage="fast_updates")

# Add data
table.append_row({"id": 1, "name": "Alice", "score": 95.5, "joined": date(2024, 9, 1)})
table.append_rows([
    {"id": 2, "name": "Bob", "score": 87.0, "joined": date(2024, 9, 1)},
    {"id": 3, "name": "Charlie", "score": None, "joined": date(2024, 9, 15)},
])

# Query with Pythonic syntax
for row in table:
    print(f"{row['name']}: {row['score']}")

names = [row["name"] for row in table if row["score"] and row["score"] >= 90]

# Create filtered view
high_scorers = table.filter(lambda row: row["score"] is not None and row["score"] >= 90)
print(f"High scorers: {len(high_scorers)}")

# Sort by score
sorted_table = table.sort("score", descending=True)
print(f"Top student: {sorted_table[0]['name']}")

# Join tables
enrollments = livetable.Table("enrollments", enrollment_schema)
joined = table.join(enrollments, left_on="id", right_on="student_id")

# Group by with aggregations
by_dept = table.group_by("department", agg=[
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

```bash
# Run all tests (recommended)
cd tests && ./run_all.sh

# Individual test suites
cd tests && pytest python/ -v      # Python unit tests (294 tests)
cd tests && pytest integration/    # Integration tests (5 workflows)
cd impl && cargo test --lib        # Rust tests (75 tests)
```

## React Frontend

Real-time table editor with WebSocket sync.

```bash
# Terminal 1: Start backend
cd impl
cargo run --bin livetable-server --features server

# Terminal 2: Start frontend (Node.js 18+)
cd frontend
npm install && npm run dev
```

Connects to `ws://127.0.0.1:8080/ws` by default. Override with `VITE_WS_URL`.

## Project Structure

```
livetable/
├── impl/                   # Rust implementation + Python bindings
│   ├── src/
│   │   ├── lib.rs         # Library root
│   │   ├── table.rs       # Table implementation
│   │   ├── column.rs      # Column types and values
│   │   ├── view.rs        # View implementations
│   │   ├── sequence.rs    # Storage backends
│   │   └── python_bindings.rs
│   └── install.sh         # Build + install script
│
├── examples/               # Python examples
├── tests/                  # Test suites
├── frontend/               # React real-time editor
├── docs/                   # Additional documentation
└── benchmarks/             # Performance comparisons
```

## Architecture

- **Language**: Rust core with PyO3 Python bindings
- **Storage**: Pluggable backends (ArraySequence / TieredVectorSequence) - see [Design Philosophy](#design-philosophy)
- **Views**: Zero-copy DAG with incremental change propagation
- **Type System**: Strongly typed columns with NULL support
- **Memory**: Optional string interning for categorical data

## License

MIT
