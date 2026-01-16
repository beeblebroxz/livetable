# LiveTable

A high-performance columnar table system written in Rust with Python bindings. Get the performance of Rust with the ease of Python!

## Quick Start

```bash
# Build and install
cd impl
./install.sh

# Try the examples
cd ../examples
python3 quickstart.py      # 5-minute tutorial
python3 playground.py      # Interactive examples
```

## Project Structure

```
livetable/
├── README.md                    # This file
├── docs/                        # Documentation
│   ├── PYTHON_BINDINGS_README.md  # Python API reference
│   ├── API_GUIDE.md            # Complete API docs
│   └── JOIN_FEATURE.md         # Join operations guide
│
├── impl/                        # Rust implementation + Python bindings
│   ├── src/
│   │   ├── lib.rs              # Library root
│   │   ├── column.rs           # Column implementation
│   │   ├── sequence.rs         # Storage backends
│   │   ├── table.rs            # Table implementation
│   │   ├── view.rs             # Views and joins
│   │   └── python_bindings.rs  # PyO3 Python bindings
│   ├── build.sh                # Build script
│   ├── install.sh              # Build + install script
│   ├── examples/               # Rust usage examples
│   └── benches/                # Performance benchmarks
│
├── examples/                    # Python examples
│   ├── quickstart.py           # Quick tutorial
│   ├── playground.py           # Interactive playground
│   └── scratch.py              # Your blank canvas
│
├── frontend/                    # React frontend (real-time editor)
│   └── ...
│
├── tests/                       # Comprehensive test suite
│   ├── python/                 # Python unit tests
│   ├── integration/            # Integration tests
│   └── run_all.sh              # Run all tests
│
└── benchmarks/                  # Performance comparisons
```

## Features

### Core Table Operations
- Create tables with strongly-typed schemas
- CRUD operations (Create, Read, Update, Delete)
- NULL value support
- Type validation at compile time (Rust) and runtime (Python)
- **Date and DateTime types** - Native support for dates and timestamps

### Advanced Views
- **FilterView** - Filter rows with Python lambda functions
- **Expression Filtering** - Filter with string expressions (2x faster than lambdas)
- **ProjectionView** - Select specific columns
- **ComputedView** - Add computed columns with lambdas
- **JoinView** - LEFT and INNER joins between tables (supports multi-column composite keys)
- **SortedView** - Sorted views with multi-column support
- **AggregateView** - GROUP BY with SUM, AVG, MIN, MAX, COUNT

### Aggregations
- Simple aggregations: `sum()`, `avg()`, `min()`, `max()`, `count_non_null()`
- GROUP BY support with `AggregateView`
- Incremental updates - aggregates update efficiently on table changes
- Multiple aggregations per view

### Serialization
- Export tables to CSV and JSON formats
- Import tables from CSV and JSON strings
- Automatic type inference on import
- Proper handling of NULL values and special characters

### Data Types
- **Integers**: INT32, INT64
- **Floats**: FLOAT32, FLOAT64
- **Text**: STRING (with optional interning for memory efficiency)
- **Boolean**: BOOL
- **Temporal**: DATE, DATETIME (native Python `date`/`datetime` support)
- **Nullable**: Any column can be marked nullable with NULL support

### Performance Optimizations
- **String Interning** - Memory-efficient storage for repeated strings
- **Incremental Updates** - Views update incrementally, not rebuild
- **TieredVector Storage** - Efficient for insert-heavy workloads

### Pythonic API
- **Indexing**: `table[0]`, `table[-1]` (negative indexing for last row)
- **Slicing**: `table[1:5]`, `table[:10]`, `table[-3:]`
- **Column Access**: `table["column_name"]` returns all values as a list
- **Iteration**: `for row in table:` works on tables and all view types
- **Comprehensions**: `[row["name"] for row in filtered]`
- **Bulk Insert**: `table.append_rows([...])` for efficient multi-row inserts

### Pandas Integration
- **Export**: `df = table.to_pandas()` - Convert any table/view to DataFrame
- **Import**: `table = Table.from_pandas("name", df)` - Create table from DataFrame
- Automatic type mapping between LiveTable and pandas dtypes

## Performance

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
- **Real-time sync** - WebSocket support for live dashboards

**When to use pandas instead:** Bulk vectorized operations (filtering large datasets, aggregations at scale) where numpy's optimized C code excels.

## Example

```python
import livetable
from datetime import date, datetime

# Create table with schema
schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    ("age", livetable.ColumnType.INT32, True),  # Nullable
    ("score", livetable.ColumnType.FLOAT64, False),
    ("enrolled", livetable.ColumnType.DATE, False),
])
table = livetable.Table("students", schema)

# Add data - single row
table.append_row({"id": 1, "name": "Alice", "age": 20, "score": 95.5, "enrolled": date(2024, 9, 1)})

# Bulk insert - many rows at once (more efficient)
table.append_rows([
    {"id": 2, "name": "Bob", "age": 22, "score": 87.3, "enrolled": date(2024, 9, 1)},
    {"id": 3, "name": "Charlie", "age": None, "score": 92.1, "enrolled": date(2024, 9, 15)},
])

# Pythonic access
first = table[0]              # First row
last = table[-1]              # Last row (negative indexing)
subset = table[0:2]           # Slicing - rows 0 and 1
names = table["name"]         # Column access - ["Alice", "Bob", "Charlie"]

# Iterate over rows
for row in table:
    print(f"{row['name']}: {row['score']}")

# List comprehensions work naturally
high_scores = [row["name"] for row in table if row["score"] >= 90]

# Filter with Python lambda (Rust speed!)
high_scorers = table.filter(lambda row: row["score"] >= 90)
print(f"Found {len(high_scorers)} high scorers")

# Expression-based filter (2x faster than lambda!)
indices = table.filter_expr("score >= 90 AND name != 'Test'")
# Supports: =, !=, <, >, <=, >=, AND, OR, NOT, IS NULL, IS NOT NULL

# Project specific columns
summary = table.select(["name", "score"])

# Add computed column
with_grade = table.add_computed_column(
    "grade",
    lambda row: "A" if row["score"] >= 90 else "B"
)

# Sort by score descending
sorted_view = livetable.SortedView(
    "by_score",
    table,
    [livetable.SortKey.descending("score")]
)
top_student = sorted_view[0]  # Highest scorer

# Join tables (single column)
joined = livetable.JoinView(
    "student_courses",
    students,
    enrollments,
    "id",           # Column in students
    "student_id",   # Column in enrollments
    livetable.JoinType.LEFT
)

# Multi-column join (composite keys)
sales_targets_joined = livetable.JoinView(
    "sales_vs_targets",
    sales,
    targets,
    ["year", "month", "region"],  # Left keys
    ["target_year", "target_month", "target_region"],  # Right keys
    livetable.JoinType.INNER
)

# Simple aggregations
total = table.sum("score")       # Sum of all scores
avg = table.avg("score")         # Average score
min_score = table.min("score")   # Minimum score
max_score = table.max("score")   # Maximum score

# GROUP BY aggregations
agg = livetable.AggregateView(
    "scores_by_age",
    table,
    ["age"],  # Group by age
    [
        ("total_score", "score", livetable.AggregateFunction.SUM),
        ("avg_score", "score", livetable.AggregateFunction.AVG),
        ("count", "score", livetable.AggregateFunction.COUNT),
    ]
)
for group in agg:
    print(group)  # {"age": 20, "total_score": 95.5, ...}

# Export to CSV/JSON
csv_string = table.to_csv()
json_string = table.to_json()

# Import from CSV/JSON (types auto-inferred)
from_csv = livetable.Table.from_csv("imported", csv_string)
from_json = livetable.Table.from_json("imported", json_string)

# Pandas integration
import pandas as pd
df = table.to_pandas()                          # Table -> DataFrame
new_table = livetable.Table.from_pandas("from_df", df)  # DataFrame -> Table

# Save to file
with open("data.csv", "w") as f:
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

# Install maturin (if needed)
pip install maturin

# Build and install
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release
pip install target/wheels/livetable-*.whl
```

## Testing

### Run All Tests (Recommended)
```bash
cd tests
./run_all.sh
```

This runs:
- Rust unit tests (61 tests)
- Python unit tests (226 tests)
- Integration tests (5 real-world workflows)

### Run Specific Test Suites

```bash
# Python tests only
cd tests
pytest                          # All Python tests
pytest python/                  # Unit tests only
pytest integration/             # Integration tests only
pytest -v                       # Verbose output

# Rust tests only
cd impl
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib

# Rust benchmarks
cd impl
cargo bench
```

## React Frontend

The `frontend/` directory hosts a React + TypeScript client that streams live table updates via WebSocket.

```bash
# In one terminal start the backend server
cd impl
cargo run --bin livetable-server --features server

# In another terminal (requires Node.js 18+)
cd frontend
npm install
npm run dev
```

By default the UI connects to `ws://127.0.0.1:8080/ws`. Override with `VITE_WS_URL=ws://host:port/ws npm run dev`.

## Why LiveTable?

- **Blazing Fast** - Rust performance with Python ease
- **Type Safe** - Compile-time safety, runtime validation
- **Zero Copy** - Views don't duplicate data
- **Pythonic** - Natural Python API with lambdas
- **Battle-Tested** - Comprehensive test suite
- **Well Documented** - Examples and guides

## Architecture

- **Language**: Rust
- **Python Bindings**: PyO3
- **Build System**: Maturin
- **Storage**: ArraySequence (contiguous) or TieredVector (efficient inserts)
- **Views**: Zero-copy, lazy evaluation
- **Type System**: Strongly typed with NULL support

## License

MIT
