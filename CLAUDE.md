# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LiveTable is a high-performance columnar table system written in Rust with Python bindings via PyO3. It provides 10-100x faster operations compared to pure Python implementations through zero-copy views and lazy evaluation.

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

## Documentation Maintenance

**IMPORTANT**: When adding new features or making API changes, always update:

1. **README.md** - Features list and Example section
2. **CLAUDE.md** - Python API Usage section (this file)
3. **docs/PYTHON_BINDINGS_README.md** - Full API reference and examples
4. **docs/ORIGINAL_VISION.md** - Mark implemented features as complete

**Checklist for new features:**
- [ ] Add to README.md Features section
- [ ] Add example code to README.md Example section
- [ ] Add to CLAUDE.md Python API Usage
- [ ] Add full documentation to docs/PYTHON_BINDINGS_README.md
- [ ] Add API reference (constructor, methods, parameters)
- [ ] Update docs/ORIGINAL_VISION.md Implementation Status
- [ ] Update Future Enhancements if applicable
- [ ] Add Python tests in tests/python/


## Build Commands

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
# Run all tests (Rust + Python + Integration)
cd tests && ./run_all.sh

# Rust tests only
cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib

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
cd impl && cargo run --bin livetable-server --features server

# Then start frontend (requires Node.js 18+)
cd frontend && npm install && npm run dev
```

## Architecture

### Layered Design (impl/src/)
- **sequence.rs** - Storage backends: `ArraySequence` (contiguous, default), `TieredVectorSequence` (O(√N) inserts, backed by [tiered-vector](https://crates.io/crates/tiered-vector) crate)
- **column.rs** - Strongly-typed column values with NULL support (INT32, INT64, FLOAT32, FLOAT64, STRING, BOOL, DATE, DATETIME)
- **table.rs** - Row-level CRUD operations on column collections
- **view.rs** - Zero-copy views: `FilterView`, `ProjectionView`, `ComputedView`, `JoinView`, `SortedView`
- **python_bindings.rs** - PyO3 bindings exposing Rust types as Python classes
- **websocket.rs** + **messages.rs** - Real-time sync via Actix-web WebSocket server

### Key Patterns
- Views use `Rc<RefCell<>>` for shared table access without data duplication
- Python lambdas are converted to Rust closures for filter/computed operations
- Join operations use O(N+M) algorithm
- WebSocket protocol: `UpdateCell`, `AddRow`, `DeleteRow` messages with broadcast to all clients

### Frontend (frontend/src/)
- React 18 + TypeScript + Vite + Tailwind CSS
- `hooks/useTableWebSocket.ts` - WebSocket connection management
- `components/LiveTable.tsx` - Real-time table rendering with TanStack Table

## Critical Notes

1. **Always use ABI3 forward compatibility** when building Python bindings:
   ```bash
   env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release
   ```

2. **Python package must be installed** before running Python tests - if imports fail, run `cd impl && ./install.sh`

3. **Port usage**: Backend WebSocket server runs on port 8080, frontend dev server on port 5173

## Python API Usage

```python
import livetable

# Create schema and table
schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),      # (name, type, nullable)
    ("name", livetable.ColumnType.STRING, False),
    ("score", livetable.ColumnType.FLOAT64, True),
])
table = livetable.Table("students", schema)

# Storage hints - choose backend optimized for your workload
# Default: "fast_reads" (ArraySequence) - O(1) access, O(N) insert/delete
# Option: "fast_updates" (TieredVectorSequence) - O(1) access, O(√N) insert/delete
orderbook = livetable.Table("orderbook", schema, storage="fast_updates")

# CRUD operations
table.append_row({"id": 1, "name": "Alice", "score": 95.5})
table.set_value(0, "score", 97.0)
row = table[0]           # Indexing: table[idx]
row = table.get_row(0)   # Same as above
table.delete_row(0)

# Views (zero-copy)
filtered = table.filter(lambda row: row["score"] >= 90)
projected = table.select(["name", "score"])
computed = table.add_computed_column("grade", lambda row: "A" if row["score"] >= 90 else "B")

# Expression-based filtering (2x faster than lambda)
indices = table.filter_expr("score >= 90 AND name != 'Test'")
# Supports: =, !=, <, >, <=, >=, AND, OR, NOT, IS NULL, IS NOT NULL

# Sorting (simplified API)
sorted_table = table.sort("score")                    # Ascending (default)
sorted_table = table.sort("score", descending=True)   # Descending
sorted_table = table.sort(["score", "name"], descending=[True, False])  # Multi-column

# Sorting (explicit SortedView constructor)
sorted_view = livetable.SortedView("by_score", table, [livetable.SortKey.descending("score")])

# Joins (simplified API)
joined = students.join(grades, on="id")                                    # Same column name
joined = students.join(enrollments, left_on="id", right_on="student_id")   # Different names
joined = students.join(enrollments, left_on="id", right_on="student_id", how="inner")
joined = sales.join(targets, on=["year", "month"])                         # Multi-column

# Joins (explicit JoinView constructor)
joined = livetable.JoinView("result", table1, table2, "id", "ref_id", livetable.JoinType.LEFT)
joined = livetable.JoinView("result", sales, targets,
    ["year", "month", "region"],  # Left keys
    ["target_year", "target_month", "target_region"],  # Right keys
    livetable.JoinType.INNER)

# Simple aggregations
total = table.sum("score")
avg = table.avg("score")
min_val = table.min("score")
max_val = table.max("score")
count = table.count_non_null("score")

# GROUP BY (simplified API)
grouped = table.group_by("department", agg=[
    ("total", "salary", "sum"),
    ("average", "salary", "avg"),      # Aliases: avg, average, mean
    ("headcount", "salary", "count"),
    ("min_sal", "salary", "min"),
    ("max_sal", "salary", "max"),
])

# Percentile / Median aggregations
stats = table.group_by("month", agg=[
    ("median_tokens", "tokens_used", "median"),
    ("p95_tokens", "tokens_used", "p95"),
    ("p99_latency", "latency_ms", "p99"),
    ("p25_tokens", "tokens_used", "percentile(0.25)"),  # Explicit percentile
])
# Shorthand strings: median, p25, p50, p75, p90, p95, p99, percentile(X.XX)

# GROUP BY (explicit AggregateView constructor)
agg = livetable.AggregateView("by_name", table, ["name"], [
    ("total", "score", livetable.AggregateFunction.SUM),
    ("average", "score", livetable.AggregateFunction.AVG),
    ("p95", "score", livetable.AggregateFunction.PERCENTILE(0.95)),
    ("median", "score", livetable.AggregateFunction.MEDIAN),
])
agg.sync()  # Incremental update after table changes

# Automatic view propagation with tick()
# Views created via simplified API (filter, sort, group_by) are auto-registered
filtered = table.filter(lambda row: row["amount"] > 500)
sorted_view = table.sort("amount", descending=True)
grouped = table.group_by("region", agg=[("total", "amount", "sum")])

# After table changes, call tick() to update all views at once
table.append_row({"region": "South", "amount": 1500})
table.tick()  # All registered views are now updated

# Check how many views are registered
count = table.registered_view_count()  # Returns 3

# Serialization - export
csv_string = table.to_csv()
json_string = table.to_json()

# Serialization - import (types are auto-inferred)
table_from_csv = livetable.Table.from_csv("imported", csv_string)
table_from_json = livetable.Table.from_json("imported", json_string)

# Iterator protocol - works with all tables and views
for row in table:
    print(f"{row['name']}: {row['score']}")

# Use with comprehensions and built-ins
names = [row["name"] for row in filtered]
total = sum(row["score"] for row in table)

# Bulk operations - insert many rows efficiently
rows = [
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87.0},
]
count = table.append_rows(rows)  # Returns number inserted

# Date and DateTime columns
from datetime import date, datetime

date_schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("birth_date", livetable.ColumnType.DATE, False),
    ("created_at", livetable.ColumnType.DATETIME, True),
])
events = livetable.Table("events", date_schema)
events.append_row({
    "id": 1,
    "birth_date": date(1990, 5, 15),
    "created_at": datetime(2024, 1, 15, 10, 30, 0),
})

# Dates serialize as ISO 8601 strings in CSV/JSON
csv = events.to_csv()  # birth_date becomes "1990-05-15"

# Pandas interop
import pandas as pd
df = table.to_pandas()  # Table → DataFrame
table = livetable.Table.from_pandas("name", df)  # DataFrame → Table
```
