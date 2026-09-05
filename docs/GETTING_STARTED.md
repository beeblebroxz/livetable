# Getting Started with LiveTable Python

LiveTable's Python package is a PyO3 extension built from the Rust core. Python
3.8 or newer, Rust, and a working C/Rust build toolchain are required.

## Install from this repository

From the repository root:

```bash
cd impl
./install.sh
```

The script builds a wheel with Maturin and installs it with `pip3`. For a manual
or virtual-environment build, see
[PYTHON_BINDINGS_README.md](PYTHON_BINDINGS_README.md#building-from-source).

Verify the import:

```bash
python3 -c "import livetable; print(livetable.ColumnType.INT32)"
```

## Create a typed table

```python
from datetime import date, datetime
import livetable

schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    ("age", livetable.ColumnType.INT32, True),
    ("joined", livetable.ColumnType.DATE, False),
    ("updated_at", livetable.ColumnType.DATETIME, True),
])

users = livetable.Table("users", schema)
users.append_row({
    "id": 1,
    "name": "Alice",
    "age": 30,
    "joined": date(2026, 8, 3),
    "updated_at": datetime(2026, 8, 3, 9, 30),
})
```

Every field is validated against its schema. Use `None` only for nullable
columns.

Storage uses native-width column buffers with packed NULL masks on both
backends. Python values and APIs are unchanged; see
[typed column storage](TYPED_COLUMN_STORAGE.md) for memory and update-cost details.

Supported column types are `INT32`, `INT64`, `FLOAT32`, `FLOAT64`, `STRING`,
`BOOL`, `DATE`, and `DATETIME`.

## Read and mutate rows

```python
users.append_rows([
    {"id": 2, "name": "Bob", "age": None, "joined": date.today(), "updated_at": None},
    {"id": 3, "name": "Carol", "age": 41, "joined": date.today(), "updated_at": None},
])

print(users[0])
print(users[-1])
print(users[0:2])
print(users["name"])

users.set_value(1, "age", 28)
users.insert_row(1, {
    "id": 4,
    "name": "Dan",
    "age": 22,
    "joined": date.today(),
    "updated_at": None,
})
users.delete_row(1)
```

## Build views

```python
adults = users.filter(lambda row: row["age"] is not None and row["age"] >= 18)
names = users.select(["name", "age"])
with_status = users.add_computed_column(
    "status",
    lambda row: "known" if row["age"] is not None else "unknown",
)
ranked = users.sort("age", descending=True)
```

For expressions that do not need a Python callback:

```python
indices = users.filter_expr("age IS NOT NULL AND age >= 18")
```

`filter_expr` returns base-row indices, while `filter` returns a `FilterView`.

## Join and aggregate

```python
orders_schema = livetable.Schema([
    ("order_id", livetable.ColumnType.INT32, False),
    ("user_id", livetable.ColumnType.INT32, False),
    ("amount", livetable.ColumnType.FLOAT64, False),
])
orders = livetable.Table("orders", orders_schema)

joined = users.join(orders, left_on="id", right_on="user_id", how="left")

by_user = orders.group_by("user_id", agg=[
    ("total", "amount", "sum"),
    ("average", "amount", "avg"),
    ("count", "amount", "count"),
    ("p95", "amount", "p95"),
])
```

Joins support LEFT, INNER, RIGHT, FULL, and composite keys. Grouping supports
SUM, COUNT, AVG, MIN, MAX, MEDIAN, and percentile operations.

## Propagate changes

Stateful views cache indices, sort keys, or aggregate state; they are not
detached snapshots. Filter/sort/join reads still reach into their live parents,
so reading before synchronization can mix stale indices with new source values.
Simplified methods register stateful views for `tick()`:

```python
large_orders = orders.filter(lambda row: row["amount"] >= 500)
ranked_orders = large_orders.sort("amount", descending=True)
totals = ranked_orders.group_by("user_id", agg=[("total", "amount", "sum")])

orders.append_row({"order_id": 10, "user_id": 1, "amount": 900.0})
orders.tick()
assert totals[0]["total"] == 900.0
```

Python supports `FilterView.sort()`, `FilterView.group_by()`, and
`SortedView.group_by()`; arbitrary view composition is available in Rust.
Registering happens in creation order, so this chain updates parent-first.
Filters and sorts replay up to 256 input events, retaining one batch of output.
Missing history, oversized batches, and explicit refresh use rebuild fallbacks.
Row movement can still cost O(N); see the [full contract](INCREMENTAL_SORTED_PIPELINE.md).

Explicit constructors normally require manual `sync()`/`refresh()`. Calling
`group_by()` on an explicit sort also registers that sort before the new child.
An explicitly constructed filter still needs manual synchronization even when
its child is registered; prefer `table.filter()` for fully automatic chains.
After a refresh without a root mutation, manually sync descendants because
`tick()` returns immediately when the root has no pending mutations.

## Import, export, and pandas

```python
csv_text = users.to_csv()
json_text = users.to_json()

from_csv = livetable.Table.from_csv("csv_users", csv_text)
from_json = livetable.Table.from_json("json_users", json_text)

frame = users.to_pandas()
from_frame = livetable.Table.from_pandas("frame_users", frame)
```

Pandas is optional and imported only when those methods are called.

## Next steps

```bash
# From the repository root
cd examples
python3 quickstart.py
python3 demo_reactive_propagation.py --tick
python3 playground.py
```

- [Python API reference](PYTHON_BINDINGS_README.md)
- [Incremental filter → sort → group](INCREMENTAL_SORTED_PIPELINE.md)
- [Join details](JOIN_FEATURE.md)
- [Orders Lab: interactive server/client tour](ORDERS_LAB.md)
- [WebSocket protocol](WEBSOCKET_PROTOCOL.md)
- [Testing](../tests/README.md)
