# LiveTable Rust API Guide

This guide describes the public Rust API in `impl/src`. For the PyO3 API, see
[PYTHON_BINDINGS_README.md](PYTHON_BINDINGS_README.md). For the optional Actix
server, see [WEBSOCKET_PROTOCOL.md](WEBSOCKET_PROTOCOL.md).

LiveTable is currently version `0.1.0`. Treat the API as alpha: the implemented
surface is tested, but compatibility is not yet guaranteed across releases.

## Add the crate

From another local Rust project:

```toml
[dependencies]
livetable = { path = "path/to/livetable/impl" }
```

The core library has no default features. Enable `server` for the Actix server
modules or `python` when building the extension through Maturin.

## Basic table usage

```rust
use livetable::{ColumnType, ColumnValue, Schema, Table};
use std::collections::HashMap;

fn main() -> Result<(), String> {
let schema = Schema::new(vec![
    ("id".to_string(), ColumnType::Int32, false),
    ("name".to_string(), ColumnType::String, false),
    ("score".to_string(), ColumnType::Float64, true),
]);
let mut table = Table::new("students".to_string(), schema);

let mut row = HashMap::new();
row.insert("id".to_string(), ColumnValue::Int32(1));
row.insert(
    "name".to_string(),
    ColumnValue::String("Alice".to_string()),
);
row.insert("score".to_string(), ColumnValue::Float64(95.5));
table.append_row(row)?;

assert_eq!(table.len(), 1);
assert_eq!(table.get_value(0, "name")?.as_string(), Some("Alice"));
Ok(())
}
```

Rows are `HashMap<String, ColumnValue>`. A mutation is schema-validated before
it is committed; missing non-nullable columns, unknown columns, and incompatible
values return an error.

## Types and schemas

`ColumnType` supports:

| Variant | Stored value |
|---------|--------------|
| `Int32` | `i32` |
| `Int64` | `i64` |
| `Float32` | `f32` |
| `Float64` | `f64` |
| `String` | UTF-8 `String` |
| `Bool` | `bool` |
| `Date` | days since 1970-01-01 |
| `DateTime` | milliseconds since 1970-01-01 |

Nullability belongs to the schema. `ColumnValue::Null` is the value used for a
nullable cell; it is not a separate column type.

Useful `Schema` methods:

```rust
schema.len();
schema.is_empty();
schema.get_column_names();
schema.get_column_index("score");
schema.get_column_info(2);
schema.get_column_type("score");
schema.is_column_nullable("score");
```

## Storage backends

The default `Table::new` uses `StorageHint::FastReads` (`ArraySequence`). Use a
tiered vector for frequent middle inserts/deletes:

```rust
use livetable::{StorageHint, Table};

let table = Table::with_hint(
    "orderbook".to_string(),
    schema,
    StorageHint::FastUpdates,
);
```

| Hint | Backend | Random access | Middle insert/delete |
|------|---------|---------------|----------------------|
| `FastReads` | `ArraySequence` | O(1) | O(N) |
| `FastUpdates` | `TieredVectorSequence` | O(1) | O(√N) |

`Table::with_hint_and_interning` additionally enables string interning. It is
most useful when string values repeat frequently.

## Table API

Important query and mutation methods:

```rust
table.len();
table.is_empty();
table.name();
table.schema();
table.get_row(index)?;
table.get_value(index, "column")?;
table.set_value(index, "column", value)?;
table.append_row(row)?;
table.append_rows(rows)?;
table.insert_row(index, row)?;
table.delete_row(index)?;
```

The table also provides numeric reductions:

```rust
table.sum("score")?;
table.count_non_null("score")?;
table.avg("score")?;
table.min("score")?;
table.max("score")?;
```

`filter_expr` evaluates a SQL-like expression in Rust and returns matching base
row indices:

```rust
let indices = table.filter_expr("score >= 90 AND name != 'Test'")?;
```

Supported expression features are `=`, `!=`, `<`, `>`, `<=`, `>=`, `AND`,
`OR`, `NOT`, parentheses, `IS NULL`, and `IS NOT NULL`. Comparisons with NULL
are false.

## Serialization

```rust
let csv = table.to_csv();
let json = table.to_json()?;

let csv_table = Table::from_csv("csv" , &csv)?;
let json_table = Table::from_json("json", &json)?;
```

Imports infer each column by scanning all rows. Numeric types widen as needed;
dates widen to datetimes; all-null columns default to string. JSON rejects
incompatible mixed types, while CSV may fall back to string.

## Views and composition

Every table and view implements `ReadableTable`. A view parent is therefore an
`Rc<RefCell<dyn ReadableTable>>`, which permits view-over-view DAGs without
copying the source rows. The following focused fragments use `?` and assume a
surrounding function that returns `Result<(), String>`.

### Filter, projection, and computed views

```rust
use livetable::{ColumnValue, ComputedView, FilterView, ProjectionView, Table};
use std::{cell::RefCell, rc::Rc};

let table = Rc::new(RefCell::new(table));

let filtered = Rc::new(RefCell::new(FilterView::new(
    "passing".to_string(),
    table.clone(),
    |row| row.get("score").and_then(ColumnValue::as_f64).unwrap_or(0.0) >= 90.0,
)));

let projected = ProjectionView::new(
    "names".to_string(),
    filtered.clone(),
    vec!["name".to_string(), "score".to_string()],
)?;

let computed = ComputedView::new(
    "graded".to_string(),
    filtered.clone(),
    "passed".to_string(),
    |_| ColumnValue::Bool(true),
);
```

`ProjectionView` and `ComputedView` read through to their parent and need no
sync. `FilterView` tracks an index and offers `sync()` for incremental updates
or `refresh()` for a full rebuild.

### Sorting

```rust
use livetable::{SortKey, SortedView};

let sorted = Rc::new(RefCell::new(SortedView::new(
    "ranked".to_string(),
    filtered.clone(),
    vec![SortKey::descending("score"), SortKey::ascending("name")],
)?));
```

`SortKey::new(column, order, nulls_first)` provides explicit null ordering.
`SortedView::sync()` updates the sorted index; `refresh()` rebuilds it.

### Aggregation

```rust
use livetable::{AggregateFunction, AggregateView};

let grouped = Rc::new(RefCell::new(AggregateView::new(
    "by_department".to_string(),
    table.clone(),
    vec!["department".to_string()],
    vec![
        ("total".to_string(), "salary".to_string(), AggregateFunction::Sum),
        ("count".to_string(), "salary".to_string(), AggregateFunction::Count),
        (
            "p95".to_string(),
            "salary".to_string(),
            AggregateFunction::Percentile(0.95),
        ),
    ],
)?));
```

Supported functions are `Sum`, `Count`, `Avg`, `Min`, `Max`, `Median`, and
`Percentile(f64)`. Count is non-null count of its source column. NaN aggregate
values are excluded like NULL; NaN group keys form one canonical group and
`-0.0` groups with `0.0`.

### Joins

```rust
use livetable::{JoinType, JoinView};

let joined = Rc::new(RefCell::new(JoinView::new(
    "user_orders".to_string(),
    users.clone(),
    orders.clone(),
    "id".to_string(),
    "user_id".to_string(),
    JoinType::Left,
)?));

let composite = JoinView::new_multi(
    "monthly_targets".to_string(),
    sales.clone(),
    targets.clone(),
    vec!["year".to_string(), "month".to_string()],
    vec!["year".to_string(), "month".to_string()],
    JoinType::Full,
)?;
```

LiveTable implements `Left`, `Inner`, `Right`, and `Full` joins. NULL and NaN
keys do not match. Right-side output columns are prefixed with `right_`.
`JoinView::sync()` incrementally consumes changes from both parents;
`refresh()` performs a complete rebuild. See [JOIN_FEATURE.md](JOIN_FEATURE.md)
for details.

## Automatic propagation with `TickableTable`

Register stateful views in topological order, mutate the root, then call
`tick()`. It synchronizes registered views before compacting the root
changeset.

```rust
use livetable::TickableTable;

let tickable = TickableTable::new(table.clone());
tickable.register_filter(&filtered);
tickable.register_sorted(&sorted);
tickable.register_aggregate(&grouped);

table.borrow_mut().append_row(new_row)?;
let live_views = tickable.tick();
```

For joins, register the same `Rc<RefCell<JoinView>>` with
`register_join_as_left` on the left root and `register_join_as_right` on the
right root. Registration uses weak references, so dropped views are pruned on a
later tick.

The incremental fast path is optimized for one mutation per tick. A stateful
view falls back to a full rebuild when a multi-change batch cannot be replayed
safely.

## Changesets and versions

Root mutations append `TableChange` entries and increment `Table::version()`.
`Changeset::total_len()` is an absolute monotonic change count even after
compaction. Stateful views keep their own cursor; composed views include parent
versions so staleness flows through the DAG.

Prefer `TickableTable::tick()` over manually clearing a changeset. Clearing or
compacting changes before registered views synchronize can force a rebuild or
lose the incremental path.

## Optional server API

With the `server` feature, the crate exports:

- `messages`: protocol-v2 wire types.
- `pipeline_spec`: bounded wire-spec validation and view construction.
- `engine`: the single-threaded table/pipeline owner.
- `websocket`: the Actix actor transport.
- `server`: HTTP/WebSocket server construction.

Run it with:

```bash
cd impl
cargo run --bin livetable-server --features server
```

The server is an in-memory demo service, not a persistence or authentication
layer. See [WEBSOCKET_PROTOCOL.md](WEBSOCKET_PROTOCOL.md).

## Verification

From the repository root:

```bash
cd impl
cargo test --lib
cargo test --lib --features server
cargo test --test forward_prop_fuzz
cargo test --features server --test protocol_v2_websocket
cargo doc --no-deps
```

Python builds should additionally set `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`
as documented in [PYTHON_BINDINGS_README.md](PYTHON_BINDINGS_README.md).
