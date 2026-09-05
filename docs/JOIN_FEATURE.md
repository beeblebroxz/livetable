# Join Operations

LiveTable implements equality joins as read-only `JoinView` objects. Joins may
use a single key or a composite key and can synchronize after either parent
changes. Rust joins accept tables or other views; the Python join API currently
accepts root `Table` objects, not arbitrary derived parents.

## Supported join types

| Rust | Python `how=` | Result |
|------|---------------|--------|
| `JoinType::Left` | `"left"` | Every left row; unmatched right columns are NULL |
| `JoinType::Inner` | `"inner"` | Matching rows only |
| `JoinType::Right` | `"right"` | Every right row; unmatched left columns are NULL |
| `JoinType::Full` | `"full"`, `"outer"`, `"full_outer"` | Every row from both sides |

One-to-many and many-to-many matches emit one output row per matching pair.

## Python API

The simplified API is the usual entry point:

```python
# Same key name on both sides
joined = users.join(profiles, on="user_id")

# Different key names
joined = users.join(
    orders,
    left_on="id",
    right_on="user_id",
    how="left",
)

# Composite key
joined = sales.join(
    targets,
    left_on=["year", "month", "region"],
    right_on=["year", "month", "region"],
    how="full",
)
```

Specify either `on` or both `left_on` and `right_on`. Mixing the two forms,
omitting one side, using an unknown column, or providing different key counts
raises `ValueError`.

The explicit constructor is also available:

```python
joined = livetable.JoinView(
    "user_orders",
    users,
    orders,
    "id",
    "user_id",
    livetable.JoinType.LEFT,
)

composite = livetable.JoinView(
    "monthly",
    sales,
    targets,
    ["year", "month"],
    ["year", "month"],
    livetable.JoinType.INNER,
)
```

`JoinType.LEFT`, `INNER`, `RIGHT`, and `FULL` are all exposed to Python.

### Reading and synchronizing

```python
len(joined)
joined.is_empty()
joined.name()
row = joined[0]
row = joined.get_row(0)
value = joined.get_value(0, "right_amount")

changed = joined.sync()  # incremental when possible
joined.refresh()         # unconditional full rebuild
```

Join views support iteration, negative indices, and slicing through the common
Python view interface. Iterators are fail-fast: mutating a parent or syncing the
view with pending changes while iterating raises `RuntimeError` on the next
item. An unconditional `refresh()` also invalidates the iterator; a no-op
`sync()` does not.

Joins created through `table.join()` are registered with both root tables.
Calling `tick()` on the mutated root synchronizes the join automatically:

```python
joined = users.join(orders, left_on="id", right_on="user_id")
orders.append_row({"order_id": 102, "user_id": 2, "amount": 59.99})
orders.tick()
```

For explicit `JoinView(...)` construction, call `sync()`/`refresh()` yourself.

## Rust API

Single key:

```rust
let joined = JoinView::new(
    "user_orders".to_string(),
    users.clone(),
    orders.clone(),
    "id".to_string(),
    "user_id".to_string(),
    JoinType::Left,
)?;
```

Composite key:

```rust
let joined = JoinView::new_multi(
    "monthly".to_string(),
    sales.clone(),
    targets.clone(),
    vec!["year".to_string(), "month".to_string()],
    vec!["year".to_string(), "month".to_string()],
    JoinType::Full,
)?;
```

Both parents are `Rc<RefCell<dyn ReadableTable>>`, so a join may consume root
tables or derived views. The public read/sync surface includes:

```rust
joined.len();
joined.is_empty();
joined.name();
joined.join_type();
joined.get_row(index)?;
joined.get_value(index, "column")?;
joined.sync();
joined.refresh();
```

To use automatic Rust propagation, wrap each root in a `TickableTable` and
register the same `Rc<RefCell<JoinView>>` on both sides:

```rust
left_tickable.register_join_as_left(&joined);
right_tickable.register_join_as_right(&joined);
```

## Output columns

Left columns retain their original names. Every right column is prefixed with
`right_`, including the right join key:

```text
left:   id, name
right:  order_id, user_id, amount
output: id, name, right_order_id, right_user_id, right_amount
```

The prefix is unconditional, not only a collision workaround.

For unmatched rows:

- LEFT/FULL joins fill right-side columns with `ColumnValue::Null`.
- RIGHT/FULL joins fill left-side columns with `ColumnValue::Null`.

## Key semantics

- Key column counts must match and the corresponding column types must compare
  as the same `ColumnValue` variant.
- NULL keys never match.
- Float NaN keys never match.
- Float keys use their IEEE bit representation: `-0.0` and `0.0` are distinct
  join keys, unlike their tie/grouping behavior in sorts and aggregates.
- Composite keys are typed vectors, so embedded null bytes in strings cannot
  collide with key separators.

## Incremental behavior

`JoinView` maintains a cached output index. `sync()` consumes pending changes
from both parents and handles inserts, deletes, and key updates. Tables and
synchronized filters/sorts expose history that Rust joins can replay. If either
parent exposes no changeset, the join instead uses version-checked rebuilds.
Missing history, structural changes on both sides in one batch, or a key update
before a structural change on the same side also require a rebuild to avoid
mixing row-coordinate frames. Always synchronize parents first; `refresh()`
unconditionally rebuilds from their current state. Joins themselves do not
publish output changesets, so children use version-checked rebuilds.

Construction and a full refresh are O(N + M + R), where `N` and `M` are parent
sizes and `R` is the output size. Output can be much larger than either parent
for many-to-many keys.

## Current limitations

- Equality joins only; there are no range, expression, or as-of joins.
- No cross join helper.
- No join planner, persistence, spilling, or parallel execution.
- Right-side output names always use `right_`; callers cannot customize the
  prefix.
- The implementation is single-threaded because views use `Rc<RefCell<...>>`.

Self joins are possible when the caller supplies compatible handles, but they
receive no special optimization.

## Verification

```bash
cd impl
cargo test --lib view::tests::test_inner_join
cargo test --lib view::tests::test_right_join
cargo test --lib view::tests::test_full_join
cargo test --test forward_prop_fuzz differential_join_fuzz
cargo run --example joins

cd ../tests
pytest -c pytest.ini python/test_right_full_joins.py python/test_multi_column_join.py
```

See the [Rust API guide](API_GUIDE.md), the
[Rust join example](../impl/examples/joins.rs), and the
[implementation](../impl/src/view/join.rs).
