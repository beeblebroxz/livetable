# Python Chaining over Joins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `JoinView.filter()/sort()/group_by()` in Python, kept current by
`tick()` on either joined table.

**Architecture:** The Python filter's state holds any `ReadableTable` parent
instead of a root table. Python views that can chain carry the root tables
their chain depends on and register once in each root's tick registry, after
their parent. An explicit `JoinView` registers itself on first chaining.

**Tech Stack:** Rust + PyO3 (`impl/src/python_bindings.rs` and its
`include!`d files), maturin wheel, pytest.

**Spec:** `docs/superpowers/specs/2026-09-27-python-join-chaining-design.md`

## Global Constraints

- Table filters behave exactly as before: same errors, cursors, names
  (`<table>_filtered_sorted`, `<table>_filtered_grouped`), and iteration guard.
- A chained view registers once per distinct registry, after its parent.
- Derived parents never constrain root compaction (`root_changeset_cursor`
  returns `usize::MAX` for views).
- Filter reads: out-of-range → `IndexError`; unknown column → `KeyError`.
- No new explicit-constructor parents; no `joined.join(...)`.
- Rust checks: clippy `-D warnings` for default, `--features server`, and
  `--features python` (with `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`).
- Build the local wheel with the macOS 26.5 SDK (never commit this path):

  ```bash
  SP=/private/tmp/claude-501/-Users-abhishekgulati-projects-livetable/4aed5fa4-cdd7-41c5-8e60-ae744611897b/scratchpad
  cd impl && env SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk \
    CARGO_TARGET_DIR=$SP/target-ld26 PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 \
    $SP/venv/bin/maturin build --release -o $SP/wheels-chain \
    && $SP/venv/bin/pip install -q --force-reinstall $SP/wheels-chain/livetable-*.whl
  ```
  Python tests run as `cd tests && $SP/venv/bin/python -m pytest <path> -q -p no:cacheprovider`.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- A filter over a join whose join is stale (built, then a table mutated, then
  the filter synced by hand before the join): it must refresh, not replay.
  → Task 1 `test_filter_over_a_stale_join_refreshes`.
- Retrying after a predicate exception inside `tick()`: the second tick must
  finish the replay without losing the change.
  → Task 1 `test_failing_predicate_is_retryable`.
- A chained call that fails validation on an explicit join must not register
  anything. → Task 2 `test_explicit_join_registers_itself_once`.
- Intermediate Python objects dropped mid-chain (`joined.filter(p).sort(...)`
  keeps only the sort): the dropped filter must keep updating.
  → Task 2 `test_two_level_chains_over_a_join`.
- A table filter's chained sort still registers on its one table.
  → existing `tests/python/test_view_composition.py` (run in Task 1).

---

## File Structure

- Modify `impl/src/python_bindings.rs`: `register_on_roots`, `PyFilterViewInner`
  (any parent), `PyFilterView` (roots/name, reads), `PyTable::{filter, sort,
  join, tick}`, `PyJoinView` (tables, `ensure_registered`, `filter/sort/group_by`),
  `PySortedView` (roots).
- Modify `impl/src/python_bindings/conversions.rs`: `row_to_py`.
- Modify `impl/src/python_bindings/iterators.rs`: filter iterator guard.
- Create `tests/python/test_join_chaining.py`.
- Docs in Task 3.

---

### Task 1: Python filter over any parent, and `JoinView.filter()`

**Files:**
- Modify: `impl/src/python_bindings.rs`, `impl/src/python_bindings/conversions.rs`, `impl/src/python_bindings/iterators.rs`
- Create: `tests/python/test_join_chaining.py`

**Interfaces:**
- Produces: `fn register_on_roots(roots: &[PyTable], entry: &RegisteredView, present: impl Fn(&RegisteredView) -> bool)`;
  `PyFilterView::over(parent: Rc<RefCell<dyn ReadableTable>>, name: String, roots: Vec<PyTable>, predicate: PyObject) -> PyResult<PyFilterView>`;
  `PyJoinView { inner, left: PyTable, right: PyTable }` with `fn roots(&self) -> Vec<PyTable>` and `fn ensure_registered(&self)`;
  `PySortedView { inner, roots: Vec<PyTable> }`;
  `fn row_to_py(py: Python, row: &HashMap<String, RustColumnValue>) -> PyResult<PyObject>`;
  Python `JoinView.filter(predicate) -> FilterView`.

- [ ] **Step 1: Write the failing tests** — create `tests/python/test_join_chaining.py`:

```python
"""Python chaining over joins: filter/sort/group_by on a JoinView, kept current
by tick() on either joined table. See
docs/superpowers/specs/2026-09-27-python-join-chaining-design.md."""
import gc

import pytest

import livetable


def make_tables():
    """Orders (oid, cust, amount) and customers (cid, name, tier).

    Order 4's customer (9) does not exist, so LEFT joins keep it unmatched.
    """
    customers = livetable.Table("customers", livetable.Schema([
        ("cid", livetable.ColumnType.INT32, False),
        ("name", livetable.ColumnType.STRING, False),
        ("tier", livetable.ColumnType.INT32, False),
    ]))
    for cid, name, tier in [(1, "Ada", 1), (2, "Bo", 2), (3, "Cy", 1)]:
        customers.append_row({"cid": cid, "name": name, "tier": tier})
    orders = livetable.Table("orders", livetable.Schema([
        ("oid", livetable.ColumnType.INT32, False),
        ("cust", livetable.ColumnType.INT32, False),
        ("amount", livetable.ColumnType.FLOAT64, False),
    ]))
    for oid, cust, amount in [(1, 1, 10.0), (2, 2, 60.0), (3, 1, 75.0), (4, 9, 90.0), (5, 3, 20.0)]:
        orders.append_row({"oid": oid, "cust": cust, "amount": amount})
    return orders, customers


def join(orders, customers, how="left"):
    return orders.join(customers, left_on="cust", right_on="cid", how=how)


def oracle_rows(orders, customers):
    """A from-scratch LEFT join. Explicit joins are not tick-registered."""
    fresh = livetable.JoinView(
        "oracle", orders, customers, "cust", "cid", livetable.JoinType.LEFT
    )
    return list(fresh)


def canon(rows):
    """Order-free comparison: join output order may differ from a rebuild."""
    return sorted(repr(sorted(row.items())) for row in rows)


def big(row):
    return row["amount"] >= 50.0


def tier_one(row):
    return row["right_tier"] == 1


def test_filter_over_join_updates_on_left_tick():
    orders, customers = make_tables()
    rich = join(orders, customers).filter(big)
    orders.append_row({"oid": 6, "cust": 2, "amount": 55.0})
    orders.set_value(0, "amount", 99.0)
    orders.tick()
    assert canon(rich) == canon(r for r in oracle_rows(orders, customers) if big(r))
    assert len(rich) == 5


def test_filter_over_join_updates_on_right_only_tick():
    orders, customers = make_tables()
    tier1 = join(orders, customers).filter(tier_one)
    customers.set_value(1, "tier", 1)  # Bo moves to tier 1
    customers.tick()
    assert canon(tier1) == canon(r for r in oracle_rows(orders, customers) if tier_one(r))
    assert sorted(row["oid"] for row in tier1) == [1, 2, 3, 5]


def test_predicate_sees_right_columns_and_none_when_unmatched():
    orders, customers = make_tables()
    seen = []
    join(orders, customers).filter(lambda row: seen.append(row) or True)
    assert [row for row in seen if row["oid"] == 4] == [{
        "oid": 4, "cust": 9, "amount": 90.0,
        "right_cid": None, "right_name": None, "right_tier": None,
    }]
    assert {row["right_name"] for row in seen if row["oid"] != 4} == {"Ada", "Bo", "Cy"}


def test_filter_reads_keep_error_types():
    orders, customers = make_tables()
    rich = join(orders, customers).filter(big)
    with pytest.raises(IndexError):
        rich[len(rich)]
    with pytest.raises(IndexError):
        rich.get_row(len(rich))
    with pytest.raises(KeyError):
        rich.get_value(0, "missing")
    assert rich.get_value(0, "right_name") == rich[0]["right_name"]


def test_iteration_raises_when_either_joined_table_mutates():
    orders, customers = make_tables()
    rich = join(orders, customers).filter(big)
    with pytest.raises(RuntimeError):
        for _ in rich:
            customers.set_value(0, "name", "Ada2")


def test_failing_predicate_is_retryable():
    orders, customers = make_tables()
    broken = {"on": False}

    def predicate(row):
        if broken["on"]:
            raise ValueError("boom")
        return big(row)

    rich = join(orders, customers).filter(predicate)
    broken["on"] = True
    orders.set_value(0, "amount", 80.0)
    with pytest.raises(ValueError):
        orders.tick()
    broken["on"] = False
    orders.tick()
    assert canon(rich) == canon(r for r in oracle_rows(orders, customers) if big(r))


def test_predicate_mutating_the_right_table_raises():
    orders, customers = make_tables()
    armed = {"on": False}

    def predicate(row):
        if armed["on"]:
            customers.append_row({"cid": 7, "name": "Zed", "tier": 3})
        return big(row)

    join(orders, customers).filter(predicate)
    armed["on"] = True
    orders.set_value(0, "amount", 80.0)
    with pytest.raises(RuntimeError):
        orders.tick()


def test_ticks_compact_both_tables():
    orders, customers = make_tables()
    rich = join(orders, customers).filter(big)
    orders.set_value(1, "amount", 5.0)
    customers.set_value(0, "name", "Ada2")
    orders.tick()
    customers.tick()
    # A join-coordinate cursor treated as a root cursor would hold these back.
    assert orders.pending_changes_count() == 0
    assert customers.pending_changes_count() == 0
    assert canon(rich) == canon(r for r in oracle_rows(orders, customers) if big(r))


def test_no_output_join_edit_keeps_filter_history():
    orders, customers = make_tables()
    joined = join(orders, customers, how="inner")
    rich = joined.filter(big)
    ranked = rich.sort("amount")
    orders.set_value(3, "amount", 91.0)  # order 4 has no customer: no INNER output
    assert joined.sync() is False
    assert rich.sync() is False
    # Without the recorded parent version the sort would refresh and return True.
    assert ranked.sync() is False


def test_filter_over_a_stale_join_refreshes():
    orders, customers = make_tables()
    joined = join(orders, customers)
    rich = joined.filter(big)
    orders.append_row({"oid": 6, "cust": 2, "amount": 65.0})
    # The join is stale (no history): a version-checked refresh, not a replay.
    assert rich.sync() is True
    assert 6 not in [row["oid"] for row in rich], "the join has not seen the insert"
    joined.sync()
    # The stale refresh left no cursor, so the filter rebaselines.
    assert rich.sync() is True
    assert canon(rich) == canon(r for r in oracle_rows(orders, customers) if big(r))
```

- [ ] **Step 2: Build the wheel from HEAD and run the new tests (RED)**

Run the wheel build from Global Constraints, then
`cd tests && $SP/venv/bin/python -m pytest python/test_join_chaining.py -q -p no:cacheprovider`.
Expected: every test FAILS with an `AttributeError` for the missing `filter` attribute on `JoinView`.

- [ ] **Step 3: Add `row_to_py` to `impl/src/python_bindings/conversions.rs`** (after `column_value_to_py`):

```rust
/// A row as a Python dict.
fn row_to_py(py: Python, row: &HashMap<String, RustColumnValue>) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    for (key, value) in row {
        dict.set_item(key, column_value_to_py(py, value)?)?;
    }
    Ok(dict.to_object(py))
}
```

- [ ] **Step 4: Add `register_on_roots` after `impl RegisteredView`** in `python_bindings.rs`:

```rust
/// Push `entry` once into each distinct registry among `roots` (a self-join
/// lists one table twice), skipping a registry where `present` already
/// matches. Call after the view's parent is registered, so tick() syncs
/// parents before children.
fn register_on_roots(
    roots: &[PyTable],
    entry: &RegisteredView,
    present: impl Fn(&RegisteredView) -> bool,
) {
    let mut seen: Vec<&Rc<RefCell<Vec<RegisteredView>>>> = Vec::new();
    for root in roots {
        let registry = &root.registered_views;
        if seen.iter().any(|done| Rc::ptr_eq(done, registry)) {
            continue;
        }
        seen.push(registry);
        let mut views = registry.borrow_mut();
        if !views.iter().any(&present) {
            views.push(entry.clone());
        }
    }
}
```

- [ ] **Step 5: Generalize `PyFilterViewInner`**

Replace the struct's `table_inner: Rc<RefCell<RustTable>>,` field with:

```rust
    /// The filtered parent: a root table or a view such as a join.
    parent: Rc<RefCell<dyn crate::readable::ReadableTable>>,
```

Replace `refresh`, `check_parent_version`, and `sync` in `impl PyFilterViewInner` with:

```rust
    /// Rebuild atomically: callback failures preserve the previous index and
    /// output history, allowing callers to correct the predicate and retry.
    fn refresh(&mut self, py: Python) -> PyResult<()> {
        let (rows, cursor, version) = {
            let parent = self.parent.borrow();
            let rows = (0..parent.len())
                .map(|i| parent.get_row(i))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PyValueError::new_err)?;
            let cursor = parent
                .changeset()
                .map(|cs| (cs.generation(), cs.total_len()));
            (rows, cursor, parent.version())
        };
        let mut indices = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            if self.evaluate(py, row)? {
                indices.push(index);
            }
            self.check_parent_version(version)?;
        }
        self.check_parent_version(version)?;
        self.indices = indices;
        self.output_changes.invalidate();
        // A parent without history (a stale join) gives no cursor: the next
        // sync refreshes once the parent has synchronized.
        match cursor {
            Some((generation, total)) => {
                self.last_synced_generation = generation;
                self.last_processed_change_count = total;
            }
            None => self.last_processed_change_count = usize::MAX,
        }
        self.last_parent_version = version;
        self.sync_count += 1;
        Ok(())
    }

    fn check_parent_version(&self, expected: u64) -> PyResult<()> {
        if self.parent.borrow().version() != expected {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Table mutated during filter predicate — predicates must not mutate the parent",
            ));
        }
        Ok(())
    }

    fn sync(&mut self, py: Python) -> PyResult<bool> {
        use crate::filter_changes::{
            apply_filter_changes, prepare_filter_changes, MAX_FILTER_REPLAY_CHANGES,
        };
        let parent = self.parent.borrow();
        let Some(changeset) = parent.changeset() else {
            // A view parent without coherent history: version-checked refresh.
            let stale = parent.version() != self.last_parent_version;
            drop(parent);
            if !stale {
                return Ok(false);
            }
            self.refresh(py)?;
            return Ok(true);
        };
        let changes = match changeset.changes_from(self.last_processed_change_count) {
            Some(changes) => changes,
            None => {
                drop(parent);
                self.refresh(py)?;
                return Ok(true);
            }
        };
        if changes.is_empty() {
            // A view parent can advance its version without emitting rows (a
            // join edit with no output). Record it so this filter's history
            // stays visible to its children.
            self.last_parent_version = parent.version();
            return Ok(false);
        }
        if changes.len() > MAX_FILTER_REPLAY_CHANGES {
            drop(parent);
            self.refresh(py)?;
            return Ok(true);
        }
        let changes = changes.to_vec();
        let generation = changeset.generation();
        let change_count = changeset.total_len();
        let version = parent.version();
        drop(parent);

        let prepared = prepare_filter_changes(
            &changes,
            |index| {
                self.parent
                    .borrow()
                    .get_row(index)
                    .map_err(PyValueError::new_err)
            },
            |row| {
                let matched = self.evaluate(py, row)?;
                self.check_parent_version(version)?;
                Ok(matched)
            },
        )?;
        self.check_parent_version(version)?;
        let modified = apply_filter_changes(&mut self.indices, &mut self.output_changes, prepared);
        self.last_processed_change_count = change_count;
        self.last_synced_generation = generation;
        self.last_parent_version = version;
        self.sync_count += 1;
        Ok(modified)
    }

    /// This filter's consumed position translated for root compaction: the
    /// cursor itself over a table, `usize::MAX` over a view.
    fn root_cursor(&self) -> usize {
        self.parent
            .borrow()
            .root_changeset_cursor(self.last_processed_change_count)
    }

    fn parent_version(&self) -> u64 {
        self.parent.borrow().version()
    }
```

Replace the body of `impl crate::readable::ReadableTable for PyFilterViewInner`
so every `self.table_inner.borrow()` becomes `self.parent.borrow()`, with:

```rust
    fn changeset(&self) -> Option<&crate::changeset::Changeset> {
        (self.parent.borrow().version() == self.last_parent_version)
            .then_some(&self.output_changes)
    }

    fn len(&self) -> usize {
        self.indices.len()
    }

    fn column_names(&self) -> Vec<String> {
        self.parent.borrow().column_names()
    }

    fn column_index(&self, name: &str) -> Option<usize> {
        self.parent.borrow().column_index(name)
    }

    fn column_type(&self, col_idx: usize) -> Option<crate::column::ColumnType> {
        self.parent.borrow().column_type(col_idx)
    }
```

(`get_row`, `get_value`, `get_value_by_index` keep their index checks and read
`self.parent.borrow()`; `version` is `self.sync_count.wrapping_add(self.parent.borrow().version())`.)

- [ ] **Step 6: `PyFilterView` holds roots and a name, and reads through its state**

Replace the struct with:

```rust
/// FilterView uses shared inner state so tick() can update registered views.
#[pyclass(name = "FilterView", unsendable)]
pub struct PyFilterView {
    /// Shared inner state (registered with root tables for tick())
    inner: Rc<RefCell<PyFilterViewInner>>,
    /// Root tables whose tick() syncs this filter's chain.
    roots: Vec<PyTable>,
    /// Parent name, the base for chained view names.
    name: String,
}
```

Replace `#[new] fn new` with:

```rust
    #[new]
    fn new(table: PyTable, predicate: PyObject) -> PyResult<Self> {
        let parent: Rc<RefCell<dyn crate::readable::ReadableTable>> = table.inner.clone();
        let name = table.inner.borrow().name().to_string();
        PyFilterView::over(parent, name, vec![table], predicate)
    }
```

Add, outside `#[pymethods]`:

```rust
impl PyFilterView {
    /// A filter over any parent. `roots` are the tables whose tick() syncs it.
    fn over(
        parent: Rc<RefCell<dyn crate::readable::ReadableTable>>,
        name: String,
        roots: Vec<PyTable>,
        predicate: PyObject,
    ) -> PyResult<Self> {
        let inner = Rc::new(RefCell::new(PyFilterViewInner {
            parent,
            predicate,
            indices: Vec::new(),
            output_changes: crate::changeset::Changeset::new(),
            last_parent_version: 0,
            last_synced_generation: 0,
            last_processed_change_count: usize::MAX,
            sync_count: 0,
        }));
        Python::with_gil(|py| inner.borrow_mut().refresh(py))?;
        Ok(PyFilterView { inner, roots, name })
    }

    fn parent_version(&self) -> u64 {
        self.inner.borrow().parent_version()
    }
}
```

Replace `get_row` and `get_value` in `#[pymethods] impl PyFilterView`:

```rust
    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let row = {
            let inner = self.inner.borrow();
            if index >= inner.indices.len() {
                return Err(PyIndexError::new_err("Index out of range"));
            }
            crate::readable::ReadableTable::get_row(&*inner, index)
                .map_err(PyValueError::new_err)?
        };
        row_to_py(py, &row)
    }
```

```rust
    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = {
            let inner = self.inner.borrow();
            if row >= inner.indices.len() {
                return Err(PyIndexError::new_err("Index out of range"));
            }
            crate::readable::ReadableTable::get_value(&*inner, row, column)
                .map_err(PyKeyError::new_err)?
        };
        column_value_to_py(py, &value)
    }
```

In `__iter__`, replace `let start_version = slf.table.inner.borrow().version();`
with `let start_version = slf.parent_version();`.

In `PyFilterView::sort`, replace the name line and registration/return with:

```rust
        let name = format!("{}_filtered_sorted", self.name);
        let parent: Rc<RefCell<dyn crate::readable::ReadableTable>> = self.inner.clone();
        let view = RustSortedView::new(name, parent, sort_keys).map_err(PyValueError::new_err)?;
        let inner = Rc::new(RefCell::new(view));
        register_on_roots(&self.roots, &RegisteredView::Sorted(Rc::downgrade(&inner)), |_| false);
        Ok(PySortedView {
            inner,
            roots: self.roots.clone(),
        })
```

In `PyFilterView::group_by`, use `format!("{}_filtered_grouped", self.name)` and
replace the registry push with
`register_on_roots(&self.roots, &RegisteredView::Aggregate(Rc::downgrade(&inner)), |_| false);`.

In `impl/src/python_bindings/iterators.rs`, `PyFilterViewIterator::__next__`:
replace `if view.table.inner.borrow().version() != self.start_version {` with
`if view.parent_version() != self.start_version {`.

- [ ] **Step 7: `PyTable` wiring**

`PyTable::filter`: replace the registry push with
`register_on_roots(std::slice::from_ref(self), &RegisteredView::Filter(Rc::downgrade(&view.inner)), |_| false);`.

`PyTable::sort`: return `PySortedView { inner, roots: vec![self.clone()] }`.

`PyTable::join`: return `PyJoinView { inner: join_rc, left: self.clone(), right: other }`
(the existing `other.registered_views` push must run before `other` moves).

`PyTable::tick`, `ActiveRegisteredView::Filter` arm: replace
`min_cursor = min_cursor.min(inner.borrow().last_processed_change_count);` with
`min_cursor = min_cursor.min(inner.borrow().root_cursor());`.

- [ ] **Step 8: `PySortedView` roots**

Replace `table: PyTable` with:

```rust
    /// Root tables whose tick() syncs this sort's chain.
    roots: Vec<PyTable>,
```

In `PySortedView::new` return `PySortedView { inner: Rc::new(RefCell::new(view)), roots: vec![table] }`.
In `PySortedView::group_by`, replace the registry block (from
`let sorted = Rc::downgrade(&self.inner);` through the aggregate push) with:

```rust
        let sorted = Rc::downgrade(&self.inner);
        let registered = |view: &RegisteredView| {
            matches!(view, RegisteredView::Sorted(existing) if existing.ptr_eq(&sorted))
        };
        register_on_roots(&self.roots, &RegisteredView::Sorted(sorted.clone()), registered);
        register_on_roots(&self.roots, &RegisteredView::Aggregate(Rc::downgrade(&inner)), |_| false);
```

- [ ] **Step 9: `PyJoinView` tables, registration, and `filter()`**

Replace the struct with:

```rust
#[pyclass(name = "JoinView", unsendable)]
pub struct PyJoinView {
    inner: Rc<RefCell<RustJoinView>>,
    /// The joined tables: either one's tick() syncs this join and its chain.
    left: PyTable,
    right: PyTable,
}
```

In `#[new] fn new`, return `PyJoinView { inner: Rc::new(RefCell::new(join)), left: left_table, right: right_table }`
(build `join` from `left_table.inner.clone()`/`right_table.inner.clone()` first, as now).

Add to `#[pymethods] impl PyJoinView`:

```rust
    /// Filter joined rows with a Python predicate. The predicate receives the
    /// row as `joined[i]` returns it (right columns prefixed `right_`, `None`
    /// for an unmatched side). Registered for tick() on both joined tables.
    fn filter(&self, predicate: PyObject) -> PyResult<PyFilterView> {
        let parent: Rc<RefCell<dyn crate::readable::ReadableTable>> = self.inner.clone();
        let name = self.inner.borrow().name().to_string();
        let view = PyFilterView::over(parent, name, self.roots(), predicate)?;
        self.ensure_registered();
        register_on_roots(&view.roots, &RegisteredView::Filter(Rc::downgrade(&view.inner)), |_| false);
        Ok(view)
    }
```

Add, outside `#[pymethods]`:

```rust
impl PyJoinView {
    fn roots(&self) -> Vec<PyTable> {
        vec![self.left.clone(), self.right.clone()]
    }

    /// Register this join for tick() on both tables unless already there:
    /// `table.join()` registers at creation; an explicit JoinView registers
    /// the first time a view is chained on it.
    fn ensure_registered(&self) {
        let join = Rc::downgrade(&self.inner);
        let left = |view: &RegisteredView| {
            matches!(view, RegisteredView::JoinLeft(existing) if existing.ptr_eq(&join))
        };
        register_on_roots(std::slice::from_ref(&self.left), &RegisteredView::JoinLeft(join.clone()), left);
        let right = |view: &RegisteredView| {
            matches!(view, RegisteredView::JoinRight(existing) if existing.ptr_eq(&join))
        };
        register_on_roots(std::slice::from_ref(&self.right), &RegisteredView::JoinRight(join.clone()), right);
    }
}
```

- [ ] **Step 10: Compile and clippy**

Run: `cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo clippy --all-targets --features python -- -D warnings 2>&1 | tail -3`
Expected: `Finished` with no warnings. Fix any remaining `table_inner` or `.table` references the compiler reports by routing them through `parent`/`roots`.

- [ ] **Step 11: Build the wheel and run the new and existing Python tests (GREEN)**

Run the wheel build, then
`cd tests && $SP/venv/bin/python -m pytest python/test_join_chaining.py -q -p no:cacheprovider`
Expected: `10 passed`.
Then `cd tests && $SP/venv/bin/python -m pytest python/ integration/ -q -p no:cacheprovider`
Expected: all pass (435 existing + 10).

- [ ] **Step 12: Mutation checks**

Each: copy `python_bindings.rs` to the scratchpad, apply, rebuild the wheel, run
`test_join_chaining.py`, confirm the named test fails, restore (`cmp` to verify).

1. `tick()` Filter arm: `inner.borrow().root_cursor()` → `inner.borrow().last_processed_change_count` → `test_ticks_compact_both_tables` fails.
2. `PyFilterViewInner::sync` empty-batch branch: delete `self.last_parent_version = parent.version();` → `test_no_output_join_edit_keeps_filter_history` fails.

- [ ] **Step 13: Commit**

```bash
git add impl/src/python_bindings.rs impl/src/python_bindings/conversions.rs impl/src/python_bindings/iterators.rs tests/python/test_join_chaining.py
git commit -F - <<'EOF'
Let Python filters sit on joins

The Python FilterView state now holds any ReadableTable parent. A parent
without history takes a version-checked refresh, an empty batch records the
parent version, reads go through the parent, and tick() translates the
filter's cursor for root compaction. JoinView.filter() registers the filter
on both joined tables, and an explicit JoinView registers itself on first
chaining. Table filters are unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 2: `JoinView.sort()` / `group_by()` and multi-root chains

**Files:**
- Modify: `impl/src/python_bindings.rs`
- Modify: `tests/python/test_join_chaining.py`

**Interfaces:**
- Consumes (Task 1): `register_on_roots`, `PyJoinView::{roots, ensure_registered}`,
  `PySortedView { inner, roots }`, `build_sort_keys`, `parse_agg_specs`,
  `extract_string_or_list`.
- Produces: Python `JoinView.sort(by, descending=None) -> SortedView`,
  `JoinView.group_by(by, agg) -> AggregateView`.

- [ ] **Step 1: Append the failing tests** to `tests/python/test_join_chaining.py`:

```python
def oracle_groups(rows, key, column):
    totals = {}
    for row in rows:
        totals[row[key]] = totals.get(row[key], 0.0) + row[column]
    return totals


def groups(view, key):
    return {row[key]: row["total"] for row in view}


def test_sort_and_group_over_join_follow_both_tables():
    orders, customers = make_tables()
    joined = join(orders, customers)
    ranked = joined.sort("amount", descending=True)
    by_tier = joined.group_by("right_tier", agg=[("total", "amount", "sum")])
    orders.append_row({"oid": 6, "cust": 3, "amount": 40.0})
    orders.tick()
    customers.set_value(0, "tier", 2)
    customers.tick()
    rows = oracle_rows(orders, customers)
    assert [r["oid"] for r in ranked] == [
        r["oid"] for r in sorted(rows, key=lambda r: -r["amount"])
    ]
    assert groups(by_tier, "right_tier") == oracle_groups(rows, "right_tier", "amount")


def test_two_level_chains_over_a_join():
    orders, customers = make_tables()
    joined = join(orders, customers)
    rich_ranked = joined.filter(big).sort("amount")
    rich_by_name = joined.filter(big).group_by(
        "right_name", agg=[("total", "amount", "sum")]
    )
    ranked_by_tier = joined.sort("amount").group_by(
        "right_tier", agg=[("total", "amount", "sum")]
    )
    orders.set_value(0, "amount", 70.0)
    orders.tick()
    customers.set_value(2, "name", "Cyd")
    customers.set_value(2, "tier", 2)
    customers.tick()
    rows = oracle_rows(orders, customers)
    rich = [r for r in rows if big(r)]
    assert [r["oid"] for r in rich_ranked] == [
        r["oid"] for r in sorted(rich, key=lambda r: r["amount"])
    ]
    assert groups(rich_by_name, "right_name") == oracle_groups(rich, "right_name", "amount")
    assert groups(ranked_by_tier, "right_tier") == oracle_groups(rows, "right_tier", "amount")


def test_explicit_join_registers_itself_once():
    orders, customers = make_tables()
    explicit = livetable.JoinView(
        "explicit", orders, customers, "cust", "cid", livetable.JoinType.LEFT
    )
    assert (orders.registered_view_count(), customers.registered_view_count()) == (0, 0)
    with pytest.raises(ValueError):
        explicit.sort("missing")
    assert (orders.registered_view_count(), customers.registered_view_count()) == (0, 0)
    ranked = explicit.sort("amount")
    totals = explicit.group_by("right_name", agg=[("total", "amount", "sum")])
    rich = explicit.filter(big)
    # The join once on each table, then its three children.
    assert (orders.registered_view_count(), customers.registered_view_count()) == (4, 4)
    customers.set_value(1, "name", "Bea")
    customers.tick()
    rows = oracle_rows(orders, customers)
    assert groups(totals, "right_name") == oracle_groups(rows, "right_name", "amount")
    assert canon(rich) == canon(r for r in rows if big(r))
    assert [r["oid"] for r in ranked] == [
        r["oid"] for r in sorted(rows, key=lambda r: r["amount"])
    ]


def test_self_join_children_register_once():
    staff = livetable.Table("staff", livetable.Schema([
        ("sid", livetable.ColumnType.INT32, False),
        ("boss", livetable.ColumnType.INT32, False),
        ("pay", livetable.ColumnType.FLOAT64, False),
    ]))
    for sid, boss, pay in [(1, 1, 100.0), (2, 1, 50.0), (3, 2, 40.0)]:
        staff.append_row({"sid": sid, "boss": boss, "pay": pay})
    managed = staff.join(staff, left_on="boss", right_on="sid", how="inner")
    team_pay = managed.group_by("right_sid", agg=[("total", "pay", "sum")])
    # JoinLeft and JoinRight share the registry; the aggregate appears once.
    assert staff.registered_view_count() == 3
    staff.set_value(2, "pay", 45.0)
    staff.tick()
    assert groups(team_pay, "right_sid") == {1: 150.0, 2: 45.0}


def test_children_survive_dropping_the_join_object():
    orders, customers = make_tables()
    totals = join(orders, customers).group_by(
        "right_name", agg=[("total", "amount", "sum")]
    )
    gc.collect()
    orders.append_row({"oid": 6, "cust": 2, "amount": 5.0})
    orders.tick()
    assert groups(totals, "right_name") == oracle_groups(
        oracle_rows(orders, customers), "right_name", "amount"
    )
```

- [ ] **Step 2: Build and run (RED)**

Run the wheel build (HEAD = Task 1), then
`cd tests && $SP/venv/bin/python -m pytest python/test_join_chaining.py -q -p no:cacheprovider`.
Expected: the 5 new tests FAIL with `AttributeError: ... no attribute 'sort'`
(or `'group_by'`); Task 1's 10 pass.

- [ ] **Step 3: Add `sort()` and `group_by()` to `#[pymethods] impl PyJoinView`**

```rust
    /// Sort joined rows. Registered for tick() on both joined tables.
    #[pyo3(signature = (by, descending=None))]
    fn sort(
        &self,
        by: &Bound<'_, PyAny>,
        descending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PySortedView> {
        let sort_keys = build_sort_keys(by, descending)?;
        let name = format!("{}_sorted", self.inner.borrow().name());
        let view = RustSortedView::new(name, self.inner.clone(), sort_keys)
            .map_err(PyValueError::new_err)?;
        let inner = Rc::new(RefCell::new(view));
        self.ensure_registered();
        let roots = self.roots();
        register_on_roots(&roots, &RegisteredView::Sorted(Rc::downgrade(&inner)), |_| false);
        Ok(PySortedView { inner, roots })
    }

    /// Group joined rows. Registered for tick() on both joined tables.
    fn group_by(
        &self,
        by: &Bound<'_, PyAny>,
        agg: Vec<(String, String, String)>,
    ) -> PyResult<PyAggregateView> {
        let group_cols = extract_string_or_list(by)?;
        let aggregations = parse_agg_specs(&agg)?;
        let name = format!("{}_grouped", self.inner.borrow().name());
        let view = RustAggregateView::new(name, self.inner.clone(), group_cols, aggregations)
            .map_err(PyValueError::new_err)?;
        let inner = Rc::new(RefCell::new(view));
        self.ensure_registered();
        register_on_roots(&self.roots(), &RegisteredView::Aggregate(Rc::downgrade(&inner)), |_| false);
        Ok(PyAggregateView { inner })
    }
```

- [ ] **Step 4: Build and run (GREEN)**

Run the wheel build, then `pytest python/test_join_chaining.py` → `15 passed`,
then the full `python/ integration/` suite → all pass.

- [ ] **Step 5: Mutation checks**

1. `register_on_roots`: replace `for root in roots {` with `for root in roots.iter().take(1) {` →
   `test_filter_over_join_updates_on_right_only_tick` and
   `test_sort_and_group_over_join_follow_both_tables` fail.
2. `register_on_roots`: delete the `if seen.iter().any(...) { continue; }` block →
   `test_self_join_children_register_once` fails.
3. `PyJoinView::sort`: move `self.ensure_registered();` above `RustSortedView::new` →
   `test_explicit_join_registers_itself_once` fails.

Restore after each (`cmp` to the saved copy).

- [ ] **Step 6: Commit**

```bash
git add impl/src/python_bindings.rs tests/python/test_join_chaining.py
git commit -F - <<'EOF'
Add JoinView.sort() and group_by() in Python

Sorts and aggregates over a join register on both joined tables after the
join, so tick() on either table keeps them current; chains continue through
FilterView and SortedView on every root.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 3: Documentation and final verification

**Files:**
- Modify: `README.md`, `CLAUDE.md`, `docs/PYTHON_BINDINGS_README.md`,
  `docs/ORIGINAL_VISION.md`, `docs/GETTING_STARTED.md` (only if it lists
  supported chains), spec status.

- [ ] **Step 1: Docs**

- README: add to the incremental paragraph that Python can chain
  `joined.filter()/sort()/group_by()`, and add to the Example section:

  ```python
  # Views over a join (Python): tick either table to update the chain
  enriched = orders.join(customers, left_on="cust", right_on="cid")
  big_by_tier = enriched.filter(lambda r: r["amount"] >= 50).group_by(
      "right_tier", agg=[("total", "amount", "sum")]
  )
  customers.set_value(0, "tier", 2)
  customers.tick()
  ```
- CLAUDE.md Python API Usage: add the same example under the join section;
  Key Patterns: replace "Python chaining supports `FilterView.sort()`,
  `FilterView.group_by()`, and `SortedView.group_by()`, not every Rust DAG."
  with "Python chaining supports `FilterView.sort()/group_by()`,
  `SortedView.group_by()`, and `JoinView.filter()/sort()/group_by()`, not every
  Rust DAG. Views chained on a join register on both joined tables (after the
  join), so either table's tick() updates them; an explicit JoinView registers
  itself on first chaining. The Python filter accepts any ReadableTable parent
  and translates its cursor with `root_changeset_cursor()` for compaction."
- `docs/PYTHON_BINDINGS_README.md` JoinView methods: add `filter(predicate)`,
  `sort(by, descending=None)`, `group_by(by, agg)`; note the predicate row shape
  and "tick the table you mutated: either joined table's tick updates the chain".
- `docs/ORIGINAL_VISION.md`: add `- [x] Python chaining over joins
  (JoinView.filter/sort/group_by, registered on both joined tables)`.
- Spec: status `Implemented 2026-09-27`.

- [ ] **Step 2: Full verification**

```bash
cd impl && cargo clippy --all-targets -- -D warnings \
  && cargo clippy --all-targets --features server -- -D warnings \
  && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo clippy --all-targets --features python -- -D warnings
cd impl && cargo test --features server 2>&1 | grep -E 'test result|FAILED'
cd tests && $SP/venv/bin/python -m pytest python/ integration/ -q -p no:cacheprovider
```
Expected: clippy clean ×3; all Rust targets ok; Python 450 passed (435 + 15).

- [ ] **Step 3: Commit**

```bash
git add README.md CLAUDE.md docs
git commit -m "Document Python chaining over joins

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
