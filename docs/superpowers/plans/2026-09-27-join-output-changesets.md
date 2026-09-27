# Join Output Changesets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `JoinView` publishes a bounded output changeset in join coordinates, so
filters, sorts, aggregates, and joins over a join replay instead of rebuilding.

**Architecture:** `JoinView::sync` records one `TableChange` per `join_index`
mutation (insert, removal, placeholder fill) and fans non-key updates out to
every output position of the updated parent row. The value-only side of a batch
runs first so the structural side can read the other parent live. History
follows the filter/sort/aggregate contract: one retained batch, `invalidate()`
on rebuild, visible only while both parents' versions match.

**Tech Stack:** Rust (livetable crate, `impl/`), cargo tests/clippy, PyO3 wheel
via maturin for the Python check.

**Spec:** `docs/superpowers/specs/2026-09-27-join-output-changesets-design.md`

## Global Constraints

- Output events use `join_index` positions; right-parent columns are named
  `right_<column>` in payloads and `CellUpdated.column`, exactly as `get_row`.
- Retained history is one batch. A batch that would record more than 512
  events (`MAX_FILTER_REPLAY_CHANGES * 2`) invalidates history instead.
- `changeset()` is `Some` only while both parents' `version()` equal the
  versions recorded at the last sync/rebuild.
- `rebuild_index()` calls `self.output_changes.invalidate()`.
- `sync()` returns true when any output event was produced (value-only edits
  included); join semantics, key rules, `join_index` ordering, the frame-mixing
  guard, and `root_changeset_cursor` are unchanged.
- No wire/protocol changes and no new Python API.
- Checks: `cargo clippy --all-targets -- -D warnings`, the same with
  `--features server`, and with `--features python` under
  `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Local Python wheels must be built with
  `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk` (macOS 27
  linker bug; see memory). Never commit that path.

## Review Focus

- A self-join (one table as both parents): a value edit must fan out to the
  left and the `right_` half and replay; a structural edit rebuilds.
  → Task 1 `self_join_value_edits_update_both_halves`.
- NULL join keys: setting a nullable key to NULL turns matches into a
  placeholder (and, for FULL, an orphan) and back, with replayable history.
  → Task 1 `null_keys_become_placeholders_and_rejoin`.
- A right row matched by more than 512 left rows gets a value edit: history
  invalidates, the join stays correct, and a child refreshes to the right rows.
  → Task 1 `oversized_output_invalidates_history_but_stays_incremental`.
- A join whose parent is a filter: filter-coordinate events drive the join,
  and an excluded upstream edit keeps history.
  → Task 1 `empty_batches_keep_history_and_join_over_a_filter_replays`.
- A value edit followed by a delete of the same right row in one batch (the
  structural side's scan path): the delete payload carries the edited value.
  → Task 1 `a_value_edit_then_delete_of_the_same_right_row`.

---

## File Structure

- Modify `impl/src/view/join.rs` — history field, `changeset()`, emission
  helpers, `sync()` split into `apply_left_changes` / `apply_right_changes`.
- Create `impl/tests/join_pipeline.rs` — contract tests.
- Modify `impl/tests/forward_prop_fuzz.rs` — replay assertions on both join
  fuzzers; a dimension join with filter/sort/aggregate children in the DAG fuzz.
- Modify `tests/python/test_right_full_joins.py` — `sync()` return value.
- Modify `.github/workflows/ci.yml`, `tests/run_all.sh`, `tests/README.md`,
  `CLAUDE.md`, `impl/src/readable.rs`, `impl/src/lib.rs`, `docs/JOIN_FEATURE.md`,
  `docs/API_GUIDE.md`, `docs/ORIGINAL_VISION.md`, `docs/PYTHON_BINDINGS_README.md`,
  incremental pipeline docs, and the spec's status/notes.

---

### Task 1: JoinView output history

**Files:**
- Modify: `impl/src/view/join.rs`
- Create: `impl/tests/join_pipeline.rs`

**Interfaces:**
- Consumes: `crate::filter_changes::{row_after_update, MAX_FILTER_REPLAY_CHANGES}`,
  `Changeset::{clear, push, invalidate}` (`invalidate` is `pub(crate)`).
- Produces: `impl ReadableTable for JoinView { fn changeset(&self) -> Option<&Changeset> }`;
  `JoinView::sync(&mut self) -> bool` with the new return rule. Private:
  `JoinOutput`, `Half<'a>`, `apply_left_changes`, `apply_right_changes`.

- [ ] **Step 1: Write the contract tests** — create `impl/tests/join_pipeline.rs`:

```rust
//! JoinView output history: join coordinates, per-event emission, and
//! incremental children. See
//! docs/superpowers/specs/2026-09-27-join-output-changesets-design.md.
use livetable::{
    AggregateFunction, AggregateView, Changeset, ColumnType, ColumnValue, FilterView, JoinType,
    JoinView, ReadableTable, Schema, SortKey, SortedView, Table, TableChange,
};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use ColumnValue::{Int32, Null};

type Row = HashMap<String, ColumnValue>;
type Order = (i32, Option<i32>, i32);

fn order(oid: i32, cust: Option<i32>, amount: i32) -> Row {
    HashMap::from([
        ("oid".into(), Int32(oid)),
        ("cust".into(), cust.map_or(Null, Int32)),
        ("amount".into(), Int32(amount)),
    ])
}

/// Orders (oid, cust, amount); `cust` is the nullable join key.
fn orders_table(rows: &[Order]) -> Table {
    let mut table = Table::new(
        "orders".into(),
        Schema::new(vec![
            ("oid".into(), ColumnType::Int32, false),
            ("cust".into(), ColumnType::Int32, true),
            ("amount".into(), ColumnType::Int32, false),
        ]),
    );
    for &(oid, cust, amount) in rows {
        table.append_row(order(oid, cust, amount)).unwrap();
    }
    table.clear_changeset();
    table
}

fn orders(rows: &[Order]) -> Rc<RefCell<Table>> {
    Rc::new(RefCell::new(orders_table(rows)))
}

fn name(value: &str) -> ColumnValue {
    ColumnValue::String(value.into())
}

fn customer(cid: i32, customer_name: &str) -> Row {
    HashMap::from([("cid".into(), Int32(cid)), ("name".into(), name(customer_name))])
}

/// Customers (cid, name).
fn customers(rows: &[(i32, &str)]) -> Rc<RefCell<Table>> {
    let mut table = Table::new(
        "customers".into(),
        Schema::new(vec![
            ("cid".into(), ColumnType::Int32, false),
            ("name".into(), ColumnType::String, false),
        ]),
    );
    for &(cid, customer_name) in rows {
        table.append_row(customer(cid, customer_name)).unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn join(
    left: Rc<RefCell<dyn ReadableTable>>,
    right: Rc<RefCell<dyn ReadableTable>>,
    how: JoinType,
) -> JoinView {
    JoinView::new("j".into(), left, right, "cust".into(), "cid".into(), how).unwrap()
}

/// An expected output row; a missing half is all Nulls.
fn out_row(left: Option<Order>, right: Option<(i32, &str)>) -> Row {
    let mut row = match left {
        Some((oid, cust, amount)) => order(oid, cust, amount),
        None => HashMap::from([
            ("oid".into(), Null),
            ("cust".into(), Null),
            ("amount".into(), Null),
        ]),
    };
    let (cid, customer_name) = right.map_or((Null, Null), |(cid, n)| (Int32(cid), name(n)));
    row.insert("right_cid".into(), cid);
    row.insert("right_name".into(), customer_name);
    row
}

fn inserted(index: usize, data: Row) -> TableChange {
    TableChange::RowInserted { index, data }
}

fn deleted(index: usize, data: Row) -> TableChange {
    TableChange::RowDeleted { index, data }
}

fn update(row: usize, column: &str, old_value: ColumnValue, new_value: ColumnValue) -> TableChange {
    TableChange::CellUpdated {
        row,
        column: column.into(),
        old_value,
        new_value,
    }
}

fn snapshot(view: &dyn ReadableTable) -> Vec<Row> {
    (0..view.len()).map(|i| view.get_row(i).unwrap()).collect()
}

/// A synced join's rows and history cursor, taken before a mutation.
fn baseline(view: &dyn ReadableTable) -> (Vec<Row>, usize) {
    let cursor = view
        .changeset()
        .expect("a synced join exposes history")
        .total_len();
    (snapshot(view), cursor)
}

/// Apply history to an old snapshot, checking deletion payloads and old values.
fn replay(rows: &mut Vec<Row>, changes: &[TableChange]) {
    for change in changes {
        match change {
            TableChange::RowInserted { index, data } => rows.insert(*index, data.clone()),
            TableChange::RowDeleted { index, data } => {
                assert_eq!(rows.remove(*index), *data, "historical deletion payload")
            }
            TableChange::CellUpdated {
                row,
                column,
                old_value,
                new_value,
            } => {
                assert_eq!(&rows[*row][column], old_value, "historical old value");
                rows[*row].insert(column.clone(), new_value.clone());
            }
        }
    }
}

/// Sync, check that the history turns the baseline into the new rows, and
/// return it.
fn replayed(view: &mut JoinView, (mut rows, cursor): (Vec<Row>, usize)) -> Vec<TableChange> {
    view.sync();
    let changes = view
        .changeset()
        .expect("history after sync")
        .changes_from(cursor)
        .expect("incremental history")
        .to_vec();
    replay(&mut rows, &changes);
    assert_eq!(rows, snapshot(&*view), "replayed history reproduces the join");
    changes
}

fn history_kept(view: &dyn ReadableTable, cursor: usize) -> bool {
    view.changeset()
        .expect("synced")
        .changes_from(cursor)
        .is_some()
}

struct Counted<T> {
    inner: T,
    reads: Cell<usize>,
}

impl<T: ReadableTable> ReadableTable for Counted<T> {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn column_names(&self) -> Vec<String> {
        self.inner.column_names()
    }
    fn get_row(&self, index: usize) -> Result<Row, String> {
        self.reads.set(self.reads.get() + 1);
        self.inner.get_row(index)
    }
    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        self.reads.set(self.reads.get() + 1);
        self.inner.get_value(row, column)
    }
    fn version(&self) -> u64 {
        self.inner.version()
    }
    fn changeset(&self) -> Option<&Changeset> {
        self.inner.changeset()
    }
}

#[test]
fn left_insert_emits_its_matches_and_claims_right_orphans() {
    let o = orders(&[(1, Some(10), 5)]);
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut inner = join(o.clone(), c.clone(), JoinType::Inner);
    let mut right = join(o.clone(), c.clone(), JoinType::Right);
    let (inner_base, right_base) = (baseline(&inner), baseline(&right));
    o.borrow_mut().append_row(order(2, Some(20), 7)).unwrap();

    let matched = out_row(Some((2, Some(20), 7)), Some((20, "b")));
    assert_eq!(replayed(&mut inner, inner_base), [inserted(1, matched.clone())]);
    assert_eq!(
        replayed(&mut right, right_base),
        [deleted(1, out_row(None, Some((20, "b")))), inserted(1, matched)]
    );
}

#[test]
fn left_delete_orphans_the_rights_it_matched() {
    let o = orders(&[(1, Some(10), 5), (2, Some(20), 7)]);
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut full = join(o.clone(), c, JoinType::Full);
    let base = baseline(&full);
    o.borrow_mut().delete_row(0).unwrap();
    assert_eq!(
        replayed(&mut full, base),
        [
            deleted(0, out_row(Some((1, Some(10), 5)), Some((10, "a")))),
            inserted(1, out_row(None, Some((10, "a")))),
        ]
    );
}

#[test]
fn right_insert_fills_left_placeholders_in_place() {
    let o = orders(&[(1, Some(10), 5), (2, Some(30), 6)]);
    let c = customers(&[(10, "a")]);
    let mut left = join(o, c.clone(), JoinType::Left);
    let base = baseline(&left);
    c.borrow_mut().append_row(customer(30, "c")).unwrap();
    assert_eq!(
        replayed(&mut left, base),
        [
            update(1, "right_cid", Null, Int32(30)),
            update(1, "right_name", Null, name("c")),
        ]
    );
}

#[test]
fn right_delete_removes_scattered_entries_in_ascending_order() {
    let o = orders(&[(1, Some(10), 1), (2, Some(20), 2), (3, Some(10), 3)]);
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut left = join(o, c.clone(), JoinType::Left);
    let base = baseline(&left);
    c.borrow_mut().delete_row(0).unwrap();
    assert_eq!(
        replayed(&mut left, base),
        [
            deleted(0, out_row(Some((1, Some(10), 1)), Some((10, "a")))),
            deleted(1, out_row(Some((3, Some(10), 3)), Some((10, "a")))),
            inserted(0, out_row(Some((1, Some(10), 1)), None)),
            inserted(2, out_row(Some((3, Some(10), 3)), None)),
        ]
    );
}

#[test]
fn key_update_moves_the_row_between_matches() {
    let o = orders(&[(1, Some(10), 5)]);
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut inner = join(o.clone(), c, JoinType::Inner);
    let base = baseline(&inner);
    o.borrow_mut().set_value(0, "cust", Int32(20)).unwrap();
    assert_eq!(
        replayed(&mut inner, base),
        [
            deleted(0, out_row(Some((1, Some(10), 5)), Some((10, "a")))),
            inserted(0, out_row(Some((1, Some(20), 5)), Some((20, "b")))),
        ]
    );
}

#[test]
fn key_update_to_the_same_key_updates_cells() {
    let o = orders(&[(1, Some(10), 5)]);
    let c = customers(&[(10, "a")]);
    let mut inner = join(o.clone(), c, JoinType::Inner);
    let base = baseline(&inner);
    o.borrow_mut().set_value(0, "cust", Int32(10)).unwrap();
    assert_eq!(
        replayed(&mut inner, base),
        [update(0, "cust", Int32(10), Int32(10))]
    );
}

#[test]
fn value_updates_fan_out_to_every_output_row() {
    // join_index: [(0,0), (0,2), (1,1), (2,0), (2,2)]
    let o = orders(&[(1, Some(10), 1), (2, Some(20), 2), (3, Some(10), 3)]);
    let c = customers(&[(10, "a"), (20, "b"), (10, "c")]);
    let mut inner = join(o.clone(), c.clone(), JoinType::Inner);

    let base = baseline(&inner);
    o.borrow_mut().set_value(0, "amount", Int32(9)).unwrap();
    assert_eq!(
        replayed(&mut inner, base),
        [
            update(0, "amount", Int32(1), Int32(9)),
            update(1, "amount", Int32(1), Int32(9)),
        ]
    );

    let base = baseline(&inner);
    c.borrow_mut().set_value(0, "name", name("z")).unwrap();
    assert_eq!(
        replayed(&mut inner, base),
        [
            update(0, "right_name", name("a"), name("z")),
            update(3, "right_name", name("a"), name("z")),
        ]
    );
}

#[test]
fn the_value_only_side_applies_first() {
    let o = orders(&[(1, Some(10), 1)]);
    let c = customers(&[(10, "a")]);
    let mut left = join(o.clone(), c.clone(), JoinType::Left);
    let base = baseline(&left);
    c.borrow_mut().set_value(0, "name", name("z")).unwrap();
    o.borrow_mut().append_row(order(2, Some(10), 2)).unwrap();
    assert_eq!(
        replayed(&mut left, base),
        [
            update(0, "right_name", name("a"), name("z")),
            inserted(1, out_row(Some((2, Some(10), 2)), Some((10, "z")))),
        ]
    );
}

#[test]
fn a_key_update_then_a_value_edit_replays_from_the_historical_row() {
    let o = orders(&[(1, Some(10), 1)]);
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut inner = join(o.clone(), c, JoinType::Inner);
    let base = baseline(&inner);
    o.borrow_mut().set_value(0, "cust", Int32(20)).unwrap();
    o.borrow_mut().set_value(0, "amount", Int32(9)).unwrap();
    assert_eq!(
        replayed(&mut inner, base),
        [
            deleted(0, out_row(Some((1, Some(10), 1)), Some((10, "a")))),
            inserted(0, out_row(Some((1, Some(20), 1)), Some((20, "b")))),
            update(0, "amount", Int32(1), Int32(9)),
        ]
    );
}

#[test]
fn rebuilds_invalidate_history() {
    let o = orders(&[(1, Some(10), 1)]);
    let c = customers(&[(10, "a")]);
    let mut inner = join(o.clone(), c.clone(), JoinType::Inner);

    let (_, cursor) = baseline(&inner);
    inner.refresh();
    assert!(!history_kept(&inner, cursor), "refresh invalidates");

    // Structural changes on both sides take the frame-mixing rebuild.
    let (_, cursor) = baseline(&inner);
    o.borrow_mut().append_row(order(2, Some(20), 2)).unwrap();
    c.borrow_mut().append_row(customer(20, "b")).unwrap();
    assert!(inner.sync());
    assert!(!history_kept(&inner, cursor), "a rebuild invalidates");
    assert_eq!(inner.len(), 2);
}

#[test]
fn oversized_output_invalidates_history_but_stays_incremental() {
    let o = Rc::new(RefCell::new(Counted {
        inner: orders_table(&[]),
        reads: Cell::new(0),
    }));
    let c = customers(&[(10, "a")]);
    let joined = Rc::new(RefCell::new(join(o.clone(), c.clone(), JoinType::Inner)));

    for oid in 0..600 {
        o.borrow_mut()
            .inner
            .append_row(order(oid, Some(10), oid))
            .unwrap();
    }
    let (_, cursor) = baseline(&*joined.borrow());
    o.borrow().reads.set(0);
    assert!(joined.borrow_mut().sync());
    assert!(!history_kept(&*joined.borrow(), cursor), "600 inserts overflow");
    assert_eq!(o.borrow().reads.get(), 0, "no rebuild: left rows are not reread");
    assert_eq!(joined.borrow().len(), 600);

    // One right edit fans out to 600 rows: history overflows, a child refreshes.
    let big = |row: &Row| row["amount"].as_i32().is_some_and(|amount| amount >= 300);
    let mut child = FilterView::new("big".into(), joined.clone(), big);
    let (_, cursor) = baseline(&*joined.borrow());
    c.borrow_mut().set_value(0, "name", name("z")).unwrap();
    assert!(joined.borrow_mut().sync());
    assert!(!history_kept(&*joined.borrow(), cursor), "600 updates overflow");
    child.sync();
    let fresh = FilterView::new("fresh".into(), joined.clone(), big);
    assert_eq!(snapshot(&child), snapshot(&fresh));
    assert!(snapshot(&child).iter().all(|row| row["right_name"] == name("z")));
}

#[test]
fn empty_batches_keep_history_and_join_over_a_filter_replays() {
    let o = orders(&[(1, Some(10), 1), (2, Some(10), 9)]);
    let big = Rc::new(RefCell::new(FilterView::new(
        "big".into(),
        o.clone(),
        |row: &Row| row["amount"].as_i32().is_some_and(|amount| amount >= 5),
    )));
    let c = customers(&[(10, "a")]);
    let mut inner = join(big.clone(), c, JoinType::Inner);

    // Excluded upstream edit: the filter's version moves, it emits nothing.
    let base = baseline(&inner);
    o.borrow_mut().set_value(0, "amount", Int32(2)).unwrap();
    big.borrow_mut().sync();
    assert_eq!(replayed(&mut inner, base), []);

    // Included edit arrives in filter coordinates (order 2 is filter row 0).
    let base = baseline(&inner);
    o.borrow_mut().set_value(1, "amount", Int32(8)).unwrap();
    big.borrow_mut().sync();
    assert_eq!(
        replayed(&mut inner, base),
        [update(0, "amount", Int32(9), Int32(8))]
    );
}

#[test]
fn history_is_hidden_while_a_parent_is_ahead() {
    let o = orders(&[(1, Some(10), 1)]);
    let c = customers(&[(10, "a")]);
    let mut inner = join(o.clone(), c, JoinType::Inner);
    o.borrow_mut().append_row(order(2, Some(10), 2)).unwrap();
    assert!(inner.changeset().is_none());
    inner.sync();
    assert!(inner.changeset().is_some());
}

#[test]
fn filter_sort_and_group_over_a_join_replay_instead_of_rebuilding() {
    let rows: Vec<Order> = (0..100).map(|i| (i, Some(i % 10), i)).collect();
    let names: Vec<String> = (0..10).map(|i| format!("c{i}")).collect();
    let people: Vec<(i32, &str)> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (i as i32, n.as_str()))
        .collect();
    let o = orders(&rows);
    let joined = Rc::new(RefCell::new(Counted {
        inner: join(o.clone(), customers(&people), JoinType::Inner),
        reads: Cell::new(0),
    }));
    let big = |row: &Row| row["amount"].as_i32().is_some_and(|amount| amount >= 50);
    let by_amount = || vec![SortKey::descending("amount")];
    let sums = || vec![("total".to_string(), "amount".to_string(), AggregateFunction::Sum)];
    let mut filtered = FilterView::new("big".into(), joined.clone(), big);
    let mut ranked = SortedView::new("ranked".into(), joined.clone(), by_amount()).unwrap();
    let mut totals =
        AggregateView::new("totals".into(), joined.clone(), vec!["right_name".into()], sums())
            .unwrap();

    // Warm-up edit: the aggregate builds its row-to-group index lazily.
    for (row, amount) in [(0, 1000), (42, 1001)] {
        o.borrow_mut().set_value(row, "amount", Int32(amount)).unwrap();
        joined.borrow_mut().inner.sync();
        joined.borrow().reads.set(0);
        filtered.sync();
        ranked.sync();
        totals.sync();
    }
    assert!(
        joined.borrow().reads.get() <= 6,
        "children replay one row instead of rereading all 100: {} reads",
        joined.borrow().reads.get()
    );

    let fresh_filter = FilterView::new("f".into(), joined.clone(), big);
    let fresh_sort = SortedView::new("s".into(), joined.clone(), by_amount()).unwrap();
    let fresh_totals =
        AggregateView::new("t".into(), joined.clone(), vec!["right_name".into()], sums())
            .unwrap();
    assert_eq!(snapshot(&filtered), snapshot(&fresh_filter));
    assert_eq!(snapshot(&ranked), snapshot(&fresh_sort));
    assert_eq!(snapshot(&totals), snapshot(&fresh_totals));
}

#[test]
fn sync_reports_value_only_edits() {
    let o = orders(&[(1, Some(10), 1)]);
    let c = customers(&[(10, "a")]);
    let mut inner = join(o.clone(), c.clone(), JoinType::Inner);
    o.borrow_mut().set_value(0, "amount", Int32(2)).unwrap();
    assert!(inner.sync());
    assert!(!inner.sync());
    c.borrow_mut().set_value(0, "name", name("z")).unwrap();
    assert!(inner.sync());
}

#[test]
fn self_join_value_edits_update_both_halves() {
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut pairs = JoinView::new(
        "pairs".into(),
        c.clone(),
        c.clone(),
        "cid".into(),
        "cid".into(),
        JoinType::Inner,
    )
    .unwrap();
    let base = baseline(&pairs);
    c.borrow_mut().set_value(1, "name", name("z")).unwrap();
    assert_eq!(
        replayed(&mut pairs, base),
        [
            update(1, "name", name("b"), name("z")),
            update(1, "right_name", name("b"), name("z")),
        ]
    );

    // An insert is structural on both sides at once: rebuild.
    let (_, cursor) = baseline(&pairs);
    c.borrow_mut().append_row(customer(30, "c")).unwrap();
    assert!(pairs.sync());
    assert!(!history_kept(&pairs, cursor));
    assert_eq!(pairs.len(), 3);
}

#[test]
fn null_keys_become_placeholders_and_rejoin() {
    let o = orders(&[(1, Some(10), 1)]);
    let c = customers(&[(10, "a")]);
    let mut left = join(o.clone(), c.clone(), JoinType::Left);
    let mut full = join(o.clone(), c, JoinType::Full);
    let matched = out_row(Some((1, Some(10), 1)), Some((10, "a")));
    let unmatched = out_row(Some((1, None, 1)), None);

    let (left_base, full_base) = (baseline(&left), baseline(&full));
    o.borrow_mut().set_value(0, "cust", Null).unwrap();
    assert_eq!(
        replayed(&mut left, left_base),
        [deleted(0, matched.clone()), inserted(0, unmatched.clone())]
    );
    assert_eq!(
        replayed(&mut full, full_base),
        [
            deleted(0, matched.clone()),
            inserted(0, out_row(None, Some((10, "a")))),
            inserted(0, unmatched.clone()),
        ]
    );

    let left_base = baseline(&left);
    o.borrow_mut().set_value(0, "cust", Int32(10)).unwrap();
    assert_eq!(
        replayed(&mut left, left_base),
        [deleted(0, unmatched), inserted(0, matched)]
    );
}

#[test]
fn a_value_edit_then_delete_of_the_same_right_row() {
    let o = orders(&[(1, Some(10), 1), (2, Some(20), 2)]);
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut left = join(o, c.clone(), JoinType::Left);
    let base = baseline(&left);
    c.borrow_mut().set_value(0, "name", name("z")).unwrap();
    c.borrow_mut().delete_row(0).unwrap();
    assert_eq!(
        replayed(&mut left, base),
        [
            update(0, "right_name", name("a"), name("z")),
            deleted(0, out_row(Some((1, Some(10), 1)), Some((10, "z")))),
            inserted(0, out_row(Some((1, Some(10), 1)), None)),
        ]
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd impl && cargo test --features server --test join_pipeline 2>&1 | grep -E '^test |test result'`
Expected: most tests FAIL with "a synced join exposes history" (no
`changeset()` yet); `rebuilds_invalidate_history` and `history_is_hidden…` fail
on `expect("synced")`/`is_some()`.

- [ ] **Step 3: Add history types and helpers to `impl/src/view/join.rs`**

Replace the imports with:

```rust
use crate::changeset::{Changeset, TableChange};
use crate::column::ColumnValue;
use crate::filter_changes::{row_after_update, MAX_FILTER_REPLAY_CHANGES};
use crate::readable::ReadableTable;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::rc::Rc;
```

Add after the `JoinType` enum:

```rust
type Row = HashMap<String, ColumnValue>;
/// (left row, right row); `None` is the unmatched side.
type Entry = (Option<usize>, Option<usize>);

/// A batch whose output would exceed this invalidates its history instead:
/// consumers rebuild above their own replay bounds anyway.
const MAX_JOIN_OUTPUT_CHANGES: usize = MAX_FILTER_REPLAY_CHANGES * 2;

/// One incremental batch's output events. Past the bound it stops recording,
/// and building payloads; the batch then invalidates history.
#[derive(Default)]
struct JoinOutput {
    changes: Vec<TableChange>,
    overflowed: bool,
}

impl JoinOutput {
    fn record(
        &mut self,
        change: impl FnOnce() -> Result<TableChange, String>,
    ) -> Result<(), String> {
        if self.overflowed {
            return Ok(());
        }
        if self.changes.len() == MAX_JOIN_OUTPUT_CHANGES {
            self.overflowed = true;
            self.changes = Vec::new();
            return Ok(());
        }
        self.changes.push(change()?);
        Ok(())
    }

    fn emitted(&self) -> bool {
        self.overflowed || !self.changes.is_empty()
    }
}

/// Where one half of an emitted output row comes from.
#[derive(Clone, Copy)]
enum Half<'a> {
    /// The parent row as of the change being applied, from its changeset.
    Given(&'a Row),
    /// Read live from the parent; `None` is all Nulls.
    Live(Option<usize>),
}

/// A right-parent row with its columns named as the join outputs them.
fn prefix_right(row: Row) -> Row {
    row.into_iter()
        .map(|(column, value)| (format!("right_{column}"), value))
        .collect()
}
```

Add the field to `JoinView` (after `last_right_parent_version`):

```rust
    /// The latest incremental batch in JOIN coordinates (join_index
    /// positions). Rebuilds and oversized batches invalidate it.
    output_changes: Changeset,
```

In `new_multi`, add `output_changes: Changeset::new(),` to the struct literal.
In `rebuild_index`, insert `self.output_changes.invalidate();` immediately
before `self.sync_count += 1;`.

Replace `get_row` with:

```rust
    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        let &(left, right) = self
            .join_index
            .get(index)
            .ok_or_else(|| format!("Index {} out of range [0, {})", index, self.len()))?;
        self.output_row(Half::Live(left), Half::Live(right))
    }
```

Add these methods to `impl JoinView` (after `find_right_insert_position`):

```rust
    /// An output row: left columns plus `right_`-prefixed right columns.
    fn output_row(&self, left: Half, right: Half) -> Result<Row, String> {
        let mut row = match left {
            Half::Given(row) => row.clone(),
            Half::Live(Some(l)) => self.left_table.borrow().get_row(l)?,
            Half::Live(None) => self
                .left_column_names
                .iter()
                .map(|column| (column.clone(), ColumnValue::Null))
                .collect(),
        };
        match right {
            Half::Given(right) => row.extend(prefix_right(right.clone())),
            Half::Live(Some(r)) => row.extend(prefix_right(self.right_table.borrow().get_row(r)?)),
            Half::Live(None) => row.extend(
                self.right_column_names
                    .iter()
                    .map(|column| (format!("right_{column}"), ColumnValue::Null)),
            ),
        }
        Ok(row)
    }

    /// Positions of left row `l`'s entries: contiguous, since `join_index` is
    /// sorted by left row.
    fn left_range(&self, l: usize) -> Range<usize> {
        let start = self
            .join_index
            .partition_point(|(el, _)| el.is_some_and(|el| el < l));
        start..self.find_left_insert_position(l)
    }

    /// Ascending positions of right row `r`'s entries (scattered).
    fn right_positions(&self, r: usize) -> Vec<usize> {
        (0..self.join_index.len())
            .filter(|&p| self.join_index[p].1 == Some(r))
            .collect()
    }

    /// Right row → ascending positions, for a right batch that only updates
    /// values (no position moves while it applies).
    fn right_position_map(&self) -> HashMap<usize, Vec<usize>> {
        let mut map: HashMap<usize, Vec<usize>> = HashMap::new();
        for (p, &(_, r)) in self.join_index.iter().enumerate() {
            if let Some(r) = r {
                map.entry(r).or_default().push(p);
            }
        }
        map
    }

    fn insert_entry(
        &mut self,
        pos: usize,
        entry: Entry,
        left: Half,
        right: Half,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        self.join_index.insert(pos, entry);
        out.record(|| {
            Ok(TableChange::RowInserted {
                index: pos,
                data: self.output_row(left, right)?,
            })
        })
    }

    /// Remove left row `l`'s entries, recorded with `left` as the left half.
    /// Returns the right rows they matched.
    fn remove_left_entries(
        &mut self,
        l: usize,
        left: &Row,
        out: &mut JoinOutput,
    ) -> Result<Vec<usize>, String> {
        let range = self.left_range(l);
        let removed: Vec<Entry> = self.join_index.drain(range.clone()).collect();
        for &(_, r) in &removed {
            out.record(|| {
                Ok(TableChange::RowDeleted {
                    index: range.start,
                    data: self.output_row(Half::Given(left), Half::Live(r))?,
                })
            })?;
        }
        Ok(removed.into_iter().filter_map(|(_, r)| r).collect())
    }

    /// Remove right row `r`'s entries in ascending order, each recorded at its
    /// index after the removals before it. Returns the left rows they matched.
    fn remove_right_entries(
        &mut self,
        r: usize,
        right: &Row,
        out: &mut JoinOutput,
    ) -> Result<Vec<usize>, String> {
        let positions = self.right_positions(r);
        for (removed, &p) in positions.iter().enumerate() {
            let left = self.join_index[p].0;
            out.record(|| {
                Ok(TableChange::RowDeleted {
                    index: p - removed,
                    data: self.output_row(Half::Live(left), Half::Given(right))?,
                })
            })?;
        }
        let lefts = positions
            .iter()
            .filter_map(|&p| self.join_index[p].0)
            .collect();
        self.join_index.retain(|&(_, er)| er != Some(r));
        Ok(lefts)
    }

    /// RIGHT/FULL: right rows that lost their last match become unmatched
    /// entries in the tail, which is sorted by right row.
    fn orphan_unmatched(&mut self, rights: Vec<usize>, out: &mut JoinOutput) -> Result<(), String> {
        if !matches!(self.join_type, JoinType::Right | JoinType::Full) {
            return Ok(());
        }
        for r in rights {
            let still_matched = self
                .join_index
                .iter()
                .any(|&(l, er)| l.is_some() && er == Some(r));
            if !still_matched {
                let pos = self.find_orphan_insert_position(r);
                self.insert_entry(pos, (None, Some(r)), Half::Live(None), Half::Live(Some(r)), out)?;
            }
        }
        Ok(())
    }

    /// LEFT/FULL: left rows that lost their last match get a placeholder.
    fn placehold_unmatched(&mut self, lefts: Vec<usize>, out: &mut JoinOutput) -> Result<(), String> {
        if !matches!(self.join_type, JoinType::Left | JoinType::Full) {
            return Ok(());
        }
        for l in lefts {
            let range = self.left_range(l);
            if range.is_empty() {
                let pos = range.start;
                self.insert_entry(pos, (Some(l), None), Half::Live(Some(l)), Half::Live(None), out)?;
            }
        }
        Ok(())
    }

    /// Insert left row `l`'s entries: one per matching right row (claiming
    /// any RIGHT/FULL orphan first), or the LEFT/FULL placeholder.
    fn insert_left_matches(
        &mut self,
        l: usize,
        matching: Option<&Vec<usize>>,
        left: &Row,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        match matching {
            Some(rights) => {
                if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                    for &r in rights {
                        let orphan = self
                            .join_index
                            .iter()
                            .position(|&(el, er)| el.is_none() && er == Some(r));
                        if let Some(pos) = orphan {
                            out.record(|| {
                                Ok(TableChange::RowDeleted {
                                    index: pos,
                                    data: self.output_row(Half::Live(None), Half::Live(Some(r)))?,
                                })
                            })?;
                            self.join_index.remove(pos);
                        }
                    }
                }
                let pos = self.find_left_insert_position(l);
                for (offset, &r) in rights.iter().enumerate() {
                    self.insert_entry(
                        pos + offset,
                        (Some(l), Some(r)),
                        Half::Given(left),
                        Half::Live(Some(r)),
                        out,
                    )?;
                }
            }
            None if matches!(self.join_type, JoinType::Left | JoinType::Full) => {
                let pos = self.find_left_insert_position(l);
                self.insert_entry(pos, (Some(l), None), Half::Given(left), Half::Live(None), out)?;
            }
            None => {}
        }
        Ok(())
    }

    /// Insert right row `r`'s entries: fill LEFT/FULL placeholders in place,
    /// add matched entries, or the RIGHT/FULL unmatched entry.
    fn insert_right_matches(
        &mut self,
        r: usize,
        matching: Option<&Vec<usize>>,
        right: &Row,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        match matching {
            Some(lefts) => {
                for &l in lefts {
                    let placeholder = self
                        .left_range(l)
                        .find(|&p| self.join_index[p].1.is_none());
                    if let Some(pos) = placeholder {
                        self.join_index[pos] = (Some(l), Some(r));
                        for column in &self.right_column_names {
                            let value = right.get(column).cloned().unwrap_or(ColumnValue::Null);
                            if value != ColumnValue::Null {
                                out.record(|| {
                                    Ok(TableChange::CellUpdated {
                                        row: pos,
                                        column: format!("right_{column}"),
                                        old_value: ColumnValue::Null,
                                        new_value: value,
                                    })
                                })?;
                            }
                        }
                    } else {
                        let pos = self.find_right_insert_position(l, r);
                        self.insert_entry(
                            pos,
                            (Some(l), Some(r)),
                            Half::Live(Some(l)),
                            Half::Given(right),
                            out,
                        )?;
                    }
                }
            }
            None if matches!(self.join_type, JoinType::Right | JoinType::Full) => {
                let pos = self.find_orphan_insert_position(r);
                self.insert_entry(pos, (None, Some(r)), Half::Live(None), Half::Given(right), out)?;
            }
            None => {}
        }
        Ok(())
    }

    /// Record one value update at each output position.
    fn update_cells(
        positions: impl IntoIterator<Item = usize>,
        column: String,
        old_value: &ColumnValue,
        new_value: &ColumnValue,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        for pos in positions {
            out.record(|| {
                Ok(TableChange::CellUpdated {
                    row: pos,
                    column: column.clone(),
                    old_value: old_value.clone(),
                    new_value: new_value.clone(),
                })
            })?;
        }
        Ok(())
    }
```

Note on `insert_left_matches`: `matching` is `Some` only for a non-empty
match list — callers pass `key.and_then(|k| lookup.get(&k))`, and lookups never
hold empty vectors.

- [ ] **Step 4: Replace the per-side loops with `apply_left_changes` / `apply_right_changes`**

Add to `impl JoinView`:

```rust
    /// Apply the left parent's batch in order, recording output events. The
    /// right side has no structural changes, so it is read live.
    fn apply_left_changes(
        &mut self,
        changes: &[TableChange],
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        let mut right_lookup: Option<HashMap<JoinKey, Vec<usize>>> = None;
        for (t, change) in changes.iter().enumerate() {
            match change {
                TableChange::RowDeleted { index, data } => {
                    let rights = self.remove_left_entries(*index, data, out)?;
                    for (l, _) in self.join_index.iter_mut() {
                        if let Some(l) = l {
                            if *l > *index {
                                *l -= 1;
                            }
                        }
                    }
                    self.orphan_unmatched(rights, out)?;
                }
                TableChange::RowInserted { index, data } => {
                    // Tail-insert fast path: join_index is sorted by left row
                    // with None-left entries last, so the max existing left row
                    // is the last Some(l) — usually found in O(1) from the end.
                    let max_existing_left = self.join_index.iter().rev().find_map(|(l, _)| *l);
                    if max_existing_left.is_some_and(|max_l| max_l >= *index) {
                        for (l, _) in self.join_index.iter_mut() {
                            if let Some(l) = l {
                                if *l >= *index {
                                    *l += 1;
                                }
                            }
                        }
                    }
                    let key = Self::build_composite_key(data, &self.left_keys);
                    let lookup = right_lookup.get_or_insert_with(|| self.build_right_lookup());
                    self.insert_left_matches(*index, key.and_then(|k| lookup.get(&k)), data, out)?;
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } if self.left_keys.contains(column) => {
                    let after =
                        row_after_update(changes, t, |i| self.left_table.borrow().get_row(i))?;
                    let mut before = after.clone();
                    before.insert(column.clone(), old_value.clone());
                    let new_key = Self::build_composite_key(&after, &self.left_keys);
                    if Self::build_composite_key(&before, &self.left_keys) == new_key {
                        let at = self.left_range(*row);
                        Self::update_cells(at, column.clone(), old_value, new_value, out)?;
                        continue;
                    }
                    let rights = self.remove_left_entries(*row, &before, out)?;
                    self.orphan_unmatched(rights, out)?;
                    let lookup = right_lookup.get_or_insert_with(|| self.build_right_lookup());
                    self.insert_left_matches(*row, new_key.and_then(|k| lookup.get(&k)), &after, out)?;
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } => {
                    let at = self.left_range(*row);
                    Self::update_cells(at, column.clone(), old_value, new_value, out)?;
                }
            }
        }
        Ok(())
    }

    /// Apply the right parent's batch in order, recording output events. The
    /// left side has no structural changes, so it is read live. A value-only
    /// batch (`structural == false`) maps right rows to positions once.
    fn apply_right_changes(
        &mut self,
        changes: &[TableChange],
        structural: bool,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        let mut left_lookup: Option<HashMap<JoinKey, Vec<usize>>> = None;
        let mut positions: Option<HashMap<usize, Vec<usize>>> = None;
        for (t, change) in changes.iter().enumerate() {
            match change {
                TableChange::RowDeleted { index, data } => {
                    let lefts = self.remove_right_entries(*index, data, out)?;
                    for (_, r) in self.join_index.iter_mut() {
                        if let Some(r) = r {
                            if *r > *index {
                                *r -= 1;
                            }
                        }
                    }
                    self.placehold_unmatched(lefts, out)?;
                }
                TableChange::RowInserted { index, data } => {
                    // Right rows are not monotonic in join_index (sorted by
                    // left), so the shift is a single conditional pass.
                    for (_, r) in self.join_index.iter_mut() {
                        if let Some(r) = r {
                            if *r >= *index {
                                *r += 1;
                            }
                        }
                    }
                    let key = Self::build_composite_key(data, &self.right_keys);
                    let lookup = left_lookup.get_or_insert_with(|| self.build_left_lookup());
                    self.insert_right_matches(*index, key.and_then(|k| lookup.get(&k)), data, out)?;
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } if self.right_keys.contains(column) => {
                    let after =
                        row_after_update(changes, t, |i| self.right_table.borrow().get_row(i))?;
                    let mut before = after.clone();
                    before.insert(column.clone(), old_value.clone());
                    let new_key = Self::build_composite_key(&after, &self.right_keys);
                    if Self::build_composite_key(&before, &self.right_keys) == new_key {
                        let at = self.right_positions(*row);
                        Self::update_cells(at, format!("right_{column}"), old_value, new_value, out)?;
                        continue;
                    }
                    let lefts = self.remove_right_entries(*row, &before, out)?;
                    self.placehold_unmatched(lefts, out)?;
                    let lookup = left_lookup.get_or_insert_with(|| self.build_left_lookup());
                    self.insert_right_matches(*row, new_key.and_then(|k| lookup.get(&k)), &after, out)?;
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } => {
                    let at = if structural {
                        self.right_positions(*row)
                    } else {
                        let map = positions.get_or_insert_with(|| self.right_position_map());
                        map.get(row).cloned().unwrap_or_default()
                    };
                    Self::update_cells(at, format!("right_{column}"), old_value, new_value, out)?;
                }
            }
        }
        Ok(())
    }
```

- [ ] **Step 5: Rewrite `sync()` around the new passes**

Replace everything in `sync()` from `let left_changes: Vec<TableChange> = left_changes.to_vec();`
through the end of the function with:

```rust
        let left_changes: Vec<TableChange> = left_changes.to_vec();
        let right_changes: Vec<TableChange> = right_changes.to_vec();
        let parent_versions = (left_table.version(), right_table.version());
        drop(left_table);
        drop(right_table);

        if left_changes.is_empty() && right_changes.is_empty() {
            // An upstream view can advance a parent's version without emitting
            // rows (a filter's excluded edit). Keep history, but record the
            // versions so children still see a coherent baseline.
            (self.last_left_parent_version, self.last_right_parent_version) = parent_versions;
            return false;
        }

        // Frame-mixing guard. The insert/key-update handlers match new rows
        // against LIVE lookups of the other parent, while shifts treat
        // existing join_index entries as pre-batch. Two batch shapes mix those
        // frames irreparably:
        //   1. Structural changes on BOTH sides in one batch.
        //   2. A key update recorded BEFORE an insert/delete on the same side.
        // Both are rare under tick()-driven usage; a full rebuild is correct
        // by construction and advances both cursors.
        let left_structural = Self::has_structural_changes(&left_changes, &self.left_keys);
        let right_structural = Self::has_structural_changes(&right_changes, &self.right_keys);
        if (left_structural && right_structural)
            || Self::key_update_precedes_row_shift(&left_changes, &self.left_keys)
            || Self::key_update_precedes_row_shift(&right_changes, &self.right_keys)
        {
            self.rebuild_index();
            return true;
        }

        // Past every rebuild fallback: this batch replaces retained history.
        self.output_changes.clear();
        let mut out = JoinOutput::default();
        // The value-only side goes first, so the structural side reads the
        // other parent live in the state consumers have already applied.
        let applied = if left_structural {
            self.apply_right_changes(&right_changes, false, &mut out)
                .and_then(|()| self.apply_left_changes(&left_changes, &mut out))
        } else {
            self.apply_left_changes(&left_changes, &mut out)
                .and_then(|()| self.apply_right_changes(&right_changes, right_structural, &mut out))
        };
        if applied.is_err() {
            // An unreadable parent row: rebuild rather than guess.
            self.rebuild_index();
            return true;
        }
        let emitted = out.emitted();
        if out.overflowed {
            self.output_changes.invalidate();
        } else {
            for change in out.changes {
                self.output_changes.push(change);
            }
        }

        let left_table = self.left_table.borrow();
        let right_table = self.right_table.borrow();
        self.left_last_processed_change_count = left_table
            .changeset()
            .map_or(usize::MAX, |cs| cs.total_len());
        self.right_last_processed_change_count = right_table
            .changeset()
            .map_or(usize::MAX, |cs| cs.total_len());
        self.last_left_parent_version = left_table.version();
        self.last_right_parent_version = right_table.version();
        drop(left_table);
        drop(right_table);
        self.sync_count += 1;

        emitted
    }
```

Update the `sync()` doc comment to: "Incrementally sync with both parents'
changes, recording output history. Returns true if the output changed (rows
moved or any value changed)."

Add to `impl ReadableTable for JoinView`:

```rust
    fn changeset(&self) -> Option<&Changeset> {
        // A child built while this join is stale has no coherent delta
        // baseline. It must refresh once we synchronize the parents.
        (self.left_table.borrow().version() == self.last_left_parent_version
            && self.right_table.borrow().version() == self.last_right_parent_version)
            .then_some(&self.output_changes)
    }
```

Delete `use std::collections::HashSet` only if the compiler reports it unused
(it is still used by `rebuild_index`).

- [ ] **Step 6: Run the contract tests and the existing join tests**

Run: `cd impl && cargo test --features server --test join_pipeline 2>&1 | grep -E 'FAILED|panicked|test result'`
Expected: `test result: ok. 18 passed`.

Run: `cd impl && cargo test --lib --features server join 2>&1 | grep -E 'FAILED|panicked|test result'`
Expected: all pass (existing join unit tests, including the order-preservation tests).

- [ ] **Step 7: Clippy and commit**

Run: `cd impl && cargo clippy --all-targets --features server -- -D warnings 2>&1 | tail -3`
Expected: `Finished` with no warnings.

```bash
git add impl/src/view/join.rs impl/tests/join_pipeline.rs
git commit -m "Publish join output changesets

JoinView records one event per join_index mutation in join coordinates,
fans non-key updates out to every output row of the parent row, applies the
value-only side first, and invalidates history on rebuild or when a batch
would exceed 512 events. sync() now also reports value-only edits.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Fuzz replay checks and a dimension-join DAG

**Files:**
- Modify: `impl/tests/forward_prop_fuzz.rs`

**Interfaces:**
- Consumes: `JoinView: ReadableTable` with `changeset()` from Task 1; existing
  fuzz helpers `snapshot`, `assert_replay`, `capture`, `assert_replays`,
  `multiset`, `assert_ordered_eq`, `assert_agg_eq`, `passes`, `sort_keys`, `aggs`.
- Produces: nothing used elsewhere.

- [ ] **Step 1: Strict replay in the single-change join fuzz**

In `differential_join_fuzz`, at the top of the `for step` loop add:

```rust
                let before = snapshot(&joined);
                let cursor = joined.changeset().expect("synced join exposes history").total_len();
```

and immediately after `joined.sync();` add:

```rust
                // One parent change per step: never a rebuild, so history
                // must always replay.
                assert_replay(&format!("join:{jt_name}"), trial, step, before, cursor, &joined);
```

- [ ] **Step 2: Replay-or-invalidated in the batched join fuzz, with a rate floor**

Add after `assert_replays`:

```rust
/// Replay history when the batch kept it and report whether it did. Rebuilds
/// (both join parents changed structure, or a key update preceded a shift)
/// invalidate it instead.
fn replay_unless_invalidated(
    label: &str,
    trial: u64,
    step: usize,
    rows: Vec<Row>,
    cursor: usize,
    view: &dyn ReadableTable,
) -> bool {
    let kept = view
        .changeset()
        .unwrap_or_else(|| panic!("[{label}] trial {trial} step {step}: no history after sync"))
        .changes_from(cursor)
        .is_some();
    if kept {
        assert_replay(label, trial, step, rows, cursor, view);
    }
    kept
}
```

In `differential_join_batched_fuzz`, declare `let (mut kept, mut steps) = (0usize, 0usize);`
before `for trial`, take `before`/`cursor` as in Step 1 at the top of each step,
and after `joined.sync();` add:

```rust
                steps += 1;
                if replay_unless_invalidated(&format!("join_batched:{jt_name}"), trial, step, before, cursor, &joined) {
                    kept += 1;
                }
```

After the `for trial` loop (still inside `for (jt_name, jt)`), add:

```rust
        // An implementation that always invalidated would pass the replay
        // check; most small batches must keep history.
        assert!(
            kept * 4 >= steps,
            "[join_batched:{jt_name}] history kept in only {kept}/{steps} batches"
        );
```

Run `cd impl && cargo test --features server --test forward_prop_fuzz differential_join -- --nocapture`
and confirm both pass. If the floor fails, print `kept/steps` per join type and
investigate unexpected rebuilds before touching the bound.

- [ ] **Step 3: Dimension join with filter/sort/aggregate children in the DAG fuzz**

Add helpers near `region_join`:

```rust
/// Region tiers for a LEFT join. South has no tier, so its orders carry Null
/// right columns and group under a Null tier.
fn region_dim() -> Rc<RefCell<Table>> {
    let mut dim = Table::new(
        "regions".to_string(),
        Schema::new(vec![
            ("region".to_string(), ColumnType::String, false),
            ("tier".to_string(), ColumnType::Int32, false),
        ]),
    );
    for (region, tier) in [("West", 1), ("East", 2), ("North", 1)] {
        dim.append_row(HashMap::from([
            ("region".to_string(), ColumnValue::String(region.to_string())),
            ("tier".to_string(), ColumnValue::Int32(tier)),
        ]))
        .unwrap();
    }
    Rc::new(RefCell::new(dim))
}

fn dim_join(base: Rc<RefCell<Table>>, dim: Rc<RefCell<Table>>, name: &str) -> JoinView {
    JoinView::new(name.to_string(), base, dim, "region".to_string(), "region".to_string(), JoinType::Left).unwrap()
}
```

Add `Pipeline` fields:

```rust
    /// Static dimension table joined to every order (never mutated).
    dim: Rc<RefCell<Table>>,
    /// Orders LEFT-joined to region tiers, with children over the join.
    dim_join: Rc<RefCell<JoinView>>,
    join_filter: Rc<RefCell<FilterView>>,
    join_sorted: Rc<RefCell<SortedView>>,
    join_groups: Rc<RefCell<AggregateView>>,
```

Add a method on `Pipeline`:

```rust
    /// The dimension join and its children. The join rebuilds when a batch
    /// updates a region before inserting or deleting an order.
    fn join_replayed(&self) -> Vec<(&'static str, Rc<RefCell<dyn ReadableTable>>)> {
        vec![
            ("dim_join", self.dim_join.clone()),
            ("join_filter", self.join_filter.clone()),
            ("join_sorted", self.join_sorted.clone()),
            ("join_groups", self.join_groups.clone()),
        ]
    }
```

In `build_pipeline`, before the `TickableTable`:

```rust
    let dim = region_dim();
    let dim_join_view = Rc::new(RefCell::new(dim_join(base.clone(), dim.clone(), "dj")));
    let join_filter = Rc::new(RefCell::new(FilterView::new("jf".to_string(), dim_join_view.clone(), passes)));
    let join_sorted = Rc::new(RefCell::new(
        SortedView::new("js".to_string(), dim_join_view.clone(), sort_keys()).unwrap(),
    ));
    let join_groups = Rc::new(RefCell::new(
        AggregateView::new("jg".to_string(), dim_join_view.clone(), vec!["right_tier".to_string()], aggs()).unwrap(),
    ));
```

and after `tick.register_join_as_left(&agg_join);`:

```rust
    tick.register_join_as_left(&dim_join_view);
    tick.register_filter(&join_filter);
    tick.register_sorted(&join_sorted);
    tick.register_aggregate(&join_groups);
```

Add `dim, dim_join: dim_join_view, join_filter, join_sorted, join_groups,` to
the `Pipeline { .. }` literal.

At the end of `assert_pipeline_matches`:

```rust
    let odj = Rc::new(RefCell::new(dim_join(ob.clone(), p.dim.clone(), "odj")));
    assert_eq!(
        multiset(&snapshot(&*p.dim_join.borrow())),
        multiset(&snapshot(&*odj.borrow())),
        "[dim_join] trial {trial} step {step}"
    );
    // Join order may differ from a rebuild, so compare the filter order-free.
    let ojf = FilterView::new("ojf".to_string(), odj.clone(), passes);
    assert_eq!(
        multiset(&snapshot(&*p.join_filter.borrow())),
        multiset(&snapshot(&ojf)),
        "[join_filter] trial {trial} step {step}"
    );
    let ojs = SortedView::new("ojs".to_string(), odj.clone(), sort_keys()).unwrap();
    assert_ordered_eq("join_sorted", trial, step, &snapshot(&*p.join_sorted.borrow()), &snapshot(&ojs));
    let ojg = AggregateView::new("ojg".to_string(), odj.clone(), vec!["right_tier".to_string()], aggs()).unwrap();
    assert_agg_eq("join_groups", trial, step, "right_tier", &snapshot(&*p.join_groups.borrow()), &snapshot(&ojg));
```

In `differential_chained_forward_prop_fuzz` (one change per tick, so the
dimension join never rebuilds), capture and assert both lists strictly:

```rust
            let before = capture(p.replayed());
            let join_before = capture(p.join_replayed());
            apply_random_op(&mut rng, &base, &mut next_id);
            p.tick.tick();
            assert_replays(trial, step, before);
            assert_replays(trial, step, join_before);
```

In `differential_batched_forward_prop_fuzz`, capture `join_before` the same way
and after `assert_replays(trial, step, before);` add:

```rust
            for (label, view, rows, cursor) in join_before {
                replay_unless_invalidated(label, trial, step, rows, cursor, &*view.borrow());
            }
```

Update the module doc's Coverage list with:
`- Filter, sort, and aggregate over a LEFT join to a static dimension table (join output history)`
and the replay sentence to include "join and join-child output history".

- [ ] **Step 4: Run the fuzz**

Run: `cd impl && cargo test --release --features server --test forward_prop_fuzz 2>&1 | grep -E 'FAILED|panicked|test result'`
Expected: `test result: ok. 6 passed`.

- [ ] **Step 5: Mutation checks (planted bugs must fail)**

For each edit below: copy `impl/src/view/join.rs` to the scratchpad, apply the
edit, run
`cd impl && cargo test --release --features server --no-fail-fast --test join_pipeline --test forward_prop_fuzz 2>&1 | grep -E '^test .*FAILED|test result'`,
confirm at least one failure, then restore the copy and grep that the original
line is back.

1. `remove_right_entries`: `index: p - removed` → `index: p`.
2. `sync`: `let applied = if left_structural {` → `let applied = if false {`.
3. `insert_right_matches`: `if value != ColumnValue::Null {` → `if false {`.
4. `sync` empty-batch branch: delete the line
   `(self.last_left_parent_version, self.last_right_parent_version) = parent_versions;`.

- [ ] **Step 6: Commit**

```bash
git add impl/tests/forward_prop_fuzz.rs
git commit -m "Replay join history in the differential fuzz

Single-change join fuzz replays every step; the batched join fuzz replays
unless a rebuild invalidated history, with a kept-history floor. The DAG fuzz
adds filter/sort/aggregate children of a LEFT join to a dimension table.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Measure costs

**Files:**
- Create then delete: `impl/examples/join_history_probe.rs` (throwaway)
- Modify: `docs/superpowers/specs/2026-09-27-join-output-changesets-design.md`

- [ ] **Step 1: Write the probe**

```rust
// Throwaway probe: children of a join replay vs rebuild; the join's own cost
// for a batch of right-side value updates.
use livetable::{
    AggregateFunction, AggregateView, ColumnType, ColumnValue, FilterView, JoinType, JoinView,
    Schema, Table,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

fn time(f: impl FnOnce()) -> Duration {
    let start = Instant::now();
    f();
    start.elapsed()
}

fn main() {
    let n = 100_000;
    let customers = 10_000;
    let mut orders = Table::new(
        "orders".into(),
        Schema::new(vec![
            ("oid".into(), ColumnType::Int32, false),
            ("cust".into(), ColumnType::Int32, false),
            ("amount".into(), ColumnType::Int32, false),
        ]),
    );
    for i in 0..n {
        orders
            .append_row(HashMap::from([
                ("oid".into(), ColumnValue::Int32(i)),
                ("cust".into(), ColumnValue::Int32(i % customers)),
                ("amount".into(), ColumnValue::Int32(i % 1000)),
            ]))
            .unwrap();
    }
    let mut people = Table::new(
        "customers".into(),
        Schema::new(vec![
            ("cid".into(), ColumnType::Int32, false),
            ("name".into(), ColumnType::String, false),
        ]),
    );
    for i in 0..customers {
        people
            .append_row(HashMap::from([
                ("cid".into(), ColumnValue::Int32(i)),
                ("name".into(), ColumnValue::String(format!("c{i}"))),
            ]))
            .unwrap();
    }
    let orders = Rc::new(RefCell::new(orders));
    let people = Rc::new(RefCell::new(people));
    let joined = Rc::new(RefCell::new(
        JoinView::new("j".into(), orders.clone(), people.clone(), "cust".into(), "cid".into(), JoinType::Inner).unwrap(),
    ));
    let mut filter = FilterView::new("f".into(), joined.clone(), |r| {
        r["amount"].as_i32().is_some_and(|a| a >= 900)
    });
    let mut totals = AggregateView::new(
        "t".into(),
        joined.clone(),
        vec!["right_name".into()],
        vec![("total".into(), "amount".into(), AggregateFunction::Sum)],
    )
    .unwrap();

    let mut children = |label: &str| {
        joined.borrow_mut().sync();
        let replay = time(|| {
            filter.sync();
            totals.sync();
        });
        let rebuild = time(|| {
            filter.refresh();
            totals.refresh();
        });
        println!("{label:<28} children replay={replay:>10.2?} rebuild={rebuild:>10.2?}");
    };
    orders.borrow_mut().set_value(5, "amount", ColumnValue::Int32(950)).unwrap();
    children("warm-up value edit");
    orders.borrow_mut().set_value(77, "amount", ColumnValue::Int32(951)).unwrap();
    children("one left value edit");
    orders
        .borrow_mut()
        .append_row(HashMap::from([
            ("oid".into(), ColumnValue::Int32(n)),
            ("cust".into(), ColumnValue::Int32(3)),
            ("amount".into(), ColumnValue::Int32(990)),
        ]))
        .unwrap();
    children("one left insert");

    for i in 0..256 {
        people
            .borrow_mut()
            .set_value(i, "name", ColumnValue::String(format!("renamed{i}")))
            .unwrap();
    }
    let sync = time(|| {
        joined.borrow_mut().sync();
    });
    let rebuild = time(|| joined.borrow_mut().refresh());
    println!("256 right value edits        join sync={sync:>10.2?} rebuild={rebuild:>10.2?}");
}
```

- [ ] **Step 2: Run it**

Run: `cd impl && cargo run --release --example join_history_probe`
Expected: children replay far below rebuild for single edits; the join's sync
for 256 right edits (2,560 output updates → overflow, invalidated) stays below
its rebuild. Record the printed numbers.

- [ ] **Step 3: Delete the probe and record results**

Run: `rm impl/examples/join_history_probe.rs`

Change the spec's `**Status:**` line to `Implemented 2026-09-27` and add an
"Implementation notes" section after "Goal"/intro with the measured numbers
(Apple silicon, release build, 100k orders × 10k customers, INNER) and any
deviations from the design found while implementing.

---

### Task 4: Documentation, Python check, CI, final verification

**Files:**
- Modify: `CLAUDE.md`, `impl/src/readable.rs`, `impl/src/lib.rs`,
  `docs/JOIN_FEATURE.md`, `docs/API_GUIDE.md`, `docs/ORIGINAL_VISION.md`,
  `docs/PYTHON_BINDINGS_README.md`, `docs/INCREMENTAL_FILTER_PIPELINE.md`,
  `docs/INCREMENTAL_SORTED_PIPELINE.md` (only where they say join children
  rebuild), `tests/README.md`, `.github/workflows/ci.yml`, `tests/run_all.sh`,
  `tests/python/test_right_full_joins.py`

- [ ] **Step 1: Python test for the `sync()` return value**

Append to the class that holds `test_join_sync_exposed` in
`tests/python/test_right_full_joins.py`:

```python
    def test_join_sync_reports_value_only_edits(self):
        """sync() returns True after a non-key edit: joined values changed."""
        users, orders = make_users_orders()
        joined = livetable.JoinView(
            "value_sync", users, orders, "user_id", "user_id", livetable.JoinType.LEFT
        )
        users.set_value(0, "name", "Renamed")
        assert joined.sync() is True
        assert joined.sync() is False
```


- [ ] **Step 2: Build the wheel and run the Python suite**

```bash
cd impl && env SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk \
  CARGO_TARGET_DIR=<scratchpad>/target-ld26 PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 \
  <scratchpad>/venv/bin/maturin build --release -o <scratchpad>/wheels3
<scratchpad>/venv/bin/pip install --force-reinstall <scratchpad>/wheels3/livetable-*.whl
cd ../tests && <scratchpad>/venv/bin/python -m pytest python/ integration/ -q -p no:cacheprovider
```
Expected: all pass (435 = 434 + 1 new).

- [ ] **Step 3: CI and local runner**

In `.github/workflows/ci.yml`, change the propagation step's command to:
`cargo test --manifest-path impl/Cargo.toml --features server --test sorted_pipeline --test aggregate_pipeline --test join_pipeline --test forward_prop_fuzz`.
In `tests/run_all.sh`, append `--test join_pipeline` to the
`cargo test --features server --test filter_pipeline --test sorted_pipeline --test aggregate_pipeline` line.
In CLAUDE.md's "Propagation contracts" command, add `--test join_pipeline`.
In `tests/README.md`, list `join_pipeline` next to `aggregate_pipeline`
wherever the contract targets are listed.

- [ ] **Step 4: Update docs**

- `CLAUDE.md` Key Patterns: "Tables and synchronized filters/sorts/aggregates
  expose changesets" → "…filters/sorts/aggregates/joins…". Replace the join
  line's history sentence with: "JoinView publishes history in join
  coordinates (`join_index` positions, `right_`-prefixed right columns): one
  event per index mutation, non-key updates fanned out to every output row of
  the parent row, value-only side applied first so the structural side reads
  the other parent live. Batches over 512 output events invalidate. Rebuilds
  (both sides structural, key update before a shift, unreadable rows)
  invalidate." Keep the O(N+M+R) and bitwise-float sentences.
- `impl/src/readable.rs` and `impl/src/lib.rs` module docs: add joins to the
  views that expose changesets.
- `docs/JOIN_FEATURE.md` incremental section (around the "version-checked
  rebuilds" sentence): describe the output history and that children replay.
- `docs/API_GUIDE.md`: wherever the history contract lists filter/sort/aggregate.
- `docs/ORIGINAL_VISION.md`: line 242 add joins to the views that emit
  changesets; add `- [x] JoinView output changesets; filter/sort/aggregate/join
  children of joins replay instead of rebuilding` after the aggregate line;
  change the Planned item to `- [ ] Output changesets for projection and
  computed views`.
- `docs/PYTHON_BINDINGS_README.md`: `JoinView.sync()` returns True after
  value-only edits.
- `docs/INCREMENTAL_*_PIPELINE.md`: fix any sentence saying join children
  rebuild (grep `-i 'join'`).

- [ ] **Step 5: Full verification**

```bash
cd impl && cargo clippy --all-targets -- -D warnings \
  && cargo clippy --all-targets --features server -- -D warnings \
  && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo clippy --all-targets --features python -- -D warnings
cd impl && cargo test --features server 2>&1 | grep -E 'test result|FAILED|panicked'
rustfmt --edition 2021 --check src/view/join.rs tests/join_pipeline.rs
```
Expected: clippy clean ×3, every test target ok, the two files formatted
(`forward_prop_fuzz.rs` is not rustfmt-clean at HEAD; do not reformat it).

- [ ] **Step 6: Commit**

```bash
git add -A CLAUDE.md impl/src/readable.rs impl/src/lib.rs docs tests .github
git commit -m "Document join output changesets and run join_pipeline in CI

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
