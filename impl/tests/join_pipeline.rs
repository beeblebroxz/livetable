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
    HashMap::from([
        ("cid".into(), Int32(cid)),
        ("name".into(), name(customer_name)),
    ])
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
    assert_eq!(
        rows,
        snapshot(&*view),
        "replayed history reproduces the join"
    );
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
    assert_eq!(
        replayed(&mut inner, inner_base),
        [inserted(1, matched.clone())]
    );
    assert_eq!(
        replayed(&mut right, right_base),
        [
            deleted(1, out_row(None, Some((20, "b")))),
            inserted(1, matched)
        ]
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

    let (_, cursor) = baseline(&*joined.borrow());
    for oid in 0..600 {
        o.borrow_mut()
            .inner
            .append_row(order(oid, Some(10), oid))
            .unwrap();
    }
    o.borrow().reads.set(0);
    assert!(joined.borrow_mut().sync());
    assert!(
        !history_kept(&*joined.borrow(), cursor),
        "600 inserts overflow"
    );
    assert_eq!(
        o.borrow().reads.get(),
        0,
        "no rebuild: left rows are not reread"
    );
    assert_eq!(joined.borrow().len(), 600);

    // One right edit fans out to 600 rows: history overflows, a child refreshes.
    let big = |row: &Row| row["amount"].as_i32().is_some_and(|amount| amount >= 300);
    let mut child = FilterView::new("big".into(), joined.clone(), big);
    let (_, cursor) = baseline(&*joined.borrow());
    c.borrow_mut().set_value(0, "name", name("z")).unwrap();
    assert!(joined.borrow_mut().sync());
    assert!(
        !history_kept(&*joined.borrow(), cursor),
        "600 updates overflow"
    );
    child.sync();
    let fresh = FilterView::new("fresh".into(), joined.clone(), big);
    assert_eq!(snapshot(&child), snapshot(&fresh));
    assert!(snapshot(&child)
        .iter()
        .all(|row| row["right_name"] == name("z")));
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
    let sums = || {
        vec![(
            "total".to_string(),
            "amount".to_string(),
            AggregateFunction::Sum,
        )]
    };
    let mut filtered = FilterView::new("big".into(), joined.clone(), big);
    let mut ranked = SortedView::new("ranked".into(), joined.clone(), by_amount()).unwrap();
    let mut totals = AggregateView::new(
        "totals".into(),
        joined.clone(),
        vec!["right_name".into()],
        sums(),
    )
    .unwrap();

    // Warm-up edit: the aggregate builds its row-to-group index lazily.
    for (row, amount) in [(0, 1000), (42, 1001)] {
        o.borrow_mut()
            .set_value(row, "amount", Int32(amount))
            .unwrap();
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
    let fresh_totals = AggregateView::new(
        "t".into(),
        joined.clone(),
        vec!["right_name".into()],
        sums(),
    )
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
