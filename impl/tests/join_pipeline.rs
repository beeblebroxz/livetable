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
    Rc::new(RefCell::new(customers_table(rows)))
}

fn customers_table(rows: &[(i32, &str)]) -> Table {
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
    table
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

    // A two-sided batch over the per-side bound rebuilds.
    let (_, cursor) = baseline(&inner);
    for oid in 2..259 {
        o.borrow_mut().append_row(order(oid, Some(20), 2)).unwrap();
    }
    c.borrow_mut().append_row(customer(20, "b")).unwrap();
    assert!(inner.sync());
    assert!(!history_kept(&inner, cursor), "a rebuild invalidates");
    assert_eq!(inner.len(), 258);
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

    // An insert is structural on both sides at once: the two-phase sync
    // pairs the new row with itself once, in phase 2, and keeps history.
    let base = baseline(&pairs);
    let cursor = base.1;
    c.borrow_mut().append_row(customer(30, "c")).unwrap();
    replayed(&mut pairs, base);
    assert!(history_kept(&pairs, cursor));
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

/// Rows (id, k) with a join key `k` of the given type.
fn keyed(name: &str, key_type: ColumnType, rows: &[(i32, ColumnValue)]) -> Rc<RefCell<Table>> {
    let mut table = Table::new(
        name.into(),
        Schema::new(vec![
            ("id".into(), ColumnType::Int32, false),
            ("k".into(), key_type, false),
        ]),
    );
    for (id, k) in rows {
        table
            .append_row(HashMap::from([
                ("id".into(), Int32(*id)),
                ("k".into(), k.clone()),
            ]))
            .unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

/// Rows (id, k) with a Float64 join key `k`.
fn float_keyed(name: &str, rows: &[(i32, f64)]) -> Rc<RefCell<Table>> {
    let rows: Vec<_> = rows
        .iter()
        .map(|&(id, k)| (id, ColumnValue::Float64(k)))
        .collect();
    keyed(name, ColumnType::Float64, &rows)
}

fn float_join(left: &Rc<RefCell<Table>>, right: &Rc<RefCell<Table>>, how: JoinType) -> JoinView {
    JoinView::new(
        "j".into(),
        left.clone(),
        right.clone(),
        "k".into(),
        "k".into(),
        how,
    )
    .unwrap()
}

#[test]
fn signed_zero_insert_fills_the_placeholder() {
    let l = float_keyed("l", &[(1, -0.0)]);
    let r = float_keyed("r", &[]);
    let mut left = float_join(&l, &r, JoinType::Left);
    let base = baseline(&left);
    r.borrow_mut()
        .append_row(HashMap::from([
            ("id".into(), Int32(10)),
            ("k".into(), ColumnValue::Float64(0.0)),
        ]))
        .unwrap();
    let changes = replayed(&mut left, base);
    let mut filled: Vec<&str> = changes
        .iter()
        .map(|change| match change {
            TableChange::CellUpdated {
                row: 0,
                column,
                old_value: Null,
                ..
            } => column.as_str(),
            other => panic!("expected a placeholder fill, got {other:?}"),
        })
        .collect();
    filled.sort();
    assert_eq!(filled, ["right_id", "right_k"]);
    assert_eq!(left.len(), 1);
}

#[test]
fn signed_zero_key_update_rematches() {
    let l = float_keyed("l", &[(1, 0.0)]);
    let r = float_keyed("r", &[(10, 1.0)]);
    let mut inner = float_join(&l, &r, JoinType::Inner);
    let base = baseline(&inner);
    r.borrow_mut()
        .set_value(0, "k", ColumnValue::Float64(-0.0))
        .unwrap();
    let changes = replayed(&mut inner, base);
    assert!(
        matches!(
            changes.as_slice(),
            [TableChange::RowInserted { index: 0, .. }]
        ),
        "{changes:?}"
    );
    assert_eq!(inner.len(), 1);
}

#[test]
fn signed_zero_key_update_in_place() {
    let l = float_keyed("l", &[(1, 0.0)]);
    let r = float_keyed("r", &[(10, -0.0)]);
    let mut inner = float_join(&l, &r, JoinType::Inner);
    let base = baseline(&inner);
    r.borrow_mut()
        .set_value(0, "k", ColumnValue::Float64(0.0))
        .unwrap();
    let changes = replayed(&mut inner, base);
    // ColumnValue equality treats the zeros as equal, so check the old sign too.
    assert_eq!(
        changes,
        [update(
            0,
            "right_k",
            ColumnValue::Float64(-0.0),
            ColumnValue::Float64(0.0)
        )]
    );
    assert!(matches!(
        changes[0],
        TableChange::CellUpdated { old_value: ColumnValue::Float64(v), .. } if v.is_sign_negative()
    ));
}

#[test]
fn signed_zero_left_insert_matches_existing_rows() {
    let zeros = [
        (
            ColumnType::Float64,
            ColumnValue::Float64(-0.0),
            ColumnValue::Float64(0.0),
        ),
        (
            ColumnType::Float32,
            ColumnValue::Float32(-0.0),
            ColumnValue::Float32(0.0),
        ),
    ];
    for (key_type, negative, positive) in zeros {
        for how in [JoinType::Inner, JoinType::Full] {
            let l = keyed("l", key_type, &[]);
            let r = keyed("r", key_type, &[(10, positive.clone())]);
            let mut joined =
                JoinView::new("j".into(), l.clone(), r, "k".into(), "k".into(), how).unwrap();
            let base = baseline(&joined);
            l.borrow_mut()
                .append_row(HashMap::from([
                    ("id".into(), Int32(1)),
                    ("k".into(), negative.clone()),
                ]))
                .unwrap();
            replayed(&mut joined, base);
            assert_eq!(
                joined.len(),
                1,
                "{key_type:?} {how:?}: -0.0 joins the existing 0.0 row"
            );
            let row = joined.get_row(0).unwrap();
            assert_eq!((&row["id"], &row["right_id"]), (&Int32(1), &Int32(10)));
            let left_key_negative = match row["k"] {
                ColumnValue::Float64(v) => v.is_sign_negative(),
                ColumnValue::Float32(v) => v.is_sign_negative(),
                ref other => panic!("unexpected key {other:?}"),
            };
            assert!(
                left_key_negative,
                "{key_type:?} {how:?}: output keeps the stored -0.0"
            );
        }
    }
}

/// One sync where the left changes structure and the right only edits values
/// (as for a join over an aggregate of its own table): the value edit lands on
/// the pre-batch position, and later matches still see the remaining order.
#[test]
fn value_edits_land_while_the_left_side_shifts() {
    let o = orders(&[(1, Some(10), 5), (2, Some(20), 7)]);
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut left = join(o.clone(), c.clone(), JoinType::Left);
    let base = baseline(&left);
    c.borrow_mut().set_value(1, "name", name("z")).unwrap();
    o.borrow_mut().delete_row(0).unwrap();
    replayed(&mut left, base);
    assert_eq!(left.len(), 1);
    assert_eq!(left.get_row(0).unwrap()["right_name"], name("z"));

    let base = baseline(&left);
    c.borrow_mut().append_row(customer(20, "c")).unwrap();
    replayed(&mut left, base);
    assert_eq!(left.len(), 2, "order 2 matches both customers with cid 20");
}

/// Rows as sorted canonical strings, for order-free comparison.
fn rows_sorted(view: &dyn ReadableTable) -> Vec<String> {
    let mut rows: Vec<String> = snapshot(view)
        .into_iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
            cells.sort();
            cells.join("|")
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn one_sided_inserts_read_only_matched_rows() {
    let order_rows: Vec<Order> = (0..50).map(|i| (i, Some(i % 25), i)).collect();
    let o = Rc::new(RefCell::new(Counted {
        inner: orders_table(&order_rows),
        reads: Cell::new(0),
    }));
    let names: Vec<String> = (0..25).map(|i| format!("c{i}")).collect();
    let customer_rows: Vec<(i32, &str)> =
        (0..25).map(|i| (i, names[i as usize].as_str())).collect();
    let c = Rc::new(RefCell::new(Counted {
        inner: customers_table(&customer_rows),
        reads: Cell::new(0),
    }));
    let mut joined = join(o.clone(), c.clone(), JoinType::Inner);
    // The first insert on each side may build per-side state.
    o.borrow_mut()
        .inner
        .append_row(order(100, Some(0), 1))
        .unwrap();
    joined.sync();
    c.borrow_mut()
        .inner
        .append_row(customer(25, "c25"))
        .unwrap();
    joined.sync();

    let base = baseline(&joined);
    o.borrow_mut()
        .inner
        .append_row(order(101, Some(3), 1))
        .unwrap();
    c.borrow().reads.set(0);
    joined.sync();
    let reads = c.borrow().reads.get();
    assert!(reads <= 4, "an order insert read {reads} customer rows");
    replayed(&mut joined, base);

    let base = baseline(&joined);
    c.borrow_mut()
        .inner
        .append_row(customer(7, "dup7"))
        .unwrap(); // matches orders 7 and 32
    o.borrow().reads.set(0);
    joined.sync();
    let reads = o.borrow().reads.get();
    assert!(reads <= 6, "a customer insert read {reads} order rows");
    replayed(&mut joined, base);
}

#[test]
fn a_customers_only_order_can_be_deleted_and_readded() {
    for how in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
    ] {
        let o = orders(&[(1, Some(10), 5), (2, Some(20), 7)]);
        let c = customers(&[(10, "a"), (20, "b")]);
        let mut joined = join(o.clone(), c.clone(), how);
        let base = baseline(&joined);
        o.borrow_mut().delete_row(1).unwrap(); // customer 20's only order
        replayed(&mut joined, base);
        assert_eq!(
            rows_sorted(&joined),
            rows_sorted(&join(o.clone(), c.clone(), how)),
            "{how:?} delete"
        );
        let base = baseline(&joined);
        o.borrow_mut().append_row(order(2, Some(20), 7)).unwrap();
        replayed(&mut joined, base);
        assert_eq!(
            rows_sorted(&joined),
            rows_sorted(&join(o.clone(), c.clone(), how)),
            "{how:?} re-add"
        );
    }
}

/// A parent over a real table's rows whose changeset the test writes by hand.
struct Scripted {
    inner: Table,
    changes: Changeset,
    version: u64,
}

impl ReadableTable for Scripted {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn column_names(&self) -> Vec<String> {
        self.inner.column_names()
    }
    fn get_row(&self, index: usize) -> Result<Row, String> {
        self.inner.get_row(index)
    }
    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        self.inner.get_value(row, column)
    }
    fn version(&self) -> u64 {
        self.version
    }
    fn changeset(&self) -> Option<&Changeset> {
        Some(&self.changes)
    }
}

#[test]
fn out_of_range_events_rebuild_instead_of_panicking() {
    let cases = [
        ("delete at len", deleted(2, order(1, Some(10), 5))),
        ("insert past len", inserted(3, order(9, Some(10), 1))),
        ("update at len", update(2, "amount", Int32(5), Int32(6))),
    ];
    for (label, change) in cases {
        let left = Rc::new(RefCell::new(Scripted {
            inner: orders_table(&[(1, Some(10), 5), (2, Some(20), 7)]),
            changes: Changeset::new(),
            version: 0,
        }));
        let c = customers(&[(10, "a"), (20, "b")]);
        let mut joined = join(left.clone(), c.clone(), JoinType::Left);
        let cursor = joined.changeset().expect("history").total_len();
        {
            let mut left = left.borrow_mut();
            left.changes.push(change);
            left.version += 1;
        }
        joined.sync();
        assert!(
            joined
                .changeset()
                .expect("synced")
                .changes_from(cursor)
                .is_none(),
            "{label}: an out-of-range event must rebuild"
        );
        assert_eq!(
            rows_sorted(&joined),
            rows_sorted(&join(left.clone(), c, JoinType::Left)),
            "{label}"
        );
    }
}

/// A parent whose changeset disagrees with its live rows must make the join
/// rebuild, not panic: here the changeset claims an insert the rows never got,
/// and a later lazy build reads the shorter live table.
#[test]
fn a_parent_that_disagrees_with_its_changeset_rebuilds_instead_of_panicking() {
    let left = Rc::new(RefCell::new(Scripted {
        inner: orders_table(&[(1, Some(10), 5), (2, Some(20), 7)]),
        changes: Changeset::new(),
        version: 0,
    }));
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut joined = join(left.clone(), c.clone(), JoinType::Left);
    {
        let mut left = left.borrow_mut();
        left.changes.push(inserted(2, order(3, Some(10), 1)));
        left.version += 1;
    }
    joined.sync();
    c.borrow_mut().append_row(customer(30, "x")).unwrap();
    joined.sync(); // builds the left key hashes from the live rows
    {
        let mut left = left.borrow_mut();
        left.changes.push(deleted(2, order(3, Some(10), 1)));
        left.version += 1;
    }
    joined.sync();
    assert_eq!(
        rows_sorted(&joined),
        rows_sorted(&join(left.clone(), c, JoinType::Left))
    );
}

/// A key update recorded before an insert on the same side: the key-update
/// handler reconstructs its row with `row_after_update`, so this applies
/// incrementally instead of rebuilding.
#[test]
fn a_key_update_before_an_insert_keeps_history() {
    for how in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
    ] {
        let o = orders(&[(1, Some(10), 5), (2, Some(20), 7)]);
        let c = customers(&[(10, "a"), (20, "b")]);
        let mut joined = join(o.clone(), c.clone(), how);
        let base = baseline(&joined);
        let cursor = base.1;
        o.borrow_mut().set_value(0, "cust", Int32(20)).unwrap();
        o.borrow_mut().insert_row(0, order(3, Some(10), 1)).unwrap();
        replayed(&mut joined, base);
        assert!(history_kept(&joined, cursor), "{how:?}: history kept");
        assert_eq!(
            rows_sorted(&joined),
            rows_sorted(&join(o.clone(), c.clone(), how)),
            "{how:?}"
        );
    }
}

const ALL_JOINS: [JoinType; 4] = [
    JoinType::Inner,
    JoinType::Left,
    JoinType::Right,
    JoinType::Full,
];

#[test]
fn a_new_customer_and_first_order_sync_together() {
    for how in ALL_JOINS {
        let o = orders(&[(1, Some(10), 5), (2, Some(20), 7)]);
        let c = customers(&[(10, "a"), (20, "b")]);
        let mut joined = join(o.clone(), c.clone(), how);
        let base = baseline(&joined);
        let cursor = base.1;
        c.borrow_mut().append_row(customer(30, "c")).unwrap();
        o.borrow_mut().append_row(order(3, Some(30), 9)).unwrap();
        replayed(&mut joined, base);
        assert!(history_kept(&joined, cursor), "{how:?}: history kept");
        assert_eq!(
            rows_sorted(&joined),
            rows_sorted(&join(o.clone(), c.clone(), how)),
            "{how:?}"
        );
    }
}

/// Both sides structural: the left batch applies first, against the right as
/// consumers last saw it, so a deleted order's payload carries the customer's
/// old name even though the live customers table already has the new one.
#[test]
fn phase_one_payloads_show_the_old_right_rows() {
    let o = orders(&[(1, Some(10), 5), (2, Some(20), 7)]);
    let c = customers(&[(10, "a"), (20, "b")]);
    let mut left = join(o.clone(), c.clone(), JoinType::Left);
    let base = baseline(&left);
    let cursor = base.1;
    c.borrow_mut().set_value(0, "name", name("z")).unwrap();
    c.borrow_mut().append_row(customer(30, "c")).unwrap();
    o.borrow_mut().delete_row(0).unwrap(); // customer 10's order
    let changes = replayed(&mut left, base);
    assert!(history_kept(&left, cursor), "history kept");
    let payload = changes
        .iter()
        .find_map(|change| match change {
            TableChange::RowDeleted { data, .. } if data["oid"] == Int32(1) => Some(data.clone()),
            _ => None,
        })
        .expect("order 1 deleted");
    assert_eq!(payload["right_name"], name("a"));
    assert_eq!(
        rows_sorted(&left),
        rows_sorted(&join(o.clone(), c.clone(), JoinType::Left))
    );
}

#[test]
fn a_self_join_syncs_incrementally() {
    fn abc(id: i32, a: i32, b: i32) -> Row {
        HashMap::from([
            ("id".into(), Int32(id)),
            ("a".into(), Int32(a)),
            ("b".into(), Int32(b)),
        ])
    }
    for how in ALL_JOINS {
        let mut table = Table::new(
            "t".into(),
            Schema::new(vec![
                ("id".into(), ColumnType::Int32, false),
                ("a".into(), ColumnType::Int32, false),
                ("b".into(), ColumnType::Int32, false),
            ]),
        );
        for (id, a, b) in [(1, 1, 2), (2, 2, 1), (3, 1, 1)] {
            table.append_row(abc(id, a, b)).unwrap();
        }
        table.clear_changeset();
        let t = Rc::new(RefCell::new(table));
        let self_join = |name: &str| {
            JoinView::new(
                name.into(),
                t.clone(),
                t.clone(),
                "a".into(),
                "b".into(),
                how,
            )
            .unwrap()
        };
        let mut joined = self_join("self");
        for step in 0..3 {
            let base = baseline(&joined);
            let cursor = base.1;
            match step {
                0 => t.borrow_mut().insert_row(1, abc(4, 2, 2)).unwrap(),
                1 => {
                    t.borrow_mut().delete_row(0).unwrap();
                }
                _ => t.borrow_mut().set_value(0, "a", Int32(1)).unwrap(),
            }
            replayed(&mut joined, base);
            assert!(
                history_kept(&joined, cursor),
                "{how:?} step {step}: history kept"
            );
            assert_eq!(
                rows_sorted(&joined),
                rows_sorted(&self_join("fresh")),
                "{how:?} step {step}"
            );
        }
    }
}

/// Two-sided batches above 256 events on a side still rebuild.
#[test]
fn two_sided_batches_over_the_bound_rebuild() {
    let o = orders(&[]);
    let c = customers(&[(10, "a")]);
    let mut joined = join(o.clone(), c.clone(), JoinType::Inner);
    let (_, cursor) = baseline(&joined);
    for i in 0..257 {
        o.borrow_mut().append_row(order(i, Some(10), 1)).unwrap();
    }
    c.borrow_mut().append_row(customer(20, "b")).unwrap();
    joined.sync();
    assert!(!history_kept(&joined, cursor), "over the bound rebuilds");
    assert_eq!(joined.len(), 257);
}
