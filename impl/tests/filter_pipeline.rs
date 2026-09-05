//! Correctness, replay-window, compaction, and bounded-work contracts.
use livetable::{
    AggregateFunction, AggregateView, Changeset, ColumnType, ColumnValue, FilterView, JoinType,
    JoinView, ReadableTable, Schema, SortKey, SortedView, Table, TableChange, TickableTable,
};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

type Row = HashMap<String, ColumnValue>;

fn row(id: i32, amount: i32) -> Row {
    HashMap::from([
        ("id".into(), ColumnValue::Int32(id)),
        ("region".into(), ColumnValue::String("West".into())),
        ("amount".into(), ColumnValue::Int32(amount)),
    ])
}

fn base(size: i32) -> Rc<RefCell<Table>> {
    let mut table = Table::new(
        "sales".into(),
        Schema::new(vec![
            ("id".into(), ColumnType::Int32, false),
            ("region".into(), ColumnType::String, false),
            ("amount".into(), ColumnType::Int32, false),
        ]),
    );
    for id in 0..size {
        table
            .append_row(row(id, if id % 2 == 0 { 10 } else { 0 }))
            .unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn matches(row: &Row) -> bool {
    row["amount"].as_i32().unwrap() >= 5
}

fn aggregate(parent: Rc<RefCell<dyn ReadableTable>>) -> AggregateView {
    AggregateView::new(
        "totals".into(),
        parent,
        vec!["region".into()],
        vec![("total".into(), "amount".into(), AggregateFunction::Sum)],
    )
    .unwrap()
}

fn snapshot(table: &dyn ReadableTable) -> Vec<Row> {
    (0..table.len())
        .map(|i| table.get_row(i).unwrap())
        .collect()
}

struct CountedFilter {
    inner: FilterView,
    reads: Cell<usize>,
}

impl ReadableTable for CountedFilter {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn column_names(&self) -> Vec<String> {
        self.inner.column_names()
    }
    fn column_type(&self, index: usize) -> Option<ColumnType> {
        self.inner.column_type(index)
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
fn scalar_and_excluded_updates_do_not_scan_downstream_rows() {
    let table = base(10_000);
    let calls = Rc::new(Cell::new(0));
    let counted_calls = calls.clone();
    let filtered = Rc::new(RefCell::new(CountedFilter {
        inner: FilterView::new("filtered".into(), table.clone(), move |r| {
            counted_calls.set(counted_calls.get() + 1);
            matches(r)
        }),
        reads: Cell::new(0),
    }));
    let mut totals = aggregate(filtered.clone());
    filtered.borrow().reads.set(0);
    calls.set(0);

    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(20))
        .unwrap();
    filtered.borrow_mut().inner.sync();
    assert!(totals.sync());
    assert_eq!(
        totals.get_value(0, "total").unwrap(),
        ColumnValue::Float64(50_010.0)
    );
    assert_eq!(calls.get(), 1);
    assert_eq!(
        filtered.borrow().reads.get(),
        0,
        "SUM must not rescan even when the old value was an extremum"
    );

    calls.set(0);
    table
        .borrow_mut()
        .set_value(1, "amount", ColumnValue::Int32(1))
        .unwrap();
    assert!(!filtered.borrow_mut().inner.sync());
    assert!(!totals.sync());
    assert_eq!(calls.get(), 1);
    assert_eq!(filtered.borrow().reads.get(), 0);

    calls.set(0);
    for index in 0..16 {
        table
            .borrow_mut()
            .set_value(index * 2, "amount", ColumnValue::Int32(30))
            .unwrap();
    }
    filtered.borrow_mut().inner.sync();
    totals.sync();
    assert_eq!(calls.get(), 16, "a small batch evaluates only changed rows");
    assert_eq!(filtered.borrow().reads.get(), 0);
}

#[test]
fn emitted_events_replay_in_filter_coordinates_with_historical_values() {
    let table = base(4);
    let mut filtered = FilterView::new("filtered".into(), table.clone(), matches);
    let mut replayed = snapshot(&filtered);
    let mut table_mut = table.borrow_mut();
    table_mut
        .set_value(1, "amount", ColumnValue::Int32(8))
        .unwrap();
    table_mut
        .set_value(2, "region", ColumnValue::String("East".into()))
        .unwrap();
    table_mut.insert_row(0, row(4, 20)).unwrap();
    table_mut
        .set_value(1, "amount", ColumnValue::Int32(0))
        .unwrap();
    table_mut
        .set_value(2, "amount", ColumnValue::Int32(9))
        .unwrap();
    table_mut.delete_row(2).unwrap();
    table_mut
        .set_value(2, "region", ColumnValue::String("North".into()))
        .unwrap();
    table_mut
        .set_value(2, "amount", ColumnValue::Int32(15))
        .unwrap();
    drop(table_mut);
    filtered.sync();
    for change in filtered.changeset().unwrap().changes() {
        match change {
            TableChange::RowInserted { index, data } => replayed.insert(*index, data.clone()),
            TableChange::RowDeleted { index, data } => assert_eq!(replayed.remove(*index), *data),
            TableChange::CellUpdated {
                row,
                column,
                old_value,
                new_value,
            } => {
                assert_eq!(replayed[*row][column], *old_value);
                replayed[*row].insert(column.clone(), new_value.clone());
            }
        }
    }
    let expected: Vec<_> = snapshot(&*table.borrow())
        .into_iter()
        .filter(matches)
        .collect();
    assert_eq!(replayed, expected);
    assert_eq!(snapshot(&filtered), expected);
}

#[test]
fn multiple_consumers_and_lagging_consumer_recover_from_bounded_history() {
    let table = base(4);
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "filtered".into(),
        table.clone(),
        matches,
    )));
    let mut fast = aggregate(filtered.clone());
    let mut slow = aggregate(filtered.clone());
    for value in 11..20 {
        table
            .borrow_mut()
            .set_value(0, "amount", ColumnValue::Int32(value))
            .unwrap();
        filtered.borrow_mut().sync();
        assert!(
            !filtered.borrow_mut().sync(),
            "no-op sync must preserve pending output"
        );
        fast.sync();
        assert!(filtered.borrow().changeset().unwrap().len() <= 1);
    }
    slow.sync();
    assert_eq!(snapshot(&fast), snapshot(&slow));
    assert_eq!(
        slow.get_value(0, "total").unwrap(),
        ColumnValue::Float64(29.0)
    );
}

#[test]
fn rebuild_invalidates_even_a_consumer_at_the_previous_history_end() {
    let table = base(4);
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "filtered".into(),
        table.clone(),
        matches,
    )));
    let mut totals = aggregate(filtered.clone());
    for _ in 0..300 {
        table
            .borrow_mut()
            .set_value(0, "amount", ColumnValue::Int32(20))
            .unwrap();
    }
    filtered.borrow_mut().sync();
    assert!(totals.sync());
    assert_eq!(
        totals.get_value(0, "total").unwrap(),
        ColumnValue::Float64(30.0)
    );
    // Explicit refresh also invalidates, even with no new root events.
    filtered.borrow_mut().refresh();
    assert!(totals.sync());
    assert_eq!(
        totals.get_value(0, "total").unwrap(),
        ColumnValue::Float64(30.0)
    );
    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(25))
        .unwrap();
    filtered.borrow_mut().sync();
    totals.sync();
    assert_eq!(
        totals.get_value(0, "total").unwrap(),
        ColumnValue::Float64(35.0)
    );
    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(30))
        .unwrap();
    table.borrow_mut().clear_changeset();
    filtered.borrow_mut().sync();
    totals.sync();
    assert_eq!(
        totals.get_value(0, "total").unwrap(),
        ColumnValue::Float64(40.0)
    );
}

#[test]
fn child_created_while_filter_is_stale_rebuilds_its_baseline() {
    let table = base(4);
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "filtered".into(),
        table.clone(),
        matches,
    )));
    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(20))
        .unwrap();
    let mut totals = aggregate(filtered.clone());
    filtered.borrow_mut().sync();
    totals.sync();
    assert_eq!(
        totals.get_value(0, "total").unwrap(),
        ColumnValue::Float64(30.0)
    );
}

#[test]
fn derived_cursors_do_not_pin_root_history() {
    let table = base(4);
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "filtered".into(),
        table.clone(),
        matches,
    )));
    let totals = Rc::new(RefCell::new(aggregate(filtered.clone())));
    let sorted = Rc::new(RefCell::new(
        SortedView::new(
            "sorted".into(),
            filtered.clone(),
            vec![SortKey::ascending("amount")],
        )
        .unwrap(),
    ));
    let tick = TickableTable::new(table.clone());
    tick.register_filter(&filtered);
    tick.register_aggregate(&totals);
    tick.register_sorted(&sorted);
    for value in 0..500 {
        table
            .borrow_mut()
            .set_value(1, "amount", ColumnValue::Int32(value % 2))
            .unwrap();
        tick.tick();
        assert!(table.borrow().changeset().is_empty());
        assert!(filtered.borrow().changeset().unwrap().is_empty());
    }
}

#[test]
fn excluded_change_through_nested_filters_keeps_history_available() {
    let table = base(4);
    let first = Rc::new(RefCell::new(FilterView::new(
        "first".into(),
        table.clone(),
        matches,
    )));
    let second = Rc::new(RefCell::new(FilterView::new(
        "second".into(),
        first.clone(),
        matches,
    )));
    let mut totals = aggregate(second.clone());
    table
        .borrow_mut()
        .set_value(1, "amount", ColumnValue::Int32(1))
        .unwrap();
    assert!(!first.borrow_mut().sync());
    assert!(!second.borrow_mut().sync());
    assert!(second.borrow().changeset().is_some());
    assert!(!totals.sync());
    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(20))
        .unwrap();
    first.borrow_mut().sync();
    second.borrow_mut().sync();
    totals.sync();
    assert_eq!(
        totals.get_value(0, "total").unwrap(),
        ColumnValue::Float64(30.0)
    );
}

#[test]
fn joins_consume_filter_changes_without_mixing_root_cursors() {
    let left = base(4);
    let right = base(4);
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "filtered".into(),
        left.clone(),
        matches,
    )));
    let joined = Rc::new(RefCell::new(
        JoinView::new(
            "joined".into(),
            filtered.clone(),
            right.clone(),
            "id".into(),
            "id".into(),
            JoinType::Left,
        )
        .unwrap(),
    ));
    let left_tick = TickableTable::new(left.clone());
    let right_tick = TickableTable::new(right.clone());
    left_tick.register_filter(&filtered);
    left_tick.register_join_as_left(&joined);
    right_tick.register_join_as_right(&joined);
    for value in 0..20 {
        left.borrow_mut()
            .set_value(
                1,
                "amount",
                ColumnValue::Int32(if value % 2 == 0 { 20 } else { 0 }),
            )
            .unwrap();
        right
            .borrow_mut()
            .set_value(0, "amount", ColumnValue::Int32(value))
            .unwrap();
        left_tick.tick();
        right_tick.tick();
        let rebuilt = JoinView::new(
            "oracle".into(),
            filtered.clone(),
            right.clone(),
            "id".into(),
            "id".into(),
            JoinType::Left,
        )
        .unwrap();
        assert_eq!(snapshot(&*joined.borrow()), snapshot(&rebuilt));
        assert!(left.borrow().changeset().is_empty());
        assert!(right.borrow().changeset().is_empty());
    }
}

#[test]
fn nullable_integer_groups_agree_before_and_after_incremental_sync() {
    let mut table = Table::new(
        "nullable_groups".into(),
        Schema::new(vec![
            ("bucket".into(), ColumnType::Int32, true),
            ("amount".into(), ColumnType::Int32, false),
        ]),
    );
    for bucket in [ColumnValue::Null, ColumnValue::Int32(1)] {
        table
            .append_row(HashMap::from([
                ("bucket".into(), bucket),
                ("amount".into(), ColumnValue::Int32(10)),
            ]))
            .unwrap();
    }
    let table = Rc::new(RefCell::new(table));
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "filtered".into(),
        table.clone(),
        matches,
    )));
    let mut totals = AggregateView::new(
        "totals".into(),
        filtered.clone(),
        vec!["bucket".into()],
        vec![("total".into(), "amount".into(), AggregateFunction::Sum)],
    )
    .unwrap();
    let values = |view: &AggregateView| -> HashMap<Option<i32>, f64> {
        snapshot(view)
            .into_iter()
            .map(|r| (r["bucket"].as_i32(), r["total"].as_f64().unwrap()))
            .collect()
    };
    assert_eq!(
        values(&totals),
        HashMap::from([(None, 10.0), (Some(1), 10.0)])
    );
    table
        .borrow_mut()
        .set_value(0, "bucket", ColumnValue::Int32(1))
        .unwrap();
    table
        .borrow_mut()
        .set_value(1, "bucket", ColumnValue::Null)
        .unwrap();
    filtered.borrow_mut().sync();
    totals.sync();
    assert_eq!(
        values(&totals),
        HashMap::from([(None, 10.0), (Some(1), 10.0)])
    );
    table
        .borrow_mut()
        .set_value(1, "amount", ColumnValue::Int32(0))
        .unwrap();
    filtered.borrow_mut().sync();
    totals.sync();
    assert_eq!(values(&totals), HashMap::from([(Some(1), 10.0)]));
}
