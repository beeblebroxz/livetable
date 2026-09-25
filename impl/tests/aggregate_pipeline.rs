//! Aggregate output history: group coordinates, net per-batch diffs, and
//! incremental children. See
//! docs/superpowers/specs/2026-09-23-aggregate-output-changesets-design.md.
use livetable::{
    AggregateFunction, AggregateView, Changeset, ColumnType, ColumnValue, FilterView,
    ReadableTable, Schema, SortKey, SortedView, Table, TableChange,
};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use ColumnValue::{Float64, Int64};

type Row = HashMap<String, ColumnValue>;

fn order(region: &str, amount: i32) -> Row {
    HashMap::from([
        ("region".into(), ColumnValue::String(region.into())),
        ("amount".into(), ColumnValue::Int32(amount)),
        ("note".into(), ColumnValue::Int32(0)),
    ])
}

fn orders(rows: &[(&str, i32)]) -> Rc<RefCell<Table>> {
    let mut table = Table::new(
        "orders".into(),
        Schema::new(vec![
            ("region".into(), ColumnType::String, false),
            ("amount".into(), ColumnType::Int32, false),
            ("note".into(), ColumnType::Int32, false),
        ]),
    );
    for (region, amount) in rows {
        table.append_row(order(region, *amount)).unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn by_region(parent: Rc<RefCell<dyn ReadableTable>>) -> AggregateView {
    AggregateView::new(
        "by_region".into(),
        parent,
        vec!["region".into()],
        vec![
            ("total".into(), "amount".into(), AggregateFunction::Sum),
            ("orders".into(), "amount".into(), AggregateFunction::Count),
            ("top".into(), "amount".into(), AggregateFunction::Max),
        ],
    )
    .unwrap()
}

fn group(region: &str, total: f64, orders: i64, top: f64) -> Row {
    HashMap::from([
        ("region".into(), ColumnValue::String(region.into())),
        ("total".into(), Float64(total)),
        ("orders".into(), Int64(orders)),
        ("top".into(), Float64(top)),
    ])
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
                assert_eq!(&rows[*row][column], old_value, "historical cell value");
                rows[*row].insert(column.clone(), new_value.clone());
            }
        }
    }
}

/// Record the view and its cursor, mutate the source, sync, check that the
/// emitted history turns the old rows into the new ones, and return it.
fn sync_replayed(view: &mut AggregateView, mutate: impl FnOnce()) -> Vec<TableChange> {
    let mut rows = snapshot(&*view);
    let cursor = view
        .changeset()
        .expect("a synced aggregate exposes history")
        .total_len();
    mutate();
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
        "replayed history reproduces the view"
    );
    changes
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
fn value_update_emits_only_the_changed_aggregate_cells() {
    let table = orders(&[("West", 10), ("East", 20), ("West", 30)]);
    let mut view = by_region(table.clone());
    let changes = sync_replayed(&mut view, || {
        table
            .borrow_mut()
            .set_value(0, "amount", ColumnValue::Int32(15))
            .unwrap();
    });
    // COUNT and MAX are unchanged, so only SUM is sent.
    assert_eq!(changes, [update(0, "total", Float64(40.0), Float64(45.0))]);
}

#[test]
fn writes_that_change_no_result_emit_nothing() {
    let table = orders(&[("West", 10), ("East", 20)]);
    let mut view = by_region(table.clone());
    let changes = sync_replayed(&mut view, || {
        let mut table = table.borrow_mut();
        table
            .set_value(0, "amount", ColumnValue::Int32(10))
            .unwrap(); // same value
        table.set_value(1, "note", ColumnValue::Int32(7)).unwrap(); // not aggregated
    });
    assert_eq!(changes, []);
}

#[test]
fn new_groups_append_and_emptied_groups_delete_their_last_row() {
    let table = orders(&[("West", 10), ("East", 20)]);
    let mut view = by_region(table.clone());

    let changes = sync_replayed(&mut view, || {
        table.borrow_mut().append_row(order("North", 5)).unwrap();
    });
    assert_eq!(
        changes,
        [TableChange::RowInserted {
            index: 2,
            data: group("North", 5.0, 1, 5.0),
        }]
    );

    let changes = sync_replayed(&mut view, || {
        table.borrow_mut().delete_row(1).unwrap(); // East's only order
    });
    assert_eq!(
        changes,
        [TableChange::RowDeleted {
            index: 1,
            data: group("East", 20.0, 1, 20.0),
        }]
    );
}

#[test]
fn a_group_emptied_and_recreated_in_one_batch_moves_to_the_tail() {
    let table = orders(&[("West", 10), ("East", 20), ("North", 5)]);
    let mut view = by_region(table.clone());
    let changes = sync_replayed(&mut view, || {
        let mut table = table.borrow_mut();
        table.delete_row(1).unwrap();
        table.append_row(order("East", 7)).unwrap();
        table.append_row(order("South", 1)).unwrap();
        table.delete_row(3).unwrap(); // South never reaches consumers
    });
    assert_eq!(
        changes,
        [
            TableChange::RowDeleted {
                index: 1,
                data: group("East", 20.0, 1, 20.0),
            },
            TableChange::RowInserted {
                index: 2,
                data: group("East", 7.0, 1, 7.0),
            },
        ]
    );
}

#[test]
fn removing_a_group_shifts_the_index_of_later_groups() {
    let table = orders(&[("West", 10), ("East", 20), ("North", 5)]);
    let mut view = by_region(table.clone());
    let changes = sync_replayed(&mut view, || {
        let mut table = table.borrow_mut();
        table.delete_row(1).unwrap(); // empties East
        table.set_value(1, "amount", ColumnValue::Int32(8)).unwrap(); // North
    });
    assert_eq!(
        changes,
        [
            TableChange::RowDeleted {
                index: 1,
                data: group("East", 20.0, 1, 20.0),
            },
            update(1, "total", Float64(5.0), Float64(8.0)),
            update(1, "top", Float64(5.0), Float64(8.0)),
        ]
    );
}

#[test]
fn moving_a_row_between_groups_updates_both_in_group_order() {
    let table = orders(&[("West", 10), ("East", 20), ("West", 30)]);
    let mut view = by_region(table.clone());
    let changes = sync_replayed(&mut view, || {
        table
            .borrow_mut()
            .set_value(0, "region", ColumnValue::String("East".into()))
            .unwrap();
    });
    assert_eq!(
        changes,
        [
            update(0, "total", Float64(40.0), Float64(30.0)),
            update(0, "orders", Int64(2), Int64(1)),
            update(1, "total", Float64(20.0), Float64(30.0)),
            update(1, "orders", Int64(1), Int64(2)),
        ]
    );
}

#[test]
fn deleting_the_max_in_a_mixed_batch_emits_the_rescanned_value() {
    let table = orders(&[("West", 10), ("West", 30), ("West", 25), ("East", 20)]);
    let mut view = by_region(table.clone());
    let changes = sync_replayed(&mut view, || {
        let mut table = table.borrow_mut();
        table.delete_row(1).unwrap(); // West's max
        table.append_row(order("East", 5)).unwrap();
    });
    assert_eq!(
        changes,
        [
            update(0, "total", Float64(65.0), Float64(35.0)),
            update(0, "orders", Int64(3), Int64(2)),
            update(0, "top", Float64(30.0), Float64(25.0)),
            update(1, "total", Float64(20.0), Float64(25.0)),
            update(1, "orders", Int64(1), Int64(2)),
        ]
    );
}

#[test]
fn only_the_latest_batch_is_retained_and_rebuilds_invalidate() {
    let table = orders(&[("West", 10), ("East", 20)]);
    let mut view = by_region(table.clone());
    let lagging = view.changeset().unwrap().total_len();
    for amount in [11, 12] {
        table
            .borrow_mut()
            .set_value(0, "amount", ColumnValue::Int32(amount))
            .unwrap();
        view.sync();
    }
    assert!(view.changeset().unwrap().changes_from(lagging).is_none());

    let caught_up = view.changeset().unwrap().total_len();
    view.refresh();
    assert!(view.changeset().unwrap().changes_from(caught_up).is_none());

    // More than 256 group-key updates rebuild instead of replaying.
    let table = orders(&[("West", 1); 300]);
    let mut view = by_region(table.clone());
    let caught_up = view.changeset().unwrap().total_len();
    for index in 0..257 {
        table
            .borrow_mut()
            .set_value(index, "region", ColumnValue::String("East".into()))
            .unwrap();
    }
    view.sync();
    assert!(view.changeset().unwrap().changes_from(caught_up).is_none());
    assert_eq!(snapshot(&view), snapshot(&by_region(table.clone())));
}

#[test]
fn history_is_hidden_while_the_parent_is_ahead() {
    let table = orders(&[("West", 10)]);
    let mut view = by_region(table.clone());
    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(11))
        .unwrap();
    assert!(view.changeset().is_none());
    view.sync();
    assert!(view.changeset().is_some());
}

#[test]
fn excluded_upstream_edits_keep_history_available() {
    let table = orders(&[("West", 10), ("East", 20)]);
    let big = Rc::new(RefCell::new(FilterView::new(
        "big".into(),
        table.clone(),
        |row| row["amount"].as_i32().is_some_and(|amount| amount >= 20),
    )));
    let mut view = by_region(big.clone());
    let cursor = view.changeset().unwrap().total_len();

    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(11))
        .unwrap(); // still excluded
    big.borrow_mut().sync();
    assert!(!view.sync());
    assert_eq!(
        view.changeset()
            .expect("coherent after an excluded edit")
            .changes_from(cursor),
        Some(&[][..])
    );
}

#[test]
fn filter_and_sort_over_an_aggregate_replay_instead_of_rebuilding() {
    let names: Vec<String> = (0..100).map(|i| format!("r{i}")).collect();
    let rows: Vec<(&str, i32)> = names.iter().map(|name| (name.as_str(), 10)).collect();
    let table = orders(&rows);
    let grouped = Rc::new(RefCell::new(Counted {
        inner: by_region(table.clone()),
        reads: Cell::new(0),
    }));
    let high = |row: &Row| row["total"].as_f64().is_some_and(|total| total >= 15.0);
    let by_total = || vec![SortKey::descending("total")];
    let mut filtered = FilterView::new("high".into(), grouped.clone(), high);
    let mut ranked = SortedView::new("ranked".into(), grouped.clone(), by_total()).unwrap();

    table
        .borrow_mut()
        .set_value(42, "amount", ColumnValue::Int32(99))
        .unwrap();
    grouped.borrow_mut().inner.sync();
    grouped.borrow().reads.set(0);
    filtered.sync();
    ranked.sync();
    assert!(
        grouped.borrow().reads.get() <= 4,
        "children replay one group instead of rereading all 100: {} reads",
        grouped.borrow().reads.get()
    );

    let fresh_filter = FilterView::new("fresh".into(), grouped.clone(), high);
    let fresh_sort = SortedView::new("fresh".into(), grouped.clone(), by_total()).unwrap();
    assert_eq!(snapshot(&filtered), snapshot(&fresh_filter));
    assert_eq!(snapshot(&filtered).len(), 1);
    assert_eq!(snapshot(&ranked), snapshot(&fresh_sort));
}
