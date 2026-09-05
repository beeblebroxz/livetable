//! Sorted-coordinate replay, bounded work, and downstream history contracts.
use livetable::{
    AggregateFunction, AggregateView, Changeset, ColumnType, ColumnValue, FilterView,
    ReadableTable, Schema, SortKey, SortOrder, SortedView, Table, TableChange, TickableTable,
};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

type Row = HashMap<String, ColumnValue>;

fn row(id: i32, rank: Option<i32>, amount: i32) -> Row {
    HashMap::from([
        ("id".into(), ColumnValue::Int32(id)),
        (
            "rank".into(),
            rank.map_or(ColumnValue::Null, ColumnValue::Int32),
        ),
        (
            "region".into(),
            ColumnValue::String(format!("region{}", id % 3)),
        ),
        ("amount".into(), ColumnValue::Int32(amount)),
    ])
}

fn base(size: i32) -> Rc<RefCell<Table>> {
    let mut table = Table::new(
        "source".into(),
        Schema::new(vec![
            ("id".into(), ColumnType::Int32, false),
            ("rank".into(), ColumnType::Int32, true),
            ("region".into(), ColumnType::String, false),
            ("amount".into(), ColumnType::Int32, false),
        ]),
    );
    for id in 0..size {
        table.append_row(row(id, Some(id), 10)).unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn sorted(parent: Rc<RefCell<dyn ReadableTable>>) -> SortedView {
    SortedView::new("sorted".into(), parent, vec![SortKey::ascending("rank")]).unwrap()
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

fn snapshot(view: &dyn ReadableTable) -> Vec<Row> {
    (0..view.len()).map(|i| view.get_row(i).unwrap()).collect()
}

fn totals(view: &AggregateView) -> HashMap<String, f64> {
    snapshot(view)
        .iter()
        .map(|r| {
            (
                r["region"].as_string().unwrap().into(),
                r["total"].as_f64().unwrap(),
            )
        })
        .collect()
}

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

struct Counted<T> {
    inner: T,
    reads: Cell<usize>,
}

impl<T: ReadableTable> Counted<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            reads: Cell::new(0),
        }
    }
}

impl<T: ReadableTable> ReadableTable for Counted<T> {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn column_names(&self) -> Vec<String> {
        self.inner.column_names()
    }
    fn column_index(&self, name: &str) -> Option<usize> {
        self.inner.column_index(name)
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
fn scalar_batches_and_excluded_edits_do_not_scan_sort_or_aggregate_sources() {
    let table = base(10_000);
    table
        .borrow_mut()
        .set_value(1, "amount", ColumnValue::Int32(-1))
        .unwrap();
    let filtered = Rc::new(RefCell::new(Counted::new(FilterView::new(
        "filtered".into(),
        table.clone(),
        |r| r["amount"].as_i32().unwrap() >= 0,
    ))));
    let ranked = Rc::new(RefCell::new(Counted::new(sorted(filtered.clone()))));
    let mut grouped = aggregate(ranked.clone());
    for changes in [1, 16] {
        filtered.borrow().reads.set(0);
        ranked.borrow().reads.set(0);
        for index in 0..changes {
            table
                .borrow_mut()
                .set_value(
                    5_000 + index,
                    "amount",
                    ColumnValue::Int32(20 + changes as i32),
                )
                .unwrap();
        }
        filtered.borrow_mut().inner.sync();
        assert!(
            ranked.borrow_mut().inner.sync(),
            "non-sort edits still change visible cells"
        );
        grouped.sync();
        assert_eq!(
            filtered.borrow().reads.get(),
            0,
            "sort reads no rows or key columns for scalar edits"
        );
        assert_eq!(
            ranked.borrow().reads.get(),
            0,
            "SUM consumes only emitted scalars"
        );
        let view = ranked.borrow();
        assert_eq!(view.changeset().unwrap().len(), changes);
        assert!(view
            .changeset()
            .unwrap()
            .changes()
            .iter()
            .all(|c| matches!(c, TableChange::CellUpdated { .. })));
        assert_eq!(totals(&grouped), totals(&aggregate(filtered.clone())));
    }
    // An excluded input changes version but must not make the sort expose a
    // stale history baseline or force its aggregate child to refresh.
    filtered.borrow().reads.set(0);
    ranked.borrow().reads.set(0);
    table
        .borrow_mut()
        .set_value(1, "amount", ColumnValue::Int32(-2))
        .unwrap();
    assert!(!filtered.borrow_mut().inner.sync());
    assert!(!ranked.borrow_mut().inner.sync());
    assert!(ranked.borrow().changeset().is_some());
    assert!(!grouped.sync());
    assert_eq!(filtered.borrow().reads.get(), 0);
    assert_eq!(ranked.borrow().reads.get(), 0);
}

#[test]
fn sort_key_batch_reads_only_changed_rows_and_emits_moves() {
    let table = Rc::new(RefCell::new(Counted::new(Table::new(
        "source".into(),
        Schema::new(vec![
            ("id".into(), ColumnType::Int32, false),
            ("rank".into(), ColumnType::Int32, true),
            ("region".into(), ColumnType::String, false),
            ("amount".into(), ColumnType::Int32, false),
        ]),
    ))));
    for id in 0..10_000 {
        table
            .borrow_mut()
            .inner
            .append_row(row(id, Some(id), 10))
            .unwrap();
    }
    let mut ranked = sorted(table.clone());
    let cursor = ranked.changeset().unwrap().total_len();
    let mut rows = snapshot(&ranked);
    table.borrow().reads.set(0);
    for id in 0..16 {
        table
            .borrow_mut()
            .inner
            .set_value(id, "rank", ColumnValue::Int32(20_000 + id as i32))
            .unwrap();
    }
    ranked.sync();
    assert_eq!(
        table.borrow().reads.get(),
        16,
        "comparisons must use cached keys, not live rows"
    );
    let events = ranked.changeset().unwrap().changes_from(cursor).unwrap();
    assert_eq!(events.len(), 32);
    replay(&mut rows, events);
    assert_eq!(rows, snapshot(&ranked));
    assert_eq!(rows, snapshot(&sorted(table.clone())));
}

#[test]
fn mixed_batches_replay_historical_values_ties_and_nulls_in_sorted_coordinates() {
    for order in [SortOrder::Ascending, SortOrder::Descending] {
        for nulls_first in [false, true] {
            let table = base(8);
            let keys = vec![
                SortKey::new("rank", order, nulls_first),
                SortKey::descending("amount"),
                SortKey::ascending("rank"),
            ];
            let mut ranked = SortedView::new("ranked".into(), table.clone(), keys.clone()).unwrap();
            let mut rows = snapshot(&ranked);
            let cursor = ranked.changeset().unwrap().total_len();
            {
                let mut table = table.borrow_mut();
                table.set_value(0, "rank", ColumnValue::Int32(20)).unwrap();
                table.set_value(1, "rank", ColumnValue::Null).unwrap();
                table.set_value(7, "rank", ColumnValue::Int32(-20)).unwrap();
                table
                    .set_value(0, "amount", ColumnValue::Int32(99))
                    .unwrap();
                table.insert_row(0, row(8, Some(2), 10)).unwrap();
                table.set_value(1, "rank", ColumnValue::Int32(2)).unwrap();
                table.delete_row(1).unwrap(); // updated row no longer in the live parent
                table.set_value(0, "rank", ColumnValue::Null).unwrap();
                table.set_value(2, "rank", ColumnValue::Null).unwrap();
                table.insert_row(2, row(9, None, 10)).unwrap();
                table
                    .set_value(2, "amount", ColumnValue::Int32(20))
                    .unwrap();
                table
                    .set_value(2, "amount", ColumnValue::Int32(10))
                    .unwrap();
                table
                    .set_value(3, "region", ColumnValue::String("changed".into()))
                    .unwrap();
            }
            ranked.sync();
            replay(
                &mut rows,
                ranked
                    .changeset()
                    .unwrap()
                    .changes_from(cursor)
                    .expect("small batches must replay"),
            );
            assert_eq!(rows, snapshot(&ranked));
            assert_eq!(
                rows,
                snapshot(&SortedView::new("oracle".into(), table.clone(), keys).unwrap())
            );
            // Cached and inverse indices must remain valid on the next batch.
            table
                .borrow_mut()
                .set_value(4, "rank", ColumnValue::Int32(30))
                .unwrap();
            table
                .borrow_mut()
                .set_value(4, "amount", ColumnValue::Int32(45))
                .unwrap();
            ranked.sync();
            replay(&mut rows, ranked.changeset().unwrap().changes());
            assert_eq!(rows, snapshot(&ranked));
        }
    }
}

#[test]
fn stationary_sort_key_edit_is_a_cell_update() {
    let table = base(3);
    let mut ranked = sorted(table.clone());
    table
        .borrow_mut()
        .set_value(2, "rank", ColumnValue::Int32(100))
        .unwrap();
    ranked.sync();
    assert!(matches!(
        ranked.changeset().unwrap().changes(),
        [TableChange::CellUpdated { row: 2, .. }]
    ));
}

#[test]
fn aggregate_handles_many_moves_without_rebuilding_or_reading_source_rows() {
    for count in [16, 256] {
        let table = base(1024);
        let ranked = Rc::new(RefCell::new(Counted::new(sorted(table.clone()))));
        let mut grouped = aggregate(ranked.clone());
        ranked.borrow().reads.set(0);
        for index in 0..count {
            table
                .borrow_mut()
                .set_value(index, "rank", ColumnValue::Int32(2000 + index as i32))
                .unwrap();
        }
        ranked.borrow_mut().inner.sync();
        assert_eq!(ranked.borrow().changeset().unwrap().len(), count * 2);
        grouped.sync();
        assert_eq!(
            ranked.borrow().reads.get(),
            0,
            "aggregate remaps identities, not source rows"
        );
        assert_eq!(totals(&grouped), totals(&aggregate(table.clone())));
        // All old rows shifted. Subsequent scalar changes must find the right
        // group in the newly reindexed map, including rows not moved directly.
        for index in [0, count / 2, count, 1023] {
            table
                .borrow_mut()
                .set_value(index, "amount", ColumnValue::Int32(index as i32))
                .unwrap();
        }
        ranked.borrow_mut().inner.sync();
        grouped.sync();
        assert_eq!(ranked.borrow().reads.get(), 0);
        assert_eq!(totals(&grouped), totals(&aggregate(table.clone())));
    }
}

#[test]
fn multiple_consumers_keep_independent_cursors_and_lagging_consumers_rebuild() {
    let table = base(8);
    let ranked = Rc::new(RefCell::new(sorted(table.clone())));
    let mut fast = aggregate(ranked.clone());
    let mut slow = aggregate(ranked.clone());
    let old_cursor = ranked.borrow().changeset().unwrap().total_len();
    for amount in 11..20 {
        table
            .borrow_mut()
            .set_value(0, "amount", ColumnValue::Int32(amount))
            .unwrap();
        ranked.borrow_mut().sync();
        let end = ranked.borrow().changeset().unwrap().total_len();
        assert!(!ranked.borrow_mut().sync());
        assert_eq!(ranked.borrow().changeset().unwrap().total_len(), end);
        assert_eq!(ranked.borrow().changeset().unwrap().len(), 1);
        fast.sync();
    }
    assert!(ranked
        .borrow()
        .changeset()
        .unwrap()
        .changes_from(old_cursor)
        .is_none());
    slow.sync();
    assert_eq!(totals(&fast), totals(&slow));
    assert_eq!(totals(&slow), totals(&aggregate(table.clone())));
}

#[test]
fn large_batch_refresh_and_compacted_input_invalidate_output_history() {
    let table = base(4);
    let ranked = Rc::new(RefCell::new(sorted(table.clone())));
    let mut grouped = aggregate(ranked.clone());
    // Exactly 256 changes still replay; 257 requires a rebuild.
    for batch_size in [256, 257] {
        let cursor = ranked.borrow().changeset().unwrap().total_len();
        for value in 0..batch_size {
            table
                .borrow_mut()
                .set_value(0, "amount", ColumnValue::Int32(value))
                .unwrap();
        }
        ranked.borrow_mut().sync();
        assert_eq!(
            ranked
                .borrow()
                .changeset()
                .unwrap()
                .changes_from(cursor)
                .is_some(),
            batch_size == 256
        );
        assert!(grouped.sync());
        assert_eq!(totals(&grouped), totals(&aggregate(table.clone())));
    }
    let cursor = ranked.borrow().changeset().unwrap().total_len();
    ranked.borrow_mut().refresh();
    assert!(ranked
        .borrow()
        .changeset()
        .unwrap()
        .changes_from(cursor)
        .is_none());
    assert!(grouped.sync());
    let cursor = ranked.borrow().changeset().unwrap().total_len();
    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(77))
        .unwrap();
    table.borrow_mut().clear_changeset();
    ranked.borrow_mut().sync();
    assert!(ranked
        .borrow()
        .changeset()
        .unwrap()
        .changes_from(cursor)
        .is_none());
    grouped.sync();
    assert_eq!(totals(&grouped), totals(&aggregate(table.clone())));
    table
        .borrow_mut()
        .set_value(0, "rank", ColumnValue::Null)
        .unwrap();
    ranked.borrow_mut().sync();
    grouped.sync();
    assert_eq!(totals(&grouped), totals(&aggregate(table.clone())));
}

#[test]
fn child_created_while_sort_or_its_parent_is_stale_rebaselines() {
    let table = base(4);
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "filtered".into(),
        table.clone(),
        |_| true,
    )));
    let ranked = Rc::new(RefCell::new(sorted(filtered.clone())));
    table
        .borrow_mut()
        .set_value(0, "rank", ColumnValue::Int32(100))
        .unwrap();
    table
        .borrow_mut()
        .set_value(0, "amount", ColumnValue::Int32(30))
        .unwrap();
    assert!(ranked.borrow().changeset().is_none());
    let mut early = aggregate(ranked.clone());
    let mut early_sort = sorted(filtered.clone());
    filtered.borrow_mut().sync();
    let mut middle = aggregate(ranked.clone());
    ranked.borrow_mut().sync();
    early_sort.sync();
    early.sync();
    middle.sync();
    assert_eq!(snapshot(&early_sort), snapshot(&*ranked.borrow()));
    assert_eq!(totals(&early), totals(&aggregate(table.clone())));
    assert_eq!(totals(&middle), totals(&early));
}

#[test]
fn nested_sorts_filters_and_root_compaction_remain_coherent() {
    let table = base(12);
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "included".into(),
        table.clone(),
        |r| r["amount"].as_i32().unwrap() >= 0,
    )));
    let first = Rc::new(RefCell::new(sorted(filtered.clone())));
    let second = Rc::new(RefCell::new(
        SortedView::new(
            "second".into(),
            first.clone(),
            vec![SortKey::descending("amount")],
        )
        .unwrap(),
    ));
    let child = Rc::new(RefCell::new(FilterView::new(
        "child".into(),
        second.clone(),
        |r| r["id"].as_i32().unwrap() % 2 == 0,
    )));
    let grouped = Rc::new(RefCell::new(aggregate(child.clone())));
    let tick = TickableTable::new(table.clone());
    tick.register_filter(&filtered);
    tick.register_sorted(&first);
    tick.register_sorted(&second);
    tick.register_filter(&child);
    tick.register_aggregate(&grouped);
    for step in 0..100 {
        table
            .borrow_mut()
            .set_value(
                0,
                "rank",
                ColumnValue::Int32(if step % 2 == 0 { 50 } else { -50 }),
            )
            .unwrap();
        table
            .borrow_mut()
            .set_value(0, "amount", ColumnValue::Int32(step))
            .unwrap();
        table
            .borrow_mut()
            .set_value(1, "amount", ColumnValue::Int32(-step - 1))
            .unwrap();
        tick.tick();
        assert!(
            table.borrow().changeset().is_empty(),
            "derived cursors must not pin root history"
        );
        assert!(first.borrow().changeset().is_some());
        assert!(second.borrow().changeset().is_some());
        let oracle_first = Rc::new(RefCell::new(sorted(filtered.clone())));
        let oracle_second = SortedView::new(
            "oracle".into(),
            oracle_first,
            vec![SortKey::descending("amount")],
        )
        .unwrap();
        assert_eq!(snapshot(&*second.borrow()), snapshot(&oracle_second));
        let expected: Vec<_> = snapshot(&oracle_second)
            .into_iter()
            .filter(|r| r["id"].as_i32().unwrap() % 2 == 0)
            .collect();
        assert_eq!(snapshot(&*child.borrow()), expected);
        assert_eq!(totals(&grouped.borrow()), totals(&aggregate(child.clone())));
    }
    table
        .borrow_mut()
        .set_value(1, "amount", ColumnValue::Int32(-200))
        .unwrap();
    filtered.borrow_mut().sync();
    assert!(!first.borrow_mut().sync());
    assert!(!second.borrow_mut().sync());
    assert!(!child.borrow_mut().sync());
    assert!(!grouped.borrow_mut().sync());
}

#[test]
fn float_sort_has_transitive_nan_order_and_stable_signed_zero_ties() {
    for float_type in [ColumnType::Float32, ColumnType::Float64] {
        for order in [SortOrder::Ascending, SortOrder::Descending] {
            let value = |v: f64| {
                if float_type == ColumnType::Float32 {
                    ColumnValue::Float32(v as f32)
                } else {
                    ColumnValue::Float64(v)
                }
            };
            let mut table = Table::new(
                "floats".into(),
                Schema::new(vec![
                    ("id".into(), ColumnType::Int32, false),
                    ("value".into(), float_type, true),
                ]),
            );
            for (id, v) in [
                f64::NAN,
                2.0,
                -0.0,
                0.0,
                f64::NEG_INFINITY,
                f64::INFINITY,
                f64::NAN,
            ]
            .into_iter()
            .enumerate()
            {
                table
                    .append_row(HashMap::from([
                        ("id".into(), ColumnValue::Int32(id as i32)),
                        ("value".into(), value(v)),
                    ]))
                    .unwrap();
            }
            let table = Rc::new(RefCell::new(table));
            let keys = vec![SortKey::new("value", order, false)];
            let mut ranked = SortedView::new("ranked".into(), table.clone(), keys.clone()).unwrap();
            let ids = |v: &SortedView| {
                (0..v.len())
                    .map(|i| v.get_value(i, "id").unwrap().as_i32().unwrap())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                ids(&ranked),
                if order == SortOrder::Ascending {
                    vec![4, 2, 3, 1, 5, 0, 6]
                } else {
                    vec![0, 6, 5, 1, 2, 3, 4]
                }
            );
            for (index, v) in [(0, -1.0), (1, f64::NAN), (3, -0.0), (6, 1.0)] {
                table
                    .borrow_mut()
                    .set_value(index, "value", value(v))
                    .unwrap();
            }
            ranked.sync();
            assert_eq!(
                ids(&ranked),
                ids(&SortedView::new("oracle".into(), table, keys).unwrap())
            );
        }
    }
}
