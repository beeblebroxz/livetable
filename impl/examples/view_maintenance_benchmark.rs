//! Per-view tick costs for single edits, inserts and deletes, on both storage
//! hints, plus a join whose two parents change in the same tick. Prints one
//! JSON line per (rows, storage, view, workload).
//! Run: cargo run --release --example view_maintenance_benchmark -- 10000 100000 1000000
//! See docs/VIEW_MAINTENANCE_BENCHMARK.md.
use livetable::{
    AggregateFunction, AggregateView, ColumnType, ColumnValue, FilterView, JoinType, JoinView,
    ReadableTable, Schema, SortKey, SortedView, StorageHint, Table, TickableTable,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

type Row = HashMap<String, ColumnValue>;
type SharedView = Rc<RefCell<dyn ReadableTable>>;

const WARMUP: usize = 5;
const MEASURED: usize = 31;

fn order(id: i64, rank: i64, cust: i32, amount: f64) -> Row {
    HashMap::from([
        ("id".into(), ColumnValue::Int64(id)),
        ("rank".into(), ColumnValue::Int64(rank)),
        ("cust".into(), ColumnValue::Int32(cust)),
        ("region".into(), ColumnValue::String(format!("region{}", id % 8))),
        ("amount".into(), ColumnValue::Float64(amount)),
    ])
}

/// N orders: id = rank = i, ten orders per customer, every other row passing.
fn orders(n: usize, hint: StorageHint) -> Rc<RefCell<Table>> {
    let mut table = Table::with_hint(
        "orders".into(),
        Schema::new(vec![
            ("id".into(), ColumnType::Int64, false),
            ("rank".into(), ColumnType::Int64, false),
            ("cust".into(), ColumnType::Int32, false),
            ("region".into(), ColumnType::String, false),
            ("amount".into(), ColumnType::Float64, false),
        ]),
        hint,
    );
    for i in 0..n {
        let amount = if i % 2 == 0 { 10.0 } else { 0.0 };
        table
            .append_row(order(i as i64, i as i64, (i % (n / 10)) as i32, amount))
            .unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn customer(cid: i32, name: String) -> Row {
    HashMap::from([
        ("cid".into(), ColumnValue::Int32(cid)),
        ("name".into(), ColumnValue::String(name)),
    ])
}

/// M customers: cid = i, name = "c{i}".
fn customers(m: usize, hint: StorageHint) -> Rc<RefCell<Table>> {
    let mut table = Table::with_hint(
        "customers".into(),
        Schema::new(vec![
            ("cid".into(), ColumnType::Int32, false),
            ("name".into(), ColumnType::String, false),
        ]),
        hint,
    );
    for i in 0..m {
        table.append_row(customer(i as i32, format!("c{i}"))).unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn passes(row: &Row) -> bool {
    row["amount"].as_f64().is_some_and(|v| v >= 5.0)
}

fn storage_name(hint: StorageHint) -> &'static str {
    match hint {
        StorageHint::FastReads => "fast_reads",
        StorageHint::FastUpdates => "fast_updates",
    }
}

struct Sample {
    mutate_us: f64,
    tick_us: f64,
    tick_parts: Option<(f64, f64)>,
}

/// Which root's TickableTable a step ticks.
#[derive(Clone, Copy)]
enum Side {
    Orders,
    Customers,
}

struct Pipeline {
    orders: Rc<RefCell<Table>>,
    orders_tick: TickableTable,
    customers: Option<(Rc<RefCell<Table>>, TickableTable)>,
    join: Option<Rc<RefCell<JoinView>>>,
    /// Registered views, kept alive here (ticks hold weak references), with
    /// their lengths right after construction.
    views: Vec<(&'static str, SharedView, usize)>,
}

impl Pipeline {
    fn new(orders: Rc<RefCell<Table>>) -> Self {
        let orders_tick = TickableTable::new(orders.clone());
        Pipeline { orders, orders_tick, customers: None, join: None, views: Vec::new() }
    }

    fn keep(&mut self, name: &'static str, view: SharedView) {
        let len = view.borrow().len();
        self.views.push((name, view, len));
    }

    fn tick(&self, side: Side) {
        match side {
            Side::Orders => self.orders_tick.tick(),
            Side::Customers => self.customers.as_ref().unwrap().1.tick(),
        };
    }

    /// Every workload restores its starting state, so lengths must not drift.
    fn check(&self, ctx: &Ctx, workload: &str) {
        for (name, view, len) in &self.views {
            assert_eq!(
                view.borrow().len(),
                *len,
                "{} rows {} {} {workload}: {name} length drifted",
                ctx.rows,
                storage_name(ctx.hint),
                ctx.view
            );
        }
    }
}

fn build(view: &'static str, n: usize, hint: StorageHint) -> Pipeline {
    let mut p = Pipeline::new(orders(n, hint));
    match view {
        "none" => {}
        "filter" => {
            let filter = Rc::new(RefCell::new(FilterView::new("f".into(), p.orders.clone(), passes)));
            p.orders_tick.register_filter(&filter);
            p.keep("filter", filter);
        }
        "sort" => {
            let sorted = Rc::new(RefCell::new(
                SortedView::new("s".into(), p.orders.clone(), vec![SortKey::ascending("rank")]).unwrap(),
            ));
            p.orders_tick.register_sorted(&sorted);
            p.keep("sort", sorted);
        }
        "aggregate" => {
            let totals = Rc::new(RefCell::new(
                AggregateView::new(
                    "g".into(),
                    p.orders.clone(),
                    vec!["region".into()],
                    vec![("total".into(), "amount".into(), AggregateFunction::Sum)],
                )
                .unwrap(),
            ));
            p.orders_tick.register_aggregate(&totals);
            p.keep("aggregate", totals);
        }
        "join" | "join_chain" => {
            let customers = customers(n / 10, hint);
            let customers_tick = TickableTable::new(customers.clone());
            let join = Rc::new(RefCell::new(
                JoinView::new(
                    "j".into(),
                    p.orders.clone(),
                    customers.clone(),
                    "cust".into(),
                    "cid".into(),
                    JoinType::Inner,
                )
                .unwrap(),
            ));
            p.orders_tick.register_join_as_left(&join);
            customers_tick.register_join_as_right(&join);
            p.keep("join", join.clone());
            if view == "join_chain" {
                // Children of a join register on both roots, as Python's
                // chaining does, so a tick on either side updates them.
                let filter = Rc::new(RefCell::new(FilterView::new("jf".into(), join.clone(), passes)));
                let totals = Rc::new(RefCell::new(
                    AggregateView::new(
                        "jg".into(),
                        filter.clone(),
                        vec!["right_name".into()],
                        vec![("total".into(), "amount".into(), AggregateFunction::Sum)],
                    )
                    .unwrap(),
                ));
                for tick in [&p.orders_tick, &customers_tick] {
                    tick.register_filter(&filter);
                    tick.register_aggregate(&totals);
                }
                p.keep("join_filter", filter);
                p.keep("join_totals", totals);
            }
            p.customers = Some((customers, customers_tick));
            p.join = Some(join);
            warm_join(&p, n);
        }
        _ => unreachable!("unknown view {view}"),
    }
    p
}

/// Untimed: one order and one customer added and removed, so any per-side
/// join state built on first use exists before anything is measured.
fn warm_join(p: &Pipeline, n: usize) {
    let (customers, _) = p.customers.as_ref().unwrap();
    let m = n / 10;
    p.orders
        .borrow_mut()
        .append_row(order(n as i64, n as i64, ((n / 2) % m) as i32, 10.0))
        .unwrap();
    p.tick(Side::Orders);
    p.orders.borrow_mut().delete_row(n).unwrap();
    p.tick(Side::Orders);
    customers.borrow_mut().append_row(customer(m as i32, "warm".into())).unwrap();
    p.tick(Side::Customers);
    customers.borrow_mut().delete_row(m).unwrap();
    p.tick(Side::Customers);
}

struct Ctx {
    rows: usize,
    hint: StorageHint,
    view: &'static str,
}

fn micros(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e6
}

fn round3(us: f64) -> f64 {
    (us * 1000.0).round() / 1000.0
}

/// Median and nearest-rank p95 of the measured samples.
fn stats(mut values: Vec<f64>) -> (f64, f64) {
    values.sort_by(f64::total_cmp);
    (values[values.len() / 2], values[(values.len() * 95).div_ceil(100) - 1])
}

fn report(rows: usize, hint: StorageHint, view: &str, workload: &str, samples: &[Sample]) {
    let measured = &samples[WARMUP..];
    let (mutate, _) = stats(measured.iter().map(|s| s.mutate_us).collect());
    let (tick, tick_p95) = stats(measured.iter().map(|s| s.tick_us).collect());
    let mut line = serde_json::json!({
        "rows": rows, "storage": storage_name(hint), "view": view, "workload": workload,
        "samples": measured.len(), "mutate_median_us": round3(mutate),
        "tick_median_us": round3(tick), "tick_p95_us": round3(tick_p95),
    });
    if measured[0].tick_parts.is_some() {
        let parts: Vec<(f64, f64)> = measured.iter().map(|s| s.tick_parts.unwrap()).collect();
        let (orders, _) = stats(parts.iter().map(|p| p.0).collect());
        let (customers, _) = stats(parts.iter().map(|p| p.1).collect());
        line["tick_orders_median_us"] = round3(orders).into();
        line["tick_customers_median_us"] = round3(customers).into();
    }
    println!("{line}");
}

/// Mutate, then tick, timing each separately. Outside the timing, check that
/// every view kept its history: a silent rebuild keeps lengths right, so the
/// length check alone would miss it.
fn timed(p: &Pipeline, side: Side, mutate: &dyn Fn()) -> Sample {
    let cursors: Vec<usize> = p
        .views
        .iter()
        .map(|(_, view, _)| view.borrow().changeset().expect("synced view exposes history").total_len())
        .collect();
    let start = Instant::now();
    mutate();
    let mutate_us = micros(start);
    let start = Instant::now();
    p.tick(side);
    let tick_us = micros(start);
    black_box(p.views.first().map(|(_, view, _)| view.borrow().len()));
    for ((name, view, _), cursor) in p.views.iter().zip(cursors) {
        let kept = view.borrow().changeset().expect("synced view exposes history").changes_from(cursor).is_some();
        assert!(kept, "{name} rebuilt instead of syncing incrementally");
    }
    Sample { mutate_us, tick_us, tick_parts: None }
}

/// One step of a two-step workload. `workload: None` marks an untimed step
/// that only restores the starting state.
struct Step<'a> {
    workload: Option<&'static str>,
    side: Side,
    mutate: Box<dyn Fn() + 'a>,
}

fn run_pair(ctx: &Ctx, p: &Pipeline, first: Step, second: Step) {
    let (mut a, mut b) = (Vec::new(), Vec::new());
    let label = first.workload.unwrap();
    for _ in 0..WARMUP + MEASURED {
        a.push(timed(p, first.side, &*first.mutate));
        let sample = timed(p, second.side, &*second.mutate);
        if second.workload.is_some() {
            b.push(sample);
        }
        p.check(ctx, label);
    }
    report(ctx.rows, ctx.hint, ctx.view, label, &a);
    if let Some(workload) = second.workload {
        report(ctx.rows, ctx.hint, ctx.view, workload, &b);
    }
}

/// value_edit, middle_insert/delete and tail_append/delete on `orders`.
fn left_workloads(ctx: &Ctx, p: &Pipeline) {
    let n = ctx.rows;
    let mid = n / 2;
    let cust = (mid % (n / 10)) as i32;
    let orders = &p.orders;
    let set = |row: usize, column: &'static str, value: ColumnValue| {
        move || orders.borrow_mut().set_value(row, column, value.clone()).unwrap()
    };
    run_pair(
        ctx,
        p,
        Step { workload: Some("value_edit"), side: Side::Orders, mutate: Box::new(set(mid, "amount", ColumnValue::Float64(11.0))) },
        Step { workload: None, side: Side::Orders, mutate: Box::new(set(mid, "amount", ColumnValue::Float64(10.0))) },
    );
    run_pair(
        ctx,
        p,
        Step {
            workload: Some("middle_insert"),
            side: Side::Orders,
            mutate: Box::new(move || orders.borrow_mut().insert_row(mid, order(n as i64, mid as i64, cust, 10.0)).unwrap()),
        },
        Step {
            workload: Some("middle_delete"),
            side: Side::Orders,
            mutate: Box::new(move || {
                orders.borrow_mut().delete_row(mid).unwrap();
            }),
        },
    );
    run_pair(
        ctx,
        p,
        Step {
            workload: Some("tail_append"),
            side: Side::Orders,
            mutate: Box::new(move || orders.borrow_mut().append_row(order(n as i64, n as i64, cust, 10.0)).unwrap()),
        },
        Step {
            workload: Some("tail_delete"),
            side: Side::Orders,
            mutate: Box::new(move || {
                orders.borrow_mut().delete_row(n).unwrap();
            }),
        },
    );
}

fn sort_key_move(ctx: &Ctx, p: &Pipeline) {
    let mid = ctx.rows / 2;
    let orders = &p.orders;
    let rank = |value: i64| move || orders.borrow_mut().set_value(mid, "rank", ColumnValue::Int64(value)).unwrap();
    run_pair(
        ctx,
        p,
        Step { workload: Some("sort_key_move"), side: Side::Orders, mutate: Box::new(rank(-1)) },
        Step { workload: None, side: Side::Orders, mutate: Box::new(rank(mid as i64)) },
    );
}

/// right_value_edit and right_insert/delete on `customers`.
fn right_workloads(ctx: &Ctx, p: &Pipeline) {
    let customers = &p.customers.as_ref().unwrap().0;
    let m = ctx.rows / 10;
    let mid = m / 2;
    let name = |value: String| {
        move || customers.borrow_mut().set_value(mid, "name", ColumnValue::String(value.clone())).unwrap()
    };
    run_pair(
        ctx,
        p,
        Step { workload: Some("right_value_edit"), side: Side::Customers, mutate: Box::new(name("alt".into())) },
        Step { workload: None, side: Side::Customers, mutate: Box::new(name(format!("c{mid}"))) },
    );
    run_pair(
        ctx,
        p,
        Step {
            workload: Some("right_insert"),
            side: Side::Customers,
            mutate: Box::new(move || customers.borrow_mut().append_row(customer(m as i32, "new".into())).unwrap()),
        },
        Step {
            workload: Some("right_delete"),
            side: Side::Customers,
            mutate: Box::new(move || {
                customers.borrow_mut().delete_row(m).unwrap();
            }),
        },
    );
}

/// The join's history cursor; only valid while both parents are synced.
fn join_cursor(join: &RefCell<JoinView>) -> usize {
    join.borrow().changeset().expect("synced join exposes history").total_len()
}

fn kept_history(join: &RefCell<JoinView>, cursor: usize) -> bool {
    join.borrow().changeset().expect("synced join exposes history").changes_from(cursor).is_some()
}

/// A new customer and that customer's first order, ticked together
/// (`both_then_tick`: one two-phase sync; before 2026-10-29 it rebuilt the
/// join) or one side at a time.
fn two_sided(ctx: &Ctx, p: &Pipeline) {
    let (n, m) = (ctx.rows, ctx.rows / 10);
    let mid = n / 2;
    let customers = &p.customers.as_ref().unwrap().0;
    let join = p.join.as_ref().unwrap();
    let insert_order = || {
        let start = Instant::now();
        p.orders.borrow_mut().insert_row(mid, order(n as i64, mid as i64, m as i32, 10.0)).unwrap();
        micros(start)
    };
    let insert_customer = || {
        let start = Instant::now();
        customers.borrow_mut().append_row(customer(m as i32, "new".into())).unwrap();
        micros(start)
    };
    let tick = |side| {
        let start = Instant::now();
        p.tick(side);
        micros(start)
    };
    for variant in ["both_then_tick", "tick_each_side"] {
        let mut samples = Vec::new();
        for _ in 0..WARMUP + MEASURED {
            let cursor = join_cursor(join);
            let sample = if variant == "both_then_tick" {
                let mutate_us = insert_order() + insert_customer();
                let orders_us = tick(Side::Orders);
                assert!(
                    kept_history(join, cursor),
                    "{n} rows {} {} {variant}: two-sided sync rebuilt",
                    storage_name(ctx.hint),
                    ctx.view
                );
                let customers_us = tick(Side::Customers);
                Sample { mutate_us, tick_us: orders_us + customers_us, tick_parts: Some((orders_us, customers_us)) }
            } else {
                let order_us = insert_order();
                let orders_us = tick(Side::Orders);
                assert!(
                    kept_history(join, cursor),
                    "{n} rows {} {} {variant}: orders tick rebuilt",
                    storage_name(ctx.hint),
                    ctx.view
                );
                let cursor = join_cursor(join);
                let customer_us = insert_customer();
                let customers_us = tick(Side::Customers);
                assert!(
                    kept_history(join, cursor),
                    "{n} rows {} {} {variant}: customers tick rebuilt",
                    storage_name(ctx.hint),
                    ctx.view
                );
                Sample {
                    mutate_us: order_us + customer_us,
                    tick_us: orders_us + customers_us,
                    tick_parts: Some((orders_us, customers_us)),
                }
            };
            samples.push(sample);
            // Untimed cleanup, one side at a time so it never rebuilds.
            p.orders.borrow_mut().delete_row(mid).unwrap();
            p.tick(Side::Orders);
            customers.borrow_mut().delete_row(m).unwrap();
            p.tick(Side::Customers);
            p.check(ctx, variant);
        }
        report(n, ctx.hint, ctx.view, &format!("two_sided_{variant}"), &samples);
    }
}

fn main() {
    assert!(
        !cfg!(debug_assertions),
        "debug builds give meaningless timings: run with --release"
    );
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .map(|s| s.parse().expect("integer row count"))
        .collect();
    let sizes = if sizes.is_empty() {
        vec![10_000, 100_000, 1_000_000]
    } else {
        sizes
    };
    for &rows in &sizes {
        assert!(rows >= 1000 && rows % 10 == 0, "row counts must be >= 1000 and divisible by 10");
        for hint in [StorageHint::FastReads, StorageHint::FastUpdates] {
            for view in ["none", "filter", "sort", "aggregate", "join", "join_chain"] {
                eprintln!("{rows} {} {view}", storage_name(hint));
                let ctx = Ctx { rows, hint, view };
                let p = build(view, rows, hint);
                left_workloads(&ctx, &p);
                if view == "sort" {
                    sort_key_move(&ctx, &p);
                }
                if p.join.is_some() {
                    right_workloads(&ctx, &p);
                    two_sided(&ctx, &p);
                }
            }
        }
    }
}
