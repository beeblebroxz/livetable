//! What one `sync()` costs a join or a sorted view after a batch of B
//! structural events, beside a full rebuild of the same view. Prints one JSON
//! line per (rows, view, workload, batch).
//! Run: cargo run --release --example batch_replay_benchmark -- 100000 1000000
//! See docs/VIEW_MAINTENANCE_BENCHMARK.md.
use livetable::{
    ColumnType, ColumnValue, JoinType, JoinView, ReadableTable, Schema, SortKey, SortedView,
    StorageHint, Table,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;

type Row = HashMap<String, ColumnValue>;
type Shared = Rc<RefCell<Table>>;

/// Samples per figure; the median is reported.
const SAMPLES: usize = 3;
const BATCHES: [usize; 4] = [64, 256, 1024, 4096];
/// Sorted views rebuild above 256 events, so larger batches measure nothing new.
const SORT_BATCHES: [usize; 2] = [64, 256];

/// Deterministic positions and keys.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, limit: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % limit
    }
}

fn order(id: i64, rank: i64, cust: i32) -> Row {
    HashMap::from([
        ("id".into(), ColumnValue::Int64(id)),
        ("rank".into(), ColumnValue::Int64(rank)),
        ("cust".into(), ColumnValue::Int32(cust)),
        ("amount".into(), ColumnValue::Float64(1.0)),
    ])
}

/// N orders, ten per customer, with `rank` in no particular order. FastUpdates
/// keeps the untimed root mutations cheap; view sync costs are within about
/// 10% of FastReads (see the view maintenance benchmark).
fn orders(n: usize, rng: &mut Lcg) -> Shared {
    let mut table = Table::with_hint(
        "orders".into(),
        Schema::new(vec![
            ("id".into(), ColumnType::Int64, false),
            ("rank".into(), ColumnType::Int64, false),
            ("cust".into(), ColumnType::Int32, false),
            ("amount".into(), ColumnType::Float64, false),
        ]),
        StorageHint::FastUpdates,
    );
    for i in 0..n {
        let rank = rng.below(n * 4) as i64;
        table
            .append_row(order(i as i64, rank, (i % (n / 10)) as i32))
            .unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn customers(m: usize) -> Shared {
    let mut table = Table::with_hint(
        "customers".into(),
        Schema::new(vec![
            ("cid".into(), ColumnType::Int32, false),
            ("name".into(), ColumnType::String, false),
        ]),
        StorageHint::FastUpdates,
    );
    for cid in 0..m {
        table
            .append_row(HashMap::from([
                ("cid".into(), ColumnValue::Int32(cid as i32)),
                ("name".into(), ColumnValue::String(format!("c{cid}"))),
            ]))
            .unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn micros(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e6
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

/// One measured batch: the incremental sync, whether it kept history, and a
/// rebuild of the same view over the same rows.
struct Sample {
    sync_us: f64,
    rebuild_us: f64,
    kept_history: bool,
}

fn report(rows: usize, view: &str, workload: &str, batch: usize, samples: &[Sample]) {
    let sync_us = median(samples.iter().map(|s| s.sync_us).collect());
    let rebuild_us = median(samples.iter().map(|s| s.rebuild_us).collect());
    println!(
        "{{\"rows\":{rows},\"view\":\"{view}\",\"workload\":\"{workload}\",\"batch\":{batch},\
         \"samples\":{},\"sync_us\":{:.1},\"rebuild_us\":{:.1},\"sync_over_rebuild\":{:.2},\
         \"kept_history\":{}}}",
        samples.len(),
        sync_us,
        rebuild_us,
        sync_us / rebuild_us,
        samples.iter().all(|s| s.kept_history),
    );
}

fn cursor(view: &dyn ReadableTable) -> usize {
    view.changeset()
        .expect("a synced view exposes history")
        .total_len()
}

fn kept(view: &dyn ReadableTable, cursor: usize) -> bool {
    view.changeset()
        .is_some_and(|history| history.changes_from(cursor).is_some())
}

/// Which parent a join workload mutates, and how.
#[derive(Clone, Copy)]
enum Edit {
    Append,
    Insert,
    Delete,
}

fn edit_orders(o: &Shared, edit: Edit, batch: usize, customers: usize, rng: &mut Lcg) {
    let mut table = o.borrow_mut();
    for _ in 0..batch {
        let len = table.len();
        let row = order(len as i64, 0, rng.below(customers) as i32);
        match edit {
            Edit::Append => table.append_row(row).unwrap(),
            Edit::Insert => table.insert_row(rng.below(len), row).unwrap(),
            Edit::Delete => {
                table.delete_row(rng.below(len)).unwrap();
            }
        }
    }
}

/// `orders_left` is orders LEFT JOIN customers, so the batch is on the left
/// side; `orders_right` is customers LEFT JOIN orders, the batch on the right.
fn join_workloads(n: usize, o: &Shared, c: &Shared, rng: &mut Lcg) {
    let m = n / 10;
    let layouts: [(&str, JoinView); 2] = [
        (
            "orders_left",
            JoinView::new(
                "j".into(),
                o.clone(),
                c.clone(),
                "cust".into(),
                "cid".into(),
                JoinType::Left,
            )
            .unwrap(),
        ),
        (
            "orders_right",
            JoinView::new(
                "j".into(),
                c.clone(),
                o.clone(),
                "cid".into(),
                "cust".into(),
                JoinType::Left,
            )
            .unwrap(),
        ),
    ];
    for (layout, mut join) in layouts {
        for (name, edit) in [
            ("append", Edit::Append),
            ("insert_scattered", Edit::Insert),
            ("delete_scattered", Edit::Delete),
        ] {
            for batch in BATCHES {
                let mut samples = Vec::new();
                for _ in 0..SAMPLES {
                    o.borrow_mut().clear_changeset();
                    c.borrow_mut().clear_changeset();
                    join.refresh();
                    // Untimed: one event of the same kind builds whatever
                    // per-side state the first sync after a rebuild builds.
                    edit_orders(o, edit, 1, m, rng);
                    join.sync();

                    let before = cursor(&join);
                    edit_orders(o, edit, batch, m, rng);
                    let start = Instant::now();
                    join.sync();
                    let sync_us = micros(start);
                    let kept_history = kept(&join, before);
                    let synced_len = join.len();

                    let start = Instant::now();
                    join.refresh();
                    let rebuild_us = micros(start);
                    assert_eq!(synced_len, join.len(), "sync and rebuild disagree");
                    samples.push(Sample {
                        sync_us,
                        rebuild_us,
                        kept_history,
                    });
                }
                report(n, "join", &format!("{layout}_{name}"), batch, &samples);
            }
        }
    }
}

fn sort_workloads(n: usize, o: &Shared, rng: &mut Lcg) {
    let mut sorted = SortedView::new("s".into(), o.clone(), vec![SortKey::ascending("rank")])
        .expect("rank column exists");
    for (name, edit) in [
        ("append", Edit::Append),
        ("insert_scattered", Edit::Insert),
        ("delete_scattered", Edit::Delete),
    ] {
        for batch in SORT_BATCHES {
            let mut samples = Vec::new();
            for _ in 0..SAMPLES {
                o.borrow_mut().clear_changeset();
                sorted.refresh();
                let before = cursor(&sorted);
                {
                    let mut table = o.borrow_mut();
                    for _ in 0..batch {
                        let len = table.len();
                        let row = order(len as i64, rng.below(n * 4) as i64, 0);
                        match edit {
                            Edit::Append => table.append_row(row).unwrap(),
                            Edit::Insert => table.insert_row(rng.below(len), row).unwrap(),
                            Edit::Delete => {
                                table.delete_row(rng.below(len)).unwrap();
                            }
                        }
                    }
                }
                let start = Instant::now();
                sorted.sync();
                let sync_us = micros(start);
                let kept_history = kept(&sorted, before);

                let start = Instant::now();
                sorted.refresh();
                let rebuild_us = micros(start);
                samples.push(Sample {
                    sync_us,
                    rebuild_us,
                    kept_history,
                });
            }
            report(n, "sort", name, batch, &samples);
        }
    }
}

fn main() {
    // Not `assert!`: clippy rejects an assertion on a constant.
    if cfg!(debug_assertions) {
        panic!("debug builds give meaningless timings: run with --release");
    }
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .map(|s| s.parse().expect("integer row count"))
        .collect();
    let sizes = if sizes.is_empty() {
        vec![100_000, 1_000_000]
    } else {
        sizes
    };
    for &rows in &sizes {
        assert!(
            rows >= 10_000 && rows % 10 == 0,
            "row counts must be >= 10000 and divisible by 10"
        );
        let mut rng = Lcg(rows as u64);
        let o = orders(rows, &mut rng);
        let c = customers(rows / 10);
        eprintln!("{rows} join");
        join_workloads(rows, &o, &c, &mut rng);
        o.borrow_mut().clear_changeset();
        eprintln!("{rows} sort");
        sort_workloads(rows, &o, &mut rng);
    }
}
