//! AggregateView construction cost, MEDIAN against SUM, plus one incremental
//! middle insert per view. Prints one JSON line per (rows, groups, function).
//! Run: cargo run --release --example aggregate_build_benchmark
//! See "After the percentile build fix" in docs/VIEW_MAINTENANCE_BENCHMARK.md.
use livetable::{
    AggregateFunction, AggregateView, ColumnType, ColumnValue, Schema, Table, TickableTable,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

const BUILDS: usize = 3;
const TICK_WARMUP: usize = 2;
const TICKS: usize = 15;

/// `rows` orders spread round-robin over `groups` regions, with amounts in
/// a scrambled order so no group receives its values sorted.
fn orders(rows: usize, groups: usize) -> Rc<RefCell<Table>> {
    let mut table = Table::new(
        "orders".into(),
        Schema::new(vec![
            ("region".into(), ColumnType::String, false),
            ("amount".into(), ColumnType::Float64, false),
        ]),
    );
    for i in 0..rows {
        table
            .append_row(HashMap::from([
                ("region".into(), ColumnValue::String(format!("r{}", i % groups))),
                ("amount".into(), ColumnValue::Float64(((i * 7919) % 100_000) as f64)),
            ]))
            .unwrap();
    }
    table.clear_changeset();
    Rc::new(RefCell::new(table))
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn main() {
    assert!(
        !cfg!(debug_assertions),
        "debug builds give meaningless timings: run with --release"
    );
    for (rows, groups) in [(10_000, 8), (100_000, 8), (1_000_000, 8), (100_000, 1), (300_000, 1)] {
        let table = orders(rows, groups);
        for (name, func) in [("sum", AggregateFunction::Sum), ("median", AggregateFunction::Median)] {
            eprintln!("{rows} {groups} {name}");
            let build = || {
                AggregateView::new(
                    "by_region".into(),
                    table.clone(),
                    vec!["region".into()],
                    vec![("x".into(), "amount".into(), func)],
                )
                .unwrap()
            };
            let mut builds = Vec::new();
            for _ in 0..BUILDS {
                let start = Instant::now();
                let view = build();
                builds.push(start.elapsed().as_secs_f64() * 1e3);
                assert_eq!(black_box(view).len(), groups);
            }

            // A middle insert into group r0, timed through one tick, then
            // deleted again so every sample starts from the same table.
            let view = Rc::new(RefCell::new(build()));
            let tick = TickableTable::new(table.clone());
            tick.register_aggregate(&view);
            let mut ticks = Vec::new();
            for sample in 0..TICK_WARMUP + TICKS {
                let row = HashMap::from([
                    ("region".into(), ColumnValue::String("r0".into())),
                    ("amount".into(), ColumnValue::Float64(50_000.5)),
                ]);
                table.borrow_mut().insert_row(rows / 2, row).unwrap();
                let start = Instant::now();
                tick.tick();
                if sample >= TICK_WARMUP {
                    ticks.push(start.elapsed().as_secs_f64() * 1e6);
                }
                table.borrow_mut().delete_row(rows / 2).unwrap();
                tick.tick();
            }
            assert_eq!(view.borrow().len(), groups);

            let line = serde_json::json!({
                "rows": rows, "groups": groups, "function": name,
                "builds": BUILDS, "build_median_ms": (median(builds) * 10.0).round() / 10.0,
                "ticks": TICKS, "tick_median_us": (median(ticks) * 10.0).round() / 10.0,
            });
            println!("{line}");
        }
    }
}
