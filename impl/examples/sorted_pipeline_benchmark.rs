//! Mutation + tick timings for table -> filter -> sort -> aggregate.
//! Run: cargo run --release --example sorted_pipeline_benchmark -- 10000 100000
use livetable::{
    AggregateFunction, AggregateView, ColumnType, ColumnValue, FilterView, Schema, SortKey,
    SortedView, Table, TickableTable,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;

fn row(id: usize, amount: f64) -> HashMap<String, ColumnValue> {
    HashMap::from([
        ("id".into(), ColumnValue::Int64(id as i64)),
        ("rank".into(), ColumnValue::Int64(id as i64)),
        (
            "region".into(),
            ColumnValue::String(format!("region{}", id % 8)),
        ),
        ("amount".into(), ColumnValue::Float64(amount)),
    ])
}

fn run(size: usize, workload: &str, consumers: usize) {
    let mut table = Table::new(
        "sales".into(),
        Schema::new(vec![
            ("id".into(), ColumnType::Int64, false),
            ("rank".into(), ColumnType::Int64, false),
            ("region".into(), ColumnType::String, false),
            ("amount".into(), ColumnType::Float64, false),
        ]),
    );
    for id in 0..size {
        table
            .append_row(row(id, if id % 2 == 0 { 10.0 } else { 0.0 }))
            .unwrap();
    }
    table.clear_changeset();
    let table = Rc::new(RefCell::new(table));
    let tickable = TickableTable::new(table.clone());
    let filtered = Rc::new(RefCell::new(FilterView::new(
        "filtered".into(),
        table.clone(),
        |row| row["amount"].as_f64().is_some_and(|v| v >= 5.0),
    )));
    tickable.register_filter(&filtered);
    let sorted = Rc::new(RefCell::new(
        SortedView::new(
            "sorted".into(),
            filtered.clone(),
            vec![SortKey::ascending("rank")],
        )
        .unwrap(),
    ));
    tickable.register_sorted(&sorted);
    let aggregates: Vec<_> = (0..consumers)
        .map(|i| {
            let view = Rc::new(RefCell::new(
                AggregateView::new(
                    format!("totals{i}"),
                    sorted.clone(),
                    vec!["region".into()],
                    vec![("total".into(), "amount".into(), AggregateFunction::Sum)],
                )
                .unwrap(),
            ));
            tickable.register_aggregate(&view);
            view
        })
        .collect();
    let mut samples = Vec::new();
    for sample in 0..36 {
        let value = (sample % 2) as f64;
        let start = Instant::now();
        {
            let mut table = table.borrow_mut();
            match workload {
                "non_sort_update" => table
                    .set_value(size / 2, "amount", ColumnValue::Float64(10.0 + value))
                    .unwrap(),
                "excluded_update" => table
                    .set_value(1, "amount", ColumnValue::Float64(value))
                    .unwrap(),
                "batch_16_non_sort" => {
                    for index in 0..16 {
                        table
                            .set_value(index * 2, "amount", ColumnValue::Float64(10.0 + value))
                            .unwrap();
                    }
                }
                "sort_key_move" => table
                    .set_value(
                        0,
                        "rank",
                        ColumnValue::Int64(if sample % 2 == 0 { size as i64 } else { 0 }),
                    )
                    .unwrap(),
                "batch_16_sort_keys" => {
                    for index in 0..16 {
                        table
                            .set_value(
                                index * 2,
                                "rank",
                                ColumnValue::Int64(if sample % 2 == 0 {
                                    (size + index) as i64
                                } else {
                                    (index * 2) as i64
                                }),
                            )
                            .unwrap();
                    }
                }
                "mixed_batch" => {
                    table
                        .set_value(0, "amount", ColumnValue::Float64(10.0 + value))
                        .unwrap();
                    table
                        .insert_row(size / 2, row(size + sample, 20.0))
                        .unwrap();
                    table.delete_row(size / 2).unwrap();
                    table
                        .set_value(1, "amount", ColumnValue::Float64(value))
                        .unwrap();
                }
                _ => unreachable!(),
            }
        }
        tickable.tick();
        let elapsed = start.elapsed().as_secs_f64() * 1e6;
        std::hint::black_box(aggregates[0].borrow().get_row(0).unwrap());
        if sample >= 5 {
            samples.push(elapsed);
        }
    }
    samples.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({
            "rows": size, "workload": workload, "consumers": consumers,
            "samples": samples.len(), "median_us": samples[samples.len() / 2],
            "p95_us": samples[(samples.len() * 95).div_ceil(100) - 1],
        })
    );
}

fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .map(|s| s.parse().expect("integer row count"))
        .collect();
    let sizes = if sizes.is_empty() {
        vec![10_000, 100_000]
    } else {
        sizes
    };
    for size in sizes {
        assert!(size >= 32 && size % 4 == 0);
        for workload in [
            "non_sort_update",
            "excluded_update",
            "batch_16_non_sort",
            "sort_key_move",
            "batch_16_sort_keys",
            "mixed_batch",
        ] {
            run(size, workload, 1);
        }
        run(size, "non_sort_update", 4);
    }
}
