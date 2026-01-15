/// Performance Example
///
/// This example demonstrates:
/// - Difference between ArraySequence and TieredVectorSequence
/// - When to use each storage backend
/// - Performance characteristics of different operations

use livetable::{ColumnType, ColumnValue, Schema, Table};
use std::collections::HashMap;
use std::time::Instant;

fn main() {
    println!("=== LiveTable Performance Example ===\n");

    let n = 10_000;

    // 1. Array-based table (default)
    println!("1. Array-based Table (default)");
    println!("   Good for: Read-heavy workloads, append-only");
    println!("   Performance: O(1) access, O(N) insert/delete\n");

    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("value".to_string(), ColumnType::Float64, false),
    ]);

    let mut array_table = Table::new("array_table".to_string(), schema.clone());

    let start = Instant::now();
    for i in 0..n {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(i as i32));
        row.insert("value".to_string(), ColumnValue::Float64(i as f64 * 1.5));
        array_table.append_row(row).unwrap();
    }
    let append_time = start.elapsed();
    println!("   Appending {} rows: {:?}", n, append_time);

    let start = Instant::now();
    for i in 0..1000 {
        let _ = array_table.get_value(i * (n / 1000), "value");
    }
    let access_time = start.elapsed();
    println!("   Random access (1000 reads): {:?}", access_time);

    let start = Instant::now();
    for _ in 0..100 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(99999));
        row.insert("value".to_string(), ColumnValue::Float64(0.0));
        array_table.insert_row(n / 2, row).unwrap();
    }
    let insert_time = start.elapsed();
    println!("   Middle inserts (100 inserts): {:?}\n", insert_time);

    // 2. Tiered Vector-based table
    println!("2. Tiered Vector Table");
    println!("   Good for: Mixed read/write, frequent inserts/deletes");
    println!("   Performance: O(1) access (small overhead), O(sqrt(N)) insert/delete\n");

    let mut tiered_table = Table::new_with_options("tiered_table".to_string(), schema, true);

    let start = Instant::now();
    for i in 0..n {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(i as i32));
        row.insert("value".to_string(), ColumnValue::Float64(i as f64 * 1.5));
        tiered_table.append_row(row).unwrap();
    }
    let append_time = start.elapsed();
    println!("   Appending {} rows: {:?}", n, append_time);

    let start = Instant::now();
    for i in 0..1000 {
        let _ = tiered_table.get_value(i * (n / 1000), "value");
    }
    let access_time = start.elapsed();
    println!("   Random access (1000 reads): {:?}", access_time);

    let start = Instant::now();
    for _ in 0..100 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(99999));
        row.insert("value".to_string(), ColumnValue::Float64(0.0));
        tiered_table.insert_row(n / 2, row).unwrap();
    }
    let insert_time = start.elapsed();
    println!("   Middle inserts (100 inserts): {:?}\n", insert_time);

    // 3. Summary
    println!("3. Recommendations:");
    println!("   Use Array-based (default) when:");
    println!("      - Mostly reading data");
    println!("      - Append-only workloads");
    println!("      - Memory is tight");
    println!("      - Want simplest/fastest code");
    println!();
    println!("   Use Tiered Vector when:");
    println!("      - Frequent inserts/deletes in middle");
    println!("      - Large tables with mixed operations");
    println!("      - Don't mind small memory overhead");
    println!("      - Want more predictable insert performance");

    println!("\n=== Example Complete ===");
}
