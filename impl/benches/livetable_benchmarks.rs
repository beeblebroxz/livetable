use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use livetable::*;
use std::collections::HashMap;

fn bench_array_sequence_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_sequence_append");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut seq = ArraySequence::<i32>::new();
                for i in 0..size {
                    seq.append(black_box(i));
                }
            });
        });
    }
    group.finish();
}

fn bench_tiered_vector_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("tiered_vector_append");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut seq = TieredVectorSequence::<i32>::new();
                for i in 0..size {
                    seq.append(black_box(i));
                }
            });
        });
    }
    group.finish();
}

fn bench_array_sequence_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_sequence_random_access");

    for size in [100, 1000, 10000].iter() {
        let mut seq = ArraySequence::<i32>::new();
        for i in 0..*size {
            seq.append(i);
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let idx = black_box((size / 2) as usize);
                seq.get(idx).unwrap()
            });
        });
    }
    group.finish();
}

fn bench_tiered_vector_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("tiered_vector_random_access");

    for size in [100, 1000, 10000].iter() {
        let mut seq = TieredVectorSequence::<i32>::new();
        for i in 0..*size {
            seq.append(i);
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let idx = black_box((size / 2) as usize);
                seq.get(idx).unwrap()
            });
        });
    }
    group.finish();
}

fn bench_array_sequence_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_sequence_insert");

    for size in [100, 1000, 5000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut seq = ArraySequence::<i32>::new();
                for i in 0..size {
                    seq.append(i);
                }
                // Insert in the middle
                seq.insert((size / 2) as usize, black_box(999)).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_tiered_vector_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("tiered_vector_insert");

    for size in [100, 1000, 5000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut seq = TieredVectorSequence::<i32>::new();
                for i in 0..size {
                    seq.append(i);
                }
                // Insert in the middle
                seq.insert((size / 2) as usize, black_box(999)).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_table_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_append");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let schema = Schema::new(vec![
                    ("id".to_string(), ColumnType::Int32, false),
                    ("value".to_string(), ColumnType::Float64, false),
                    ("name".to_string(), ColumnType::String, false),
                ]);

                let mut table = Table::new("benchmark".to_string(), schema);

                for i in 0..size {
                    let mut row = HashMap::new();
                    row.insert("id".to_string(), ColumnValue::Int32(i));
                    row.insert("value".to_string(), ColumnValue::Float64(i as f64 * 1.5));
                    row.insert("name".to_string(), ColumnValue::String(format!("item_{}", i)));
                    table.append_row(row).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_table_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_random_access");

    for size in [100, 1000, 10000].iter() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::Float64, false),
            ("name".to_string(), ColumnType::String, false),
        ]);

        let mut table = Table::new("benchmark".to_string(), schema);

        for i in 0..*size {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            row.insert("value".to_string(), ColumnValue::Float64(i as f64 * 1.5));
            row.insert("name".to_string(), ColumnValue::String(format!("item_{}", i)));
            table.append_row(row).unwrap();
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let idx = black_box((size / 2) as usize);
                table.get_row(idx).unwrap()
            });
        });
    }
    group.finish();
}

fn bench_column_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_operations");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut col = Column::new("test".to_string(), ColumnType::Int32, false);
                for i in 0..size {
                    col.append(ColumnValue::Int32(i));
                }

                // Random access
                col.get((size / 2) as usize).unwrap();

                // Update
                col.set((size / 4) as usize, ColumnValue::Int32(999)).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_array_sequence_append,
    bench_tiered_vector_append,
    bench_array_sequence_random_access,
    bench_tiered_vector_random_access,
    bench_array_sequence_insert,
    bench_tiered_vector_insert,
    bench_table_append,
    bench_table_random_access,
    bench_column_operations,
);

criterion_main!(benches);
