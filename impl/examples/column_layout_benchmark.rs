//! Retained allocator-requested bytes and warm column operations, without tables,
//! changesets, views, Python, or transport. Run this same harness on both revisions.
use livetable::{Column, ColumnType, ColumnValue, StringInterner};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

struct CountingAllocator;
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = System.realloc(ptr, layout, new_size);
        if !next.is_null() {
            LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn value(ty: ColumnType, i: usize, nullable: bool) -> ColumnValue {
    if nullable && i.is_multiple_of(10) {
        return ColumnValue::Null;
    }
    match ty {
        ColumnType::Int32 => ColumnValue::Int32((i % 1000) as i32),
        ColumnType::Float64 => ColumnValue::Float64((i % 1000) as f64 + 0.5),
        ColumnType::String => ColumnValue::String(format!("region-{}", i % 16)),
        _ => unreachable!(),
    }
}

fn measure(n: usize, tiered: bool, ty: ColumnType, nullable: bool, interned: bool) {
    let start_bytes = LIVE_BYTES.load(Ordering::Relaxed);
    let interner = interned.then(|| Arc::new(Mutex::new(StringInterner::new())));
    let mut column = Column::new_with_interner("value".into(), ty, nullable, tiered, interner);
    for i in 0..n {
        column.append(value(ty, i, nullable)).unwrap();
    }
    let retained_bytes = LIVE_BYTES.load(Ordering::Relaxed) - start_bytes;
    let mut scans = Vec::with_capacity(31);
    let mut edits = Vec::with_capacity(31);
    for sample in 0..36 {
        let start = Instant::now();
        if ty == ColumnType::String {
            for i in 0..n {
                black_box(column.get(black_box(i)).unwrap());
            }
        } else {
            let mut sum = 0.0;
            for i in 0..n {
                sum += column.get_f64(black_box(i)).unwrap_or(0.0);
            }
            black_box(sum);
        }
        let scan_us = start.elapsed().as_secs_f64() * 1e6;
        let added = value(ty, sample + 1, nullable);
        let start = Instant::now();
        column.insert(n / 2, added).unwrap();
        black_box(column.delete(n / 2).unwrap());
        let edit_us = start.elapsed().as_secs_f64() * 1e6;
        if sample >= 5 {
            scans.push(scan_us);
            edits.push(edit_us);
        }
    }
    scans.sort_by(f64::total_cmp);
    edits.sort_by(f64::total_cmp);
    println!(
        "{n},{},{ty:?},{nullable},{interned},{retained_bytes},{:.3},{:.3},{:.3},{:.3}",
        if tiered { "tiered" } else { "array" },
        scans[15],
        scans[29],
        edits[15],
        edits[29],
    );
}

fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .map(|s| s.parse().unwrap())
        .collect();
    let sizes = if sizes.is_empty() {
        vec![10_000, 100_000]
    } else {
        sizes
    };
    println!(
        "ColumnValue size: {} bytes; Column header: {} bytes",
        std::mem::size_of::<ColumnValue>(),
        std::mem::size_of::<Column>()
    );
    println!("rows,backend,type,nullable,interned,retained_heap_bytes,scan_median_us,scan_p95_us,middle_pair_median_us,middle_pair_p95_us");
    for n in sizes {
        assert!(n > 0);
        for tiered in [false, true] {
            for (ty, nullable, interned) in [
                (ColumnType::Int32, false, false),
                (ColumnType::Int32, true, false),
                (ColumnType::Float64, true, false),
                (ColumnType::String, true, false),
                (ColumnType::String, true, true),
            ] {
                measure(n, tiered, ty, nullable, interned);
            }
        }
    }
}
