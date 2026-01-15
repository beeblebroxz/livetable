#!/usr/bin/env python3
"""Analyze filter_expr performance bottleneck."""

import time
import livetable

# Create a table with 100k rows
SIZE = 100_000

schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("value", livetable.ColumnType.FLOAT64, False),
])
table = livetable.Table("test", schema)

rows = [{"id": i, "value": float(i)} for i in range(SIZE)]
table.append_rows(rows)

threshold = SIZE / 2

# Benchmark 1: filter_expr
print(f"\nBenchmark with {SIZE:,} rows, threshold={threshold:,}\n")

times = []
for _ in range(5):
    start = time.perf_counter()
    indices = table.filter_expr(f"value > {threshold}")
    end = time.perf_counter()
    times.append((end - start) * 1000)

median = sorted(times)[len(times)//2]
print(f"filter_expr:       {median:.2f}ms  ({len(indices)} matches)")

# Benchmark 2: Lambda filter
times = []
for _ in range(5):
    start = time.perf_counter()
    filtered = table.filter(lambda r: r["value"] > threshold)
    count = len(filtered)
    end = time.perf_counter()
    times.append((end - start) * 1000)

median = sorted(times)[len(times)//2]
print(f"lambda filter:     {median:.2f}ms  ({count} matches)")

# Benchmark 3: Just get_row overhead (no filtering)
times = []
for _ in range(5):
    start = time.perf_counter()
    for i in range(SIZE):
        row = table.get_row(i)
    end = time.perf_counter()
    times.append((end - start) * 1000)

median = sorted(times)[len(times)//2]
print(f"get_row loop:      {median:.2f}ms  (just row access)")

# Benchmark 4: Iteration (for row in table)
times = []
for _ in range(5):
    start = time.perf_counter()
    count = 0
    for row in table:
        if row["value"] > threshold:
            count += 1
    end = time.perf_counter()
    times.append((end - start) * 1000)

median = sorted(times)[len(times)//2]
print(f"iteration filter:  {median:.2f}ms  ({count} matches)")

print("\n--- Analysis ---")
print("If get_row loop is slow, the bottleneck is HashMap allocation per row.")
print("Optimization: Evaluate expressions directly against columns without building HashMaps.")
