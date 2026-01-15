#!/usr/bin/env python3
"""Compare optimized filter_expr against pandas."""

import time
import livetable
import pandas as pd
import numpy as np

SIZE = 100_000

# Setup livetable
schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("value", livetable.ColumnType.FLOAT64, False),
])
lt_table = livetable.Table("test", schema)
rows = [{"id": i, "value": float(i)} for i in range(SIZE)]
lt_table.append_rows(rows)

# Setup pandas
pd_df = pd.DataFrame({"id": range(SIZE), "value": np.arange(SIZE, dtype=np.float64)})

threshold = SIZE / 2

print(f"\nFilter Benchmark ({SIZE:,} rows, threshold={threshold:,})\n")
print(f"{'Method':<25} {'Time':>10} {'Matches':>10} {'vs Pandas':>12}")
print("-" * 60)

# Benchmark pandas
times = []
for _ in range(5):
    start = time.perf_counter()
    result = pd_df[pd_df["value"] > threshold]
    count = len(result)
    end = time.perf_counter()
    times.append((end - start) * 1000)
pandas_median = sorted(times)[len(times)//2]
print(f"{'pandas (vectorized)':<25} {pandas_median:>8.2f}ms {count:>10}")

# Benchmark filter_expr (optimized)
times = []
for _ in range(5):
    start = time.perf_counter()
    indices = lt_table.filter_expr(f"value > {threshold}")
    count = len(indices)
    end = time.perf_counter()
    times.append((end - start) * 1000)
expr_median = sorted(times)[len(times)//2]
speedup = pandas_median / expr_median
status = f"{speedup:.1f}x {'faster' if speedup > 1 else 'slower'}"
print(f"{'filter_expr (rust)':<25} {expr_median:>8.2f}ms {count:>10} {status:>12}")

# Benchmark lambda filter
times = []
for _ in range(5):
    start = time.perf_counter()
    filtered = lt_table.filter(lambda r: r["value"] > threshold)
    count = len(filtered)
    end = time.perf_counter()
    times.append((end - start) * 1000)
lambda_median = sorted(times)[len(times)//2]
speedup = pandas_median / lambda_median
status = f"{speedup:.1f}x {'faster' if speedup > 1 else 'slower'}"
print(f"{'lambda filter (python)':<25} {lambda_median:>8.2f}ms {count:>10} {status:>12}")

print("\n" + "=" * 60)
print("Summary:")
print(f"  filter_expr is {lambda_median/expr_median:.1f}x faster than lambda")
print(f"  pandas is {expr_median/pandas_median:.1f}x faster than filter_expr")
print("=" * 60)
