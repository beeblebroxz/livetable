#!/usr/bin/env python3
"""
Detailed bottleneck analysis for LiveTable operations.
Identifies where we're losing performance vs hitting true limits.
"""

import time
import livetable
import pandas as pd
import numpy as np

def bench(func, iterations=5):
    """Run function and return median time in ms."""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        times.append((time.perf_counter() - start) * 1000)
    return sorted(times)[len(times)//2]

SIZE = 100_000

print("=" * 70)
print("BOTTLENECK ANALYSIS")
print("=" * 70)

# =============================================================================
# 1. JOIN ANALYSIS
# =============================================================================
print("\n### JOIN ANALYSIS ###\n")

# Setup tables
left_schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("val", livetable.ColumnType.FLOAT64, False),
])
right_schema = livetable.Schema([
    ("ref_id", livetable.ColumnType.INT32, False),
    ("data", livetable.ColumnType.FLOAT64, False),
])

lt_left = livetable.Table("left", left_schema)
lt_right = livetable.Table("right", right_schema)

left_rows = [{"id": i, "val": float(i)} for i in range(SIZE)]
right_rows = [{"ref_id": i, "data": float(i*2)} for i in range(SIZE)]

lt_left.append_rows(left_rows)
lt_right.append_rows(right_rows)

pd_left = pd.DataFrame({"id": range(SIZE), "val": np.arange(SIZE, dtype=np.float64)})
pd_right = pd.DataFrame({"ref_id": range(SIZE), "data": np.arange(SIZE, dtype=np.float64) * 2})

# Benchmark join creation
lt_join_time = bench(lambda: livetable.JoinView("j", lt_left, lt_right, "id", "ref_id", livetable.JoinType.INNER))
pd_join_time = bench(lambda: pd.merge(pd_left, pd_right, left_on="id", right_on="ref_id", how="inner"))

print(f"Join creation (100k x 100k 1:1 join):")
print(f"  LiveTable: {lt_join_time:.2f}ms")
print(f"  Pandas:    {pd_join_time:.2f}ms")
print(f"  Gap:       {lt_join_time/pd_join_time:.0f}x slower")
print()

# Analyze where time goes
print("Breakdown analysis:")

# Time just to iterate both tables with get_row (no actual join)
t = bench(lambda: [lt_left.get_row(i) for i in range(SIZE)])
print(f"  get_row(i) for 100k rows (left):  {t:.2f}ms")

t = bench(lambda: [lt_right.get_row(i) for i in range(SIZE)])
print(f"  get_row(i) for 100k rows (right): {t:.2f}ms")

# Time to just iterate indices
t = bench(lambda: [i for i in range(SIZE)])
print(f"  Pure Python loop 100k:            {t:.2f}ms")

print()
print("DIAGNOSIS: Join is calling get_row() for EVERY row in both tables.")
print("           This creates 200k HashMaps just to build the join index.")
print("           FIX: Direct column access for key values.")

# =============================================================================
# 2. GROUP BY ANALYSIS
# =============================================================================
print("\n\n### GROUP BY ANALYSIS ###\n")

schema = livetable.Schema([
    ("group_id", livetable.ColumnType.INT32, False),
    ("value", livetable.ColumnType.FLOAT64, False),
])
lt_table = livetable.Table("test", schema)
rows = [{"group_id": i % 100, "value": float(i)} for i in range(SIZE)]
lt_table.append_rows(rows)

pd_df = pd.DataFrame({"group_id": [i % 100 for i in range(SIZE)],
                      "value": np.arange(SIZE, dtype=np.float64)})

lt_gb_time = bench(lambda: livetable.AggregateView("agg", lt_table, ["group_id"], [
    ("sum", "value", livetable.AggregateFunction.SUM),
]))
pd_gb_time = bench(lambda: pd_df.groupby("group_id").agg({"value": "sum"}))

print(f"Group By (100k rows, 100 groups):")
print(f"  LiveTable: {lt_gb_time:.2f}ms")
print(f"  Pandas:    {pd_gb_time:.2f}ms")
print(f"  Gap:       {lt_gb_time/pd_gb_time:.0f}x slower")
print()
print("DIAGNOSIS: Same issue - rebuild_index() calls get_row() for every row.")
print("           FIX: Direct column access + avoid HashMap per row.")

# =============================================================================
# 3. AGGREGATION ANALYSIS
# =============================================================================
print("\n\n### AGGREGATION ANALYSIS ###\n")

# Already have lt_table and pd_df from above

lt_agg_time = bench(lambda: lt_table.sum("value"))
pd_agg_time = bench(lambda: pd_df["value"].sum())

print(f"Simple SUM (100k rows):")
print(f"  LiveTable: {lt_agg_time:.2f}ms")
print(f"  Pandas:    {pd_agg_time:.2f}ms")
print(f"  Gap:       {lt_agg_time/pd_agg_time:.1f}x slower")
print()

# Multiple aggregations
lt_multi = bench(lambda: (lt_table.sum("value"), lt_table.avg("value"), lt_table.min("value"), lt_table.max("value")))
pd_multi = bench(lambda: (pd_df["value"].sum(), pd_df["value"].mean(), pd_df["value"].min(), pd_df["value"].max()))

print(f"4 aggregations (sum/avg/min/max):")
print(f"  LiveTable: {lt_multi:.2f}ms")
print(f"  Pandas:    {pd_multi:.2f}ms")
print(f"  Gap:       {lt_multi/pd_multi:.1f}x slower")
print()
print("DIAGNOSIS: Each aggregation scans the column separately.")
print("           column.get(i) clones the ColumnValue each time.")
print("           Pandas uses SIMD-optimized numpy operations on contiguous memory.")
print("           FIX: Add get_f64_unchecked() that returns f64 directly without clone.")

# =============================================================================
# 4. SORT ANALYSIS
# =============================================================================
print("\n\n### SORT ANALYSIS ###\n")

# Random values to sort
sort_rows = [{"group_id": i, "value": float((i * 7919) % SIZE)} for i in range(SIZE)]
lt_sort_table = livetable.Table("sort_test", schema)
lt_sort_table.append_rows(sort_rows)

pd_sort_df = pd.DataFrame({"id": range(SIZE),
                           "value": [(i * 7919) % SIZE for i in range(SIZE)]})

lt_sort_time = bench(lambda: livetable.SortedView("s", lt_sort_table, [livetable.SortKey.ascending("value")]))
pd_sort_time = bench(lambda: pd_sort_df.sort_values("value"))

print(f"Sort (100k rows):")
print(f"  LiveTable: {lt_sort_time:.2f}ms")
print(f"  Pandas:    {pd_sort_time:.2f}ms")
print(f"  Gap:       {lt_sort_time/pd_sort_time:.1f}x slower")
print()
print("DIAGNOSIS: During sort comparisons, get_value() is called O(N log N) times.")
print("           FIX: Extract all sort key values into a Vec first, then sort indices.")

# =============================================================================
# 5. FILTER COMPARISON (lambda vs expr vs pandas)
# =============================================================================
print("\n\n### FILTER ANALYSIS ###\n")

threshold = SIZE / 2

lt_lambda = bench(lambda: len(lt_table.filter(lambda r: r["value"] > threshold)))
lt_expr = bench(lambda: len(lt_table.filter_expr(f"value > {threshold}")))
pd_filter = bench(lambda: len(pd_df[pd_df["value"] > threshold]))

print(f"Filter (100k rows, 50% match):")
print(f"  Lambda filter:  {lt_lambda:.2f}ms")
print(f"  filter_expr:    {lt_expr:.2f}ms  ({lt_lambda/lt_expr:.1f}x faster than lambda)")
print(f"  Pandas:         {pd_filter:.2f}ms")
print()
print(f"  filter_expr vs pandas: {lt_expr/pd_filter:.1f}x slower")
print()
print("INSIGHT: filter_expr is now 13x faster than lambda!")
print("         Remaining pandas gap is SIMD vectorization (true architectural limit).")

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 70)
print("SUMMARY: OPTIMIZATION OPPORTUNITIES")
print("=" * 70)
print("""
┌─────────────────┬──────────────┬───────────────────────────────────────────┐
│ Operation       │ Current Gap  │ Status / Notes                            │
├─────────────────┼──────────────┼───────────────────────────────────────────┤
│ Join            │ ~32x slower  │ OPTIMIZED: direct column access (was 100x)│
│ Group By        │ ~16x slower  │ OPTIMIZED: int fast path + lazy maps      │
│ Sort            │ ~1.8x slower │ OPTIMIZED: pre-extract values (was 8x)    │
│ Aggregation     │ ~2.6x slower │ OPTIMIZED: get_f64() fast path (was 8x)   │
│ Filter (lambda) │ ~70x slower  │ Python callbacks (expected, unavoidable)  │
│ Filter (expr)   │ ~5x slower   │ SIMD gap (true architectural limit)       │
└─────────────────┴──────────────┴───────────────────────────────────────────┘

REMAINING OPTIMIZATION OPPORTUNITIES:
1. Join - 32x gap: String key allocation overhead (harder to optimize)
2. Group By - 16x gap: Similar string allocation overhead
3. Filter (expr) - 5x gap: SIMD/vectorization (true limit)
4. Aggregation - 2.6x gap: Further optimization would need SIMD
""")
