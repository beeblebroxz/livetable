#!/usr/bin/env python3
"""
LiveTable vs Pandas Performance Benchmarks

Compares livetable (Rust + PyO3) against pandas for common operations.
Run with: python benchmark_vs_pandas.py
"""

import time
import json
import statistics
from dataclasses import dataclass
from typing import Callable, List, Optional

# Check dependencies
try:
    import livetable
except ImportError:
    print("ERROR: livetable not installed. Run: cd impl && ./install.sh")
    exit(1)

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("ERROR: pandas/numpy not installed. Run: pip install pandas numpy")
    exit(1)


@dataclass
class BenchmarkResult:
    """Result of a single benchmark comparison."""
    name: str
    size: int
    livetable_ms: float
    pandas_ms: float

    @property
    def speedup(self) -> float:
        """How many times faster livetable is vs pandas."""
        if self.livetable_ms == 0:
            return float('inf')
        return self.pandas_ms / self.livetable_ms

    @property
    def winner(self) -> str:
        return "livetable" if self.speedup > 1 else "pandas"


def benchmark(func: Callable, warmup: int = 2, iterations: int = 10) -> float:
    """Run function multiple times, return median duration in milliseconds."""
    # Warmup runs
    for _ in range(warmup):
        func()

    # Timed runs
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms

    return statistics.median(times)


# ============================================================================
# Benchmark: Row Insertion
# ============================================================================

def bench_insert_rows(size: int) -> BenchmarkResult:
    """Compare inserting rows one at a time."""

    def livetable_insert():
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("value", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("test", schema)
        for i in range(size):
            table.append_row({"id": i, "name": f"item_{i}", "value": float(i) * 1.5})

    def pandas_insert():
        rows = []
        for i in range(size):
            rows.append({"id": i, "name": f"item_{i}", "value": float(i) * 1.5})
        df = pd.DataFrame(rows)

    lt_time = benchmark(livetable_insert)
    pd_time = benchmark(pandas_insert)

    return BenchmarkResult("insert_rows", size, lt_time, pd_time)


def bench_bulk_insert(size: int) -> BenchmarkResult:
    """Compare bulk row insertion."""

    rows = [{"id": i, "name": f"item_{i}", "value": float(i) * 1.5} for i in range(size)]

    def livetable_bulk():
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("value", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("test", schema)
        table.append_rows(rows)

    def pandas_bulk():
        df = pd.DataFrame(rows)

    lt_time = benchmark(livetable_bulk)
    pd_time = benchmark(pandas_bulk)

    return BenchmarkResult("bulk_insert", size, lt_time, pd_time)


# ============================================================================
# Benchmark: Filtering
# ============================================================================

def bench_filter(size: int) -> BenchmarkResult:
    """Compare filtering rows."""

    # Setup livetable
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("value", livetable.ColumnType.FLOAT64, False),
    ])
    lt_table = livetable.Table("test", schema)
    rows = [{"id": i, "value": float(i)} for i in range(size)]
    lt_table.append_rows(rows)

    # Setup pandas
    pd_df = pd.DataFrame(rows)

    threshold = size / 2

    def livetable_filter():
        filtered = lt_table.filter(lambda r: r["value"] > threshold)
        # Force evaluation by getting length
        _ = len(filtered)

    def pandas_filter():
        filtered = pd_df[pd_df["value"] > threshold]
        _ = len(filtered)

    lt_time = benchmark(livetable_filter)
    pd_time = benchmark(pandas_filter)

    return BenchmarkResult("filter", size, lt_time, pd_time)


# ============================================================================
# Benchmark: Aggregations
# ============================================================================

def bench_aggregation(size: int) -> BenchmarkResult:
    """Compare simple aggregations (sum, avg, min, max)."""

    # Setup livetable
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("value", livetable.ColumnType.FLOAT64, False),
    ])
    lt_table = livetable.Table("test", schema)
    rows = [{"id": i, "value": float(i)} for i in range(size)]
    lt_table.append_rows(rows)

    # Setup pandas
    pd_df = pd.DataFrame(rows)

    def livetable_agg():
        _ = lt_table.sum("value")
        _ = lt_table.avg("value")
        _ = lt_table.min("value")
        _ = lt_table.max("value")

    def pandas_agg():
        _ = pd_df["value"].sum()
        _ = pd_df["value"].mean()
        _ = pd_df["value"].min()
        _ = pd_df["value"].max()

    lt_time = benchmark(livetable_agg)
    pd_time = benchmark(pandas_agg)

    return BenchmarkResult("aggregation", size, lt_time, pd_time)


# ============================================================================
# Benchmark: Joins
# ============================================================================

def bench_join(size: int) -> BenchmarkResult:
    """Compare join operations."""

    # Setup livetable
    left_schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("left_val", livetable.ColumnType.STRING, False),
    ])
    right_schema = livetable.Schema([
        ("ref_id", livetable.ColumnType.INT32, False),
        ("right_val", livetable.ColumnType.FLOAT64, False),
    ])

    lt_left = livetable.Table("left", left_schema)
    lt_right = livetable.Table("right", right_schema)

    left_rows = [{"id": i, "left_val": f"L{i}"} for i in range(size)]
    right_rows = [{"ref_id": i, "right_val": float(i) * 2} for i in range(size)]

    lt_left.append_rows(left_rows)
    lt_right.append_rows(right_rows)

    # Setup pandas
    pd_left = pd.DataFrame(left_rows)
    pd_right = pd.DataFrame(right_rows)

    def livetable_join():
        joined = livetable.JoinView("j", lt_left, lt_right, "id", "ref_id", livetable.JoinType.INNER)
        _ = len(joined)

    def pandas_join():
        joined = pd.merge(pd_left, pd_right, left_on="id", right_on="ref_id", how="inner")
        _ = len(joined)

    lt_time = benchmark(livetable_join)
    pd_time = benchmark(pandas_join)

    return BenchmarkResult("join", size, lt_time, pd_time)


# ============================================================================
# Benchmark: Sorting
# ============================================================================

def bench_sort(size: int) -> BenchmarkResult:
    """Compare sorting operations."""

    # Setup livetable
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("value", livetable.ColumnType.FLOAT64, False),
    ])
    lt_table = livetable.Table("test", schema)

    # Use random-ish values
    rows = [{"id": i, "value": float((i * 7) % size)} for i in range(size)]
    lt_table.append_rows(rows)

    # Setup pandas
    pd_df = pd.DataFrame(rows)

    def livetable_sort():
        sorted_view = livetable.SortedView("s", lt_table, [livetable.SortKey.ascending("value")])
        _ = len(sorted_view)

    def pandas_sort():
        sorted_df = pd_df.sort_values("value")
        _ = len(sorted_df)

    lt_time = benchmark(livetable_sort)
    pd_time = benchmark(pandas_sort)

    return BenchmarkResult("sort", size, lt_time, pd_time)


# ============================================================================
# Benchmark: Iteration
# ============================================================================

def bench_iteration(size: int) -> BenchmarkResult:
    """Compare iterating over all rows."""

    # Setup livetable
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("value", livetable.ColumnType.FLOAT64, False),
    ])
    lt_table = livetable.Table("test", schema)
    rows = [{"id": i, "value": float(i)} for i in range(size)]
    lt_table.append_rows(rows)

    # Setup pandas
    pd_df = pd.DataFrame(rows)

    def livetable_iter():
        total = 0
        for row in lt_table:
            total += row["value"]
        return total

    def pandas_iter():
        total = 0
        for _, row in pd_df.iterrows():
            total += row["value"]
        return total

    lt_time = benchmark(livetable_iter)
    pd_time = benchmark(pandas_iter)

    return BenchmarkResult("iteration", size, lt_time, pd_time)


# ============================================================================
# Benchmark: Random Access
# ============================================================================

def bench_random_access(size: int) -> BenchmarkResult:
    """Compare random row access."""

    # Setup livetable
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("value", livetable.ColumnType.FLOAT64, False),
    ])
    lt_table = livetable.Table("test", schema)
    rows = [{"id": i, "value": float(i)} for i in range(size)]
    lt_table.append_rows(rows)

    # Setup pandas
    pd_df = pd.DataFrame(rows)

    # Access pattern: every 10th row
    indices = list(range(0, size, 10))

    def livetable_access():
        for idx in indices:
            _ = lt_table.get_row(idx)

    def pandas_access():
        for idx in indices:
            _ = pd_df.iloc[idx].to_dict()

    lt_time = benchmark(livetable_access)
    pd_time = benchmark(pandas_access)

    return BenchmarkResult("random_access", size, lt_time, pd_time)


# ============================================================================
# Benchmark: Group By Aggregation
# ============================================================================

def bench_group_by(size: int) -> BenchmarkResult:
    """Compare GROUP BY aggregation."""

    num_groups = min(100, size // 10)

    # Setup livetable
    schema = livetable.Schema([
        ("group_id", livetable.ColumnType.INT32, False),
        ("value", livetable.ColumnType.FLOAT64, False),
    ])
    lt_table = livetable.Table("test", schema)
    rows = [{"group_id": i % num_groups, "value": float(i)} for i in range(size)]
    lt_table.append_rows(rows)

    # Setup pandas
    pd_df = pd.DataFrame(rows)

    def livetable_groupby():
        agg = livetable.AggregateView("agg", lt_table, ["group_id"], [
            ("sum_val", "value", livetable.AggregateFunction.SUM),
            ("avg_val", "value", livetable.AggregateFunction.AVG),
        ])
        _ = len(agg)

    def pandas_groupby():
        grouped = pd_df.groupby("group_id").agg({"value": ["sum", "mean"]})
        _ = len(grouped)

    lt_time = benchmark(livetable_groupby)
    pd_time = benchmark(pandas_groupby)

    return BenchmarkResult("group_by", size, lt_time, pd_time)


# ============================================================================
# Main Runner
# ============================================================================

def run_all_benchmarks() -> List[BenchmarkResult]:
    """Run all benchmarks and return results."""

    sizes = [1_000, 10_000, 100_000]

    benchmarks = [
        ("Row Insertion (one-by-one)", bench_insert_rows),
        ("Bulk Insert (append_rows)", bench_bulk_insert),
        ("Filter", bench_filter),
        ("Aggregation (sum/avg/min/max)", bench_aggregation),
        ("Join (INNER)", bench_join),
        ("Sort", bench_sort),
        ("Iteration (for row in table)", bench_iteration),
        ("Random Access (get_row)", bench_random_access),
        ("Group By Aggregation", bench_group_by),
    ]

    results = []

    print("=" * 80)
    print("LiveTable vs Pandas Performance Benchmark")
    print("=" * 80)
    print(f"\nSizes: {sizes}")
    print(f"System: livetable (Rust + PyO3) vs pandas {pd.__version__}")
    print()

    for bench_name, bench_func in benchmarks:
        print(f"\n{bench_name}")
        print("-" * 60)
        print(f"{'Size':>10} | {'LiveTable':>12} | {'Pandas':>12} | {'Speedup':>10} | Winner")
        print("-" * 60)

        for size in sizes:
            try:
                result = bench_func(size)
                results.append(result)

                speedup_str = f"{result.speedup:.1f}x"
                winner_mark = "*" if result.winner == "livetable" else ""

                print(f"{size:>10,} | {result.livetable_ms:>10.2f}ms | {result.pandas_ms:>10.2f}ms | {speedup_str:>10} | {result.winner}{winner_mark}")
            except Exception as e:
                print(f"{size:>10,} | ERROR: {e}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    lt_wins = sum(1 for r in results if r.winner == "livetable")
    pd_wins = len(results) - lt_wins

    print(f"\nLiveTable wins: {lt_wins}/{len(results)} benchmarks")
    print(f"Pandas wins: {pd_wins}/{len(results)} benchmarks")

    if lt_wins > 0:
        avg_speedup = statistics.mean([r.speedup for r in results if r.speedup > 1])
        max_speedup = max(r.speedup for r in results)
        print(f"\nWhere LiveTable wins:")
        print(f"  Average speedup: {avg_speedup:.1f}x faster")
        print(f"  Max speedup: {max_speedup:.1f}x faster")

    # Save results to JSON
    results_json = [{
        "name": r.name,
        "size": r.size,
        "livetable_ms": r.livetable_ms,
        "pandas_ms": r.pandas_ms,
        "speedup": r.speedup,
        "winner": r.winner,
    } for r in results]

    with open("benchmark_results.json", "w") as f:
        json.dump(results_json, f, indent=2)

    print(f"\nResults saved to benchmark_results.json")

    return results


if __name__ == "__main__":
    run_all_benchmarks()
