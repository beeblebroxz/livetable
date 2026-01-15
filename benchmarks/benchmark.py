"""
Python Benchmarks for LiveTable
Equivalent benchmarks to the Rust implementation for fair comparison
"""

import time
import json
from typing import Callable, Any, List, Tuple
from sequence import ArraySequence, TieredVectorSequence
from column import Column, ColumnType
from table import Table, Schema


class BenchmarkResult:
    def __init__(self, name: str, size: int, duration: float):
        self.name = name
        self.size = size
        self.duration = duration
        self.ops_per_sec = size / duration if duration > 0 else 0

    def __repr__(self):
        return f"{self.name}({self.size}): {self.duration*1000:.2f}ms ({self.ops_per_sec:.0f} ops/sec)"


def benchmark(func: Callable, iterations: int = 10) -> float:
    """Run a function multiple times and return average duration in seconds."""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)

    # Return median time to reduce noise
    times.sort()
    return times[len(times) // 2]


def bench_array_sequence_append(size: int) -> BenchmarkResult:
    """Benchmark ArraySequence append operations."""
    def run():
        seq = ArraySequence()
        for i in range(size):
            seq.append(i)

    duration = benchmark(run)
    return BenchmarkResult(f"array_sequence_append", size, duration)


def bench_tiered_vector_append(size: int) -> BenchmarkResult:
    """Benchmark TieredVectorSequence append operations."""
    def run():
        seq = TieredVectorSequence()
        for i in range(size):
            seq.append(i)

    duration = benchmark(run)
    return BenchmarkResult(f"tiered_vector_append", size, duration)


def bench_array_sequence_random_access(size: int) -> BenchmarkResult:
    """Benchmark ArraySequence random access."""
    seq = ArraySequence()
    for i in range(size):
        seq.append(i)

    def run():
        idx = size // 2
        _ = seq.get(idx)

    duration = benchmark(run, iterations=1000)
    return BenchmarkResult(f"array_sequence_random_access", size, duration * 1000)


def bench_tiered_vector_random_access(size: int) -> BenchmarkResult:
    """Benchmark TieredVectorSequence random access."""
    seq = TieredVectorSequence()
    for i in range(size):
        seq.append(i)

    def run():
        idx = size // 2
        _ = seq.get(idx)

    duration = benchmark(run, iterations=1000)
    return BenchmarkResult(f"tiered_vector_random_access", size, duration * 1000)


def bench_array_sequence_insert(size: int) -> BenchmarkResult:
    """Benchmark ArraySequence insert operations."""
    def run():
        seq = ArraySequence()
        for i in range(size):
            seq.append(i)
        # Insert in the middle
        seq.insert(size // 2, 999)

    duration = benchmark(run)
    return BenchmarkResult(f"array_sequence_insert", size, duration)


def bench_tiered_vector_insert(size: int) -> BenchmarkResult:
    """Benchmark TieredVectorSequence insert operations."""
    def run():
        seq = TieredVectorSequence()
        for i in range(size):
            seq.append(i)
        # Insert in the middle
        seq.insert(size // 2, 999)

    duration = benchmark(run)
    return BenchmarkResult(f"tiered_vector_insert", size, duration)


def bench_table_append(size: int) -> BenchmarkResult:
    """Benchmark Table append operations."""
    def run():
        schema = Schema([
            ("id", ColumnType.INT32, False),
            ("value", ColumnType.FLOAT64, False),
            ("name", ColumnType.STRING, False),
        ])
        table = Table("benchmark", schema)

        for i in range(size):
            table.append_row({
                "id": i,
                "value": i * 1.5,
                "name": f"item_{i}"
            })

    duration = benchmark(run)
    return BenchmarkResult(f"table_append", size, duration)


def bench_table_random_access(size: int) -> BenchmarkResult:
    """Benchmark Table random access."""
    schema = Schema([
        ("id", ColumnType.INT32, False),
        ("value", ColumnType.FLOAT64, False),
        ("name", ColumnType.STRING, False),
    ])
    table = Table("benchmark", schema)

    for i in range(size):
        table.append_row({
            "id": i,
            "value": i * 1.5,
            "name": f"item_{i}"
        })

    def run():
        idx = size // 2
        _ = table.get_row(idx)

    duration = benchmark(run, iterations=1000)
    return BenchmarkResult(f"table_random_access", size, duration * 1000)


def bench_column_operations(size: int) -> BenchmarkResult:
    """Benchmark Column operations."""
    def run():
        col = Column("test", ColumnType.INT32, nullable=False)
        for i in range(size):
            col.append(i)

        # Random access
        _ = col.get(size // 2)

        # Update
        col.set(size // 4, 999)

    duration = benchmark(run)
    return BenchmarkResult(f"column_operations", size, duration)


def run_all_benchmarks():
    """Run all benchmarks and print results."""
    sizes = [100, 1000, 10000]

    benchmarks = [
        ("ArraySequence Append", bench_array_sequence_append, sizes),
        ("TieredVector Append", bench_tiered_vector_append, sizes),
        ("ArraySequence Random Access", bench_array_sequence_random_access, sizes),
        ("TieredVector Random Access", bench_tiered_vector_random_access, sizes),
        ("ArraySequence Insert", bench_array_sequence_insert, [100, 1000, 5000]),
        ("TieredVector Insert", bench_tiered_vector_insert, [100, 1000, 5000]),
        ("Table Append", bench_table_append, sizes),
        ("Table Random Access", bench_table_random_access, sizes),
        ("Column Operations", bench_column_operations, sizes),
    ]

    results = []

    print("=" * 80)
    print("Python LiveTable Benchmarks")
    print("=" * 80)
    print()

    for bench_name, bench_func, bench_sizes in benchmarks:
        print(f"\n{bench_name}:")
        print("-" * 40)

        for size in bench_sizes:
            result = bench_func(size)
            results.append({
                "name": result.name,
                "size": result.size,
                "duration_ms": result.duration * 1000,
                "ops_per_sec": result.ops_per_sec
            })
            print(f"  Size {size:>6}: {result.duration*1000:>8.2f}ms  ({result.ops_per_sec:>12,.0f} ops/sec)")

    print("\n" + "=" * 80)

    # Save results to JSON for comparison
    with open('benchmark_results_python.json', 'w') as f:
        json.dump(results, f, indent=2)

    print("\nResults saved to benchmark_results_python.json")

    return results


if __name__ == "__main__":
    run_all_benchmarks()
