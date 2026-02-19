#!/usr/bin/env python3
"""
LLM Usage Analytics with LiveTable
====================================
Shows how to use percentile aggregations for analyzing LLM usage patterns.

Use case: You have a year of monthly LLM usage data and want to break it
into quintiles per month to understand usage distribution — without building
a cube or waiting for slow SQL full-table scans.

LiveTable keeps running percentiles in memory. When new data arrives,
call tick() and only the new rows are processed.
"""

import random
import time
import livetable

random.seed(42)

print("LLM Usage Analytics with LiveTable")
print("=" * 60)

# --- 1. Schema and table ---

schema = livetable.Schema([
    ("month", livetable.ColumnType.STRING, False),
    ("user_id", livetable.ColumnType.INT32, False),
    ("tokens_used", livetable.ColumnType.FLOAT64, False),
    ("latency_ms", livetable.ColumnType.FLOAT64, False),
    ("model", livetable.ColumnType.STRING, False),
])
table = livetable.Table("llm_usage", schema)

# --- 2. Generate a year of synthetic usage data ---

models = ["opus", "sonnet", "haiku"]
months = [f"2025-{m:02d}" for m in range(1, 13)]

rows = []
for month in months:
    # ~500 requests per month from 50 users
    for _ in range(500):
        user = random.randint(1, 50)
        model = random.choice(models)
        # Token usage: heavy-tailed distribution (most small, some huge)
        tokens = random.lognormvariate(7, 1.5)
        latency = random.lognormvariate(5, 0.8) if model != "haiku" else random.lognormvariate(4, 0.6)
        rows.append({
            "month": month,
            "user_id": user,
            "tokens_used": round(tokens, 1),
            "latency_ms": round(latency, 1),
            "model": model,
        })

t0 = time.perf_counter()
table.append_rows(rows)
load_time = (time.perf_counter() - t0) * 1000
print(f"\nLoaded {len(rows):,} rows in {load_time:.1f}ms")

# --- 3. Monthly quintiles + stats ---

print("\n--- Monthly Token Usage Quintiles ---")
print(f"{'Month':<10} {'P20':>10} {'P40':>10} {'P60':>10} {'P80':>10} {'P95':>10} {'Median':>10}")
print("-" * 72)

t0 = time.perf_counter()
monthly = table.group_by("month", agg=[
    ("p20", "tokens_used", "p25"),
    ("p40", "tokens_used", "percentile(0.4)"),
    ("median", "tokens_used", "median"),
    ("p60", "tokens_used", "percentile(0.6)"),
    ("p80", "tokens_used", "p75"),
    ("p95", "tokens_used", "p95"),
])
agg_time = (time.perf_counter() - t0) * 1000

for i in range(len(monthly)):
    r = monthly.get_row(i)
    print(f"{r['month']:<10} {r['p20']:>10,.0f} {r['p40']:>10,.0f} {r['p60']:>10,.0f} {r['p80']:>10,.0f} {r['p95']:>10,.0f} {r['median']:>10,.0f}")

print(f"\nComputed in {agg_time:.2f}ms")

# --- 4. Per-model latency percentiles ---

print("\n--- Latency Percentiles by Model ---")
print(f"{'Model':<10} {'P50 (ms)':>10} {'P90 (ms)':>10} {'P99 (ms)':>10}")
print("-" * 42)

by_model = table.group_by("model", agg=[
    ("p50", "latency_ms", "median"),
    ("p90", "latency_ms", "p90"),
    ("p99", "latency_ms", "p99"),
])

for i in range(len(by_model)):
    r = by_model.get_row(i)
    print(f"{r['model']:<10} {r['p50']:>10,.1f} {r['p90']:>10,.1f} {r['p99']:>10,.1f}")

# --- 5. Incremental update: new month arrives ---

print("\n--- Incremental Update: January 2026 data arrives ---")

new_rows = []
for _ in range(500):
    new_rows.append({
        "month": "2026-01",
        "user_id": random.randint(1, 50),
        "tokens_used": round(random.lognormvariate(7, 1.5), 1),
        "latency_ms": round(random.lognormvariate(5, 0.8), 1),
        "model": random.choice(models),
    })

t0 = time.perf_counter()
table.append_rows(new_rows)
table.tick()  # Only processes the 500 new rows, not all 6,500
tick_time = (time.perf_counter() - t0) * 1000

print(f"Added 500 rows and tick()'d all views in {tick_time:.2f}ms")
print(f"Monthly view now has {len(monthly)} groups (was 12, now includes 2026-01)")

# Show the new month
for i in range(len(monthly)):
    r = monthly.get_row(i)
    if r["month"] == "2026-01":
        print(f"  2026-01 median tokens: {r['median']:,.0f}")

# --- 6. Mix percentiles with other aggregations ---

print("\n--- Combined Stats (last example) ---")

combined = table.group_by("model", agg=[
    ("total_tokens", "tokens_used", "sum"),
    ("avg_tokens", "tokens_used", "avg"),
    ("median_tokens", "tokens_used", "median"),
    ("p95_tokens", "tokens_used", "p95"),
    ("requests", "tokens_used", "count"),
])

print(f"{'Model':<10} {'Requests':>10} {'Total':>14} {'Avg':>10} {'Median':>10} {'P95':>10}")
print("-" * 66)
for i in range(len(combined)):
    r = combined.get_row(i)
    print(f"{r['model']:<10} {r['requests']:>10,} {r['total_tokens']:>14,.0f} {r['avg_tokens']:>10,.0f} {r['median_tokens']:>10,.0f} {r['p95_tokens']:>10,.0f}")

print("\nDone.")
