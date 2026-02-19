# Percentile/Quantile Aggregation — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add exact Percentile(p) and Median aggregation functions to LiveTable's AggregateView, with incremental update support and Python bindings.

**Architecture:** Extend `AggregateFunction` enum with `Percentile(f64)` and `Median` variants. Add an optional `sorted_values: Vec<f64>` to `ColumnAggState` that is only populated when percentile-type aggregations target that column. Percentile uses linear interpolation (PERCENTILE_CONT). The sorted vec is maintained incrementally via binary-search insert/remove.

**Tech Stack:** Rust (view.rs core), PyO3 (python_bindings.rs), pytest (Python tests)

---

### Task 1: Extend AggregateFunction enum and ColumnAggState

**Files:**
- Modify: `impl/src/view.rs:1346-1352` (AggregateFunction enum)
- Modify: `impl/src/view.rs:1356-1365` (ColumnAggState struct)
- Modify: `impl/src/view.rs:1367-1428` (ColumnAggState impl)

**Step 1: Write a Rust unit test for percentile computation**

Add to the `#[cfg(test)] mod tests` block at `impl/src/view.rs:2262`:

```rust
#[test]
fn test_column_agg_state_percentile() {
    let mut state = ColumnAggState::new(true); // needs_sorted = true
    // Values: 10, 20, 30, 40, 50
    for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
        state.add_value(v);
    }

    // Median (P50) of [10,20,30,40,50] = 30.0
    let median = state.percentile(0.5).unwrap();
    assert!((median - 30.0).abs() < 1e-9);

    // P0 = 10.0 (minimum)
    let p0 = state.percentile(0.0).unwrap();
    assert!((p0 - 10.0).abs() < 1e-9);

    // P100 = 50.0 (maximum)
    let p100 = state.percentile(1.0).unwrap();
    assert!((p100 - 50.0).abs() < 1e-9);

    // P25 = 20.0
    let p25 = state.percentile(0.25).unwrap();
    assert!((p25 - 20.0).abs() < 1e-9);

    // P75 = 40.0
    let p75 = state.percentile(0.75).unwrap();
    assert!((p75 - 40.0).abs() < 1e-9);
}

#[test]
fn test_column_agg_state_percentile_interpolation() {
    let mut state = ColumnAggState::new(true);
    // Even number of values: 10, 20, 30, 40
    for v in [10.0, 20.0, 30.0, 40.0] {
        state.add_value(v);
    }
    // Median of [10,20,30,40] = interpolation at index 1.5 = 25.0
    let median = state.percentile(0.5).unwrap();
    assert!((median - 25.0).abs() < 1e-9);
}

#[test]
fn test_column_agg_state_percentile_single_value() {
    let mut state = ColumnAggState::new(true);
    state.add_value(42.0);
    // Any percentile of a single value = that value
    assert!((state.percentile(0.0).unwrap() - 42.0).abs() < 1e-9);
    assert!((state.percentile(0.5).unwrap() - 42.0).abs() < 1e-9);
    assert!((state.percentile(1.0).unwrap() - 42.0).abs() < 1e-9);
}

#[test]
fn test_column_agg_state_percentile_empty() {
    let state = ColumnAggState::new(true);
    assert!(state.percentile(0.5).is_none());
}

#[test]
fn test_column_agg_state_no_sorted_when_not_needed() {
    let mut state = ColumnAggState::new(false); // needs_sorted = false
    state.add_value(10.0);
    assert!(state.sorted_values.is_none());
    assert!(state.percentile(0.5).is_none());
}
```

**Step 2: Run test to verify it fails**

Run: `cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib test_column_agg_state_percentile -- --nocapture`
Expected: Compilation error — `ColumnAggState::new` doesn't accept arguments yet, `percentile` method doesn't exist, `sorted_values` field doesn't exist.

**Step 3: Extend the AggregateFunction enum**

In `impl/src/view.rs:1346-1352`, change:

```rust
pub enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Percentile(f64),  // p value in 0.0..=1.0
    Median,           // Sugar for Percentile(0.5)
}
```

Note: `AggregateFunction` currently derives `Copy`. Since `f64` is `Copy`, `Percentile(f64)` is still `Copy`-safe.

**Step 4: Add sorted_values to ColumnAggState**

In `impl/src/view.rs:1356-1365`, change struct to:

```rust
struct ColumnAggState {
    sum: f64,
    count: usize,
    min: Option<f64>,
    max: Option<f64>,
    /// Sorted values for percentile calculations. Only populated when
    /// a Percentile or Median aggregation targets this source column.
    sorted_values: Option<Vec<f64>>,
}
```

**Step 5: Update ColumnAggState::new to accept needs_sorted flag**

Change `impl/src/view.rs:1367-1375`:

```rust
impl ColumnAggState {
    fn new(needs_sorted: bool) -> Self {
        ColumnAggState {
            sum: 0.0,
            count: 0,
            min: None,
            max: None,
            sorted_values: if needs_sorted { Some(Vec::new()) } else { None },
        }
    }
```

**Step 6: Update add_value to maintain sorted_values**

In `impl/src/view.rs:1378-1383`, extend:

```rust
    fn add_value(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
        if let Some(ref mut sorted) = self.sorted_values {
            let pos = sorted.partition_point(|&v| v < value);
            sorted.insert(pos, value);
        }
    }
```

**Step 7: Update remove_value to maintain sorted_values**

In `impl/src/view.rs:1387-1396`, extend:

```rust
    fn remove_value(&mut self, value: f64) -> bool {
        self.sum -= value;
        self.count = self.count.saturating_sub(1);

        if let Some(ref mut sorted) = self.sorted_values {
            // Find and remove the value (binary search for position)
            let pos = sorted.partition_point(|&v| v < value);
            if pos < sorted.len() && (sorted[pos] - value).abs() < f64::EPSILON {
                sorted.remove(pos);
            }
        }

        // Check if we need to recalculate min/max
        let needs_recalc = self.min.map_or(false, |m| (m - value).abs() < f64::EPSILON)
            || self.max.map_or(false, |m| (m - value).abs() < f64::EPSILON);

        !needs_recalc
    }
```

**Step 8: Update recalculate_min_max to also rebuild sorted_values**

In `impl/src/view.rs:1399-1407`, extend:

```rust
    fn recalculate_min_max(&mut self, values: &[f64]) {
        if values.is_empty() {
            self.min = None;
            self.max = None;
        } else {
            self.min = values.iter().copied().reduce(f64::min);
            self.max = values.iter().copied().reduce(f64::max);
        }
        // Rebuild sorted_values if tracking percentiles
        if self.sorted_values.is_some() {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            self.sorted_values = Some(sorted);
        }
    }
```

**Step 9: Add percentile() method to ColumnAggState**

Add after `recalculate_min_max`, before `get_result`:

```rust
    /// Compute percentile using linear interpolation (PERCENTILE_CONT semantics).
    /// p must be in 0.0..=1.0. Returns None if no values.
    fn percentile(&self, p: f64) -> Option<f64> {
        let sorted = self.sorted_values.as_ref()?;
        if sorted.is_empty() {
            return None;
        }
        if sorted.len() == 1 {
            return Some(sorted[0]);
        }
        let idx = p * (sorted.len() - 1) as f64;
        let lo = idx.floor() as usize;
        let hi = lo + 1;
        if hi >= sorted.len() {
            return Some(sorted[lo]);
        }
        let frac = idx - lo as f64;
        Some(sorted[lo] * (1.0 - frac) + sorted[hi] * frac)
    }
```

**Step 10: Extend get_result to handle Percentile and Median**

In `impl/src/view.rs:1409-1427`, add match arms:

```rust
    fn get_result(&self, func: AggregateFunction) -> ColumnValue {
        match func {
            AggregateFunction::Sum => ColumnValue::Float64(self.sum),
            AggregateFunction::Count => ColumnValue::Int64(self.count as i64),
            AggregateFunction::Avg => {
                if self.count > 0 {
                    ColumnValue::Float64(self.sum / self.count as f64)
                } else {
                    ColumnValue::Null
                }
            }
            AggregateFunction::Min => {
                self.min.map_or(ColumnValue::Null, ColumnValue::Float64)
            }
            AggregateFunction::Max => {
                self.max.map_or(ColumnValue::Null, ColumnValue::Float64)
            }
            AggregateFunction::Percentile(p) => {
                self.percentile(p).map_or(ColumnValue::Null, ColumnValue::Float64)
            }
            AggregateFunction::Median => {
                self.percentile(0.5).map_or(ColumnValue::Null, ColumnValue::Float64)
            }
        }
    }
```

**Step 11: Update GroupState to pass needs_sorted when creating ColumnAggState**

The `GroupState::add_column_value` method (line 1448) creates `ColumnAggState` via `or_insert_with(ColumnAggState::new)`. We need it to know whether sorted values are needed. Add a `needs_sorted_columns` set to `GroupState`:

```rust
struct GroupState {
    column_stats: HashMap<String, ColumnAggState>,
    row_indices: HashSet<usize>,
    /// Source columns that need sorted_values for percentile calculations
    percentile_columns: HashSet<String>,
}

impl GroupState {
    fn new() -> Self {
        GroupState {
            column_stats: HashMap::new(),
            row_indices: HashSet::new(),
            percentile_columns: HashSet::new(),
        }
    }

    fn add_column_value(&mut self, source_col: &str, value: f64) {
        let needs_sorted = self.percentile_columns.contains(source_col);
        let stats = self.column_stats
            .entry(source_col.to_string())
            .or_insert_with(|| ColumnAggState::new(needs_sorted));
        stats.add_value(value);
    }
    // ... rest unchanged
}
```

**Step 12: Update AggregateView to populate percentile_columns on GroupState**

In `AggregateView`, compute the set of source columns that need percentiles. This set is derived from `self.aggregations` once and reused:

Add a helper method to `AggregateView`:

```rust
    /// Get source columns that need sorted_values for percentile/median
    fn percentile_source_columns(&self) -> HashSet<String> {
        self.aggregations.iter()
            .filter(|(_, _, func)| matches!(func, AggregateFunction::Percentile(_) | AggregateFunction::Median))
            .map(|(_, source_col, _)| source_col.clone())
            .collect()
    }
```

Then in `rebuild_index`, after creating each `GroupState::new()`, set its `percentile_columns`:

```rust
let pct_cols = self.percentile_source_columns();
// ... then when creating groups:
let state = ... .or_insert_with(|| {
    let mut gs = GroupState::new();
    gs.percentile_columns = pct_cols.clone();
    gs
});
```

Apply the same pattern in `add_row_to_aggregates`.

**Step 13: Run tests to verify they pass**

Run: `cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib test_column_agg_state -- --nocapture`
Expected: All 5 new tests pass.

**Step 14: Run all existing Rust tests to verify no regressions**

Run: `cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib`
Expected: All existing tests still pass.

**Step 15: Commit**

```
feat: add Percentile and Median to AggregateFunction enum

Extends ColumnAggState with optional sorted_values vec for exact
percentile computation using linear interpolation (PERCENTILE_CONT).
Only allocates sorted buffer when percentile aggregations target
the column.
```

---

### Task 2: Add Rust-level AggregateView integration test

**Files:**
- Modify: `impl/src/view.rs` (test module at line 2262)

**Step 1: Write AggregateView test with Percentile**

Add to `#[cfg(test)] mod tests`:

```rust
#[test]
fn test_aggregate_view_percentile() {
    let schema = Schema::new(vec![
        ("region".to_string(), ColumnType::String, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));
    {
        let mut t = table.borrow_mut();
        // North: 10, 20, 30, 40, 50  (median=30, p90=46)
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            let mut row = HashMap::new();
            row.insert("region".to_string(), ColumnValue::String("North".to_string()));
            row.insert("amount".to_string(), ColumnValue::Float64(v));
            t.append_row(row).unwrap();
        }
        // South: 100, 200  (median=150)
        for v in [100.0, 200.0] {
            let mut row = HashMap::new();
            row.insert("region".to_string(), ColumnValue::String("South".to_string()));
            row.insert("amount".to_string(), ColumnValue::Float64(v));
            t.append_row(row).unwrap();
        }
    }

    let agg = AggregateView::new(
        "by_region".to_string(),
        table.clone(),
        vec!["region".to_string()],
        vec![
            ("median_amount".to_string(), "amount".to_string(), AggregateFunction::Median),
            ("p90_amount".to_string(), "amount".to_string(), AggregateFunction::Percentile(0.9)),
        ],
    ).unwrap();

    assert_eq!(agg.len(), 2);

    for i in 0..agg.len() {
        let row = agg.get_row(i).unwrap();
        match row.get("region").unwrap() {
            ColumnValue::String(s) if s == "North" => {
                let median = match row.get("median_amount").unwrap() {
                    ColumnValue::Float64(v) => *v,
                    _ => panic!("Expected Float64"),
                };
                assert!((median - 30.0).abs() < 1e-9);
            }
            ColumnValue::String(s) if s == "South" => {
                let median = match row.get("median_amount").unwrap() {
                    ColumnValue::Float64(v) => *v,
                    _ => panic!("Expected Float64"),
                };
                assert!((median - 150.0).abs() < 1e-9);
            }
            _ => panic!("Unexpected region"),
        }
    }
}

#[test]
fn test_aggregate_view_percentile_incremental() {
    let schema = Schema::new(vec![
        ("group".to_string(), ColumnType::String, false),
        ("val".to_string(), ColumnType::Float64, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));
    {
        let mut t = table.borrow_mut();
        for v in [10.0, 20.0, 30.0] {
            let mut row = HashMap::new();
            row.insert("group".to_string(), ColumnValue::String("A".to_string()));
            row.insert("val".to_string(), ColumnValue::Float64(v));
            t.append_row(row).unwrap();
        }
    }

    let mut agg = AggregateView::new(
        "test_agg".to_string(),
        table.clone(),
        vec!["group".to_string()],
        vec![("median_val".to_string(), "val".to_string(), AggregateFunction::Median)],
    ).unwrap();

    // Median of [10, 20, 30] = 20.0
    let row = agg.get_row(0).unwrap();
    let median = match row.get("median_val").unwrap() {
        ColumnValue::Float64(v) => *v,
        _ => panic!("Expected Float64"),
    };
    assert!((median - 20.0).abs() < 1e-9);

    // Add a value and sync
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("group".to_string(), ColumnValue::String("A".to_string()));
        row.insert("val".to_string(), ColumnValue::Float64(40.0));
        t.append_row(row).unwrap();
    }
    agg.sync();

    // Median of [10, 20, 30, 40] = 25.0
    let row = agg.get_row(0).unwrap();
    let median = match row.get("median_val").unwrap() {
        ColumnValue::Float64(v) => *v,
        _ => panic!("Expected Float64"),
    };
    assert!((median - 25.0).abs() < 1e-9);
}
```

**Step 2: Run tests**

Run: `cd impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib test_aggregate_view_percentile -- --nocapture`
Expected: Both tests pass.

**Step 3: Commit**

```
test: add Rust unit tests for percentile aggregation
```

---

### Task 3: Python bindings — PyAggregateFunction + group_by string parsing

**Files:**
- Modify: `impl/src/python_bindings.rs:2383-2437` (PyAggregateFunction)
- Modify: `impl/src/python_bindings.rs:1257-1270` (group_by string parsing)

**Step 1: Write Python test for percentile via explicit API**

Create: `tests/python/test_percentile.py`

```python
#!/usr/bin/env python3
"""Tests for Percentile and Median aggregation functions."""

import pytest
import livetable


@pytest.fixture
def scores_table():
    """Table with numeric data for percentile testing."""
    schema = livetable.Schema([
        ("group", livetable.ColumnType.STRING, False),
        ("score", livetable.ColumnType.FLOAT64, False),
    ])
    table = livetable.Table("scores", schema)
    # Group A: 10, 20, 30, 40, 50
    for v in [10.0, 20.0, 30.0, 40.0, 50.0]:
        table.append_row({"group": "A", "score": v})
    # Group B: 100, 200
    for v in [100.0, 200.0]:
        table.append_row({"group": "B", "score": v})
    return table


class TestPercentileExplicitAPI:
    """Test Percentile and Median via explicit AggregateFunction constructors."""

    def test_median_enum_exists(self):
        assert livetable.AggregateFunction.MEDIAN is not None

    def test_percentile_constructor(self):
        p95 = livetable.AggregateFunction.PERCENTILE(0.95)
        assert p95 is not None

    def test_percentile_invalid_value(self):
        with pytest.raises(Exception):
            livetable.AggregateFunction.PERCENTILE(1.5)
        with pytest.raises(Exception):
            livetable.AggregateFunction.PERCENTILE(-0.1)

    def test_aggregate_view_median(self, scores_table):
        agg = livetable.AggregateView(
            "by_group", scores_table, ["group"],
            [("median_score", "score", livetable.AggregateFunction.MEDIAN)],
        )
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["median_score"]

        assert abs(results["A"] - 30.0) < 0.01   # median of [10,20,30,40,50]
        assert abs(results["B"] - 150.0) < 0.01   # median of [100,200]

    def test_aggregate_view_percentile(self, scores_table):
        agg = livetable.AggregateView(
            "by_group", scores_table, ["group"],
            [("p25", "score", livetable.AggregateFunction.PERCENTILE(0.25))],
        )
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["p25"]

        assert abs(results["A"] - 20.0) < 0.01  # P25 of [10,20,30,40,50]


class TestPercentileSimplifiedAPI:
    """Test Percentile and Median via group_by string shorthands."""

    def test_median_string(self, scores_table):
        agg = scores_table.group_by("group", agg=[
            ("med", "score", "median"),
        ])
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["med"]
        assert abs(results["A"] - 30.0) < 0.01

    def test_p95_shorthand(self, scores_table):
        agg = scores_table.group_by("group", agg=[
            ("p95_score", "score", "p95"),
        ])
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["p95_score"]
        # P95 of [10,20,30,40,50]: idx=0.95*4=3.8, lerp(40,50,0.8) = 48.0
        assert abs(results["A"] - 48.0) < 0.01

    def test_all_shorthand_names(self, scores_table):
        """All shorthand names parse without error."""
        for name in ["p25", "p50", "p75", "p90", "p95", "p99", "median"]:
            agg = scores_table.group_by("group", agg=[
                ("result", "score", name),
            ])
            assert len(agg) == 2

    def test_percentile_explicit_string(self, scores_table):
        agg = scores_table.group_by("group", agg=[
            ("p10", "score", "percentile(0.1)"),
        ])
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["p10"]
        # P10 of [10,20,30,40,50]: idx=0.1*4=0.4, lerp(10,20,0.4) = 14.0
        assert abs(results["A"] - 14.0) < 0.01

    def test_invalid_percentile_string(self, scores_table):
        with pytest.raises(Exception):
            scores_table.group_by("group", agg=[
                ("bad", "score", "percentile(2.0)"),
            ])


class TestPercentileIncremental:
    """Test incremental updates with percentile aggregations."""

    def test_tick_updates_percentile(self):
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)
        for v in [10.0, 20.0, 30.0]:
            table.append_row({"grp": "A", "val": v})

        agg = table.group_by("grp", agg=[("med", "val", "median")])

        # Median of [10,20,30] = 20.0
        row = agg.get_row(0)
        assert abs(row["med"] - 20.0) < 0.01

        # Add value and tick
        table.append_row({"grp": "A", "val": 40.0})
        table.tick()

        # Median of [10,20,30,40] = 25.0
        row = agg.get_row(0)
        assert abs(row["med"] - 25.0) < 0.01

    def test_sync_updates_percentile(self):
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)
        for v in [10.0, 20.0, 30.0]:
            table.append_row({"grp": "A", "val": v})

        agg = livetable.AggregateView(
            "test", table, ["grp"],
            [("med", "val", livetable.AggregateFunction.MEDIAN)],
        )

        # Add and sync
        table.append_row({"grp": "A", "val": 40.0})
        agg.sync()

        row = agg.get_row(0)
        assert abs(row["med"] - 25.0) < 0.01


class TestPercentileEdgeCases:
    """Edge cases for percentile computations."""

    def test_single_row_group(self):
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)
        table.append_row({"grp": "A", "val": 42.0})

        agg = table.group_by("grp", agg=[("med", "val", "median")])
        row = agg.get_row(0)
        assert abs(row["med"] - 42.0) < 0.01

    def test_empty_table(self):
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)

        agg = table.group_by("grp", agg=[("med", "val", "median")])
        assert len(agg) == 0

    def test_mixed_aggregations(self):
        """Percentile works alongside SUM, AVG, etc."""
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)
        for v in [10.0, 20.0, 30.0]:
            table.append_row({"grp": "A", "val": v})

        agg = table.group_by("grp", agg=[
            ("total", "val", "sum"),
            ("med", "val", "median"),
            ("p95", "val", "p95"),
            ("avg_val", "val", "avg"),
        ])

        row = agg.get_row(0)
        assert abs(row["total"] - 60.0) < 0.01
        assert abs(row["med"] - 20.0) < 0.01
        assert abs(row["avg_val"] - 20.0) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

**Step 2: Run test to verify it fails**

Run: `cd tests && pytest python/test_percentile.py -v`
Expected: Failures — `PERCENTILE` and `MEDIAN` not found on `AggregateFunction`, string parsing rejects "median"/"p95".

**Step 3: Extend PyAggregateFunction with PERCENTILE and MEDIAN**

In `impl/src/python_bindings.rs`, in the `#[pymethods] impl PyAggregateFunction` block (around line 2391):

Add after the existing `MAX()` classattr:

```rust
    /// Median value (equivalent to PERCENTILE(0.5))
    #[classattr]
    fn MEDIAN() -> Self {
        PyAggregateFunction { inner: RustAggregateFunction::Median }
    }

    /// Percentile value. p must be between 0.0 and 1.0 inclusive.
    #[staticmethod]
    fn PERCENTILE(p: f64) -> PyResult<Self> {
        if !(0.0..=1.0).contains(&p) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Percentile value must be between 0.0 and 1.0, got {}", p)
            ));
        }
        Ok(PyAggregateFunction { inner: RustAggregateFunction::Percentile(p) })
    }
```

Update `__repr__` (around line 2422) to add:

```rust
    fn __repr__(&self) -> String {
        match self.inner {
            RustAggregateFunction::Sum => "AggregateFunction.SUM".to_string(),
            RustAggregateFunction::Count => "AggregateFunction.COUNT".to_string(),
            RustAggregateFunction::Avg => "AggregateFunction.AVG".to_string(),
            RustAggregateFunction::Min => "AggregateFunction.MIN".to_string(),
            RustAggregateFunction::Max => "AggregateFunction.MAX".to_string(),
            RustAggregateFunction::Median => "AggregateFunction.MEDIAN".to_string(),
            RustAggregateFunction::Percentile(p) => format!("AggregateFunction.PERCENTILE({})", p),
        }
    }
```

**Step 4: Extend group_by string parsing**

In `impl/src/python_bindings.rs`, the `group_by` method (around line 1260), extend the match:

```rust
                let func = match func_str.to_lowercase().as_str() {
                    "sum" => RustAggregateFunction::Sum,
                    "avg" | "average" | "mean" => RustAggregateFunction::Avg,
                    "min" | "minimum" => RustAggregateFunction::Min,
                    "max" | "maximum" => RustAggregateFunction::Max,
                    "count" => RustAggregateFunction::Count,
                    "median" | "med" => RustAggregateFunction::Median,
                    "p25" => RustAggregateFunction::Percentile(0.25),
                    "p50" => RustAggregateFunction::Percentile(0.50),
                    "p75" => RustAggregateFunction::Percentile(0.75),
                    "p90" => RustAggregateFunction::Percentile(0.90),
                    "p95" => RustAggregateFunction::Percentile(0.95),
                    "p99" => RustAggregateFunction::Percentile(0.99),
                    other => {
                        // Try to parse "percentile(X.XX)" format
                        if let Some(inner) = other.strip_prefix("percentile(")
                            .and_then(|s| s.strip_suffix(")"))
                        {
                            let p: f64 = inner.parse().map_err(|_| PyValueError::new_err(
                                format!("Invalid percentile value '{}' in '{}'", inner, func_str)
                            ))?;
                            if !(0.0..=1.0).contains(&p) {
                                return Err(PyValueError::new_err(
                                    format!("Percentile value must be between 0.0 and 1.0, got {}", p)
                                ));
                            }
                            RustAggregateFunction::Percentile(p)
                        } else {
                            return Err(PyValueError::new_err(format!(
                                "Unknown aggregation function '{}'. Use: sum, avg, min, max, count, median, p25, p50, p75, p90, p95, p99, or percentile(0.XX)",
                                func_str
                            )));
                        }
                    }
                };
```

**Step 5: Build the Python package**

Run: `cd impl && ./install.sh`
Expected: Build succeeds.

**Step 6: Run the Python tests**

Run: `cd tests && pytest python/test_percentile.py -v`
Expected: All tests pass.

**Step 7: Run all existing Python tests to check for regressions**

Run: `cd tests && pytest python/ -v`
Expected: All tests pass.

**Step 8: Commit**

```
feat: add Python bindings for Percentile and Median aggregations

Adds AggregateFunction.PERCENTILE(p) staticmethod and MEDIAN classattr.
Extends group_by() string parsing with shorthands: median, p25, p50,
p75, p90, p95, p99, and percentile(X.XX) format.
```

---

### Task 4: Run full test suite and update documentation

**Files:**
- Modify: `CLAUDE.md` (Python API Usage section)
- Modify: `docs/PYTHON_BINDINGS_README.md` (if exists, add percentile docs)
- Modify: `README.md` (Features list)
- Modify: `docs/ORIGINAL_VISION.md` (mark percentile as implemented)

**Step 1: Run the full test suite**

Run: `cd tests && ./run_all.sh`
Expected: All Rust + Python + Integration tests pass.

**Step 2: Update CLAUDE.md Python API section**

Add after the GROUP BY examples in the Python API Usage section:

```python
# Percentile / Median aggregations
stats = table.group_by("month", agg=[
    ("median_tokens", "tokens_used", "median"),
    ("p95_tokens", "tokens_used", "p95"),
    ("p99_latency", "latency_ms", "p99"),
    ("p25_tokens", "tokens_used", "percentile(0.25)"),  # Explicit percentile
])
# Shorthand strings: median, p25, p50, p75, p90, p95, p99, percentile(X.XX)

# Explicit AggregateFunction constructors
agg = livetable.AggregateView("stats", table, ["month"], [
    ("p95", "tokens_used", livetable.AggregateFunction.PERCENTILE(0.95)),
    ("median", "tokens_used", livetable.AggregateFunction.MEDIAN),
])
```

**Step 3: Update README.md features list**

Add "Percentile/Median aggregations (P25, P50, P75, P90, P95, P99)" to the features section.

**Step 4: Update docs/ORIGINAL_VISION.md implementation status**

Mark percentile aggregations as implemented.

**Step 5: Commit**

```
docs: add percentile/median aggregation to documentation
```
