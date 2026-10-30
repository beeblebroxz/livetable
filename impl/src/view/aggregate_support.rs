//! Aggregate-support types — was previously spliced into view.rs via
//! `include!()`. Promoted to a real submodule so the file participates in
//! Rust's normal module/privacy boundary. All types and methods used by
//! `view.rs::AggregateView` are exposed as `pub(super)`; the rest stays
//! private to this submodule.

use super::{canonical_f32_bits, canonical_f64_bits, JoinKeyPart};
use crate::column::ColumnValue;
use crate::readable::ReadableTable;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

/// Supported aggregation functions
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Percentile(f64), // p value in 0.0..=1.0
    Median,          // Sugar for Percentile(0.5)
}

/// An f64 ordered by `total_cmp`, so it can key a `BTreeMap`. NaN never
/// enters: `extract_numeric` excludes it. -0.0 orders below 0.0.
#[derive(Debug, Clone, Copy)]
struct OrderedF64(f64);

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Internal state for tracking aggregate statistics for one source column
#[derive(Debug, Clone)]
pub(super) struct ColumnAggState {
    /// Running sum for SUM and AVG calculations
    pub(super) sum: f64,
    /// Count of non-null values
    pub(super) count: usize,
    /// Every value with its multiplicity, so MIN/MAX are the first and last
    /// keys and removing an extreme never rescans the source. Only populated
    /// when a MIN or MAX aggregation targets this source column.
    extremes: Option<BTreeMap<OrderedF64, usize>>,
    /// Sorted values for percentile calculations. Only populated when
    /// a Percentile or Median aggregation targets this source column.
    pub(super) sorted_values: Option<Vec<f64>>,
}

impl ColumnAggState {
    pub(super) fn new(needs_sorted: bool, needs_extremes: bool) -> Self {
        ColumnAggState {
            sum: 0.0,
            count: 0,
            extremes: needs_extremes.then(BTreeMap::new),
            sorted_values: if needs_sorted { Some(Vec::new()) } else { None },
        }
    }

    /// Add a numeric value to the aggregate state
    pub(super) fn add_value(&mut self, value: f64) {
        self.add_to_totals(value);
        if let Some(ref mut sorted) = self.sorted_values {
            // Total order, so -0.0 and 0.0 sort apart and removal finds the
            // exact value an incremental sync added.
            let pos = sorted.partition_point(|v| v.total_cmp(&value).is_lt());
            sorted.insert(pos, value);
        }
    }

    /// Like `add_value`, but appends to the percentile values without
    /// keeping them sorted. A build adds every value this way and then calls
    /// `sort_values` once, instead of paying an insert per value.
    pub(super) fn add_value_unsorted(&mut self, value: f64) {
        self.add_to_totals(value);
        if let Some(ref mut sorted) = self.sorted_values {
            sorted.push(value);
        }
    }

    /// The sum, count and MIN/MAX multiset, which both adds update.
    fn add_to_totals(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        if let Some(extremes) = &mut self.extremes {
            *extremes.entry(OrderedF64(value)).or_insert(0) += 1;
        }
    }

    /// Sort the percentile values in the order `add_value` keeps them.
    pub(super) fn sort_values(&mut self) {
        if let Some(ref mut sorted) = self.sorted_values {
            sorted.sort_unstable_by(f64::total_cmp);
        }
    }

    /// Remove a numeric value from the aggregate state. Removed values come
    /// from changeset payloads, so they are always present.
    pub(super) fn remove_value(&mut self, value: f64) {
        self.sum -= value;
        self.count = self.count.saturating_sub(1);

        if let Some(ref mut sorted) = self.sorted_values {
            let pos = sorted.partition_point(|v| v.total_cmp(&value).is_lt());
            let present = pos < sorted.len() && sorted[pos].to_bits() == value.to_bits();
            debug_assert!(present, "removed value {value} missing from sorted_values");
            if present {
                sorted.remove(pos);
            }
        }

        if let Some(extremes) = &mut self.extremes {
            let key = OrderedF64(value);
            match extremes.get_mut(&key) {
                Some(1) => {
                    extremes.remove(&key);
                }
                Some(count) => *count -= 1,
                None => debug_assert!(false, "removed value {value} missing from extremes"),
            }
        }
    }

    /// Compute percentile using linear interpolation (PERCENTILE_CONT semantics).
    /// p must be in 0.0..=1.0. Returns None if no values.
    pub(super) fn percentile(&self, p: f64) -> Option<f64> {
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

    fn extreme(key: Option<&OrderedF64>) -> ColumnValue {
        key.map_or(ColumnValue::Null, |value| ColumnValue::Float64(value.0))
    }

    pub(super) fn get_result(&self, func: AggregateFunction) -> ColumnValue {
        match func {
            // SQL semantics: SUM over zero non-null values is NULL, not 0. This
            // also makes a state that was emptied by incremental removal agree
            // with a from-scratch rebuild (which has no per-column state at all).
            AggregateFunction::Sum if self.count == 0 => ColumnValue::Null,
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
                Self::extreme(self.extremes.as_ref().and_then(|e| e.keys().next()))
            }
            AggregateFunction::Max => {
                Self::extreme(self.extremes.as_ref().and_then(|e| e.keys().next_back()))
            }
            AggregateFunction::Percentile(p) => self
                .percentile(p)
                .map_or(ColumnValue::Null, ColumnValue::Float64),
            AggregateFunction::Median => self
                .percentile(0.5)
                .map_or(ColumnValue::Null, ColumnValue::Float64),
        }
    }
}

/// Internal state for tracking aggregates per group
#[derive(Debug, Clone)]
pub(super) struct GroupState {
    /// Per-source-column aggregate statistics
    column_stats: HashMap<String, ColumnAggState>,
    /// Number of parent rows in this group; the group is removed at zero.
    pub(super) row_count: usize,
    /// This group's key, shared by the view's position-indexed row map.
    pub(super) key: Rc<GroupKey>,
    /// Source columns that need sorted_values for percentile calculations
    pub(super) percentile_columns: HashSet<String>,
    /// Source columns that need a value multiset for MIN/MAX
    pub(super) extreme_columns: HashSet<String>,
    /// This group's index in the view's group order (its output row).
    pub(super) position: usize,
}

impl GroupState {
    pub(super) fn new(
        position: usize,
        percentile_columns: HashSet<String>,
        extreme_columns: HashSet<String>,
        key: Rc<GroupKey>,
    ) -> Self {
        GroupState {
            column_stats: HashMap::new(),
            row_count: 0,
            key,
            percentile_columns,
            extreme_columns,
            position,
        }
    }

    /// Add a value for a specific source column
    pub(super) fn add_column_value(&mut self, source_col: &str, value: f64) {
        self.column_state(source_col).add_value(value);
    }

    /// Like `add_column_value`, for a build that calls `finish_build` after
    /// adding every value.
    pub(super) fn add_column_value_unsorted(&mut self, source_col: &str, value: f64) {
        self.column_state(source_col).add_value_unsorted(value);
    }

    /// The state for `source_col`, created on its first value.
    fn column_state(&mut self, source_col: &str) -> &mut ColumnAggState {
        let needs_sorted = self.percentile_columns.contains(source_col);
        let needs_extremes = self.extreme_columns.contains(source_col);
        self.column_stats
            .entry(source_col.to_string())
            .or_insert_with(|| ColumnAggState::new(needs_sorted, needs_extremes))
    }

    /// Sort the percentile values a build added unsorted.
    pub(super) fn finish_build(&mut self) {
        for stats in self.column_stats.values_mut() {
            stats.sort_values();
        }
    }

    /// Remove a value for a specific source column
    pub(super) fn remove_column_value(&mut self, source_col: &str, value: f64) {
        if let Some(stats) = self.column_stats.get_mut(source_col) {
            stats.remove_value(value);
        } else {
            debug_assert!(
                false,
                "removed value {value} from {source_col}, which has no state"
            );
        }
    }

    /// Get result for a specific aggregation (source column + function)
    pub(super) fn get_result(&self, source_col: &str, func: AggregateFunction) -> ColumnValue {
        if let Some(stats) = self.column_stats.get(source_col) {
            stats.get_result(func)
        } else {
            // The group exists but has no non-null values for this column
            // (a from-scratch rebuild never creates per-column state for a
            // column with no numeric values). COUNT of nothing is 0; every
            // other aggregate is NULL. This mirrors the emptied incremental
            // state so both code paths produce identical output.
            match func {
                AggregateFunction::Count => ColumnValue::Int64(0),
                _ => ColumnValue::Null,
            }
        }
    }
}

/// A key for grouping rows — one typed part per group-by column, `None` = NULL.
///
/// Uses the same typed `JoinKeyPart` representation as joins. Both fold -0.0
/// into +0.0; they differ only on NaN: GROUP BY folds all NaNs into a single
/// group (Postgres-style), whereas joins exclude NaN keys entirely.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct GroupKey(Vec<Option<JoinKeyPart>>);

/// Convert a group-by column value into a typed key part (None = NULL group).
fn group_key_part(value: ColumnValue) -> Option<JoinKeyPart> {
    match value {
        ColumnValue::Float32(v) => Some(JoinKeyPart::Float32Bits(canonical_f32_bits(v))),
        ColumnValue::Float64(v) => Some(JoinKeyPart::Float64Bits(canonical_f64_bits(v))),
        other => super::column_value_into_join_key_part(other),
    }
}

impl GroupKey {
    pub(super) fn from_row(row: &HashMap<String, ColumnValue>, group_by: &[String]) -> Self {
        GroupKey(
            group_by
                .iter()
                .map(|col| row.get(col).cloned().and_then(group_key_part))
                .collect(),
        )
    }

    /// Build GroupKey directly from table using column indices (faster than from_row)
    pub(super) fn from_indices(
        table: &dyn ReadableTable,
        row_idx: usize,
        col_indices: &[usize],
    ) -> Self {
        GroupKey(
            col_indices
                .iter()
                .map(|&col_idx| {
                    table
                        .get_value_by_index(row_idx, col_idx)
                        .ok()
                        .and_then(group_key_part)
                })
                .collect(),
        )
    }

    #[inline]
    pub(super) fn from_single_int(value: Option<i32>) -> Self {
        GroupKey(vec![value.map(JoinKeyPart::Int32)])
    }

    pub(super) fn to_column_values(&self, group_by: &[String]) -> HashMap<String, ColumnValue> {
        group_by
            .iter()
            .zip(&self.0)
            .map(|(col_name, part)| {
                let value = match part {
                    None => ColumnValue::Null,
                    Some(JoinKeyPart::Int32(v)) => ColumnValue::Int32(*v),
                    Some(JoinKeyPart::Int64(v)) => ColumnValue::Int64(*v),
                    Some(JoinKeyPart::Float32Bits(b)) => ColumnValue::Float32(f32::from_bits(*b)),
                    Some(JoinKeyPart::Float64Bits(b)) => ColumnValue::Float64(f64::from_bits(*b)),
                    Some(JoinKeyPart::String(s)) => ColumnValue::String(s.clone()),
                    Some(JoinKeyPart::Bool(b)) => ColumnValue::Bool(*b),
                    Some(JoinKeyPart::Date(d)) => ColumnValue::Date(*d),
                    Some(JoinKeyPart::DateTime(dt)) => ColumnValue::DateTime(*dt),
                };
                (col_name.clone(), value)
            })
            .collect()
    }
}
