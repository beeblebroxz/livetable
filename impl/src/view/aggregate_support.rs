//! Aggregate-support types — was previously spliced into view.rs via
//! `include!()`. Promoted to a real submodule so the file participates in
//! Rust's normal module/privacy boundary. All types and methods used by
//! `view.rs::AggregateView` are exposed as `pub(super)`; the rest stays
//! private to this submodule.

use super::JoinKeyPart;
use crate::column::ColumnValue;
use crate::readable::ReadableTable;
use std::collections::{HashMap, HashSet};

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

/// Internal state for tracking aggregate statistics for one source column
#[derive(Debug, Clone)]
pub(super) struct ColumnAggState {
    /// Running sum for SUM and AVG calculations
    pub(super) sum: f64,
    /// Count of non-null values
    pub(super) count: usize,
    /// Current minimum value
    pub(super) min: Option<f64>,
    /// Current maximum value
    pub(super) max: Option<f64>,
    /// Sorted values for percentile calculations. Only populated when
    /// a Percentile or Median aggregation targets this source column.
    pub(super) sorted_values: Option<Vec<f64>>,
}

impl ColumnAggState {
    pub(super) fn new(needs_sorted: bool) -> Self {
        ColumnAggState {
            sum: 0.0,
            count: 0,
            min: None,
            max: None,
            sorted_values: if needs_sorted { Some(Vec::new()) } else { None },
        }
    }

    /// Add a numeric value to the aggregate state
    pub(super) fn add_value(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
        if let Some(ref mut sorted) = self.sorted_values {
            let pos = sorted.partition_point(|&v| v < value);
            sorted.insert(pos, value);
        }
    }

    /// Remove a numeric value from the aggregate state
    /// Returns false if MIN/MAX needs recalculation (deleted value was min or max)
    pub(super) fn remove_value(&mut self, value: f64) -> bool {
        self.sum -= value;
        self.count = self.count.saturating_sub(1);

        if let Some(ref mut sorted) = self.sorted_values {
            let pos = sorted.partition_point(|&v| v < value);
            if pos < sorted.len() && sorted[pos] == value {
                sorted.remove(pos);
            }
        }

        let needs_recalc = self.min == Some(value) || self.max == Some(value);
        !needs_recalc
    }

    /// Recalculate MIN/MAX from a set of values
    pub(super) fn recalculate_min_max(&mut self, values: &[f64]) {
        if values.is_empty() {
            self.min = None;
            self.max = None;
        } else {
            self.min = values.iter().copied().reduce(f64::min);
            self.max = values.iter().copied().reduce(f64::max);
        }
        if self.sorted_values.is_some() {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            self.sorted_values = Some(sorted);
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
            AggregateFunction::Min => self.min.map_or(ColumnValue::Null, ColumnValue::Float64),
            AggregateFunction::Max => self.max.map_or(ColumnValue::Null, ColumnValue::Float64),
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
    /// Parent row indices belonging to this group (for MIN/MAX recalc on delete)
    pub(super) row_indices: HashSet<usize>,
    /// Source columns that need sorted_values for percentile calculations
    pub(super) percentile_columns: HashSet<String>,
}

impl GroupState {
    pub(super) fn new() -> Self {
        GroupState {
            column_stats: HashMap::new(),
            row_indices: HashSet::new(),
            percentile_columns: HashSet::new(),
        }
    }

    /// Add a value for a specific source column
    pub(super) fn add_column_value(&mut self, source_col: &str, value: f64) {
        let needs_sorted = self.percentile_columns.contains(source_col);
        let stats = self
            .column_stats
            .entry(source_col.to_string())
            .or_insert_with(|| ColumnAggState::new(needs_sorted));
        stats.add_value(value);
    }

    /// Remove a value for a specific source column
    /// Returns false if MIN/MAX needs recalculation
    pub(super) fn remove_column_value(&mut self, source_col: &str, value: f64) -> bool {
        if let Some(stats) = self.column_stats.get_mut(source_col) {
            stats.remove_value(value)
        } else {
            true
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

    /// Recalculate MIN/MAX for a source column from a set of values
    pub(super) fn recalculate_column_min_max(&mut self, source_col: &str, values: &[f64]) {
        if let Some(stats) = self.column_stats.get_mut(source_col) {
            stats.recalculate_min_max(values);
        }
    }
}

/// A key for grouping rows — one typed part per group-by column, `None` = NULL.
///
/// Uses the same typed `JoinKeyPart` representation as joins, but with a
/// different float policy: GROUP BY folds all NaNs into a single group
/// (Postgres-style) and folds -0.0 into +0.0 (they compare equal), whereas
/// joins exclude NaN keys entirely.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct GroupKey(Vec<Option<JoinKeyPart>>);

fn canonical_f32_bits(v: f32) -> u32 {
    if v.is_nan() {
        f32::NAN.to_bits()
    } else if v == 0.0 {
        0.0_f32.to_bits()
    } else {
        v.to_bits()
    }
}

fn canonical_f64_bits(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else if v == 0.0 {
        0.0_f64.to_bits()
    } else {
        v.to_bits()
    }
}

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
    pub(super) fn from_single_int(value: i32) -> Self {
        GroupKey(vec![Some(JoinKeyPart::Int32(value))])
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
