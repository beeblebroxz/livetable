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
struct ColumnAggState {
    /// Running sum for SUM and AVG calculations
    sum: f64,
    /// Count of non-null values
    count: usize,
    /// Current minimum value
    min: Option<f64>,
    /// Current maximum value
    max: Option<f64>,
    /// Sorted values for percentile calculations. Only populated when
    /// a Percentile or Median aggregation targets this source column.
    sorted_values: Option<Vec<f64>>,
}

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

    /// Add a numeric value to the aggregate state
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

    /// Remove a numeric value from the aggregate state
    /// Returns false if MIN/MAX needs recalculation (deleted value was min or max)
    fn remove_value(&mut self, value: f64) -> bool {
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
    fn recalculate_min_max(&mut self, values: &[f64]) {
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
struct GroupState {
    /// Per-source-column aggregate statistics
    column_stats: HashMap<String, ColumnAggState>,
    /// Parent row indices belonging to this group (for MIN/MAX recalc on delete)
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

    /// Add a value for a specific source column
    fn add_column_value(&mut self, source_col: &str, value: f64) {
        let needs_sorted = self.percentile_columns.contains(source_col);
        let stats = self
            .column_stats
            .entry(source_col.to_string())
            .or_insert_with(|| ColumnAggState::new(needs_sorted));
        stats.add_value(value);
    }

    /// Remove a value for a specific source column
    /// Returns false if MIN/MAX needs recalculation
    fn remove_column_value(&mut self, source_col: &str, value: f64) -> bool {
        if let Some(stats) = self.column_stats.get_mut(source_col) {
            stats.remove_value(value)
        } else {
            true
        }
    }

    /// Get result for a specific aggregation (source column + function)
    fn get_result(&self, source_col: &str, func: AggregateFunction) -> ColumnValue {
        if let Some(stats) = self.column_stats.get(source_col) {
            stats.get_result(func)
        } else {
            ColumnValue::Null
        }
    }

    /// Recalculate MIN/MAX for a source column from a set of values
    fn recalculate_column_min_max(&mut self, source_col: &str, values: &[f64]) {
        if let Some(stats) = self.column_stats.get_mut(source_col) {
            stats.recalculate_min_max(values);
        }
    }
}

/// A key for grouping rows - vector of column values converted to comparable strings
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey(Vec<Option<String>>);

impl GroupKey {
    fn from_row(row: &HashMap<String, ColumnValue>, group_by: &[String]) -> Self {
        let values: Vec<Option<String>> = group_by
            .iter()
            .map(|col| {
                row.get(col).and_then(|v| match v {
                    ColumnValue::Null => None,
                    ColumnValue::Int32(n) => Some(format!("i{}", n)),
                    ColumnValue::Int64(n) => Some(format!("I{}", n)),
                    ColumnValue::Float32(f) => Some(format!("f{}", f)),
                    ColumnValue::Float64(f) => Some(format!("F{}", f)),
                    ColumnValue::String(s) => Some(format!("s{}", s)),
                    ColumnValue::Bool(b) => Some(if *b {
                        "B1".to_string()
                    } else {
                        "B0".to_string()
                    }),
                    ColumnValue::Date(d) => Some(format!("d{}", d)),
                    ColumnValue::DateTime(dt) => Some(format!("D{}", dt)),
                })
            })
            .collect();
        GroupKey(values)
    }

    /// Build GroupKey directly from table using column indices (faster than from_row)
    fn from_indices(table: &Table, row_idx: usize, col_indices: &[usize]) -> Self {
        let values: Vec<Option<String>> = col_indices
            .iter()
            .map(|&col_idx| match table.get_value_by_index(row_idx, col_idx) {
                Ok(ColumnValue::Null) => None,
                Ok(ColumnValue::Int32(v)) => Some(format!("i{}", v)),
                Ok(ColumnValue::Int64(v)) => Some(format!("I{}", v)),
                Ok(ColumnValue::Float32(v)) => Some(format!("f{}", v)),
                Ok(ColumnValue::Float64(v)) => Some(format!("F{}", v)),
                Ok(ColumnValue::String(s)) => Some(format!("s{}", s)),
                Ok(ColumnValue::Bool(b)) => Some(if b {
                    "B1".to_string()
                } else {
                    "B0".to_string()
                }),
                Ok(ColumnValue::Date(d)) => Some(format!("d{}", d)),
                Ok(ColumnValue::DateTime(dt)) => Some(format!("D{}", dt)),
                Err(_) => None,
            })
            .collect();
        GroupKey(values)
    }

    #[inline]
    fn from_single_int(value: i32) -> Self {
        GroupKey(vec![Some(format!("i{}", value))])
    }

    fn to_column_values(
        &self,
        group_by: &[String],
        parent: &Table,
    ) -> HashMap<String, ColumnValue> {
        let mut result = HashMap::new();
        for (i, col_name) in group_by.iter().enumerate() {
            let value = match &self.0[i] {
                None => ColumnValue::Null,
                Some(s) => {
                    if let Some(col_idx) = parent.schema().get_column_index(col_name) {
                        if let Some((_, col_type, _)) = parent.schema().get_column_info(col_idx) {
                            match col_type {
                                crate::column::ColumnType::String => {
                                    if let Some(stripped) = s.strip_prefix('s') {
                                        ColumnValue::String(stripped.to_string())
                                    } else if s.starts_with("String(\"") && s.ends_with("\")") {
                                        let inner = &s[8..s.len() - 2];
                                        ColumnValue::String(inner.to_string())
                                    } else {
                                        ColumnValue::String(s.clone())
                                    }
                                }
                                crate::column::ColumnType::Int32 => {
                                    if let Some(stripped) = s.strip_prefix('i') {
                                        stripped
                                            .parse()
                                            .map(ColumnValue::Int32)
                                            .unwrap_or(ColumnValue::Null)
                                    } else if s.starts_with("Int32(") && s.ends_with(')') {
                                        let inner = &s[6..s.len() - 1];
                                        inner
                                            .parse()
                                            .map(ColumnValue::Int32)
                                            .unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Int64 => {
                                    if let Some(stripped) = s.strip_prefix('I') {
                                        stripped
                                            .parse()
                                            .map(ColumnValue::Int64)
                                            .unwrap_or(ColumnValue::Null)
                                    } else if s.starts_with("Int64(") && s.ends_with(')') {
                                        let inner = &s[6..s.len() - 1];
                                        inner
                                            .parse()
                                            .map(ColumnValue::Int64)
                                            .unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Float32 => {
                                    if let Some(stripped) = s.strip_prefix('f') {
                                        stripped
                                            .parse()
                                            .map(ColumnValue::Float32)
                                            .unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Float64 => {
                                    if let Some(stripped) = s.strip_prefix('F') {
                                        stripped
                                            .parse()
                                            .map(ColumnValue::Float64)
                                            .unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Bool => {
                                    if s == "B1" || s == "Bool(true)" {
                                        ColumnValue::Bool(true)
                                    } else if s == "B0" || s == "Bool(false)" {
                                        ColumnValue::Bool(false)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Date => {
                                    if let Some(stripped) = s.strip_prefix('d') {
                                        stripped
                                            .parse()
                                            .map(ColumnValue::Date)
                                            .unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::DateTime => {
                                    if let Some(stripped) = s.strip_prefix('D') {
                                        stripped
                                            .parse()
                                            .map(ColumnValue::DateTime)
                                            .unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                            }
                        } else {
                            ColumnValue::String(s.clone())
                        }
                    } else {
                        ColumnValue::String(s.clone())
                    }
                }
            };
            result.insert(col_name.clone(), value);
        }
        result
    }
}
