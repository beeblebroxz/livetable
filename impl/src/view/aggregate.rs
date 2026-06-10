//! AggregateView — GROUP BY with incrementally maintained aggregates.

use crate::changeset::{IncrementalView, TableChange};
use crate::column::ColumnValue;
use crate::table::Table;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::aggregate_support::{AggregateFunction, GroupKey, GroupState};

/// AggregateView groups rows and computes aggregate functions per group.
/// Supports incremental updates when the parent table changes.
///
/// # Example
/// ```ignore
/// let agg = AggregateView::new(
///     "sales_by_region".to_string(),
///     table.clone(),
///     vec!["region".to_string()],
///     vec![
///         ("total".to_string(), "amount".to_string(), AggregateFunction::Sum),
///         ("avg_amount".to_string(), "amount".to_string(), AggregateFunction::Avg),
///     ],
/// )?;
/// ```
pub struct AggregateView {
    name: String,
    parent: Rc<RefCell<Table>>,
    group_by_columns: Vec<String>,
    /// (result_column_name, source_column_name, function)
    aggregations: Vec<(String, String, AggregateFunction)>,
    /// Map from group key to aggregate state
    groups: HashMap<GroupKey, GroupState>,
    /// Ordered list of group keys for consistent iteration
    group_order: Vec<GroupKey>,
    /// Map from parent row index to its group key
    row_to_group: HashMap<usize, GroupKey>,
    /// Whether row_to_group is populated (deferred for fast initial build)
    row_to_group_built: bool,
    /// Last synced generation from parent's changeset
    last_synced_generation: u64,
    /// Number of changes already processed (to skip old changes on sync)
    last_processed_change_count: usize,
}

impl AggregateView {
    pub fn new(
        name: String,
        parent: Rc<RefCell<Table>>,
        group_by_columns: Vec<String>,
        aggregations: Vec<(String, String, AggregateFunction)>,
    ) -> Result<Self, String> {
        // Validate group_by columns exist
        {
            let p = parent.borrow();
            for col in &group_by_columns {
                if p.schema().get_column_index(col).is_none() {
                    return Err(format!("Group-by column '{}' not found in table", col));
                }
            }
            // Validate aggregation source columns exist
            for (_, source_col, _) in &aggregations {
                if p.schema().get_column_index(source_col).is_none() {
                    return Err(format!(
                        "Aggregation source column '{}' not found in table",
                        source_col
                    ));
                }
            }
        }

        if aggregations.is_empty() {
            return Err("At least one aggregation is required".to_string());
        }

        let (generation, change_count) = {
            let p = parent.borrow();
            (p.changeset_generation(), p.changeset().total_len())
        };
        let mut view = AggregateView {
            name,
            parent,
            group_by_columns,
            aggregations,
            groups: HashMap::new(),
            group_order: Vec::new(),
            row_to_group: HashMap::new(),
            row_to_group_built: false,
            last_synced_generation: generation,
            last_processed_change_count: change_count,
        };
        view.rebuild_index();
        Ok(view)
    }

    fn rebuild_index(&mut self) {
        self.groups.clear();
        self.group_order.clear();
        self.row_to_group.clear();
        self.row_to_group_built = false;

        let pct_cols = self.percentile_source_columns();

        // Pre-compute column indices for fast access
        let parent = self.parent.borrow();
        let group_col_indices: Vec<usize> = self
            .group_by_columns
            .iter()
            .filter_map(|name| parent.schema().get_column_index(name))
            .collect();

        // Get unique source columns and their indices
        let source_col_info: Vec<(usize, String)> = self
            .unique_source_columns()
            .into_iter()
            .filter_map(|col| parent.schema().get_column_index(&col).map(|idx| (idx, col)))
            .collect();

        let num_rows = parent.len();
        let generation = parent.changeset_generation();
        let change_count = parent.changeset().total_len();

        // Check for single-column integer group-by fast path
        let is_single_int_group = group_col_indices.len() == 1 && {
            let col_idx = group_col_indices[0];
            matches!(
                parent.schema().get_column_info(col_idx),
                Some((_, crate::column::ColumnType::Int32, _))
            )
        };

        // Check for single-column aggregation on a numeric column
        let single_source_col_idx = if source_col_info.len() == 1 {
            Some(source_col_info[0].0)
        } else {
            None
        };
        let single_source_col_name = if source_col_info.len() == 1 {
            Some(source_col_info[0].1.clone())
        } else {
            None
        };

        // Use integer key map for single-int group by - minimizes allocations
        if is_single_int_group {
            let group_col_idx = group_col_indices[0];

            // For single-int groups with single aggregation (most common case),
            // use direct integer -> GroupState map to avoid ALL string allocations
            let has_single_source = single_source_col_idx.is_some();
            let source_col_idx = single_source_col_idx.unwrap_or(0);
            let source_col_name_ref = single_source_col_name.as_ref();

            // Build using integer keys directly
            let mut int_groups: HashMap<i32, GroupState> = HashMap::new();
            let mut int_group_order: Vec<i32> = Vec::new();

            for row_idx in 0..num_rows {
                // Get group key value directly as int
                let group_val = match parent.get_value_by_index(row_idx, group_col_idx) {
                    Ok(ColumnValue::Int32(v)) => v,
                    _ => continue, // Skip null/invalid
                };

                // Get or create group state directly with int key
                let is_new_group = !int_groups.contains_key(&group_val);
                let state = int_groups.entry(group_val).or_insert_with(|| {
                    let mut gs = GroupState::new();
                    gs.percentile_columns = pct_cols.clone();
                    gs
                });
                state.row_indices.insert(row_idx);

                // Add aggregation value(s)
                if has_single_source {
                    if let Ok(v) = parent.get_value_by_index(row_idx, source_col_idx) {
                        if let Some(num) = Self::extract_numeric(&v) {
                            state.add_column_value(source_col_name_ref.unwrap(), num);
                        }
                    }
                } else {
                    for (col_idx, col_name) in &source_col_info {
                        if let Ok(v) = parent.get_value_by_index(row_idx, *col_idx) {
                            if let Some(num) = Self::extract_numeric(&v) {
                                state.add_column_value(col_name, num);
                            }
                        }
                    }
                }

                if is_new_group {
                    int_group_order.push(group_val);
                }
            }

            // Convert integer groups to GroupKey-based maps (only allocate once per group)
            // NOTE: We skip populating row_to_group for initial build.
            // It will be populated lazily if incremental sync is needed.
            for &int_val in &int_group_order {
                let key = GroupKey::from_single_int(int_val);
                if let Some(state) = int_groups.remove(&int_val) {
                    self.groups.insert(key.clone(), state);
                    self.group_order.push(key);
                }
            }
            self.row_to_group_built = false; // Deferred for fast path
        } else {
            // General case - multi-column or non-int group by
            for row_idx in 0..num_rows {
                let key = GroupKey::from_indices(&parent, row_idx, &group_col_indices);
                self.row_to_group.insert(row_idx, key.clone());

                let col_values: Vec<(String, f64)> = source_col_info
                    .iter()
                    .filter_map(|(col_idx, col_name)| {
                        parent
                            .get_value_by_index(row_idx, *col_idx)
                            .ok()
                            .and_then(|v| Self::extract_numeric(&v))
                            .map(|num| (col_name.clone(), num))
                    })
                    .collect();

                let is_new_group = !self.groups.contains_key(&key);
                let state = self.groups.entry(key.clone()).or_insert_with(|| {
                    let mut gs = GroupState::new();
                    gs.percentile_columns = pct_cols.clone();
                    gs
                });
                state.row_indices.insert(row_idx);

                for (source_col, num) in col_values {
                    state.add_column_value(&source_col, num);
                }

                if is_new_group {
                    self.group_order.push(key);
                }
            }
            self.row_to_group_built = true; // General case builds it eagerly
        }
        drop(parent);

        self.last_synced_generation = generation;
        self.last_processed_change_count = change_count;
    }

    /// Build row_to_group mapping if not already built (lazy initialization)
    fn ensure_row_to_group_built(&mut self) {
        if self.row_to_group_built {
            return;
        }

        // Populate row_to_group from groups' row_indices
        for (key, state) in &self.groups {
            for &row_idx in &state.row_indices {
                self.row_to_group.insert(row_idx, key.clone());
            }
        }
        self.row_to_group_built = true;
    }

    /// Get unique source columns from aggregations
    fn unique_source_columns(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for (_, source_col, _) in &self.aggregations {
            if seen.insert(source_col.clone()) {
                result.push(source_col.clone());
            }
        }
        result
    }

    /// Get source columns that need sorted_values for percentile/median
    fn percentile_source_columns(&self) -> HashSet<String> {
        self.aggregations
            .iter()
            .filter(|(_, _, func)| {
                matches!(
                    func,
                    AggregateFunction::Percentile(_) | AggregateFunction::Median
                )
            })
            .map(|(_, source_col, _)| source_col.clone())
            .collect()
    }

    fn add_row_to_aggregates(&mut self, row_idx: usize, row: &HashMap<String, ColumnValue>) {
        let key = GroupKey::from_row(row, &self.group_by_columns);

        // Track which group this row belongs to
        self.row_to_group.insert(row_idx, key.clone());

        // Collect source columns and their values first
        let source_cols = self.unique_source_columns();
        let col_values: Vec<(String, f64)> = source_cols
            .into_iter()
            .filter_map(|col| {
                row.get(&col)
                    .and_then(Self::extract_numeric)
                    .map(|num| (col, num))
            })
            .collect();

        // Get or create group state
        let is_new_group = !self.groups.contains_key(&key);
        let pct_cols = self.percentile_source_columns();
        let state = self.groups.entry(key.clone()).or_insert_with(|| {
            let mut gs = GroupState::new();
            gs.percentile_columns = pct_cols;
            gs
        });

        // Add row index to group
        state.row_indices.insert(row_idx);

        // Add values for each unique source column (not per-aggregation!)
        for (source_col, num) in col_values {
            state.add_column_value(&source_col, num);
        }

        // Track group order
        if is_new_group {
            self.group_order.push(key);
        }
    }

    fn remove_row_from_aggregates(
        &mut self,
        row_idx: usize,
        row: &HashMap<String, ColumnValue>,
    ) -> bool {
        let key = GroupKey::from_row(row, &self.group_by_columns);

        // Remove from row_to_group
        self.row_to_group.remove(&row_idx);

        // Collect source columns and their values first
        let source_cols = self.unique_source_columns();
        let col_values: Vec<(String, f64)> = source_cols
            .into_iter()
            .filter_map(|col| {
                row.get(&col)
                    .and_then(Self::extract_numeric)
                    .map(|num| (col, num))
            })
            .collect();

        if let Some(state) = self.groups.get_mut(&key) {
            state.row_indices.remove(&row_idx);

            // Remove values for each unique source column
            let mut cols_needing_recalc = Vec::new();
            for (source_col, num) in col_values {
                if !state.remove_column_value(&source_col, num) {
                    cols_needing_recalc.push(source_col);
                }
            }

            // If group is now empty, remove it
            if state.row_indices.is_empty() {
                self.groups.remove(&key);
                self.group_order.retain(|k| k != &key);
                return true;
            }

            // Recalculate MIN/MAX for columns that need it
            for source_col in cols_needing_recalc {
                self.recalculate_group_column_min_max(&key, &source_col);
            }

            true
        } else {
            false
        }
    }

    fn recalculate_group_column_min_max(&mut self, key: &GroupKey, source_col: &str) {
        // Collect row indices and values first to avoid borrow conflicts
        let row_indices: Vec<usize> = if let Some(state) = self.groups.get(key) {
            state.row_indices.iter().copied().collect()
        } else {
            return;
        };

        let values: Vec<f64> = {
            let parent = self.parent.borrow();
            // Get column index once for direct access
            if let Some(col_idx) = parent.schema().get_column_index(source_col) {
                row_indices
                    .iter()
                    .filter_map(|&row_idx| {
                        parent
                            .get_value_by_index(row_idx, col_idx)
                            .ok()
                            .and_then(|value| Self::extract_numeric(&value))
                    })
                    .collect()
            } else {
                Vec::new()
            }
        };

        if let Some(state) = self.groups.get_mut(key) {
            state.recalculate_column_min_max(source_col, &values);
        }
    }

    /// Extract numeric value from ColumnValue, converting to f64
    fn extract_numeric(value: &ColumnValue) -> Option<f64> {
        match value {
            ColumnValue::Int32(v) => Some(*v as f64),
            ColumnValue::Int64(v) => Some(*v as f64),
            // NaN is excluded like NULL: once a NaN enters the running state it
            // can never be removed (sum -= NaN stays NaN, and binary search in
            // sorted_values can't locate it), permanently corrupting the group.
            ColumnValue::Float32(v) if v.is_nan() => None,
            ColumnValue::Float32(v) => Some(*v as f64),
            ColumnValue::Float64(v) if v.is_nan() => None,
            ColumnValue::Float64(v) => Some(*v),
            ColumnValue::Null => None,
            _ => None, // String, Bool not numeric
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn last_processed_change_count(&self) -> usize {
        self.last_processed_change_count
    }

    pub fn len(&self) -> usize {
        self.group_order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.group_order.is_empty()
    }

    /// Get column names: group-by columns + result columns
    pub fn column_names(&self) -> Vec<String> {
        let mut names = self.group_by_columns.clone();
        for (result_col, _, _) in &self.aggregations {
            names.push(result_col.clone());
        }
        names
    }

    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if index >= self.group_order.len() {
            return Err(format!("Index {} out of range [0, {})", index, self.len()));
        }

        let key = &self.group_order[index];
        let mut result =
            HashMap::with_capacity(self.group_by_columns.len() + self.aggregations.len());
        result.extend(key.to_column_values(&self.group_by_columns));

        if let Some(state) = self.groups.get(key) {
            for (result_col, source_col, func) in &self.aggregations {
                result.insert(result_col.clone(), state.get_result(source_col, *func));
            }
        }

        Ok(result)
    }

    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        if row >= self.group_order.len() {
            return Err(format!("Row {} out of range [0, {})", row, self.len()));
        }

        let key = &self.group_order[row];

        // Check if it's a group-by column
        if self.group_by_columns.iter().any(|c| c == column) {
            let values = key.to_column_values(&self.group_by_columns);
            return values
                .get(column)
                .cloned()
                .ok_or_else(|| format!("Column '{}' not found", column));
        }

        // Check if it's an aggregation result column
        if let Some(state) = self.groups.get(key) {
            for (result_col, source_col, func) in &self.aggregations {
                if result_col == column {
                    return Ok(state.get_result(source_col, *func));
                }
            }
        }

        Err(format!("Column '{}' not found", column))
    }

    pub fn refresh(&mut self) {
        self.rebuild_index();
    }

    /// Incrementally sync with parent table's changes
    pub fn sync(&mut self) -> bool {
        let parent = self.parent.borrow();
        let changes = match parent
            .changeset()
            .changes_from(self.last_processed_change_count)
        {
            Some(changes) => changes,
            None => {
                drop(parent);
                self.rebuild_index();
                return true;
            }
        };

        if changes.is_empty() {
            return false;
        }

        let new_changes: Vec<TableChange> = changes.to_vec();
        let new_count = parent.changeset().total_len();
        drop(parent);

        // Ensure row_to_group is built before processing changes
        self.ensure_row_to_group_built();

        let modified = self.apply_changes(&new_changes);
        self.last_processed_change_count = new_count;
        self.last_synced_generation = self.parent.borrow().changeset_generation();
        modified
    }
}

impl IncrementalView for AggregateView {
    fn apply_changes(&mut self, changes: &[TableChange]) -> bool {
        let mut modified = false;

        for change in changes {
            match change {
                TableChange::RowInserted { index, data } => {
                    // Adjust existing row indices
                    self.adjust_indices_for_insert(*index);
                    // Add the new row to aggregates
                    self.add_row_to_aggregates(*index, data);
                    modified = true;
                }

                TableChange::RowDeleted { index, data } => {
                    // Remove from aggregates
                    self.remove_row_from_aggregates(*index, data);
                    // Adjust remaining row indices
                    self.adjust_indices_for_delete(*index);
                    modified = true;
                }

                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } => {
                    // Check if group-by column changed
                    if self.group_by_columns.contains(column) {
                        // Need to move row to a different group
                        if let Ok(old_row) = self.reconstruct_old_row(*row, column, old_value) {
                            self.remove_row_from_aggregates(*row, &old_row);
                        }
                        // Get new row separately to avoid borrow conflicts
                        let new_row = self.parent.borrow().get_row(*row).ok();
                        if let Some(new_row) = new_row {
                            self.add_row_to_aggregates(*row, &new_row);
                        }
                        modified = true;
                    } else {
                        // Check if aggregated column changed
                        let affects_aggregation =
                            self.aggregations.iter().any(|(_, src, _)| src == column);
                        if affects_aggregation {
                            // Update the aggregate values
                            if let Some(key) = self.row_to_group.get(row).cloned() {
                                let mut needs_recalc = false;

                                // Remove old value
                                if let Some(state) = self.groups.get_mut(&key) {
                                    if let Some(old_num) = Self::extract_numeric(old_value) {
                                        needs_recalc = !state.remove_column_value(column, old_num);
                                    }
                                }

                                // Recalculate if needed (outside of the borrow)
                                if needs_recalc {
                                    self.recalculate_group_column_min_max(&key, column);
                                }

                                // Add new value
                                if let Some(state) = self.groups.get_mut(&key) {
                                    if let Some(new_num) = Self::extract_numeric(new_value) {
                                        state.add_column_value(column, new_num);
                                    }
                                }
                                modified = true;
                            }
                        }
                    }
                }
            }
        }

        modified
    }

    fn last_synced_generation(&self) -> u64 {
        self.last_synced_generation
    }

    fn rebuild(&mut self) {
        self.rebuild_index();
    }
}

impl AggregateView {
    /// Adjust row indices when a row is inserted
    fn adjust_indices_for_insert(&mut self, inserted_index: usize) {
        // Fast path: appending at the end doesn't shift any existing indices
        let max_existing = self.row_to_group.keys().max().copied();
        if max_existing.is_none_or(|max| inserted_index > max) {
            return;
        }

        // Update row_to_group keys
        let mut new_row_to_group = HashMap::new();
        for (idx, key) in self.row_to_group.drain() {
            let new_idx = if idx >= inserted_index { idx + 1 } else { idx };
            new_row_to_group.insert(new_idx, key);
        }
        self.row_to_group = new_row_to_group;

        // Update row_indices in each group
        for state in self.groups.values_mut() {
            let old_indices: Vec<usize> = state.row_indices.iter().copied().collect();
            state.row_indices.clear();
            for idx in old_indices {
                let new_idx = if idx >= inserted_index { idx + 1 } else { idx };
                state.row_indices.insert(new_idx);
            }
        }
    }

    /// Adjust row indices when a row is deleted
    fn adjust_indices_for_delete(&mut self, deleted_index: usize) {
        // Update row_to_group keys
        let mut new_row_to_group = HashMap::new();
        for (idx, key) in self.row_to_group.drain() {
            if idx > deleted_index {
                new_row_to_group.insert(idx - 1, key);
            } else if idx < deleted_index {
                new_row_to_group.insert(idx, key);
            }
            // idx == deleted_index is already removed
        }
        self.row_to_group = new_row_to_group;

        // Update row_indices in each group
        for state in self.groups.values_mut() {
            let old_indices: Vec<usize> = state.row_indices.iter().copied().collect();
            state.row_indices.clear();
            for idx in old_indices {
                if idx > deleted_index {
                    state.row_indices.insert(idx - 1);
                } else if idx < deleted_index {
                    state.row_indices.insert(idx);
                }
            }
        }
    }

    /// Reconstruct what a row looked like before a cell update
    fn reconstruct_old_row(
        &self,
        row_idx: usize,
        changed_col: &str,
        old_value: &ColumnValue,
    ) -> Result<HashMap<String, ColumnValue>, String> {
        let parent = self.parent.borrow();
        let mut row = parent.get_row(row_idx)?;
        row.insert(changed_col.to_string(), old_value.clone());
        Ok(row)
    }
}

// =============================================================================
// Rust-side tick() registry (fix #10)
//
// PyTable already exposes `tick()` for Python users by maintaining a private
// registry of view inner-states keyed by view type. Native Rust callers had
// to invoke `view.sync()` manually for each view they created.
//
// `TickableTable` (below) closes that gap WITHOUT adding state to `Table`
// itself — `Table` stays `Send` so the WebSocket server feature still works
// (it shares tables across threads via `Arc<Mutex<...>>`). Users who want
// auto-propagation construct a `TickableTable` wrapping their `Rc<RefCell<Table>>`.
//
// Layout:
//   - `Table` (in table.rs): pure data + mutation API. Send + Sync-friendly.
//   - `TickableTable` (here): owns a strong handle to the table AND a
//     registry of view "syncer" closures. NOT Send (holds `Rc` + `RefCell`),
//     so single-threaded use only. Mirrors what `PyTable` does internally.
//
// Why a wrapper rather than methods on `Rc<RefCell<Table>>`?
//   - The registry has to live somewhere persistent. Storing on `Table`
//     would force `Rc<RefCell<...>>` into `Table` and break Send.
//   - The wrapper lets `Table` stay primitive; users who don't need tick
//     don't pay any cost.
// =============================================================================
