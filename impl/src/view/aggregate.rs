//! AggregateView — GROUP BY with incrementally maintained aggregates.

use crate::changeset::{Changeset, IncrementalView, TableChange};
use crate::column::{ColumnType, ColumnValue};
use crate::filter_changes::{row_after_update, MAX_FILTER_REPLAY_CHANGES};
use crate::readable::ReadableTable;
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
    parent: Rc<RefCell<dyn ReadableTable>>,
    group_by_columns: Vec<String>,
    /// (result_column_name, source_column_name, function)
    aggregations: Vec<(String, String, AggregateFunction)>,
    /// Map from group key to aggregate state
    groups: HashMap<GroupKey, GroupState>,
    /// Ordered list of group keys for consistent iteration
    group_order: Vec<GroupKey>,
    /// Each parent row's group, by parent position (`None`: in no group). A
    /// structural event shifts it with one memmove instead of a rehash.
    row_groups: Vec<Option<Rc<GroupKey>>>,
    /// Last synced generation from the parent's changeset.
    last_synced_generation: u64,
    /// Consumed cursor in the parent's stream, or usize::MAX without a coherent
    /// baseline. root_changeset_cursor() translates for root compaction.
    last_processed_change_count: usize,
    /// Own sync counter; the visible version() adds the parent's version.
    sync_count: u64,
    /// Parent version() observed at the last sync/rebuild.
    last_parent_version: u64,
    /// The latest incremental batch in GROUP coordinates (group_order
    /// positions). Rebuilds invalidate it; consumers must then refresh.
    output_changes: Changeset,
}

/// One group's output row: key columns plus aggregate results.
type Row = HashMap<String, ColumnValue>;

/// How one incremental batch has touched a group so far. Output is a net diff:
/// deletions are emitted as they happen, updates and insertions at batch end.
enum Touch {
    /// Existed before the batch; holds its output row from before any change.
    Existing(Row),
    /// Created during the batch (including re-creating a deleted key).
    New,
    /// Removed during the batch; any RowDeleted has already been emitted.
    Deleted,
}

/// Result equality for output diffs. Floats compare by bits, so a NaN result
/// (e.g. SUM of +inf and -inf) is not re-emitted on every touch.
fn same_value(old: &ColumnValue, new: &ColumnValue) -> bool {
    match (old, new) {
        (ColumnValue::Float64(old), ColumnValue::Float64(new)) => old.to_bits() == new.to_bits(),
        _ => old == new,
    }
}

impl AggregateView {
    pub fn new(
        name: String,
        parent: Rc<RefCell<dyn ReadableTable>>,
        group_by_columns: Vec<String>,
        aggregations: Vec<(String, String, AggregateFunction)>,
    ) -> Result<Self, String> {
        // Validate group_by columns exist
        {
            let p = parent.borrow();
            for col in &group_by_columns {
                if p.column_index(col).is_none() {
                    return Err(format!("Group-by column '{}' not found in table", col));
                }
            }
            // Validate aggregation source columns exist
            for (_, source_col, _) in &aggregations {
                if p.column_index(source_col).is_none() {
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

        let (generation, change_count, parent_version) = {
            let p = parent.borrow();
            let (g, c) = match p.changeset() {
                Some(cs) => (cs.generation(), cs.total_len()),
                None => (0, usize::MAX),
            };
            (g, c, p.version())
        };
        let mut view = AggregateView {
            name,
            parent,
            group_by_columns,
            aggregations,
            groups: HashMap::new(),
            group_order: Vec::new(),
            row_groups: Vec::new(),
            last_synced_generation: generation,
            last_processed_change_count: change_count,
            sync_count: 0,
            last_parent_version: parent_version,
            output_changes: Changeset::new(),
        };
        view.rebuild_index();
        Ok(view)
    }

    fn rebuild_index(&mut self) {
        self.groups.clear();
        self.group_order.clear();
        self.row_groups.clear();

        let pct_cols = self.percentile_source_columns();
        let extreme_cols = self.extreme_source_columns();

        // Pre-compute column indices for fast access
        let parent = self.parent.borrow();
        let group_col_indices: Vec<usize> = self
            .group_by_columns
            .iter()
            .filter_map(|name| parent.column_index(name))
            .collect();

        // Get unique source columns and their indices
        let source_col_info: Vec<(usize, String)> = self
            .unique_source_columns()
            .into_iter()
            .filter_map(|col| parent.column_index(&col).map(|idx| (idx, col)))
            .collect();

        let num_rows = parent.len();
        self.row_groups.reserve(num_rows);
        let (generation, change_count) = match parent.changeset() {
            Some(cs) => (cs.generation(), cs.total_len()),
            None => (self.last_synced_generation, usize::MAX),
        };
        let parent_version = parent.version();

        // Check for single-column integer group-by fast path
        let is_single_int_group = group_col_indices.len() == 1
            && parent.column_type(group_col_indices[0]) == Some(ColumnType::Int32);

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
            let mut int_groups: HashMap<Option<i32>, GroupState> = HashMap::new();
            let mut int_group_order: Vec<Option<i32>> = Vec::new();

            for row_idx in 0..num_rows {
                // Get group key value directly as int
                let group_val = match parent.get_value_by_index(row_idx, group_col_idx) {
                    Ok(ColumnValue::Int32(v)) => Some(v),
                    Ok(ColumnValue::Null) => None,
                    _ => {
                        // In no group, but still holds its position.
                        self.row_groups.push(None);
                        continue;
                    }
                };

                // Get or create group state directly with int key
                let is_new_group = !int_groups.contains_key(&group_val);
                let position = int_group_order.len();
                let state = int_groups.entry(group_val).or_insert_with(|| {
                    let key = Rc::new(GroupKey::from_single_int(group_val));
                    GroupState::new(position, pct_cols.clone(), extreme_cols.clone(), key)
                });
                state.row_count += 1;
                self.row_groups.push(Some(state.key.clone()));

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
            for &int_val in &int_group_order {
                if let Some(state) = int_groups.remove(&int_val) {
                    let key = (*state.key).clone();
                    self.groups.insert(key.clone(), state);
                    self.group_order.push(key);
                }
            }
        } else {
            // General case - multi-column or non-int group by
            for row_idx in 0..num_rows {
                let key = GroupKey::from_indices(&*parent, row_idx, &group_col_indices);

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
                let position = self.group_order.len();
                let state = self.groups.entry(key.clone()).or_insert_with(|| {
                    GroupState::new(
                        position,
                        pct_cols.clone(),
                        extreme_cols.clone(),
                        Rc::new(key.clone()),
                    )
                });
                state.row_count += 1;
                self.row_groups.push(Some(state.key.clone()));

                for (source_col, num) in col_values {
                    state.add_column_value(&source_col, num);
                }

                if is_new_group {
                    self.group_order.push(key);
                }
            }
        }
        drop(parent);

        self.last_synced_generation = generation;
        self.last_processed_change_count = change_count;
        self.last_parent_version = parent_version;
        self.output_changes.invalidate();
        self.sync_count += 1;
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

    /// Source columns with a MIN or MAX aggregation, which keep a value multiset
    fn extreme_source_columns(&self) -> HashSet<String> {
        self.aggregations
            .iter()
            .filter(|(_, _, func)| matches!(func, AggregateFunction::Min | AggregateFunction::Max))
            .map(|(_, source_col, _)| source_col.clone())
            .collect()
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

    /// Add a row's values to its group, creating the group if needed. Returns
    /// the group's shared key; the caller records it at the row's position.
    fn add_to_group(&mut self, row: &Row, touched: &mut HashMap<GroupKey, Touch>) -> Rc<GroupKey> {
        let key = GroupKey::from_row(row, &self.group_by_columns);
        self.touch_existing(&key, touched);

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
        let extreme_cols = self.extreme_source_columns();
        let position = self.group_order.len();
        let state = self.groups.entry(key.clone()).or_insert_with(|| {
            GroupState::new(position, pct_cols, extreme_cols, Rc::new(key.clone()))
        });
        state.row_count += 1;

        // Add values for each unique source column (not per-aggregation!)
        for (source_col, num) in col_values {
            state.add_column_value(&source_col, num);
        }
        let shared = state.key.clone();

        // Track group order
        if is_new_group {
            touched.insert(key.clone(), Touch::New);
            self.group_order.push(key);
        }
        shared
    }

    /// Remove a row's values from its group, removing the group once empty.
    /// The caller updates the row's position.
    fn remove_from_group(
        &mut self,
        row: &Row,
        touched: &mut HashMap<GroupKey, Touch>,
        removed_slots: &mut Vec<usize>,
    ) {
        let key = GroupKey::from_row(row, &self.group_by_columns);
        self.touch_existing(&key, touched);

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
            debug_assert!(state.row_count > 0, "removing a row from an empty group");
            state.row_count -= 1;

            // Remove values for each unique source column
            for (source_col, num) in col_values {
                state.remove_column_value(&source_col, num);
            }

            // If group is now empty, remove it
            if state.row_count == 0 {
                self.remove_group(&key, touched, removed_slots);
            }
        }
    }

    /// Whether every event's index is in range for the rows before it:
    /// inserts at or before the end, deletes and updates before it.
    fn indices_in_range(mut len: usize, changes: &[TableChange]) -> bool {
        for change in changes {
            match change {
                TableChange::RowInserted { index, .. } if *index <= len => len += 1,
                TableChange::RowDeleted { index, .. } if *index < len => len -= 1,
                TableChange::CellUpdated { row, .. } if *row < len => {}
                _ => return false,
            }
        }
        true
    }

    /// Record an existing group's output row the first time a batch touches
    /// it, before any change, as the old side of the batch's net diff.
    fn touch_existing(&self, key: &GroupKey, touched: &mut HashMap<GroupKey, Touch>) {
        if !touched.contains_key(key) && self.groups.contains_key(key) {
            touched.insert(key.clone(), Touch::Existing(self.row_for_key(key)));
        }
    }

    /// Remove an emptied group. Its slot stays in `group_order` until the
    /// batch ends (`compact_group_order`), so no other position shifts per
    /// removal. Groups created in this batch sit after every pre-batch group,
    /// so a pre-batch group's row index, for a consumer that has applied the
    /// deletions emitted so far, is its slot minus earlier removed slots.
    fn remove_group(
        &mut self,
        key: &GroupKey,
        touched: &mut HashMap<GroupKey, Touch>,
        removed_slots: &mut Vec<usize>,
    ) {
        let Some(state) = self.groups.remove(key) else {
            return;
        };
        let slot = state.position;
        debug_assert!(self.group_order[slot] == *key, "stale group position");
        let removed_before = removed_slots.partition_point(|&removed| removed < slot);
        removed_slots.insert(removed_before, slot);
        if let Some(Touch::Existing(data)) = touched.insert(key.clone(), Touch::Deleted) {
            self.output_changes.push(TableChange::RowDeleted {
                index: slot - removed_before,
                data,
            });
        }
    }

    /// Drop the sorted slots of groups removed during a batch, then renumber
    /// the groups after the first one once.
    fn compact_group_order(&mut self, removed_slots: &[usize]) {
        let Some(&first) = removed_slots.first() else {
            return;
        };
        let mut removed = removed_slots.iter().peekable();
        let mut slot = 0;
        self.group_order.retain(|_| {
            let keep = removed.next_if_eq(&&slot).is_none();
            slot += 1;
            keep
        });
        for (position, key) in self.group_order.iter().enumerate().skip(first) {
            if let Some(state) = self.groups.get_mut(key) {
                state.position = position;
            }
        }
    }

    /// After a batch's structural changes, emit
    /// changed result cells of surviving groups, then insertions of new ones.
    /// Ascending positions put every update before the new groups at the tail.
    fn emit_batch_output(&mut self, touched: HashMap<GroupKey, Touch>) {
        let mut survivors: Vec<(usize, GroupKey, Option<Row>)> = touched
            .into_iter()
            .filter_map(|(key, touch)| {
                let before = match touch {
                    Touch::Existing(row) => Some(row),
                    Touch::New => None,
                    Touch::Deleted => return None,
                };
                let position = self.groups.get(&key)?.position;
                Some((position, key, before))
            })
            .collect();
        survivors.sort_unstable_by_key(|(position, _, _)| *position);

        for (position, key, before) in survivors {
            let Some(mut before) = before else {
                let data = self.row_for_key(&key);
                self.output_changes.push(TableChange::RowInserted {
                    index: position,
                    data,
                });
                continue;
            };
            let state = &self.groups[&key];
            for (result_col, source_col, func) in &self.aggregations {
                let new_value = state.get_result(source_col, *func);
                let old_value = before.remove(result_col).unwrap_or(ColumnValue::Null);
                if !same_value(&old_value, &new_value) {
                    self.output_changes.push(TableChange::CellUpdated {
                        row: position,
                        column: result_col.clone(),
                        old_value,
                        new_value,
                    });
                }
            }
        }
    }

    /// Output row for a group: its key columns plus aggregate results.
    fn row_for_key(&self, key: &GroupKey) -> Row {
        let mut result =
            HashMap::with_capacity(self.group_by_columns.len() + self.aggregations.len());
        result.extend(key.to_column_values(&self.group_by_columns));
        if let Some(state) = self.groups.get(key) {
            for (result_col, source_col, func) in &self.aggregations {
                result.insert(result_col.clone(), state.get_result(source_col, *func));
            }
        }
        result
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

    pub(crate) fn root_changeset_cursor(&self) -> usize {
        self.parent
            .borrow()
            .root_changeset_cursor(self.last_processed_change_count)
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

        Ok(self.row_for_key(&self.group_order[index]))
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
        let Some(changeset) = parent.changeset() else {
            // View parent: version-checked refresh fallback.
            let stale = parent.version() != self.last_parent_version;
            drop(parent);
            if !stale {
                return false;
            }
            self.rebuild_index();
            return true;
        };
        let changes = match changeset.changes_from(self.last_processed_change_count) {
            Some(changes) => changes,
            None => {
                drop(parent);
                self.rebuild_index();
                return true;
            }
        };

        if changes.is_empty() {
            // A filter can advance its ancestor version without emitting rows
            // (an excluded edit). Keep retained history, but record the parent
            // version so children and delivery still see a coherent baseline.
            self.last_parent_version = parent.version();
            return false;
        }

        let new_changes: Vec<TableChange> = changes.to_vec();
        let new_count = changeset.total_len();
        let new_generation = changeset.generation();
        let parent_version = parent.version();
        drop(parent);

        let modified = self.apply_changes(&new_changes);
        self.last_processed_change_count = new_count;
        self.last_synced_generation = new_generation;
        self.last_parent_version = parent_version;
        self.sync_count += 1;
        modified
    }
}

impl IncrementalView for AggregateView {
    fn apply_changes(&mut self, changes: &[TableChange]) -> bool {
        // Recover group-key updates in their original layout before replay.
        // Other updates need only the old/new scalar values in the changeset.
        let mut updated_rows = HashMap::new();
        for (index, change) in changes.iter().enumerate() {
            if matches!(change, TableChange::CellUpdated { column, .. }
                if self.group_by_columns.contains(column))
            {
                if changes.len() > MAX_FILTER_REPLAY_CHANGES {
                    self.rebuild_index();
                    return true;
                }
                let row = row_after_update(changes, index, |i| self.parent.borrow().get_row(i));
                match row {
                    Ok(row) => {
                        updated_rows.insert(index, row);
                    }
                    Err(_) => {
                        self.rebuild_index();
                        return true;
                    }
                }
            }
        }
        // Each structural event shifts `row_groups` once, a memmove. The bound
        // (doubled to cover 256 sort updates emitting 512 events) caps a batch
        // at about one rebuild's worth of memmoves.
        let multiple_shifts = changes
            .iter()
            .filter(|c| c.shifts_indices())
            .take(2)
            .count()
            == 2;
        if (multiple_shifts && changes.len() > MAX_FILTER_REPLAY_CHANGES * 2)
            || !Self::indices_in_range(self.row_groups.len(), changes)
        {
            self.rebuild_index();
            return true;
        }
        let mut modified = false;
        // Past every rebuild fallback: this batch replaces retained history.
        self.output_changes.clear();
        let mut touched: HashMap<GroupKey, Touch> = HashMap::new();
        let mut removed_slots: Vec<usize> = Vec::new();

        // Event indices refer to the rows as earlier events left them, so each
        // event updates `row_groups` in order.
        for (change_index, change) in changes.iter().enumerate() {
            match change {
                TableChange::RowInserted { index, data } => {
                    let key = self.add_to_group(data, &mut touched);
                    self.row_groups.insert(*index, Some(key));
                    modified = true;
                }

                TableChange::RowDeleted { index, data } => {
                    self.remove_from_group(data, &mut touched, &mut removed_slots);
                    self.row_groups.remove(*index);
                    modified = true;
                }

                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } => {
                    // A group-key update moves the row in place: no shift.
                    if self.group_by_columns.contains(column) {
                        let new_row = &updated_rows[&change_index];
                        let mut old_row = new_row.clone();
                        old_row.insert(column.clone(), old_value.clone());
                        self.remove_from_group(&old_row, &mut touched, &mut removed_slots);
                        let key = self.add_to_group(new_row, &mut touched);
                        self.row_groups[*row] = Some(key);
                        modified = true;
                    } else {
                        // Check if aggregated column changed
                        let affects_aggregation =
                            self.aggregations.iter().any(|(_, src, _)| src == column);
                        if affects_aggregation {
                            // Update the aggregate values
                            if let Some(shared) = self.row_groups[*row].clone() {
                                let key: &GroupKey = &shared;
                                self.touch_existing(key, &mut touched);

                                // Remove old value
                                if let Some(state) = self.groups.get_mut(key) {
                                    if let Some(old_num) = Self::extract_numeric(old_value) {
                                        state.remove_column_value(column, old_num);
                                    }
                                }

                                // Add new value
                                if let Some(state) = self.groups.get_mut(key) {
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

        self.compact_group_order(&removed_slots);
        self.emit_batch_output(touched);

        modified
    }

    fn last_synced_generation(&self) -> u64 {
        self.last_synced_generation
    }

    fn rebuild(&mut self) {
        self.rebuild_index();
    }
}

impl ReadableTable for AggregateView {
    fn len(&self) -> usize {
        self.group_order.len()
    }

    fn column_names(&self) -> Vec<String> {
        AggregateView::column_names(self)
    }

    fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        AggregateView::get_row(self, index)
    }

    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        AggregateView::get_value(self, row, column)
    }

    fn version(&self) -> u64 {
        self.sync_count.wrapping_add(self.parent.borrow().version())
    }

    fn changeset(&self) -> Option<&Changeset> {
        // Groups built from a stale parent are not a coherent baseline.
        (self.parent.borrow().version() == self.last_parent_version).then_some(&self.output_changes)
    }
}
