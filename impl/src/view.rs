/// LiveTable View Implementation
///
/// Views are read-only derived tables that automatically propagate changes from parent tables.
/// This is a simplified implementation focusing on core functionality.

use crate::column::ColumnValue;
use crate::table::Table;
use crate::changeset::{TableChange, IncrementalView, IndexAdjuster};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::cell::RefCell;

/// A FilterView filters rows from the parent table based on a predicate.
/// Maintains a mapping from view indices to parent indices.
///
/// Supports incremental updates: when the parent table changes, the view
/// can efficiently update its index mapping without a full rebuild.
pub struct FilterView {
    name: String,
    parent: Rc<RefCell<Table>>,
    predicate: Box<dyn Fn(&HashMap<String, ColumnValue>) -> bool>,
    view_to_parent: Vec<usize>,
    /// Last synced generation from parent's changeset
    last_synced_generation: u64,
    /// Number of changes already processed (absolute index)
    last_processed_change_count: usize,
}

impl FilterView {
    pub fn new<F>(name: String, parent: Rc<RefCell<Table>>, predicate: F) -> Self
    where
        F: Fn(&HashMap<String, ColumnValue>) -> bool + 'static,
    {
        let generation = parent.borrow().changeset_generation();
        let change_count = parent.borrow().changeset().total_len();
        let mut view = FilterView {
            name,
            parent,
            predicate: Box::new(predicate),
            view_to_parent: Vec::new(),
            last_synced_generation: generation,
            last_processed_change_count: change_count,
        };
        view.rebuild_index();
        view
    }

    fn rebuild_index(&mut self) {
        self.view_to_parent.clear();
        let parent = self.parent.borrow();

        for i in 0..parent.len() {
            if let Ok(row) = parent.get_row(i) {
                if (self.predicate)(&row) {
                    self.view_to_parent.push(i);
                }
            }
        }

        self.last_synced_generation = parent.changeset_generation();
        self.last_processed_change_count = parent.changeset().total_len();
    }

    pub fn len(&self) -> usize {
        self.view_to_parent.len()
    }

    pub fn is_empty(&self) -> bool {
        self.view_to_parent.is_empty()
    }

    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if index >= self.view_to_parent.len() {
            return Err(format!("Index {} out of range [0, {})", index, self.len()));
        }
        let parent_index = self.view_to_parent[index];
        self.parent.borrow().get_row(parent_index)
    }

    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        if row >= self.view_to_parent.len() {
            return Err(format!("Row {} out of range [0, {})", row, self.len()));
        }
        let parent_index = self.view_to_parent[row];
        self.parent.borrow().get_value(parent_index, column)
    }

    pub fn refresh(&mut self) {
        self.rebuild_index();
    }

    /// Incrementally sync with parent table's changes
    /// Returns true if any changes were applied
    pub fn sync(&mut self) -> bool {
        let parent = self.parent.borrow();
        let changes = match parent.changeset().changes_from(self.last_processed_change_count) {
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

        // Clone changes so we can drop the borrow
        let changes: Vec<TableChange> = changes.to_vec();
        drop(parent);

        let modified = self.apply_changes(&changes);
        let parent = self.parent.borrow();
        self.last_processed_change_count = parent.changeset().total_len();
        self.last_synced_generation = parent.changeset_generation();
        modified
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn last_processed_change_count(&self) -> usize {
        self.last_processed_change_count
    }
}

impl IncrementalView for FilterView {
    fn apply_changes(&mut self, changes: &[TableChange]) -> bool {
        let mut modified = false;

        for change in changes {
            match change {
                TableChange::RowInserted { index, data } => {
                    // First, adjust all existing parent indices >= insert index
                    IndexAdjuster::adjust_mapping_for_insert(&mut self.view_to_parent, *index);

                    // Check if the new row matches the predicate
                    if (self.predicate)(data) {
                        // Find where to insert in view_to_parent to maintain sorted order
                        let insert_pos = self.view_to_parent
                            .iter()
                            .position(|&parent_idx| parent_idx > *index)
                            .unwrap_or(self.view_to_parent.len());
                        self.view_to_parent.insert(insert_pos, *index);
                        modified = true;
                    }
                }

                TableChange::RowDeleted { index, .. } => {
                    // Find indices that need to be removed and adjust others
                    let to_remove = IndexAdjuster::adjust_mapping_for_delete(&mut self.view_to_parent, *index);

                    // Remove from back to front to maintain valid indices
                    for view_idx in to_remove.into_iter().rev() {
                        self.view_to_parent.remove(view_idx);
                        modified = true;
                    }
                }

                TableChange::CellUpdated { row, .. } => {
                    // Check if this row is currently in our view
                    let currently_in_view = self.view_to_parent.contains(row);

                    // Re-evaluate the predicate for this row
                    let now_matches = self.parent.borrow()
                        .get_row(*row)
                        .map(|data| (self.predicate)(&data))
                        .unwrap_or(false);

                    match (currently_in_view, now_matches) {
                        (false, true) => {
                            // Row now matches - add it
                            let insert_pos = self.view_to_parent
                                .iter()
                                .position(|&parent_idx| parent_idx > *row)
                                .unwrap_or(self.view_to_parent.len());
                            self.view_to_parent.insert(insert_pos, *row);
                            modified = true;
                        }
                        (true, false) => {
                            // Row no longer matches - remove it
                            if let Some(pos) = self.view_to_parent.iter().position(|&idx| idx == *row) {
                                self.view_to_parent.remove(pos);
                                modified = true;
                            }
                        }
                        _ => {
                            // No change in membership (still matches or still doesn't)
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

/// A ProjectionView selects specific columns from the parent table.
pub struct ProjectionView {
    name: String,
    parent: Rc<RefCell<Table>>,
    selected_columns: Vec<String>,
}

impl ProjectionView {
    pub fn new(name: String, parent: Rc<RefCell<Table>>, columns: Vec<String>) -> Result<Self, String> {
        // Validate columns exist
        {
            let parent_borrowed = parent.borrow();
            let schema = parent_borrowed.schema();
            for col in &columns {
                if schema.get_column_index(col).is_none() {
                    return Err(format!("Column '{}' not found in parent table", col));
                }
            }
        }

        Ok(ProjectionView {
            name,
            parent,
            selected_columns: columns,
        })
    }

    pub fn len(&self) -> usize {
        self.parent.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.parent.borrow().is_empty()
    }

    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        let full_row = self.parent.borrow().get_row(index)?;
        let mut result = HashMap::new();

        for col in &self.selected_columns {
            if let Some(value) = full_row.get(col) {
                result.insert(col.clone(), value.clone());
            }
        }

        Ok(result)
    }

    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        if !self.selected_columns.contains(&column.to_string()) {
            return Err(format!("Column '{}' not in projection", column));
        }
        self.parent.borrow().get_value(row, column)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.selected_columns
    }
}

/// A ComputedView adds a computed column to the parent table.
/// The computed column's value is calculated on-the-fly from other columns in each row.
pub struct ComputedView {
    name: String,
    parent: Rc<RefCell<Table>>,
    computed_col_name: String,
    compute_func: Box<dyn Fn(&HashMap<String, ColumnValue>) -> ColumnValue>,
}

impl ComputedView {
    pub fn new<F>(
        name: String,
        parent: Rc<RefCell<Table>>,
        computed_col_name: String,
        compute_func: F,
    ) -> Self
    where
        F: Fn(&HashMap<String, ColumnValue>) -> ColumnValue + 'static,
    {
        ComputedView {
            name,
            parent,
            computed_col_name,
            compute_func: Box::new(compute_func),
        }
    }

    pub fn len(&self) -> usize {
        self.parent.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.parent.borrow().is_empty()
    }

    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        let mut row = self.parent.borrow().get_row(index)?;
        let computed_value = (self.compute_func)(&row);
        row.insert(self.computed_col_name.clone(), computed_value);
        Ok(row)
    }

    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        if column == self.computed_col_name {
            let parent_row = self.parent.borrow().get_row(row)?;
            Ok((self.compute_func)(&parent_row))
        } else {
            self.parent.borrow().get_value(row, column)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn computed_column_name(&self) -> &str {
        &self.computed_col_name
    }
}

/// Join type specification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Left join: All rows from left table, matched rows from right (nulls if no match)
    Left,
    /// Inner join: Only rows that match in both tables
    Inner,
}

/// A JoinView combines two tables based on matching column values.
///
/// Currently supports:
/// - Left Join: All rows from left table, with matching data from right table
/// - Inner Join: Only rows where both tables have matching values
///
/// # Examples
///
/// ```
/// use livetable::{Table, Schema, ColumnType, ColumnValue, JoinView, JoinType};
/// use std::rc::Rc;
/// use std::cell::RefCell;
/// use std::collections::HashMap;
///
/// // Create users table
/// let users_schema = Schema::new(vec![
///     ("user_id".to_string(), ColumnType::Int32, false),
///     ("name".to_string(), ColumnType::String, false),
/// ]);
/// let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
///
/// // Create orders table
/// let orders_schema = Schema::new(vec![
///     ("order_id".to_string(), ColumnType::Int32, false),
///     ("user_id".to_string(), ColumnType::Int32, false),
///     ("amount".to_string(), ColumnType::Float64, false),
/// ]);
/// let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));
///
/// // Left join users with orders on user_id
/// let joined = JoinView::new(
///     "user_orders".to_string(),
///     users.clone(),
///     orders.clone(),
///     "user_id".to_string(),
///     "user_id".to_string(),
///     JoinType::Left,
/// ).unwrap();
/// ```
pub struct JoinView {
    name: String,
    left_table: Rc<RefCell<Table>>,
    right_table: Rc<RefCell<Table>>,
    /// Column names in left table to join on (supports multi-column joins)
    left_keys: Vec<String>,
    /// Column names in right table to join on (supports multi-column joins)
    right_keys: Vec<String>,
    join_type: JoinType,
    /// Cached joined rows: (left_row_index, optional_right_row_index)
    join_index: Vec<(usize, Option<usize>)>,
    /// Last synced generation from left table's changeset
    left_last_synced: u64,
    /// Last synced generation from right table's changeset
    right_last_synced: u64,
    /// Number of changes already processed from left table (absolute index)
    left_last_processed_change_count: usize,
    /// Number of changes already processed from right table (absolute index)
    right_last_processed_change_count: usize,
}

impl JoinView {
    /// Creates a new join view with single-column join keys.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for this view
    /// * `left_table` - Left table (all rows included in left join)
    /// * `right_table` - Right table (matched rows included)
    /// * `left_key` - Column name in left table to join on
    /// * `right_key` - Column name in right table to join on
    /// * `join_type` - Type of join (Left or Inner)
    ///
    /// # Returns
    ///
    /// Result containing the JoinView or an error if columns don't exist
    pub fn new(
        name: String,
        left_table: Rc<RefCell<Table>>,
        right_table: Rc<RefCell<Table>>,
        left_key: String,
        right_key: String,
        join_type: JoinType,
    ) -> Result<Self, String> {
        Self::new_multi(name, left_table, right_table, vec![left_key], vec![right_key], join_type)
    }

    /// Creates a new join view with multi-column join keys.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for this view
    /// * `left_table` - Left table (all rows included in left join)
    /// * `right_table` - Right table (matched rows included)
    /// * `left_keys` - Column names in left table to join on
    /// * `right_keys` - Column names in right table to join on
    /// * `join_type` - Type of join (Left or Inner)
    ///
    /// # Returns
    ///
    /// Result containing the JoinView or an error if columns don't exist
    /// or key counts don't match
    pub fn new_multi(
        name: String,
        left_table: Rc<RefCell<Table>>,
        right_table: Rc<RefCell<Table>>,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        join_type: JoinType,
    ) -> Result<Self, String> {
        // Validate key counts match
        if left_keys.len() != right_keys.len() {
            return Err(format!(
                "Join key count mismatch: left has {} keys, right has {} keys",
                left_keys.len(),
                right_keys.len()
            ));
        }

        if left_keys.is_empty() {
            return Err("At least one join key is required".to_string());
        }

        // Validate all left keys exist
        {
            let left = left_table.borrow();
            for key in &left_keys {
                if left.schema().get_column_index(key).is_none() {
                    return Err(format!("Left table missing column '{}'", key));
                }
            }
        }

        // Validate all right keys exist
        {
            let right = right_table.borrow();
            for key in &right_keys {
                if right.schema().get_column_index(key).is_none() {
                    return Err(format!("Right table missing column '{}'", key));
                }
            }
        }

        let left_gen = left_table.borrow().changeset_generation();
        let right_gen = right_table.borrow().changeset_generation();
        let left_change_count = left_table.borrow().changeset().total_len();
        let right_change_count = right_table.borrow().changeset().total_len();

        let mut view = JoinView {
            name,
            left_table,
            right_table,
            left_keys,
            right_keys,
            join_type,
            join_index: Vec::new(),
            left_last_synced: left_gen,
            right_last_synced: right_gen,
            left_last_processed_change_count: left_change_count,
            right_last_processed_change_count: right_change_count,
        };

        view.rebuild_index();
        Ok(view)
    }

    /// Build a composite key string from a HashMap row (for incremental sync).
    /// Returns None if any key column is missing or contains NULL.
    /// IMPORTANT: Key format must match build_key_from_indices exactly.
    fn build_composite_key(row: &HashMap<String, ColumnValue>, keys: &[String]) -> Option<String> {
        // Fast path for single-column join (most common case)
        if keys.len() == 1 {
            return match row.get(&keys[0]) {
                Some(ColumnValue::Null) | None => None,
                Some(ColumnValue::Int32(v)) => Some(v.to_string()),
                Some(ColumnValue::Int64(v)) => Some(v.to_string()),
                Some(ColumnValue::String(s)) => Some(s.clone()),
                Some(value) => Some(format!("{:?}", value)),
            };
        }

        let mut parts: Vec<String> = Vec::with_capacity(keys.len());
        for key in keys {
            match row.get(key) {
                Some(ColumnValue::Null) => return None,
                Some(ColumnValue::Int32(v)) => parts.push(v.to_string()),
                Some(ColumnValue::Int64(v)) => parts.push(v.to_string()),
                Some(ColumnValue::String(s)) => parts.push(s.clone()),
                Some(value) => parts.push(format!("{:?}", value)),
                None => return None,
            }
        }
        Some(parts.join("\x00"))
    }

    /// Build a composite key string from column values at given indices.
    /// Returns None if any key column contains NULL (SQL semantics: NULL != NULL)
    /// This is the fast path - uses pre-computed column indices.
    /// Optimized to minimize allocations for common types.
    fn build_key_from_indices(
        table: &Table,
        row: usize,
        col_indices: &[usize],
    ) -> Option<String> {
        // Fast path for single-column join (most common case)
        if col_indices.len() == 1 {
            return match table.get_value_by_index(row, col_indices[0]) {
                Ok(ColumnValue::Null) => None,
                Ok(ColumnValue::Int32(v)) => Some(v.to_string()),
                Ok(ColumnValue::Int64(v)) => Some(v.to_string()),
                Ok(ColumnValue::String(s)) => Some(s),
                Ok(value) => Some(format!("{:?}", value)),
                Err(_) => None,
            };
        }

        // Multi-column join path
        let mut parts: Vec<String> = Vec::with_capacity(col_indices.len());
        for &col_idx in col_indices {
            match table.get_value_by_index(row, col_idx) {
                Ok(ColumnValue::Null) => return None,
                Ok(ColumnValue::Int32(v)) => parts.push(v.to_string()),
                Ok(ColumnValue::Int64(v)) => parts.push(v.to_string()),
                Ok(ColumnValue::String(s)) => parts.push(s),
                Ok(value) => parts.push(format!("{:?}", value)),
                Err(_) => return None,
            }
        }
        Some(parts.join("\x00"))
    }

    /// Rebuilds the join index by scanning both tables.
    /// Optimized to use direct column access instead of building HashMaps per row.
    fn rebuild_index(&mut self) {
        self.join_index.clear();

        let left = self.left_table.borrow();
        let right = self.right_table.borrow();

        // Pre-compute column indices for join keys (done once, not per row)
        let left_col_indices: Vec<usize> = self.left_keys
            .iter()
            .filter_map(|k| left.schema().get_column_index(k))
            .collect();
        let right_col_indices: Vec<usize> = self.right_keys
            .iter()
            .filter_map(|k| right.schema().get_column_index(k))
            .collect();

        // Build a hashmap of right table values for efficient lookup
        // Uses direct column access instead of get_row()
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();

        for i in 0..right.len() {
            if let Some(key_str) = Self::build_key_from_indices(&right, i, &right_col_indices) {
                right_index.entry(key_str).or_insert_with(Vec::new).push(i);
            }
        }

        // For each left row, find matching right rows
        for i in 0..left.len() {
            if let Some(key_str) = Self::build_key_from_indices(&left, i, &left_col_indices) {
                if let Some(matching_indices) = right_index.get(&key_str) {
                    // Found matches - add each combination
                    for &right_idx in matching_indices {
                        self.join_index.push((i, Some(right_idx)));
                    }
                } else {
                    // No match
                    match self.join_type {
                        JoinType::Left => {
                            // Include left row with null right values
                            self.join_index.push((i, None));
                        }
                        JoinType::Inner => {
                            // Skip - no match means not included
                        }
                    }
                }
            }
        }

        // Update generation trackers
        self.left_last_synced = left.changeset_generation();
        self.right_last_synced = right.changeset_generation();
        self.left_last_processed_change_count = left.changeset().total_len();
        self.right_last_processed_change_count = right.changeset().total_len();
    }

    /// Build a lookup map from right table for efficient join operations
    fn build_right_lookup(&self) -> HashMap<String, Vec<usize>> {
        let right = self.right_table.borrow();
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();

        // Pre-compute column indices
        let right_col_indices: Vec<usize> = self.right_keys
            .iter()
            .filter_map(|k| right.schema().get_column_index(k))
            .collect();

        for i in 0..right.len() {
            if let Some(key_str) = Self::build_key_from_indices(&right, i, &right_col_indices) {
                right_index.entry(key_str).or_insert_with(Vec::new).push(i);
            }
        }

        right_index
    }

    /// Returns the number of rows in the joined result
    pub fn len(&self) -> usize {
        self.join_index.len()
    }

    /// Returns true if the join has no rows
    pub fn is_empty(&self) -> bool {
        self.join_index.is_empty()
    }

    /// Gets a row from the joined view
    ///
    /// The returned row contains all columns from both tables.
    /// For left joins where no right match exists, right columns will be Null.
    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if index >= self.join_index.len() {
            return Err(format!("Index {} out of range [0, {})", index, self.len()));
        }

        let (left_idx, right_idx_opt) = self.join_index[index];

        let mut result = HashMap::new();

        // Add all columns from left table
        let left_row = self.left_table.borrow().get_row(left_idx)?;
        result.extend(left_row);

        // Add columns from right table (or nulls if no match)
        if let Some(right_idx) = right_idx_opt {
            let right_row = self.right_table.borrow().get_row(right_idx)?;
            // Add right columns, prefixing with "right_" to avoid collisions
            for (col_name, value) in right_row {
                // All columns are included with "right_" prefix
                // (join keys are included to allow verification if needed)
                result.insert(format!("right_{}", col_name), value);
            }
        } else {
            // No match - add null values for all right columns
            let right_schema = self.right_table.borrow().schema().clone();
            for col_name in right_schema.get_column_names() {
                result.insert(format!("right_{}", col_name), ColumnValue::Null);
            }
        }

        Ok(result)
    }

    /// Gets a specific value from the joined view
    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        let full_row = self.get_row(row)?;
        full_row
            .get(column)
            .cloned()
            .ok_or_else(|| format!("Column '{}' not found in joined view", column))
    }

    /// Refreshes the join index after tables have changed
    pub fn refresh(&mut self) {
        self.rebuild_index();
    }

    /// Returns the name of the view
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the join type
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn last_processed_change_count(&self) -> (usize, usize) {
        (
            self.left_last_processed_change_count,
            self.right_last_processed_change_count,
        )
    }

    /// Incrementally sync with both parent tables' changes
    /// Returns true if any changes were applied
    ///
    /// Note: For complex changes (deletes, updates to join keys), this falls back
    /// to a full rebuild. Only appends are handled incrementally.
    pub fn sync(&mut self) -> bool {
        let left_table = self.left_table.borrow();
        let right_table = self.right_table.borrow();
        let left_changes = match left_table
            .changeset()
            .changes_from(self.left_last_processed_change_count)
        {
            Some(changes) => changes,
            None => {
                drop(left_table);
                drop(right_table);
                self.rebuild_index();
                return true;
            }
        };
        let right_changes = match right_table
            .changeset()
            .changes_from(self.right_last_processed_change_count)
        {
            Some(changes) => changes,
            None => {
                drop(left_table);
                drop(right_table);
                self.rebuild_index();
                return true;
            }
        };

        let left_changes: Vec<TableChange> = left_changes.to_vec();
        let right_changes: Vec<TableChange> = right_changes.to_vec();
        drop(left_table);
        drop(right_table);

        if left_changes.is_empty() && right_changes.is_empty() {
            return false;
        }

        // For simplicity, if there are any deletes or key updates, do a full rebuild
        // This is a conservative approach that ensures correctness
        let left_needs_rebuild = left_changes.iter().any(|c| match c {
            TableChange::RowDeleted { .. } => true,
            TableChange::CellUpdated { column, .. } => self.left_keys.contains(column),
            _ => false,
        });

        let right_needs_rebuild = right_changes.iter().any(|c| match c {
            TableChange::RowDeleted { .. } => true,
            TableChange::CellUpdated { column, .. } => self.right_keys.contains(column),
            _ => false,
        });

        let needs_rebuild = left_needs_rebuild || right_needs_rebuild;

        if needs_rebuild {
            self.rebuild_index();
            return true;
        }

        let mut modified = false;

        // Handle left table inserts — build right lookup once, not per insert
        let has_left_inserts = left_changes.iter().any(|c| matches!(c, TableChange::RowInserted { .. }));
        let right_lookup = if has_left_inserts { Some(self.build_right_lookup()) } else { None };

        for change in &left_changes {
            if let TableChange::RowInserted { index, data } = change {
                // Adjust existing left indices
                for (left_idx, _) in self.join_index.iter_mut() {
                    if *left_idx >= *index {
                        *left_idx += 1;
                    }
                }

                // Find matches for the new left row
                if let Some(key_str) = Self::build_composite_key(data, &self.left_keys) {
                    let lookup = right_lookup.as_ref().unwrap();

                    if let Some(matching_indices) = lookup.get(&key_str) {
                        for &right_idx in matching_indices {
                            self.join_index.push((*index, Some(right_idx)));
                            modified = true;
                        }
                    } else if self.join_type == JoinType::Left {
                        self.join_index.push((*index, None));
                        modified = true;
                    }
                }
            }
        }

        // Handle right table inserts
        for change in &right_changes {
            if let TableChange::RowInserted { index: right_idx, data } = change {
                // Adjust existing right indices
                for (_, right_opt) in self.join_index.iter_mut() {
                    if let Some(r_idx) = right_opt {
                        if *r_idx >= *right_idx {
                            *r_idx += 1;
                        }
                    }
                }

                // Find left rows that match this new right row
                if let Some(right_key_str) = Self::build_composite_key(data, &self.right_keys) {
                    let left = self.left_table.borrow();

                    // Pre-compute left column indices for fast access
                    let left_col_indices: Vec<usize> = self.left_keys
                        .iter()
                        .filter_map(|k| left.schema().get_column_index(k))
                        .collect();

                    for left_idx in 0..left.len() {
                        // Use fast direct column access instead of get_row()
                        if let Some(left_key_str) = Self::build_key_from_indices(&left, left_idx, &left_col_indices) {
                            if left_key_str == right_key_str {
                                // For left joins, we might need to replace a (left_idx, None)
                                // with (left_idx, Some(right_idx))
                                if self.join_type == JoinType::Left {
                                    // Check if there's an existing null match to replace
                                    let existing_null = self.join_index.iter()
                                        .position(|(l, r)| *l == left_idx && r.is_none());

                                    if let Some(pos) = existing_null {
                                        self.join_index[pos] = (left_idx, Some(*right_idx));
                                    } else {
                                        self.join_index.push((left_idx, Some(*right_idx)));
                                    }
                                } else {
                                    self.join_index.push((left_idx, Some(*right_idx)));
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
}

/// Sort order specification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Ascending order (smallest first)
    Ascending,
    /// Descending order (largest first)
    Descending,
}

/// A single sort key specifying a column and order
#[derive(Debug, Clone)]
pub struct SortKey {
    /// Column name to sort by
    pub column: String,
    /// Sort order (ascending or descending)
    pub order: SortOrder,
    /// Whether NULL values should be placed first or last
    pub nulls_first: bool,
}

impl SortKey {
    /// Create a new sort key with ascending order (nulls last)
    pub fn ascending(column: impl Into<String>) -> Self {
        SortKey {
            column: column.into(),
            order: SortOrder::Ascending,
            nulls_first: false,
        }
    }

    /// Create a new sort key with descending order (nulls last)
    pub fn descending(column: impl Into<String>) -> Self {
        SortKey {
            column: column.into(),
            order: SortOrder::Descending,
            nulls_first: false,
        }
    }

    /// Create a new sort key with custom options
    pub fn new(column: impl Into<String>, order: SortOrder, nulls_first: bool) -> Self {
        SortKey {
            column: column.into(),
            order,
            nulls_first,
        }
    }
}

/// A SortedView presents rows from the parent table in sorted order.
///
/// The view maintains a sorted index mapping from view positions to parent table
/// row indices. Sorting is performed on construction and after refresh/sync.
///
/// # Features
///
/// - Multi-column sorting with primary, secondary, etc. sort keys
/// - Ascending and descending order per column
/// - NULL handling (nulls first or nulls last)
/// - Incremental updates when parent table changes
/// - Binary search for efficient lookups (when applicable)
///
/// # Examples
///
/// ```
/// use livetable::{Table, Schema, ColumnType, ColumnValue, SortedView, SortKey, SortOrder};
/// use std::rc::Rc;
/// use std::cell::RefCell;
/// use std::collections::HashMap;
///
/// // Create a table
/// let schema = Schema::new(vec![
///     ("name".to_string(), ColumnType::String, false),
///     ("score".to_string(), ColumnType::Int32, false),
/// ]);
/// let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));
///
/// // Add data
/// {
///     let mut t = table.borrow_mut();
///     let mut row = HashMap::new();
///     row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
///     row.insert("score".to_string(), ColumnValue::Int32(85));
///     t.append_row(row).unwrap();
///
///     let mut row = HashMap::new();
///     row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
///     row.insert("score".to_string(), ColumnValue::Int32(92));
///     t.append_row(row).unwrap();
/// }
///
/// // Create sorted view (sorted by score descending)
/// let sorted = SortedView::new(
///     "by_score".to_string(),
///     table.clone(),
///     vec![SortKey::descending("score")],
/// ).unwrap();
///
/// assert_eq!(sorted.len(), 2);
/// // First row should be Alice (score 92)
/// assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Alice"));
/// ```
#[derive(Debug)]
pub struct SortedView {
    name: String,
    parent: Rc<RefCell<Table>>,
    sort_keys: Vec<SortKey>,
    /// Sorted index: sorted_index[view_pos] = parent_row_index
    sorted_index: Vec<usize>,
    /// Last synced generation from parent's changeset
    last_synced_generation: u64,
    /// Number of changes already processed (absolute index)
    last_processed_change_count: usize,
}

impl SortedView {
    /// Create a new sorted view with the given sort keys
    ///
    /// # Arguments
    ///
    /// * `name` - Name for this view
    /// * `parent` - Reference to the parent table
    /// * `sort_keys` - List of sort keys (first key is primary, etc.)
    ///
    /// # Returns
    ///
    /// Result containing the SortedView or an error if columns don't exist
    pub fn new(
        name: String,
        parent: Rc<RefCell<Table>>,
        sort_keys: Vec<SortKey>,
    ) -> Result<Self, String> {
        // Validate sort keys exist
        {
            let table = parent.borrow();
            for key in &sort_keys {
                if table.schema().get_column_index(&key.column).is_none() {
                    return Err(format!("Sort column '{}' not found in table", key.column));
                }
            }
        }

        if sort_keys.is_empty() {
            return Err("At least one sort key is required".to_string());
        }

        let generation = parent.borrow().changeset_generation();
        let change_count = parent.borrow().changeset().total_len();
        let mut view = SortedView {
            name,
            parent,
            sort_keys,
            sorted_index: Vec::new(),
            last_synced_generation: generation,
            last_processed_change_count: change_count,
        };
        view.rebuild_index();
        Ok(view)
    }

    /// Rebuild the sorted index from scratch
    fn rebuild_index(&mut self) {
        let table = self.parent.borrow();
        let len = table.len();

        // Create initial indices
        self.sorted_index = (0..len).collect();

        // Pre-extract all sort key values (O(N) instead of O(N log N) get_value calls)
        let sort_keys = self.sort_keys.clone();
        let sort_col_indices: Vec<usize> = sort_keys.iter()
            .filter_map(|key| table.schema().get_column_index(&key.column))
            .collect();

        // Pre-extract values for each sort key column
        let sort_values: Vec<Vec<ColumnValue>> = sort_col_indices.iter()
            .map(|&col_idx| {
                (0..len)
                    .map(|row_idx| {
                        table.get_value_by_index(row_idx, col_idx)
                            .unwrap_or(ColumnValue::Null)
                    })
                    .collect()
            })
            .collect();

        // Sort using pre-extracted values (reference, no clone needed)
        self.sorted_index.sort_by(|&a, &b| {
            for (key_idx, key) in sort_keys.iter().enumerate() {
                let val_a = &sort_values[key_idx][a];
                let val_b = &sort_values[key_idx][b];

                let cmp = Self::compare_values_ref(val_a, val_b, key);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        self.last_synced_generation = table.changeset_generation();
        self.last_processed_change_count = table.changeset().total_len();
    }

    /// Compare two column values by reference (avoids cloning)
    fn compare_values_ref(
        val_a: &ColumnValue,
        val_b: &ColumnValue,
        key: &SortKey,
    ) -> Ordering {
        let a_is_null = val_a.is_null();
        let b_is_null = val_b.is_null();

        // Handle NULL comparisons
        match (a_is_null, b_is_null) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if key.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, true) => {
                return if key.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, false) => {}
        }

        let base_cmp = match (val_a, val_b) {
            (ColumnValue::Int32(a), ColumnValue::Int32(b)) => a.cmp(b),
            (ColumnValue::Int64(a), ColumnValue::Int64(b)) => a.cmp(b),
            (ColumnValue::Float32(a), ColumnValue::Float32(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (ColumnValue::Float64(a), ColumnValue::Float64(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (ColumnValue::String(a), ColumnValue::String(b)) => a.cmp(b),
            (ColumnValue::Bool(a), ColumnValue::Bool(b)) => a.cmp(b),
            (ColumnValue::Date(a), ColumnValue::Date(b)) => a.cmp(b),
            (ColumnValue::DateTime(a), ColumnValue::DateTime(b)) => a.cmp(b),
            // Mixed types - compare type ordering for deterministic results
            _ => Ordering::Equal, // Different types are equal for stability
        };

        // Apply sort order
        match key.order {
            SortOrder::Ascending => base_cmp,
            SortOrder::Descending => base_cmp.reverse(),
        }
    }

    /// Compare two column values according to a sort key
    fn compare_values(
        val_a: &Option<ColumnValue>,
        val_b: &Option<ColumnValue>,
        key: &SortKey,
    ) -> Ordering {
        let a_is_null = val_a.is_none() || val_a.as_ref().map(|v| v.is_null()).unwrap_or(true);
        let b_is_null = val_b.is_none() || val_b.as_ref().map(|v| v.is_null()).unwrap_or(true);

        // Handle NULL comparisons
        match (a_is_null, b_is_null) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if key.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, true) => {
                return if key.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, false) => {}
        }

        let base_cmp = match (val_a.as_ref().unwrap(), val_b.as_ref().unwrap()) {
            (ColumnValue::Int32(a), ColumnValue::Int32(b)) => a.cmp(b),
            (ColumnValue::Int64(a), ColumnValue::Int64(b)) => a.cmp(b),
            (ColumnValue::Float32(a), ColumnValue::Float32(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (ColumnValue::Float64(a), ColumnValue::Float64(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (ColumnValue::String(a), ColumnValue::String(b)) => a.cmp(b),
            (ColumnValue::Bool(a), ColumnValue::Bool(b)) => a.cmp(b),
            (ColumnValue::Date(a), ColumnValue::Date(b)) => a.cmp(b),
            (ColumnValue::DateTime(a), ColumnValue::DateTime(b)) => a.cmp(b),
            // Mixed types - compare by type name for deterministic ordering
            (a, b) => format!("{:?}", a).cmp(&format!("{:?}", b)),
        };

        // Apply sort order
        match key.order {
            SortOrder::Ascending => base_cmp,
            SortOrder::Descending => base_cmp.reverse(),
        }
    }

    /// Find the insertion position for a new value using binary search
    fn find_insertion_position(&self, parent_index: usize) -> usize {
        let table = self.parent.borrow();

        let result = self.sorted_index.binary_search_by(|&existing_idx| {
            for key in &self.sort_keys {
                let val_existing = table.get_value(existing_idx, &key.column).ok();
                let val_new = table.get_value(parent_index, &key.column).ok();

                let cmp = Self::compare_values(&val_existing, &val_new, key);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            // For equal values, maintain stable sort by comparing parent indices
            existing_idx.cmp(&parent_index)
        });

        match result {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    /// Returns the number of rows in the sorted view
    pub fn len(&self) -> usize {
        self.sorted_index.len()
    }

    /// Returns true if the view is empty
    pub fn is_empty(&self) -> bool {
        self.sorted_index.is_empty()
    }

    /// Get a row at the given view position (sorted order)
    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if index >= self.sorted_index.len() {
            return Err(format!("Index {} out of range [0, {})", index, self.len()));
        }
        let parent_index = self.sorted_index[index];
        self.parent.borrow().get_row(parent_index)
    }

    /// Get a specific value at the given view position (sorted order)
    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        if row >= self.sorted_index.len() {
            return Err(format!("Row {} out of range [0, {})", row, self.len()));
        }
        let parent_index = self.sorted_index[row];
        self.parent.borrow().get_value(parent_index, column)
    }

    /// Returns the parent table row index for a given view position
    pub fn get_parent_index(&self, view_index: usize) -> Option<usize> {
        self.sorted_index.get(view_index).copied()
    }

    /// Force a full refresh of the sorted index
    pub fn refresh(&mut self) {
        self.rebuild_index();
    }

    /// Incrementally sync with parent table's changes
    /// Returns true if any changes were applied
    pub fn sync(&mut self) -> bool {
        let parent = self.parent.borrow();
        let changes = match parent.changeset().changes_from(self.last_processed_change_count) {
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

        let changes: Vec<TableChange> = changes.to_vec();
        drop(parent);
        let modified = self.apply_changes(&changes);
        let parent = self.parent.borrow();
        self.last_processed_change_count = parent.changeset().total_len();
        self.last_synced_generation = parent.changeset_generation();
        modified
    }

    /// Returns the name of this view
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the sort keys
    pub fn sort_keys(&self) -> &[SortKey] {
        &self.sort_keys
    }

    pub fn last_processed_change_count(&self) -> usize {
        self.last_processed_change_count
    }
}

impl IncrementalView for SortedView {
    fn apply_changes(&mut self, changes: &[TableChange]) -> bool {
        let mut modified = false;

        for change in changes {
            match change {
                TableChange::RowInserted { index, .. } => {
                    // First, adjust all existing parent indices >= insert index
                    for parent_idx in self.sorted_index.iter_mut() {
                        if *parent_idx >= *index {
                            *parent_idx += 1;
                        }
                    }

                    // Find correct sorted position for the new row
                    let insert_pos = self.find_insertion_position(*index);
                    self.sorted_index.insert(insert_pos, *index);
                    modified = true;
                }

                TableChange::RowDeleted { index, .. } => {
                    // Find the view position that points to this parent index
                    let view_pos = self.sorted_index.iter().position(|&i| i == *index);

                    // Adjust indices and remove
                    for parent_idx in self.sorted_index.iter_mut() {
                        if *parent_idx > *index {
                            *parent_idx -= 1;
                        }
                    }

                    if let Some(pos) = view_pos {
                        self.sorted_index.remove(pos);
                        modified = true;
                    }
                }

                TableChange::CellUpdated { row, column, .. } => {
                    // Check if the updated column is one of our sort keys
                    let affects_sort = self.sort_keys.iter().any(|k| k.column == *column);

                    if affects_sort {
                        // The row's sort position may have changed
                        // Remove from current position and re-insert at correct position
                        if let Some(current_pos) = self.sorted_index.iter().position(|&i| i == *row) {
                            self.sorted_index.remove(current_pos);
                            let new_pos = self.find_insertion_position(*row);
                            self.sorted_index.insert(new_pos, *row);
                            modified = true;
                        }
                    }
                    // If update doesn't affect sort keys, no change needed
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

// ============================================================================
// AggregateView - Grouped aggregations with incremental updates
// ============================================================================

/// Supported aggregation functions
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Percentile(f64),  // p value in 0.0..=1.0
    Median,           // Sugar for Percentile(0.5)
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

        // Check if we need to recalculate min/max
        let needs_recalc = self.min.map_or(false, |m| m == value)
            || self.max.map_or(false, |m| m == value);

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
        // Rebuild sorted_values if tracking percentiles
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
        let stats = self.column_stats
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
                row.get(col).and_then(|v| {
                    // Use compact format consistent with from_indices
                    match v {
                        ColumnValue::Null => None,
                        ColumnValue::Int32(n) => Some(format!("i{}", n)),
                        ColumnValue::Int64(n) => Some(format!("I{}", n)),
                        ColumnValue::Float32(f) => Some(format!("f{}", f)),
                        ColumnValue::Float64(f) => Some(format!("F{}", f)),
                        ColumnValue::String(s) => Some(format!("s{}", s)),
                        ColumnValue::Bool(b) => Some(if *b { "B1".to_string() } else { "B0".to_string() }),
                        ColumnValue::Date(d) => Some(format!("d{}", d)),
                        ColumnValue::DateTime(dt) => Some(format!("D{}", dt)),
                    }
                })
            })
            .collect();
        GroupKey(values)
    }

    /// Build GroupKey directly from table using column indices (faster than from_row)
    fn from_indices(table: &Table, row_idx: usize, col_indices: &[usize]) -> Self {
        let values: Vec<Option<String>> = col_indices
            .iter()
            .map(|&col_idx| {
                match table.get_value_by_index(row_idx, col_idx) {
                    Ok(ColumnValue::Null) => None,
                    // Use simpler string representation - just the value
                    // We prepend a type marker byte to disambiguate types with same string repr
                    Ok(ColumnValue::Int32(v)) => Some(format!("i{}", v)),
                    Ok(ColumnValue::Int64(v)) => Some(format!("I{}", v)),
                    Ok(ColumnValue::Float32(v)) => Some(format!("f{}", v)),
                    Ok(ColumnValue::Float64(v)) => Some(format!("F{}", v)),
                    Ok(ColumnValue::String(s)) => Some(format!("s{}", s)),
                    Ok(ColumnValue::Bool(b)) => Some(if b { "B1".to_string() } else { "B0".to_string() }),
                    Ok(ColumnValue::Date(d)) => Some(format!("d{}", d)),
                    Ok(ColumnValue::DateTime(dt)) => Some(format!("D{}", dt)),
                    Err(_) => None,
                }
            })
            .collect();
        GroupKey(values)
    }

    /// Build GroupKey for a single integer column (most common case) - ultra fast path
    #[inline]
    fn from_single_int(value: i32) -> Self {
        // Avoid string allocation for the most common case - we use a static prefix
        GroupKey(vec![Some(format!("i{}", value))])
    }

    fn to_column_values(&self, group_by: &[String], parent: &Table) -> HashMap<String, ColumnValue> {
        let mut result = HashMap::new();
        for (i, col_name) in group_by.iter().enumerate() {
            let value = match &self.0[i] {
                None => ColumnValue::Null,
                Some(s) => {
                    // Try to reconstruct the original value based on column type
                    if let Some(col_idx) = parent.schema().get_column_index(col_name) {
                        if let Some((_, col_type, _)) = parent.schema().get_column_info(col_idx) {
                            // Parse the key string back to ColumnValue
                            // Supports both old format (Int32(...)) and new compact format (i...)
                            match col_type {
                                crate::column::ColumnType::String => {
                                    // New format: s<value>
                                    if s.starts_with('s') {
                                        ColumnValue::String(s[1..].to_string())
                                    // Old format: String("value")
                                    } else if s.starts_with("String(\"") && s.ends_with("\")") {
                                        let inner = &s[8..s.len()-2];
                                        ColumnValue::String(inner.to_string())
                                    } else {
                                        ColumnValue::String(s.clone())
                                    }
                                }
                                crate::column::ColumnType::Int32 => {
                                    // New format: i<value>
                                    if s.starts_with('i') {
                                        s[1..].parse().map(ColumnValue::Int32).unwrap_or(ColumnValue::Null)
                                    // Old format: Int32(value)
                                    } else if s.starts_with("Int32(") && s.ends_with(")") {
                                        let inner = &s[6..s.len()-1];
                                        inner.parse().map(ColumnValue::Int32).unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Int64 => {
                                    // New format: I<value>
                                    if s.starts_with('I') {
                                        s[1..].parse().map(ColumnValue::Int64).unwrap_or(ColumnValue::Null)
                                    // Old format: Int64(value)
                                    } else if s.starts_with("Int64(") && s.ends_with(")") {
                                        let inner = &s[6..s.len()-1];
                                        inner.parse().map(ColumnValue::Int64).unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Float32 => {
                                    // New format: f<value>
                                    if s.starts_with('f') {
                                        s[1..].parse().map(ColumnValue::Float32).unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Float64 => {
                                    // New format: F<value>
                                    if s.starts_with('F') {
                                        s[1..].parse().map(ColumnValue::Float64).unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Bool => {
                                    // New format: B0 or B1
                                    if s == "B1" || s == "Bool(true)" {
                                        ColumnValue::Bool(true)
                                    } else if s == "B0" || s == "Bool(false)" {
                                        ColumnValue::Bool(false)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Date => {
                                    // New format: d<days>
                                    if s.starts_with('d') {
                                        s[1..].parse().map(ColumnValue::Date).unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::DateTime => {
                                    // New format: D<milliseconds>
                                    if s.starts_with('D') {
                                        s[1..].parse().map(ColumnValue::DateTime).unwrap_or(ColumnValue::Null)
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
                    return Err(format!("Aggregation source column '{}' not found in table", source_col));
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
        let group_col_indices: Vec<usize> = self.group_by_columns
            .iter()
            .filter_map(|name| parent.schema().get_column_index(name))
            .collect();

        // Get unique source columns and their indices
        let source_col_info: Vec<(usize, String)> = self.unique_source_columns()
            .into_iter()
            .filter_map(|col| {
                parent.schema().get_column_index(&col).map(|idx| (idx, col))
            })
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
                        parent.get_value_by_index(row_idx, *col_idx)
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
        self.aggregations.iter()
            .filter(|(_, _, func)| matches!(func, AggregateFunction::Percentile(_) | AggregateFunction::Median))
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
                    .and_then(|v| Self::extract_numeric(v))
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

    fn remove_row_from_aggregates(&mut self, row_idx: usize, row: &HashMap<String, ColumnValue>) -> bool {
        let key = GroupKey::from_row(row, &self.group_by_columns);

        // Remove from row_to_group
        self.row_to_group.remove(&row_idx);

        // Collect source columns and their values first
        let source_cols = self.unique_source_columns();
        let col_values: Vec<(String, f64)> = source_cols
            .into_iter()
            .filter_map(|col| {
                row.get(&col)
                    .and_then(|v| Self::extract_numeric(v))
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
                        parent.get_value_by_index(row_idx, col_idx)
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
            ColumnValue::Float32(v) => Some(*v as f64),
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
        let parent = self.parent.borrow();
        let mut result = key.to_column_values(&self.group_by_columns, &parent);

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
            let parent = self.parent.borrow();
            let values = key.to_column_values(&self.group_by_columns, &parent);
            return values.get(column).cloned().ok_or_else(|| format!("Column '{}' not found", column));
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
        let changes = match parent.changeset().changes_from(self.last_processed_change_count) {
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

                TableChange::CellUpdated { row, column, old_value, new_value } => {
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
                        let affects_aggregation = self.aggregations.iter().any(|(_, src, _)| src == column);
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
        if max_existing.map_or(true, |max| inserted_index > max) {
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
    fn reconstruct_old_row(&self, row_idx: usize, changed_col: &str, old_value: &ColumnValue) -> Result<HashMap<String, ColumnValue>, String> {
        let parent = self.parent.borrow();
        let mut row = parent.get_row(row_idx)?;
        row.insert(changed_col.to_string(), old_value.clone());
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnType;
    use crate::table::Schema;

    // === ColumnAggState Percentile Tests ===

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

    // === AggregateView Percentile Integration Tests ===

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

    // === Existing Tests ===

    #[test]
    fn test_filter_view() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        // Add some rows
        {
            let mut t = table.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("id".to_string(), ColumnValue::Int32(1));
            row1.insert("value".to_string(), ColumnValue::Int32(10));
            t.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("id".to_string(), ColumnValue::Int32(2));
            row2.insert("value".to_string(), ColumnValue::Int32(25));
            t.append_row(row2).unwrap();

            let mut row3 = HashMap::new();
            row3.insert("id".to_string(), ColumnValue::Int32(3));
            row3.insert("value".to_string(), ColumnValue::Int32(30));
            t.append_row(row3).unwrap();
        }

        // Create filter view: value > 20
        let view = FilterView::new(
            "filtered".to_string(),
            table.clone(),
            |row| {
                if let Some(ColumnValue::Int32(v)) = row.get("value") {
                    *v > 20
                } else {
                    false
                }
            },
        );

        assert_eq!(view.len(), 2);
        assert_eq!(view.get_value(0, "id").unwrap().as_i32(), Some(2));
        assert_eq!(view.get_value(1, "id").unwrap().as_i32(), Some(3));
    }

    #[test]
    fn test_filter_view_propagation() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("active".to_string(), ColumnType::Bool, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        // Add initial rows
        {
            let mut t = table.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("id".to_string(), ColumnValue::Int32(1));
            row1.insert("active".to_string(), ColumnValue::Bool(true));
            t.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("id".to_string(), ColumnValue::Int32(2));
            row2.insert("active".to_string(), ColumnValue::Bool(false));
            t.append_row(row2).unwrap();
        }

        let mut view = FilterView::new(
            "active_only".to_string(),
            table.clone(),
            |row| {
                if let Some(ColumnValue::Bool(active)) = row.get("active") {
                    *active
                } else {
                    false
                }
            },
        );

        assert_eq!(view.len(), 1);

        // Add another active row
        {
            let mut t = table.borrow_mut();
            let mut row3 = HashMap::new();
            row3.insert("id".to_string(), ColumnValue::Int32(3));
            row3.insert("active".to_string(), ColumnValue::Bool(true));
            t.append_row(row3).unwrap();
        }

        // Refresh view to see new row
        view.refresh();
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_projection_view() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
            ("secret".to_string(), ColumnType::String, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(1));
            row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            row.insert("secret".to_string(), ColumnValue::String("password123".to_string()));
            t.append_row(row).unwrap();
        }

        // Create projection without secret column
        let view = ProjectionView::new(
            "public".to_string(),
            table.clone(),
            vec!["id".to_string(), "name".to_string()],
        ).unwrap();

        assert_eq!(view.len(), 1);

        let row = view.get_row(0).unwrap();
        assert_eq!(row.get("id").unwrap().as_i32(), Some(1));
        assert_eq!(row.get("name").unwrap().as_string(), Some("Alice"));
        assert!(row.get("secret").is_none()); // Secret column not in projection
    }

    #[test]
    fn test_view_readonly() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        let view = FilterView::new(
            "readonly".to_string(),
            table.clone(),
            |_| true,
        );

        // Views don't have mutation methods - they're read-only by design
        // This test just verifies the view exists and works
        assert_eq!(view.len(), 0);
    }

    #[test]
    fn test_computed_view() {
        let schema = Schema::new(vec![
            ("price".to_string(), ColumnType::Float64, false),
            ("quantity".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("sales".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("price".to_string(), ColumnValue::Float64(10.5));
            row.insert("quantity".to_string(), ColumnValue::Int32(3));
            t.append_row(row).unwrap();
        }

        // Create computed view with total column
        let view = ComputedView::new(
            "with_total".to_string(),
            table.clone(),
            "total".to_string(),
            |row| {
                let price = match row.get("price") {
                    Some(ColumnValue::Float64(p)) => *p,
                    _ => 0.0,
                };
                let qty = match row.get("quantity") {
                    Some(ColumnValue::Int32(q)) => *q as f64,
                    _ => 0.0,
                };
                ColumnValue::Float64(price * qty)
            },
        );

        assert_eq!(view.len(), 1);
        assert_eq!(view.get_value(0, "total").unwrap().as_f64(), Some(31.5));

        // Check full row includes computed column
        let row = view.get_row(0).unwrap();
        assert_eq!(row.get("price").unwrap().as_f64(), Some(10.5));
        assert_eq!(row.get("quantity").unwrap().as_i32(), Some(3));
        assert_eq!(row.get("total").unwrap().as_f64(), Some(31.5));
    }

    #[test]
    fn test_left_join() {
        // Create users table
        let users_schema = Schema::new(vec![
            ("user_id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));

        {
            let mut u = users.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("user_id".to_string(), ColumnValue::Int32(1));
            row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            u.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("user_id".to_string(), ColumnValue::Int32(2));
            row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            u.append_row(row2).unwrap();

            let mut row3 = HashMap::new();
            row3.insert("user_id".to_string(), ColumnValue::Int32(3));
            row3.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
            u.append_row(row3).unwrap();
        }

        // Create orders table
        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
            ("amount".to_string(), ColumnType::Float64, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));

        {
            let mut o = orders.borrow_mut();
            // Order for Alice
            let mut row1 = HashMap::new();
            row1.insert("order_id".to_string(), ColumnValue::Int32(101));
            row1.insert("user_id".to_string(), ColumnValue::Int32(1));
            row1.insert("amount".to_string(), ColumnValue::Float64(99.99));
            o.append_row(row1).unwrap();

            // Another order for Alice
            let mut row2 = HashMap::new();
            row2.insert("order_id".to_string(), ColumnValue::Int32(102));
            row2.insert("user_id".to_string(), ColumnValue::Int32(1));
            row2.insert("amount".to_string(), ColumnValue::Float64(49.99));
            o.append_row(row2).unwrap();

            // Order for Charlie
            let mut row3 = HashMap::new();
            row3.insert("order_id".to_string(), ColumnValue::Int32(103));
            row3.insert("user_id".to_string(), ColumnValue::Int32(3));
            row3.insert("amount".to_string(), ColumnValue::Float64(199.99));
            o.append_row(row3).unwrap();

            // Bob has no orders
        }

        // Left join users with orders
        let joined = JoinView::new(
            "user_orders".to_string(),
            users.clone(),
            orders.clone(),
            "user_id".to_string(),
            "user_id".to_string(),
            JoinType::Left,
        )
        .unwrap();

        // Should have 4 rows: Alice (2 orders), Bob (0 orders = 1 row with nulls), Charlie (1 order)
        assert_eq!(joined.len(), 4);

        // Check Alice's first order
        let row0 = joined.get_row(0).unwrap();
        assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(row0.get("right_order_id").unwrap().as_i32(), Some(101));
        assert_eq!(row0.get("right_amount").unwrap().as_f64(), Some(99.99));

        // Check Alice's second order
        let row1 = joined.get_row(1).unwrap();
        assert_eq!(row1.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(row1.get("right_order_id").unwrap().as_i32(), Some(102));

        // Check Bob (no orders - should have nulls)
        let row2 = joined.get_row(2).unwrap();
        assert_eq!(row2.get("name").unwrap().as_string(), Some("Bob"));
        assert!(row2.get("right_order_id").unwrap().is_null());
        assert!(row2.get("right_amount").unwrap().is_null());

        // Check Charlie's order
        let row3 = joined.get_row(3).unwrap();
        assert_eq!(row3.get("name").unwrap().as_string(), Some("Charlie"));
        assert_eq!(row3.get("right_order_id").unwrap().as_i32(), Some(103));
    }

    #[test]
    fn test_inner_join() {
        // Create same tables as left join test
        let users_schema = Schema::new(vec![
            ("user_id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));

        {
            let mut u = users.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("user_id".to_string(), ColumnValue::Int32(1));
            row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            u.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("user_id".to_string(), ColumnValue::Int32(2));
            row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            u.append_row(row2).unwrap();
        }

        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
            ("amount".to_string(), ColumnType::Float64, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));

        {
            let mut o = orders.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("order_id".to_string(), ColumnValue::Int32(101));
            row1.insert("user_id".to_string(), ColumnValue::Int32(1));
            row1.insert("amount".to_string(), ColumnValue::Float64(99.99));
            o.append_row(row1).unwrap();
        }

        // Inner join - only Alice should appear (Bob has no orders)
        let joined = JoinView::new(
            "user_orders".to_string(),
            users.clone(),
            orders.clone(),
            "user_id".to_string(),
            "user_id".to_string(),
            JoinType::Inner,
        )
        .unwrap();

        assert_eq!(joined.len(), 1);

        let row = joined.get_row(0).unwrap();
        assert_eq!(row.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(row.get("right_order_id").unwrap().as_i32(), Some(101));
    }

    #[test]
    fn test_join_refresh() {
        let users_schema = Schema::new(vec![
            ("user_id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));

        {
            let mut u = users.borrow_mut();
            let mut row = HashMap::new();
            row.insert("user_id".to_string(), ColumnValue::Int32(1));
            row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            u.append_row(row).unwrap();
        }

        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));

        let mut joined = JoinView::new(
            "user_orders".to_string(),
            users.clone(),
            orders.clone(),
            "user_id".to_string(),
            "user_id".to_string(),
            JoinType::Left,
        )
        .unwrap();

        // Initially, Alice has no orders (left join shows 1 row with nulls)
        assert_eq!(joined.len(), 1);

        // Add an order for Alice
        {
            let mut o = orders.borrow_mut();
            let mut row = HashMap::new();
            row.insert("order_id".to_string(), ColumnValue::Int32(101));
            row.insert("user_id".to_string(), ColumnValue::Int32(1));
            o.append_row(row).unwrap();
        }

        // Before refresh, still shows old data
        assert_eq!(joined.len(), 1);

        // After refresh, should show the new order
        joined.refresh();
        assert_eq!(joined.len(), 1);

        let row = joined.get_row(0).unwrap();
        assert_eq!(row.get("right_order_id").unwrap().as_i32(), Some(101));
    }

    // === Incremental Propagation Tests ===

    #[test]
    fn test_filter_view_incremental_insert() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        // Add initial rows
        {
            let mut t = table.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("id".to_string(), ColumnValue::Int32(1));
            row1.insert("value".to_string(), ColumnValue::Int32(10));
            t.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("id".to_string(), ColumnValue::Int32(2));
            row2.insert("value".to_string(), ColumnValue::Int32(30));
            t.append_row(row2).unwrap();
        }

        // Create filter view: value > 20
        let mut view = FilterView::new(
            "filtered".to_string(),
            table.clone(),
            |row| {
                if let Some(ColumnValue::Int32(v)) = row.get("value") {
                    *v > 20
                } else {
                    false
                }
            },
        );

        // Clear initial changes so we can test incremental
        table.borrow_mut().clear_changeset();

        assert_eq!(view.len(), 1); // Only row with value=30

        // Add a new row that matches the filter
        {
            let mut t = table.borrow_mut();
            let mut row3 = HashMap::new();
            row3.insert("id".to_string(), ColumnValue::Int32(3));
            row3.insert("value".to_string(), ColumnValue::Int32(50));
            t.append_row(row3).unwrap();
        }

        // Use incremental sync
        let changed = view.sync();
        assert!(changed);
        assert_eq!(view.len(), 2);
        assert_eq!(view.get_value(1, "id").unwrap().as_i32(), Some(3));

        // Add a row that doesn't match the filter
        table.borrow_mut().clear_changeset();
        {
            let mut t = table.borrow_mut();
            let mut row4 = HashMap::new();
            row4.insert("id".to_string(), ColumnValue::Int32(4));
            row4.insert("value".to_string(), ColumnValue::Int32(15)); // < 20
            t.append_row(row4).unwrap();
        }

        view.sync();
        assert_eq!(view.len(), 2); // Still 2, new row didn't match
    }

    #[test]
    fn test_filter_view_incremental_delete() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        // Add rows
        {
            let mut t = table.borrow_mut();
            for i in 1..=5 {
                let mut row = HashMap::new();
                row.insert("id".to_string(), ColumnValue::Int32(i));
                row.insert("value".to_string(), ColumnValue::Int32(i * 10));
                t.append_row(row).unwrap();
            }
        }

        // Filter: value > 20 (rows 3, 4, 5)
        let mut view = FilterView::new(
            "filtered".to_string(),
            table.clone(),
            |row| {
                if let Some(ColumnValue::Int32(v)) = row.get("value") {
                    *v > 20
                } else {
                    false
                }
            },
        );

        table.borrow_mut().clear_changeset();
        assert_eq!(view.len(), 3);

        // Delete row at index 2 (id=3, value=30) - this is in the filter
        {
            table.borrow_mut().delete_row(2).unwrap();
        }

        view.sync();
        assert_eq!(view.len(), 2); // Now only rows 4 and 5 remain
    }

    #[test]
    fn test_filter_view_incremental_update() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("id".to_string(), ColumnValue::Int32(1));
            row1.insert("value".to_string(), ColumnValue::Int32(10)); // < 20
            t.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("id".to_string(), ColumnValue::Int32(2));
            row2.insert("value".to_string(), ColumnValue::Int32(30)); // > 20
            t.append_row(row2).unwrap();
        }

        let mut view = FilterView::new(
            "filtered".to_string(),
            table.clone(),
            |row| {
                if let Some(ColumnValue::Int32(v)) = row.get("value") {
                    *v > 20
                } else {
                    false
                }
            },
        );

        table.borrow_mut().clear_changeset();
        assert_eq!(view.len(), 1);

        // Update row 0's value to 25 (now matches filter)
        {
            table.borrow_mut().set_value(0, "value", ColumnValue::Int32(25)).unwrap();
        }

        view.sync();
        assert_eq!(view.len(), 2); // Both rows now match

        // Update row 1's value to 15 (no longer matches filter)
        table.borrow_mut().clear_changeset();
        {
            table.borrow_mut().set_value(1, "value", ColumnValue::Int32(15)).unwrap();
        }

        view.sync();
        assert_eq!(view.len(), 1); // Only row 0 matches now
    }

    #[test]
    fn test_table_changeset_tracking() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
        ]);

        let mut table = Table::new("test".to_string(), schema);

        // Initially no changes
        assert!(!table.has_pending_changes());
        assert_eq!(table.changeset_generation(), 0);

        // Append creates a change
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        table.append_row(row).unwrap();

        assert!(table.has_pending_changes());
        assert_eq!(table.changeset().len(), 1);

        // Clear changeset
        table.clear_changeset();
        assert!(!table.has_pending_changes());
        assert_eq!(table.changeset_generation(), 1);

        // Update creates a change
        table.set_value(0, "id", ColumnValue::Int32(2)).unwrap();
        assert!(table.has_pending_changes());

        // Drain returns changes and increments generation
        let changes = table.drain_changes();
        assert_eq!(changes.len(), 1);
        assert!(!table.has_pending_changes());
        assert_eq!(table.changeset_generation(), 2);
    }

    // === SortedView Tests ===

    #[test]
    fn test_sorted_view_basic() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
            row1.insert("score".to_string(), ColumnValue::Int32(75));
            t.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            row2.insert("score".to_string(), ColumnValue::Int32(92));
            t.append_row(row2).unwrap();

            let mut row3 = HashMap::new();
            row3.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            row3.insert("score".to_string(), ColumnValue::Int32(85));
            t.append_row(row3).unwrap();
        }

        // Sort by name ascending
        let sorted = SortedView::new(
            "by_name".to_string(),
            table.clone(),
            vec![SortKey::ascending("name")],
        ).unwrap();

        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Alice"));
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Bob"));
        assert_eq!(sorted.get_value(2, "name").unwrap().as_string(), Some("Charlie"));
    }

    #[test]
    fn test_sorted_view_descending() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row1 = HashMap::new();
            row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            row1.insert("score".to_string(), ColumnValue::Int32(75));
            t.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            row2.insert("score".to_string(), ColumnValue::Int32(92));
            t.append_row(row2).unwrap();

            let mut row3 = HashMap::new();
            row3.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
            row3.insert("score".to_string(), ColumnValue::Int32(85));
            t.append_row(row3).unwrap();
        }

        // Sort by score descending (highest first)
        let sorted = SortedView::new(
            "by_score_desc".to_string(),
            table.clone(),
            vec![SortKey::descending("score")],
        ).unwrap();

        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted.get_value(0, "score").unwrap().as_i32(), Some(92)); // Bob
        assert_eq!(sorted.get_value(1, "score").unwrap().as_i32(), Some(85)); // Charlie
        assert_eq!(sorted.get_value(2, "score").unwrap().as_i32(), Some(75)); // Alice
    }

    #[test]
    fn test_sorted_view_multi_column() {
        let schema = Schema::new(vec![
            ("department".to_string(), ColumnType::String, false),
            ("name".to_string(), ColumnType::String, false),
            ("salary".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("employees".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            // Engineering - Alice
            let mut row = HashMap::new();
            row.insert("department".to_string(), ColumnValue::String("Engineering".to_string()));
            row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            row.insert("salary".to_string(), ColumnValue::Int32(100000));
            t.append_row(row).unwrap();

            // Sales - Bob
            let mut row = HashMap::new();
            row.insert("department".to_string(), ColumnValue::String("Sales".to_string()));
            row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            row.insert("salary".to_string(), ColumnValue::Int32(80000));
            t.append_row(row).unwrap();

            // Engineering - Charlie
            let mut row = HashMap::new();
            row.insert("department".to_string(), ColumnValue::String("Engineering".to_string()));
            row.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
            row.insert("salary".to_string(), ColumnValue::Int32(90000));
            t.append_row(row).unwrap();

            // Sales - Diana
            let mut row = HashMap::new();
            row.insert("department".to_string(), ColumnValue::String("Sales".to_string()));
            row.insert("name".to_string(), ColumnValue::String("Diana".to_string()));
            row.insert("salary".to_string(), ColumnValue::Int32(85000));
            t.append_row(row).unwrap();
        }

        // Sort by department (asc), then by salary (desc)
        let sorted = SortedView::new(
            "by_dept_salary".to_string(),
            table.clone(),
            vec![
                SortKey::ascending("department"),
                SortKey::descending("salary"),
            ],
        ).unwrap();

        assert_eq!(sorted.len(), 4);

        // Engineering first (Alice 100k, then Charlie 90k)
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Alice"));
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Charlie"));

        // Sales second (Diana 85k, then Bob 80k)
        assert_eq!(sorted.get_value(2, "name").unwrap().as_string(), Some("Diana"));
        assert_eq!(sorted.get_value(3, "name").unwrap().as_string(), Some("Bob"));
    }

    #[test]
    fn test_sorted_view_with_nulls() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
            ("age".to_string(), ColumnType::Int32, true), // nullable
        ]);

        let table = Rc::new(RefCell::new(Table::new("people".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            row.insert("age".to_string(), ColumnValue::Int32(30));
            t.append_row(row).unwrap();

            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            row.insert("age".to_string(), ColumnValue::Null);
            t.append_row(row).unwrap();

            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
            row.insert("age".to_string(), ColumnValue::Int32(25));
            t.append_row(row).unwrap();
        }

        // Sort by age ascending (nulls last by default)
        let sorted = SortedView::new(
            "by_age".to_string(),
            table.clone(),
            vec![SortKey::ascending("age")],
        ).unwrap();

        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Charlie")); // 25
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Alice"));   // 30
        assert_eq!(sorted.get_value(2, "name").unwrap().as_string(), Some("Bob"));     // null

        // Sort by age ascending (nulls first)
        let sorted_nulls_first = SortedView::new(
            "by_age_nulls_first".to_string(),
            table.clone(),
            vec![SortKey::new("age", SortOrder::Ascending, true)],
        ).unwrap();

        assert_eq!(sorted_nulls_first.get_value(0, "name").unwrap().as_string(), Some("Bob"));      // null
        assert_eq!(sorted_nulls_first.get_value(1, "name").unwrap().as_string(), Some("Charlie")); // 25
        assert_eq!(sorted_nulls_first.get_value(2, "name").unwrap().as_string(), Some("Alice"));   // 30
    }

    #[test]
    fn test_sorted_view_incremental_insert() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(85));
            t.append_row(row).unwrap();

            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Diana".to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(95));
            t.append_row(row).unwrap();
        }

        let mut sorted = SortedView::new(
            "by_name".to_string(),
            table.clone(),
            vec![SortKey::ascending("name")],
        ).unwrap();

        table.borrow_mut().clear_changeset();
        assert_eq!(sorted.len(), 2);
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Bob"));
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Diana"));

        // Add Alice (should go first alphabetically)
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(92));
            t.append_row(row).unwrap();
        }

        let changed = sorted.sync();
        assert!(changed);
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Alice"));
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Bob"));
        assert_eq!(sorted.get_value(2, "name").unwrap().as_string(), Some("Diana"));

        // Add Charlie (should go between Bob and Diana)
        table.borrow_mut().clear_changeset();
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(80));
            t.append_row(row).unwrap();
        }

        sorted.sync();
        assert_eq!(sorted.len(), 4);
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Alice"));
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Bob"));
        assert_eq!(sorted.get_value(2, "name").unwrap().as_string(), Some("Charlie"));
        assert_eq!(sorted.get_value(3, "name").unwrap().as_string(), Some("Diana"));
    }

    #[test]
    fn test_sorted_view_incremental_delete() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            for (name, score) in [("Alice", 92), ("Bob", 85), ("Charlie", 80), ("Diana", 95)] {
                let mut row = HashMap::new();
                row.insert("name".to_string(), ColumnValue::String(name.to_string()));
                row.insert("score".to_string(), ColumnValue::Int32(score));
                t.append_row(row).unwrap();
            }
        }

        let mut sorted = SortedView::new(
            "by_name".to_string(),
            table.clone(),
            vec![SortKey::ascending("name")],
        ).unwrap();

        table.borrow_mut().clear_changeset();
        assert_eq!(sorted.len(), 4);

        // Delete Bob (parent index 1)
        table.borrow_mut().delete_row(1).unwrap();

        sorted.sync();
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Alice"));
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Charlie"));
        assert_eq!(sorted.get_value(2, "name").unwrap().as_string(), Some("Diana"));
    }

    #[test]
    fn test_sorted_view_incremental_update() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(70));
            t.append_row(row).unwrap();

            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(80));
            t.append_row(row).unwrap();

            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(90));
            t.append_row(row).unwrap();
        }

        // Sort by score ascending
        let mut sorted = SortedView::new(
            "by_score".to_string(),
            table.clone(),
            vec![SortKey::ascending("score")],
        ).unwrap();

        table.borrow_mut().clear_changeset();

        // Initial order: Alice (70), Bob (80), Charlie (90)
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Alice"));
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Bob"));
        assert_eq!(sorted.get_value(2, "name").unwrap().as_string(), Some("Charlie"));

        // Update Alice's score to 95 (should move to end)
        table.borrow_mut().set_value(0, "score", ColumnValue::Int32(95)).unwrap();

        sorted.sync();

        // New order: Bob (80), Charlie (90), Alice (95)
        assert_eq!(sorted.get_value(0, "name").unwrap().as_string(), Some("Bob"));
        assert_eq!(sorted.get_value(1, "name").unwrap().as_string(), Some("Charlie"));
        assert_eq!(sorted.get_value(2, "name").unwrap().as_string(), Some("Alice"));
    }

    #[test]
    fn test_sorted_view_parent_index() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::Int32, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            // Parent indices: 0=100, 1=50, 2=75
            for (id, value) in [(1, 100), (2, 50), (3, 75)] {
                let mut row = HashMap::new();
                row.insert("id".to_string(), ColumnValue::Int32(id));
                row.insert("value".to_string(), ColumnValue::Int32(value));
                t.append_row(row).unwrap();
            }
        }

        let sorted = SortedView::new(
            "by_value".to_string(),
            table.clone(),
            vec![SortKey::ascending("value")],
        ).unwrap();

        // Sorted order by value: 50 (parent 1), 75 (parent 2), 100 (parent 0)
        assert_eq!(sorted.get_parent_index(0), Some(1)); // 50
        assert_eq!(sorted.get_parent_index(1), Some(2)); // 75
        assert_eq!(sorted.get_parent_index(2), Some(0)); // 100
        assert_eq!(sorted.get_parent_index(3), None);    // out of range
    }

    #[test]
    fn test_sorted_view_empty_table() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("empty".to_string(), schema)));

        let sorted = SortedView::new(
            "sorted_empty".to_string(),
            table.clone(),
            vec![SortKey::ascending("name")],
        ).unwrap();

        assert_eq!(sorted.len(), 0);
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_sorted_view_invalid_column() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        let result = SortedView::new(
            "invalid".to_string(),
            table.clone(),
            vec![SortKey::ascending("nonexistent")],
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_sorted_view_no_sort_keys() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        let result = SortedView::new(
            "invalid".to_string(),
            table.clone(),
            vec![],
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("At least one sort key"));
    }

    // === Changeset Compaction Tests ===

    #[test]
    fn test_filter_view_sync_incremental_with_cursor() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::Int32, false),
        ]);
        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        // Add initial rows
        {
            let mut t = table.borrow_mut();
            for i in 0..3 {
                let mut row = HashMap::new();
                row.insert("id".to_string(), ColumnValue::Int32(i));
                row.insert("value".to_string(), ColumnValue::Int32(i * 10));
                t.append_row(row).unwrap();
            }
        }

        let mut view = FilterView::new(
            "filtered".to_string(),
            table.clone(),
            |row| {
                row.get("value")
                    .and_then(|v| v.as_i32())
                    .map(|v| v >= 10)
                    .unwrap_or(false)
            },
        );

        assert_eq!(view.len(), 2); // rows with value 10, 20
        let initial_cursor = view.last_processed_change_count();
        assert_eq!(initial_cursor, 3); // Processed 3 initial inserts

        // Add a new matching row
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(3));
            row.insert("value".to_string(), ColumnValue::Int32(30));
            t.append_row(row).unwrap();
        }

        // Sync should process only the new change
        let modified = view.sync();
        assert!(modified);
        assert_eq!(view.len(), 3);
        assert_eq!(view.last_processed_change_count(), 4);
    }

    #[test]
    fn test_filter_view_sync_fallback_to_rebuild() {
        let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);
        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        // Add rows
        {
            let mut t = table.borrow_mut();
            for i in 0..3 {
                let mut row = HashMap::new();
                row.insert("id".to_string(), ColumnValue::Int32(i));
                t.append_row(row).unwrap();
            }
        }

        let mut view = FilterView::new("all".to_string(), table.clone(), |_| true);
        assert_eq!(view.len(), 3);

        // Compact all changes away
        table.borrow_mut().compact_changeset(100);

        // Add more rows
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(10));
            t.append_row(row).unwrap();
        }

        // Sync should fallback to rebuild (returns true)
        let modified = view.sync();
        assert!(modified);
        assert_eq!(view.len(), 4);
    }

    #[test]
    fn test_sorted_view_sync_with_cursor() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);
        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        // Add initial rows
        {
            let mut t = table.borrow_mut();
            for (id, score) in [(1, 50), (2, 30), (3, 70)] {
                let mut row = HashMap::new();
                row.insert("id".to_string(), ColumnValue::Int32(id));
                row.insert("score".to_string(), ColumnValue::Int32(score));
                t.append_row(row).unwrap();
            }
        }

        let mut view = SortedView::new(
            "sorted".to_string(),
            table.clone(),
            vec![SortKey::descending("score")],
        )
        .unwrap();

        assert_eq!(view.len(), 3);
        assert_eq!(view.get_value(0, "score").unwrap().as_i32(), Some(70));
        let initial_cursor = view.last_processed_change_count();

        // Add a new highest score
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(4));
            row.insert("score".to_string(), ColumnValue::Int32(100));
            t.append_row(row).unwrap();
        }

        let modified = view.sync();
        assert!(modified);
        assert_eq!(view.len(), 4);
        assert_eq!(view.get_value(0, "score").unwrap().as_i32(), Some(100));
        assert!(view.last_processed_change_count() > initial_cursor);
    }

    #[test]
    fn test_sorted_view_sync_fallback_to_rebuild() {
        let schema = Schema::new(vec![("value".to_string(), ColumnType::Int32, false)]);
        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            for v in [30, 10, 20] {
                let mut row = HashMap::new();
                row.insert("value".to_string(), ColumnValue::Int32(v));
                t.append_row(row).unwrap();
            }
        }

        let mut view = SortedView::new(
            "sorted".to_string(),
            table.clone(),
            vec![SortKey::ascending("value")],
        )
        .unwrap();

        assert_eq!(view.get_value(0, "value").unwrap().as_i32(), Some(10));

        // Compact changes away
        table.borrow_mut().compact_changeset(100);

        // Add new row
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("value".to_string(), ColumnValue::Int32(5));
            t.append_row(row).unwrap();
        }

        // Sync falls back to rebuild
        let modified = view.sync();
        assert!(modified);
        assert_eq!(view.len(), 4);
        assert_eq!(view.get_value(0, "value").unwrap().as_i32(), Some(5));
    }

    #[test]
    fn test_multiple_syncs_accumulate_correctly() {
        let schema = Schema::new(vec![("value".to_string(), ColumnType::Int32, false)]);
        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("value".to_string(), ColumnValue::Int32(10));
            t.append_row(row).unwrap();
        }

        let mut view = FilterView::new("all".to_string(), table.clone(), |_| true);
        assert_eq!(view.len(), 1);
        assert_eq!(view.last_processed_change_count(), 1);

        // Add row and sync
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("value".to_string(), ColumnValue::Int32(20));
            t.append_row(row).unwrap();
        }
        view.sync();
        assert_eq!(view.len(), 2);
        assert_eq!(view.last_processed_change_count(), 2);

        // Add another row and sync again
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert("value".to_string(), ColumnValue::Int32(30));
            t.append_row(row).unwrap();
        }
        view.sync();
        assert_eq!(view.len(), 3);
        assert_eq!(view.last_processed_change_count(), 3);
    }

    #[test]
    fn test_view_sync_after_partial_compaction() {
        let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);
        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        // Add 5 rows
        {
            let mut t = table.borrow_mut();
            for i in 0..5 {
                let mut row = HashMap::new();
                row.insert("id".to_string(), ColumnValue::Int32(i));
                t.append_row(row).unwrap();
            }
        }

        let mut view = FilterView::new("all".to_string(), table.clone(), |_| true);
        assert_eq!(view.len(), 5);
        assert_eq!(view.last_processed_change_count(), 5);

        // Compact first 3 changes (view has already processed them)
        table.borrow_mut().compact_changeset(3);

        // Add more rows
        {
            let mut t = table.borrow_mut();
            for i in 5..8 {
                let mut row = HashMap::new();
                row.insert("id".to_string(), ColumnValue::Int32(i));
                t.append_row(row).unwrap();
            }
        }

        // Sync should still work because view's cursor (5) >= base_index (3)
        let modified = view.sync();
        assert!(modified);
        assert_eq!(view.len(), 8);
        assert_eq!(view.last_processed_change_count(), 8);
    }
}
