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
}

impl FilterView {
    pub fn new<F>(name: String, parent: Rc<RefCell<Table>>, predicate: F) -> Self
    where
        F: Fn(&HashMap<String, ColumnValue>) -> bool + 'static,
    {
        let generation = parent.borrow().changeset_generation();
        let mut view = FilterView {
            name,
            parent,
            predicate: Box::new(predicate),
            view_to_parent: Vec::new(),
            last_synced_generation: generation,
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
        let changes = parent.changeset().changes();

        if changes.is_empty() {
            return false;
        }

        // Clone changes so we can drop the borrow
        let changes: Vec<TableChange> = changes.to_vec();
        drop(parent);

        self.apply_changes(&changes)
    }

    pub fn name(&self) -> &str {
        &self.name
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
        };

        view.rebuild_index();
        Ok(view)
    }

    /// Build a composite key string from multiple column values.
    /// Returns None if any key column is missing or contains NULL (SQL semantics: NULL != NULL)
    fn build_composite_key(row: &HashMap<String, ColumnValue>, keys: &[String]) -> Option<String> {
        let mut parts: Vec<String> = Vec::with_capacity(keys.len());
        for key in keys {
            match row.get(key) {
                Some(ColumnValue::Null) => return None, // NULL doesn't match anything
                Some(value) => parts.push(format!("{:?}", value)),
                None => return None, // Missing key column
            }
        }
        Some(parts.join("\x00")) // Use null byte as separator
    }

    /// Rebuilds the join index by scanning both tables
    fn rebuild_index(&mut self) {
        self.join_index.clear();

        let left = self.left_table.borrow();
        let right = self.right_table.borrow();

        // Build a hashmap of right table values for efficient lookup
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();

        for i in 0..right.len() {
            if let Ok(row) = right.get_row(i) {
                if let Some(key_str) = Self::build_composite_key(&row, &self.right_keys) {
                    right_index.entry(key_str).or_insert_with(Vec::new).push(i);
                }
            }
        }

        // For each left row, find matching right rows
        for i in 0..left.len() {
            if let Ok(row) = left.get_row(i) {
                if let Some(key_str) = Self::build_composite_key(&row, &self.left_keys) {
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
        }

        // Update generation trackers
        self.left_last_synced = left.changeset_generation();
        self.right_last_synced = right.changeset_generation();
    }

    /// Build a lookup map from right table for efficient join operations
    fn build_right_lookup(&self) -> HashMap<String, Vec<usize>> {
        let right = self.right_table.borrow();
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();

        for i in 0..right.len() {
            if let Ok(row) = right.get_row(i) {
                if let Some(key_str) = Self::build_composite_key(&row, &self.right_keys) {
                    right_index.entry(key_str).or_insert_with(Vec::new).push(i);
                }
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

    /// Incrementally sync with both parent tables' changes
    /// Returns true if any changes were applied
    ///
    /// Note: For complex changes (deletes, updates to join keys), this falls back
    /// to a full rebuild. Only appends are handled incrementally.
    pub fn sync(&mut self) -> bool {
        let left_changes: Vec<TableChange> = self.left_table.borrow().changeset().changes().to_vec();
        let right_changes: Vec<TableChange> = self.right_table.borrow().changeset().changes().to_vec();

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

        // Handle left table inserts
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
                    let right_lookup = self.build_right_lookup();

                    if let Some(matching_indices) = right_lookup.get(&key_str) {
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

                    for left_idx in 0..left.len() {
                        if let Ok(left_row) = left.get_row(left_idx) {
                            if let Some(left_key_str) = Self::build_composite_key(&left_row, &self.left_keys) {
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
        let mut view = SortedView {
            name,
            parent,
            sort_keys,
            sorted_index: Vec::new(),
            last_synced_generation: generation,
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

        // Sort by collecting values for comparison
        let sort_keys = self.sort_keys.clone();
        self.sorted_index.sort_by(|&a, &b| {
            for key in &sort_keys {
                let val_a = table.get_value(a, &key.column).ok();
                let val_b = table.get_value(b, &key.column).ok();

                let cmp = Self::compare_values(&val_a, &val_b, key);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        self.last_synced_generation = table.changeset_generation();
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
        let changes: Vec<TableChange> = self.parent.borrow().changeset().changes().to_vec();

        if changes.is_empty() {
            return false;
        }

        self.apply_changes(&changes)
    }

    /// Returns the name of this view
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the sort keys
    pub fn sort_keys(&self) -> &[SortKey] {
        &self.sort_keys
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
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
}

impl ColumnAggState {
    fn new() -> Self {
        ColumnAggState {
            sum: 0.0,
            count: 0,
            min: None,
            max: None,
        }
    }

    /// Add a numeric value to the aggregate state
    fn add_value(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
    }

    /// Remove a numeric value from the aggregate state
    /// Returns false if MIN/MAX needs recalculation (deleted value was min or max)
    fn remove_value(&mut self, value: f64) -> bool {
        self.sum -= value;
        self.count = self.count.saturating_sub(1);

        // Check if we need to recalculate min/max
        let needs_recalc = self.min.map_or(false, |m| (m - value).abs() < f64::EPSILON)
            || self.max.map_or(false, |m| (m - value).abs() < f64::EPSILON);

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
}

impl GroupState {
    fn new() -> Self {
        GroupState {
            column_stats: HashMap::new(),
            row_indices: HashSet::new(),
        }
    }

    /// Add a value for a specific source column
    fn add_column_value(&mut self, source_col: &str, value: f64) {
        let stats = self.column_stats
            .entry(source_col.to_string())
            .or_insert_with(ColumnAggState::new);
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
                    if v.is_null() {
                        None
                    } else {
                        Some(format!("{:?}", v))
                    }
                })
            })
            .collect();
        GroupKey(values)
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
                            // Parse the debug string back - this is a simplification
                            // In production, we'd store actual ColumnValues
                            match col_type {
                                crate::column::ColumnType::String => {
                                    // Extract string from String("value") format
                                    if s.starts_with("String(\"") && s.ends_with("\")") {
                                        let inner = &s[8..s.len()-2];
                                        ColumnValue::String(inner.to_string())
                                    } else {
                                        ColumnValue::String(s.clone())
                                    }
                                }
                                crate::column::ColumnType::Int32 => {
                                    if s.starts_with("Int32(") && s.ends_with(")") {
                                        let inner = &s[6..s.len()-1];
                                        inner.parse().map(ColumnValue::Int32).unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                crate::column::ColumnType::Int64 => {
                                    if s.starts_with("Int64(") && s.ends_with(")") {
                                        let inner = &s[6..s.len()-1];
                                        inner.parse().map(ColumnValue::Int64).unwrap_or(ColumnValue::Null)
                                    } else {
                                        ColumnValue::Null
                                    }
                                }
                                _ => ColumnValue::String(s.clone()),
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
            (p.changeset_generation(), p.changeset().changes().len())
        };
        let mut view = AggregateView {
            name,
            parent,
            group_by_columns,
            aggregations,
            groups: HashMap::new(),
            group_order: Vec::new(),
            row_to_group: HashMap::new(),
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

        // Collect rows first to avoid borrow conflicts
        let (rows, generation, change_count) = {
            let parent = self.parent.borrow();
            let mut rows = Vec::new();
            for row_idx in 0..parent.len() {
                if let Ok(row) = parent.get_row(row_idx) {
                    rows.push((row_idx, row));
                }
            }
            (rows, parent.changeset_generation(), parent.changeset().changes().len())
        };

        // Now process the rows
        for (row_idx, row) in rows {
            self.add_row_to_aggregates(row_idx, &row);
        }

        self.last_synced_generation = generation;
        self.last_processed_change_count = change_count;
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
        let state = self.groups.entry(key.clone()).or_insert_with(GroupState::new);

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
            row_indices
                .iter()
                .filter_map(|&row_idx| {
                    parent.get_row(row_idx).ok().and_then(|row| {
                        row.get(source_col).and_then(|value| Self::extract_numeric(value))
                    })
                })
                .collect()
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
        let all_changes = parent.changeset().changes();

        // Only process changes we haven't seen yet
        if all_changes.len() <= self.last_processed_change_count {
            return false;
        }

        let new_changes: Vec<TableChange> = all_changes[self.last_processed_change_count..].to_vec();
        let new_count = all_changes.len();
        drop(parent);

        let modified = self.apply_changes(&new_changes);
        self.last_processed_change_count = new_count;
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
}
