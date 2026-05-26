use crate::changeset::{
    apply_filter_cell_updated, apply_filter_row_deleted, apply_filter_row_inserted,
    IncrementalView, TableChange,
};
/// LiveTable View Implementation
///
/// Views are read-only derived tables that automatically propagate changes from parent tables.
/// This is a simplified implementation focusing on core functionality.
use crate::column::ColumnValue;
use crate::table::Table;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

type RowPredicate = dyn Fn(&HashMap<String, ColumnValue>) -> bool;
type ComputeFunction = dyn Fn(&HashMap<String, ColumnValue>) -> ColumnValue;

/// Typed, hashable composite-key component for joins.
///
/// Replaces the previous String-based serialization (which used `format!("{:?}", v)`
/// for non-Int/String variants — not a stable format — and `\x00` separators,
/// which collided when string data contained `\x00`).
///
/// Float variants store IEEE 754 bit patterns so `Eq`/`Hash` derive cleanly.
/// Per SQL semantics, NaN never equals anything: builders return `None` for any
/// NaN-bearing row, which excludes it from joins.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum JoinKeyPart {
    Int32(i32),
    Int64(i64),
    Float32Bits(u32),
    Float64Bits(u64),
    String(String),
    Bool(bool),
    Date(i32),
    DateTime(i64),
}

type JoinKey = Vec<JoinKeyPart>;

/// Convert a single ColumnValue into a JoinKeyPart. Returns None if the value
/// cannot participate in equality-based joins (NULL or NaN floats).
fn column_value_to_join_key_part(value: &ColumnValue) -> Option<JoinKeyPart> {
    match value {
        ColumnValue::Null => None,
        ColumnValue::Int32(v) => Some(JoinKeyPart::Int32(*v)),
        ColumnValue::Int64(v) => Some(JoinKeyPart::Int64(*v)),
        ColumnValue::Float32(v) if v.is_nan() => None,
        ColumnValue::Float32(v) => Some(JoinKeyPart::Float32Bits(v.to_bits())),
        ColumnValue::Float64(v) if v.is_nan() => None,
        ColumnValue::Float64(v) => Some(JoinKeyPart::Float64Bits(v.to_bits())),
        ColumnValue::String(s) => Some(JoinKeyPart::String(s.clone())),
        ColumnValue::Bool(b) => Some(JoinKeyPart::Bool(*b)),
        ColumnValue::Date(d) => Some(JoinKeyPart::Date(*d)),
        ColumnValue::DateTime(dt) => Some(JoinKeyPart::DateTime(*dt)),
    }
}

/// Same as `column_value_to_join_key_part` but consumes an owned value
/// (saves a String clone on the string fast path).
fn column_value_into_join_key_part(value: ColumnValue) -> Option<JoinKeyPart> {
    match value {
        ColumnValue::Null => None,
        ColumnValue::Int32(v) => Some(JoinKeyPart::Int32(v)),
        ColumnValue::Int64(v) => Some(JoinKeyPart::Int64(v)),
        ColumnValue::Float32(v) if v.is_nan() => None,
        ColumnValue::Float32(v) => Some(JoinKeyPart::Float32Bits(v.to_bits())),
        ColumnValue::Float64(v) if v.is_nan() => None,
        ColumnValue::Float64(v) => Some(JoinKeyPart::Float64Bits(v.to_bits())),
        ColumnValue::String(s) => Some(JoinKeyPart::String(s)),
        ColumnValue::Bool(b) => Some(JoinKeyPart::Bool(b)),
        ColumnValue::Date(d) => Some(JoinKeyPart::Date(d)),
        ColumnValue::DateTime(dt) => Some(JoinKeyPart::DateTime(dt)),
    }
}

/// A FilterView filters rows from the parent table based on a predicate.
/// Maintains a mapping from view indices to parent indices.
///
/// Supports incremental updates: when the parent table changes, the view
/// can efficiently update its index mapping without a full rebuild.
pub struct FilterView {
    name: String,
    parent: Rc<RefCell<Table>>,
    predicate: Box<RowPredicate>,
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
        self.view_to_parent.reserve(parent.len());

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
                    let matched = (self.predicate)(data);
                    if apply_filter_row_inserted(&mut self.view_to_parent, *index, matched) {
                        modified = true;
                    }
                }

                TableChange::RowDeleted { index, .. } => {
                    if apply_filter_row_deleted(&mut self.view_to_parent, *index) {
                        modified = true;
                    }
                }

                TableChange::CellUpdated { row, .. } => {
                    let now_matches = self
                        .parent
                        .borrow()
                        .get_row(*row)
                        .map(|data| (self.predicate)(&data))
                        .unwrap_or(false);
                    if apply_filter_cell_updated(&mut self.view_to_parent, *row, now_matches) {
                        modified = true;
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
    pub fn new(
        name: String,
        parent: Rc<RefCell<Table>>,
        columns: Vec<String>,
    ) -> Result<Self, String> {
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
        let mut result = HashMap::with_capacity(self.selected_columns.len());

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
    compute_func: Box<ComputeFunction>,
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
    /// Right join: All rows from right table, matched rows from left (nulls if no match)
    Right,
    /// Full outer join: All rows from both tables (nulls where no match on either side)
    Full,
}

/// A JoinView combines two tables based on matching column values.
///
/// Supports:
/// - Left Join: All rows from left table, matched rows from right (nulls if no match)
/// - Inner Join: Only rows that match in both tables
/// - Right Join: All rows from right table, matched rows from left (nulls if no match)
/// - Full Outer Join: All rows from both tables (nulls where no match)
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
    /// Cached joined rows: (optional_left_row_index, optional_right_row_index)
    /// - INNER: always (Some(left), Some(right))
    /// - LEFT: (Some(left), Some(right)) or (Some(left), None)
    /// - RIGHT: (Some(left), Some(right)) or (None, Some(right))
    /// - FULL: all three patterns
    join_index: Vec<(Option<usize>, Option<usize>)>,
    /// Cached column names from parent schemas — captured at construction so
    /// `get_row` does not clone schemas on every call (schemas are immutable
    /// after Table construction in this crate).
    left_column_names: Vec<String>,
    right_column_names: Vec<String>,
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
        Self::new_multi(
            name,
            left_table,
            right_table,
            vec![left_key],
            vec![right_key],
            join_type,
        )
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

        let left_change_count = left_table.borrow().changeset().total_len();
        let right_change_count = right_table.borrow().changeset().total_len();

        // Snapshot column names once — schemas are immutable post-construction,
        // so we never need to re-read them on each get_row call.
        let left_column_names: Vec<String> = left_table
            .borrow()
            .schema()
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let right_column_names: Vec<String> = right_table
            .borrow()
            .schema()
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut view = JoinView {
            name,
            left_table,
            right_table,
            left_keys,
            right_keys,
            join_type,
            join_index: Vec::new(),
            left_column_names,
            right_column_names,
            left_last_processed_change_count: left_change_count,
            right_last_processed_change_count: right_change_count,
        };

        view.rebuild_index();
        Ok(view)
    }

    /// Build a typed composite key from a HashMap row (used in incremental sync).
    /// Returns None if any key column is missing, NULL, or contains a NaN float
    /// (SQL semantics: NaN never equals anything, so NaN keys can't participate).
    /// IMPORTANT: Output must match build_key_from_indices structurally.
    fn build_composite_key(row: &HashMap<String, ColumnValue>, keys: &[String]) -> Option<JoinKey> {
        let mut parts: Vec<JoinKeyPart> = Vec::with_capacity(keys.len());
        for key in keys {
            let value = row.get(key)?;
            parts.push(column_value_to_join_key_part(value)?);
        }
        Some(parts)
    }

    /// Build a typed composite key from column values at given indices.
    /// Returns None if any key column is NULL or NaN — these rows are excluded
    /// from joins per SQL semantics.
    fn build_key_from_indices(table: &Table, row: usize, col_indices: &[usize]) -> Option<JoinKey> {
        let mut parts: Vec<JoinKeyPart> = Vec::with_capacity(col_indices.len());
        for &col_idx in col_indices {
            let value = table.get_value_by_index(row, col_idx).ok()?;
            parts.push(column_value_into_join_key_part(value)?);
        }
        Some(parts)
    }

    /// Rebuilds the join index by scanning both tables.
    /// Unified 4-phase algorithm for all join types (INNER, LEFT, RIGHT, FULL).
    fn rebuild_index(&mut self) {
        self.join_index.clear();

        let left = self.left_table.borrow();
        let right = self.right_table.borrow();

        // Phase 1: Pre-compute column indices for join keys (done once, not per row)
        let left_col_indices: Vec<usize> = self
            .left_keys
            .iter()
            .filter_map(|k| left.schema().get_column_index(k))
            .collect();
        let right_col_indices: Vec<usize> = self
            .right_keys
            .iter()
            .filter_map(|k| right.schema().get_column_index(k))
            .collect();

        // Phase 2: Build a hashmap of right table values for efficient lookup
        let mut right_index: HashMap<JoinKey, Vec<usize>> = HashMap::new();

        for i in 0..right.len() {
            if let Some(key) = Self::build_key_from_indices(&right, i, &right_col_indices) {
                right_index.entry(key).or_default().push(i);
            }
        }

        // Phase 3: Scan left rows — find matching right rows
        let mut matched_right: HashSet<usize> = HashSet::new();

        for i in 0..left.len() {
            if let Some(key) = Self::build_key_from_indices(&left, i, &left_col_indices) {
                if let Some(matching_indices) = right_index.get(&key) {
                    // Found matches - add each combination
                    for &right_idx in matching_indices {
                        self.join_index.push((Some(i), Some(right_idx)));
                        matched_right.insert(right_idx);
                    }
                } else {
                    // Non-NULL key, no match
                    match self.join_type {
                        JoinType::Left | JoinType::Full => {
                            self.join_index.push((Some(i), None));
                        }
                        JoinType::Inner | JoinType::Right => {
                            // Skip - no match means not included
                        }
                    }
                }
            } else {
                // NULL key — row exists but can never match anything
                match self.join_type {
                    JoinType::Left | JoinType::Full => {
                        self.join_index.push((Some(i), None));
                    }
                    JoinType::Inner | JoinType::Right => {
                        // Skip
                    }
                }
            }
        }

        // Phase 4: (RIGHT/FULL only) Scan right rows for unmatched entries
        if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
            for i in 0..right.len() {
                if !matched_right.contains(&i) {
                    self.join_index.push((None, Some(i)));
                }
            }
        }

        // Update cursor trackers (one per parent)
        self.left_last_processed_change_count = left.changeset().total_len();
        self.right_last_processed_change_count = right.changeset().total_len();
    }

    /// Build a lookup map from right table for efficient join operations
    fn build_right_lookup(&self) -> HashMap<JoinKey, Vec<usize>> {
        let right = self.right_table.borrow();
        let mut right_index: HashMap<JoinKey, Vec<usize>> = HashMap::new();

        // Pre-compute column indices
        let right_col_indices: Vec<usize> = self
            .right_keys
            .iter()
            .filter_map(|k| right.schema().get_column_index(k))
            .collect();

        for i in 0..right.len() {
            if let Some(key) = Self::build_key_from_indices(&right, i, &right_col_indices) {
                right_index.entry(key).or_default().push(i);
            }
        }

        right_index
    }

    /// Mirror of `build_right_lookup` for the left table. Used to make
    /// right-table inserts O(matches) instead of O(left.len()) per insert.
    fn build_left_lookup(&self) -> HashMap<JoinKey, Vec<usize>> {
        let left = self.left_table.borrow();
        let mut left_index: HashMap<JoinKey, Vec<usize>> = HashMap::new();

        let left_col_indices: Vec<usize> = self
            .left_keys
            .iter()
            .filter_map(|k| left.schema().get_column_index(k))
            .collect();

        for i in 0..left.len() {
            if let Some(key) = Self::build_key_from_indices(&left, i, &left_col_indices) {
                left_index.entry(key).or_default().push(i);
            }
        }

        left_index
    }

    /// Binary search for the insertion position of a new (Some(left_idx), _) entry.
    /// `join_index` is partitioned: entries before — Some(el) with el ≤ left_idx;
    /// entries at-or-after — Some(el) with el > left_idx, or None-left.
    /// Was a linear `.iter().position(...)` — O(N); now O(log N).
    fn find_left_insert_position(&self, left_idx: usize) -> usize {
        self.join_index
            .partition_point(|(existing_left, _)| match existing_left {
                Some(el) => *el <= left_idx,
                None => false, // None-left entries are after; not "before our insert"
            })
    }

    /// Binary search for the insertion position of a new (None, Some(right_idx))
    /// orphan entry within the None-left tail. Tail invariant: orphans are
    /// sorted by right_idx ASC (matching rebuild_index's iteration order).
    fn find_orphan_insert_position(&self, right_idx: usize) -> usize {
        self.join_index.partition_point(|(l, r)| match (l, r) {
            (Some(_), _) => true, // All Some(l) entries precede the None-left tail
            (None, Some(r_existing)) => *r_existing < right_idx,
            (None, None) => true, // Defensive; not produced by current code
        })
    }

    /// Binary search for the insertion position of a new (Some(left_idx), Some(right_idx))
    /// entry. Ordering invariant within same left_idx: matched entries (Some right)
    /// sorted by right_idx ASC, then the unmatched (None right) entry if any.
    /// Was a linear scan — O(N); now O(log N).
    fn find_right_insert_position(&self, left_idx: usize, right_idx: usize) -> usize {
        self.join_index
            .partition_point(|(existing_left, existing_right)| match existing_left {
                Some(el) if *el < left_idx => true,
                Some(el) if *el > left_idx => false,
                Some(_) => {
                    // existing_left == left_idx — order by right_idx; None-right is after.
                    match existing_right {
                        Some(existing_right_idx) => *existing_right_idx <= right_idx,
                        None => false,
                    }
                }
                None => false, // None-left entries are after
            })
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

        let (left_idx_opt, right_idx_opt) = self.join_index[index];

        let mut result =
            HashMap::with_capacity(self.left_column_names.len() + self.right_column_names.len());

        // Add all columns from left table (or nulls if no left match)
        if let Some(left_idx) = left_idx_opt {
            let left_row = self.left_table.borrow().get_row(left_idx)?;
            result.extend(left_row);
        } else {
            for col_name in &self.left_column_names {
                result.insert(col_name.clone(), ColumnValue::Null);
            }
        }

        // Add columns from right table (or nulls if no match), prefixing with "right_"
        if let Some(right_idx) = right_idx_opt {
            let right_row = self.right_table.borrow().get_row(right_idx)?;
            for (col_name, value) in right_row {
                result.insert(format!("right_{}", col_name), value);
            }
        } else {
            for col_name in &self.right_column_names {
                result.insert(format!("right_{}", col_name), ColumnValue::Null);
            }
        }

        Ok(result)
    }

    /// Gets a specific value from the joined view
    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        if row >= self.join_index.len() {
            return Err(format!("Row {} out of range [0, {})", row, self.len()));
        }

        let (left_idx_opt, right_idx_opt) = self.join_index[row];
        if let Some(right_column) = column.strip_prefix("right_") {
            if self
                .right_table
                .borrow()
                .schema()
                .get_column_index(right_column)
                .is_none()
            {
                return Err(format!("Column '{}' not found in joined view", column));
            }

            return match right_idx_opt {
                Some(right_idx) => self.right_table.borrow().get_value(right_idx, right_column),
                None => Ok(ColumnValue::Null),
            };
        }

        // Left column
        match left_idx_opt {
            Some(left_idx) => self.left_table.borrow().get_value(left_idx, column),
            None => {
                // Verify the column exists in left schema
                if self
                    .left_table
                    .borrow()
                    .schema()
                    .get_column_index(column)
                    .is_none()
                {
                    return Err(format!("Column '{}' not found in joined view", column));
                }
                Ok(ColumnValue::Null)
            }
        }
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

        // All RowInserted, RowDeleted, and key-column CellUpdated changes on
        // both sides are now handled incrementally below. (Non-key
        // CellUpdated never required join_index changes — views read live
        // data via get_row.) The historical rebuild fallback is gone; only
        // the compaction case above (changes_from returning None) still
        // requires a full rebuild.

        let mut modified = false;

        // Handle left table inserts — build right lookup once, not per insert
        let has_left_inserts = left_changes
            .iter()
            .any(|c| matches!(c, TableChange::RowInserted { .. }));
        let right_lookup = if has_left_inserts {
            Some(self.build_right_lookup())
        } else {
            None
        };

        for change in &left_changes {
            match change {
                TableChange::RowDeleted { index: del_idx, .. } => {
                    // Step 1: capture right indices that were matched by this
                    // left row — needed for RIGHT/FULL orphan handling below.
                    let removed_right_indices: Vec<usize> = self
                        .join_index
                        .iter()
                        .filter_map(|(l, r)| {
                            if *l == Some(*del_idx) {
                                *r
                            } else {
                                None
                            }
                        })
                        .collect();

                    // Step 2: remove all join_index entries pointing at the
                    // deleted left row (matched entries AND LEFT/FULL placeholders).
                    let before_len = self.join_index.len();
                    self.join_index.retain(|(l, _)| *l != Some(*del_idx));
                    if self.join_index.len() != before_len {
                        modified = true;
                    }

                    // Step 3: shift remaining left indices > del_idx down by 1.
                    for (l_opt, _) in self.join_index.iter_mut() {
                        if let Some(l) = l_opt {
                            if *l > *del_idx {
                                *l -= 1;
                            }
                        }
                    }

                    // Step 4: RIGHT/FULL only — any right row that was
                    // previously matched by the deleted left and is no longer
                    // matched by ANY remaining left becomes unmatched: insert
                    // (None, Some(right_idx)) at the sorted position within the
                    // None-left tail to match rebuild_index's ordering.
                    if self.join_type == JoinType::Right
                        || self.join_type == JoinType::Full
                    {
                        for r_idx in removed_right_indices {
                            let still_matched = self
                                .join_index
                                .iter()
                                .any(|(l, r)| l.is_some() && *r == Some(r_idx));
                            if !still_matched {
                                let pos = self.find_orphan_insert_position(r_idx);
                                self.join_index.insert(pos, (None, Some(r_idx)));
                                modified = true;
                            }
                        }
                    }
                }

                TableChange::RowInserted { index, data } => {
                // Tail-insert fast path. join_index is sorted (Some(l) entries
                // first, ascending by l; None-left entries last). The max
                // existing left_idx is the last Some(l) before the None-left
                // tail — found in O(K) by scanning from the end, where K is
                // typically 0 (INNER) or small (LEFT/FULL with few unmatched).
                let max_existing_left =
                    self.join_index.iter().rev().find_map(|(l, _)| *l);
                let needs_shift =
                    max_existing_left.is_some_and(|max_l| max_l >= *index);

                if needs_shift {
                    for (left_idx_opt, _) in self.join_index.iter_mut() {
                        if let Some(left_idx) = left_idx_opt {
                            if *left_idx >= *index {
                                *left_idx += 1;
                            }
                        }
                    }
                }

                // Find matches for the new left row
                if let Some(key) = Self::build_composite_key(data, &self.left_keys) {
                    let lookup = right_lookup.as_ref().unwrap();

                    if let Some(matching_indices) = lookup.get(&key) {
                        // For RIGHT/FULL joins: matched right rows may currently exist
                        // as unmatched entries (None, Some(right_idx)) in the tail section.
                        // Remove those before inserting the proper matched entries.
                        if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
                            for &right_idx in matching_indices {
                                if let Some(pos) = self
                                    .join_index
                                    .iter()
                                    .position(|(l, r)| l.is_none() && *r == Some(right_idx))
                                {
                                    self.join_index.remove(pos);
                                }
                            }
                            // insert_pos is computed *after* removals so it reflects
                            // the post-removal join_index layout.
                        }
                        let insert_pos = self.find_left_insert_position(*index);
                        for (offset, &right_idx) in matching_indices.iter().enumerate() {
                            self.join_index
                                .insert(insert_pos + offset, (Some(*index), Some(right_idx)));
                            modified = true;
                        }
                    } else if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
                        let insert_pos = self.find_left_insert_position(*index);
                        self.join_index.insert(insert_pos, (Some(*index), None));
                        modified = true;
                    }
                } else {
                    // NULL key — matches nothing, but LEFT/FULL include the row
                    if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
                        let insert_pos = self.find_left_insert_position(*index);
                        self.join_index.insert(insert_pos, (Some(*index), None));
                        modified = true;
                    }
                }
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    ..
                } if self.left_keys.contains(column) => {
                    // Key column changed: rematch this left row.
                    // Reconstruct the pre-update row by swapping in old_value.
                    let current_row = match self.left_table.borrow().get_row(*row) {
                        Ok(r) => r,
                        Err(_) => continue, // unreadable — skip; matches old fallback semantic
                    };
                    let mut old_row = current_row.clone();
                    old_row.insert(column.clone(), old_value.clone());

                    let old_key = Self::build_composite_key(&old_row, &self.left_keys);
                    let new_key = Self::build_composite_key(&current_row, &self.left_keys);

                    if old_key == new_key {
                        // The single changed cell didn't move the composite key
                        // (e.g., a Null↔Null no-op or equivalent typed value).
                        continue;
                    }

                    // ---- Remove old matches for this left row ----
                    let removed_right_indices: Vec<usize> = self
                        .join_index
                        .iter()
                        .filter_map(|(l, r)| {
                            if *l == Some(*row) {
                                *r
                            } else {
                                None
                            }
                        })
                        .collect();

                    let before_len = self.join_index.len();
                    self.join_index.retain(|(l, _)| *l != Some(*row));
                    if self.join_index.len() != before_len {
                        modified = true;
                    }

                    // RIGHT/FULL: resurrect any newly orphaned right rows.
                    if self.join_type == JoinType::Right
                        || self.join_type == JoinType::Full
                    {
                        for r_idx in removed_right_indices {
                            let still_matched = self
                                .join_index
                                .iter()
                                .any(|(l, r)| l.is_some() && *r == Some(r_idx));
                            if !still_matched {
                                let pos = self.find_orphan_insert_position(r_idx);
                                self.join_index.insert(pos, (None, Some(r_idx)));
                                modified = true;
                            }
                        }
                    }

                    // ---- Add new matches for the updated row ----
                    if let Some(new_key_val) = new_key {
                        let right_lookup = self.build_right_lookup();
                        if let Some(matching_right) = right_lookup.get(&new_key_val) {
                            // RIGHT/FULL: any orphan (None, Some(r)) for the
                            // now-matched right rows must be removed first.
                            if self.join_type == JoinType::Right
                                || self.join_type == JoinType::Full
                            {
                                for &r_idx in matching_right {
                                    self.join_index.retain(|(l, r)| {
                                        !(l.is_none() && *r == Some(r_idx))
                                    });
                                }
                            }
                            let insert_pos = self.find_left_insert_position(*row);
                            for (offset, &r_idx) in matching_right.iter().enumerate() {
                                self.join_index.insert(
                                    insert_pos + offset,
                                    (Some(*row), Some(r_idx)),
                                );
                                modified = true;
                            }
                        } else if self.join_type == JoinType::Left
                            || self.join_type == JoinType::Full
                        {
                            let insert_pos = self.find_left_insert_position(*row);
                            self.join_index.insert(insert_pos, (Some(*row), None));
                            modified = true;
                        }
                    } else if self.join_type == JoinType::Left
                        || self.join_type == JoinType::Full
                    {
                        // New key is None (NULL or NaN): LEFT/FULL keeps the row
                        // with a None-right placeholder.
                        let insert_pos = self.find_left_insert_position(*row);
                        self.join_index.insert(insert_pos, (Some(*row), None));
                        modified = true;
                    }
                }
                _ => {
                    // CellUpdated on a non-key column: no join_index change
                    // (views read live data on get_row).
                }
            }
        }

        // Handle right table inserts — build LEFT lookup once, not per right insert.
        // Mirrors the existing right_lookup pattern used for left-insert handling.
        // Was O(left.len()) per right insert via linear scan; now O(matches) via hash lookup.
        //
        // De-duplication: lefts that were also inserted in this same sync batch
        // already had their match against any new right added by the left-insert
        // pass above. The right-insert pass must NOT re-add the same pair, so
        // collect the new-left index set and skip those during matching below.
        let new_left_indices: std::collections::HashSet<usize> = left_changes
            .iter()
            .filter_map(|c| match c {
                TableChange::RowInserted { index, .. } => Some(*index),
                _ => None,
            })
            .collect();
        let has_right_inserts = right_changes
            .iter()
            .any(|c| matches!(c, TableChange::RowInserted { .. }));
        let left_lookup = if has_right_inserts {
            Some(self.build_left_lookup())
        } else {
            None
        };

        for change in &right_changes {
            match change {
                TableChange::RowDeleted { index: del_idx, .. } => {
                    // Symmetric to the left-delete handler.
                    // Step 1: capture left indices that were matched by this
                    // right row — needed for LEFT/FULL orphan handling below.
                    let removed_left_indices: Vec<usize> = self
                        .join_index
                        .iter()
                        .filter_map(|(l, r)| {
                            if *r == Some(*del_idx) {
                                *l
                            } else {
                                None
                            }
                        })
                        .collect();

                    // Step 2: remove all join_index entries pointing at the
                    // deleted right row.
                    let before_len = self.join_index.len();
                    self.join_index.retain(|(_, r)| *r != Some(*del_idx));
                    if self.join_index.len() != before_len {
                        modified = true;
                    }

                    // Step 3: shift remaining right indices > del_idx down by 1.
                    for (_, r_opt) in self.join_index.iter_mut() {
                        if let Some(r) = r_opt {
                            if *r > *del_idx {
                                *r -= 1;
                            }
                        }
                    }

                    // Step 4: LEFT/FULL only — any left row that was previously
                    // matched by the deleted right and is no longer matched by
                    // ANY remaining right becomes a (Some(left_idx), None)
                    // placeholder at its sorted position.
                    if self.join_type == JoinType::Left
                        || self.join_type == JoinType::Full
                    {
                        for l_idx in removed_left_indices {
                            let still_matched = self
                                .join_index
                                .iter()
                                .any(|(l, r)| *l == Some(l_idx) && r.is_some());
                            if !still_matched {
                                let pos = self.find_left_insert_position(l_idx);
                                self.join_index.insert(pos, (Some(l_idx), None));
                                modified = true;
                            }
                        }
                    }
                }
                TableChange::RowInserted {
                    index: right_idx,
                    data,
                } => {
                // Adjust existing right indices. Unlike left, right_idx
                // values are not monotonic in join_index (which is sorted
                // primarily by left), so we can't shortcut detection in
                // sub-linear time; the existing single-pass conditional
                // shift is already optimal for this layout.
                for (_, right_opt) in self.join_index.iter_mut() {
                    if let Some(r_idx) = right_opt {
                        if *r_idx >= *right_idx {
                            *r_idx += 1;
                        }
                    }
                }

                // Find left rows that match this new right row via the precomputed lookup
                if let Some(right_key) = Self::build_composite_key(data, &self.right_keys) {
                    let lookup = left_lookup.as_ref().unwrap();
                    // Filter out lefts that were inserted in this same sync —
                    // their match was already added by the left-insert pass.
                    let candidate_lefts: Vec<usize> = lookup
                        .get(&right_key)
                        .map(|v| {
                            v.iter()
                                .copied()
                                .filter(|l| !new_left_indices.contains(l))
                                .collect()
                        })
                        .unwrap_or_default();
                    let any_match = !candidate_lefts.is_empty();

                    if !candidate_lefts.is_empty() {
                        for left_idx in candidate_lefts.iter().copied() {
                            // For LEFT/FULL: may need to replace a (Some(left_idx), None)
                            // placeholder rather than insert a new entry.
                            if self.join_type == JoinType::Left
                                || self.join_type == JoinType::Full
                            {
                                let existing_null = self
                                    .join_index
                                    .iter()
                                    .position(|(l, r)| *l == Some(left_idx) && r.is_none());

                                if let Some(pos) = existing_null {
                                    self.join_index[pos] = (Some(left_idx), Some(*right_idx));
                                } else {
                                    let insert_pos =
                                        self.find_right_insert_position(left_idx, *right_idx);
                                    self.join_index
                                        .insert(insert_pos, (Some(left_idx), Some(*right_idx)));
                                }
                            } else {
                                let insert_pos =
                                    self.find_right_insert_position(left_idx, *right_idx);
                                self.join_index
                                    .insert(insert_pos, (Some(left_idx), Some(*right_idx)));
                            }
                            modified = true;
                        }
                    }

                    // RIGHT/FULL: if no left match, insert as unmatched right
                    // row at the sorted position within the None-left tail.
                    // (Push-at-end happened to produce sorted output today
                    // because parent right indices are monotonic, but the
                    // invariant should be structural, not incidental.)
                    if !any_match
                        && (self.join_type == JoinType::Right || self.join_type == JoinType::Full)
                    {
                        let pos = self.find_orphan_insert_position(*right_idx);
                        self.join_index.insert(pos, (None, Some(*right_idx)));
                        modified = true;
                    }
                } else {
                    // NULL key on right — for RIGHT/FULL, add as unmatched.
                    if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
                        let pos = self.find_orphan_insert_position(*right_idx);
                        self.join_index.insert(pos, (None, Some(*right_idx)));
                        modified = true;
                    }
                }
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    ..
                } if self.right_keys.contains(column) => {
                    // Symmetric to the left-key-update handler.
                    let current_row = match self.right_table.borrow().get_row(*row) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                    let mut old_row = current_row.clone();
                    old_row.insert(column.clone(), old_value.clone());

                    let old_key = Self::build_composite_key(&old_row, &self.right_keys);
                    let new_key = Self::build_composite_key(&current_row, &self.right_keys);

                    if old_key == new_key {
                        continue;
                    }

                    // ---- Remove old matches that pointed at this right row ----
                    let removed_left_indices: Vec<usize> = self
                        .join_index
                        .iter()
                        .filter_map(|(l, r)| {
                            if *r == Some(*row) {
                                *l
                            } else {
                                None
                            }
                        })
                        .collect();

                    let before_len = self.join_index.len();
                    self.join_index.retain(|(_, r)| *r != Some(*row));
                    if self.join_index.len() != before_len {
                        modified = true;
                    }

                    // LEFT/FULL: resurrect any newly orphaned left rows.
                    if self.join_type == JoinType::Left
                        || self.join_type == JoinType::Full
                    {
                        for l_idx in removed_left_indices {
                            let still_matched = self
                                .join_index
                                .iter()
                                .any(|(l, r)| *l == Some(l_idx) && r.is_some());
                            if !still_matched {
                                let pos = self.find_left_insert_position(l_idx);
                                self.join_index.insert(pos, (Some(l_idx), None));
                                modified = true;
                            }
                        }
                    }

                    // ---- Add new matches for the updated right row ----
                    if let Some(new_key_val) = new_key {
                        let left_lookup = self.build_left_lookup();
                        if let Some(matching_left) = left_lookup.get(&new_key_val) {
                            for &l_idx in matching_left {
                                // LEFT/FULL: a (Some(l_idx), None) placeholder
                                // for this left is no longer correct (the left
                                // just gained a match) — replace it in place.
                                let existing_null = self.join_index.iter().position(
                                    |(l, r)| *l == Some(l_idx) && r.is_none(),
                                );
                                if let Some(pos) = existing_null {
                                    self.join_index[pos] = (Some(l_idx), Some(*row));
                                } else {
                                    let insert_pos =
                                        self.find_right_insert_position(l_idx, *row);
                                    self.join_index.insert(
                                        insert_pos,
                                        (Some(l_idx), Some(*row)),
                                    );
                                }
                                modified = true;
                            }
                        } else if self.join_type == JoinType::Right
                            || self.join_type == JoinType::Full
                        {
                            // No left match: RIGHT/FULL adds an orphan entry.
                            let pos = self.find_orphan_insert_position(*row);
                            self.join_index.insert(pos, (None, Some(*row)));
                            modified = true;
                        }
                    } else if self.join_type == JoinType::Right
                        || self.join_type == JoinType::Full
                    {
                        // New key is None (NULL or NaN): RIGHT/FULL keeps the
                        // row as an unmatched orphan.
                        let pos = self.find_orphan_insert_position(*row);
                        self.join_index.insert(pos, (None, Some(*row)));
                        modified = true;
                    }
                }
                _ => {
                    // CellUpdated on a non-key column: no join_index change
                    // (views read live data on get_row).
                }
            }
        }

        let left_table = self.left_table.borrow();
        let right_table = self.right_table.borrow();
        self.left_last_processed_change_count = left_table.changeset().total_len();
        self.right_last_processed_change_count = right_table.changeset().total_len();

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
        let sort_col_indices: Vec<usize> = sort_keys
            .iter()
            .filter_map(|key| table.schema().get_column_index(&key.column))
            .collect();

        // Pre-extract values for each sort key column
        let sort_values: Vec<Vec<ColumnValue>> = sort_col_indices
            .iter()
            .map(|&col_idx| {
                (0..len)
                    .map(|row_idx| {
                        table
                            .get_value_by_index(row_idx, col_idx)
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

                let cmp = Self::compare_values(val_a, val_b, key);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        self.last_synced_generation = table.changeset_generation();
        self.last_processed_change_count = table.changeset().total_len();
    }

    /// Compare two ColumnValues by reference under a sort key.
    ///
    /// Unified implementation — used by both `rebuild_index` (full sort)
    /// and `find_insertion_position` (incremental binary search). Previously
    /// these paths had two divergent impls (`compare_values_ref` returned
    /// Equal for mixed types; `compare_values` used `format!("{:?}", ...)`
    /// which is not a stability-guaranteed format).
    ///
    /// Mixed types fall back to a fixed `type_rank` ordering — deterministic
    /// across Rust versions, and the same direction for both call paths so
    /// rebuild and incremental insert produce identical orderings.
    fn compare_values(val_a: &ColumnValue, val_b: &ColumnValue, key: &SortKey) -> Ordering {
        // NULL handling (configurable per SortKey)
        match (val_a.is_null(), val_b.is_null()) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if key.nulls_first { Ordering::Less } else { Ordering::Greater };
            }
            (false, true) => {
                return if key.nulls_first { Ordering::Greater } else { Ordering::Less };
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
            // Mixed types — fixed deterministic ordering by type-rank.
            // Schema enforcement makes this unreachable through the normal
            // public API; the fallback exists so all code paths agree.
            (a, b) => Self::type_rank(a).cmp(&Self::type_rank(b)),
        };

        match key.order {
            SortOrder::Ascending => base_cmp,
            SortOrder::Descending => base_cmp.reverse(),
        }
    }

    /// Fixed per-variant rank for the mixed-type comparison fallback.
    /// Order chosen for stable, debuggable output (NULL first, then numerics
    /// from narrowest to widest, then dates, then strings).
    fn type_rank(v: &ColumnValue) -> u8 {
        match v {
            ColumnValue::Null => 0,
            ColumnValue::Bool(_) => 1,
            ColumnValue::Int32(_) => 2,
            ColumnValue::Int64(_) => 3,
            ColumnValue::Float32(_) => 4,
            ColumnValue::Float64(_) => 5,
            ColumnValue::Date(_) => 6,
            ColumnValue::DateTime(_) => 7,
            ColumnValue::String(_) => 8,
        }
    }

    /// Find the insertion position for a new value using binary search.
    /// The new row's sort-key values are pre-extracted once before the search
    /// (was previously re-fetched O(log N) times inside the closure).
    fn find_insertion_position(&self, parent_index: usize) -> usize {
        let table = self.parent.borrow();

        // Hoist: read new-row values once, not per binary-search step.
        let new_vals: Vec<ColumnValue> = self
            .sort_keys
            .iter()
            .map(|key| {
                table
                    .get_value(parent_index, &key.column)
                    .unwrap_or(ColumnValue::Null)
            })
            .collect();

        let result = self.sorted_index.binary_search_by(|&existing_idx| {
            for (key_idx, key) in self.sort_keys.iter().enumerate() {
                let val_existing = table
                    .get_value(existing_idx, &key.column)
                    .unwrap_or(ColumnValue::Null);
                let val_new = &new_vals[key_idx];

                let cmp = Self::compare_values(&val_existing, val_new, key);
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
                    // Tail-insert fast path: SortedView is 1:1 with parent,
                    // so `*index == sorted_index.len()` means the new row's
                    // parent index is past every existing one — no shift.
                    let is_tail = *index == self.sorted_index.len();
                    if !is_tail {
                        for parent_idx in self.sorted_index.iter_mut() {
                            if *parent_idx >= *index {
                                *parent_idx += 1;
                            }
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
                        if let Some(current_pos) = self.sorted_index.iter().position(|&i| i == *row)
                        {
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

pub mod aggregate_support;
pub use aggregate_support::AggregateFunction;
use aggregate_support::{GroupKey, GroupState};

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
        let mut result =
            HashMap::with_capacity(self.group_by_columns.len() + self.aggregations.len());
        result.extend(key.to_column_values(&self.group_by_columns, &parent));

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

/// Per-view "syncer" closure stored in the registry.
///
/// When invoked: upgrades the captured `Weak<RefCell<View>>`, calls
/// `view.sync()`, and returns the cursor (absolute change index) the view
/// has now consumed from the parent table's changeset. Returns `None` if
/// the view has been dropped, letting `TickableTable::tick` prune dead
/// entries in one pass.
type SyncerFn = Box<dyn FnMut() -> Option<usize>>;

/// Wrapper that pairs a table with a view registry for auto-tick propagation.
///
/// Construct with [`TickableTable::new`] from an existing
/// `Rc<RefCell<Table>>`. Register views via `register_filter`,
/// `register_sorted`, `register_aggregate`, `register_join_as_left`, or
/// `register_join_as_right`. Call [`tick`](TickableTable::tick) after
/// mutations to sync all registered views and compact the changeset.
///
/// Views are held as `Weak` references — dropping the strong `Rc` to a
/// view de-registers it automatically (entry pruned on next `tick()`).
pub struct TickableTable {
    table: Rc<RefCell<Table>>,
    syncers: RefCell<Vec<SyncerFn>>,
}

impl TickableTable {
    /// Wrap an existing table for tick-based view propagation.
    pub fn new(table: Rc<RefCell<Table>>) -> Self {
        TickableTable {
            table,
            syncers: RefCell::new(Vec::new()),
        }
    }

    /// Borrow the underlying table handle — useful for mutations or for
    /// constructing views (which take `Rc<RefCell<Table>>` as parent).
    pub fn table(&self) -> &Rc<RefCell<Table>> {
        &self.table
    }

    /// Register a `FilterView` for auto-sync on `tick()`.
    pub fn register_filter(&self, view: &Rc<RefCell<FilterView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let cursor = v.borrow().last_processed_change_count();
            Some(cursor)
        }));
    }

    /// Register a `SortedView` for auto-sync on `tick()`.
    pub fn register_sorted(&self, view: &Rc<RefCell<SortedView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let cursor = v.borrow().last_processed_change_count();
            Some(cursor)
        }));
    }

    /// Register an `AggregateView` for auto-sync on `tick()`.
    pub fn register_aggregate(&self, view: &Rc<RefCell<AggregateView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let cursor = v.borrow().last_processed_change_count();
            Some(cursor)
        }));
    }

    /// Register a `JoinView` on its LEFT parent table. Both this AND
    /// `register_join_as_right` on the right parent's TickableTable must
    /// be called for the join to be fully wired up.
    pub fn register_join_as_left(&self, view: &Rc<RefCell<JoinView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let (left_cursor, _) = v.borrow().last_processed_change_count();
            let cursor = left_cursor;
            Some(cursor)
        }));
    }

    /// Register a `JoinView` on its RIGHT parent table. See `register_join_as_left`.
    pub fn register_join_as_right(&self, view: &Rc<RefCell<JoinView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let (_, right_cursor) = v.borrow().last_processed_change_count();
            let cursor = right_cursor;
            Some(cursor)
        }));
    }

    /// Returns the number of registered syncers. Dead `Weak` references
    /// are only pruned by `tick()`, so this can include entries whose
    /// underlying views have been dropped.
    pub fn registered_view_count(&self) -> usize {
        self.syncers.borrow().len()
    }

    /// Synchronize all registered views with the table's pending changes.
    ///
    /// For each live view: invokes its sync closure (which calls
    /// `view.sync()` internally and reports the view's post-sync cursor).
    /// Dead `Weak` references (views that have been dropped) are pruned
    /// in the same pass. After all syncs, compacts the changeset up to
    /// `min(all reported cursors)` so memory doesn't grow unbounded.
    ///
    /// Returns the number of live views synced.
    pub fn tick(&self) -> usize {
        if !self.table.borrow().has_pending_changes() {
            return 0;
        }

        // Move syncers OUT so calling them does not hold a borrow of
        // `self.syncers`. The closure bodies call `view.borrow_mut().sync()`,
        // which in turn does `self.parent.borrow()` on the view — and
        // `self.parent` is the SAME `Rc<RefCell<Table>>` as `self.table`,
        // so the only borrow held during the loop is whatever the view's
        // sync acquires internally (immutable).
        let mut syncers: Vec<SyncerFn> = std::mem::take(&mut *self.syncers.borrow_mut());
        let mut min_cursor = self.table.borrow().changeset().total_len();
        let mut synced = 0usize;
        let mut alive: Vec<SyncerFn> = Vec::with_capacity(syncers.len());

        for mut syncer in syncers.drain(..) {
            match syncer() {
                Some(cursor) => {
                    min_cursor = min_cursor.min(cursor);
                    alive.push(syncer);
                    synced += 1;
                }
                None => {
                    // Weak upgrade failed — view dropped; do not re-add.
                }
            }
        }

        *self.syncers.borrow_mut() = alive;
        // Compaction in its own borrow_mut scope, AFTER the sync pass —
        // syncs only need shared borrows of the parent table.
        self.table.borrow_mut().compact_changeset(min_cursor);

        synced
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::aggregate_support::ColumnAggState;
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
                row.insert(
                    "region".to_string(),
                    ColumnValue::String("North".to_string()),
                );
                row.insert("amount".to_string(), ColumnValue::Float64(v));
                t.append_row(row).unwrap();
            }
            // South: 100, 200  (median=150)
            for v in [100.0, 200.0] {
                let mut row = HashMap::new();
                row.insert(
                    "region".to_string(),
                    ColumnValue::String("South".to_string()),
                );
                row.insert("amount".to_string(), ColumnValue::Float64(v));
                t.append_row(row).unwrap();
            }
        }

        let agg = AggregateView::new(
            "by_region".to_string(),
            table.clone(),
            vec!["region".to_string()],
            vec![
                (
                    "median_amount".to_string(),
                    "amount".to_string(),
                    AggregateFunction::Median,
                ),
                (
                    "p90_amount".to_string(),
                    "amount".to_string(),
                    AggregateFunction::Percentile(0.9),
                ),
            ],
        )
        .unwrap();

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
            vec![(
                "median_val".to_string(),
                "val".to_string(),
                AggregateFunction::Median,
            )],
        )
        .unwrap();

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
        let view = FilterView::new("filtered".to_string(), table.clone(), |row| {
            if let Some(ColumnValue::Int32(v)) = row.get("value") {
                *v > 20
            } else {
                false
            }
        });

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

        let mut view = FilterView::new("active_only".to_string(), table.clone(), |row| {
            if let Some(ColumnValue::Bool(active)) = row.get("active") {
                *active
            } else {
                false
            }
        });

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
            row.insert(
                "secret".to_string(),
                ColumnValue::String("password123".to_string()),
            );
            t.append_row(row).unwrap();
        }

        // Create projection without secret column
        let view = ProjectionView::new(
            "public".to_string(),
            table.clone(),
            vec!["id".to_string(), "name".to_string()],
        )
        .unwrap();

        assert_eq!(view.len(), 1);

        let row = view.get_row(0).unwrap();
        assert_eq!(row.get("id").unwrap().as_i32(), Some(1));
        assert_eq!(row.get("name").unwrap().as_string(), Some("Alice"));
        assert!(!row.contains_key("secret")); // Secret column not in projection
    }

    #[test]
    fn test_view_readonly() {
        let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        let view = FilterView::new("readonly".to_string(), table.clone(), |_| true);

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
            row3.insert(
                "name".to_string(),
                ColumnValue::String("Charlie".to_string()),
            );
            u.append_row(row3).unwrap();
        }

        // Create orders table
        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
            ("amount".to_string(), ColumnType::Float64, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new(
            "orders".to_string(),
            orders_schema,
        )));

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
        let orders = Rc::new(RefCell::new(Table::new(
            "orders".to_string(),
            orders_schema,
        )));

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
        let orders = Rc::new(RefCell::new(Table::new(
            "orders".to_string(),
            orders_schema,
        )));

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

    #[test]
    fn test_join_sync_advances_cursors_and_is_idempotent() {
        let users_schema = Schema::new(vec![
            ("user_id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
        users
            .borrow_mut()
            .append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(1)),
                ("name".to_string(), ColumnValue::String("Alice".to_string())),
            ]))
            .unwrap();

        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new(
            "orders".to_string(),
            orders_schema,
        )));

        let mut joined = JoinView::new(
            "user_orders".to_string(),
            users.clone(),
            orders.clone(),
            "user_id".to_string(),
            "user_id".to_string(),
            JoinType::Left,
        )
        .unwrap();

        orders
            .borrow_mut()
            .append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(101)),
                ("user_id".to_string(), ColumnValue::Int32(1)),
            ]))
            .unwrap();

        assert!(joined.sync());
        assert_eq!(joined.len(), 1);
        assert_eq!(
            joined.get_value(0, "right_order_id").unwrap().as_i32(),
            Some(101)
        );

        let first_cursors = joined.last_processed_change_count();
        assert!(!joined.sync());
        assert_eq!(joined.len(), 1);
        assert_eq!(joined.last_processed_change_count(), first_cursors);
    }

    #[test]
    fn test_join_sync_preserves_full_rebuild_order_on_left_insert() {
        let users_schema = Schema::new(vec![
            ("user_id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
        {
            let mut users_ref = users.borrow_mut();
            users_ref
                .append_row(HashMap::from([
                    ("user_id".to_string(), ColumnValue::Int32(1)),
                    ("name".to_string(), ColumnValue::String("Alice".to_string())),
                ]))
                .unwrap();
            users_ref
                .append_row(HashMap::from([
                    ("user_id".to_string(), ColumnValue::Int32(2)),
                    ("name".to_string(), ColumnValue::String("Bob".to_string())),
                ]))
                .unwrap();
        }

        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new(
            "orders".to_string(),
            orders_schema,
        )));
        {
            let mut orders_ref = orders.borrow_mut();
            orders_ref
                .append_row(HashMap::from([
                    ("order_id".to_string(), ColumnValue::Int32(101)),
                    ("user_id".to_string(), ColumnValue::Int32(1)),
                ]))
                .unwrap();
            orders_ref
                .append_row(HashMap::from([
                    ("order_id".to_string(), ColumnValue::Int32(301)),
                    ("user_id".to_string(), ColumnValue::Int32(3)),
                ]))
                .unwrap();
            orders_ref
                .append_row(HashMap::from([
                    ("order_id".to_string(), ColumnValue::Int32(201)),
                    ("user_id".to_string(), ColumnValue::Int32(2)),
                ]))
                .unwrap();
        }

        let mut joined = JoinView::new(
            "user_orders".to_string(),
            users.clone(),
            orders.clone(),
            "user_id".to_string(),
            "user_id".to_string(),
            JoinType::Inner,
        )
        .unwrap();

        users
            .borrow_mut()
            .insert_row(
                1,
                HashMap::from([
                    ("user_id".to_string(), ColumnValue::Int32(3)),
                    ("name".to_string(), ColumnValue::String("Carol".to_string())),
                ]),
            )
            .unwrap();

        assert!(joined.sync());
        let incremental_rows: Vec<(String, i32)> = (0..joined.len())
            .map(|idx| {
                let row = joined.get_row(idx).unwrap();
                (
                    row.get("name").unwrap().as_string().unwrap().to_string(),
                    row.get("right_order_id").unwrap().as_i32().unwrap(),
                )
            })
            .collect();

        joined.refresh();
        let rebuilt_rows: Vec<(String, i32)> = (0..joined.len())
            .map(|idx| {
                let row = joined.get_row(idx).unwrap();
                (
                    row.get("name").unwrap().as_string().unwrap().to_string(),
                    row.get("right_order_id").unwrap().as_i32().unwrap(),
                )
            })
            .collect();

        assert_eq!(incremental_rows, rebuilt_rows);
        assert_eq!(
            incremental_rows,
            vec![
                ("Alice".to_string(), 101),
                ("Carol".to_string(), 301),
                ("Bob".to_string(), 201),
            ]
        );
    }

    #[test]
    fn test_join_sync_preserves_full_rebuild_order_on_right_insert() {
        let users_schema = Schema::new(vec![
            ("user_id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
        users
            .borrow_mut()
            .append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(1)),
                ("name".to_string(), ColumnValue::String("Alice".to_string())),
            ]))
            .unwrap();

        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new(
            "orders".to_string(),
            orders_schema,
        )));
        {
            let mut orders_ref = orders.borrow_mut();
            orders_ref
                .append_row(HashMap::from([
                    ("order_id".to_string(), ColumnValue::Int32(101)),
                    ("user_id".to_string(), ColumnValue::Int32(1)),
                ]))
                .unwrap();
            orders_ref
                .append_row(HashMap::from([
                    ("order_id".to_string(), ColumnValue::Int32(301)),
                    ("user_id".to_string(), ColumnValue::Int32(1)),
                ]))
                .unwrap();
        }

        let mut joined = JoinView::new(
            "user_orders".to_string(),
            users.clone(),
            orders.clone(),
            "user_id".to_string(),
            "user_id".to_string(),
            JoinType::Inner,
        )
        .unwrap();

        orders
            .borrow_mut()
            .insert_row(
                1,
                HashMap::from([
                    ("order_id".to_string(), ColumnValue::Int32(201)),
                    ("user_id".to_string(), ColumnValue::Int32(1)),
                ]),
            )
            .unwrap();

        assert!(joined.sync());
        let incremental_order_ids: Vec<i32> = (0..joined.len())
            .map(|idx| {
                joined
                    .get_value(idx, "right_order_id")
                    .unwrap()
                    .as_i32()
                    .unwrap()
            })
            .collect();

        joined.refresh();
        let rebuilt_order_ids: Vec<i32> = (0..joined.len())
            .map(|idx| {
                joined
                    .get_value(idx, "right_order_id")
                    .unwrap()
                    .as_i32()
                    .unwrap()
            })
            .collect();

        assert_eq!(incremental_order_ids, rebuilt_order_ids);
        assert_eq!(incremental_order_ids, vec![101, 201, 301]);
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
        let mut view = FilterView::new("filtered".to_string(), table.clone(), |row| {
            if let Some(ColumnValue::Int32(v)) = row.get("value") {
                *v > 20
            } else {
                false
            }
        });

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
        let mut view = FilterView::new("filtered".to_string(), table.clone(), |row| {
            if let Some(ColumnValue::Int32(v)) = row.get("value") {
                *v > 20
            } else {
                false
            }
        });

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

        let mut view = FilterView::new("filtered".to_string(), table.clone(), |row| {
            if let Some(ColumnValue::Int32(v)) = row.get("value") {
                *v > 20
            } else {
                false
            }
        });

        table.borrow_mut().clear_changeset();
        assert_eq!(view.len(), 1);

        // Update row 0's value to 25 (now matches filter)
        {
            table
                .borrow_mut()
                .set_value(0, "value", ColumnValue::Int32(25))
                .unwrap();
        }

        view.sync();
        assert_eq!(view.len(), 2); // Both rows now match

        // Update row 1's value to 15 (no longer matches filter)
        table.borrow_mut().clear_changeset();
        {
            table
                .borrow_mut()
                .set_value(1, "value", ColumnValue::Int32(15))
                .unwrap();
        }

        view.sync();
        assert_eq!(view.len(), 1); // Only row 0 matches now
    }

    #[test]
    fn test_table_changeset_tracking() {
        let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);

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
            row1.insert(
                "name".to_string(),
                ColumnValue::String("Charlie".to_string()),
            );
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
        )
        .unwrap();

        assert_eq!(sorted.len(), 3);
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Alice")
        );
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Bob")
        );
        assert_eq!(
            sorted.get_value(2, "name").unwrap().as_string(),
            Some("Charlie")
        );
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
            row3.insert(
                "name".to_string(),
                ColumnValue::String("Charlie".to_string()),
            );
            row3.insert("score".to_string(), ColumnValue::Int32(85));
            t.append_row(row3).unwrap();
        }

        // Sort by score descending (highest first)
        let sorted = SortedView::new(
            "by_score_desc".to_string(),
            table.clone(),
            vec![SortKey::descending("score")],
        )
        .unwrap();

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
            row.insert(
                "department".to_string(),
                ColumnValue::String("Engineering".to_string()),
            );
            row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
            row.insert("salary".to_string(), ColumnValue::Int32(100000));
            t.append_row(row).unwrap();

            // Sales - Bob
            let mut row = HashMap::new();
            row.insert(
                "department".to_string(),
                ColumnValue::String("Sales".to_string()),
            );
            row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
            row.insert("salary".to_string(), ColumnValue::Int32(80000));
            t.append_row(row).unwrap();

            // Engineering - Charlie
            let mut row = HashMap::new();
            row.insert(
                "department".to_string(),
                ColumnValue::String("Engineering".to_string()),
            );
            row.insert(
                "name".to_string(),
                ColumnValue::String("Charlie".to_string()),
            );
            row.insert("salary".to_string(), ColumnValue::Int32(90000));
            t.append_row(row).unwrap();

            // Sales - Diana
            let mut row = HashMap::new();
            row.insert(
                "department".to_string(),
                ColumnValue::String("Sales".to_string()),
            );
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
        )
        .unwrap();

        assert_eq!(sorted.len(), 4);

        // Engineering first (Alice 100k, then Charlie 90k)
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Alice")
        );
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Charlie")
        );

        // Sales second (Diana 85k, then Bob 80k)
        assert_eq!(
            sorted.get_value(2, "name").unwrap().as_string(),
            Some("Diana")
        );
        assert_eq!(
            sorted.get_value(3, "name").unwrap().as_string(),
            Some("Bob")
        );
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
            row.insert(
                "name".to_string(),
                ColumnValue::String("Charlie".to_string()),
            );
            row.insert("age".to_string(), ColumnValue::Int32(25));
            t.append_row(row).unwrap();
        }

        // Sort by age ascending (nulls last by default)
        let sorted = SortedView::new(
            "by_age".to_string(),
            table.clone(),
            vec![SortKey::ascending("age")],
        )
        .unwrap();

        assert_eq!(sorted.len(), 3);
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Charlie")
        ); // 25
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Alice")
        ); // 30
        assert_eq!(
            sorted.get_value(2, "name").unwrap().as_string(),
            Some("Bob")
        ); // null

        // Sort by age ascending (nulls first)
        let sorted_nulls_first = SortedView::new(
            "by_age_nulls_first".to_string(),
            table.clone(),
            vec![SortKey::new("age", SortOrder::Ascending, true)],
        )
        .unwrap();

        assert_eq!(
            sorted_nulls_first.get_value(0, "name").unwrap().as_string(),
            Some("Bob")
        ); // null
        assert_eq!(
            sorted_nulls_first.get_value(1, "name").unwrap().as_string(),
            Some("Charlie")
        ); // 25
        assert_eq!(
            sorted_nulls_first.get_value(2, "name").unwrap().as_string(),
            Some("Alice")
        ); // 30
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
        )
        .unwrap();

        table.borrow_mut().clear_changeset();
        assert_eq!(sorted.len(), 2);
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Bob")
        );
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Diana")
        );

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
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Alice")
        );
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Bob")
        );
        assert_eq!(
            sorted.get_value(2, "name").unwrap().as_string(),
            Some("Diana")
        );

        // Add Charlie (should go between Bob and Diana)
        table.borrow_mut().clear_changeset();
        {
            let mut t = table.borrow_mut();
            let mut row = HashMap::new();
            row.insert(
                "name".to_string(),
                ColumnValue::String("Charlie".to_string()),
            );
            row.insert("score".to_string(), ColumnValue::Int32(80));
            t.append_row(row).unwrap();
        }

        sorted.sync();
        assert_eq!(sorted.len(), 4);
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Alice")
        );
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Bob")
        );
        assert_eq!(
            sorted.get_value(2, "name").unwrap().as_string(),
            Some("Charlie")
        );
        assert_eq!(
            sorted.get_value(3, "name").unwrap().as_string(),
            Some("Diana")
        );
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
        )
        .unwrap();

        table.borrow_mut().clear_changeset();
        assert_eq!(sorted.len(), 4);

        // Delete Bob (parent index 1)
        table.borrow_mut().delete_row(1).unwrap();

        sorted.sync();
        assert_eq!(sorted.len(), 3);
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Alice")
        );
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Charlie")
        );
        assert_eq!(
            sorted.get_value(2, "name").unwrap().as_string(),
            Some("Diana")
        );
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
            row.insert(
                "name".to_string(),
                ColumnValue::String("Charlie".to_string()),
            );
            row.insert("score".to_string(), ColumnValue::Int32(90));
            t.append_row(row).unwrap();
        }

        // Sort by score ascending
        let mut sorted = SortedView::new(
            "by_score".to_string(),
            table.clone(),
            vec![SortKey::ascending("score")],
        )
        .unwrap();

        table.borrow_mut().clear_changeset();

        // Initial order: Alice (70), Bob (80), Charlie (90)
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Alice")
        );
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Bob")
        );
        assert_eq!(
            sorted.get_value(2, "name").unwrap().as_string(),
            Some("Charlie")
        );

        // Update Alice's score to 95 (should move to end)
        table
            .borrow_mut()
            .set_value(0, "score", ColumnValue::Int32(95))
            .unwrap();

        sorted.sync();

        // New order: Bob (80), Charlie (90), Alice (95)
        assert_eq!(
            sorted.get_value(0, "name").unwrap().as_string(),
            Some("Bob")
        );
        assert_eq!(
            sorted.get_value(1, "name").unwrap().as_string(),
            Some("Charlie")
        );
        assert_eq!(
            sorted.get_value(2, "name").unwrap().as_string(),
            Some("Alice")
        );
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
        )
        .unwrap();

        // Sorted order by value: 50 (parent 1), 75 (parent 2), 100 (parent 0)
        assert_eq!(sorted.get_parent_index(0), Some(1)); // 50
        assert_eq!(sorted.get_parent_index(1), Some(2)); // 75
        assert_eq!(sorted.get_parent_index(2), Some(0)); // 100
        assert_eq!(sorted.get_parent_index(3), None); // out of range
    }

    #[test]
    fn test_sorted_view_empty_table() {
        let schema = Schema::new(vec![("name".to_string(), ColumnType::String, false)]);

        let table = Rc::new(RefCell::new(Table::new("empty".to_string(), schema)));

        let sorted = SortedView::new(
            "sorted_empty".to_string(),
            table.clone(),
            vec![SortKey::ascending("name")],
        )
        .unwrap();

        assert_eq!(sorted.len(), 0);
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_sorted_view_invalid_column() {
        let schema = Schema::new(vec![("name".to_string(), ColumnType::String, false)]);

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
        let schema = Schema::new(vec![("name".to_string(), ColumnType::String, false)]);

        let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

        let result = SortedView::new("invalid".to_string(), table.clone(), vec![]);

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

        let mut view = FilterView::new("filtered".to_string(), table.clone(), |row| {
            row.get("value")
                .and_then(|v| v.as_i32())
                .map(|v| v >= 10)
                .unwrap_or(false)
        });

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

    // === RIGHT and FULL OUTER Join Tests ===

    #[test]
    fn test_right_join() {
        // Users: Alice/1, Bob/2, Charlie/3
        let users_schema = Schema::new(vec![
            ("user_id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
        {
            let mut u = users.borrow_mut();
            u.append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(1)),
                ("name".to_string(), ColumnValue::String("Alice".to_string())),
            ]))
            .unwrap();
            u.append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(2)),
                ("name".to_string(), ColumnValue::String("Bob".to_string())),
            ]))
            .unwrap();
            u.append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(3)),
                (
                    "name".to_string(),
                    ColumnValue::String("Charlie".to_string()),
                ),
            ]))
            .unwrap();
        }

        // Orders: 101/1 (Alice), 102/4 (Dave — no matching user)
        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new(
            "orders".to_string(),
            orders_schema,
        )));
        {
            let mut o = orders.borrow_mut();
            o.append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(101)),
                ("user_id".to_string(), ColumnValue::Int32(1)),
            ]))
            .unwrap();
            o.append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(102)),
                ("user_id".to_string(), ColumnValue::Int32(4)),
            ]))
            .unwrap();
        }

        let joined = JoinView::new(
            "user_orders".to_string(),
            users.clone(),
            orders.clone(),
            "user_id".to_string(),
            "user_id".to_string(),
            JoinType::Right,
        )
        .unwrap();

        // RIGHT JOIN: all right rows. Alice matched (order 101), Dave unmatched (order 102).
        assert_eq!(joined.len(), 2);

        // Row 0: Alice matched with order 101
        let row0 = joined.get_row(0).unwrap();
        assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(row0.get("right_order_id").unwrap().as_i32(), Some(101));

        // Row 1: Unmatched right row (order 102, user_id=4) — left columns are NULL
        let row1 = joined.get_row(1).unwrap();
        assert!(row1.get("name").unwrap().is_null());
        assert!(row1.get("user_id").unwrap().is_null());
        assert_eq!(row1.get("right_order_id").unwrap().as_i32(), Some(102));
        assert_eq!(row1.get("right_user_id").unwrap().as_i32(), Some(4));
    }

    #[test]
    fn test_full_join() {
        // Users: Alice/1, Bob/2
        let users_schema = Schema::new(vec![
            ("user_id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
        {
            let mut u = users.borrow_mut();
            u.append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(1)),
                ("name".to_string(), ColumnValue::String("Alice".to_string())),
            ]))
            .unwrap();
            u.append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(2)),
                ("name".to_string(), ColumnValue::String("Bob".to_string())),
            ]))
            .unwrap();
        }

        // Orders: 101/1 (Alice), 102/4 (Dave — no matching user)
        let orders_schema = Schema::new(vec![
            ("order_id".to_string(), ColumnType::Int32, false),
            ("user_id".to_string(), ColumnType::Int32, false),
        ]);
        let orders = Rc::new(RefCell::new(Table::new(
            "orders".to_string(),
            orders_schema,
        )));
        {
            let mut o = orders.borrow_mut();
            o.append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(101)),
                ("user_id".to_string(), ColumnValue::Int32(1)),
            ]))
            .unwrap();
            o.append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(102)),
                ("user_id".to_string(), ColumnValue::Int32(4)),
            ]))
            .unwrap();
        }

        let joined = JoinView::new(
            "user_orders".to_string(),
            users.clone(),
            orders.clone(),
            "user_id".to_string(),
            "user_id".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // FULL JOIN: 3 rows — Alice matched, Bob unmatched left, Dave unmatched right
        assert_eq!(joined.len(), 3);

        // Row 0: Alice matched with order 101
        let row0 = joined.get_row(0).unwrap();
        assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(row0.get("right_order_id").unwrap().as_i32(), Some(101));

        // Row 1: Bob unmatched (left only)
        let row1 = joined.get_row(1).unwrap();
        assert_eq!(row1.get("name").unwrap().as_string(), Some("Bob"));
        assert!(row1.get("right_order_id").unwrap().is_null());

        // Row 2: Dave unmatched (right only, user_id=4)
        let row2 = joined.get_row(2).unwrap();
        assert!(row2.get("name").unwrap().is_null());
        assert_eq!(row2.get("right_order_id").unwrap().as_i32(), Some(102));
        assert_eq!(row2.get("right_user_id").unwrap().as_i32(), Some(4));
    }

    #[test]
    fn test_right_join_multiple_matches() {
        // Two left rows with same key, one right row
        let left_schema = Schema::new(vec![
            ("key".to_string(), ColumnType::Int32, false),
            ("val".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
        {
            let mut l = left.borrow_mut();
            l.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(1)),
                ("val".to_string(), ColumnValue::String("A".to_string())),
            ]))
            .unwrap();
            l.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(1)),
                ("val".to_string(), ColumnValue::String("B".to_string())),
            ]))
            .unwrap();
        }

        let right_schema = Schema::new(vec![
            ("key".to_string(), ColumnType::Int32, false),
            ("data".to_string(), ColumnType::String, false),
        ]);
        let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
        {
            let mut r = right.borrow_mut();
            r.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(1)),
                ("data".to_string(), ColumnValue::String("X".to_string())),
            ]))
            .unwrap();
        }

        let joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "key".to_string(),
            "key".to_string(),
            JoinType::Right,
        )
        .unwrap();

        // RIGHT JOIN: one right row matches two left rows -> 2 result rows
        assert_eq!(joined.len(), 2);

        let row0 = joined.get_row(0).unwrap();
        assert_eq!(row0.get("val").unwrap().as_string(), Some("A"));
        assert_eq!(row0.get("right_data").unwrap().as_string(), Some("X"));

        let row1 = joined.get_row(1).unwrap();
        assert_eq!(row1.get("val").unwrap().as_string(), Some("B"));
        assert_eq!(row1.get("right_data").unwrap().as_string(), Some("X"));
    }

    #[test]
    fn test_full_join_no_matches() {
        // Disjoint keys: left has {1,2}, right has {3,4}
        let left_schema = Schema::new(vec![
            ("key".to_string(), ColumnType::Int32, false),
            ("lval".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
        {
            let mut l = left.borrow_mut();
            l.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(1)),
                ("lval".to_string(), ColumnValue::String("A".to_string())),
            ]))
            .unwrap();
            l.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(2)),
                ("lval".to_string(), ColumnValue::String("B".to_string())),
            ]))
            .unwrap();
        }

        let right_schema = Schema::new(vec![
            ("key".to_string(), ColumnType::Int32, false),
            ("rval".to_string(), ColumnType::String, false),
        ]);
        let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
        {
            let mut r = right.borrow_mut();
            r.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(3)),
                ("rval".to_string(), ColumnValue::String("X".to_string())),
            ]))
            .unwrap();
            r.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(4)),
                ("rval".to_string(), ColumnValue::String("Y".to_string())),
            ]))
            .unwrap();
        }

        let joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "key".to_string(),
            "key".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // FULL JOIN with disjoint keys: 4 rows, all cross-columns are NULL
        assert_eq!(joined.len(), 4);

        // Left rows first (unmatched)
        let row0 = joined.get_row(0).unwrap();
        assert_eq!(row0.get("lval").unwrap().as_string(), Some("A"));
        assert!(row0.get("right_rval").unwrap().is_null());

        let row1 = joined.get_row(1).unwrap();
        assert_eq!(row1.get("lval").unwrap().as_string(), Some("B"));
        assert!(row1.get("right_rval").unwrap().is_null());

        // Right rows after (unmatched)
        let row2 = joined.get_row(2).unwrap();
        assert!(row2.get("lval").unwrap().is_null());
        assert_eq!(row2.get("right_rval").unwrap().as_string(), Some("X"));

        let row3 = joined.get_row(3).unwrap();
        assert!(row3.get("lval").unwrap().is_null());
        assert_eq!(row3.get("right_rval").unwrap().as_string(), Some("Y"));
    }

    #[test]
    fn test_full_join_null_key_rows() {
        // Both tables have NULL-key rows. FULL -> 3 rows
        // Left: (1, "Alice"), (NULL, "Ghost")
        // Right: (1, "Order1"), (NULL, "Phantom")
        let left_schema = Schema::new(vec![
            ("key".to_string(), ColumnType::Int32, true),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
        {
            let mut l = left.borrow_mut();
            l.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(1)),
                ("name".to_string(), ColumnValue::String("Alice".to_string())),
            ]))
            .unwrap();
            l.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Null),
                ("name".to_string(), ColumnValue::String("Ghost".to_string())),
            ]))
            .unwrap();
        }

        let right_schema = Schema::new(vec![
            ("key".to_string(), ColumnType::Int32, true),
            ("data".to_string(), ColumnType::String, false),
        ]);
        let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
        {
            let mut r = right.borrow_mut();
            r.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Int32(1)),
                (
                    "data".to_string(),
                    ColumnValue::String("Order1".to_string()),
                ),
            ]))
            .unwrap();
            r.append_row(HashMap::from([
                ("key".to_string(), ColumnValue::Null),
                (
                    "data".to_string(),
                    ColumnValue::String("Phantom".to_string()),
                ),
            ]))
            .unwrap();
        }

        let joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "key".to_string(),
            "key".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // FULL JOIN: 3 rows
        // - Alice matched with Order1: (Some(0), Some(0))
        // - Ghost (NULL key, unmatched left): (Some(1), None)
        // - Phantom (NULL key, unmatched right): (None, Some(1))
        assert_eq!(joined.len(), 3);

        // Row 0: Alice matched
        let row0 = joined.get_row(0).unwrap();
        assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(row0.get("right_data").unwrap().as_string(), Some("Order1"));

        // Row 1: Ghost (NULL key, left only)
        let row1 = joined.get_row(1).unwrap();
        assert_eq!(row1.get("name").unwrap().as_string(), Some("Ghost"));
        assert!(row1.get("right_data").unwrap().is_null());

        // Row 2: Phantom (NULL key, right only)
        let row2 = joined.get_row(2).unwrap();
        assert!(row2.get("name").unwrap().is_null());
        assert_eq!(row2.get("right_data").unwrap().as_string(), Some("Phantom"));
    }

    // === RIGHT/FULL JOIN incremental sync tests ===

    /// Helper: collect join_index from a JoinView by reading rows and extracting
    /// a comparable tuple for each row. Returns Vec of (Option<left_key>, Option<right_key>).
    fn collect_join_rows(joined: &JoinView) -> Vec<(Option<i32>, Option<i32>)> {
        (0..joined.len())
            .map(|idx| {
                let row = joined.get_row(idx).unwrap();
                let left_id = match row.get("id").unwrap() {
                    ColumnValue::Null => None,
                    v => Some(v.as_i32().unwrap()),
                };
                let right_id = match row.get("right_id").unwrap() {
                    ColumnValue::Null => None,
                    v => Some(v.as_i32().unwrap()),
                };
                (left_id, right_id)
            })
            .collect()
    }

    fn make_left_table() -> Rc<RefCell<Table>> {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        Rc::new(RefCell::new(Table::new("left".to_string(), schema)))
    }

    fn make_right_table() -> Rc<RefCell<Table>> {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("val".to_string(), ColumnType::String, false),
        ]);
        Rc::new(RefCell::new(Table::new("right".to_string(), schema)))
    }

    fn left_row(id: i32, name: &str) -> HashMap<String, ColumnValue> {
        HashMap::from([
            ("id".to_string(), ColumnValue::Int32(id)),
            ("name".to_string(), ColumnValue::String(name.to_string())),
        ])
    }

    fn right_row(id: i32, val: &str) -> HashMap<String, ColumnValue> {
        HashMap::from([
            ("id".to_string(), ColumnValue::Int32(id)),
            ("val".to_string(), ColumnValue::String(val.to_string())),
        ])
    }

    #[test]
    fn test_right_join_sync_left_insert() {
        // Start: empty left, 2 right rows. RIGHT JOIN => 2 unmatched right rows.
        // Insert left row matching right id=1. Sync. Verify matches rebuild.
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
            r.append_row(right_row(2, "R2")).unwrap();
        }

        let mut joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Right,
        )
        .unwrap();

        // Initial state: 2 unmatched right rows
        assert_eq!(joined.len(), 2);

        // Insert left row matching right id=1
        left.borrow_mut().append_row(left_row(1, "L1")).unwrap();
        assert!(joined.sync());

        let synced = collect_join_rows(&joined);

        // Rebuild from scratch and compare
        joined.refresh();
        let rebuilt = collect_join_rows(&joined);

        assert_eq!(synced, rebuilt);
        // Expected: (Some(1), Some(1)) matched, then (None, Some(2)) unmatched right
        assert_eq!(synced, vec![(Some(1), Some(1)), (None, Some(2))]);
    }

    #[test]
    fn test_right_join_sync_right_insert() {
        // Start: 1 left row (id=1), empty right. RIGHT JOIN => empty.
        // Insert 2 right rows: id=1 (matching), id=99 (not matching). Sync.
        let left = make_left_table();
        let right = make_right_table();
        left.borrow_mut().append_row(left_row(1, "L1")).unwrap();

        let mut joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Right,
        )
        .unwrap();

        assert_eq!(joined.len(), 0);

        // Insert matching and non-matching right rows
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
            r.append_row(right_row(99, "R99")).unwrap();
        }
        assert!(joined.sync());

        let synced = collect_join_rows(&joined);

        joined.refresh();
        let rebuilt = collect_join_rows(&joined);

        assert_eq!(synced, rebuilt);
        // Expected: (Some(1), Some(1)) matched, then (None, Some(99)) unmatched right
        assert_eq!(synced, vec![(Some(1), Some(1)), (None, Some(99))]);
    }

    #[test]
    fn test_full_join_sync_left_insert() {
        // Start: empty left, 1 right row (id=5). FULL JOIN => 1 unmatched right row.
        // Insert 2 left rows: id=5 (matching), id=10 (not matching). Sync.
        let left = make_left_table();
        let right = make_right_table();
        right.borrow_mut().append_row(right_row(5, "R5")).unwrap();

        let mut joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // Initial: 1 unmatched right row (None, Some(0))
        assert_eq!(joined.len(), 1);

        // Insert 2 left rows
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(5, "L5")).unwrap();
            l.append_row(left_row(10, "L10")).unwrap();
        }
        assert!(joined.sync());

        let synced = collect_join_rows(&joined);

        joined.refresh();
        let rebuilt = collect_join_rows(&joined);

        assert_eq!(synced, rebuilt);
        // Expected: (Some(5), Some(5)) matched, (Some(10), None) unmatched left
        assert_eq!(synced, vec![(Some(5), Some(5)), (Some(10), None)]);
    }

    #[test]
    fn test_full_join_sync_right_insert() {
        // Start: 1 left row (id=3), empty right. FULL JOIN => 1 unmatched left row.
        // Insert 2 right rows: id=3 (matching), id=7 (not matching). Sync.
        let left = make_left_table();
        let right = make_right_table();
        left.borrow_mut().append_row(left_row(3, "L3")).unwrap();

        let mut joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // Initial: 1 unmatched left row (Some(0), None)
        assert_eq!(joined.len(), 1);

        // Insert matching and non-matching right rows
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(3, "R3")).unwrap();
            r.append_row(right_row(7, "R7")).unwrap();
        }
        assert!(joined.sync());

        let synced = collect_join_rows(&joined);

        joined.refresh();
        let rebuilt = collect_join_rows(&joined);

        assert_eq!(synced, rebuilt);
        // Expected: (Some(3), Some(3)) matched, then (None, Some(7)) unmatched right
        assert_eq!(synced, vec![(Some(3), Some(3)), (None, Some(7))]);
    }

    #[test]
    fn test_full_join_unmatched_becomes_matched() {
        // Start: left(id=1), right(id=2) — no overlap.
        // FULL JOIN has 2 rows: (Some(0), None) and (None, Some(0)).
        // Insert left(id=2) which matches the previously-unmatched right row.
        // Sync. Verify (None, Some(0)) is replaced with (Some(1), Some(0)).
        let left = make_left_table();
        let right = make_right_table();
        left.borrow_mut().append_row(left_row(1, "L1")).unwrap();
        right.borrow_mut().append_row(right_row(2, "R2")).unwrap();

        let mut joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // Initial: 2 rows — (Some(0), None) unmatched left, (None, Some(0)) unmatched right
        assert_eq!(joined.len(), 2);

        // Insert left row that matches the right row's id=2
        left.borrow_mut().append_row(left_row(2, "L2")).unwrap();
        assert!(joined.sync());

        let synced = collect_join_rows(&joined);

        joined.refresh();
        let rebuilt = collect_join_rows(&joined);

        assert_eq!(synced, rebuilt);
        // Expected: (Some(1), None) for left id=1, (Some(2), Some(2)) for matched pair
        assert_eq!(synced, vec![(Some(1), None), (Some(2), Some(2))]);
    }

    #[test]
    fn test_right_join_rebuild_after_delete() {
        // RIGHT JOIN, delete from left, sync triggers rebuild. Verify correct result.
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
            l.append_row(left_row(2, "L2")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
            r.append_row(right_row(3, "R3")).unwrap();
        }

        let mut joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Right,
        )
        .unwrap();

        // Initial: (Some(0), Some(0)) for id=1 matched, (None, Some(1)) for id=3 unmatched
        assert_eq!(joined.len(), 2);

        // Delete left row id=1 (index 0)
        left.borrow_mut().delete_row(0).unwrap();
        assert!(joined.sync()); // Should trigger rebuild

        let result = collect_join_rows(&joined);
        // After delete: left has only id=2, right has id=1 and id=3
        // RIGHT JOIN: both right rows are unmatched
        assert_eq!(result, vec![(None, Some(1)), (None, Some(3))]);
    }

    #[test]
    fn test_full_join_rebuild_after_delete() {
        // FULL JOIN, delete from right, sync triggers rebuild. Verify correct result.
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
            l.append_row(left_row(2, "L2")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(2, "R2")).unwrap();
            r.append_row(right_row(3, "R3")).unwrap();
        }

        let mut joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // Initial: (Some(0), None) for left id=1 unmatched,
        //          (Some(1), Some(0)) for id=2 matched,
        //          (None, Some(1)) for right id=3 unmatched
        assert_eq!(joined.len(), 3);

        // Delete right row id=2 (index 0)
        right.borrow_mut().delete_row(0).unwrap();
        assert!(joined.sync()); // Should trigger rebuild

        let result = collect_join_rows(&joined);
        // After delete: left has id=1 and id=2, right has only id=3
        // FULL JOIN: left id=1 unmatched, left id=2 unmatched, right id=3 unmatched
        assert_eq!(
            result,
            vec![(Some(1), None), (Some(2), None), (None, Some(3))]
        );
    }

    // === Incremental left-delete regression tests (fix #9) ===

    /// INNER join, delete left row with a match. Entry removed; following
    /// left indices shift down by 1.
    #[test]
    fn test_inner_join_incremental_left_delete() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
            l.append_row(left_row(2, "L2")).unwrap();
            l.append_row(left_row(3, "L3")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
            r.append_row(right_row(2, "R2")).unwrap();
            r.append_row(right_row(3, "R3")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        )
        .unwrap();
        assert_eq!(joined.len(), 3);

        // Delete left row id=2 (parent index 1) — the middle match
        left.borrow_mut().delete_row(1).unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // After delete: left has id=1, id=3; right unchanged
        // INNER: id=1 ↔ id=1, id=3 ↔ id=3 (id=2 was shifted down to where id=3 was)
        assert_eq!(result, vec![(Some(1), Some(1)), (Some(3), Some(3))]);
    }

    /// LEFT join, delete a left row that had a None-right placeholder.
    /// Placeholder removed; following left indices shift down.
    #[test]
    fn test_left_join_incremental_left_delete_placeholder() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
            l.append_row(left_row(2, "L2")).unwrap(); // no right match
            l.append_row(left_row(3, "L3")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
            r.append_row(right_row(3, "R3")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Left,
        )
        .unwrap();
        assert_eq!(joined.len(), 3); // (1,1), (2,None), (3,3)

        // Delete left row id=2 (parent index 1) — the placeholder
        left.borrow_mut().delete_row(1).unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // After delete: left has id=1, id=3; right has id=1, id=3
        // LEFT: id=1 ↔ id=1, id=3 ↔ id=3 (id=3 was parent_idx 2, now 1)
        assert_eq!(result, vec![(Some(1), Some(1)), (Some(3), Some(3))]);
    }

    /// LEFT join, delete a left row with one of two matches to the same right
    /// row. The right row is NOT orphaned because another left still matches.
    #[test]
    fn test_right_join_incremental_left_delete_no_orphan_when_other_left_matches() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1a")).unwrap();
            l.append_row(left_row(1, "L1b")).unwrap(); // both match right id=1
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Right,
        )
        .unwrap();
        assert_eq!(joined.len(), 2); // (1, 1), (1, 1) — same right id=1

        // Delete first left row
        left.borrow_mut().delete_row(0).unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // After delete: left has id=1 (was L1b), right has id=1
        // RIGHT JOIN: should be (1, 1) — NOT orphaned because the remaining
        // left row still matches the right.
        assert_eq!(result, vec![(Some(1), Some(1))]);
    }

    /// LEFT join, delete a right row that was the only match for a left row.
    /// The left row should resurrect as (Some(left_idx), None) placeholder
    /// at its sorted position.
    #[test]
    fn test_left_join_incremental_right_delete_resurrects_orphan() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
            l.append_row(left_row(2, "L2")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
            r.append_row(right_row(2, "R2")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Left,
        )
        .unwrap();
        assert_eq!(joined.len(), 2); // (1,1), (2,2)

        // Delete right row id=2 (parent index 1) — was the only match for left id=2
        right.borrow_mut().delete_row(1).unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // Left id=2 must resurrect as (Some(1), None) at its sorted position
        assert_eq!(result, vec![(Some(1), Some(1)), (Some(2), None)]);
    }

    // === Incremental left-key-update regression tests (fix #9) ===

    /// INNER join, change a left row's key to one that matches a different
    /// right row. Old match removed; new match inserted.
    #[test]
    fn test_inner_join_incremental_left_key_update_to_new_match() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
            r.append_row(right_row(2, "R2")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        )
        .unwrap();
        assert_eq!(joined.len(), 1); // left id=1 ↔ right id=1

        // Change left id from 1 to 2 — should match the OTHER right row
        left.borrow_mut()
            .set_value(0, "id", ColumnValue::Int32(2))
            .unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        assert_eq!(result, vec![(Some(2), Some(2))]);
    }

    /// LEFT join, change a left key so no right matches. Result becomes
    /// (Some(left_idx), None) placeholder. RIGHT/FULL would also resurrect
    /// the previously-matched right row as orphan — covered by next test.
    #[test]
    fn test_left_join_incremental_left_key_update_to_no_match() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Left,
        )
        .unwrap();
        assert_eq!(joined.len(), 1);

        // Change left id to 99 — no right with id=99
        left.borrow_mut()
            .set_value(0, "id", ColumnValue::Int32(99))
            .unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // LEFT: row appears as (Some(99), None) placeholder
        assert_eq!(result, vec![(Some(99), None)]);
    }

    /// FULL join, change a left key. Previous right match becomes orphan;
    /// new left key matches a different right row.
    #[test]
    fn test_full_join_incremental_left_key_update_orphan_and_match() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
            r.append_row(right_row(2, "R2")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Full,
        )
        .unwrap();
        // Initial: (Some(0), Some(0)) for id=1, (None, Some(1)) for id=2 unmatched
        assert_eq!(joined.len(), 2);

        // Change left id from 1 to 2
        left.borrow_mut()
            .set_value(0, "id", ColumnValue::Int32(2))
            .unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // Right id=1 (was matched) now orphaned; left now matches right id=2
        // (which removes the orphan placeholder it had)
        assert_eq!(result, vec![(Some(2), Some(2)), (None, Some(1))]);
    }

    // === Incremental right-key-update regression tests (fix #9) ===

    /// INNER join, change a right row's key to match a different left row.
    /// Old match removed; new match inserted.
    #[test]
    fn test_inner_join_incremental_right_key_update_to_new_match() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
            l.append_row(left_row(2, "L2")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        )
        .unwrap();
        assert_eq!(joined.len(), 1); // left id=1 ↔ right id=1

        // Change right id from 1 to 2 — should now match left id=2
        right.borrow_mut()
            .set_value(0, "id", ColumnValue::Int32(2))
            .unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        assert_eq!(result, vec![(Some(2), Some(2))]);
    }

    /// FULL join, change a right key so its previous left match becomes orphan
    /// (resurrects as Some(l), None placeholder), and the new key matches
    /// nothing (the right itself appears as a (None, Some) orphan).
    #[test]
    fn test_full_join_incremental_right_key_update_creates_two_orphans() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Full,
        )
        .unwrap();
        assert_eq!(joined.len(), 1); // (1, 1)

        // Change right id from 1 to 99 — no left has id=99
        right.borrow_mut()
            .set_value(0, "id", ColumnValue::Int32(99))
            .unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // FULL: left id=1 resurrects as (Some(1), None); right id=99 becomes (None, Some(99))
        assert_eq!(result, vec![(Some(1), None), (None, Some(99))]);
    }

    // === Rust-side tick() registry tests (fix #10) ===

    /// Native Rust callers can register a FilterView with a TickableTable
    /// wrapper and have tick() auto-sync it after mutations — no manual
    /// sync() calls needed. Mirrors what PyTable already does for Python users.
    #[test]
    fn test_table_tick_propagates_to_filter_view() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("v".to_string(), ColumnType::Int32, false),
        ]);
        let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
        let tickable = TickableTable::new(table.clone());

        // Seed 2 rows
        for &(id, v) in &[(1, 10), (2, 20)] {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(id));
            row.insert("v".to_string(), ColumnValue::Int32(v));
            table.borrow_mut().append_row(row).unwrap();
        }

        let view = Rc::new(RefCell::new(FilterView::new(
            "f".to_string(),
            table.clone(),
            |row| row.get("v").and_then(|v| v.as_i32()).unwrap_or(0) >= 15,
        )));
        tickable.register_filter(&view);

        assert_eq!(view.borrow().len(), 1);

        // Append a matching row WITHOUT calling sync() manually.
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(3));
        row.insert("v".to_string(), ColumnValue::Int32(30));
        table.borrow_mut().append_row(row).unwrap();

        let synced = tickable.tick();
        assert!(synced >= 1, "tick should have synced at least one view");
        assert_eq!(view.borrow().len(), 2);
    }

    /// Registry must drop dead Weak references so dropped views don't leak.
    #[test]
    fn test_table_tick_prunes_dropped_views() {
        let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);
        let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
        let tickable = TickableTable::new(table.clone());

        {
            let view = Rc::new(RefCell::new(FilterView::new(
                "f".to_string(),
                table.clone(),
                |_| true,
            )));
            tickable.register_filter(&view);
            assert_eq!(tickable.registered_view_count(), 1);
            // view drops at end of block
        }

        // Mutate so tick has something to do
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        table.borrow_mut().append_row(row).unwrap();

        // tick() should prune the dead Weak and return 0 syncs
        assert_eq!(tickable.tick(), 0);
        assert_eq!(tickable.registered_view_count(), 0);
    }

    /// SortedView via TickableTable: register, mutate parent, tick — sorted
    /// position of the new row must reflect in the view without any manual
    /// sync() call.
    #[test]
    fn test_table_tick_propagates_to_sorted_view() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);
        let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
        let tickable = TickableTable::new(table.clone());

        for &(id, score) in &[(1, 50), (2, 30), (3, 70)] {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(id));
            row.insert("score".to_string(), ColumnValue::Int32(score));
            table.borrow_mut().append_row(row).unwrap();
        }

        let view = Rc::new(RefCell::new(
            SortedView::new(
                "s".to_string(),
                table.clone(),
                vec![SortKey::descending("score")],
            )
            .unwrap(),
        ));
        tickable.register_sorted(&view);

        assert_eq!(view.borrow().len(), 3);
        // Initial DESC order by score: 70, 50, 30
        assert_eq!(view.borrow().get_value(0, "score").unwrap().as_i32(), Some(70));

        // Append a row that should sort to the very top
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(4));
        row.insert("score".to_string(), ColumnValue::Int32(100));
        table.borrow_mut().append_row(row).unwrap();

        let synced = tickable.tick();
        assert!(synced >= 1);
        assert_eq!(view.borrow().len(), 4);
        // New row (score=100) must be at view index 0 after auto-sync.
        assert_eq!(view.borrow().get_value(0, "score").unwrap().as_i32(), Some(100));
    }

    /// AggregateView via TickableTable: register, mutate parent, tick —
    /// aggregate state must reflect the new row.
    #[test]
    fn test_table_tick_propagates_to_aggregate_view() {
        let schema = Schema::new(vec![
            ("region".to_string(), ColumnType::String, false),
            ("amount".to_string(), ColumnType::Float64, false),
        ]);
        let table = Rc::new(RefCell::new(Table::new("sales".to_string(), schema)));
        let tickable = TickableTable::new(table.clone());

        for &(region, amount) in &[("North", 100.0_f64), ("South", 200.0), ("North", 150.0)] {
            let mut row = HashMap::new();
            row.insert("region".to_string(), ColumnValue::String(region.to_string()));
            row.insert("amount".to_string(), ColumnValue::Float64(amount));
            table.borrow_mut().append_row(row).unwrap();
        }

        let view = Rc::new(RefCell::new(
            AggregateView::new(
                "by_region".to_string(),
                table.clone(),
                vec!["region".to_string()],
                vec![(
                    "total".to_string(),
                    "amount".to_string(),
                    AggregateFunction::Sum,
                )],
            )
            .unwrap(),
        ));
        tickable.register_aggregate(&view);

        assert_eq!(view.borrow().len(), 2); // 2 groups: North, South

        // Append a row in a NEW region — should add a third group on next tick.
        let mut row = HashMap::new();
        row.insert("region".to_string(), ColumnValue::String("West".to_string()));
        row.insert("amount".to_string(), ColumnValue::Float64(99.0));
        table.borrow_mut().append_row(row).unwrap();

        let synced = tickable.tick();
        assert!(synced >= 1);
        assert_eq!(view.borrow().len(), 3); // 3 groups now: North, South, West
    }

    /// Heterogeneous registry: a Filter, a Sorted, AND an Aggregate on the
    /// SAME table — one tick() syncs all three. Verifies the
    /// `Box<dyn FnMut>`-based registry isn't accidentally specialized to
    /// one view type.
    #[test]
    fn test_table_tick_with_mixed_view_types() {
        let schema = Schema::new(vec![
            ("region".to_string(), ColumnType::String, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);
        let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
        let tickable = TickableTable::new(table.clone());

        for &(region, score) in &[("A", 10), ("B", 50), ("A", 30)] {
            let mut row = HashMap::new();
            row.insert("region".to_string(), ColumnValue::String(region.to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(score));
            table.borrow_mut().append_row(row).unwrap();
        }

        let filter = Rc::new(RefCell::new(FilterView::new(
            "f".to_string(),
            table.clone(),
            |row| row.get("score").and_then(|v| v.as_i32()).unwrap_or(0) >= 20,
        )));
        let sorted = Rc::new(RefCell::new(
            SortedView::new(
                "s".to_string(),
                table.clone(),
                vec![SortKey::descending("score")],
            )
            .unwrap(),
        ));
        let aggregate = Rc::new(RefCell::new(
            AggregateView::new(
                "a".to_string(),
                table.clone(),
                vec!["region".to_string()],
                vec![(
                    "total".to_string(),
                    "score".to_string(),
                    AggregateFunction::Sum,
                )],
            )
            .unwrap(),
        ));

        tickable.register_filter(&filter);
        tickable.register_sorted(&sorted);
        tickable.register_aggregate(&aggregate);

        assert_eq!(tickable.registered_view_count(), 3);
        assert_eq!(filter.borrow().len(), 2); // scores >= 20: {50, 30}
        assert_eq!(sorted.borrow().len(), 3);
        assert_eq!(aggregate.borrow().len(), 2); // groups A, B

        // Single mutation that affects all three views distinctly:
        // - filter: new row (score=80) matches → len 2 → 3
        // - sorted: new row joins → len 3 → 4, sorted to top (score=80 highest)
        // - aggregate: still 2 regions (B), but B's sum changes
        let mut row = HashMap::new();
        row.insert("region".to_string(), ColumnValue::String("B".to_string()));
        row.insert("score".to_string(), ColumnValue::Int32(80));
        table.borrow_mut().append_row(row).unwrap();

        let synced = tickable.tick();
        assert_eq!(synced, 3, "all 3 registered views should sync in one tick");

        assert_eq!(filter.borrow().len(), 3);
        assert_eq!(sorted.borrow().len(), 4);
        assert_eq!(
            sorted.borrow().get_value(0, "score").unwrap().as_i32(),
            Some(80)
        );
        assert_eq!(aggregate.borrow().len(), 2);
    }

    /// tick() must call compact_changeset(min_cursor) so memory does not
    /// grow unbounded across long-running streams. Verify by reading the
    /// changeset's base_index and pending length directly.
    #[test]
    fn test_table_tick_compacts_changeset() {
        let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);
        let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
        let tickable = TickableTable::new(table.clone());

        // Register a view so tick has something to advance the cursor for.
        let view = Rc::new(RefCell::new(FilterView::new(
            "f".to_string(),
            table.clone(),
            |_| true,
        )));
        tickable.register_filter(&view);

        for i in 0..5 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            table.borrow_mut().append_row(row).unwrap();
        }

        // Before tick: 5 pending changes, base_index=0 (none compacted yet).
        assert_eq!(table.borrow().changeset().len(), 5);
        assert_eq!(table.borrow().changeset().base_index(), 0);
        assert_eq!(table.borrow().changeset().total_len(), 5);

        let synced = tickable.tick();
        assert!(synced >= 1);

        // After tick: changes all processed; compaction must have advanced
        // base_index to 5, leaving the pending vec empty. total_len is
        // still 5 (it's monotonic across compactions).
        assert_eq!(table.borrow().changeset().len(), 0);
        assert_eq!(table.borrow().changeset().base_index(), 5);
        assert_eq!(table.borrow().changeset().total_len(), 5);
    }

    /// JoinView must be registerable on BOTH parent TickableTables; tick on
    /// either parent must propagate to the join. Mirrors the JoinLeft/
    /// JoinRight variants on PyTable.
    #[test]
    fn test_table_tick_propagates_to_join_view_from_both_parents() {
        let left = make_left_table();
        let right = make_right_table();
        let left_tickable = TickableTable::new(left.clone());
        let right_tickable = TickableTable::new(right.clone());

        // Seed initial match: left id=1 ↔ right id=1
        left.borrow_mut().append_row(left_row(1, "L1")).unwrap();
        right.borrow_mut().append_row(right_row(1, "R1")).unwrap();

        let join = Rc::new(RefCell::new(
            JoinView::new(
                "j".to_string(),
                left.clone(),
                right.clone(),
                "id".to_string(),
                "id".to_string(),
                JoinType::Inner,
            )
            .unwrap(),
        ));
        left_tickable.register_join_as_left(&join);
        right_tickable.register_join_as_right(&join);

        assert_eq!(join.borrow().len(), 1);

        // Append on BOTH sides; the convergence test mirrors what fix #9's
        // de-dup ensures: a single tick must add (1, 1) match exactly once.
        left.borrow_mut().append_row(left_row(2, "L2")).unwrap();
        right.borrow_mut().append_row(right_row(2, "R2")).unwrap();
        assert!(left_tickable.tick() >= 1);
        // Right tick may be a no-op (changes already consumed via left tick)
        // or sync 1 (if right's changeset still has the pending insert) —
        // both are fine. The size invariant is what matters.
        right_tickable.tick();
        assert_eq!(join.borrow().len(), 2);
    }

    // === Incremental JoinView edge cases (fix #9 — extended coverage) ===

    /// 1:N delete on RIGHT join: deleting the single left row that matched
    /// three right rows must orphan all three, preserved in right_idx ASC
    /// order to match rebuild output.
    #[test]
    fn test_right_join_incremental_left_delete_orphans_multiple_rights_ordered() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            l.append_row(left_row(1, "L1")).unwrap();
        }
        {
            let mut r = right.borrow_mut();
            r.append_row(right_row(1, "R1a")).unwrap();
            r.append_row(right_row(1, "R1b")).unwrap();
            r.append_row(right_row(1, "R1c")).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Right,
        )
        .unwrap();
        assert_eq!(joined.len(), 3); // left id=1 matches each of 3 right rows

        // Delete left id=1 — all three rights become orphaned
        left.borrow_mut().delete_row(0).unwrap();
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // Orphans must be ordered by right_idx ASC (matches rebuild order)
        assert_eq!(
            result,
            vec![(None, Some(1)), (None, Some(1)), (None, Some(1))]
        );
    }

    /// Bulk delete: delete 3 left rows back-to-back, sync once. Verifies
    /// cumulative index shifts compose correctly across changeset entries
    /// processed in a single batch.
    #[test]
    fn test_inner_join_incremental_bulk_left_delete_in_one_sync() {
        let left = make_left_table();
        let right = make_right_table();
        {
            let mut l = left.borrow_mut();
            for id in 1..=5 {
                l.append_row(left_row(id, "L")).unwrap();
            }
        }
        {
            let mut r = right.borrow_mut();
            for id in 1..=5 {
                r.append_row(right_row(id, "R")).unwrap();
            }
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        )
        .unwrap();
        assert_eq!(joined.len(), 5);

        // Delete first three left rows in sequence WITHOUT syncing between
        {
            let mut l = left.borrow_mut();
            l.delete_row(0).unwrap(); // id=1 gone, ids 2,3,4,5 now at indices 0..4
            l.delete_row(0).unwrap(); // id=2 gone, ids 3,4,5 at 0..3
            l.delete_row(0).unwrap(); // id=3 gone, ids 4,5 at 0..2
        }
        assert!(joined.sync());

        let result = collect_join_rows(&joined);
        // After all deletes: left has id=4, id=5; right unchanged
        assert_eq!(result, vec![(Some(4), Some(4)), (Some(5), Some(5))]);
    }

    /// LEFT join, key-update from a NULL key (was a placeholder) to a value
    /// that matches a right row. The placeholder should be replaced with the
    /// actual match.
    #[test]
    fn test_left_join_incremental_left_key_update_null_to_match() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, true),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
        let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));
        {
            // Left starts with NULL id — placeholder in LEFT join
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Null);
            row.insert("name".to_string(), ColumnValue::String("L_null".to_string()));
            left.borrow_mut().append_row(row).unwrap();
        }
        {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(7));
            row.insert("name".to_string(), ColumnValue::String("R7".to_string()));
            right.borrow_mut().append_row(row).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Left,
        )
        .unwrap();
        assert_eq!(joined.len(), 1);
        let initial = joined.get_row(0).unwrap();
        assert!(initial.get("right_id").unwrap().is_null());

        // Change left id from NULL to 7 — should now match
        left.borrow_mut()
            .set_value(0, "id", ColumnValue::Int32(7))
            .unwrap();
        assert!(joined.sync());

        let row = joined.get_row(0).unwrap();
        assert_eq!(row.get("id").unwrap().as_i32(), Some(7));
        assert_eq!(row.get("right_id").unwrap().as_i32(), Some(7));
        assert_eq!(row.get("right_name").unwrap().as_string(), Some("R7"));
    }

    /// LEFT join, key-update from a value (matched) to NULL. The matched
    /// entry should be replaced with a (Some(left_idx), None) placeholder.
    #[test]
    fn test_left_join_incremental_left_key_update_match_to_null() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, true),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
        let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));
        {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(5));
            row.insert("name".to_string(), ColumnValue::String("L5".to_string()));
            left.borrow_mut().append_row(row).unwrap();
        }
        {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(5));
            row.insert("name".to_string(), ColumnValue::String("R5".to_string()));
            right.borrow_mut().append_row(row).unwrap();
        }

        let mut joined = JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Left,
        )
        .unwrap();
        assert_eq!(joined.len(), 1);

        // Change left id from 5 to NULL — match disappears, placeholder appears
        left.borrow_mut()
            .set_value(0, "id", ColumnValue::Null)
            .unwrap();
        assert!(joined.sync());

        assert_eq!(joined.len(), 1);
        let row = joined.get_row(0).unwrap();
        assert!(row.get("id").unwrap().is_null());
        assert!(row.get("right_id").unwrap().is_null());
    }

    /// Multi-column join with a delete on the LEFT side. Exercises the
    /// composite-key path (Vec<JoinKeyPart> rather than single key).
    #[test]
    fn test_inner_join_multi_column_incremental_left_delete() {
        let schema = Schema::new(vec![
            ("a".to_string(), ColumnType::Int32, false),
            ("b".to_string(), ColumnType::String, false),
            ("payload".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
        let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));

        let mkrow = |a: i32, b: &str, payload: &str| {
            let mut row = HashMap::new();
            row.insert("a".to_string(), ColumnValue::Int32(a));
            row.insert("b".to_string(), ColumnValue::String(b.to_string()));
            row.insert("payload".to_string(), ColumnValue::String(payload.to_string()));
            row
        };

        // Left: (1,"x"), (1,"y"), (2,"x")
        // Right: (1,"x"), (1,"y")
        // Expected matches: L0↔R0, L1↔R1 (INNER)
        left.borrow_mut().append_row(mkrow(1, "x", "L0")).unwrap();
        left.borrow_mut().append_row(mkrow(1, "y", "L1")).unwrap();
        left.borrow_mut().append_row(mkrow(2, "x", "L2")).unwrap();
        right.borrow_mut().append_row(mkrow(1, "x", "R0")).unwrap();
        right.borrow_mut().append_row(mkrow(1, "y", "R1")).unwrap();

        let mut joined = JoinView::new_multi(
            "j".to_string(),
            left.clone(),
            right.clone(),
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string()],
            JoinType::Inner,
        )
        .unwrap();
        assert_eq!(joined.len(), 2);

        // Delete left row 0 → only L1↔R1 remains; indices shift
        left.borrow_mut().delete_row(0).unwrap();
        assert!(joined.sync());

        assert_eq!(joined.len(), 1);
        let row = joined.get_row(0).unwrap();
        assert_eq!(row.get("payload").unwrap().as_string(), Some("L1"));
        assert_eq!(row.get("right_payload").unwrap().as_string(), Some("R1"));
    }

    /// Multi-column join with a key-column update. Updating one part of a
    /// composite key must rebuild the typed JoinKey and match correctly.
    #[test]
    fn test_inner_join_multi_column_incremental_key_update() {
        let schema = Schema::new(vec![
            ("a".to_string(), ColumnType::Int32, false),
            ("b".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
        let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));

        let mkrow = |a: i32, b: &str| {
            let mut row = HashMap::new();
            row.insert("a".to_string(), ColumnValue::Int32(a));
            row.insert("b".to_string(), ColumnValue::String(b.to_string()));
            row
        };

        left.borrow_mut().append_row(mkrow(1, "x")).unwrap();
        right.borrow_mut().append_row(mkrow(1, "x")).unwrap();
        right.borrow_mut().append_row(mkrow(1, "y")).unwrap();

        let mut joined = JoinView::new_multi(
            "j".to_string(),
            left.clone(),
            right.clone(),
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string()],
            JoinType::Inner,
        )
        .unwrap();
        assert_eq!(joined.len(), 1); // L0 matches R0 (1,x)

        // Update left b from "x" to "y" — should now match R1, not R0
        left.borrow_mut()
            .set_value(0, "b", ColumnValue::String("y".to_string()))
            .unwrap();
        assert!(joined.sync());

        assert_eq!(joined.len(), 1);
        let row = joined.get_row(0).unwrap();
        assert_eq!(row.get("a").unwrap().as_i32(), Some(1));
        assert_eq!(row.get("b").unwrap().as_string(), Some("y"));
        assert_eq!(row.get("right_a").unwrap().as_i32(), Some(1));
        assert_eq!(row.get("right_b").unwrap().as_string(), Some("y"));
    }

    /// Convergence test: apply a mixed sequence of inserts, deletes, and key
    /// updates to TWO parallel table pairs; on one pair sync incrementally
    /// after each change, on the other build a fresh JoinView at the end.
    /// Both must produce byte-identical result rows.
    #[test]
    fn test_full_join_incremental_converges_to_rebuild() {
        fn build_pair() -> (Rc<RefCell<Table>>, Rc<RefCell<Table>>) {
            (make_left_table(), make_right_table())
        }

        let (left_a, right_a) = build_pair();
        let (left_b, right_b) = build_pair();

        // Seed both pairs identically
        for &(id, name) in &[(1, "L1"), (2, "L2"), (3, "L3")] {
            left_a.borrow_mut().append_row(left_row(id, name)).unwrap();
            left_b.borrow_mut().append_row(left_row(id, name)).unwrap();
        }
        for &(id, val) in &[(1, "R1"), (2, "R2a"), (2, "R2b"), (4, "R4")] {
            right_a.borrow_mut().append_row(right_row(id, val)).unwrap();
            right_b.borrow_mut().append_row(right_row(id, val)).unwrap();
        }

        // Build incremental view on pair A; rebuild on pair B at end.
        let mut joined_a = JoinView::new(
            "a".to_string(),
            left_a.clone(),
            right_a.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // Mixed sequence: insert left, delete right, update left key, insert right,
        // update right key, delete left, insert at end. Each op is mirrored to
        // both pairs so the rebuilt JoinView at the end sees the same state.

        left_a.borrow_mut().append_row(left_row(5, "L5")).unwrap();
        left_b.borrow_mut().append_row(left_row(5, "L5")).unwrap();
        joined_a.sync();

        right_a.borrow_mut().delete_row(0).unwrap();
        right_b.borrow_mut().delete_row(0).unwrap();
        joined_a.sync();

        left_a.borrow_mut().set_value(0, "id", ColumnValue::Int32(99)).unwrap();
        left_b.borrow_mut().set_value(0, "id", ColumnValue::Int32(99)).unwrap();
        joined_a.sync();

        right_a.borrow_mut().append_row(right_row(99, "R99")).unwrap();
        right_b.borrow_mut().append_row(right_row(99, "R99")).unwrap();
        joined_a.sync();

        right_a.borrow_mut().set_value(0, "id", ColumnValue::Int32(2)).unwrap();
        right_b.borrow_mut().set_value(0, "id", ColumnValue::Int32(2)).unwrap();
        joined_a.sync();

        left_a.borrow_mut().delete_row(2).unwrap();
        left_b.borrow_mut().delete_row(2).unwrap();
        joined_a.sync();

        left_a.borrow_mut().append_row(left_row(2, "L2_new")).unwrap();
        left_b.borrow_mut().append_row(left_row(2, "L2_new")).unwrap();
        joined_a.sync();

        // Fresh rebuild on pair B.
        let joined_b = JoinView::new(
            "b".to_string(),
            left_b.clone(),
            right_b.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Full,
        )
        .unwrap();

        // Compare result rows (same length, same (left_id, right_id) sequence).
        let a_rows = collect_join_rows(&joined_a);
        let b_rows = collect_join_rows(&joined_b);
        assert_eq!(
            a_rows.len(),
            b_rows.len(),
            "incremental row count diverged from rebuild"
        );
        assert_eq!(
            a_rows, b_rows,
            "incremental result diverged from rebuild after mixed-op sequence"
        );
    }

    // === Tail-insert fast-path regression tests (fix #12) ===

    /// Bulk-append 50 rows after creating a FilterView and sync; verify all
    /// matching rows appear in correct order. Exercises the no-shift path.
    #[test]
    fn test_filter_view_bulk_tail_insert_ordering() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("v".to_string(), ColumnType::Int32, false),
        ]);
        let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));

        // Seed: ids 0..5 with v=id*2 — predicate v >= 4 selects ids 2..5
        for i in 0..5 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            row.insert("v".to_string(), ColumnValue::Int32(i * 2));
            table.borrow_mut().append_row(row).unwrap();
        }

        let mut view = FilterView::new(
            "f".to_string(),
            table.clone(),
            |row| row.get("v").and_then(|v| v.as_i32()).unwrap_or(0) >= 4,
        );
        assert_eq!(view.len(), 3); // ids 2, 3, 4

        // Bulk append 50 more rows at the tail. Predicate matches all (v >= 4).
        for i in 5..55 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            row.insert("v".to_string(), ColumnValue::Int32(i * 2));
            table.borrow_mut().append_row(row).unwrap();
        }
        assert!(view.sync());

        // View should contain ids 2..55 (53 rows), parent indices in order.
        assert_eq!(view.len(), 53);
        for (view_idx, expected_parent_idx) in (2..55).enumerate() {
            let row = view.get_row(view_idx).unwrap();
            assert_eq!(row.get("id").unwrap().as_i32(), Some(expected_parent_idx));
        }
    }

    /// Bulk-append 20 rows to a SortedView (sorted DESC). Verify sort
    /// invariant holds after the no-shift tail-insert fast path.
    #[test]
    fn test_sorted_view_bulk_tail_insert_ordering() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("score".to_string(), ColumnType::Int32, false),
        ]);
        let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));

        // Seed 5 rows
        for i in 0..5 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            row.insert("score".to_string(), ColumnValue::Int32(i * 10));
            table.borrow_mut().append_row(row).unwrap();
        }

        let mut view = SortedView::new(
            "s".to_string(),
            table.clone(),
            vec![SortKey::descending("score")],
        )
        .unwrap();
        assert_eq!(view.len(), 5);

        // Bulk-append 20 more, intentionally with varying scores
        for i in 0..20 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(100 + i));
            // Scores in a pattern that interleaves with existing
            row.insert("score".to_string(), ColumnValue::Int32((i * 7) % 50));
            table.borrow_mut().append_row(row).unwrap();
        }
        assert!(view.sync());

        assert_eq!(view.len(), 25);

        // Verify sort invariant: scores are monotonically non-increasing
        let mut prev: Option<i32> = None;
        for i in 0..view.len() {
            let s = view.get_value(i, "score").unwrap().as_i32().unwrap();
            if let Some(p) = prev {
                assert!(p >= s, "Sort invariant violated at view index {}: {} < {}", i, p, s);
            }
            prev = Some(s);
        }
    }

    // === Typed join key tests (fix #1: replace string serialization) ===

    /// Two rows whose String parts contain `\x00` must not collide via
    /// composite-key serialization. With the old String-based scheme,
    /// keys (\"a\\x00b\", \"c\") and (\"a\", \"b\\x00c\") both produced
    /// \"a\\x00b\\x00c\". Typed keys ([\"a\\x00b\", \"c\"]) vs ([\"a\", \"b\\x00c\"])
    /// are structurally distinct.
    #[test]
    fn test_join_composite_key_null_byte_collision() {
        let schema = Schema::new(vec![
            ("k1".to_string(), ColumnType::String, false),
            ("k2".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
        let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));

        // Left: (k1="a\x00b", k2="c")
        let mut lrow = HashMap::new();
        lrow.insert("k1".to_string(), ColumnValue::String("a\x00b".to_string()));
        lrow.insert("k2".to_string(), ColumnValue::String("c".to_string()));
        left.borrow_mut().append_row(lrow).unwrap();

        // Right: (k1="a", k2="b\x00c") - DIFFERENT row, but old code thought they match
        let mut rrow = HashMap::new();
        rrow.insert("k1".to_string(), ColumnValue::String("a".to_string()));
        rrow.insert("k2".to_string(), ColumnValue::String("b\x00c".to_string()));
        right.borrow_mut().append_row(rrow).unwrap();

        let joined = JoinView::new_multi(
            "j".to_string(),
            left,
            right,
            vec!["k1".to_string(), "k2".to_string()],
            vec!["k1".to_string(), "k2".to_string()],
            JoinType::Inner,
        )
        .unwrap();

        // INNER join: must be empty (keys differ structurally)
        assert_eq!(joined.len(), 0,
            "Composite key collision: rows with \\x00-containing parts incorrectly matched");
    }

    /// Float64 join keys must work without relying on format!("{:?}", value),
    /// which is not a stability-guaranteed format. Same f64 bit patterns join;
    /// NaN never joins (SQL semantics).
    #[test]
    fn test_join_float64_keys() {
        let schema = Schema::new(vec![
            ("k".to_string(), ColumnType::Float64, false),
            ("label".to_string(), ColumnType::String, false),
        ]);
        let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
        let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));

        // Left: 1.5, 2.5, NaN
        for (k, label) in [(1.5_f64, "L_one"), (2.5, "L_two"), (f64::NAN, "L_nan")] {
            let mut row = HashMap::new();
            row.insert("k".to_string(), ColumnValue::Float64(k));
            row.insert("label".to_string(), ColumnValue::String(label.to_string()));
            left.borrow_mut().append_row(row).unwrap();
        }
        // Right: 1.5, NaN
        for (k, label) in [(1.5_f64, "R_one"), (f64::NAN, "R_nan")] {
            let mut row = HashMap::new();
            row.insert("k".to_string(), ColumnValue::Float64(k));
            row.insert("label".to_string(), ColumnValue::String(label.to_string()));
            right.borrow_mut().append_row(row).unwrap();
        }

        let joined = JoinView::new(
            "j".to_string(),
            left,
            right,
            "k".to_string(),
            "k".to_string(),
            JoinType::Inner,
        )
        .unwrap();

        // Only 1.5 matches; NaN must NEVER match (SQL semantics)
        assert_eq!(joined.len(), 1,
            "Expected exactly one Float64 match (1.5↔1.5); NaN must not match itself");
        let row = joined.get_row(0).unwrap();
        assert_eq!(row.get("label").unwrap().as_string(), Some("L_one"));
        assert_eq!(row.get("right_label").unwrap().as_string(), Some("R_one"));
    }

    // === Regression coverage for hot-path optimizations in JoinView left-insert ===

    /// Covers the LEFT "no-match incremental insert" branch where a new left
    /// row arrives with no corresponding right row. The new row must appear
    /// in join_index in left-index order, paired with `None` on the right.
    /// Exercises view.rs:970 (else-if branch) — the consumer of the outer
    /// `insert_pos` computation we are about to move into the branch.
    #[test]
    fn test_left_join_incremental_insert_no_right_match() {
        let left = make_left_table();
        let right = make_right_table();

        // Right has only id=1
        right.borrow_mut().append_row(right_row(1, "R1")).unwrap();
        // Left starts with id=1 (matches)
        left.borrow_mut().append_row(left_row(1, "L1")).unwrap();

        let mut joined = JoinView::new(
            "test".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Left,
        )
        .unwrap();
        assert_eq!(joined.len(), 1);

        // Incremental: add a left row with NO match in right
        left.borrow_mut().append_row(left_row(2, "L2")).unwrap();
        assert!(joined.sync());

        // Expected: (left.id=1, right.id=1), (left.id=2, right=None)
        let result = collect_join_rows(&joined);
        assert_eq!(result, vec![(Some(1), Some(1)), (Some(2), None)]);
    }
}
