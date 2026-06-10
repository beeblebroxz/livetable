//! JoinView — LEFT/INNER/RIGHT/FULL joins with incremental sync.

use crate::changeset::TableChange;
use crate::column::ColumnValue;
use crate::table::Table;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::{column_value_into_join_key_part, column_value_to_join_key_part, JoinKey, JoinKeyPart};

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

    /// True if the batch contains changes that can alter join_index
    /// membership or row positions: inserts, deletes, or updates to a
    /// join-key column. Non-key cell updates are positionally inert.
    fn has_structural_changes(changes: &[TableChange], keys: &[String]) -> bool {
        changes.iter().any(|c| match c {
            TableChange::RowInserted { .. } | TableChange::RowDeleted { .. } => true,
            TableChange::CellUpdated { column, .. } => keys.contains(column),
        })
    }

    /// True if a join-key update is recorded BEFORE an insert/delete in the
    /// same batch. The key-update handler reads the row's current data from
    /// the live parent by its recorded index — which the later insert/delete
    /// has already shifted, so it would read the wrong row.
    fn key_update_precedes_row_shift(changes: &[TableChange], keys: &[String]) -> bool {
        let mut seen_key_update = false;
        for c in changes {
            match c {
                TableChange::CellUpdated { column, .. } if keys.contains(column) => {
                    seen_key_update = true;
                }
                TableChange::RowInserted { .. } | TableChange::RowDeleted { .. }
                    if seen_key_update =>
                {
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    /// Incrementally sync with both parent tables' changes
    /// Returns true if any changes were applied
    ///
    /// Single-side batches (all structural changes on one parent) are handled
    /// incrementally. Batches that mix reference frames fall back to a full
    /// rebuild — see the guard below for the two cases.
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

        // Frame-mixing guard. The insert/key-update handlers below match new
        // rows against LIVE parent lookups (post-batch indices), while the
        // per-change shift loops treat existing join_index entries as
        // pre-batch. Two batch shapes mix those frames irreparably:
        //   1. Structural changes on BOTH sides in one batch — pairs added by
        //      the left pass carry live right indices, which the right-insert
        //      pass then re-shifts (and the de-dup against raw changeset
        //      indices misses shifted new lefts).
        //   2. A key update recorded BEFORE an insert/delete on the same side
        //      — the handler reads live row data at the recorded index, which
        //      the later change has already shifted.
        // Both are rare under tick()-driven usage (each side's queue is
        // consumed promptly); a full rebuild is correct by construction and
        // advances both cursors.
        let left_structural = Self::has_structural_changes(&left_changes, &self.left_keys);
        let right_structural = Self::has_structural_changes(&right_changes, &self.right_keys);
        if (left_structural && right_structural)
            || Self::key_update_precedes_row_shift(&left_changes, &self.left_keys)
            || Self::key_update_precedes_row_shift(&right_changes, &self.right_keys)
        {
            self.rebuild_index();
            return true;
        }

        // Single-side batches: all RowInserted, RowDeleted, and key-column
        // CellUpdated changes are handled incrementally below. (Non-key
        // CellUpdated never requires join_index changes — views read live
        // data via get_row.)

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
                        .filter_map(|(l, r)| if *l == Some(*del_idx) { *r } else { None })
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
                    if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
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
                    let max_existing_left = self.join_index.iter().rev().find_map(|(l, _)| *l);
                    let needs_shift = max_existing_left.is_some_and(|max_l| max_l >= *index);

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
                            if self.join_type == JoinType::Right || self.join_type == JoinType::Full
                            {
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
                        } else if self.join_type == JoinType::Left
                            || self.join_type == JoinType::Full
                        {
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
                        .filter_map(|(l, r)| if *l == Some(*row) { *r } else { None })
                        .collect();

                    let before_len = self.join_index.len();
                    self.join_index.retain(|(l, _)| *l != Some(*row));
                    if self.join_index.len() != before_len {
                        modified = true;
                    }

                    // RIGHT/FULL: resurrect any newly orphaned right rows.
                    if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
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
                            if self.join_type == JoinType::Right || self.join_type == JoinType::Full
                            {
                                for &r_idx in matching_right {
                                    self.join_index
                                        .retain(|(l, r)| !(l.is_none() && *r == Some(r_idx)));
                                }
                            }
                            let insert_pos = self.find_left_insert_position(*row);
                            for (offset, &r_idx) in matching_right.iter().enumerate() {
                                self.join_index
                                    .insert(insert_pos + offset, (Some(*row), Some(r_idx)));
                                modified = true;
                            }
                        } else if self.join_type == JoinType::Left
                            || self.join_type == JoinType::Full
                        {
                            let insert_pos = self.find_left_insert_position(*row);
                            self.join_index.insert(insert_pos, (Some(*row), None));
                            modified = true;
                        }
                    } else if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
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
        // No de-duplication against same-batch new lefts is needed: the
        // frame-mixing guard above rebuilds whenever both sides have
        // structural changes, so right inserts here never coexist with left
        // inserts.
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
                        .filter_map(|(l, r)| if *r == Some(*del_idx) { *l } else { None })
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
                    if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
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
                        let candidate_lefts: Vec<usize> =
                            lookup.get(&right_key).cloned().unwrap_or_default();
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
                            && (self.join_type == JoinType::Right
                                || self.join_type == JoinType::Full)
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
                        .filter_map(|(l, r)| if *r == Some(*row) { *l } else { None })
                        .collect();

                    let before_len = self.join_index.len();
                    self.join_index.retain(|(_, r)| *r != Some(*row));
                    if self.join_index.len() != before_len {
                        modified = true;
                    }

                    // LEFT/FULL: resurrect any newly orphaned left rows.
                    if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
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
                                let existing_null = self
                                    .join_index
                                    .iter()
                                    .position(|(l, r)| *l == Some(l_idx) && r.is_none());
                                if let Some(pos) = existing_null {
                                    self.join_index[pos] = (Some(l_idx), Some(*row));
                                } else {
                                    let insert_pos = self.find_right_insert_position(l_idx, *row);
                                    self.join_index
                                        .insert(insert_pos, (Some(l_idx), Some(*row)));
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
                    } else if self.join_type == JoinType::Right || self.join_type == JoinType::Full
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
