//! SortedView — multi-key sorted view with incremental re-sort.

use crate::changeset::{IncrementalView, TableChange};
use crate::column::{ColumnType, ColumnValue};
use crate::readable::ReadableTable;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

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
// Manual Debug: the dyn ReadableTable parent prevents derive.
impl std::fmt::Debug for SortedView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortedView")
            .field("name", &self.name)
            .field("sort_keys", &self.sort_keys)
            .field("len", &self.sorted_index.len())
            .finish_non_exhaustive()
    }
}

pub struct SortedView {
    name: String,
    parent: Rc<RefCell<dyn ReadableTable>>,
    sort_keys: Vec<SortKey>,
    /// Sorted index: sorted_index[view_pos] = parent_row_index
    sorted_index: Vec<usize>,
    /// Last synced generation from parent's changeset (root-table parents)
    last_synced_generation: u64,
    /// Number of changes already processed (absolute index). `usize::MAX`
    /// when the parent is a view (no changeset) — a "not cursor-tracked"
    /// sentinel that keeps tick()'s min-cursor compaction folds correct.
    last_processed_change_count: usize,
    /// Own sync counter; the visible version() adds the parent's version.
    sync_count: u64,
    /// Parent version() observed at the last sync/rebuild.
    last_parent_version: u64,
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
        parent: Rc<RefCell<dyn ReadableTable>>,
        sort_keys: Vec<SortKey>,
    ) -> Result<Self, String> {
        // Validate sort keys exist
        {
            let table = parent.borrow();
            for key in &sort_keys {
                if table.column_index(&key.column).is_none() {
                    return Err(format!("Sort column '{}' not found in table", key.column));
                }
            }
        }

        if sort_keys.is_empty() {
            return Err("At least one sort key is required".to_string());
        }

        let (generation, change_count, parent_version) = {
            let p = parent.borrow();
            let (g, c) = match p.changeset() {
                Some(cs) => (cs.generation(), cs.total_len()),
                None => (0, usize::MAX),
            };
            (g, c, p.version())
        };
        let mut view = SortedView {
            name,
            parent,
            sort_keys,
            sorted_index: Vec::new(),
            last_synced_generation: generation,
            last_processed_change_count: change_count,
            sync_count: 0,
            last_parent_version: parent_version,
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
            .filter_map(|key| table.column_index(&key.column))
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

        if let Some(cs) = table.changeset() {
            self.last_synced_generation = cs.generation();
            self.last_processed_change_count = cs.total_len();
        } else {
            self.last_processed_change_count = usize::MAX;
        }
        self.last_parent_version = table.version();
        drop(table);
        self.sync_count += 1;
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
            return false;
        }

        let changes: Vec<TableChange> = changes.to_vec();
        drop(parent);

        // `find_insertion_position` reads the LIVE parent for an inserted/updated
        // row's sort key. That is only valid when the parent matches the
        // post-change state — i.e. a single change. In a multi-change batch the
        // parent has advanced past intermediate changes, so its rows/indices no
        // longer line up; fall back to a full rebuild, which is always correct.
        // (Caught by the forward_prop_fuzz batched differential test.)
        if changes.len() > 1 {
            self.rebuild_index();
            return true;
        }

        let modified = self.apply_changes(&changes);
        let parent = self.parent.borrow();
        if let Some(cs) = parent.changeset() {
            self.last_processed_change_count = cs.total_len();
            self.last_synced_generation = cs.generation();
        }
        self.last_parent_version = parent.version();
        drop(parent);
        self.sync_count += 1;
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

impl ReadableTable for SortedView {
    fn len(&self) -> usize {
        self.sorted_index.len()
    }

    fn column_names(&self) -> Vec<String> {
        self.parent.borrow().column_names()
    }

    fn column_index(&self, name: &str) -> Option<usize> {
        self.parent.borrow().column_index(name)
    }

    fn column_type(&self, col_idx: usize) -> Option<ColumnType> {
        self.parent.borrow().column_type(col_idx)
    }

    fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        SortedView::get_row(self, index)
    }

    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        SortedView::get_value(self, row, column)
    }

    fn get_value_by_index(&self, row: usize, col_idx: usize) -> Result<ColumnValue, String> {
        let parent_index = *self
            .sorted_index
            .get(row)
            .ok_or_else(|| format!("Row {} out of range [0, {})", row, self.sorted_index.len()))?;
        self.parent
            .borrow()
            .get_value_by_index(parent_index, col_idx)
    }

    fn version(&self) -> u64 {
        self.sync_count.wrapping_add(self.parent.borrow().version())
    }
}
