/// Changeset - Incremental Change Propagation for LiveTable
///
/// This module defines data structures for tracking changes to tables,
/// allowing views to update incrementally rather than rebuilding from scratch.
///
/// # Design Philosophy
///
/// When a table is modified, instead of views doing a full rebuild, they receive
/// a changeset describing what changed. Each view type can then efficiently update
/// its internal state based on the specific changes.
///
/// # Change Types
///
/// - `RowInserted`: A new row was added at a specific index
/// - `RowDeleted`: A row was removed from a specific index
/// - `CellUpdated`: A single cell value changed
///
/// # Usage Pattern
///
/// 1. Table operations generate `TableChange` events
/// 2. Changes accumulate in the table's changeset buffer
/// 3. Views call `apply_changes()` to process pending changes
/// 4. Views update their internal indices incrementally

use crate::column::ColumnValue;
use std::collections::HashMap;

/// Represents a single change to a table
#[derive(Debug, Clone)]
pub enum TableChange {
    /// A row was inserted at the given index
    /// Contains: (row_index, row_data)
    RowInserted {
        index: usize,
        data: HashMap<String, ColumnValue>,
    },

    /// A row was deleted from the given index
    /// Contains: (row_index, deleted_row_data)
    RowDeleted {
        index: usize,
        data: HashMap<String, ColumnValue>,
    },

    /// A cell value was updated
    /// Contains: (row_index, column_name, old_value, new_value)
    CellUpdated {
        row: usize,
        column: String,
        old_value: ColumnValue,
        new_value: ColumnValue,
    },
}

impl TableChange {
    /// Returns the row index affected by this change
    pub fn row_index(&self) -> usize {
        match self {
            TableChange::RowInserted { index, .. } => *index,
            TableChange::RowDeleted { index, .. } => *index,
            TableChange::CellUpdated { row, .. } => *row,
        }
    }

    /// Returns true if this change affects row indices after the changed row
    /// (i.e., inserts and deletes shift subsequent rows)
    pub fn shifts_indices(&self) -> bool {
        matches!(self, TableChange::RowInserted { .. } | TableChange::RowDeleted { .. })
    }
}

/// A collection of changes that can be applied to views
#[derive(Debug, Clone, Default)]
pub struct Changeset {
    changes: Vec<TableChange>,
    /// Generation counter - incremented each time changeset is cleared
    generation: u64,
}

impl Changeset {
    pub fn new() -> Self {
        Changeset {
            changes: Vec::new(),
            generation: 0,
        }
    }

    /// Add a change to the changeset
    pub fn push(&mut self, change: TableChange) {
        self.changes.push(change);
    }

    /// Returns all changes since the last clear
    pub fn changes(&self) -> &[TableChange] {
        &self.changes
    }

    /// Returns the current generation number
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Clear all changes and increment generation
    pub fn clear(&mut self) {
        self.changes.clear();
        self.generation += 1;
    }

    /// Returns true if there are no pending changes
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Returns the number of pending changes
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// Drain changes, returning ownership and clearing the buffer
    pub fn drain(&mut self) -> Vec<TableChange> {
        self.generation += 1;
        std::mem::take(&mut self.changes)
    }
}

/// Trait for views that support incremental updates
pub trait IncrementalView {
    /// Apply a set of changes from the parent table
    /// Returns true if the view was modified, false if no changes affected this view
    fn apply_changes(&mut self, changes: &[TableChange]) -> bool;

    /// Get the generation this view last synced to
    fn last_synced_generation(&self) -> u64;

    /// Force a full rebuild (fallback when incremental isn't possible)
    fn rebuild(&mut self);
}

/// Helper to adjust indices after an insert or delete
///
/// When a row is inserted at index I, all view indices >= I need to be incremented.
/// When a row is deleted at index I, all view indices > I need to be decremented,
/// and any index == I needs to be removed.
pub struct IndexAdjuster;

impl IndexAdjuster {
    /// Adjust a parent index after a row insertion
    /// Returns the new parent index
    pub fn adjust_for_insert(parent_index: usize, insert_index: usize) -> usize {
        if parent_index >= insert_index {
            parent_index + 1
        } else {
            parent_index
        }
    }

    /// Adjust a parent index after a row deletion
    /// Returns Some(new_index) or None if the index was the deleted row
    pub fn adjust_for_delete(parent_index: usize, delete_index: usize) -> Option<usize> {
        if parent_index == delete_index {
            None // This row was deleted
        } else if parent_index > delete_index {
            Some(parent_index - 1)
        } else {
            Some(parent_index)
        }
    }

    /// Adjust an entire index mapping for an insert
    pub fn adjust_mapping_for_insert(mapping: &mut Vec<usize>, insert_index: usize) {
        for parent_idx in mapping.iter_mut() {
            if *parent_idx >= insert_index {
                *parent_idx += 1;
            }
        }
    }

    /// Adjust an entire index mapping for a delete
    /// Returns indices in the mapping that need to be removed (were pointing to deleted row)
    pub fn adjust_mapping_for_delete(mapping: &mut Vec<usize>, delete_index: usize) -> Vec<usize> {
        let mut to_remove = Vec::new();

        for (view_idx, parent_idx) in mapping.iter_mut().enumerate() {
            if *parent_idx == delete_index {
                to_remove.push(view_idx);
            } else if *parent_idx > delete_index {
                *parent_idx -= 1;
            }
        }

        to_remove
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_changeset_basic() {
        let mut cs = Changeset::new();
        assert!(cs.is_empty());
        assert_eq!(cs.generation(), 0);

        cs.push(TableChange::RowInserted {
            index: 0,
            data: HashMap::new(),
        });

        assert!(!cs.is_empty());
        assert_eq!(cs.len(), 1);

        cs.clear();
        assert!(cs.is_empty());
        assert_eq!(cs.generation(), 1);
    }

    #[test]
    fn test_index_adjuster_insert() {
        // Insert at index 2
        // Indices 0, 1 stay the same
        // Indices 2, 3, 4 become 3, 4, 5
        assert_eq!(IndexAdjuster::adjust_for_insert(0, 2), 0);
        assert_eq!(IndexAdjuster::adjust_for_insert(1, 2), 1);
        assert_eq!(IndexAdjuster::adjust_for_insert(2, 2), 3);
        assert_eq!(IndexAdjuster::adjust_for_insert(3, 2), 4);
    }

    #[test]
    fn test_index_adjuster_delete() {
        // Delete at index 2
        // Indices 0, 1 stay the same
        // Index 2 returns None (deleted)
        // Indices 3, 4 become 2, 3
        assert_eq!(IndexAdjuster::adjust_for_delete(0, 2), Some(0));
        assert_eq!(IndexAdjuster::adjust_for_delete(1, 2), Some(1));
        assert_eq!(IndexAdjuster::adjust_for_delete(2, 2), None);
        assert_eq!(IndexAdjuster::adjust_for_delete(3, 2), Some(2));
        assert_eq!(IndexAdjuster::adjust_for_delete(4, 2), Some(3));
    }

    #[test]
    fn test_mapping_adjust_for_insert() {
        let mut mapping = vec![0, 2, 5, 7];
        IndexAdjuster::adjust_mapping_for_insert(&mut mapping, 3);
        // 0 stays 0, 2 stays 2, 5 becomes 6, 7 becomes 8
        assert_eq!(mapping, vec![0, 2, 6, 8]);
    }

    #[test]
    fn test_mapping_adjust_for_delete() {
        let mut mapping = vec![0, 2, 3, 5, 7];
        let removed = IndexAdjuster::adjust_mapping_for_delete(&mut mapping, 3);
        // Index 2 in mapping pointed to parent row 3, which was deleted
        assert_eq!(removed, vec![2]);
        // 0 stays 0, 2 stays 2, 3 is marked for removal, 5 becomes 4, 7 becomes 6
        assert_eq!(mapping, vec![0, 2, 3, 4, 6]); // Note: 3 still here, caller removes it
    }
}
