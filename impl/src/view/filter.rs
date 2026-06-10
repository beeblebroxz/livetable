//! FilterView — predicate-filtered view over a parent table.

use crate::changeset::{
    apply_filter_cell_updated, apply_filter_row_deleted, apply_filter_row_inserted,
    IncrementalView, TableChange,
};
use crate::column::ColumnValue;
use crate::table::Table;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::RowPredicate;

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
