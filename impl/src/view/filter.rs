//! FilterView — predicate-filtered view over a parent table.

use crate::changeset::{
    apply_filter_cell_updated, apply_filter_row_deleted, apply_filter_row_inserted,
    IncrementalView, TableChange,
};
use crate::column::{ColumnType, ColumnValue};
use crate::readable::ReadableTable;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::RowPredicate;

/// A FilterView filters rows from the parent table based on a predicate.
/// Maintains a mapping from view indices to parent indices.
///
/// Supports incremental updates: when the parent is a root table, the view
/// consumes its changeset without a full rebuild. When the parent is itself
/// a view (no changeset), sync falls back to a version-checked rebuild.
pub struct FilterView {
    name: String,
    parent: Rc<RefCell<dyn ReadableTable>>,
    predicate: Box<RowPredicate>,
    view_to_parent: Vec<usize>,
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

impl FilterView {
    pub fn new<F>(name: String, parent: Rc<RefCell<dyn ReadableTable>>, predicate: F) -> Self
    where
        F: Fn(&HashMap<String, ColumnValue>) -> bool + 'static,
    {
        let (generation, change_count, parent_version) = {
            let p = parent.borrow();
            let (g, c) = match p.changeset() {
                Some(cs) => (cs.generation(), cs.total_len()),
                None => (0, usize::MAX),
            };
            (g, c, p.version())
        };
        let mut view = FilterView {
            name,
            parent,
            predicate: Box::new(predicate),
            view_to_parent: Vec::new(),
            last_synced_generation: generation,
            last_processed_change_count: change_count,
            sync_count: 0,
            last_parent_version: parent_version,
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

        if let Some(cs) = parent.changeset() {
            self.last_synced_generation = cs.generation();
            self.last_processed_change_count = cs.total_len();
        } else {
            self.last_processed_change_count = usize::MAX;
        }
        self.last_parent_version = parent.version();
        drop(parent);
        self.sync_count += 1;
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

        // Clone changes so we can drop the borrow
        let changes: Vec<TableChange> = changes.to_vec();
        drop(parent);

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

impl ReadableTable for FilterView {
    fn len(&self) -> usize {
        self.view_to_parent.len()
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
        FilterView::get_row(self, index)
    }

    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        FilterView::get_value(self, row, column)
    }

    fn get_value_by_index(&self, row: usize, col_idx: usize) -> Result<ColumnValue, String> {
        let parent_index = *self.view_to_parent.get(row).ok_or_else(|| {
            format!(
                "Row {} out of range [0, {})",
                row,
                self.view_to_parent.len()
            )
        })?;
        self.parent
            .borrow()
            .get_value_by_index(parent_index, col_idx)
    }

    fn version(&self) -> u64 {
        self.sync_count.wrapping_add(self.parent.borrow().version())
    }
}
