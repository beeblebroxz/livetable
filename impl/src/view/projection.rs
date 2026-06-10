//! ProjectionView — column-subset view over a parent table.

use crate::column::{ColumnType, ColumnValue};
use crate::readable::ReadableTable;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// A ProjectionView selects specific columns from the parent table.
///
/// Stateless pass-through: rows and versions are read live from the parent,
/// so it needs no sync() — children always see the parent's current state.
pub struct ProjectionView {
    name: String,
    parent: Rc<RefCell<dyn ReadableTable>>,
    selected_columns: Vec<String>,
}

impl ProjectionView {
    pub fn new(
        name: String,
        parent: Rc<RefCell<dyn ReadableTable>>,
        columns: Vec<String>,
    ) -> Result<Self, String> {
        // Validate columns exist
        {
            let parent_borrowed = parent.borrow();
            for col in &columns {
                if parent_borrowed.column_index(col).is_none() {
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

impl ReadableTable for ProjectionView {
    fn len(&self) -> usize {
        ProjectionView::len(self)
    }

    fn column_names(&self) -> Vec<String> {
        self.selected_columns.clone()
    }

    fn column_index(&self, name: &str) -> Option<usize> {
        self.selected_columns.iter().position(|c| c == name)
    }

    fn column_type(&self, col_idx: usize) -> Option<ColumnType> {
        let name = self.selected_columns.get(col_idx)?;
        let parent = self.parent.borrow();
        let parent_idx = parent.column_index(name)?;
        parent.column_type(parent_idx)
    }

    fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        ProjectionView::get_row(self, index)
    }

    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        ProjectionView::get_value(self, row, column)
    }

    fn version(&self) -> u64 {
        self.parent.borrow().version()
    }
}
