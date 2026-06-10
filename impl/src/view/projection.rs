//! ProjectionView — column-subset view over a parent table.

use crate::column::ColumnValue;
use crate::table::Table;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

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
