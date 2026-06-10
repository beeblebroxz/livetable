//! ComputedView — adds an on-the-fly computed column to a parent table.

use crate::column::{ColumnType, ColumnValue};
use crate::readable::ReadableTable;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::ComputeFunction;

/// A ComputedView adds a computed column to the parent table.
/// The computed column's value is calculated on-the-fly from other columns in each row.
///
/// Stateless pass-through: rows and versions are read live from the parent,
/// so it needs no sync() — children always see the parent's current state.
pub struct ComputedView {
    name: String,
    parent: Rc<RefCell<dyn ReadableTable>>,
    computed_col_name: String,
    compute_func: Box<ComputeFunction>,
}

impl ComputedView {
    pub fn new<F>(
        name: String,
        parent: Rc<RefCell<dyn ReadableTable>>,
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

impl ReadableTable for ComputedView {
    fn len(&self) -> usize {
        ComputedView::len(self)
    }

    fn column_names(&self) -> Vec<String> {
        let mut names = self.parent.borrow().column_names();
        names.push(self.computed_col_name.clone());
        names
    }

    fn column_type(&self, col_idx: usize) -> Option<ColumnType> {
        // The computed column's type is dynamic (whatever the closure
        // returns); parent columns keep their static types.
        let parent = self.parent.borrow();
        if col_idx < parent.column_names().len() {
            parent.column_type(col_idx)
        } else {
            None
        }
    }

    fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        ComputedView::get_row(self, index)
    }

    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        ComputedView::get_value(self, row, column)
    }

    fn version(&self) -> u64 {
        self.parent.borrow().version()
    }
}
