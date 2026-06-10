//! ComputedView — adds an on-the-fly computed column to a parent table.

use crate::column::ColumnValue;
use crate::table::Table;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::ComputeFunction;

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
