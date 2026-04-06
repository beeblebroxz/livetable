#![allow(clippy::useless_conversion)]
#![allow(non_snake_case)]

use pyo3::exceptions::{PyIndexError, PyKeyError, PyTypeError, PyValueError};
/// Python bindings for LiveTable using PyO3
///
/// This module provides Python-friendly APIs for the Rust implementation,
/// allowing Python code to use the high-performance Rust table system.
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySlice};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use crate::column::{ColumnType as RustColumnType, ColumnValue as RustColumnValue};
use crate::table::{Schema as RustSchema, StorageHint, Table as RustTable};
use crate::view::{
    AggregateFunction as RustAggregateFunction, AggregateView as RustAggregateView,
    JoinType as RustJoinType, JoinView as RustJoinView, SortKey as RustSortKey,
    SortOrder as RustSortOrder, SortedView as RustSortedView,
};

include!("python_bindings/conversions.rs");

// ============================================================================
// Schema
// ============================================================================

/// Python-exposed Schema class
#[pyclass(name = "Schema")]
#[derive(Clone)]
pub struct PySchema {
    inner: RustSchema,
}

#[pymethods]
impl PySchema {
    #[new]
    fn new(columns: Vec<(String, PyColumnType, bool)>) -> Self {
        let rust_columns: Vec<(String, RustColumnType, bool)> = columns
            .into_iter()
            .map(|(name, col_type, nullable)| (name, col_type.to_rust(), nullable))
            .collect();

        PySchema {
            inner: RustSchema::new(rust_columns),
        }
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        format!("Schema(columns={})", self.inner.len())
    }

    /// Get column names as a list
    fn get_column_names(&self) -> Vec<String> {
        self.inner
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Get column index by name
    fn get_column_index(&self, name: &str) -> Option<usize> {
        self.inner.get_column_index(name)
    }

    /// Get column info (name, type, nullable) by index
    fn get_column_info(&self, index: usize) -> Option<(String, PyColumnType, bool)> {
        self.inner
            .get_column_info(index)
            .map(|(name, col_type, nullable)| {
                (
                    name.to_string(),
                    PyColumnType::from_rust(col_type),
                    nullable,
                )
            })
    }
}

// ============================================================================
// View Registry for Automatic Propagation
// ============================================================================

/// Enum to hold references to different view types for automatic tick propagation
#[derive(Clone)]
enum RegisteredView {
    Filter(Weak<RefCell<PyFilterViewInner>>),
    Sorted(Weak<RefCell<RustSortedView>>),
    Aggregate(Weak<RefCell<RustAggregateView>>),
    JoinLeft(Weak<RefCell<RustJoinView>>),
    JoinRight(Weak<RefCell<RustJoinView>>),
}

enum ActiveRegisteredView {
    Filter(Rc<RefCell<PyFilterViewInner>>),
    Sorted(Rc<RefCell<RustSortedView>>),
    Aggregate(Rc<RefCell<RustAggregateView>>),
    JoinLeft(Rc<RefCell<RustJoinView>>),
    JoinRight(Rc<RefCell<RustJoinView>>),
}

impl RegisteredView {
    fn is_alive(&self) -> bool {
        match self {
            RegisteredView::Filter(inner) => inner.strong_count() > 0,
            RegisteredView::Sorted(inner) => inner.strong_count() > 0,
            RegisteredView::Aggregate(inner) => inner.strong_count() > 0,
            RegisteredView::JoinLeft(inner) => inner.strong_count() > 0,
            RegisteredView::JoinRight(inner) => inner.strong_count() > 0,
        }
    }

    fn upgrade(&self) -> Option<ActiveRegisteredView> {
        match self {
            RegisteredView::Filter(inner) => inner.upgrade().map(ActiveRegisteredView::Filter),
            RegisteredView::Sorted(inner) => inner.upgrade().map(ActiveRegisteredView::Sorted),
            RegisteredView::Aggregate(inner) => {
                inner.upgrade().map(ActiveRegisteredView::Aggregate)
            }
            RegisteredView::JoinLeft(inner) => inner.upgrade().map(ActiveRegisteredView::JoinLeft),
            RegisteredView::JoinRight(inner) => {
                inner.upgrade().map(ActiveRegisteredView::JoinRight)
            }
        }
    }
}

/// Inner state for PyFilterView that can be shared with the view registry
struct PyFilterViewInner {
    table_inner: Rc<RefCell<RustTable>>,
    predicate: PyObject,
    indices: Vec<usize>,
    last_synced_generation: u64,
    last_processed_change_count: usize,
}

impl PyFilterViewInner {
    /// Rebuild all indices by re-evaluating the predicate for all rows
    fn refresh(&mut self, py: Python) -> PyResult<()> {
        self.indices.clear();
        let table_ref = self.table_inner.borrow();

        for i in 0..table_ref.len() {
            if let Ok(row) = table_ref.get_row(i) {
                // Convert row to Python dict
                let dict = PyDict::new_bound(py);
                for (key, value) in row.iter() {
                    dict.set_item(key, column_value_to_py(py, value)?)?;
                }

                let result: bool = self.predicate.call1(py, (dict,))?.extract(py)?;
                if result {
                    self.indices.push(i);
                }
            }
        }

        self.last_synced_generation = table_ref.changeset_generation();
        self.last_processed_change_count = table_ref.changeset().total_len();
        Ok(())
    }

    fn sync(&mut self, py: Python) -> PyResult<bool> {
        use crate::changeset::TableChange;

        let table_ref = self.table_inner.borrow();
        let changes = match table_ref
            .changeset()
            .changes_from(self.last_processed_change_count)
        {
            Some(changes) => changes,
            None => {
                drop(table_ref);
                self.refresh(py)?;
                return Ok(true);
            }
        };

        if changes.is_empty() {
            return Ok(false);
        }

        let changes: Vec<TableChange> = changes.to_vec();
        drop(table_ref);

        let mut modified = false;
        for change in changes {
            match change {
                TableChange::RowInserted { index, data } => {
                    for idx in self.indices.iter_mut() {
                        if *idx >= index {
                            *idx += 1;
                        }
                    }

                    let dict = PyDict::new_bound(py);
                    for (key, value) in data.iter() {
                        dict.set_item(key, column_value_to_py(py, value)?)?;
                    }

                    let result: bool = self.predicate.call1(py, (dict,))?.extract(py)?;
                    if result {
                        let insert_pos = self
                            .indices
                            .iter()
                            .position(|&i| i > index)
                            .unwrap_or(self.indices.len());
                        self.indices.insert(insert_pos, index);
                        modified = true;
                    }
                }

                TableChange::RowDeleted { index, .. } => {
                    let mut to_remove = None;
                    for (view_idx, parent_idx) in self.indices.iter_mut().enumerate() {
                        if *parent_idx == index {
                            to_remove = Some(view_idx);
                        } else if *parent_idx > index {
                            *parent_idx -= 1;
                        }
                    }

                    if let Some(view_idx) = to_remove {
                        self.indices.remove(view_idx);
                        modified = true;
                    }
                }

                TableChange::CellUpdated { row, .. } => {
                    let currently_in_view = self.indices.contains(&row);
                    let table_ref = self.table_inner.borrow();
                    let now_matches = if let Ok(row_data) = table_ref.get_row(row) {
                        let dict = PyDict::new_bound(py);
                        for (key, value) in row_data.iter() {
                            dict.set_item(key, column_value_to_py(py, value)?)?;
                        }
                        drop(table_ref);
                        self.predicate.call1(py, (dict,))?.extract::<bool>(py)?
                    } else {
                        drop(table_ref);
                        false
                    };

                    match (currently_in_view, now_matches) {
                        (false, true) => {
                            let insert_pos = self
                                .indices
                                .iter()
                                .position(|&i| i > row)
                                .unwrap_or(self.indices.len());
                            self.indices.insert(insert_pos, row);
                            modified = true;
                        }
                        (true, false) => {
                            if let Some(pos) = self.indices.iter().position(|&i| i == row) {
                                self.indices.remove(pos);
                                modified = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        let table_ref = self.table_inner.borrow();
        self.last_processed_change_count = table_ref.changeset().total_len();
        self.last_synced_generation = table_ref.changeset_generation();
        Ok(modified)
    }
}

// ============================================================================
// Table
// ============================================================================

/// Python-exposed Table class
#[pyclass(name = "Table", unsendable)]
pub struct PyTable {
    inner: Rc<RefCell<RustTable>>,
    /// Registry of dependent views for automatic tick propagation
    registered_views: Rc<RefCell<Vec<RegisteredView>>>,
}

#[pymethods]
impl PyTable {
    /// Create a new table.
    ///
    /// Args:
    ///     name: Table name
    ///     schema: Table schema
    ///     storage: Storage optimization hint - "fast_reads" (default) or "fast_updates"
    ///     use_string_interning: Enable string interning for memory efficiency
    ///
    /// Examples:
    ///     # Default storage (optimized for append + read)
    ///     table = Table("logs", schema)
    ///
    ///     # Optimized for frequent inserts/deletes
    ///     table = Table("orderbook", schema, storage="fast_updates")
    #[new]
    #[pyo3(signature = (name, schema, storage=None, use_string_interning=false))]
    fn new(
        name: String,
        schema: PySchema,
        storage: Option<&str>,
        use_string_interning: bool,
    ) -> PyResult<Self> {
        let hint = match storage {
            None => StorageHint::FastReads,
            Some(s) => s.parse::<StorageHint>().map_err(PyValueError::new_err)?,
        };

        Ok(PyTable {
            inner: Rc::new(RefCell::new(RustTable::with_hint_and_interning(
                name,
                schema.inner,
                hint,
                use_string_interning,
            ))),
            registered_views: Rc::new(RefCell::new(Vec::new())),
        })
    }

    fn __len__(&self) -> usize {
        self.inner.borrow().len()
    }

    fn __repr__(&self) -> String {
        let table = self.inner.borrow();
        format!(
            "Table(name='{}', rows={}, columns={})",
            table.name(),
            table.len(),
            table.schema().get_column_names().len()
        )
    }

    /// Get table name
    fn name(&self) -> String {
        self.inner.borrow().name().to_string()
    }

    /// Check if table is empty
    fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    /// Get column names
    fn column_names(&self) -> Vec<String> {
        self.inner
            .borrow()
            .schema()
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Append a row to the table
    fn append_row(&mut self, _py: Python, row: &Bound<'_, PyDict>) -> PyResult<()> {
        // Build schema lookup upfront
        let col_info: HashMap<String, (RustColumnType, bool)> = {
            let table = self.inner.borrow();
            let schema = table.schema();
            schema
                .get_column_names()
                .iter()
                .filter_map(|name| {
                    let col_type = schema.get_column_type(name)?;
                    let nullable = schema.is_column_nullable(name).unwrap_or(false);
                    Some((name.to_string(), (col_type, nullable)))
                })
                .collect()
        };

        let mut rust_row = HashMap::new();
        for (key, value) in row.iter() {
            let key_str: String = key.extract()?;

            // Use schema-aware typed conversion
            let col_value = if let Some((col_type, nullable)) = col_info.get(&key_str) {
                py_to_column_value_typed(&value, *col_type, *nullable)?
            } else {
                return Err(PyValueError::new_err(format!(
                    "Unknown column: {}",
                    key_str
                )));
            };

            rust_row.insert(key_str, col_value);
        }

        self.inner
            .borrow_mut()
            .append_row(rust_row)
            .map_err(PyValueError::new_err)
    }

    /// Append multiple rows at once (bulk insert).
    ///
    /// This is more efficient than calling `append_row` repeatedly.
    ///
    /// Args:
    ///     rows: A list of dictionaries, where each dict represents a row
    ///
    /// Returns:
    ///     The number of rows successfully inserted
    ///
    /// Raises:
    ///     ValueError: If any row is invalid (no rows are inserted on error)
    ///
    /// Example:
    ///     count = table.append_rows([
    ///         {"id": 1, "name": "Alice"},
    ///         {"id": 2, "name": "Bob"},
    ///         {"id": 3, "name": "Charlie"},
    ///     ])
    fn append_rows(&mut self, _py: Python, rows: &Bound<'_, PyList>) -> PyResult<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Build schema lookup: column_name -> (type, nullable)
        // This avoids repeated schema lookups per row
        let table = self.inner.borrow();
        let schema = table.schema();
        let col_names: Vec<String> = schema
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let col_info: HashMap<String, (RustColumnType, bool)> = col_names
            .iter()
            .filter_map(|name| {
                let col_type = schema.get_column_type(name)?;
                let nullable = schema.is_column_nullable(name).unwrap_or(false);
                Some((name.clone(), (col_type, nullable)))
            })
            .collect();
        drop(table); // Release borrow before mutating

        let mut rust_rows = Vec::with_capacity(rows.len());

        for item in rows.iter() {
            let row_dict = item
                .downcast::<PyDict>()
                .map_err(|_| PyValueError::new_err("Each row must be a dictionary"))?;

            let mut rust_row = HashMap::new();
            for (key, value) in row_dict.iter() {
                let key_str: String = key.extract()?;

                // Use schema-aware typed conversion (faster than guessing)
                let col_value = if let Some((col_type, nullable)) = col_info.get(&key_str) {
                    py_to_column_value_typed(&value, *col_type, *nullable)?
                } else {
                    return Err(PyValueError::new_err(format!(
                        "Unknown column: {}",
                        key_str
                    )));
                };

                rust_row.insert(key_str, col_value);
            }
            rust_rows.push(rust_row);
        }

        self.inner
            .borrow_mut()
            .append_rows(rust_rows)
            .map_err(PyValueError::new_err)
    }

    /// Insert a row at a specific index
    fn insert_row(&mut self, _py: Python, index: usize, row: &Bound<'_, PyDict>) -> PyResult<()> {
        // Build schema lookup upfront
        let col_info: HashMap<String, (RustColumnType, bool)> = {
            let table = self.inner.borrow();
            let schema = table.schema();
            schema
                .get_column_names()
                .iter()
                .filter_map(|name| {
                    let col_type = schema.get_column_type(name)?;
                    let nullable = schema.is_column_nullable(name).unwrap_or(false);
                    Some((name.to_string(), (col_type, nullable)))
                })
                .collect()
        };

        let mut rust_row = HashMap::new();

        for (key, value) in row.iter() {
            let key_str: String = key.extract()?;

            // Use schema-aware typed conversion
            let col_value = if let Some((col_type, nullable)) = col_info.get(&key_str) {
                py_to_column_value_typed(&value, *col_type, *nullable)?
            } else {
                return Err(PyValueError::new_err(format!(
                    "Unknown column: {}",
                    key_str
                )));
            };
            rust_row.insert(key_str, col_value);
        }

        self.inner
            .borrow_mut()
            .insert_row(index, rust_row)
            .map_err(PyValueError::new_err)
    }

    /// Delete a row at a specific index
    fn delete_row(&mut self, index: usize) -> PyResult<()> {
        self.inner
            .borrow_mut()
            .delete_row(index)
            .map(|_| ()) // Discard the returned row
            .map_err(PyIndexError::new_err)
    }

    /// Get a value at (row, column)
    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = self
            .inner
            .borrow()
            .get_value(row, column)
            .map_err(PyKeyError::new_err)?;

        column_value_to_py(py, &value)
    }

    /// Set a value at (row, column)
    fn set_value(
        &mut self,
        _py: Python,
        row: usize,
        column: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (col_type, nullable) = {
            let table = self.inner.borrow();
            let schema = table.schema();
            let col_type = schema
                .get_column_type(column)
                .ok_or_else(|| PyValueError::new_err(format!("Unknown column: {}", column)))?;
            let nullable = schema.is_column_nullable(column).unwrap_or(false);
            (col_type, nullable)
        };

        let col_value = py_to_column_value_typed(value, col_type, nullable)?;

        self.inner
            .borrow_mut()
            .set_value(row, column, col_value)
            .map_err(PyValueError::new_err)
    }

    /// Get a full row as a dictionary
    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let row = self
            .inner
            .borrow()
            .get_row(index)
            .map_err(PyIndexError::new_err)?;

        let dict = PyDict::new_bound(py);
        for (key, value) in row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        Ok(dict.to_object(py))
    }

    /// Index access with multiple modes:
    /// - table[idx] returns the row at idx (supports negative indexing)
    /// - table[start:end] returns a list of rows (slicing)
    /// - table["column_name"] returns all values in that column as a list
    fn __getitem__(&self, py: Python, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let table_len = self.inner.borrow().len();

        // Try integer index first (supports negative indexing)
        if let Ok(mut idx) = key.extract::<isize>() {
            // Handle negative indexing
            if idx < 0 {
                idx += table_len as isize;
            }
            if idx < 0 || idx as usize >= table_len {
                return Err(PyIndexError::new_err(format!(
                    "index {} out of range for table with {} rows",
                    key, table_len
                )));
            }
            return self.get_row(py, idx as usize);
        }

        // Try slice (supports negative step for reverse slicing)
        if let Ok(slice) = key.downcast::<PySlice>() {
            let indices = slice.indices(table_len as isize)?;
            let start = indices.start;
            let stop = indices.stop;
            let step = indices.step;

            let list = PyList::empty_bound(py);
            let mut i = start;
            if step > 0 {
                while i < stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            } else {
                while i > stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            }
            return Ok(list.to_object(py));
        }

        // Try string for column access
        if let Ok(column_name) = key.extract::<String>() {
            let table = self.inner.borrow();
            let column_names = table.schema().get_column_names();
            if !column_names.iter().any(|c| c == &column_name) {
                return Err(PyKeyError::new_err(format!(
                    "Column '{}' not found. Available columns: {:?}",
                    column_name, column_names
                )));
            }

            let list = PyList::empty_bound(py);
            for i in 0..table_len {
                let value = table
                    .get_value(i, &column_name)
                    .map_err(PyIndexError::new_err)?;
                list.append(column_value_to_py(py, &value)?)?;
            }
            return Ok(list.to_object(py));
        }

        Err(PyTypeError::new_err(
            "indices must be integers, slices, or column names (strings)",
        ))
    }

    /// Display table as formatted string
    fn display(&self) -> String {
        let table = self.inner.borrow();
        let mut output = format!("Table: {}\n", table.name());
        output.push_str(&format!("Rows: {}\n", table.len()));

        let column_names = table.schema().get_column_names();
        output.push_str(&format!("Columns: {:?}\n", column_names));

        output
    }

    /// Create a filter view (auto-registered for tick() propagation)
    fn filter(&self, predicate: PyObject) -> PyResult<PyFilterView> {
        let view = PyFilterView::new(self.clone(), predicate)?;

        // Register the view's inner state for automatic tick() propagation
        self.registered_views
            .borrow_mut()
            .push(RegisteredView::Filter(Rc::downgrade(&view.inner)));

        Ok(view)
    }

    /// Filter rows using an expression string (faster than lambda-based filter).
    ///
    /// The expression is evaluated entirely in Rust without Python callbacks,
    /// making it significantly faster for large datasets.
    ///
    /// Supported syntax:
    /// - Comparisons: `score > 90`, `name == 'Alice'`, `value != 0`
    /// - Logical operators: `AND`, `OR`, `NOT`
    /// - Parentheses: `(score > 90) AND (age >= 18)`
    /// - NULL checks: `column IS NULL`, `column IS NOT NULL`
    ///
    /// Returns a list of matching row indices.
    ///
    /// Example:
    ///     indices = table.filter_expr("score > 90 AND name != 'Bob'")
    ///     for idx in indices:
    ///         print(table.get_row(idx))
    fn filter_expr(&self, expression: &str) -> PyResult<Vec<usize>> {
        self.inner
            .borrow()
            .filter_expr(expression)
            .map_err(PyValueError::new_err)
    }

    /// Create a projection view (select specific columns)
    fn select(&self, columns: Vec<String>) -> PyResult<PyProjectionView> {
        PyProjectionView::new(self.clone(), columns)
    }

    /// Create a computed view with an additional computed column
    fn add_computed_column(&self, name: String, compute_fn: PyObject) -> PyResult<PyComputedView> {
        PyComputedView::new(self.clone(), name, compute_fn)
    }

    // === Simplified View Methods ===

    /// Sort table by one or more columns.
    ///
    /// Args:
    ///     by: Column name (str) or list of column names to sort by
    ///     descending: If True, sort descending. Can be bool or list of bools
    ///                 matching the columns. Default: False (ascending)
    ///
    /// Returns:
    ///     A sorted view of the table
    ///
    /// Examples:
    ///     # Sort by single column ascending
    ///     sorted_table = table.sort("score")
    ///
    ///     # Sort by single column descending
    ///     sorted_table = table.sort("score", descending=True)
    ///
    ///     # Sort by multiple columns
    ///     sorted_table = table.sort(["name", "score"], descending=[False, True])
    #[pyo3(signature = (by, descending=None))]
    fn sort(
        &self,
        by: &Bound<'_, PyAny>,
        descending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PySortedView> {
        // Extract column name(s)
        let columns = extract_string_or_list(by)?;

        // Extract descending flag(s)
        let desc_flags: Vec<bool> = match descending {
            None => vec![false; columns.len()],
            Some(desc) => {
                if let Ok(single) = desc.extract::<bool>() {
                    vec![single; columns.len()]
                } else if let Ok(list) = desc.extract::<Vec<bool>>() {
                    if list.len() != columns.len() {
                        return Err(PyValueError::new_err(format!(
                            "descending list length ({}) must match columns length ({})",
                            list.len(),
                            columns.len()
                        )));
                    }
                    list
                } else {
                    return Err(PyTypeError::new_err(
                        "descending must be a bool or list of bools",
                    ));
                }
            }
        };

        // Build sort keys (nulls_first=true matches SQL standard)
        let sort_keys: Vec<RustSortKey> = columns
            .iter()
            .zip(desc_flags.iter())
            .map(|(col, desc)| {
                let order = if *desc {
                    RustSortOrder::Descending
                } else {
                    RustSortOrder::Ascending
                };
                RustSortKey::new(col.clone(), order, true)
            })
            .collect();

        // Auto-generate name
        let name = format!("{}_sorted", self.inner.borrow().name());

        let view = RustSortedView::new(name, self.inner.clone(), sort_keys)
            .map_err(PyValueError::new_err)?;

        let inner = Rc::new(RefCell::new(view));

        // Register the view's inner state for automatic tick() propagation
        self.registered_views
            .borrow_mut()
            .push(RegisteredView::Sorted(Rc::downgrade(&inner)));

        Ok(PySortedView { inner })
    }

    /// Join this table with another table.
    ///
    /// Args:
    ///     other: The table to join with
    ///     on: Column name(s) for join key (if same in both tables)
    ///     left_on: Column name(s) in this table (if different from right)
    ///     right_on: Column name(s) in other table (if different from left)
    ///     how: Join type - "left", "inner", "right", "full", or "outer" (default: "left")
    ///
    /// Returns:
    ///     A joined view of the two tables
    ///
    /// Examples:
    ///     # Simple join on same column name
    ///     joined = students.join(enrollments, on="student_id")
    ///
    ///     # Join with different column names
    ///     joined = students.join(enrollments, left_on="id", right_on="student_id")
    ///
    ///     # Inner join
    ///     joined = students.join(enrollments, on="id", how="inner")
    ///
    ///     # Multi-column join (composite key)
    ///     joined = sales.join(targets, on=["year", "month", "region"])
    #[pyo3(signature = (other, on=None, left_on=None, right_on=None, how="left"))]
    fn join(
        &self,
        other: PyTable,
        on: Option<&Bound<'_, PyAny>>,
        left_on: Option<&Bound<'_, PyAny>>,
        right_on: Option<&Bound<'_, PyAny>>,
        how: &str,
    ) -> PyResult<PyJoinView> {
        // Determine left and right keys
        let (left_keys, right_keys) = match (on, left_on, right_on) {
            (Some(on_cols), None, None) => {
                let cols = extract_string_or_list(on_cols)?;
                (cols.clone(), cols)
            }
            (None, Some(left), Some(right)) => (
                extract_string_or_list(left)?,
                extract_string_or_list(right)?,
            ),
            (None, None, None) => {
                return Err(PyValueError::new_err(
                    "Must specify either 'on' or both 'left_on' and 'right_on'",
                ));
            }
            _ => {
                return Err(PyValueError::new_err(
                    "Cannot specify both 'on' and 'left_on'/'right_on'",
                ));
            }
        };

        // Parse join type
        let join_type = match how.to_lowercase().as_str() {
            "left" => RustJoinType::Left,
            "inner" => RustJoinType::Inner,
            "right" => RustJoinType::Right,
            "full" | "outer" | "full_outer" => RustJoinType::Full,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unknown join type '{}'. Use 'left', 'inner', 'right', 'full', or 'outer'",
                    how
                )))
            }
        };

        // Auto-generate name
        let name = format!(
            "{}_{}_join",
            self.inner.borrow().name(),
            other.inner.borrow().name()
        );

        let join = RustJoinView::new_multi(
            name,
            self.inner.clone(),
            other.inner.clone(),
            left_keys,
            right_keys,
            join_type,
        )
        .map_err(PyValueError::new_err)?;

        let join_rc = Rc::new(RefCell::new(join));

        // Register with both parent tables for tick() propagation
        self.registered_views
            .borrow_mut()
            .push(RegisteredView::JoinLeft(Rc::downgrade(&join_rc)));
        other
            .registered_views
            .borrow_mut()
            .push(RegisteredView::JoinRight(Rc::downgrade(&join_rc)));

        Ok(PyJoinView { inner: join_rc })
    }

    /// Group table by columns and compute aggregations.
    ///
    /// Args:
    ///     by: Column name (str) or list of column names to group by
    ///     agg: List of aggregation tuples: (result_name, source_column, function)
    ///          where function is "sum", "avg", "min", "max", or "count"
    ///
    /// Returns:
    ///     An aggregate view with computed groups
    ///
    /// Examples:
    ///     # Single aggregation
    ///     totals = table.group_by("department", agg=[("total", "salary", "sum")])
    ///
    ///     # Multiple aggregations
    ///     stats = table.group_by("department", agg=[
    ///         ("total_salary", "salary", "sum"),
    ///         ("avg_salary", "salary", "avg"),
    ///         ("headcount", "id", "count"),
    ///     ])
    ///
    ///     # Group by multiple columns
    ///     stats = table.group_by(["year", "month"], agg=[("total", "sales", "sum")])
    #[pyo3(signature = (by, agg))]
    fn group_by(
        &self,
        by: &Bound<'_, PyAny>,
        agg: Vec<(String, String, String)>,
    ) -> PyResult<PyAggregateView> {
        // Extract group-by columns
        let group_cols = extract_string_or_list(by)?;

        // Convert string function names to RustAggregateFunction
        let aggregations: Vec<(String, String, RustAggregateFunction)> = agg.iter()
            .map(|(result_name, source_col, func_str)| {
                let func = match func_str.to_lowercase().as_str() {
                    "sum" => RustAggregateFunction::Sum,
                    "avg" | "average" | "mean" => RustAggregateFunction::Avg,
                    "min" | "minimum" => RustAggregateFunction::Min,
                    "max" | "maximum" => RustAggregateFunction::Max,
                    "count" => RustAggregateFunction::Count,
                    "median" | "med" => RustAggregateFunction::Median,
                    "p25" => RustAggregateFunction::Percentile(0.25),
                    "p50" => RustAggregateFunction::Percentile(0.50),
                    "p75" => RustAggregateFunction::Percentile(0.75),
                    "p90" => RustAggregateFunction::Percentile(0.90),
                    "p95" => RustAggregateFunction::Percentile(0.95),
                    "p99" => RustAggregateFunction::Percentile(0.99),
                    other => {
                        // Try to parse "percentile(X.XX)" format
                        if let Some(inner) = other.strip_prefix("percentile(")
                            .and_then(|s| s.strip_suffix(")"))
                        {
                            let p: f64 = inner.parse().map_err(|_| PyValueError::new_err(
                                format!("Invalid percentile value '{}' in '{}'", inner, func_str)
                            ))?;
                            if !(0.0..=1.0).contains(&p) {
                                return Err(PyValueError::new_err(
                                    format!("Percentile value must be between 0.0 and 1.0, got {}", p)
                                ));
                            }
                            RustAggregateFunction::Percentile(p)
                        } else {
                            return Err(PyValueError::new_err(format!(
                                "Unknown aggregation function '{}'. Use: sum, avg, min, max, count, median, p25, p50, p75, p90, p95, p99, or percentile(0.XX)",
                                func_str
                            )));
                        }
                    }
                };
                Ok((result_name.clone(), source_col.clone(), func))
            })
            .collect::<PyResult<Vec<_>>>()?;

        // Auto-generate name
        let name = format!("{}_grouped", self.inner.borrow().name());

        let view = RustAggregateView::new(name, self.inner.clone(), group_cols, aggregations)
            .map_err(PyValueError::new_err)?;

        let inner = Rc::new(RefCell::new(view));

        // Register the view's inner state for automatic tick() propagation
        self.registered_views
            .borrow_mut()
            .push(RegisteredView::Aggregate(Rc::downgrade(&inner)));

        Ok(PyAggregateView { inner })
    }

    // === Changeset API for incremental propagation ===

    /// Check if there are pending changes
    fn has_pending_changes(&self) -> bool {
        self.inner.borrow().has_pending_changes()
    }

    /// Get the current changeset generation number
    fn changeset_generation(&self) -> u64 {
        self.inner.borrow().changeset_generation()
    }

    /// Clear pending changes without processing them
    fn clear_changeset(&mut self) {
        self.inner.borrow_mut().clear_changeset();
    }

    /// Get the number of pending changes
    fn pending_changes_count(&self) -> usize {
        self.inner.borrow().changeset().len()
    }

    // === String Interning API ===

    /// Check if this table uses string interning
    fn uses_string_interning(&self) -> bool {
        self.inner.borrow().uses_string_interning()
    }

    /// Get string interning statistics
    /// Returns a dict with: unique_strings, total_references, free_slots, memory_bytes
    /// Returns None if string interning is not enabled
    fn interner_stats(&self, py: Python) -> PyResult<Option<PyObject>> {
        let stats = self.inner.borrow().interner_stats();
        match stats {
            Some(s) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("unique_strings", s.unique_strings)?;
                dict.set_item("total_references", s.total_references)?;
                dict.set_item("free_slots", s.free_slots)?;
                dict.set_item("memory_bytes", s.memory_bytes)?;
                Ok(Some(dict.to_object(py)))
            }
            None => Ok(None),
        }
    }

    // === Aggregation Methods ===

    /// Calculate the sum of all numeric values in a column.
    /// NULL values are skipped.
    fn sum(&self, column: &str) -> PyResult<f64> {
        self.inner
            .borrow()
            .sum(column)
            .map_err(PyValueError::new_err)
    }

    /// Count the number of non-NULL values in a column.
    fn count_non_null(&self, column: &str) -> PyResult<usize> {
        self.inner
            .borrow()
            .count_non_null(column)
            .map_err(PyValueError::new_err)
    }

    /// Calculate the average of all numeric values in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    fn avg(&self, column: &str) -> PyResult<Option<f64>> {
        self.inner
            .borrow()
            .avg(column)
            .map_err(PyValueError::new_err)
    }

    /// Find the minimum numeric value in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    fn min(&self, column: &str) -> PyResult<Option<f64>> {
        self.inner
            .borrow()
            .min(column)
            .map_err(PyValueError::new_err)
    }

    /// Find the maximum numeric value in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    fn max(&self, column: &str) -> PyResult<Option<f64>> {
        self.inner
            .borrow()
            .max(column)
            .map_err(PyValueError::new_err)
    }

    // === Serialization Methods ===

    /// Export table to CSV string.
    ///
    /// Returns a CSV string with headers and data rows.
    /// NULL values become empty strings.
    /// Strings containing commas, quotes, or newlines are properly escaped.
    ///
    /// Example:
    ///     csv_str = table.to_csv()
    ///     with open("data.csv", "w") as f:
    ///         f.write(csv_str)
    fn to_csv(&self) -> String {
        self.inner.borrow().to_csv()
    }

    /// Export table to JSON string (array of objects).
    ///
    /// Returns a pretty-printed JSON string representing the table as an array
    /// of objects, where each object is a row with column names as keys.
    ///
    /// Example:
    ///     json_str = table.to_json()
    ///     with open("data.json", "w") as f:
    ///         f.write(json_str)
    fn to_json(&self) -> PyResult<String> {
        self.inner.borrow().to_json().map_err(PyValueError::new_err)
    }

    /// Create a table from a CSV string.
    ///
    /// The first line is treated as the header row containing column names.
    /// Column types are inferred from the first data row:
    /// - Numbers that fit in i32 → INT32
    /// - Larger integers → INT64
    /// - Numbers with decimals → FLOAT64
    /// - "true"/"false" (case-insensitive) → BOOL
    /// - Everything else → STRING
    ///
    /// All columns are created as nullable.
    ///
    /// Example:
    ///     with open("data.csv", "r") as f:
    ///         table = livetable.Table.from_csv("my_table", f.read())
    #[staticmethod]
    fn from_csv(name: &str, csv: &str) -> PyResult<Self> {
        let table = RustTable::from_csv(name, csv).map_err(PyValueError::new_err)?;
        Ok(PyTable {
            inner: Rc::new(RefCell::new(table)),
            registered_views: Rc::new(RefCell::new(Vec::new())),
        })
    }

    /// Create a table from a JSON string (array of objects).
    ///
    /// Expects a JSON array where each element is an object representing a row.
    /// Column types are inferred from the first object.
    /// All columns are created as nullable.
    ///
    /// Example:
    ///     with open("data.json", "r") as f:
    ///         table = livetable.Table.from_json("my_table", f.read())
    #[staticmethod]
    fn from_json(name: &str, json: &str) -> PyResult<Self> {
        let table = RustTable::from_json(name, json).map_err(PyValueError::new_err)?;
        Ok(PyTable {
            inner: Rc::new(RefCell::new(table)),
            registered_views: Rc::new(RefCell::new(Vec::new())),
        })
    }

    // === Pandas Interop Methods ===

    /// Convert table to a pandas DataFrame.
    ///
    /// Requires pandas to be installed. If pandas is not available,
    /// raises ImportError.
    ///
    /// Returns:
    ///     A pandas DataFrame containing all table data
    ///
    /// Example:
    ///     df = table.to_pandas()
    ///     print(df.describe())
    fn to_pandas(&self, py: Python) -> PyResult<PyObject> {
        // Import pandas
        let pandas = py.import_bound("pandas").map_err(|_| {
            PyValueError::new_err("pandas is not installed. Install with: pip install pandas")
        })?;

        // Collect all data into owned Rust structures while holding the borrow,
        // then release the borrow before making Python calls (avoids RefCell
        // conflicts if Python GC triggers a finalizer that accesses this table).
        let (column_names, column_data): (Vec<String>, Vec<Vec<RustColumnValue>>) = {
            let table = self.inner.borrow();
            let names: Vec<String> = table
                .schema()
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let mut data = Vec::with_capacity(names.len());
            for col_name in &names {
                let mut col_values = Vec::with_capacity(table.len());
                for row_idx in 0..table.len() {
                    let value = table
                        .get_value(row_idx, col_name)
                        .map_err(PyValueError::new_err)?;
                    col_values.push(value);
                }
                data.push(col_values);
            }
            (names, data)
        }; // borrow released here

        // Now build Python objects with no borrow held
        let data_dict = PyDict::new_bound(py);
        for (col_name, col_values) in column_names.iter().zip(column_data.iter()) {
            let mut py_values: Vec<PyObject> = Vec::with_capacity(col_values.len());
            for value in col_values {
                py_values.push(column_value_to_py(py, value)?);
            }
            let py_list = PyList::new_bound(py, py_values);
            data_dict.set_item(col_name.as_str(), py_list)?;
        }

        // Create DataFrame
        let df = pandas.call_method1("DataFrame", (data_dict,))?;
        Ok(df.to_object(py))
    }

    /// Create a table from a pandas DataFrame.
    ///
    /// Column types are inferred from DataFrame dtypes:
    /// - int32, int64 → INT32, INT64
    /// - float32, float64 → FLOAT32, FLOAT64
    /// - bool → BOOL
    /// - object, string → STRING
    ///
    /// All columns are created as nullable to handle NaN values.
    ///
    /// Args:
    ///     name: Name for the new table
    ///     df: A pandas DataFrame
    ///
    /// Returns:
    ///     A new Table containing the DataFrame data
    ///
    /// Example:
    ///     import pandas as pd
    ///     df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    ///     table = livetable.Table.from_pandas("users", df)
    #[staticmethod]
    fn from_pandas(py: Python, name: &str, df: &Bound<'_, PyAny>) -> PyResult<Self> {
        // Get column names
        let columns = df.getattr("columns")?;
        let column_list: Vec<String> = columns.call_method0("tolist")?.extract()?;

        if column_list.is_empty() {
            return Err(PyValueError::new_err("DataFrame has no columns"));
        }

        // Get dtypes and build schema
        let dtypes = df.getattr("dtypes")?;
        let mut schema_cols = Vec::new();
        let mut col_types = Vec::new();

        for col_name in &column_list {
            let dtype = dtypes.get_item(col_name.as_str())?;
            let dtype_str: String = dtype.str()?.extract()?;

            let col_type = dtype_str_to_column_type(&dtype_str);
            col_types.push(col_type);
            schema_cols.push((col_name.clone(), col_type, true)); // All nullable
        }

        let schema = RustSchema::new(schema_cols);
        let mut table = RustTable::new(name.to_string(), schema);

        // Iterate over rows using itertuples for efficiency
        let itertuples = df.call_method1("itertuples", (false,))?;

        for row_tuple in itertuples.iter()? {
            let row_tuple = row_tuple?;
            let mut rust_row = HashMap::new();

            for (i, col_name) in column_list.iter().enumerate() {
                let value = row_tuple.get_item(i)?;
                let expected_type = col_types[i];
                let col_value = pandas_value_to_column_value(py, &value, expected_type)?;
                rust_row.insert(col_name.clone(), col_value);
            }

            table.append_row(rust_row).map_err(PyValueError::new_err)?;
        }

        Ok(PyTable {
            inner: Rc::new(RefCell::new(table)),
            registered_views: Rc::new(RefCell::new(Vec::new())),
        })
    }

    /// Return an iterator over the table rows.
    /// Enables: `for row in table:`
    fn __iter__(slf: PyRef<'_, Self>) -> PyTableIterator {
        PyTableIterator {
            table: slf.clone(),
            index: 0,
            length: slf.inner.borrow().len(),
        }
    }

    /// Synchronize all registered dependent views with pending changes.
    ///
    /// This method propagates changes from the table through all views that were
    /// created using the simplified API (filter(), sort(), group_by()).
    ///
    /// Returns the number of views that were synced.
    fn tick(&self, py: Python) -> PyResult<usize> {
        // Check if there are pending changes
        let has_changes = self.inner.borrow().has_pending_changes();

        if !has_changes {
            return Ok(0);
        }

        let views = self.live_registered_views();
        let mut synced_count = 0;
        let mut min_cursor = self.inner.borrow().changeset().total_len();

        if views.is_empty() {
            self.inner.borrow_mut().clear_changeset();
            return Ok(0);
        }

        for view in views {
            match view {
                ActiveRegisteredView::Filter(inner) => {
                    inner.borrow_mut().sync(py)?;
                    min_cursor = min_cursor.min(inner.borrow().last_processed_change_count);
                    synced_count += 1;
                }
                ActiveRegisteredView::Sorted(inner) => {
                    inner.borrow_mut().sync();
                    min_cursor = min_cursor.min(inner.borrow().last_processed_change_count());
                    synced_count += 1;
                }
                ActiveRegisteredView::Aggregate(inner) => {
                    inner.borrow_mut().sync();
                    min_cursor = min_cursor.min(inner.borrow().last_processed_change_count());
                    synced_count += 1;
                }
                ActiveRegisteredView::JoinLeft(inner) => {
                    inner.borrow_mut().sync();
                    let (left_cursor, _) = inner.borrow().last_processed_change_count();
                    min_cursor = min_cursor.min(left_cursor);
                    synced_count += 1;
                }
                ActiveRegisteredView::JoinRight(inner) => {
                    inner.borrow_mut().sync();
                    let (_, right_cursor) = inner.borrow().last_processed_change_count();
                    min_cursor = min_cursor.min(right_cursor);
                    synced_count += 1;
                }
            }
        }

        // Compact changes that all registered views have already processed
        self.inner.borrow_mut().compact_changeset(min_cursor);

        Ok(synced_count)
    }

    /// Get the number of registered views that will be synced by tick().
    fn registered_view_count(&self) -> usize {
        self.live_registered_views().len()
    }
}

impl Clone for PyTable {
    fn clone(&self) -> Self {
        PyTable {
            inner: Rc::clone(&self.inner),
            registered_views: Rc::clone(&self.registered_views),
        }
    }
}

impl PyTable {
    fn live_registered_views(&self) -> Vec<ActiveRegisteredView> {
        let mut views = self.registered_views.borrow_mut();
        views.retain(RegisteredView::is_alive);
        views.iter().filter_map(RegisteredView::upgrade).collect()
    }
}

// ============================================================================
// FilterView
// ============================================================================

/// FilterView uses shared inner state so tick() can update registered views.
#[pyclass(name = "FilterView", unsendable)]
pub struct PyFilterView {
    /// Reference to the parent table (for get_row operations)
    table: PyTable,
    /// Shared inner state (can be registered with table for tick())
    inner: Rc<RefCell<PyFilterViewInner>>,
}

#[pymethods]
impl PyFilterView {
    #[new]
    fn new(table: PyTable, predicate: PyObject) -> PyResult<Self> {
        let generation = table.inner.borrow().changeset_generation();
        let change_count = table.inner.borrow().changeset().total_len();
        let inner = Rc::new(RefCell::new(PyFilterViewInner {
            table_inner: Rc::clone(&table.inner),
            predicate,
            indices: Vec::new(),
            last_synced_generation: generation,
            last_processed_change_count: change_count,
        }));

        // Initial refresh
        Python::with_gil(|py| -> PyResult<()> {
            inner.borrow_mut().refresh(py)?;
            Ok(())
        })?;

        Ok(PyFilterView { table, inner })
    }

    fn __len__(&self) -> usize {
        self.inner.borrow().indices.len()
    }

    fn __repr__(&self) -> String {
        format!("FilterView(rows={})", self.inner.borrow().indices.len())
    }

    fn refresh(&mut self) -> PyResult<()> {
        Python::with_gil(|py| self.inner.borrow_mut().refresh(py))
    }

    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let inner = self.inner.borrow();
        if index >= inner.indices.len() {
            return Err(PyIndexError::new_err("Index out of range"));
        }

        let actual_index = inner.indices[index];
        drop(inner); // Release borrow before calling table method
        self.table.get_row(py, actual_index)
    }

    /// Index access with negative indexing and slicing support
    fn __getitem__(&self, py: Python, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let view_len = self.inner.borrow().indices.len();

        // Try integer index (supports negative indexing)
        if let Ok(mut idx) = key.extract::<isize>() {
            if idx < 0 {
                idx += view_len as isize;
            }
            if idx < 0 || idx as usize >= view_len {
                return Err(PyIndexError::new_err(format!(
                    "index {} out of range for view with {} rows",
                    key, view_len
                )));
            }
            return self.get_row(py, idx as usize);
        }

        // Try slice (supports negative step for reverse slicing)
        if let Ok(slice) = key.downcast::<PySlice>() {
            let indices = slice.indices(view_len as isize)?;
            let start = indices.start;
            let stop = indices.stop;
            let step = indices.step;

            let list = PyList::empty_bound(py);
            let mut i = start;
            if step > 0 {
                while i < stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            } else {
                while i > stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            }
            return Ok(list.to_object(py));
        }

        Err(PyTypeError::new_err("indices must be integers or slices"))
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let inner = self.inner.borrow();
        if row >= inner.indices.len() {
            return Err(PyIndexError::new_err("Index out of range"));
        }

        let actual_index = inner.indices[row];
        drop(inner); // Release borrow before calling table method
        self.table.get_value(py, actual_index, column)
    }

    /// Incrementally sync with parent table changes
    /// Returns True if any changes were applied, False otherwise
    ///
    /// This is more efficient than refresh() when only a few changes have occurred
    fn sync(&mut self) -> PyResult<bool> {
        Python::with_gil(|py| self.inner.borrow_mut().sync(py))
    }

    /// Get the last synced generation number
    fn last_synced_generation(&self) -> u64 {
        self.inner.borrow().last_synced_generation
    }

    /// Return an iterator over the filtered rows.
    /// Enables: `for row in filter_view:`
    fn __iter__(slf: PyRef<'_, Self>, py: Python) -> PyFilterViewIterator {
        let length = slf.inner.borrow().indices.len();
        PyFilterViewIterator {
            view: slf.into_py(py).extract(py).unwrap(),
            index: 0,
            length,
        }
    }
}

// ============================================================================
// ProjectionView
// ============================================================================

#[pyclass(name = "ProjectionView", unsendable)]
pub struct PyProjectionView {
    table: PyTable,
    columns: Vec<String>,
}

#[pymethods]
impl PyProjectionView {
    #[new]
    fn new(table: PyTable, columns: Vec<String>) -> PyResult<Self> {
        // Validate columns exist
        {
            let table_ref = table.inner.borrow();
            let available_columns = table_ref.schema().get_column_names();
            for col in &columns {
                if !available_columns.contains(&col.as_str()) {
                    return Err(PyValueError::new_err(format!("Column '{}' not found", col)));
                }
            }
        }

        Ok(PyProjectionView { table, columns })
    }

    fn __len__(&self) -> usize {
        self.table.inner.borrow().len()
    }

    fn __repr__(&self) -> String {
        format!(
            "ProjectionView(columns={:?}, rows={})",
            self.columns,
            self.table.inner.borrow().len()
        )
    }

    fn column_names(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let full_row = self
            .table
            .inner
            .borrow()
            .get_row(index)
            .map_err(PyIndexError::new_err)?;

        let dict = PyDict::new_bound(py);
        for col in &self.columns {
            if let Some(value) = full_row.get(col) {
                dict.set_item(col, column_value_to_py(py, value)?)?;
            }
        }
        Ok(dict.to_object(py))
    }

    /// Index access with negative indexing and slicing support
    fn __getitem__(&self, py: Python, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let view_len = self.table.inner.borrow().len();

        if let Ok(mut idx) = key.extract::<isize>() {
            if idx < 0 {
                idx += view_len as isize;
            }
            if idx < 0 || idx as usize >= view_len {
                return Err(PyIndexError::new_err(format!(
                    "index {} out of range for view with {} rows",
                    key, view_len
                )));
            }
            return self.get_row(py, idx as usize);
        }

        if let Ok(slice) = key.downcast::<PySlice>() {
            let indices = slice.indices(view_len as isize)?;
            let start = indices.start;
            let stop = indices.stop;
            let step = indices.step;

            let list = PyList::empty_bound(py);
            let mut i = start;
            if step > 0 {
                while i < stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            } else {
                while i > stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            }
            return Ok(list.to_object(py));
        }

        Err(PyTypeError::new_err("indices must be integers or slices"))
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        if !self.columns.contains(&column.to_string()) {
            return Err(PyKeyError::new_err(format!(
                "Column '{}' not in projection",
                column
            )));
        }

        self.table.get_value(py, row, column)
    }

    /// Return an iterator over the projected rows.
    /// Enables: `for row in projection_view:`
    fn __iter__(slf: PyRef<'_, Self>, py: Python) -> PyProjectionViewIterator {
        let length = slf.table.inner.borrow().len();
        PyProjectionViewIterator {
            view: slf.into_py(py).extract(py).unwrap(),
            index: 0,
            length,
        }
    }
}

// ============================================================================
// ComputedView
// ============================================================================

#[pyclass(name = "ComputedView", unsendable)]
pub struct PyComputedView {
    table: PyTable,
    computed_column_name: String,
    compute_fn: PyObject,
}

#[pymethods]
impl PyComputedView {
    #[new]
    fn new(table: PyTable, computed_column_name: String, compute_fn: PyObject) -> PyResult<Self> {
        Ok(PyComputedView {
            table,
            computed_column_name,
            compute_fn,
        })
    }

    fn __len__(&self) -> usize {
        self.table.inner.borrow().len()
    }

    fn __repr__(&self) -> String {
        format!(
            "ComputedView(computed_column='{}', rows={})",
            self.computed_column_name,
            self.table.inner.borrow().len()
        )
    }

    fn column_names(&self) -> Vec<String> {
        let mut names = self.table.column_names();
        names.push(self.computed_column_name.clone());
        names
    }

    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let full_row = self
            .table
            .inner
            .borrow()
            .get_row(index)
            .map_err(PyIndexError::new_err)?;

        // Convert to dict
        let dict = PyDict::new_bound(py);
        for (key, value) in full_row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }

        // Compute the new column
        let computed_value = self.compute_fn.call1(py, (dict.clone(),))?;

        // Add to result dict
        let result_dict = PyDict::new_bound(py);
        for (key, value) in full_row.iter() {
            result_dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        result_dict.set_item(&self.computed_column_name, computed_value)?;

        Ok(result_dict.to_object(py))
    }

    /// Index access with negative indexing and slicing support
    fn __getitem__(&self, py: Python, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let view_len = self.table.inner.borrow().len();

        if let Ok(mut idx) = key.extract::<isize>() {
            if idx < 0 {
                idx += view_len as isize;
            }
            if idx < 0 || idx as usize >= view_len {
                return Err(PyIndexError::new_err(format!(
                    "index {} out of range for view with {} rows",
                    key, view_len
                )));
            }
            return self.get_row(py, idx as usize);
        }

        if let Ok(slice) = key.downcast::<PySlice>() {
            let indices = slice.indices(view_len as isize)?;
            let start = indices.start;
            let stop = indices.stop;
            let step = indices.step;

            let list = PyList::empty_bound(py);
            let mut i = start;
            if step > 0 {
                while i < stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            } else {
                while i > stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            }
            return Ok(list.to_object(py));
        }

        Err(PyTypeError::new_err("indices must be integers or slices"))
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        if column == self.computed_column_name {
            // Need to compute this value
            let full_row = self
                .table
                .inner
                .borrow()
                .get_row(row)
                .map_err(PyIndexError::new_err)?;

            let dict = PyDict::new_bound(py);
            for (key, value) in full_row.iter() {
                dict.set_item(key, column_value_to_py(py, value)?)?;
            }

            self.compute_fn.call1(py, (dict,))
        } else {
            self.table.get_value(py, row, column)
        }
    }

    /// Return an iterator over the rows with computed column.
    /// Enables: `for row in computed_view:`
    fn __iter__(slf: PyRef<'_, Self>, py: Python) -> PyComputedViewIterator {
        let length = slf.table.inner.borrow().len();
        PyComputedViewIterator {
            view: slf.into_py(py).extract(py).unwrap(),
            index: 0,
            length,
        }
    }
}

// ============================================================================
// JoinView
// ============================================================================

/// Python-exposed JoinType enum
#[pyclass(name = "JoinType")]
#[derive(Clone, Copy)]
pub struct PyJoinType {
    inner: RustJoinType,
}

#[pymethods]
impl PyJoinType {
    #[classattr]
    fn LEFT() -> Self {
        PyJoinType {
            inner: RustJoinType::Left,
        }
    }

    #[classattr]
    fn INNER() -> Self {
        PyJoinType {
            inner: RustJoinType::Inner,
        }
    }

    #[classattr]
    fn RIGHT() -> Self {
        PyJoinType {
            inner: RustJoinType::Right,
        }
    }

    #[classattr]
    fn FULL() -> Self {
        PyJoinType {
            inner: RustJoinType::Full,
        }
    }

    fn __repr__(&self) -> String {
        match self.inner {
            RustJoinType::Left => "JoinType.LEFT".to_string(),
            RustJoinType::Inner => "JoinType.INNER".to_string(),
            RustJoinType::Right => "JoinType.RIGHT".to_string(),
            RustJoinType::Full => "JoinType.FULL".to_string(),
        }
    }
}

#[pyclass(name = "JoinView", unsendable)]
pub struct PyJoinView {
    inner: Rc<RefCell<RustJoinView>>,
}

#[pymethods]
impl PyJoinView {
    /// Create a new JoinView.
    ///
    /// # Arguments
    /// * `name` - Name for this view
    /// * `left_table` - Left table
    /// * `right_table` - Right table
    /// * `left_keys` - Column name(s) in left table to join on (string or list of strings)
    /// * `right_keys` - Column name(s) in right table to join on (string or list of strings)
    /// * `join_type` - Type of join (JoinType.LEFT, JoinType.INNER, JoinType.RIGHT, or JoinType.FULL)
    #[new]
    fn new(
        name: String,
        left_table: PyTable,
        right_table: PyTable,
        left_keys: &Bound<'_, PyAny>,
        right_keys: &Bound<'_, PyAny>,
        join_type: PyJoinType,
    ) -> PyResult<Self> {
        // Convert left_keys to Vec<String>
        let left_keys_vec = extract_string_or_list(left_keys)?;
        let right_keys_vec = extract_string_or_list(right_keys)?;

        let join = RustJoinView::new_multi(
            name,
            left_table.inner.clone(),
            right_table.inner.clone(),
            left_keys_vec,
            right_keys_vec,
            join_type.inner,
        )
        .map_err(PyValueError::new_err)?;

        Ok(PyJoinView {
            inner: Rc::new(RefCell::new(join)),
        })
    }

    fn __len__(&self) -> usize {
        self.inner.borrow().len()
    }

    fn __repr__(&self) -> String {
        let view = self.inner.borrow();
        format!("JoinView(name='{}', rows={})", view.name(), view.len())
    }

    fn name(&self) -> String {
        self.inner.borrow().name().to_string()
    }

    fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let row = self
            .inner
            .borrow()
            .get_row(index)
            .map_err(PyIndexError::new_err)?;

        let dict = PyDict::new_bound(py);
        for (key, value) in row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        Ok(dict.to_object(py))
    }

    /// Index access with negative indexing and slicing support
    fn __getitem__(&self, py: Python, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let view_len = self.inner.borrow().len();

        if let Ok(mut idx) = key.extract::<isize>() {
            if idx < 0 {
                idx += view_len as isize;
            }
            if idx < 0 || idx as usize >= view_len {
                return Err(PyIndexError::new_err(format!(
                    "index {} out of range for view with {} rows",
                    key, view_len
                )));
            }
            return self.get_row(py, idx as usize);
        }

        if let Ok(slice) = key.downcast::<PySlice>() {
            let indices = slice.indices(view_len as isize)?;
            let start = indices.start;
            let stop = indices.stop;
            let step = indices.step;

            let list = PyList::empty_bound(py);
            let mut i = start;
            if step > 0 {
                while i < stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            } else {
                while i > stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            }
            return Ok(list.to_object(py));
        }

        Err(PyTypeError::new_err("indices must be integers or slices"))
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = self
            .inner
            .borrow()
            .get_value(row, column)
            .map_err(PyKeyError::new_err)?;

        column_value_to_py(py, &value)
    }

    fn refresh(&mut self) {
        self.inner.borrow_mut().refresh();
    }

    /// Incrementally sync the join view with parent table changes.
    /// Returns True if any changes were applied.
    fn sync(&mut self) -> bool {
        self.inner.borrow_mut().sync()
    }

    /// Return an iterator over the joined rows.
    /// Enables: `for row in join_view:`
    fn __iter__(slf: PyRef<'_, Self>, py: Python) -> PyJoinViewIterator {
        let length = slf.inner.borrow().len();
        PyJoinViewIterator {
            view: slf.into_py(py).extract(py).unwrap(),
            index: 0,
            length,
        }
    }
}

// ============================================================================
// SortedView
// ============================================================================

/// Python-exposed SortOrder enum
#[pyclass(name = "SortOrder")]
#[derive(Clone, Copy)]
pub struct PySortOrder {
    inner: RustSortOrder,
}

#[pymethods]
impl PySortOrder {
    #[classattr]
    fn ASCENDING() -> Self {
        PySortOrder {
            inner: RustSortOrder::Ascending,
        }
    }

    #[classattr]
    fn DESCENDING() -> Self {
        PySortOrder {
            inner: RustSortOrder::Descending,
        }
    }

    fn __repr__(&self) -> String {
        match self.inner {
            RustSortOrder::Ascending => "SortOrder.ASCENDING".to_string(),
            RustSortOrder::Descending => "SortOrder.DESCENDING".to_string(),
        }
    }
}

/// Python-exposed SortKey class
#[pyclass(name = "SortKey")]
#[derive(Clone)]
pub struct PySortKey {
    column: String,
    order: RustSortOrder,
    nulls_first: bool,
}

#[pymethods]
impl PySortKey {
    #[new]
    #[pyo3(signature = (column, order=None, nulls_first=false))]
    fn new(column: String, order: Option<PySortOrder>, nulls_first: bool) -> Self {
        PySortKey {
            column,
            order: order.map(|o| o.inner).unwrap_or(RustSortOrder::Ascending),
            nulls_first,
        }
    }

    /// Create an ascending sort key
    #[staticmethod]
    #[pyo3(signature = (column, nulls_first=false))]
    fn ascending(column: String, nulls_first: bool) -> Self {
        PySortKey {
            column,
            order: RustSortOrder::Ascending,
            nulls_first,
        }
    }

    /// Create a descending sort key
    #[staticmethod]
    #[pyo3(signature = (column, nulls_first=false))]
    fn descending(column: String, nulls_first: bool) -> Self {
        PySortKey {
            column,
            order: RustSortOrder::Descending,
            nulls_first,
        }
    }

    fn __repr__(&self) -> String {
        let order_str = match self.order {
            RustSortOrder::Ascending => "ASCENDING",
            RustSortOrder::Descending => "DESCENDING",
        };
        format!(
            "SortKey(column='{}', order={}, nulls_first={})",
            self.column, order_str, self.nulls_first
        )
    }

    #[getter]
    fn column(&self) -> String {
        self.column.clone()
    }

    #[getter]
    fn order(&self) -> PySortOrder {
        PySortOrder { inner: self.order }
    }

    #[getter]
    fn nulls_first(&self) -> bool {
        self.nulls_first
    }
}

impl PySortKey {
    fn to_rust(&self) -> RustSortKey {
        RustSortKey::new(self.column.clone(), self.order, self.nulls_first)
    }
}

/// Python-exposed SortedView class
#[pyclass(name = "SortedView", unsendable)]
pub struct PySortedView {
    inner: Rc<RefCell<RustSortedView>>,
}

#[pymethods]
impl PySortedView {
    #[new]
    fn new(name: String, table: PyTable, sort_keys: Vec<PySortKey>) -> PyResult<Self> {
        let rust_keys: Vec<RustSortKey> = sort_keys.iter().map(|k| k.to_rust()).collect();

        let view = RustSortedView::new(name, table.inner.clone(), rust_keys)
            .map_err(PyValueError::new_err)?;

        Ok(PySortedView {
            inner: Rc::new(RefCell::new(view)),
        })
    }

    fn __len__(&self) -> usize {
        self.inner.borrow().len()
    }

    fn __repr__(&self) -> String {
        let view = self.inner.borrow();
        format!("SortedView(name='{}', rows={})", view.name(), view.len())
    }

    fn name(&self) -> String {
        self.inner.borrow().name().to_string()
    }

    fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let row = self
            .inner
            .borrow()
            .get_row(index)
            .map_err(PyIndexError::new_err)?;

        let dict = PyDict::new_bound(py);
        for (key, value) in row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        Ok(dict.to_object(py))
    }

    /// Index access with negative indexing and slicing support
    fn __getitem__(&self, py: Python, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let view_len = self.inner.borrow().len();

        if let Ok(mut idx) = key.extract::<isize>() {
            if idx < 0 {
                idx += view_len as isize;
            }
            if idx < 0 || idx as usize >= view_len {
                return Err(PyIndexError::new_err(format!(
                    "index {} out of range for view with {} rows",
                    key, view_len
                )));
            }
            return self.get_row(py, idx as usize);
        }

        if let Ok(slice) = key.downcast::<PySlice>() {
            let indices = slice.indices(view_len as isize)?;
            let start = indices.start;
            let stop = indices.stop;
            let step = indices.step;

            let list = PyList::empty_bound(py);
            let mut i = start;
            if step > 0 {
                while i < stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            } else {
                while i > stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            }
            return Ok(list.to_object(py));
        }

        Err(PyTypeError::new_err("indices must be integers or slices"))
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = self
            .inner
            .borrow()
            .get_value(row, column)
            .map_err(PyKeyError::new_err)?;

        column_value_to_py(py, &value)
    }

    /// Get the parent table row index for a given view position
    fn get_parent_index(&self, view_index: usize) -> Option<usize> {
        self.inner.borrow().get_parent_index(view_index)
    }

    /// Force a full refresh of the sorted index
    fn refresh(&mut self) {
        self.inner.borrow_mut().refresh();
    }

    /// Incrementally sync with parent table changes
    /// Returns True if any changes were applied
    fn sync(&mut self) -> bool {
        self.inner.borrow_mut().sync()
    }

    /// Return an iterator over the sorted rows.
    /// Enables: `for row in sorted_view:`
    fn __iter__(slf: PyRef<'_, Self>, py: Python) -> PySortedViewIterator {
        let length = slf.inner.borrow().len();
        PySortedViewIterator {
            view: slf.into_py(py).extract(py).unwrap(),
            index: 0,
            length,
        }
    }
}

// ============================================================================
// AggregateFunction
// ============================================================================

/// Supported aggregation functions
#[pyclass(name = "AggregateFunction")]
#[derive(Clone, Copy)]
pub struct PyAggregateFunction {
    inner: RustAggregateFunction,
}

#[pymethods]
impl PyAggregateFunction {
    /// Sum of values
    #[classattr]
    fn SUM() -> Self {
        PyAggregateFunction {
            inner: RustAggregateFunction::Sum,
        }
    }

    /// Count of non-null values
    #[classattr]
    fn COUNT() -> Self {
        PyAggregateFunction {
            inner: RustAggregateFunction::Count,
        }
    }

    /// Average of values
    #[classattr]
    fn AVG() -> Self {
        PyAggregateFunction {
            inner: RustAggregateFunction::Avg,
        }
    }

    /// Minimum value
    #[classattr]
    fn MIN() -> Self {
        PyAggregateFunction {
            inner: RustAggregateFunction::Min,
        }
    }

    /// Maximum value
    #[classattr]
    fn MAX() -> Self {
        PyAggregateFunction {
            inner: RustAggregateFunction::Max,
        }
    }

    /// Median value (equivalent to PERCENTILE(0.5))
    #[classattr]
    fn MEDIAN() -> Self {
        PyAggregateFunction {
            inner: RustAggregateFunction::Median,
        }
    }

    /// Percentile value. p must be between 0.0 and 1.0 inclusive.
    #[staticmethod]
    fn PERCENTILE(p: f64) -> PyResult<Self> {
        if !(0.0..=1.0).contains(&p) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Percentile value must be between 0.0 and 1.0, got {}",
                p
            )));
        }
        Ok(PyAggregateFunction {
            inner: RustAggregateFunction::Percentile(p),
        })
    }

    fn __repr__(&self) -> String {
        match self.inner {
            RustAggregateFunction::Sum => "AggregateFunction.SUM".to_string(),
            RustAggregateFunction::Count => "AggregateFunction.COUNT".to_string(),
            RustAggregateFunction::Avg => "AggregateFunction.AVG".to_string(),
            RustAggregateFunction::Min => "AggregateFunction.MIN".to_string(),
            RustAggregateFunction::Max => "AggregateFunction.MAX".to_string(),
            RustAggregateFunction::Median => "AggregateFunction.MEDIAN".to_string(),
            RustAggregateFunction::Percentile(p) => format!("AggregateFunction.PERCENTILE({})", p),
        }
    }
}

impl PyAggregateFunction {
    fn to_rust(self) -> RustAggregateFunction {
        self.inner
    }
}

// ============================================================================
// AggregateView
// ============================================================================

/// A view that groups rows and computes aggregate functions per group.
/// Supports incremental updates when the parent table changes.
#[pyclass(name = "AggregateView", unsendable)]
pub struct PyAggregateView {
    inner: Rc<RefCell<RustAggregateView>>,
}

#[pymethods]
impl PyAggregateView {
    /// Create a new AggregateView
    ///
    /// Args:
    ///     name: Name of the view
    ///     table: Parent table to aggregate
    ///     group_by: List of column names to group by
    ///     aggregations: List of (result_name, source_column, function) tuples
    #[new]
    fn new(
        name: String,
        table: PyTable,
        group_by: Vec<String>,
        aggregations: Vec<(String, String, PyAggregateFunction)>,
    ) -> PyResult<Self> {
        let rust_aggregations: Vec<(String, String, RustAggregateFunction)> = aggregations
            .into_iter()
            .map(|(result, source, func)| (result, source, func.to_rust()))
            .collect();

        let view = RustAggregateView::new(name, table.inner.clone(), group_by, rust_aggregations)
            .map_err(PyValueError::new_err)?;

        Ok(PyAggregateView {
            inner: Rc::new(RefCell::new(view)),
        })
    }

    fn __len__(&self) -> usize {
        self.inner.borrow().len()
    }

    fn __repr__(&self) -> String {
        let view = self.inner.borrow();
        format!(
            "AggregateView(name='{}', groups={})",
            view.name(),
            view.len()
        )
    }

    /// Get view name
    fn name(&self) -> String {
        self.inner.borrow().name().to_string()
    }

    /// Check if view is empty
    fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    /// Get column names (group-by columns + result columns)
    fn column_names(&self) -> Vec<String> {
        self.inner.borrow().column_names()
    }

    /// Get a row by index as a dictionary
    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let row = self
            .inner
            .borrow()
            .get_row(index)
            .map_err(PyIndexError::new_err)?;

        let dict = PyDict::new_bound(py);
        for (key, value) in row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        Ok(dict.to_object(py))
    }

    /// Index access: view[idx] returns the row at idx
    /// Supports negative indexing (view[-1] for last row)
    /// Supports slicing (view[1:5] for rows 1-4)
    fn __getitem__(&self, py: Python, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let view_len = self.inner.borrow().len();

        // Try integer index first (supports negative indexing)
        if let Ok(mut idx) = key.extract::<isize>() {
            if idx < 0 {
                idx += view_len as isize;
            }
            if idx < 0 || idx as usize >= view_len {
                return Err(PyIndexError::new_err(format!(
                    "index {} out of range for view with {} rows",
                    key, view_len
                )));
            }
            return self.get_row(py, idx as usize);
        }

        // Try slice (supports negative step for reverse slicing)
        if let Ok(slice) = key.downcast::<PySlice>() {
            let indices = slice.indices(view_len as isize)?;
            let start = indices.start;
            let stop = indices.stop;
            let step = indices.step;

            let list = PyList::empty_bound(py);
            let mut i = start;
            if step > 0 {
                while i < stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            } else {
                while i > stop {
                    list.append(self.get_row(py, i as usize)?)?;
                    i += step;
                }
            }
            return Ok(list.to_object(py));
        }

        Err(PyTypeError::new_err("indices must be integers or slices"))
    }

    /// Get a value at (row, column)
    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = self
            .inner
            .borrow()
            .get_value(row, column)
            .map_err(PyKeyError::new_err)?;

        column_value_to_py(py, &value)
    }

    /// Force a full refresh of the view
    fn refresh(&mut self) {
        self.inner.borrow_mut().refresh();
    }

    /// Incrementally sync with parent table changes
    /// Returns True if any changes were applied
    fn sync(&mut self) -> bool {
        self.inner.borrow_mut().sync()
    }

    /// Return an iterator over the aggregated groups.
    /// Enables: `for group in aggregate_view:`
    fn __iter__(slf: PyRef<'_, Self>, py: Python) -> PyAggregateViewIterator {
        let length = slf.inner.borrow().len();
        PyAggregateViewIterator {
            view: slf.into_py(py).extract(py).unwrap(),
            index: 0,
            length,
        }
    }
}

include!("python_bindings/iterators.rs");

// ============================================================================
// Module Definition
// ============================================================================

/// Python module for LiveTable
#[pymodule]
fn livetable(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Core types
    m.add_class::<PyColumnType>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyTable>()?;

    // View types
    m.add_class::<PyFilterView>()?;
    m.add_class::<PyProjectionView>()?;
    m.add_class::<PyComputedView>()?;
    m.add_class::<PyJoinType>()?;
    m.add_class::<PyJoinView>()?;
    m.add_class::<PySortOrder>()?;
    m.add_class::<PySortKey>()?;
    m.add_class::<PySortedView>()?;
    m.add_class::<PyAggregateFunction>()?;
    m.add_class::<PyAggregateView>()?;

    // Iterator types (for `for row in table:` syntax)
    m.add_class::<PyTableIterator>()?;
    m.add_class::<PyFilterViewIterator>()?;
    m.add_class::<PyProjectionViewIterator>()?;
    m.add_class::<PyComputedViewIterator>()?;
    m.add_class::<PyJoinViewIterator>()?;
    m.add_class::<PySortedViewIterator>()?;
    m.add_class::<PyAggregateViewIterator>()?;

    Ok(())
}
