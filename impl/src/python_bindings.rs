/// Python bindings for LiveTable using PyO3
///
/// This module provides Python-friendly APIs for the Rust implementation,
/// allowing Python code to use the high-performance Rust table system.

use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyIndexError, PyKeyError};
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

use crate::column::{ColumnType as RustColumnType, ColumnValue as RustColumnValue};
use crate::table::{Schema as RustSchema, Table as RustTable};
use crate::view::{JoinView as RustJoinView, JoinType as RustJoinType, SortedView as RustSortedView, SortKey as RustSortKey, SortOrder as RustSortOrder, AggregateFunction as RustAggregateFunction, AggregateView as RustAggregateView};

// ============================================================================
// Core Type Conversions
// ============================================================================

/// Python-exposed ColumnType enum
#[pyclass(name = "ColumnType")]
#[derive(Clone, Copy)]
pub struct PyColumnType {
    inner: RustColumnType,
}

#[pymethods]
impl PyColumnType {
    #[classattr]
    const INT32: PyColumnType = PyColumnType { inner: RustColumnType::Int32 };

    #[classattr]
    const INT64: PyColumnType = PyColumnType { inner: RustColumnType::Int64 };

    #[classattr]
    const FLOAT32: PyColumnType = PyColumnType { inner: RustColumnType::Float32 };

    #[classattr]
    const FLOAT64: PyColumnType = PyColumnType { inner: RustColumnType::Float64 };

    #[classattr]
    const STRING: PyColumnType = PyColumnType { inner: RustColumnType::String };

    #[classattr]
    const BOOL: PyColumnType = PyColumnType { inner: RustColumnType::Bool };

    fn __repr__(&self) -> String {
        match self.inner {
            RustColumnType::Int32 => "ColumnType.INT32".to_string(),
            RustColumnType::Int64 => "ColumnType.INT64".to_string(),
            RustColumnType::Float32 => "ColumnType.FLOAT32".to_string(),
            RustColumnType::Float64 => "ColumnType.FLOAT64".to_string(),
            RustColumnType::String => "ColumnType.STRING".to_string(),
            RustColumnType::Bool => "ColumnType.BOOL".to_string(),
        }
    }
}

impl PyColumnType {
    fn to_rust(&self) -> RustColumnType {
        self.inner
    }

    fn from_rust(col_type: RustColumnType) -> Self {
        PyColumnType { inner: col_type }
    }
}

/// Convert Python value to ColumnValue
fn py_to_column_value(_py: Python, value: &Bound<'_, PyAny>) -> PyResult<RustColumnValue> {
    if value.is_none() {
        return Ok(RustColumnValue::Null);
    }

    // Check type in order: bool, int, float, string
    // This matches Python's type hierarchy

    // Bool must be checked before int (bool is subclass of int in Python)
    if let Ok(v) = value.extract::<bool>() {
        // Double-check it's actually a bool, not just truthy
        if value.is_instance_of::<pyo3::types::PyBool>() {
            return Ok(RustColumnValue::Bool(v));
        }
    }

    // Try integer types first
    if let Ok(v) = value.extract::<i32>() {
        return Ok(RustColumnValue::Int32(v));
    }
    if let Ok(v) = value.extract::<i64>() {
        return Ok(RustColumnValue::Int64(v));
    }

    // Then floating point types
    if let Ok(v) = value.extract::<f64>() {
        return Ok(RustColumnValue::Float64(v));
    }
    if let Ok(v) = value.extract::<f32>() {
        return Ok(RustColumnValue::Float32(v));
    }

    // Finally strings
    if let Ok(v) = value.extract::<String>() {
        return Ok(RustColumnValue::String(v));
    }

    Err(PyValueError::new_err("Cannot convert value to ColumnValue"))
}

/// Convert ColumnValue to Python object
fn column_value_to_py(py: Python, value: &RustColumnValue) -> PyResult<PyObject> {
    match value {
        RustColumnValue::Int32(v) => Ok(v.to_object(py)),
        RustColumnValue::Int64(v) => Ok(v.to_object(py)),
        RustColumnValue::Float32(v) => Ok(v.to_object(py)),
        RustColumnValue::Float64(v) => Ok(v.to_object(py)),
        RustColumnValue::String(v) => Ok(v.to_object(py)),
        RustColumnValue::Bool(v) => Ok(v.to_object(py)),
        RustColumnValue::Null => Ok(py.None()),
    }
}

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
        self.inner.get_column_names().iter().map(|s| s.to_string()).collect()
    }

    /// Get column index by name
    fn get_column_index(&self, name: &str) -> Option<usize> {
        self.inner.get_column_index(name)
    }

    /// Get column info (name, type, nullable) by index
    fn get_column_info(&self, index: usize) -> Option<(String, PyColumnType, bool)> {
        self.inner.get_column_info(index)
            .map(|(name, col_type, nullable)| (name.to_string(), PyColumnType::from_rust(col_type), nullable))
    }
}

// ============================================================================
// Table
// ============================================================================

/// Python-exposed Table class
#[pyclass(name = "Table", unsendable)]
pub struct PyTable {
    inner: Rc<RefCell<RustTable>>,
}

#[pymethods]
impl PyTable {
    #[new]
    #[pyo3(signature = (name, schema, use_tiered_vector=false, use_string_interning=false))]
    fn new(name: String, schema: PySchema, use_tiered_vector: bool, use_string_interning: bool) -> Self {
        PyTable {
            inner: Rc::new(RefCell::new(RustTable::new_with_interning(
                name,
                schema.inner,
                use_tiered_vector,
                use_string_interning,
            ))),
        }
    }

    fn __len__(&self) -> usize {
        self.inner.borrow().len()
    }

    fn __repr__(&self) -> String {
        let table = self.inner.borrow();
        format!("Table(name='{}', rows={}, columns={})",
                table.name(), table.len(), table.schema().get_column_names().len())
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
        self.inner.borrow()
            .schema()
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Append a row to the table
    fn append_row(&mut self, py: Python, row: &Bound<'_, PyDict>) -> PyResult<()> {
        let mut rust_row = HashMap::new();

        for (key, value) in row.iter() {
            let key_str: String = key.extract()?;
            let col_value = py_to_column_value(py, &value)?;
            rust_row.insert(key_str, col_value);
        }

        self.inner.borrow_mut()
            .append_row(rust_row)
            .map_err(|e| PyValueError::new_err(e))
    }

    /// Insert a row at a specific index
    fn insert_row(&mut self, py: Python, index: usize, row: &Bound<'_, PyDict>) -> PyResult<()> {
        let mut rust_row = HashMap::new();

        for (key, value) in row.iter() {
            let key_str: String = key.extract()?;
            let col_value = py_to_column_value(py, &value)?;
            rust_row.insert(key_str, col_value);
        }

        self.inner.borrow_mut()
            .insert_row(index, rust_row)
            .map_err(|e| PyValueError::new_err(e))
    }

    /// Delete a row at a specific index
    fn delete_row(&mut self, index: usize) -> PyResult<()> {
        self.inner.borrow_mut()
            .delete_row(index)
            .map(|_| ())  // Discard the returned row
            .map_err(|e| PyIndexError::new_err(e))
    }

    /// Get a value at (row, column)
    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = self.inner.borrow()
            .get_value(row, column)
            .map_err(|e| PyKeyError::new_err(e))?;

        column_value_to_py(py, &value)
    }

    /// Set a value at (row, column)
    fn set_value(&mut self, py: Python, row: usize, column: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let col_value = py_to_column_value(py, value)?;

        self.inner.borrow_mut()
            .set_value(row, column, col_value)
            .map_err(|e| PyValueError::new_err(e))
    }

    /// Get a full row as a dictionary
    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let row = self.inner.borrow()
            .get_row(index)
            .map_err(|e| PyIndexError::new_err(e))?;

        let dict = PyDict::new_bound(py);
        for (key, value) in row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        Ok(dict.to_object(py))
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

    /// Create a filter view
    fn filter(&self, predicate: PyObject) -> PyResult<PyFilterView> {
        PyFilterView::new(self.clone(), predicate)
    }

    /// Create a projection view (select specific columns)
    fn select(&self, columns: Vec<String>) -> PyResult<PyProjectionView> {
        PyProjectionView::new(self.clone(), columns)
    }

    /// Create a computed view with an additional computed column
    fn add_computed_column(&self, name: String, compute_fn: PyObject) -> PyResult<PyComputedView> {
        PyComputedView::new(self.clone(), name, compute_fn)
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
        self.inner.borrow()
            .sum(column)
            .map_err(|e| PyValueError::new_err(e))
    }

    /// Count the number of non-NULL values in a column.
    fn count_non_null(&self, column: &str) -> PyResult<usize> {
        self.inner.borrow()
            .count_non_null(column)
            .map_err(|e| PyValueError::new_err(e))
    }

    /// Calculate the average of all numeric values in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    fn avg(&self, column: &str) -> PyResult<Option<f64>> {
        self.inner.borrow()
            .avg(column)
            .map_err(|e| PyValueError::new_err(e))
    }

    /// Find the minimum numeric value in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    fn min(&self, column: &str) -> PyResult<Option<f64>> {
        self.inner.borrow()
            .min(column)
            .map_err(|e| PyValueError::new_err(e))
    }

    /// Find the maximum numeric value in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    fn max(&self, column: &str) -> PyResult<Option<f64>> {
        self.inner.borrow()
            .max(column)
            .map_err(|e| PyValueError::new_err(e))
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
        self.inner.borrow()
            .to_json()
            .map_err(|e| PyValueError::new_err(e))
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
        let table = RustTable::from_csv(name, csv)
            .map_err(|e| PyValueError::new_err(e))?;
        Ok(PyTable {
            inner: Rc::new(RefCell::new(table)),
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
        let table = RustTable::from_json(name, json)
            .map_err(|e| PyValueError::new_err(e))?;
        Ok(PyTable {
            inner: Rc::new(RefCell::new(table)),
        })
    }
}

impl Clone for PyTable {
    fn clone(&self) -> Self {
        PyTable {
            inner: Rc::clone(&self.inner),
        }
    }
}

// ============================================================================
// FilterView
// ============================================================================

#[pyclass(name = "FilterView", unsendable)]
pub struct PyFilterView {
    table: PyTable,
    predicate: PyObject,
    indices: Vec<usize>,
    /// Last synced changeset generation
    last_synced_generation: u64,
}

#[pymethods]
impl PyFilterView {
    #[new]
    fn new(table: PyTable, predicate: PyObject) -> PyResult<Self> {
        let generation = table.inner.borrow().changeset_generation();
        let mut view = PyFilterView {
            table,
            predicate,
            indices: Vec::new(),
            last_synced_generation: generation,
        };
        view.refresh()?;
        Ok(view)
    }

    fn __len__(&self) -> usize {
        self.indices.len()
    }

    fn __repr__(&self) -> String {
        format!("FilterView(rows={})", self.indices.len())
    }

    fn refresh(&mut self) -> PyResult<()> {
        Python::with_gil(|py| {
            self.indices.clear();
            let table_ref = self.table.inner.borrow();

            for i in 0..table_ref.len() {
                let row = table_ref.get_row(i).map_err(|e| PyValueError::new_err(e))?;

                // Convert row to Python dict
                let dict = PyDict::new_bound(py);
                for (key, value) in row.iter() {
                    dict.set_item(key, column_value_to_py(py, value)?)?;
                }

                // Call predicate
                let result: bool = self.predicate.call1(py, (dict,))?.extract(py)?;
                if result {
                    self.indices.push(i);
                }
            }

            Ok(())
        })
    }

    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        if index >= self.indices.len() {
            return Err(PyIndexError::new_err("Index out of range"));
        }

        let actual_index = self.indices[index];
        self.table.get_row(py, actual_index)
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        if row >= self.indices.len() {
            return Err(PyIndexError::new_err("Index out of range"));
        }

        let actual_index = self.indices[row];
        self.table.get_value(py, actual_index, column)
    }

    /// Incrementally sync with parent table changes
    /// Returns True if any changes were applied, False otherwise
    ///
    /// This is more efficient than refresh() when only a few changes have occurred
    fn sync(&mut self) -> PyResult<bool> {
        use crate::changeset::TableChange;

        let changes: Vec<TableChange> = self.table.inner.borrow().changeset().changes().to_vec();

        if changes.is_empty() {
            return Ok(false);
        }

        Python::with_gil(|py| {
            let mut modified = false;

            for change in changes {
                match change {
                    TableChange::RowInserted { index, data } => {
                        // Adjust existing indices
                        for idx in self.indices.iter_mut() {
                            if *idx >= index {
                                *idx += 1;
                            }
                        }

                        // Check if new row matches predicate
                        let dict = PyDict::new_bound(py);
                        for (key, value) in data.iter() {
                            dict.set_item(key, column_value_to_py(py, value)?)?;
                        }

                        let result: bool = self.predicate.call1(py, (dict,))?.extract(py)?;
                        if result {
                            // Insert in sorted order
                            let insert_pos = self.indices.iter()
                                .position(|&i| i > index)
                                .unwrap_or(self.indices.len());
                            self.indices.insert(insert_pos, index);
                            modified = true;
                        }
                    }

                    TableChange::RowDeleted { index, .. } => {
                        // Find and adjust indices
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
                        // Re-evaluate the predicate for this row
                        let currently_in_view = self.indices.contains(&row);

                        let table_ref = self.table.inner.borrow();
                        let now_matches = if let Ok(row_data) = table_ref.get_row(row) {
                            let dict = PyDict::new_bound(py);
                            for (key, value) in row_data.iter() {
                                dict.set_item(key, column_value_to_py(py, value)?)?;
                            }
                            drop(table_ref); // Release borrow before Python call
                            self.predicate.call1(py, (dict,))?.extract::<bool>(py)?
                        } else {
                            drop(table_ref);
                            false
                        };

                        match (currently_in_view, now_matches) {
                            (false, true) => {
                                // Add to view
                                let insert_pos = self.indices.iter()
                                    .position(|&i| i > row)
                                    .unwrap_or(self.indices.len());
                                self.indices.insert(insert_pos, row);
                                modified = true;
                            }
                            (true, false) => {
                                // Remove from view
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

            self.last_synced_generation = self.table.inner.borrow().changeset_generation();
            Ok(modified)
        })
    }

    /// Get the last synced generation number
    fn last_synced_generation(&self) -> u64 {
        self.last_synced_generation
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
        format!("ProjectionView(columns={:?}, rows={})",
                self.columns, self.table.inner.borrow().len())
    }

    fn column_names(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let full_row = self.table.inner.borrow()
            .get_row(index)
            .map_err(|e| PyIndexError::new_err(e))?;

        let dict = PyDict::new_bound(py);
        for col in &self.columns {
            if let Some(value) = full_row.get(col) {
                dict.set_item(col, column_value_to_py(py, value)?)?;
            }
        }
        Ok(dict.to_object(py))
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        if !self.columns.contains(&column.to_string()) {
            return Err(PyKeyError::new_err(format!("Column '{}' not in projection", column)));
        }

        self.table.get_value(py, row, column)
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
        format!("ComputedView(computed_column='{}', rows={})",
                self.computed_column_name, self.table.inner.borrow().len())
    }

    fn column_names(&self) -> Vec<String> {
        let mut names = self.table.column_names();
        names.push(self.computed_column_name.clone());
        names
    }

    fn get_row(&self, py: Python, index: usize) -> PyResult<PyObject> {
        let full_row = self.table.inner.borrow()
            .get_row(index)
            .map_err(|e| PyIndexError::new_err(e))?;

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

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        if column == self.computed_column_name {
            // Need to compute this value
            let full_row = self.table.inner.borrow()
                .get_row(row)
                .map_err(|e| PyIndexError::new_err(e))?;

            let dict = PyDict::new_bound(py);
            for (key, value) in full_row.iter() {
                dict.set_item(key, column_value_to_py(py, value)?)?;
            }

            self.compute_fn.call1(py, (dict,))
        } else {
            self.table.get_value(py, row, column)
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
        PyJoinType { inner: RustJoinType::Left }
    }

    #[classattr]
    fn INNER() -> Self {
        PyJoinType { inner: RustJoinType::Inner }
    }

    fn __repr__(&self) -> String {
        match self.inner {
            RustJoinType::Left => "JoinType.LEFT".to_string(),
            RustJoinType::Inner => "JoinType.INNER".to_string(),
        }
    }
}

#[pyclass(name = "JoinView", unsendable)]
pub struct PyJoinView {
    inner: Rc<RefCell<RustJoinView>>,
}

#[pymethods]
impl PyJoinView {
    #[new]
    fn new(
        name: String,
        left_table: PyTable,
        right_table: PyTable,
        left_key: String,
        right_key: String,
        join_type: PyJoinType,
    ) -> PyResult<Self> {
        let join = RustJoinView::new(
            name,
            left_table.inner.clone(),
            right_table.inner.clone(),
            left_key,
            right_key,
            join_type.inner,
        ).map_err(|e| PyValueError::new_err(e))?;

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
        let row = self.inner.borrow()
            .get_row(index)
            .map_err(|e| PyIndexError::new_err(e))?;

        let dict = PyDict::new_bound(py);
        for (key, value) in row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        Ok(dict.to_object(py))
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = self.inner.borrow()
            .get_value(row, column)
            .map_err(|e| PyKeyError::new_err(e))?;

        column_value_to_py(py, &value)
    }

    fn refresh(&mut self) {
        self.inner.borrow_mut().refresh();
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
        PySortOrder { inner: RustSortOrder::Ascending }
    }

    #[classattr]
    fn DESCENDING() -> Self {
        PySortOrder { inner: RustSortOrder::Descending }
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
        format!("SortKey(column='{}', order={}, nulls_first={})",
                self.column, order_str, self.nulls_first)
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

        let view = RustSortedView::new(
            name,
            table.inner.clone(),
            rust_keys,
        ).map_err(|e| PyValueError::new_err(e))?;

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
        let row = self.inner.borrow()
            .get_row(index)
            .map_err(|e| PyIndexError::new_err(e))?;

        let dict = PyDict::new_bound(py);
        for (key, value) in row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        Ok(dict.to_object(py))
    }

    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = self.inner.borrow()
            .get_value(row, column)
            .map_err(|e| PyKeyError::new_err(e))?;

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
        PyAggregateFunction { inner: RustAggregateFunction::Sum }
    }

    /// Count of non-null values
    #[classattr]
    fn COUNT() -> Self {
        PyAggregateFunction { inner: RustAggregateFunction::Count }
    }

    /// Average of values
    #[classattr]
    fn AVG() -> Self {
        PyAggregateFunction { inner: RustAggregateFunction::Avg }
    }

    /// Minimum value
    #[classattr]
    fn MIN() -> Self {
        PyAggregateFunction { inner: RustAggregateFunction::Min }
    }

    /// Maximum value
    #[classattr]
    fn MAX() -> Self {
        PyAggregateFunction { inner: RustAggregateFunction::Max }
    }

    fn __repr__(&self) -> String {
        match self.inner {
            RustAggregateFunction::Sum => "AggregateFunction.SUM".to_string(),
            RustAggregateFunction::Count => "AggregateFunction.COUNT".to_string(),
            RustAggregateFunction::Avg => "AggregateFunction.AVG".to_string(),
            RustAggregateFunction::Min => "AggregateFunction.MIN".to_string(),
            RustAggregateFunction::Max => "AggregateFunction.MAX".to_string(),
        }
    }
}

impl PyAggregateFunction {
    fn to_rust(&self) -> RustAggregateFunction {
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

        let view = RustAggregateView::new(
            name,
            table.inner.clone(),
            group_by,
            rust_aggregations,
        ).map_err(|e| PyValueError::new_err(e))?;

        Ok(PyAggregateView {
            inner: Rc::new(RefCell::new(view)),
        })
    }

    fn __len__(&self) -> usize {
        self.inner.borrow().len()
    }

    fn __repr__(&self) -> String {
        let view = self.inner.borrow();
        format!("AggregateView(name='{}', groups={})", view.name(), view.len())
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
        let row = self.inner.borrow()
            .get_row(index)
            .map_err(|e| PyIndexError::new_err(e))?;

        let dict = PyDict::new_bound(py);
        for (key, value) in row.iter() {
            dict.set_item(key, column_value_to_py(py, value)?)?;
        }
        Ok(dict.to_object(py))
    }

    /// Get a value at (row, column)
    fn get_value(&self, py: Python, row: usize, column: &str) -> PyResult<PyObject> {
        let value = self.inner.borrow()
            .get_value(row, column)
            .map_err(|e| PyKeyError::new_err(e))?;

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
}

// ============================================================================
// Module Definition
// ============================================================================

/// Python module for LiveTable
#[pymodule]
fn livetable(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyColumnType>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyTable>()?;
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

    Ok(())
}
