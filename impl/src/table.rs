/// LiveTable Table Implementation in Rust
///
/// A Table is a collection of columns with a schema.
/// Provides row-level operations (insert, delete, update, query).
///
/// # Examples
///
/// ```
/// use livetable::{Table, Schema, ColumnType, ColumnValue};
/// use std::collections::HashMap;
///
/// // Create a schema
/// let schema = Schema::new(vec![
///     ("id".to_string(), ColumnType::Int32, false),
///     ("name".to_string(), ColumnType::String, false),
///     ("age".to_string(), ColumnType::Int32, true),
/// ]);
///
/// // Create a table
/// let mut table = Table::new("users".to_string(), schema);
///
/// // Add a row
/// let mut row = HashMap::new();
/// row.insert("id".to_string(), ColumnValue::Int32(1));
/// row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
/// row.insert("age".to_string(), ColumnValue::Int32(30));
/// table.append_row(row).unwrap();
///
/// // Query data
/// assert_eq!(table.len(), 1);
/// assert_eq!(table.get_value(0, "name").unwrap().as_string(), Some("Alice"));
/// ```

use crate::column::{Column, ColumnType, ColumnValue};
use crate::changeset::{Changeset, TableChange};
use crate::interner::{StringInterner, InternerStats};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Hint for selecting the underlying storage strategy.
///
/// This allows users to optimize for their workload without needing to
/// understand the implementation details of the underlying data structures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageHint {
    /// Optimized for append and read-heavy workloads (default).
    ///
    /// Best for: logs, time-series, analytics, streaming data.
    /// - O(1) random access
    /// - O(1) amortized append
    /// - O(N) insert/delete in middle
    #[default]
    FastReads,

    /// Optimized for frequent inserts and deletes anywhere in the table.
    ///
    /// Best for: order books, priority queues, ranked lists, live updates.
    /// - O(1) random access
    /// - O(√N) insert/delete anywhere
    /// - O(√N) append (slightly slower than FastReads)
    FastUpdates,
}

impl StorageHint {
    /// Parse a storage hint from a string (for Python API).
    ///
    /// Accepts: "fast_reads", "fast_updates"
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "fast_reads" | "fastreads" => Ok(StorageHint::FastReads),
            "fast_updates" | "fastupdates" => Ok(StorageHint::FastUpdates),
            _ => Err(format!(
                "Unknown storage hint: '{}'. Use 'fast_reads' or 'fast_updates'",
                s
            )),
        }
    }

    /// Returns true if this hint uses tiered vector storage.
    pub(crate) fn use_tiered_vector(&self) -> bool {
        matches!(self, StorageHint::FastUpdates)
    }
}

/// Schema definition with column names and types.
///
/// A schema defines the structure of a table, specifying the name, type,
/// and nullability of each column.
///
/// # Examples
///
/// ```
/// use livetable::{Schema, ColumnType};
///
/// let schema = Schema::new(vec![
///     ("id".to_string(), ColumnType::Int32, false),      // Required
///     ("email".to_string(), ColumnType::String, false),  // Required
///     ("age".to_string(), ColumnType::Int32, true),      // Nullable
/// ]);
///
/// assert_eq!(schema.len(), 3);
/// assert_eq!(schema.get_column_index("email"), Some(1));
/// ```
#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<(String, ColumnType, bool)>, // (name, type, nullable)
}

impl Schema {
    /// Creates a new schema with the specified columns.
    ///
    /// # Arguments
    ///
    /// * `columns` - Vector of tuples: (column_name, column_type, is_nullable)
    pub fn new(columns: Vec<(String, ColumnType, bool)>) -> Self {
        Schema { columns }
    }

    /// Returns the number of columns in the schema.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns true if the schema has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Returns a list of all column names.
    pub fn get_column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|(name, _, _)| name.as_str()).collect()
    }

    /// Returns the index of a column by name, or None if not found.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|(n, _, _)| n == name)
    }

    /// Returns information about a column at the given index.
    ///
    /// Returns a tuple of (name, type, nullable) or None if index is out of bounds.
    pub fn get_column_info(&self, index: usize) -> Option<(&str, ColumnType, bool)> {
        self.columns.get(index).map(|(name, ty, nullable)| (name.as_str(), *ty, *nullable))
    }

    /// Returns the type of a column by name, or None if not found.
    pub fn get_column_type(&self, name: &str) -> Option<ColumnType> {
        self.columns.iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, ty, _)| *ty)
    }

    /// Returns whether a column is nullable by name, or None if not found.
    pub fn is_column_nullable(&self, name: &str) -> Option<bool> {
        self.columns.iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, _, nullable)| *nullable)
    }
}

/// Root table owning its data.
///
/// A Table is a collection of typed columns that can be queried and modified.
/// Tables support both array-based storage (fast random access) and tiered vector
/// storage (faster inserts/deletes).
///
/// # Examples
///
/// ```
/// use livetable::{Table, Schema, ColumnType, ColumnValue};
/// use std::collections::HashMap;
///
/// let schema = Schema::new(vec![
///     ("id".to_string(), ColumnType::Int32, false),
///     ("score".to_string(), ColumnType::Float64, false),
/// ]);
///
/// let mut table = Table::new("scores".to_string(), schema);
///
/// let mut row = HashMap::new();
/// row.insert("id".to_string(), ColumnValue::Int32(1));
/// row.insert("score".to_string(), ColumnValue::Float64(95.5));
/// table.append_row(row).unwrap();
///
/// assert_eq!(table.len(), 1);
/// ```
pub struct Table {
    name: String,
    schema: Schema,
    columns: Vec<Column>,
    row_count: usize,
    /// Tracks changes for incremental view propagation
    changeset: Changeset,
    /// Optional string interner shared across all string columns
    interner: Option<Rc<RefCell<StringInterner>>>,
}

impl Table {
    /// Create a new table with default settings (FastReads storage).
    pub fn new(name: String, schema: Schema) -> Self {
        Self::with_hint(name, schema, StorageHint::default())
    }

    /// Create a new table with a storage hint.
    ///
    /// # Arguments
    ///
    /// * `name` - Table name
    /// * `schema` - Table schema
    /// * `hint` - Storage optimization hint (FastReads or FastUpdates)
    ///
    /// # Examples
    ///
    /// ```
    /// use livetable::{Table, Schema, ColumnType, StorageHint};
    ///
    /// let schema = Schema::new(vec![
    ///     ("id".to_string(), ColumnType::Int32, false),
    /// ]);
    ///
    /// // For append-heavy workloads (default)
    /// let logs = Table::with_hint("logs".to_string(), schema.clone(), StorageHint::FastReads);
    ///
    /// // For frequent inserts/deletes
    /// let orderbook = Table::with_hint("orders".to_string(), schema, StorageHint::FastUpdates);
    /// ```
    pub fn with_hint(name: String, schema: Schema, hint: StorageHint) -> Self {
        Self::with_hint_and_interning(name, schema, hint, false)
    }

    /// Create a new table with storage hint and optional string interning.
    ///
    /// When `use_string_interning` is true, all String columns will share a
    /// common string interner, reducing memory usage for repeated strings.
    ///
    /// # Arguments
    ///
    /// * `name` - Table name
    /// * `schema` - Table schema
    /// * `hint` - Storage optimization hint
    /// * `use_string_interning` - Enable string interning for memory efficiency
    pub fn with_hint_and_interning(
        name: String,
        schema: Schema,
        hint: StorageHint,
        use_string_interning: bool,
    ) -> Self {
        let use_tiered_vector = hint.use_tiered_vector();
        // Create shared interner if string interning is enabled
        let interner = if use_string_interning {
            Some(Rc::new(RefCell::new(StringInterner::new())))
        } else {
            None
        };

        let columns: Vec<Column> = schema
            .columns
            .iter()
            .map(|(col_name, col_type, nullable)| {
                Column::new_with_interner(
                    col_name.clone(),
                    *col_type,
                    *nullable,
                    use_tiered_vector,
                    interner.clone(),
                )
            })
            .collect();

        Table {
            name,
            schema,
            columns,
            row_count: 0,
            changeset: Changeset::new(),
            interner,
        }
    }

    // ==================== Backward-compatible methods ====================

    /// Create a new table with boolean storage option (deprecated).
    ///
    /// Prefer using `with_hint()` for clearer intent.
    #[deprecated(since = "0.2.0", note = "Use `with_hint()` with `StorageHint` instead")]
    pub fn new_with_options(name: String, schema: Schema, use_tiered_vector: bool) -> Self {
        let hint = if use_tiered_vector {
            StorageHint::FastUpdates
        } else {
            StorageHint::FastReads
        };
        Self::with_hint(name, schema, hint)
    }

    /// Create a new table with boolean storage option and interning (deprecated).
    ///
    /// Prefer using `with_hint_and_interning()` for clearer intent.
    #[deprecated(since = "0.2.0", note = "Use `with_hint_and_interning()` with `StorageHint` instead")]
    pub fn new_with_interning(
        name: String,
        schema: Schema,
        use_tiered_vector: bool,
        use_string_interning: bool,
    ) -> Self {
        let hint = if use_tiered_vector {
            StorageHint::FastUpdates
        } else {
            StorageHint::FastReads
        };
        Self::with_hint_and_interning(name, schema, hint, use_string_interning)
    }

    /// Returns true if this table uses string interning
    pub fn uses_string_interning(&self) -> bool {
        self.interner.is_some()
    }

    /// Returns statistics about the string interner, if enabled
    pub fn interner_stats(&self) -> Option<InternerStats> {
        self.interner.as_ref().map(|i| i.borrow().stats())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn len(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        let col_idx = self.schema
            .get_column_index(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;

        self.columns[col_idx].get(row)
    }

    /// Get a value by column index (faster than get_value when column index is known).
    /// Used for performance-critical loops where column lookup can be done once upfront.
    #[inline]
    pub fn get_value_by_index(&self, row: usize, col_idx: usize) -> Result<ColumnValue, String> {
        self.columns.get(col_idx)
            .ok_or_else(|| format!("Column index {} out of range", col_idx))?
            .get(row)
    }

    pub fn set_value(&mut self, row: usize, column: &str, value: ColumnValue) -> Result<(), String> {
        let col_idx = self.schema
            .get_column_index(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;

        // Capture old value for changeset
        let old_value = self.columns[col_idx].get(row)?;

        // Update the value
        self.columns[col_idx].set(row, value.clone())?;

        // Record the change
        self.changeset.push(TableChange::CellUpdated {
            row,
            column: column.to_string(),
            old_value,
            new_value: value,
        });

        Ok(())
    }

    pub fn get_row(&self, row: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if row >= self.row_count {
            return Err(format!("Row {} out of range [0, {})", row, self.row_count));
        }

        let mut result = HashMap::new();
        for (i, col) in self.columns.iter().enumerate() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            result.insert(col_name.to_string(), col.get(row)?);
        }

        Ok(result)
    }

    pub fn append_row(&mut self, row: HashMap<String, ColumnValue>) -> Result<(), String> {
        // Validate all columns are present
        for col_name in self.schema.get_column_names() {
            if !row.contains_key(col_name) {
                return Err(format!("Missing value for column '{}'", col_name));
            }
        }

        let insert_index = self.row_count;

        // Append to each column
        for (i, col) in self.columns.iter_mut().enumerate() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            let value = row.get(col_name).unwrap().clone();
            col.append(value);
        }

        self.row_count += 1;

        // Record the change
        self.changeset.push(TableChange::RowInserted {
            index: insert_index,
            data: row,
        });

        Ok(())
    }

    /// Append multiple rows at once (bulk insert).
    ///
    /// This is more efficient than calling `append_row` repeatedly because:
    /// 1. Validation is done once for the column structure
    /// 2. Reduces function call overhead
    /// 3. Better memory allocation patterns
    ///
    /// # Arguments
    ///
    /// * `rows` - Vector of row data as HashMaps
    ///
    /// # Returns
    ///
    /// * `Ok(count)` - Number of rows successfully inserted
    /// * `Err(message)` - Error if any row is invalid (no rows inserted on error)
    ///
    /// # Example
    ///
    /// ```
    /// use livetable::{Table, Schema, ColumnType, ColumnValue};
    /// use std::collections::HashMap;
    ///
    /// let schema = Schema::new(vec![
    ///     ("id".to_string(), ColumnType::Int32, false),
    ///     ("name".to_string(), ColumnType::String, false),
    /// ]);
    /// let mut table = Table::new("users".to_string(), schema);
    ///
    /// let rows = vec![
    ///     {
    ///         let mut r = HashMap::new();
    ///         r.insert("id".to_string(), ColumnValue::Int32(1));
    ///         r.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
    ///         r
    ///     },
    ///     {
    ///         let mut r = HashMap::new();
    ///         r.insert("id".to_string(), ColumnValue::Int32(2));
    ///         r.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
    ///         r
    ///     },
    /// ];
    ///
    /// let count = table.append_rows(rows).unwrap();
    /// assert_eq!(count, 2);
    /// assert_eq!(table.len(), 2);
    /// ```
    pub fn append_rows(&mut self, rows: Vec<HashMap<String, ColumnValue>>) -> Result<usize, String> {
        if rows.is_empty() {
            return Ok(0);
        }

        let col_names: Vec<&str> = self.schema.get_column_names();

        // Validate all rows first (before inserting any)
        for (row_idx, row) in rows.iter().enumerate() {
            for col_name in &col_names {
                if !row.contains_key(*col_name) {
                    return Err(format!(
                        "Row {}: Missing value for column '{}'",
                        row_idx, col_name
                    ));
                }
            }
        }

        let start_index = self.row_count;
        let num_rows = rows.len();

        // Insert all rows
        for (row_offset, row) in rows.into_iter().enumerate() {
            let insert_index = start_index + row_offset;

            // Append to each column
            for (i, col) in self.columns.iter_mut().enumerate() {
                let col_name = col_names[i];
                let value = row.get(col_name).unwrap().clone();
                col.append(value);
            }

            self.row_count += 1;

            // Record the change
            self.changeset.push(TableChange::RowInserted {
                index: insert_index,
                data: row,
            });
        }

        Ok(num_rows)
    }

    pub fn insert_row(&mut self, index: usize, row: HashMap<String, ColumnValue>) -> Result<(), String> {
        if index > self.row_count {
            return Err(format!("Index {} out of range [0, {}]", index, self.row_count));
        }

        // Validate all columns are present
        for col_name in self.schema.get_column_names() {
            if !row.contains_key(col_name) {
                return Err(format!("Missing value for column '{}'", col_name));
            }
        }

        // Insert into each column
        for (i, col) in self.columns.iter_mut().enumerate() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            let value = row.get(col_name).unwrap().clone();
            col.insert(index, value)?;
        }

        self.row_count += 1;

        // Record the change
        self.changeset.push(TableChange::RowInserted {
            index,
            data: row,
        });

        Ok(())
    }

    pub fn delete_row(&mut self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if index >= self.row_count {
            return Err(format!("Row {} out of range [0, {})", index, self.row_count));
        }

        let mut result = HashMap::new();

        for (i, col) in self.columns.iter_mut().enumerate() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            result.insert(col_name.to_string(), col.delete(index)?);
        }

        self.row_count -= 1;

        // Record the change
        self.changeset.push(TableChange::RowDeleted {
            index,
            data: result.clone(),
        });

        Ok(result)
    }

    pub fn iter_rows(&self) -> TableRowIterator<'_> {
        TableRowIterator {
            table: self,
            index: 0,
        }
    }

    // === Changeset API for incremental view propagation ===

    /// Returns a reference to the current changeset
    pub fn changeset(&self) -> &Changeset {
        &self.changeset
    }

    /// Returns the current changeset generation number
    pub fn changeset_generation(&self) -> u64 {
        self.changeset.generation()
    }

    /// Drains and returns all pending changes, clearing the buffer
    /// Use this when you've finished propagating changes to all views
    pub fn drain_changes(&mut self) -> Vec<TableChange> {
        self.changeset.drain()
    }

    /// Clears the changeset without returning the changes
    pub fn clear_changeset(&mut self) {
        self.changeset.clear();
    }

    /// Compact the changeset up to the given absolute change index
    pub fn compact_changeset(&mut self, up_to_index: usize) {
        self.changeset.compact(up_to_index);
    }

    /// Returns true if there are pending changes
    pub fn has_pending_changes(&self) -> bool {
        !self.changeset.is_empty()
    }

    // ========================================================================
    // Aggregation Methods
    // ========================================================================

    /// Calculate the sum of all numeric values in a column.
    /// NULL values are skipped.
    pub fn sum(&self, column: &str) -> Result<f64, String> {
        let col_idx = self.schema
            .get_column_index(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;

        let col = &self.columns[col_idx];
        let mut total = 0.0;
        for i in 0..self.row_count {
            if let Some(num) = col.get_f64(i) {
                total += num;
            }
        }
        Ok(total)
    }

    /// Count the number of non-NULL values in a column.
    pub fn count_non_null(&self, column: &str) -> Result<usize, String> {
        let col_idx = self.schema
            .get_column_index(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;

        let col = &self.columns[col_idx];
        let mut count = 0;
        for i in 0..self.row_count {
            if !col.is_null_at(i) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Calculate the average of all numeric values in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    pub fn avg(&self, column: &str) -> Result<Option<f64>, String> {
        let col_idx = self.schema
            .get_column_index(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;

        let col = &self.columns[col_idx];
        let mut sum = 0.0;
        let mut count = 0;
        for i in 0..self.row_count {
            if let Some(num) = col.get_f64(i) {
                sum += num;
                count += 1;
            }
        }

        if count > 0 {
            Ok(Some(sum / count as f64))
        } else {
            Ok(None)
        }
    }

    /// Find the minimum numeric value in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    pub fn min(&self, column: &str) -> Result<Option<f64>, String> {
        let col_idx = self.schema
            .get_column_index(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;

        let col = &self.columns[col_idx];
        let mut min_val: Option<f64> = None;
        for i in 0..self.row_count {
            if let Some(num) = col.get_f64(i) {
                min_val = Some(min_val.map_or(num, |m| m.min(num)));
            }
        }
        Ok(min_val)
    }

    /// Find the maximum numeric value in a column.
    /// NULL values are skipped. Returns None if there are no non-NULL numeric values.
    pub fn max(&self, column: &str) -> Result<Option<f64>, String> {
        let col_idx = self.schema
            .get_column_index(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;

        let col = &self.columns[col_idx];
        let mut max_val: Option<f64> = None;
        for i in 0..self.row_count {
            if let Some(num) = col.get_f64(i) {
                max_val = Some(max_val.map_or(num, |m| m.max(num)));
            }
        }
        Ok(max_val)
    }

    // ========================================================================
    // Serialization Methods
    // ========================================================================

    /// Export table to CSV format.
    ///
    /// Returns a CSV string with headers and data rows.
    /// NULL values become empty strings.
    /// Strings containing commas, quotes, or newlines are properly escaped.
    ///
    /// # Example
    ///
    /// ```
    /// use livetable::{Table, Schema, ColumnType, ColumnValue};
    /// use std::collections::HashMap;
    ///
    /// let schema = Schema::new(vec![
    ///     ("id".to_string(), ColumnType::Int32, false),
    ///     ("name".to_string(), ColumnType::String, false),
    /// ]);
    /// let mut table = Table::new("test".to_string(), schema);
    /// let mut row = HashMap::new();
    /// row.insert("id".to_string(), ColumnValue::Int32(1));
    /// row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
    /// table.append_row(row).unwrap();
    ///
    /// let csv = table.to_csv();
    /// assert!(csv.contains("id,name"));
    /// assert!(csv.contains("1,Alice"));
    /// ```
    pub fn to_csv(&self) -> String {
        let mut result = String::new();
        let column_names = self.schema.get_column_names();

        // Header row
        result.push_str(&column_names.join(","));
        result.push('\n');

        // Data rows
        for row in self.iter_rows() {
            let values: Vec<String> = column_names.iter().map(|col| {
                match row.get(*col) {
                    Some(ColumnValue::Null) | None => String::new(),
                    Some(ColumnValue::String(s)) => {
                        // Escape quotes and wrap if contains comma/quote/newline
                        if s.contains(',') || s.contains('"') || s.contains('\n') {
                            format!("\"{}\"", s.replace('"', "\"\""))
                        } else {
                            s.clone()
                        }
                    }
                    Some(ColumnValue::Bool(b)) => b.to_string(),
                    Some(ColumnValue::Int32(n)) => n.to_string(),
                    Some(ColumnValue::Int64(n)) => n.to_string(),
                    Some(ColumnValue::Float32(f)) => f.to_string(),
                    Some(ColumnValue::Float64(f)) => f.to_string(),
                    Some(ColumnValue::Date(days)) => format_date(*days),
                    Some(ColumnValue::DateTime(ms)) => format_datetime(*ms),
                }
            }).collect();
            result.push_str(&values.join(","));
            result.push('\n');
        }
        result
    }

    /// Export table to JSON format (array of objects).
    ///
    /// Returns a pretty-printed JSON string representing the table as an array
    /// of objects, where each object is a row with column names as keys.
    ///
    /// # Example
    ///
    /// ```
    /// use livetable::{Table, Schema, ColumnType, ColumnValue};
    /// use std::collections::HashMap;
    ///
    /// let schema = Schema::new(vec![
    ///     ("id".to_string(), ColumnType::Int32, false),
    ///     ("name".to_string(), ColumnType::String, false),
    /// ]);
    /// let mut table = Table::new("test".to_string(), schema);
    /// let mut row = HashMap::new();
    /// row.insert("id".to_string(), ColumnValue::Int32(1));
    /// row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
    /// table.append_row(row).unwrap();
    ///
    /// let json = table.to_json().unwrap();
    /// assert!(json.contains("\"id\": 1"));
    /// assert!(json.contains("\"name\": \"Alice\""));
    /// ```
    pub fn to_json(&self) -> Result<String, String> {
        let column_names = self.schema.get_column_names();
        let rows: Vec<serde_json::Value> = self.iter_rows()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = column_names.iter()
                    .map(|col| {
                        let json_val = match row.get(*col) {
                            Some(ColumnValue::Int32(n)) => serde_json::Value::Number((*n).into()),
                            Some(ColumnValue::Int64(n)) => serde_json::Value::Number((*n).into()),
                            Some(ColumnValue::Float32(f)) => {
                                serde_json::Number::from_f64(*f as f64)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            }
                            Some(ColumnValue::Float64(f)) => {
                                serde_json::Number::from_f64(*f)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            }
                            Some(ColumnValue::String(s)) => serde_json::Value::String(s.clone()),
                            Some(ColumnValue::Bool(b)) => serde_json::Value::Bool(*b),
                            Some(ColumnValue::Date(days)) => serde_json::Value::String(format_date(*days)),
                            Some(ColumnValue::DateTime(ms)) => serde_json::Value::String(format_datetime(*ms)),
                            Some(ColumnValue::Null) | None => serde_json::Value::Null,
                        };
                        (col.to_string(), json_val)
                    })
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();

        serde_json::to_string_pretty(&rows)
            .map_err(|e| format!("JSON serialization error: {}", e))
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
    /// All columns are created as nullable to handle empty values.
    ///
    /// # Example
    ///
    /// ```
    /// use livetable::Table;
    ///
    /// let csv = "id,name,score\n1,Alice,95.5\n2,Bob,87.0";
    /// let table = Table::from_csv("students", csv).unwrap();
    /// assert_eq!(table.len(), 2);
    /// ```
    pub fn from_csv(name: &str, csv: &str) -> Result<Table, String> {
        // Parse all rows at once to handle multi-line quoted fields
        let mut all_rows = parse_csv_rows(csv);

        if all_rows.is_empty() {
            return Err("CSV is empty".to_string());
        }

        // First row is the header
        let column_names = all_rows.remove(0);

        if column_names.is_empty() {
            return Err("CSV header is empty".to_string());
        }

        // Filter out empty rows
        let rows: Vec<Vec<String>> = all_rows
            .into_iter()
            .filter(|row| !row.iter().all(|f| f.is_empty()))
            .collect();

        // Infer types from first row (or default to STRING if no data)
        let types = if rows.is_empty() {
            // No data rows - default all columns to STRING
            vec![ColumnType::String; column_names.len()]
        } else {
            let inferred = infer_types_from_csv_row(rows.first());
            // Ensure we have the right number of types
            if inferred.len() != column_names.len() {
                return Err(format!(
                    "Column count mismatch: header has {}, but data row has {} values",
                    column_names.len(),
                    inferred.len()
                ));
            }
            inferred
        };

        // Create schema (all nullable)
        let schema_cols: Vec<(String, ColumnType, bool)> = column_names
            .iter()
            .zip(types.iter())
            .map(|(name, typ)| (name.clone(), *typ, true))
            .collect();

        let schema = Schema::new(schema_cols);
        let mut table = Table::new(name.to_string(), schema);

        // Populate table
        for row_values in rows {
            let row_map = build_row_map_from_csv(&column_names, &row_values, &types)?;
            table.append_row(row_map)?;
        }

        Ok(table)
    }

    /// Create a table from a JSON string (array of objects).
    ///
    /// Expects a JSON array where each element is an object representing a row.
    /// Column types are inferred from the first object:
    /// - JSON numbers → FLOAT64 (for safety with decimals) or INT64 (if integer)
    /// - JSON strings → STRING
    /// - JSON booleans → BOOL
    /// - JSON null → nullable column
    ///
    /// All columns are created as nullable.
    ///
    /// # Example
    ///
    /// ```
    /// use livetable::Table;
    ///
    /// let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
    /// let table = Table::from_json("users", json).unwrap();
    /// assert_eq!(table.len(), 2);
    /// ```
    pub fn from_json(name: &str, json: &str) -> Result<Table, String> {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(json)
            .map_err(|e| format!("JSON parse error: {}", e))?;

        if parsed.is_empty() {
            return Err("JSON array is empty".to_string());
        }

        // Infer schema from first object
        let first = parsed[0].as_object()
            .ok_or("Expected array of objects")?;

        let schema = infer_schema_from_json(first)?;
        let mut table = Table::new(name.to_string(), schema);

        // Populate rows
        for item in &parsed {
            let obj = item.as_object().ok_or("Expected object in array")?;
            let row_map = json_object_to_row_map(obj)?;
            table.append_row(row_map)?;
        }

        Ok(table)
    }

    // ========================================================================
    // Expression-based Filtering
    // ========================================================================

    /// Filter rows using an expression string.
    ///
    /// This is faster than lambda-based filtering because the expression is
    /// evaluated entirely in Rust without Python callbacks.
    ///
    /// # Supported syntax
    ///
    /// - Comparisons: `column > 90`, `name == 'Alice'`, `value != 0`
    /// - Logical operators: `AND`, `OR`, `NOT`
    /// - Parentheses: `(score > 90) AND (age >= 18)`
    /// - NULL checks: `column IS NULL`, `column IS NOT NULL`
    ///
    /// # Example
    ///
    /// ```
    /// use livetable::{Table, Schema, ColumnType, ColumnValue};
    /// use std::collections::HashMap;
    ///
    /// let schema = Schema::new(vec![
    ///     ("name".to_string(), ColumnType::String, false),
    ///     ("score".to_string(), ColumnType::Float64, false),
    /// ]);
    /// let mut table = Table::new("test".to_string(), schema);
    ///
    /// let mut row = HashMap::new();
    /// row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
    /// row.insert("score".to_string(), ColumnValue::Float64(95.0));
    /// table.append_row(row).unwrap();
    ///
    /// let indices = table.filter_expr("score > 90").unwrap();
    /// assert_eq!(indices.len(), 1);
    /// assert_eq!(indices[0], 0);
    /// ```
    pub fn filter_expr(&self, expression: &str) -> Result<Vec<usize>, String> {
        let expr = crate::expr::parse_expr(expression)?;

        // Pre-compute column indices for columns used in the expression
        let expr_columns = crate::expr::extract_columns(&expr);
        let column_indices: std::collections::HashMap<String, usize> = expr_columns
            .iter()
            .filter_map(|name| {
                self.schema.get_column_index(name).map(|idx| (name.clone(), idx))
            })
            .collect();

        let mut matching_indices = Vec::new();

        for row_idx in 0..self.row_count {
            // Use eval_expr_fast with a closure that directly accesses columns
            let matches = crate::expr::eval_expr_fast(&expr, &|col_name: &str| {
                column_indices.get(col_name).and_then(|&col_idx| {
                    self.columns.get(col_idx).and_then(|col| col.get(row_idx).ok())
                })
            });

            if matches {
                matching_indices.push(row_idx);
            }
        }

        Ok(matching_indices)
    }
}

// ============================================================================
// Helper functions for serialization
// ============================================================================

/// Convert days since Unix epoch (1970-01-01) to (year, month, day)
fn ymd_from_days(days: i32) -> (i32, u32, u32) {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = if z >= 0 { z / 146097 } else { (z - 146096) / 146097 };
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i32) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Convert (year, month, day) to days since Unix epoch
fn days_from_ymd(year: i32, month: u32, day: u32) -> i32 {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = (y - era * 400) as u32;
    let m = month;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe as i32) - 719468
}

/// Format a date (days since epoch) as ISO 8601 date string (YYYY-MM-DD)
fn format_date(days: i32) -> String {
    let (year, month, day) = ymd_from_days(days);
    format!("{:04}-{:02}-{:02}", year, month, day)
}

/// Format a datetime (milliseconds since epoch) as ISO 8601 datetime string
fn format_datetime(ms: i64) -> String {
    // Handle negative milliseconds (dates before epoch)
    let (days, time_ms) = if ms >= 0 {
        ((ms / 86_400_000) as i32, (ms % 86_400_000) as u32)
    } else {
        // For negative ms, we need to adjust to get correct date
        let d = (ms / 86_400_000) as i32 - if ms % 86_400_000 != 0 { 1 } else { 0 };
        let t = ((ms % 86_400_000) + 86_400_000) as u32 % 86_400_000;
        (d, t)
    };

    let (year, month, day) = ymd_from_days(days);
    let hour = time_ms / 3_600_000;
    let minute = (time_ms % 3_600_000) / 60_000;
    let second = (time_ms % 60_000) / 1000;
    let millisecond = time_ms % 1000;

    if millisecond > 0 {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}",
            year, month, day, hour, minute, second, millisecond)
    } else {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            year, month, day, hour, minute, second)
    }
}

/// Parse an ISO 8601 date string (YYYY-MM-DD) to days since epoch
fn parse_date(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;

    if month < 1 || month > 12 || day < 1 || day > 31 {
        return None;
    }

    Some(days_from_ymd(year, month, day))
}

/// Parse an ISO 8601 datetime string to milliseconds since epoch
fn parse_datetime(s: &str) -> Option<i64> {
    // Try to split on 'T' or space
    let (date_part, time_part) = if s.contains('T') {
        let parts: Vec<&str> = s.splitn(2, 'T').collect();
        if parts.len() != 2 {
            return None;
        }
        (parts[0], parts[1])
    } else if s.contains(' ') {
        let parts: Vec<&str> = s.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return None;
        }
        (parts[0], parts[1])
    } else {
        // Just a date, treat as midnight
        return parse_date(s).map(|d| (d as i64) * 86_400_000);
    };

    let days = parse_date(date_part)?;

    // Parse time part (HH:MM:SS or HH:MM:SS.mmm)
    let time_part = time_part.trim_end_matches('Z'); // Remove trailing Z if present
    let (time_str, ms) = if time_part.contains('.') {
        let parts: Vec<&str> = time_part.splitn(2, '.').collect();
        let ms_str = parts.get(1)?;
        // Handle variable length milliseconds (e.g., .1, .12, .123, .123456)
        let ms: u32 = if ms_str.len() >= 3 {
            ms_str[..3].parse().ok()?
        } else {
            // Pad with zeros: "1" -> 100, "12" -> 120
            let padded = format!("{:0<3}", ms_str);
            padded.parse().ok()?
        };
        (parts[0], ms)
    } else {
        (time_part, 0)
    };

    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.len() < 2 {
        return None;
    }

    let hour: u32 = time_parts[0].parse().ok()?;
    let minute: u32 = time_parts[1].parse().ok()?;
    let second: u32 = time_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let time_ms = (hour as i64) * 3_600_000
        + (minute as i64) * 60_000
        + (second as i64) * 1000
        + (ms as i64);

    Some((days as i64) * 86_400_000 + time_ms)
}

/// Parse a CSV string into rows, handling quoted fields with embedded newlines
fn parse_csv_rows(csv: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut current_row = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = csv.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes => {
                // Check for escaped quote (double quote)
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current_field.push('"');
                } else {
                    in_quotes = false;
                }
            }
            '"' if !in_quotes => {
                in_quotes = true;
            }
            ',' if !in_quotes => {
                current_row.push(current_field.clone());
                current_field.clear();
            }
            '\n' if !in_quotes => {
                // Handle \r\n line endings
                current_row.push(current_field.clone());
                current_field.clear();
                if !current_row.iter().all(|f| f.is_empty()) || !current_row.is_empty() {
                    rows.push(current_row);
                }
                current_row = Vec::new();
            }
            '\r' if !in_quotes => {
                // Skip \r, will be followed by \n
            }
            _ => {
                current_field.push(c);
            }
        }
    }

    // Don't forget the last field/row
    if !current_field.is_empty() || !current_row.is_empty() {
        current_row.push(current_field);
        rows.push(current_row);
    }

    rows
}

/// Infer column types from a CSV row
fn infer_types_from_csv_row(row: Option<&Vec<String>>) -> Vec<ColumnType> {
    match row {
        None => Vec::new(),
        Some(values) => values.iter().map(|v| infer_type_from_csv_value(v)).collect(),
    }
}

/// Infer the type of a single CSV value
fn infer_type_from_csv_value(value: &str) -> ColumnType {
    let trimmed = value.trim();

    // Empty string → STRING (will be nullable)
    if trimmed.is_empty() {
        return ColumnType::String;
    }

    // Check for boolean
    if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
        return ColumnType::Bool;
    }

    // Check for datetime (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS)
    if (trimmed.contains('T') || (trimmed.contains(' ') && trimmed.contains(':')))
        && parse_datetime(trimmed).is_some() {
        return ColumnType::DateTime;
    }

    // Check for date (YYYY-MM-DD)
    if trimmed.len() == 10 && trimmed.chars().nth(4) == Some('-') && parse_date(trimmed).is_some() {
        return ColumnType::Date;
    }

    // Check for integer
    if let Ok(n) = trimmed.parse::<i64>() {
        // Use INT32 if it fits, otherwise INT64
        if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
            return ColumnType::Int32;
        }
        return ColumnType::Int64;
    }

    // Check for float
    if trimmed.parse::<f64>().is_ok() {
        return ColumnType::Float64;
    }

    // Default to STRING
    ColumnType::String
}

/// Build a row map from CSV values
fn build_row_map_from_csv(
    column_names: &[String],
    values: &[String],
    types: &[ColumnType],
) -> Result<HashMap<String, ColumnValue>, String> {
    let mut row = HashMap::new();

    for (i, col_name) in column_names.iter().enumerate() {
        let value_str = values.get(i).map(|s| s.as_str()).unwrap_or("");
        let col_type = types.get(i).copied().unwrap_or(ColumnType::String);

        let value = parse_csv_value(value_str, col_type)?;
        row.insert(col_name.clone(), value);
    }

    Ok(row)
}

/// Parse a CSV value into a ColumnValue based on expected type
fn parse_csv_value(value: &str, col_type: ColumnType) -> Result<ColumnValue, String> {
    let trimmed = value.trim();

    // Empty string → NULL for any type
    if trimmed.is_empty() {
        return Ok(ColumnValue::Null);
    }

    match col_type {
        ColumnType::Int32 => {
            trimmed.parse::<i32>()
                .map(ColumnValue::Int32)
                .map_err(|_| format!("Cannot parse '{}' as INT32", trimmed))
        }
        ColumnType::Int64 => {
            trimmed.parse::<i64>()
                .map(ColumnValue::Int64)
                .map_err(|_| format!("Cannot parse '{}' as INT64", trimmed))
        }
        ColumnType::Float32 => {
            trimmed.parse::<f32>()
                .map(ColumnValue::Float32)
                .map_err(|_| format!("Cannot parse '{}' as FLOAT32", trimmed))
        }
        ColumnType::Float64 => {
            trimmed.parse::<f64>()
                .map(ColumnValue::Float64)
                .map_err(|_| format!("Cannot parse '{}' as FLOAT64", trimmed))
        }
        ColumnType::Bool => {
            if trimmed.eq_ignore_ascii_case("true") {
                Ok(ColumnValue::Bool(true))
            } else if trimmed.eq_ignore_ascii_case("false") {
                Ok(ColumnValue::Bool(false))
            } else {
                Err(format!("Cannot parse '{}' as BOOL", trimmed))
            }
        }
        ColumnType::String => {
            Ok(ColumnValue::String(trimmed.to_string()))
        }
        ColumnType::Date => {
            parse_date(trimmed)
                .map(ColumnValue::Date)
                .ok_or_else(|| format!("Cannot parse '{}' as DATE (expected YYYY-MM-DD)", trimmed))
        }
        ColumnType::DateTime => {
            parse_datetime(trimmed)
                .map(ColumnValue::DateTime)
                .ok_or_else(|| format!("Cannot parse '{}' as DATETIME (expected YYYY-MM-DDTHH:MM:SS)", trimmed))
        }
    }
}

/// Infer schema from a JSON object
fn infer_schema_from_json(obj: &serde_json::Map<String, serde_json::Value>) -> Result<Schema, String> {
    let mut columns = Vec::new();

    for (key, value) in obj {
        let col_type = match value {
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    let val = n.as_i64().unwrap();
                    if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                        ColumnType::Int32
                    } else {
                        ColumnType::Int64
                    }
                } else {
                    ColumnType::Float64
                }
            }
            serde_json::Value::String(s) => {
                // Try to detect date/datetime strings
                if (s.contains('T') || (s.contains(' ') && s.contains(':')))
                    && parse_datetime(s).is_some() {
                    ColumnType::DateTime
                } else if s.len() == 10 && s.chars().nth(4) == Some('-') && parse_date(s).is_some() {
                    ColumnType::Date
                } else {
                    ColumnType::String
                }
            }
            serde_json::Value::Bool(_) => ColumnType::Bool,
            serde_json::Value::Null => ColumnType::String, // Default to String for null
            _ => return Err(format!("Unsupported JSON value type for column '{}'", key)),
        };
        columns.push((key.clone(), col_type, true)); // All nullable
    }

    Ok(Schema::new(columns))
}

/// Convert a JSON object to a row map
fn json_object_to_row_map(obj: &serde_json::Map<String, serde_json::Value>) -> Result<HashMap<String, ColumnValue>, String> {
    let mut row = HashMap::new();

    for (key, value) in obj {
        let col_value = match value {
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    let val = n.as_i64().unwrap();
                    if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                        ColumnValue::Int32(val as i32)
                    } else {
                        ColumnValue::Int64(val)
                    }
                } else {
                    ColumnValue::Float64(n.as_f64().unwrap())
                }
            }
            serde_json::Value::String(s) => {
                // Try to detect and parse date/datetime strings
                if (s.contains('T') || (s.contains(' ') && s.contains(':')))
                    && parse_datetime(s).is_some() {
                    ColumnValue::DateTime(parse_datetime(s).unwrap())
                } else if s.len() == 10 && s.chars().nth(4) == Some('-') && parse_date(s).is_some() {
                    ColumnValue::Date(parse_date(s).unwrap())
                } else {
                    ColumnValue::String(s.clone())
                }
            }
            serde_json::Value::Bool(b) => ColumnValue::Bool(*b),
            serde_json::Value::Null => ColumnValue::Null,
            _ => return Err(format!("Unsupported JSON value type for key '{}'", key)),
        };
        row.insert(key.clone(), col_value);
    }

    Ok(row)
}

pub struct TableRowIterator<'a> {
    table: &'a Table,
    index: usize,
}

impl<'a> Iterator for TableRowIterator<'a> {
    type Item = HashMap<String, ColumnValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.table.row_count {
            None
        } else {
            let result = self.table.get_row(self.index).ok();
            self.index += 1;
            result
        }
    }
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Table {{ name: '{}', columns: {}, rows: {} }}",
            self.name,
            self.schema.len(),
            self.row_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_basic() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
            ("age".to_string(), ColumnType::Int32, true),
        ]);

        let mut table = Table::new("users".to_string(), schema);

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row1.insert("age".to_string(), ColumnValue::Int32(30));

        table.append_row(row1).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(
            table.get_value(0, "name").unwrap().as_string(),
            Some("Alice")
        );
    }

    #[test]
    fn test_table_insert() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
        ]);

        let mut table = Table::new("test".to_string(), schema);

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        table.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(3));
        table.append_row(row2).unwrap();

        let mut row_mid = HashMap::new();
        row_mid.insert("id".to_string(), ColumnValue::Int32(2));
        table.insert_row(1, row_mid).unwrap();

        assert_eq!(table.len(), 3);
        assert_eq!(table.get_value(0, "id").unwrap().as_i32(), Some(1));
        assert_eq!(table.get_value(1, "id").unwrap().as_i32(), Some(2));
        assert_eq!(table.get_value(2, "id").unwrap().as_i32(), Some(3));
    }

    #[test]
    fn test_table_delete() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::String, false),
        ]);

        let mut table = Table::new("test".to_string(), schema);

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("value".to_string(), ColumnValue::String("first".to_string()));
        table.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("value".to_string(), ColumnValue::String("second".to_string()));
        table.append_row(row2).unwrap();

        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), ColumnValue::Int32(3));
        row3.insert("value".to_string(), ColumnValue::String("third".to_string()));
        table.append_row(row3).unwrap();

        // Delete middle row
        let deleted = table.delete_row(1).unwrap();
        assert_eq!(deleted.get("id").unwrap().as_i32(), Some(2));
        assert_eq!(deleted.get("value").unwrap().as_string(), Some("second"));

        assert_eq!(table.len(), 2);
        assert_eq!(table.get_value(0, "id").unwrap().as_i32(), Some(1));
        assert_eq!(table.get_value(1, "id").unwrap().as_i32(), Some(3));
    }

    #[test]
    fn test_table_get_row() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
            ("score".to_string(), ColumnType::Float64, false),
        ]);

        let mut table = Table::new("test".to_string(), schema);

        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(42));
        row.insert("name".to_string(), ColumnValue::String("test".to_string()));
        row.insert("score".to_string(), ColumnValue::Float64(95.5));
        table.append_row(row).unwrap();

        let retrieved = table.get_row(0).unwrap();
        assert_eq!(retrieved.get("id").unwrap().as_i32(), Some(42));
        assert_eq!(retrieved.get("name").unwrap().as_string(), Some("test"));
        assert_eq!(retrieved.get("score").unwrap().as_f64(), Some(95.5));
    }

    #[test]
    fn test_table_set_value() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("value".to_string(), ColumnType::Int32, false),
        ]);

        let mut table = Table::new("test".to_string(), schema);

        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("value".to_string(), ColumnValue::Int32(100));
        table.append_row(row).unwrap();

        // Update value
        table.set_value(0, "value", ColumnValue::Int32(200)).unwrap();

        assert_eq!(table.get_value(0, "value").unwrap().as_i32(), Some(200));
        assert_eq!(table.get_value(0, "id").unwrap().as_i32(), Some(1));
    }

    #[test]
    fn test_table_string_interning() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("category".to_string(), ColumnType::String, false),
            ("name".to_string(), ColumnType::String, false),
        ]);

        // Create table with string interning enabled
        let mut table = Table::new_with_interning("products".to_string(), schema, false, true);
        assert!(table.uses_string_interning());

        // Add rows with repeated category strings
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("category".to_string(), ColumnValue::String("Electronics".to_string()));
        row1.insert("name".to_string(), ColumnValue::String("Phone".to_string()));
        table.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("category".to_string(), ColumnValue::String("Electronics".to_string())); // Duplicate
        row2.insert("name".to_string(), ColumnValue::String("Laptop".to_string()));
        table.append_row(row2).unwrap();

        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), ColumnValue::Int32(3));
        row3.insert("category".to_string(), ColumnValue::String("Clothing".to_string()));
        row3.insert("name".to_string(), ColumnValue::String("Shirt".to_string()));
        table.append_row(row3).unwrap();

        let mut row4 = HashMap::new();
        row4.insert("id".to_string(), ColumnValue::Int32(4));
        row4.insert("category".to_string(), ColumnValue::String("Electronics".to_string())); // Another duplicate
        row4.insert("name".to_string(), ColumnValue::String("Tablet".to_string()));
        table.append_row(row4).unwrap();

        // Verify data is correct
        assert_eq!(table.len(), 4);
        assert_eq!(table.get_value(0, "category").unwrap().as_string(), Some("Electronics"));
        assert_eq!(table.get_value(1, "category").unwrap().as_string(), Some("Electronics"));
        assert_eq!(table.get_value(2, "category").unwrap().as_string(), Some("Clothing"));
        assert_eq!(table.get_value(3, "category").unwrap().as_string(), Some("Electronics"));

        // Check interner stats - should have deduplicated strings
        let stats = table.interner_stats().unwrap();
        // "Electronics" (3 refs), "Clothing" (1 ref), "Phone", "Laptop", "Shirt", "Tablet" (1 ref each)
        assert_eq!(stats.unique_strings, 6);
        // Total refs: Electronics(3) + Clothing(1) + Phone(1) + Laptop(1) + Shirt(1) + Tablet(1) = 8
        assert_eq!(stats.total_references, 8);
    }

    #[test]
    fn test_table_string_interning_update() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("status".to_string(), ColumnType::String, false),
        ]);

        let mut table = Table::new_with_interning("orders".to_string(), schema, false, true);

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("status".to_string(), ColumnValue::String("pending".to_string()));
        table.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("status".to_string(), ColumnValue::String("pending".to_string()));
        table.append_row(row2).unwrap();

        // Update first order status
        table.set_value(0, "status", ColumnValue::String("completed".to_string())).unwrap();

        assert_eq!(table.get_value(0, "status").unwrap().as_string(), Some("completed"));
        assert_eq!(table.get_value(1, "status").unwrap().as_string(), Some("pending"));

        // Check stats after update
        let stats = table.interner_stats().unwrap();
        assert_eq!(stats.unique_strings, 2); // "pending" and "completed"
    }

    #[test]
    fn test_table_without_string_interning() {
        let schema = Schema::new(vec![
            ("name".to_string(), ColumnType::String, false),
        ]);

        // Create table without string interning (default)
        let table = Table::new("simple".to_string(), schema);
        assert!(!table.uses_string_interning());
        assert!(table.interner_stats().is_none());
    }
}
