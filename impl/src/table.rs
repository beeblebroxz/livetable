use crate::changeset::{Changeset, TableChange};
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
use crate::interner::{InternerStats, StringInterner};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

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
    /// Returns true if this hint uses tiered vector storage.
    pub(crate) fn use_tiered_vector(&self) -> bool {
        matches!(self, StorageHint::FastUpdates)
    }
}

impl FromStr for StorageHint {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fast_reads" | "fastreads" => Ok(StorageHint::FastReads),
            "fast_updates" | "fastupdates" => Ok(StorageHint::FastUpdates),
            _ => Err(format!(
                "Unknown storage hint: '{}'. Use 'fast_reads' or 'fast_updates'",
                s
            )),
        }
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
        self.columns
            .iter()
            .map(|(name, _, _)| name.as_str())
            .collect()
    }

    /// Returns the index of a column by name, or None if not found.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|(n, _, _)| n == name)
    }

    /// Returns information about a column at the given index.
    ///
    /// Returns a tuple of (name, type, nullable) or None if index is out of bounds.
    pub fn get_column_info(&self, index: usize) -> Option<(&str, ColumnType, bool)> {
        self.columns
            .get(index)
            .map(|(name, ty, nullable)| (name.as_str(), *ty, *nullable))
    }

    /// Returns the type of a column by name, or None if not found.
    pub fn get_column_type(&self, name: &str) -> Option<ColumnType> {
        self.columns
            .iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, ty, _)| *ty)
    }

    /// Returns whether a column is nullable by name, or None if not found.
    pub fn is_column_nullable(&self, name: &str) -> Option<bool> {
        self.columns
            .iter()
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
    interner: Option<Arc<Mutex<StringInterner>>>,
    /// Monotonic counter bumped on every row-data mutation. Used by
    /// iterators to detect mutation-during-iteration. Never decreases;
    /// unaffected by changeset lifecycle operations.
    mutation_count: u64,
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
            Some(Arc::new(Mutex::new(StringInterner::new())))
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
            mutation_count: 0,
        }
    }

    /// Returns a monotonic counter that bumps on every row-data mutation.
    /// Iterators capture this at creation and recheck during `__next__`
    /// to detect mutation-during-iteration (Python dict/set semantics).
    #[inline]
    pub fn version(&self) -> u64 {
        self.mutation_count
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
    #[deprecated(
        since = "0.2.0",
        note = "Use `with_hint_and_interning()` with `StorageHint` instead"
    )]
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
        self.interner.as_ref().map(|i| i.lock().unwrap().stats())
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
        if row >= self.row_count {
            return Err(format!("Row {} out of range [0, {})", row, self.row_count));
        }
        let col_idx = self
            .schema
            .get_column_index(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;

        self.columns[col_idx].get(row)
    }

    /// Get a value by column index (faster than get_value when column index is known).
    /// Used for performance-critical loops where column lookup can be done once upfront.
    #[inline]
    pub fn get_value_by_index(&self, row: usize, col_idx: usize) -> Result<ColumnValue, String> {
        self.columns
            .get(col_idx)
            .ok_or_else(|| format!("Column index {} out of range", col_idx))?
            .get(row)
    }

    pub fn set_value(
        &mut self,
        row: usize,
        column: &str,
        value: ColumnValue,
    ) -> Result<(), String> {
        if row >= self.row_count {
            return Err(format!("Row {} out of range [0, {})", row, self.row_count));
        }
        let col_idx = self
            .schema
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
        self.mutation_count += 1;

        Ok(())
    }

    pub fn get_row(&self, row: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if row >= self.row_count {
            return Err(format!("Row {} out of range [0, {})", row, self.row_count));
        }

        let mut result = HashMap::with_capacity(self.columns.len());
        for (i, col) in self.columns.iter().enumerate() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            result.insert(col_name.to_string(), col.get(row)?);
        }

        Ok(result)
    }

    pub fn append_row(&mut self, row: HashMap<String, ColumnValue>) -> Result<(), String> {
        // Validate all columns are present and type-compatible before any mutation
        for (i, col) in self.columns.iter().enumerate() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            match row.get(col_name) {
                None => return Err(format!("Missing value for column '{}'", col_name)),
                Some(value) => col.check_value_type(value)?,
            }
        }

        let insert_index = self.row_count;
        let snapshot_len = self.row_count;

        // Two-phase commit: if any column.append fails (e.g. interner mutex
        // poisoned from a prior panic), roll every column back to the
        // pre-append length so table invariants stay consistent.
        for (i, col) in self.columns.iter_mut().enumerate() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            let value = row.get(col_name).unwrap().clone();
            if let Err(e) = col.append(value) {
                for c in self.columns.iter_mut() {
                    c.truncate_to(snapshot_len);
                }
                return Err(format!("Column '{}': {}", col_name, e));
            }
        }

        self.row_count += 1;

        // Record the change
        self.changeset.push(TableChange::RowInserted {
            index: insert_index,
            data: row,
        });
        self.mutation_count += 1;

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
    pub fn append_rows(
        &mut self,
        rows: Vec<HashMap<String, ColumnValue>>,
    ) -> Result<usize, String> {
        if rows.is_empty() {
            return Ok(0);
        }

        let col_names: Vec<&str> = self.schema.get_column_names();

        // Validate all rows first (presence + types) before inserting any
        for (row_idx, row) in rows.iter().enumerate() {
            for (i, col) in self.columns.iter().enumerate() {
                let col_name = col_names[i];
                match row.get(col_name) {
                    None => {
                        return Err(format!(
                            "Row {}: Missing value for column '{}'",
                            row_idx, col_name
                        ))
                    }
                    Some(value) => col
                        .check_value_type(value)
                        .map_err(|e| format!("Row {}: {}", row_idx, e))?,
                }
            }
        }

        let start_index = self.row_count;
        let num_rows = rows.len();
        let snapshot_len = self.row_count;
        let snapshot_changeset_len = self.changeset.total_len();

        // Insert all rows. On any per-column append failure, truncate every
        // column back to the pre-batch length so no partial rows remain.
        for (row_offset, row) in rows.into_iter().enumerate() {
            let insert_index = start_index + row_offset;

            for (i, col) in self.columns.iter_mut().enumerate() {
                let col_name = col_names[i];
                let value = row.get(col_name).unwrap().clone();
                if let Err(e) = col.append(value) {
                    for c in self.columns.iter_mut() {
                        c.truncate_to(snapshot_len);
                    }
                    self.row_count = snapshot_len;
                    self.changeset.truncate_to(snapshot_changeset_len);
                    return Err(format!("Row {} column '{}': {}", row_offset, col_name, e));
                }
            }

            self.row_count += 1;

            // Record the change
            self.changeset.push(TableChange::RowInserted {
                index: insert_index,
                data: row,
            });
            self.mutation_count += 1;
        }

        Ok(num_rows)
    }

    pub fn insert_row(
        &mut self,
        index: usize,
        row: HashMap<String, ColumnValue>,
    ) -> Result<(), String> {
        if index > self.row_count {
            return Err(format!(
                "Index {} out of range [0, {}]",
                index, self.row_count
            ));
        }

        // Validate all columns are present and type-compatible before any mutation
        for (i, col) in self.columns.iter().enumerate() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            match row.get(col_name) {
                None => return Err(format!("Missing value for column '{}'", col_name)),
                Some(value) => col.check_value_type(value)?,
            }
        }

        // Two-phase commit with rollback on any col.insert failure.
        for i in 0..self.columns.len() {
            let col_name = self.schema.get_column_info(i).unwrap().0;
            let value = row.get(col_name).unwrap().clone();
            if let Err(e) = self.columns[i].insert(index, value) {
                // Roll back every column that already inserted.
                for j in 0..i {
                    let _ = self.columns[j].delete(index);
                }
                return Err(format!("Column '{}': {}", col_name, e));
            }
        }

        self.row_count += 1;

        // Record the change
        self.changeset
            .push(TableChange::RowInserted { index, data: row });
        self.mutation_count += 1;

        Ok(())
    }

    pub fn delete_row(&mut self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if index >= self.row_count {
            return Err(format!(
                "Row {} out of range [0, {})",
                index, self.row_count
            ));
        }

        // Two-phase commit: if any column's delete fails (e.g. a desynced
        // length or poisoned interner mutex), re-insert the already-deleted
        // values so no column is left one row short.
        let mut deleted: Vec<ColumnValue> = Vec::with_capacity(self.columns.len());
        for i in 0..self.columns.len() {
            match self.columns[i].delete(index) {
                Ok(value) => deleted.push(value),
                Err(e) => {
                    for (j, value) in deleted.into_iter().enumerate() {
                        let _ = self.columns[j].insert(index, value);
                    }
                    let col_name = self.schema.get_column_info(i).unwrap().0;
                    return Err(format!("Column '{}': {}", col_name, e));
                }
            }
        }

        let result: HashMap<String, ColumnValue> = deleted
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                let col_name = self.schema.get_column_info(i).unwrap().0;
                (col_name.to_string(), value)
            })
            .collect();

        self.row_count -= 1;

        // Record the change
        self.changeset.push(TableChange::RowDeleted {
            index,
            data: result.clone(),
        });
        self.mutation_count += 1;

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
        let col_idx = self
            .schema
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
        let col_idx = self
            .schema
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
        let col_idx = self
            .schema
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
        let col_idx = self
            .schema
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
        let col_idx = self
            .schema
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
            let values: Vec<String> = column_names
                .iter()
                .map(|col| {
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
                })
                .collect();
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
        let rows: Vec<serde_json::Value> = self
            .iter_rows()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = column_names
                    .iter()
                    .map(|col| {
                        let json_val = match row.get(*col) {
                            Some(ColumnValue::Int32(n)) => serde_json::Value::Number((*n).into()),
                            Some(ColumnValue::Int64(n)) => serde_json::Value::Number((*n).into()),
                            Some(ColumnValue::Float32(f)) => {
                                serde_json::Number::from_f64(*f as f64)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            }
                            Some(ColumnValue::Float64(f)) => serde_json::Number::from_f64(*f)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null),
                            Some(ColumnValue::String(s)) => serde_json::Value::String(s.clone()),
                            Some(ColumnValue::Bool(b)) => serde_json::Value::Bool(*b),
                            Some(ColumnValue::Date(days)) => {
                                serde_json::Value::String(format_date(*days))
                            }
                            Some(ColumnValue::DateTime(ms)) => {
                                serde_json::Value::String(format_datetime(*ms))
                            }
                            Some(ColumnValue::Null) | None => serde_json::Value::Null,
                        };
                        (col.to_string(), json_val)
                    })
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();

        serde_json::to_string_pretty(&rows).map_err(|e| format!("JSON serialization error: {}", e))
    }

    /// Create a table from a CSV string.
    ///
    /// The first line is treated as the header row containing column names.
    /// Column types are inferred by scanning every data row and unifying:
    /// - Numbers that fit in i32 → INT32, widened to INT64 (or FLOAT64 when
    ///   any value has a decimal point) as later rows require
    /// - "true"/"false" (case-insensitive) → BOOL
    /// - Dates → DATE, widened to DATETIME if any value carries a time
    /// - Empty values are skipped for inference (they import as NULL)
    /// - Anything else, or a column mixing incompatible types → STRING
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

        // Infer types from all rows (or default to STRING if no data)
        let types = if rows.is_empty() {
            // No data rows - default all columns to STRING
            vec![ColumnType::String; column_names.len()]
        } else {
            let first_len = rows.first().map(|r| r.len()).unwrap_or(0);
            if first_len != column_names.len() {
                return Err(format!(
                    "Column count mismatch: header has {}, but data row has {} values",
                    column_names.len(),
                    first_len
                ));
            }
            infer_types_from_csv_rows(&rows, column_names.len())
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
    /// Column names come from the first object; types are inferred by scanning
    /// every row and unifying what's found:
    /// - JSON integers → INT32, widened to INT64 (or FLOAT64 if any value has
    ///   a fractional part) when later rows require it
    /// - JSON strings → STRING, or DATE/DATETIME when *all* values in the
    ///   column parse as dates (a DATE column widens to DATETIME if any value
    ///   carries a time component)
    /// - JSON booleans → BOOL
    /// - JSON null → skipped for inference; a column of only nulls is STRING
    ///
    /// Columns whose non-null values mix incompatible types (e.g. a number
    /// and a string) are rejected with an error. All columns are created as
    /// nullable.
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
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(json).map_err(|e| format!("JSON parse error: {}", e))?;

        if parsed.is_empty() {
            return Err("JSON array is empty".to_string());
        }

        let schema = infer_schema_from_json(&parsed)?;
        let mut table = Table::new(name.to_string(), schema);

        // Populate rows, converting each value against the inferred schema
        for item in &parsed {
            let obj = item.as_object().ok_or("Expected object in array")?;
            let row_map = json_object_to_row_map(obj, table.schema())?;
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
                self.schema
                    .get_column_index(name)
                    .map(|idx| (name.clone(), idx))
            })
            .collect();

        let mut matching_indices = Vec::new();

        for row_idx in 0..self.row_count {
            // Use eval_expr_fast with a closure that directly accesses columns
            let matches = crate::expr::eval_expr_fast(&expr, &|col_name: &str| {
                column_indices.get(col_name).and_then(|&col_idx| {
                    self.columns
                        .get(col_idx)
                        .and_then(|col| col.get(row_idx).ok())
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
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
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
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}",
            year, month, day, hour, minute, second, millisecond
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        )
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

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
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

    let time_ms =
        (hour as i64) * 3_600_000 + (minute as i64) * 60_000 + (second as i64) * 1000 + (ms as i64);

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
/// Infer each column's type by unifying every non-empty value across all
/// rows. Empty values import as NULL and don't influence the type; a column
/// with no non-empty values defaults to STRING.
fn infer_types_from_csv_rows(rows: &[Vec<String>], column_count: usize) -> Vec<ColumnType> {
    (0..column_count)
        .map(|i| {
            let mut inferred: Option<ColumnType> = None;
            for row in rows {
                let value = row.get(i).map(|s| s.trim()).unwrap_or("");
                if value.is_empty() {
                    continue;
                }
                let natural = infer_type_from_csv_value(value);
                inferred = Some(match inferred {
                    None => natural,
                    Some(prev) => unify_csv_column_types(prev, natural),
                });
            }
            inferred.unwrap_or(ColumnType::String)
        })
        .collect()
}

/// Combine the types of two CSV values in the same column. Numeric and
/// temporal families widen; any other mix is STRING — every CSV value is a
/// string at heart, so that fallback is always lossless.
fn unify_csv_column_types(a: ColumnType, b: ColumnType) -> ColumnType {
    use ColumnType::*;
    if a == b {
        return a;
    }
    match (a, b) {
        (Int32, Int64) | (Int64, Int32) => Int64,
        (Int32 | Int64, Float64) | (Float64, Int32 | Int64) => Float64,
        (Date, DateTime) | (DateTime, Date) => DateTime,
        _ => String,
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
        && parse_datetime(trimmed).is_some()
    {
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
        ColumnType::Int32 => trimmed
            .parse::<i32>()
            .map(ColumnValue::Int32)
            .map_err(|_| format!("Cannot parse '{}' as INT32", trimmed)),
        ColumnType::Int64 => trimmed
            .parse::<i64>()
            .map(ColumnValue::Int64)
            .map_err(|_| format!("Cannot parse '{}' as INT64", trimmed)),
        ColumnType::Float32 => trimmed
            .parse::<f32>()
            .map(ColumnValue::Float32)
            .map_err(|_| format!("Cannot parse '{}' as FLOAT32", trimmed)),
        ColumnType::Float64 => trimmed
            .parse::<f64>()
            .map(ColumnValue::Float64)
            .map_err(|_| format!("Cannot parse '{}' as FLOAT64", trimmed)),
        ColumnType::Bool => {
            if trimmed.eq_ignore_ascii_case("true") {
                Ok(ColumnValue::Bool(true))
            } else if trimmed.eq_ignore_ascii_case("false") {
                Ok(ColumnValue::Bool(false))
            } else {
                Err(format!("Cannot parse '{}' as BOOL", trimmed))
            }
        }
        ColumnType::String => Ok(ColumnValue::String(trimmed.to_string())),
        ColumnType::Date => parse_date(trimmed)
            .map(ColumnValue::Date)
            .ok_or_else(|| format!("Cannot parse '{}' as DATE (expected YYYY-MM-DD)", trimmed)),
        ColumnType::DateTime => parse_datetime(trimmed)
            // A bare date in a DATETIME column lands at midnight
            .or_else(|| parse_date(trimmed).map(|days| days as i64 * 86_400_000))
            .map(ColumnValue::DateTime)
            .ok_or_else(|| {
                format!(
                    "Cannot parse '{}' as DATETIME (expected YYYY-MM-DDTHH:MM:SS)",
                    trimmed
                )
            }),
    }
}

/// The type a single non-null JSON value would have on its own.
fn json_value_natural_type(key: &str, value: &serde_json::Value) -> Result<ColumnType, String> {
    match value {
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                let val = n.as_i64().unwrap();
                if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                    Ok(ColumnType::Int32)
                } else {
                    Ok(ColumnType::Int64)
                }
            } else {
                Ok(ColumnType::Float64)
            }
        }
        serde_json::Value::String(s) => {
            // Try to detect date/datetime strings
            if (s.contains('T') || (s.contains(' ') && s.contains(':')))
                && parse_datetime(s).is_some()
            {
                Ok(ColumnType::DateTime)
            } else if s.len() == 10 && s.chars().nth(4) == Some('-') && parse_date(s).is_some() {
                Ok(ColumnType::Date)
            } else {
                Ok(ColumnType::String)
            }
        }
        serde_json::Value::Bool(_) => Ok(ColumnType::Bool),
        _ => Err(format!("Unsupported JSON value type for column '{}'", key)),
    }
}

/// Combine the types of two values seen in the same column, widening within
/// the numeric (INT32 → INT64 → FLOAT64) and temporal (DATE → DATETIME)
/// families. A date-looking string mixed with a plain string is just STRING.
fn unify_json_column_types(key: &str, a: ColumnType, b: ColumnType) -> Result<ColumnType, String> {
    use ColumnType::*;
    if a == b {
        return Ok(a);
    }
    match (a, b) {
        (Int32, Int64) | (Int64, Int32) => Ok(Int64),
        (Int32 | Int64, Float64) | (Float64, Int32 | Int64) => Ok(Float64),
        (Date, DateTime) | (DateTime, Date) => Ok(DateTime),
        (Date | DateTime, String) | (String, Date | DateTime) => Ok(String),
        _ => Err(format!(
            "Column '{}' has mixed incompatible types: {:?} and {:?}",
            key, a, b
        )),
    }
}

/// Infer a schema from all rows of a JSON array: column names come from the
/// first object, each column's type from unifying every non-null value.
fn infer_schema_from_json(rows: &[serde_json::Value]) -> Result<Schema, String> {
    let first = rows[0].as_object().ok_or("Expected array of objects")?;
    let mut columns = Vec::new();

    for key in first.keys() {
        let mut inferred: Option<ColumnType> = None;
        for item in rows {
            let obj = item.as_object().ok_or("Expected array of objects")?;
            match obj.get(key) {
                None | Some(serde_json::Value::Null) => continue,
                Some(value) => {
                    let natural = json_value_natural_type(key, value)?;
                    inferred = Some(match inferred {
                        None => natural,
                        Some(prev) => unify_json_column_types(key, prev, natural)?,
                    });
                }
            }
        }
        // A column with no non-null values defaults to String
        columns.push((key.clone(), inferred.unwrap_or(ColumnType::String), true));
    }

    Ok(Schema::new(columns))
}

/// Convert a single JSON value to the ColumnValue the schema expects.
fn json_value_to_column_value(
    key: &str,
    value: &serde_json::Value,
    col_type: ColumnType,
) -> Result<ColumnValue, String> {
    let mismatch = |value: &serde_json::Value| format!("Column '{}': cannot store {} ", key, value);

    match value {
        serde_json::Value::Null => Ok(ColumnValue::Null),
        serde_json::Value::Number(n) => match col_type {
            ColumnType::Int32 => n
                .as_i64()
                .filter(|v| *v >= i32::MIN as i64 && *v <= i32::MAX as i64)
                .map(|v| ColumnValue::Int32(v as i32))
                .ok_or_else(|| mismatch(value)),
            ColumnType::Int64 => n
                .as_i64()
                .map(ColumnValue::Int64)
                .ok_or_else(|| mismatch(value)),
            ColumnType::Float64 => n
                .as_f64()
                .map(ColumnValue::Float64)
                .ok_or_else(|| mismatch(value)),
            _ => Err(mismatch(value)),
        },
        serde_json::Value::String(s) => match col_type {
            ColumnType::String => Ok(ColumnValue::String(s.clone())),
            ColumnType::Date => parse_date(s)
                .map(ColumnValue::Date)
                .ok_or_else(|| format!("Column '{}': invalid date '{}'", key, s)),
            ColumnType::DateTime => parse_datetime(s)
                // A bare date in a DATETIME column lands at midnight
                .or_else(|| parse_date(s).map(|days| days as i64 * 86_400_000))
                .map(ColumnValue::DateTime)
                .ok_or_else(|| format!("Column '{}': invalid datetime '{}'", key, s)),
            _ => Err(mismatch(value)),
        },
        serde_json::Value::Bool(b) => match col_type {
            ColumnType::Bool => Ok(ColumnValue::Bool(*b)),
            _ => Err(mismatch(value)),
        },
        _ => Err(format!("Unsupported JSON value type for key '{}'", key)),
    }
}

/// Convert a JSON object to a row map, coercing each value to its column's
/// inferred type. Keys absent from the schema are ignored.
fn json_object_to_row_map(
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &Schema,
) -> Result<HashMap<String, ColumnValue>, String> {
    let mut row = HashMap::new();

    for (key, value) in obj {
        let Some(col_type) = schema.get_column_type(key) else {
            continue;
        };
        row.insert(
            key.clone(),
            json_value_to_column_value(key, value, col_type)?,
        );
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
        let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);

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
        row1.insert(
            "value".to_string(),
            ColumnValue::String("first".to_string()),
        );
        table.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert(
            "value".to_string(),
            ColumnValue::String("second".to_string()),
        );
        table.append_row(row2).unwrap();

        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), ColumnValue::Int32(3));
        row3.insert(
            "value".to_string(),
            ColumnValue::String("third".to_string()),
        );
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
        table
            .set_value(0, "value", ColumnValue::Int32(200))
            .unwrap();

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
        let mut table = Table::with_hint_and_interning(
            "products".to_string(),
            schema,
            StorageHint::FastReads,
            true,
        );
        assert!(table.uses_string_interning());

        // Add rows with repeated category strings
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert(
            "category".to_string(),
            ColumnValue::String("Electronics".to_string()),
        );
        row1.insert("name".to_string(), ColumnValue::String("Phone".to_string()));
        table.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert(
            "category".to_string(),
            ColumnValue::String("Electronics".to_string()),
        ); // Duplicate
        row2.insert(
            "name".to_string(),
            ColumnValue::String("Laptop".to_string()),
        );
        table.append_row(row2).unwrap();

        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), ColumnValue::Int32(3));
        row3.insert(
            "category".to_string(),
            ColumnValue::String("Clothing".to_string()),
        );
        row3.insert("name".to_string(), ColumnValue::String("Shirt".to_string()));
        table.append_row(row3).unwrap();

        let mut row4 = HashMap::new();
        row4.insert("id".to_string(), ColumnValue::Int32(4));
        row4.insert(
            "category".to_string(),
            ColumnValue::String("Electronics".to_string()),
        ); // Another duplicate
        row4.insert(
            "name".to_string(),
            ColumnValue::String("Tablet".to_string()),
        );
        table.append_row(row4).unwrap();

        // Verify data is correct
        assert_eq!(table.len(), 4);
        assert_eq!(
            table.get_value(0, "category").unwrap().as_string(),
            Some("Electronics")
        );
        assert_eq!(
            table.get_value(1, "category").unwrap().as_string(),
            Some("Electronics")
        );
        assert_eq!(
            table.get_value(2, "category").unwrap().as_string(),
            Some("Clothing")
        );
        assert_eq!(
            table.get_value(3, "category").unwrap().as_string(),
            Some("Electronics")
        );

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

        let mut table = Table::with_hint_and_interning(
            "orders".to_string(),
            schema,
            StorageHint::FastReads,
            true,
        );

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert(
            "status".to_string(),
            ColumnValue::String("pending".to_string()),
        );
        table.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert(
            "status".to_string(),
            ColumnValue::String("pending".to_string()),
        );
        table.append_row(row2).unwrap();

        // Update first order status
        table
            .set_value(0, "status", ColumnValue::String("completed".to_string()))
            .unwrap();

        assert_eq!(
            table.get_value(0, "status").unwrap().as_string(),
            Some("completed")
        );
        assert_eq!(
            table.get_value(1, "status").unwrap().as_string(),
            Some("pending")
        );

        // Check stats after update
        let stats = table.interner_stats().unwrap();
        assert_eq!(stats.unique_strings, 2); // "pending" and "completed"
    }

    #[test]
    fn test_table_without_string_interning() {
        let schema = Schema::new(vec![("name".to_string(), ColumnType::String, false)]);

        // Create table without string interning (default)
        let table = Table::new("simple".to_string(), schema);
        assert!(!table.uses_string_interning());
        assert!(table.interner_stats().is_none());
    }

    /// Helper: assert that every column's length equals row_count.
    /// This is the core invariant that partial-mutation bugs violate.
    fn assert_table_consistent(table: &Table) {
        let rc = table.len();
        for (i, col) in table.columns.iter().enumerate() {
            assert_eq!(
                col.len(),
                rc,
                "column {} has length {} but table.row_count is {}",
                i,
                col.len(),
                rc
            );
        }
    }

    #[test]
    fn test_append_row_failure_preserves_column_length_invariant() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
            ("score".to_string(), ColumnType::Float64, false),
        ]);
        let mut table = Table::new("t".to_string(), schema);

        let mut good = HashMap::new();
        good.insert("id".to_string(), ColumnValue::Int32(1));
        good.insert("name".to_string(), ColumnValue::String("a".to_string()));
        good.insert("score".to_string(), ColumnValue::Float64(1.5));
        table.append_row(good).unwrap();
        assert_table_consistent(&table);

        // Type mismatch on a later column: must not partially mutate earlier columns.
        let mut bad = HashMap::new();
        bad.insert("id".to_string(), ColumnValue::Int32(2));
        bad.insert("name".to_string(), ColumnValue::String("b".to_string()));
        bad.insert(
            "score".to_string(),
            ColumnValue::String("not-a-float".to_string()),
        );
        assert!(table.append_row(bad).is_err());
        assert_table_consistent(&table);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_append_rows_failure_rolls_back_all_partial_rows() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let mut table = Table::new("t".to_string(), schema);

        let r = |id: i32, name: &str| {
            let mut h = HashMap::new();
            h.insert("id".to_string(), ColumnValue::Int32(id));
            h.insert("name".to_string(), ColumnValue::String(name.to_string()));
            h
        };
        table.append_row(r(1, "a")).unwrap();
        let start_len = table.len();

        // First two rows valid, third has type mismatch.
        let mut bad_third = HashMap::new();
        bad_third.insert("id".to_string(), ColumnValue::Int32(4));
        bad_third.insert("name".to_string(), ColumnValue::Int32(99)); // wrong type

        let rows = vec![r(2, "b"), r(3, "c"), bad_third];
        assert!(table.append_rows(rows).is_err());
        assert_table_consistent(&table);
        assert_eq!(
            table.len(),
            start_len,
            "no rows should have been added on failure"
        );
    }

    #[test]
    fn test_version_increments_monotonically_on_mutation() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);
        let mut table = Table::new("t".to_string(), schema);

        let v0 = table.version();

        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("a".to_string()));
        table.append_row(row).unwrap();
        let v1 = table.version();
        assert!(v1 > v0, "append_row must increment version");

        table
            .set_value(0, "name", ColumnValue::String("b".to_string()))
            .unwrap();
        let v2 = table.version();
        assert!(v2 > v1, "set_value must increment version");

        table.delete_row(0).unwrap();
        let v3 = table.version();
        assert!(v3 > v2, "delete_row must increment version");

        // Reads must not change version
        let v4 = table.version();
        let _ = table.len();
        assert_eq!(v4, table.version(), "reads must not change version");

        // clear_changeset must not decrement version (monotonic)
        table.clear_changeset();
        assert!(
            table.version() >= v3,
            "clear_changeset must not rewind version"
        );
    }

    #[test]
    fn test_from_json_null_in_first_row_infers_from_later_rows() {
        let json = r#"[
            {"id": null, "score": null, "name": null},
            {"id": 7, "score": 1.5, "name": "Alice"}
        ]"#;
        let table = Table::from_json("test", json).unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(
            table.schema().get_column_type("id"),
            Some(ColumnType::Int32)
        );
        assert_eq!(
            table.schema().get_column_type("score"),
            Some(ColumnType::Float64)
        );
        assert_eq!(
            table.schema().get_column_type("name"),
            Some(ColumnType::String)
        );
        assert!(table.get_value(0, "id").unwrap().is_null());
        assert_eq!(table.get_value(1, "id").unwrap().as_i32(), Some(7));
        assert_eq!(table.get_value(1, "score").unwrap().as_f64(), Some(1.5));
    }

    #[test]
    fn test_from_json_all_null_column_defaults_to_string() {
        let json = r#"[{"x": null}, {"x": null}]"#;
        let table = Table::from_json("test", json).unwrap();

        assert_eq!(
            table.schema().get_column_type("x"),
            Some(ColumnType::String)
        );
        assert!(table.get_value(0, "x").unwrap().is_null());
        assert!(table.get_value(1, "x").unwrap().is_null());
    }

    #[test]
    fn test_from_json_widens_int32_to_int64() {
        let json = r#"[{"id": 1}, {"id": 5000000000}]"#;
        let table = Table::from_json("test", json).unwrap();

        assert_eq!(
            table.schema().get_column_type("id"),
            Some(ColumnType::Int64)
        );
        assert_eq!(table.get_value(0, "id").unwrap().as_i64(), Some(1));
        assert_eq!(
            table.get_value(1, "id").unwrap().as_i64(),
            Some(5_000_000_000)
        );
    }

    #[test]
    fn test_from_json_widens_int_to_float() {
        let json = r#"[{"x": 1}, {"x": 2.5}]"#;
        let table = Table::from_json("test", json).unwrap();

        assert_eq!(
            table.schema().get_column_type("x"),
            Some(ColumnType::Float64)
        );
        assert_eq!(table.get_value(0, "x").unwrap().as_f64(), Some(1.0));
        assert_eq!(table.get_value(1, "x").unwrap().as_f64(), Some(2.5));
    }

    #[test]
    fn test_from_json_date_like_string_in_string_column() {
        // First non-null is a plain string, so the column is STRING; a later
        // date-looking value must stay a string, not become a Date value.
        let json = r#"[{"note": "hello"}, {"note": "2024-01-15"}]"#;
        let table = Table::from_json("test", json).unwrap();

        assert_eq!(
            table.schema().get_column_type("note"),
            Some(ColumnType::String)
        );
        assert_eq!(
            table.get_value(1, "note").unwrap().as_string(),
            Some("2024-01-15")
        );
    }

    #[test]
    fn test_delete_row_rolls_back_on_partial_failure() {
        let schema = Schema::new(vec![
            ("a".to_string(), ColumnType::Int32, false),
            ("b".to_string(), ColumnType::String, false),
        ]);
        let mut table = Table::new("test".to_string(), schema);
        for (a, b) in [(1, "x"), (2, "y")] {
            let mut row = HashMap::new();
            row.insert("a".to_string(), ColumnValue::Int32(a));
            row.insert("b".to_string(), ColumnValue::String(b.to_string()));
            table.append_row(row).unwrap();
        }

        // Inject a fault: shorten column "b" so its delete(1) fails after
        // column "a" has already deleted successfully.
        table.columns[1].truncate_to(1);

        assert!(table.delete_row(1).is_err());

        // Column "a" must be rolled back, not left one row short.
        assert_eq!(table.columns[0].len(), 2);
        assert_eq!(table.get_value(1, "a").unwrap().as_i32(), Some(2));
        assert_eq!(table.len(), 2, "row_count must be unchanged on failure");
    }

    #[test]
    fn test_from_csv_empty_in_first_row_infers_from_later_rows() {
        // Fully empty rows are skipped entirely, so use a partially empty one
        let csv = "id,score\n,9.5\n7,1.5";
        let table = Table::from_csv("test", csv).unwrap();

        assert_eq!(
            table.schema().get_column_type("id"),
            Some(ColumnType::Int32)
        );
        assert_eq!(
            table.schema().get_column_type("score"),
            Some(ColumnType::Float64)
        );
        assert!(table.get_value(0, "id").unwrap().is_null());
        assert_eq!(table.get_value(1, "id").unwrap().as_i32(), Some(7));
    }

    #[test]
    fn test_from_csv_widens_int32_to_int64() {
        let csv = "id\n1\n5000000000";
        let table = Table::from_csv("test", csv).unwrap();

        assert_eq!(
            table.schema().get_column_type("id"),
            Some(ColumnType::Int64)
        );
        assert_eq!(
            table.get_value(1, "id").unwrap().as_i64(),
            Some(5_000_000_000)
        );
    }

    #[test]
    fn test_from_csv_widens_int_to_float() {
        let csv = "x\n1\n2.5";
        let table = Table::from_csv("test", csv).unwrap();

        assert_eq!(
            table.schema().get_column_type("x"),
            Some(ColumnType::Float64)
        );
        assert_eq!(table.get_value(0, "x").unwrap().as_f64(), Some(1.0));
        assert_eq!(table.get_value(1, "x").unwrap().as_f64(), Some(2.5));
    }

    #[test]
    fn test_from_csv_mixed_types_fall_back_to_string() {
        // CSV values are all strings at heart, so a number/text mix is STRING
        let csv = "x\n1\nhello";
        let table = Table::from_csv("test", csv).unwrap();

        assert_eq!(
            table.schema().get_column_type("x"),
            Some(ColumnType::String)
        );
        assert_eq!(table.get_value(0, "x").unwrap().as_string(), Some("1"));
        assert_eq!(table.get_value(1, "x").unwrap().as_string(), Some("hello"));
    }

    #[test]
    fn test_from_csv_widens_date_to_datetime() {
        let csv = "ts\n2024-01-15\n2024-01-15T10:30:00";
        let table = Table::from_csv("test", csv).unwrap();

        assert_eq!(
            table.schema().get_column_type("ts"),
            Some(ColumnType::DateTime)
        );
        let day_ms = 86_400_000i64;
        let midnight = table.get_value(0, "ts").unwrap().as_datetime().unwrap();
        assert_eq!(midnight % day_ms, 0);
    }

    #[test]
    fn test_from_json_widens_date_to_datetime() {
        let json = r#"[{"ts": "2024-01-15"}, {"ts": "2024-01-15T10:30:00"}]"#;
        let table = Table::from_json("test", json).unwrap();

        assert_eq!(
            table.schema().get_column_type("ts"),
            Some(ColumnType::DateTime)
        );
        // The bare date lands at midnight of that day.
        let day_ms = 86_400_000i64;
        let midnight = table.get_value(0, "ts").unwrap().as_datetime().unwrap();
        assert_eq!(midnight % day_ms, 0);
    }
}
