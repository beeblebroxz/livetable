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
    pub fn new(name: String, schema: Schema) -> Self {
        Self::new_with_options(name, schema, false)
    }

    pub fn new_with_options(name: String, schema: Schema, use_tiered_vector: bool) -> Self {
        Self::new_with_interning(name, schema, use_tiered_vector, false)
    }

    /// Create a new table with optional string interning
    ///
    /// When `use_string_interning` is true, all String columns will share a
    /// common string interner, reducing memory usage for repeated strings.
    ///
    /// # Arguments
    ///
    /// * `name` - Table name
    /// * `schema` - Table schema
    /// * `use_tiered_vector` - Use TieredVector storage (faster inserts/deletes)
    /// * `use_string_interning` - Enable string interning for memory efficiency
    pub fn new_with_interning(
        name: String,
        schema: Schema,
        use_tiered_vector: bool,
        use_string_interning: bool,
    ) -> Self {
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

    /// Returns true if there are pending changes
    pub fn has_pending_changes(&self) -> bool {
        !self.changeset.is_empty()
    }
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
