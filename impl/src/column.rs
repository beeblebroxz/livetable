/// LiveTable Column Implementation
///
/// A Column is an array-like random-access data container indexed by integer.
/// Each Column has a type specifying the type of every value stored.
///
/// # String Interning
///
/// String columns can optionally use a shared `StringInterner` to deduplicate
/// strings. When an interner is provided, strings are stored as integer IDs
/// internally, significantly reducing memory for columns with repeated values.

use crate::sequence::{ArraySequence, Sequence, TieredVectorSequence};
use crate::interner::{StringInterner, StringId};
use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

/// Column data types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bool,
}

/// Column value enum to support multiple types
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bool(bool),
    Null,
}

impl ColumnValue {
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            ColumnValue::Int32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ColumnValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            ColumnValue::Float32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ColumnValue::Float64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            ColumnValue::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ColumnValue::Bool(v) => Some(*v),
            _ => None,
        }
    }
}

/// Base column class that wraps a Sequence.
/// Handles type checking and nullable values.
///
/// For string columns with an interner, strings are stored as integer IDs
/// in a separate `string_ids` sequence, while the main `sequence` stores
/// placeholder values.
pub struct Column {
    name: String,
    column_type: ColumnType,
    nullable: bool,
    sequence: Box<dyn Sequence<ColumnValue>>,
    null_flags: Option<Box<dyn Sequence<bool>>>,
    /// Optional string interner for String columns (shared across table)
    interner: Option<Rc<RefCell<StringInterner>>>,
    /// String IDs storage (used only when interner is Some and column_type is String)
    string_ids: Option<Box<dyn Sequence<StringId>>>,
}

impl Column {
    pub fn new(name: String, column_type: ColumnType, nullable: bool) -> Self {
        Self::new_with_options(name, column_type, nullable, false)
    }

    pub fn new_with_options(
        name: String,
        column_type: ColumnType,
        nullable: bool,
        use_tiered_vector: bool,
    ) -> Self {
        Self::new_with_interner(name, column_type, nullable, use_tiered_vector, None)
    }

    /// Create a new column with an optional string interner
    ///
    /// If an interner is provided and the column type is String, strings will
    /// be deduplicated using the interner, reducing memory usage for columns
    /// with repeated string values.
    pub fn new_with_interner(
        name: String,
        column_type: ColumnType,
        nullable: bool,
        use_tiered_vector: bool,
        interner: Option<Rc<RefCell<StringInterner>>>,
    ) -> Self {
        let sequence: Box<dyn Sequence<ColumnValue>> = if use_tiered_vector {
            Box::new(TieredVectorSequence::new())
        } else {
            Box::new(ArraySequence::new())
        };

        let null_flags: Option<Box<dyn Sequence<bool>>> = if nullable {
            if use_tiered_vector {
                Some(Box::new(TieredVectorSequence::new()))
            } else {
                Some(Box::new(ArraySequence::new()))
            }
        } else {
            None
        };

        // Create string_ids storage only for String columns with an interner
        let string_ids: Option<Box<dyn Sequence<StringId>>> =
            if interner.is_some() && column_type == ColumnType::String {
                if use_tiered_vector {
                    Some(Box::new(TieredVectorSequence::new()))
                } else {
                    Some(Box::new(ArraySequence::new()))
                }
            } else {
                None
            };

        Column {
            name,
            column_type,
            nullable,
            sequence,
            null_flags,
            interner,
            string_ids,
        }
    }

    /// Returns a reference to the interner if this column uses one
    pub fn interner(&self) -> Option<&Rc<RefCell<StringInterner>>> {
        self.interner.as_ref()
    }

    /// Returns true if this column uses string interning
    pub fn uses_interning(&self) -> bool {
        self.interner.is_some() && self.column_type == ColumnType::String
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn len(&self) -> usize {
        self.sequence.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sequence.is_empty()
    }

    /// Validate and convert value to appropriate type
    fn validate_value(&self, value: ColumnValue) -> Result<ColumnValue, String> {
        if value.is_null() {
            if !self.nullable {
                return Err(format!("Column '{}' is not nullable", self.name));
            }
            return Ok(ColumnValue::Null);
        }

        // Type validation
        match (&value, self.column_type) {
            (ColumnValue::Int32(_), ColumnType::Int32) => Ok(value),
            (ColumnValue::Int64(_), ColumnType::Int64) => Ok(value),
            (ColumnValue::Float32(_), ColumnType::Float32) => Ok(value),
            (ColumnValue::Float64(_), ColumnType::Float64) => Ok(value),
            (ColumnValue::String(_), ColumnType::String) => Ok(value),
            (ColumnValue::Bool(_), ColumnType::Bool) => Ok(value),
            _ => Err(format!(
                "Type mismatch: expected {:?}, got {:?}",
                self.column_type, value
            )),
        }
    }

    pub fn get(&self, index: usize) -> Result<ColumnValue, String> {
        if self.nullable {
            if let Some(ref null_flags) = self.null_flags {
                if null_flags.get(index)? {
                    return Ok(ColumnValue::Null);
                }
            }
        }

        // If using string interning, resolve the ID to the actual string
        if let Some(ref string_ids) = self.string_ids {
            if let Some(ref interner) = self.interner {
                let id = string_ids.get(index)?;
                let interner = interner.borrow();
                if let Some(s) = interner.resolve_unchecked(id) {
                    return Ok(ColumnValue::String(s.to_string()));
                } else {
                    return Err(format!("Invalid string ID {} at index {}", id, index));
                }
            }
        }

        self.sequence.get(index)
    }

    /// Fast numeric access - returns the value as f64 without cloning ColumnValue.
    /// Returns None if the value is null, not a numeric type, or index out of bounds.
    /// This is optimized for aggregation operations.
    #[inline]
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        // Check null flag first (fast path)
        if self.nullable {
            if let Some(ref null_flags) = self.null_flags {
                if null_flags.get_ref(index).copied() == Some(true) {
                    return None;
                }
            }
        }

        // Get reference to the value without cloning
        self.sequence.get_ref(index).and_then(|v| match v {
            ColumnValue::Int32(n) => Some(*n as f64),
            ColumnValue::Int64(n) => Some(*n as f64),
            ColumnValue::Float32(f) => Some(*f as f64),
            ColumnValue::Float64(f) => Some(*f),
            _ => None, // Not numeric (String, Bool, Null)
        })
    }

    /// Check if a value at index is null (fast path without cloning).
    #[inline]
    pub fn is_null_at(&self, index: usize) -> bool {
        if !self.nullable {
            return false;
        }
        if let Some(ref null_flags) = self.null_flags {
            null_flags.get_ref(index).copied() == Some(true)
        } else {
            false
        }
    }

    pub fn set(&mut self, index: usize, value: ColumnValue) -> Result<(), String> {
        let value = self.validate_value(value)?;

        if value.is_null() {
            if let Some(ref mut null_flags) = self.null_flags {
                null_flags.set(index, true)?;
            }
            // Release old string ID if using interning
            if let Some(ref mut string_ids) = self.string_ids {
                if let Some(ref interner) = self.interner {
                    let old_id = string_ids.get(index)?;
                    interner.borrow_mut().release(old_id);
                    string_ids.set(index, 0)?; // Placeholder ID for null
                }
            }
            self.sequence.set(index, self.get_default_value())?;
        } else {
            if let Some(ref mut null_flags) = self.null_flags {
                null_flags.set(index, false)?;
            }
            // Handle string interning
            if let (Some(ref mut string_ids), Some(ref interner)) = (&mut self.string_ids, &self.interner) {
                if let ColumnValue::String(ref s) = value {
                    // Release old string ID
                    let old_id = string_ids.get(index)?;
                    interner.borrow_mut().release(old_id);
                    // Intern new string
                    let new_id = interner.borrow_mut().intern(s);
                    string_ids.set(index, new_id)?;
                    // Store placeholder in sequence (not used for interned strings)
                    self.sequence.set(index, ColumnValue::String(String::new()))?;
                    return Ok(());
                }
            }
            self.sequence.set(index, value)?;
        }

        Ok(())
    }

    pub fn insert(&mut self, index: usize, value: ColumnValue) -> Result<(), String> {
        let value = self.validate_value(value)?;

        if value.is_null() {
            if let Some(ref mut null_flags) = self.null_flags {
                null_flags.insert(index, true)?;
            }
            // Insert placeholder ID for null
            if let Some(ref mut string_ids) = self.string_ids {
                string_ids.insert(index, 0)?;
            }
            self.sequence.insert(index, self.get_default_value())?;
        } else {
            if let Some(ref mut null_flags) = self.null_flags {
                null_flags.insert(index, false)?;
            }
            // Handle string interning
            if let (Some(ref mut string_ids), Some(ref interner)) = (&mut self.string_ids, &self.interner) {
                if let ColumnValue::String(ref s) = value {
                    let id = interner.borrow_mut().intern(s);
                    string_ids.insert(index, id)?;
                    self.sequence.insert(index, ColumnValue::String(String::new()))?;
                    return Ok(());
                }
            }
            self.sequence.insert(index, value)?;
        }

        Ok(())
    }

    pub fn delete(&mut self, index: usize) -> Result<ColumnValue, String> {
        let is_null = if let Some(ref mut null_flags) = self.null_flags {
            null_flags.delete(index)?
        } else {
            false
        };

        // Handle string interning - get the string before deleting the ID
        if let (Some(ref mut string_ids), Some(ref interner)) = (&mut self.string_ids, &self.interner) {
            let id = string_ids.delete(index)?;
            self.sequence.delete(index)?; // Delete placeholder

            if is_null {
                return Ok(ColumnValue::Null);
            }

            // Get the string value before releasing
            let result = {
                let interner_ref = interner.borrow();
                interner_ref.resolve_unchecked(id)
                    .map(|s| ColumnValue::String(s.to_string()))
            };

            // Release the reference
            interner.borrow_mut().release(id);

            return result.ok_or_else(|| format!("Invalid string ID {} at index {}", id, index));
        }

        let value = self.sequence.delete(index)?;

        if is_null {
            Ok(ColumnValue::Null)
        } else {
            Ok(value)
        }
    }

    pub fn append(&mut self, value: ColumnValue) {
        let value = self.validate_value(value).expect("Invalid value");

        if value.is_null() {
            if let Some(ref mut null_flags) = self.null_flags {
                null_flags.append(true);
            }
            // Append placeholder ID for null
            if let Some(ref mut string_ids) = self.string_ids {
                string_ids.append(0);
            }
            self.sequence.append(self.get_default_value());
        } else {
            if let Some(ref mut null_flags) = self.null_flags {
                null_flags.append(false);
            }
            // Handle string interning
            if let (Some(ref mut string_ids), Some(ref interner)) = (&mut self.string_ids, &self.interner) {
                if let ColumnValue::String(ref s) = value {
                    let id = interner.borrow_mut().intern(s);
                    string_ids.append(id);
                    self.sequence.append(ColumnValue::String(String::new()));
                    return;
                }
            }
            self.sequence.append(value);
        }
    }

    pub fn is_null(&self, index: usize) -> Result<bool, String> {
        if !self.nullable {
            return Ok(false);
        }

        if let Some(ref null_flags) = self.null_flags {
            null_flags.get(index)
        } else {
            Ok(false)
        }
    }

    fn get_default_value(&self) -> ColumnValue {
        match self.column_type {
            ColumnType::Int32 => ColumnValue::Int32(0),
            ColumnType::Int64 => ColumnValue::Int64(0),
            ColumnType::Float32 => ColumnValue::Float32(0.0),
            ColumnType::Float64 => ColumnValue::Float64(0.0),
            ColumnType::String => ColumnValue::String(String::new()),
            ColumnType::Bool => ColumnValue::Bool(false),
        }
    }

    pub fn iter(&self) -> ColumnIterator<'_> {
        ColumnIterator {
            column: self,
            index: 0,
        }
    }
}

pub struct ColumnIterator<'a> {
    column: &'a Column,
    index: usize,
}

impl<'a> Iterator for ColumnIterator<'a> {
    type Item = ColumnValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.column.len() {
            None
        } else {
            let result = self.column.get(self.index).ok();
            self.index += 1;
            result
        }
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Column {{ name: '{}', type: {:?}, nullable: {}, len: {} }}",
            self.name,
            self.column_type,
            self.nullable,
            self.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_basic() {
        let mut col = Column::new("test".to_string(), ColumnType::Int32, false);
        col.append(ColumnValue::Int32(10));
        col.append(ColumnValue::Int32(20));
        col.append(ColumnValue::Int32(30));

        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0).unwrap().as_i32(), Some(10));
        assert_eq!(col.get(1).unwrap().as_i32(), Some(20));
        assert_eq!(col.get(2).unwrap().as_i32(), Some(30));
    }

    #[test]
    fn test_column_nullable() {
        let mut col = Column::new("test".to_string(), ColumnType::Int32, true);
        col.append(ColumnValue::Int32(10));
        col.append(ColumnValue::Null);
        col.append(ColumnValue::Int32(30));

        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0).unwrap().as_i32(), Some(10));
        assert!(col.get(1).unwrap().is_null());
        assert!(col.is_null(1).unwrap());
        assert_eq!(col.get(2).unwrap().as_i32(), Some(30));
    }

    #[test]
    fn test_column_set() {
        let mut col = Column::new("test".to_string(), ColumnType::Int32, false);
        col.append(ColumnValue::Int32(10));
        col.append(ColumnValue::Int32(20));

        col.set(1, ColumnValue::Int32(99)).unwrap();
        assert_eq!(col.get(1).unwrap().as_i32(), Some(99));
    }

    #[test]
    fn test_column_string_interning() {
        let interner = Rc::new(RefCell::new(StringInterner::new()));

        let mut col = Column::new_with_interner(
            "names".to_string(),
            ColumnType::String,
            false,
            false,
            Some(interner.clone()),
        );

        // Add some repeated strings
        col.append(ColumnValue::String("Alice".to_string()));
        col.append(ColumnValue::String("Bob".to_string()));
        col.append(ColumnValue::String("Alice".to_string())); // Duplicate
        col.append(ColumnValue::String("Charlie".to_string()));
        col.append(ColumnValue::String("Alice".to_string())); // Another duplicate

        // Verify we can read values back
        assert_eq!(col.get(0).unwrap().as_string(), Some("Alice"));
        assert_eq!(col.get(1).unwrap().as_string(), Some("Bob"));
        assert_eq!(col.get(2).unwrap().as_string(), Some("Alice"));
        assert_eq!(col.get(3).unwrap().as_string(), Some("Charlie"));
        assert_eq!(col.get(4).unwrap().as_string(), Some("Alice"));

        // Verify the interner only has 3 unique strings
        assert_eq!(interner.borrow().len(), 3);

        // Verify reference counts
        let interner_ref = interner.borrow();
        // Alice should have 3 references
        let alice_id = interner_ref.string_to_id.get("Alice").unwrap();
        assert_eq!(interner_ref.ref_count(*alice_id), 3);
    }

    #[test]
    fn test_column_string_interning_update() {
        let interner = Rc::new(RefCell::new(StringInterner::new()));

        let mut col = Column::new_with_interner(
            "names".to_string(),
            ColumnType::String,
            false,
            false,
            Some(interner.clone()),
        );

        col.append(ColumnValue::String("Alice".to_string()));
        col.append(ColumnValue::String("Alice".to_string()));

        // Update one "Alice" to "Bob"
        col.set(1, ColumnValue::String("Bob".to_string())).unwrap();

        assert_eq!(col.get(0).unwrap().as_string(), Some("Alice"));
        assert_eq!(col.get(1).unwrap().as_string(), Some("Bob"));

        // Verify reference counts changed
        let interner_ref = interner.borrow();
        assert_eq!(interner_ref.len(), 2); // Alice and Bob
    }

    #[test]
    fn test_column_string_interning_delete() {
        let interner = Rc::new(RefCell::new(StringInterner::new()));

        let mut col = Column::new_with_interner(
            "names".to_string(),
            ColumnType::String,
            false,
            false,
            Some(interner.clone()),
        );

        col.append(ColumnValue::String("Alice".to_string()));
        col.append(ColumnValue::String("Bob".to_string()));
        col.append(ColumnValue::String("Alice".to_string()));

        // Delete the only "Bob"
        let deleted = col.delete(1).unwrap();
        assert_eq!(deleted.as_string(), Some("Bob"));

        // Bob should be released
        assert_eq!(interner.borrow().len(), 1); // Only Alice remains
        assert_eq!(col.len(), 2);
        assert_eq!(col.get(0).unwrap().as_string(), Some("Alice"));
        assert_eq!(col.get(1).unwrap().as_string(), Some("Alice"));
    }
}
