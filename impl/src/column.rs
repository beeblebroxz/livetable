//! Native-width column buffers with packed NULL flags. ColumnValue remains the
//! public read/write representation, not the per-cell storage representation.
//! Interned strings store only IDs; both array and tiered backends are supported.
mod bitmap;
#[cfg(test)]
mod layout_tests;
mod storage;

use crate::interner::{StringId, StringInterner};
use crate::sequence::Sequence;
use bitmap::NullBitmap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use storage::ColumnData;

/// Sentinel value stored in interned ID buffers for NULL entries.
/// Must never collide with a valid interner ID (which grows from 0 upward).
const NULL_STRING_ID: StringId = u32::MAX;

/// Column data types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bool,
    /// Date stored as days since Unix epoch (1970-01-01)
    Date,
    /// DateTime stored as milliseconds since Unix epoch
    DateTime,
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
    /// Date as days since Unix epoch (1970-01-01). Positive = after, negative = before.
    Date(i32),
    /// DateTime as milliseconds since Unix epoch. Positive = after, negative = before.
    DateTime(i64),
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

    /// Get date as days since Unix epoch (1970-01-01)
    pub fn as_date(&self) -> Option<i32> {
        match self {
            ColumnValue::Date(v) => Some(*v),
            _ => None,
        }
    }

    /// Get datetime as milliseconds since Unix epoch
    pub fn as_datetime(&self) -> Option<i64> {
        match self {
            ColumnValue::DateTime(v) => Some(*v),
            _ => None,
        }
    }
}

/// A typed column backed by a native-width array or tiered sequence.
/// Handles type checking, packed NULL flags, and conversion to ColumnValue.
///
/// For string columns with an interner, strings are stored as integer IDs
/// instead of storing a parallel placeholder value buffer.
pub struct Column {
    name: String,
    column_type: ColumnType,
    nullable: bool,
    data: ColumnData,
    null_flags: Option<NullBitmap>,
    /// Optional string interner for String columns (shared across table)
    interner: Option<Arc<Mutex<StringInterner>>>,
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
        interner: Option<Arc<Mutex<StringInterner>>>,
    ) -> Self {
        let data = ColumnData::new(column_type, use_tiered_vector, interner.is_some());
        let null_flags = nullable.then(|| NullBitmap::new(use_tiered_vector));

        Column {
            name,
            column_type,
            nullable,
            data,
            null_flags,
            interner,
        }
    }

    /// Returns a reference to the interner if this column uses one
    pub fn interner(&self) -> Option<&Arc<Mutex<StringInterner>>> {
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
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if a value is type-compatible with this column (without consuming it).
    pub fn check_value_type(&self, value: &ColumnValue) -> Result<(), String> {
        if value.is_null() {
            if !self.nullable {
                return Err(format!("Column '{}' is not nullable", self.name));
            }
            return Ok(());
        }

        match (value, self.column_type) {
            (ColumnValue::Int32(_), ColumnType::Int32) => Ok(()),
            (ColumnValue::Int64(_), ColumnType::Int64) => Ok(()),
            (ColumnValue::Float32(_), ColumnType::Float32) => Ok(()),
            (ColumnValue::Float64(_), ColumnType::Float64) => Ok(()),
            (ColumnValue::String(_), ColumnType::String) => Ok(()),
            (ColumnValue::Bool(_), ColumnType::Bool) => Ok(()),
            (ColumnValue::Date(_), ColumnType::Date) => Ok(()),
            (ColumnValue::DateTime(_), ColumnType::DateTime) => Ok(()),
            _ => Err(format!(
                "Type mismatch for column '{}': expected {:?}, got {:?}",
                self.name, self.column_type, value
            )),
        }
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
            (ColumnValue::Date(_), ColumnType::Date) => Ok(value),
            (ColumnValue::DateTime(_), ColumnType::DateTime) => Ok(value),
            _ => Err(format!(
                "Type mismatch: expected {:?}, got {:?}",
                self.column_type, value
            )),
        }
    }

    fn check_index(&self, index: usize, inserting: bool) -> Result<(), String> {
        if index < self.len() || inserting && index == self.len() {
            Ok(())
        } else {
            Err(format!(
                "Index {} out of range [0, {}{}",
                index,
                self.len(),
                if inserting { "]" } else { ")" }
            ))
        }
    }

    pub fn get(&self, index: usize) -> Result<ColumnValue, String> {
        self.check_index(index, false)?;
        if self.is_null_at(index) {
            return Ok(ColumnValue::Null);
        }
        if let ColumnData::StringIds(ids) = &self.data {
            let id = ids.get(index)?;
            let interner = self
                .interner
                .as_ref()
                .expect("interned storage has an interner")
                .lock()
                .map_err(|_| "string interner mutex was poisoned by a prior panic".to_string())?;
            return interner
                .resolve_unchecked(id)
                .map(|s| ColumnValue::String(s.to_owned()))
                .ok_or_else(|| format!("Invalid string ID {} at index {}", id, index));
        }
        self.data.get(index)
    }

    /// Numeric access without constructing ColumnValue. Returns None for NULL,
    /// non-numeric types (including dates/bools), or an out-of-bounds index.
    #[inline]
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        if self.is_null_at(index) {
            None
        } else {
            self.data.get_f64(index)
        }
    }

    /// Check NULL without cloning. Out-of-bounds indices return false.
    #[inline]
    pub fn is_null_at(&self, index: usize) -> bool {
        self.null_flags.as_ref().and_then(|flags| flags.get(index)) == Some(true)
    }

    pub fn set(&mut self, index: usize, value: ColumnValue) -> Result<(), String> {
        let value = self.validate_value(value)?;
        self.check_index(index, false)?;
        let is_null = value.is_null();
        let value = if is_null {
            self.get_default_value()
        } else {
            value
        };

        if let ColumnData::StringIds(ids) = &mut self.data {
            // Obtain every fallible resource before changing storage or flags.
            let mut interner = self
                .interner
                .as_ref()
                .expect("interned storage has an interner")
                .lock()
                .map_err(|_| "string interner mutex was poisoned by a prior panic".to_string())?;
            let old_id = ids.get(index)?;
            let new_id = if is_null {
                NULL_STRING_ID
            } else {
                interner.intern(value.as_string().expect("validated string"))
            };
            ids.set(index, new_id).expect("column prevalidated index");
            // Intern first: assigning the same string cannot invalidate its ID.
            if old_id != NULL_STRING_ID {
                interner.release(old_id);
            }
        } else {
            self.data.set(index, value);
        }
        if let Some(flags) = &mut self.null_flags {
            flags.set(index, is_null);
        }
        Ok(())
    }

    pub fn insert(&mut self, index: usize, value: ColumnValue) -> Result<(), String> {
        let value = self.validate_value(value)?;
        self.check_index(index, true)?;
        let is_null = value.is_null();
        let value = if is_null {
            self.get_default_value()
        } else {
            value
        };

        if let ColumnData::StringIds(ids) = &mut self.data {
            let mut interner = self
                .interner
                .as_ref()
                .expect("interned storage has an interner")
                .lock()
                .map_err(|_| "string interner mutex was poisoned by a prior panic".to_string())?;
            let id = if is_null {
                NULL_STRING_ID
            } else {
                interner.intern(value.as_string().expect("validated string"))
            };
            ids.insert(index, id).expect("column prevalidated index");
        } else {
            self.data.insert(index, value);
        }
        if let Some(flags) = &mut self.null_flags {
            flags.insert(index, is_null);
        }
        Ok(())
    }

    pub fn delete(&mut self, index: usize) -> Result<ColumnValue, String> {
        self.check_index(index, false)?;
        let is_null = self.is_null_at(index);
        let value = if let ColumnData::StringIds(ids) = &mut self.data {
            let mut interner = self
                .interner
                .as_ref()
                .expect("interned storage has an interner")
                .lock()
                .map_err(|_| "string interner mutex was poisoned by a prior panic".to_string())?;
            let id = ids.get(index)?;
            // Resolve before mutation so a failed read leaves the column intact.
            let value = if is_null {
                ColumnValue::Null
            } else {
                interner
                    .resolve_unchecked(id)
                    .map(|s| ColumnValue::String(s.to_owned()))
                    .ok_or_else(|| format!("Invalid string ID {} at index {}", id, index))?
            };
            ids.delete(index).expect("column prevalidated index");
            if id != NULL_STRING_ID {
                interner.release(id);
            }
            value
        } else {
            let value = self.data.delete(index);
            if is_null {
                ColumnValue::Null
            } else {
                value
            }
        };
        if let Some(flags) = &mut self.null_flags {
            flags.delete(index);
        }
        Ok(value)
    }

    pub fn append(&mut self, value: ColumnValue) -> Result<(), String> {
        let value = self.validate_value(value)?;
        let is_null = value.is_null();
        let value = if is_null {
            self.get_default_value()
        } else {
            value
        };

        if let ColumnData::StringIds(ids) = &mut self.data {
            let mut interner = self
                .interner
                .as_ref()
                .expect("interned storage has an interner")
                .lock()
                .map_err(|_| "string interner mutex was poisoned by a prior panic".to_string())?;
            let id = if is_null {
                NULL_STRING_ID
            } else {
                interner.intern(value.as_string().expect("validated string"))
            };
            ids.append(id);
        } else {
            self.data.append(value);
        }
        if let Some(flags) = &mut self.null_flags {
            flags.push(is_null);
        }
        Ok(())
    }

    /// Roll back appended rows, releasing their interned-string references.
    /// Idempotent if the column is already no longer than target_len.
    pub(crate) fn truncate_to(&mut self, target_len: usize) {
        while self.len() > target_len {
            if self.delete(self.len() - 1).is_err() {
                break; // rollback must never spin if a delete fails
            }
        }
    }

    pub fn is_null(&self, index: usize) -> Result<bool, String> {
        // Preserve the public non-nullable fast path, including out-of-bounds.
        if let Some(flags) = &self.null_flags {
            flags
                .get(index)
                .ok_or_else(|| format!("Index {} out of range [0, {})", index, self.len()))
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
            ColumnType::Date => ColumnValue::Date(0), // 1970-01-01
            ColumnType::DateTime => ColumnValue::DateTime(0), // 1970-01-01 00:00:00
        }
    }

    pub fn iter(&self) -> ColumnIterator<'_> {
        ColumnIterator {
            column: self,
            index: 0,
        }
    }
}

impl Drop for Column {
    fn drop(&mut self) {
        if let (ColumnData::StringIds(ids), Some(interner)) = (&self.data, &self.interner) {
            // Destruction must not panic just because an earlier caller poisoned
            // the shared lock. Each live ID owns exactly one interner reference.
            let mut interner = interner.lock().unwrap_or_else(|error| error.into_inner());
            for index in 0..ids.len() {
                if let Some(&id) = ids.get_ref(index) {
                    if id != NULL_STRING_ID {
                        interner.release(id);
                    }
                }
            }
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
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_column_basic() {
        let mut col = Column::new("test".to_string(), ColumnType::Int32, false);
        col.append(ColumnValue::Int32(10)).unwrap();
        col.append(ColumnValue::Int32(20)).unwrap();
        col.append(ColumnValue::Int32(30)).unwrap();

        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0).unwrap().as_i32(), Some(10));
        assert_eq!(col.get(1).unwrap().as_i32(), Some(20));
        assert_eq!(col.get(2).unwrap().as_i32(), Some(30));
    }

    #[test]
    fn test_column_nullable() {
        let mut col = Column::new("test".to_string(), ColumnType::Int32, true);
        col.append(ColumnValue::Int32(10)).unwrap();
        col.append(ColumnValue::Null).unwrap();
        col.append(ColumnValue::Int32(30)).unwrap();

        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0).unwrap().as_i32(), Some(10));
        assert!(col.get(1).unwrap().is_null());
        assert!(col.is_null(1).unwrap());
        assert_eq!(col.get(2).unwrap().as_i32(), Some(30));
    }

    #[test]
    fn test_column_set() {
        let mut col = Column::new("test".to_string(), ColumnType::Int32, false);
        col.append(ColumnValue::Int32(10)).unwrap();
        col.append(ColumnValue::Int32(20)).unwrap();

        col.set(1, ColumnValue::Int32(99)).unwrap();
        assert_eq!(col.get(1).unwrap().as_i32(), Some(99));
    }

    #[test]
    fn test_column_string_interning() {
        let interner = Arc::new(Mutex::new(StringInterner::new()));

        let mut col = Column::new_with_interner(
            "names".to_string(),
            ColumnType::String,
            false,
            false,
            Some(interner.clone()),
        );

        // Add some repeated strings
        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap();
        col.append(ColumnValue::String("Bob".to_string())).unwrap();
        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap(); // Duplicate
        col.append(ColumnValue::String("Charlie".to_string()))
            .unwrap();
        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap(); // Another duplicate

        // Verify we can read values back
        assert_eq!(col.get(0).unwrap().as_string(), Some("Alice"));
        assert_eq!(col.get(1).unwrap().as_string(), Some("Bob"));
        assert_eq!(col.get(2).unwrap().as_string(), Some("Alice"));
        assert_eq!(col.get(3).unwrap().as_string(), Some("Charlie"));
        assert_eq!(col.get(4).unwrap().as_string(), Some("Alice"));

        // Verify the interner only has 3 unique strings
        assert_eq!(interner.lock().unwrap().len(), 3);

        // Verify reference counts
        let interner_ref = interner.lock().unwrap();
        // Alice should have 3 references
        let alice_id = interner_ref.string_to_id.get("Alice").unwrap();
        assert_eq!(interner_ref.ref_count(*alice_id), 3);
    }

    #[test]
    fn test_column_string_interning_update() {
        let interner = Arc::new(Mutex::new(StringInterner::new()));

        let mut col = Column::new_with_interner(
            "names".to_string(),
            ColumnType::String,
            false,
            false,
            Some(interner.clone()),
        );

        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap();
        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap();

        // Update one "Alice" to "Bob"
        col.set(1, ColumnValue::String("Bob".to_string())).unwrap();

        assert_eq!(col.get(0).unwrap().as_string(), Some("Alice"));
        assert_eq!(col.get(1).unwrap().as_string(), Some("Bob"));

        // Verify reference counts changed
        let interner_ref = interner.lock().unwrap();
        assert_eq!(interner_ref.len(), 2); // Alice and Bob
    }

    #[test]
    fn test_column_string_interning_delete() {
        let interner = Arc::new(Mutex::new(StringInterner::new()));

        let mut col = Column::new_with_interner(
            "names".to_string(),
            ColumnType::String,
            false,
            false,
            Some(interner.clone()),
        );

        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap();
        col.append(ColumnValue::String("Bob".to_string())).unwrap();
        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap();

        // Delete the only "Bob"
        let deleted = col.delete(1).unwrap();
        assert_eq!(deleted.as_string(), Some("Bob"));

        // Bob should be released
        assert_eq!(interner.lock().unwrap().len(), 1); // Only Alice remains
        assert_eq!(col.len(), 2);
        assert_eq!(col.get(0).unwrap().as_string(), Some("Alice"));
        assert_eq!(col.get(1).unwrap().as_string(), Some("Alice"));
    }

    #[test]
    fn test_truncate_to_releases_interner_refs() {
        let interner = Arc::new(Mutex::new(StringInterner::new()));

        let mut col = Column::new_with_interner(
            "names".to_string(),
            ColumnType::String,
            false,
            false,
            Some(interner.clone()),
        );

        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap();
        col.append(ColumnValue::String("Alice".to_string()))
            .unwrap();
        col.append(ColumnValue::String("Bob".to_string())).unwrap();
        assert_eq!(interner.lock().unwrap().len(), 2);

        // Roll back the last two appends: drops one "Alice" ref and the only "Bob" ref
        col.truncate_to(1);
        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0).unwrap().as_string(), Some("Alice"));
        assert_eq!(interner.lock().unwrap().len(), 1);

        // Rolling back everything must leave the interner empty
        col.truncate_to(0);
        assert_eq!(col.len(), 0);
        assert_eq!(interner.lock().unwrap().len(), 0);
    }
}
