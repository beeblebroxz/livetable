/// String Interner for LiveTable
///
/// A string interner stores unique strings once and returns integer IDs.
/// This significantly reduces memory usage for tables with repeated strings.
///
/// # Design
///
/// - Strings are stored once in a `Vec<String>`
/// - A `HashMap<String, u32>` provides O(1) lookup from string to ID
/// - IDs are 32-bit unsigned integers (supports ~4 billion unique strings)
/// - Reference counting tracks how many times each string is used
///
/// # Examples
///
/// ```
/// use livetable::StringInterner;
///
/// let mut interner = StringInterner::new();
///
/// // Intern strings - same string returns same ID
/// let id1 = interner.intern("hello");
/// let id2 = interner.intern("world");
/// let id3 = interner.intern("hello");  // Returns same ID as id1
///
/// assert_eq!(id1, id3);
/// assert_ne!(id1, id2);
///
/// // Resolve ID back to string
/// assert_eq!(interner.resolve(id1), Some("hello"));
/// ```

use std::collections::HashMap;

/// Interned string ID type
pub type StringId = u32;

/// A string interner that stores unique strings and returns integer IDs
#[derive(Debug, Clone)]
pub struct StringInterner {
    /// Maps string content to its ID (pub for testing)
    pub(crate) string_to_id: HashMap<String, StringId>,
    /// Stores strings by ID (index = ID)
    id_to_string: Vec<String>,
    /// Reference counts for each interned string
    ref_counts: Vec<u32>,
    /// Free list of IDs that can be reused (from strings with ref_count = 0)
    free_ids: Vec<StringId>,
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

impl StringInterner {
    /// Create a new empty string interner
    pub fn new() -> Self {
        StringInterner {
            string_to_id: HashMap::new(),
            id_to_string: Vec::new(),
            ref_counts: Vec::new(),
            free_ids: Vec::new(),
        }
    }

    /// Create a string interner with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        StringInterner {
            string_to_id: HashMap::with_capacity(capacity),
            id_to_string: Vec::with_capacity(capacity),
            ref_counts: Vec::with_capacity(capacity),
            free_ids: Vec::new(),
        }
    }

    /// Intern a string, returning its ID
    /// If the string already exists, increments its reference count and returns existing ID
    /// If the string is new, stores it and returns a new ID
    pub fn intern(&mut self, s: &str) -> StringId {
        // Check if string already exists
        if let Some(&id) = self.string_to_id.get(s) {
            self.ref_counts[id as usize] += 1;
            return id;
        }

        // String is new - get an ID (reuse from free list or allocate new)
        let id = if let Some(free_id) = self.free_ids.pop() {
            // Reuse a freed ID
            self.id_to_string[free_id as usize] = s.to_string();
            self.ref_counts[free_id as usize] = 1;
            free_id
        } else {
            // Allocate new ID
            let new_id = self.id_to_string.len() as StringId;
            self.id_to_string.push(s.to_string());
            self.ref_counts.push(1);
            new_id
        };

        self.string_to_id.insert(s.to_string(), id);
        id
    }

    /// Increment reference count for an existing ID
    pub fn add_ref(&mut self, id: StringId) {
        if (id as usize) < self.ref_counts.len() {
            self.ref_counts[id as usize] += 1;
        }
    }

    /// Decrement reference count for an ID
    /// If count reaches zero, the string is eligible for reuse
    pub fn release(&mut self, id: StringId) {
        let idx = id as usize;
        if idx < self.ref_counts.len() && self.ref_counts[idx] > 0 {
            self.ref_counts[idx] -= 1;
            if self.ref_counts[idx] == 0 {
                // Remove from string_to_id map and add to free list
                let s = &self.id_to_string[idx];
                self.string_to_id.remove(s);
                self.free_ids.push(id);
            }
        }
    }

    /// Resolve an ID back to its string
    /// Returns None if the ID is invalid or the string was released
    pub fn resolve(&self, id: StringId) -> Option<&str> {
        let idx = id as usize;
        if idx < self.id_to_string.len() && self.ref_counts[idx] > 0 {
            Some(&self.id_to_string[idx])
        } else {
            None
        }
    }

    /// Resolve an ID back to its string, even if reference count is 0
    /// Used internally during operations
    pub fn resolve_unchecked(&self, id: StringId) -> Option<&str> {
        self.id_to_string.get(id as usize).map(|s| s.as_str())
    }

    /// Get the reference count for an ID
    pub fn ref_count(&self, id: StringId) -> u32 {
        self.ref_counts.get(id as usize).copied().unwrap_or(0)
    }

    /// Returns the number of unique strings currently interned (with ref_count > 0)
    pub fn len(&self) -> usize {
        self.string_to_id.len()
    }

    /// Returns true if no strings are interned
    pub fn is_empty(&self) -> bool {
        self.string_to_id.is_empty()
    }

    /// Returns the total number of string slots (including freed ones)
    pub fn capacity(&self) -> usize {
        self.id_to_string.len()
    }

    /// Returns total memory used by all interned strings (approximate)
    pub fn memory_usage(&self) -> usize {
        let string_bytes: usize = self.id_to_string.iter()
            .enumerate()
            .filter(|(i, _)| self.ref_counts[*i] > 0)
            .map(|(_, s)| s.len() + std::mem::size_of::<String>())
            .sum();

        let map_overhead = self.string_to_id.capacity() *
            (std::mem::size_of::<String>() + std::mem::size_of::<StringId>());

        let vec_overhead = self.id_to_string.capacity() * std::mem::size_of::<String>()
            + self.ref_counts.capacity() * std::mem::size_of::<u32>()
            + self.free_ids.capacity() * std::mem::size_of::<StringId>();

        string_bytes + map_overhead + vec_overhead
    }

    /// Returns statistics about the interner
    pub fn stats(&self) -> InternerStats {
        let total_refs: u64 = self.ref_counts.iter().map(|&r| r as u64).sum();
        InternerStats {
            unique_strings: self.len(),
            total_references: total_refs,
            free_slots: self.free_ids.len(),
            memory_bytes: self.memory_usage(),
        }
    }
}

/// Statistics about the string interner
#[derive(Debug, Clone)]
pub struct InternerStats {
    /// Number of unique strings stored
    pub unique_strings: usize,
    /// Total number of references to all strings
    pub total_references: u64,
    /// Number of free slots available for reuse
    pub free_slots: usize,
    /// Approximate memory usage in bytes
    pub memory_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interner_basic() {
        let mut interner = StringInterner::new();

        let id1 = interner.intern("hello");
        let id2 = interner.intern("world");
        let id3 = interner.intern("hello");

        assert_eq!(id1, id3);
        assert_ne!(id1, id2);
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_interner_resolve() {
        let mut interner = StringInterner::new();

        let id = interner.intern("test string");
        assert_eq!(interner.resolve(id), Some("test string"));
    }

    #[test]
    fn test_interner_ref_counting() {
        let mut interner = StringInterner::new();

        let id = interner.intern("hello");
        assert_eq!(interner.ref_count(id), 1);

        interner.intern("hello");
        assert_eq!(interner.ref_count(id), 2);

        interner.release(id);
        assert_eq!(interner.ref_count(id), 1);

        interner.release(id);
        assert_eq!(interner.ref_count(id), 0);
        assert!(interner.resolve(id).is_none()); // String released
    }

    #[test]
    fn test_interner_id_reuse() {
        let mut interner = StringInterner::new();

        let id1 = interner.intern("first");
        let _id2 = interner.intern("second");

        // Release first string
        interner.release(id1);

        // New string should reuse the freed ID
        let id3 = interner.intern("third");
        assert_eq!(id3, id1); // Reused the freed ID
        assert_eq!(interner.resolve(id3), Some("third"));
    }

    #[test]
    fn test_interner_stats() {
        let mut interner = StringInterner::new();

        interner.intern("hello");
        interner.intern("world");
        interner.intern("hello"); // Duplicate

        let stats = interner.stats();
        assert_eq!(stats.unique_strings, 2);
        assert_eq!(stats.total_references, 3); // 2 refs to "hello", 1 to "world"
    }

    #[test]
    fn test_interner_empty_string() {
        let mut interner = StringInterner::new();

        let id = interner.intern("");
        assert_eq!(interner.resolve(id), Some(""));
    }

    #[test]
    fn test_add_ref() {
        let mut interner = StringInterner::new();

        let id = interner.intern("test");
        assert_eq!(interner.ref_count(id), 1);

        interner.add_ref(id);
        assert_eq!(interner.ref_count(id), 2);
    }
}
