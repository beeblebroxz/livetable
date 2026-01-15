/// LiveTable Sequence Implementation in Rust
///
/// A Sequence is the lowest-level storage implementation for raw values.
/// Supports two implementations:
/// - ArraySequence: Simple contiguous array with O(1) access, O(N) insert/delete
/// - TieredVectorSequence: Uses indirection for O(1) access, O(sqrt(N)) insert/delete

use std::fmt::Debug;

/// Trait for sequence storage operations
pub trait Sequence<T: Clone> {
    /// Return the number of elements in the sequence
    fn len(&self) -> usize;

    /// Check if the sequence is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get value at index (0-based) - clones the value
    fn get(&self, index: usize) -> Result<T, String>;

    /// Get reference to value at index (0-based) - no clone
    fn get_ref(&self, index: usize) -> Option<&T>;

    /// Set value at index
    fn set(&mut self, index: usize, value: T) -> Result<(), String>;

    /// Insert value at index, shifting subsequent elements
    fn insert(&mut self, index: usize, value: T) -> Result<(), String>;

    /// Delete and return value at index
    fn delete(&mut self, index: usize) -> Result<T, String>;

    /// Append value to end
    fn append(&mut self, value: T);

    /// Iterate over all values
    fn iter(&self) -> Box<dyn Iterator<Item = T> + '_>;
}

/// Simple contiguous array implementation.
/// - O(1) random access
/// - O(N) insert/delete (worst case)
/// - Optimal cache locality
#[derive(Debug, Clone)]
pub struct ArraySequence<T: Clone> {
    data: Vec<T>,
}

impl<T: Clone> ArraySequence<T> {
    pub fn new() -> Self {
        ArraySequence { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ArraySequence {
            data: Vec::with_capacity(capacity),
        }
    }
}

impl<T: Clone> Default for ArraySequence<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Debug> Sequence<T> for ArraySequence<T> {
    fn len(&self) -> usize {
        self.data.len()
    }

    fn get(&self, index: usize) -> Result<T, String> {
        self.data
            .get(index)
            .cloned()
            .ok_or_else(|| format!("Index {} out of range [0, {})", index, self.data.len()))
    }

    fn get_ref(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }

    fn set(&mut self, index: usize, value: T) -> Result<(), String> {
        if index >= self.data.len() {
            return Err(format!("Index {} out of range [0, {})", index, self.data.len()));
        }
        self.data[index] = value;
        Ok(())
    }

    fn insert(&mut self, index: usize, value: T) -> Result<(), String> {
        if index > self.data.len() {
            return Err(format!("Index {} out of range [0, {}]", index, self.data.len()));
        }
        self.data.insert(index, value);
        Ok(())
    }

    fn delete(&mut self, index: usize) -> Result<T, String> {
        if index >= self.data.len() {
            return Err(format!("Index {} out of range [0, {})", index, self.data.len()));
        }
        Ok(self.data.remove(index))
    }

    fn append(&mut self, value: T) {
        self.data.push(value);
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        Box::new(self.data.iter().cloned())
    }
}

/// Tiered Vector implementation using indirection and rotation.
/// - O(1) random access (with small constant overhead)
/// - O(sqrt(N)) insert/delete
/// - Good cache locality for sequential access
///
/// A Tiered Vector of size N is represented using at most 2*sqrt(N)
/// contiguous chunks, with a rotation table for indirection.
#[derive(Debug, Clone)]
pub struct TieredVectorSequence<T: Clone> {
    chunks: Vec<Vec<T>>,
    rotation: Vec<usize>, // Maps logical chunk index to physical
    size: usize,
    chunk_size: usize,
}

impl<T: Clone> TieredVectorSequence<T> {
    pub fn new() -> Self {
        Self::with_chunk_size(64)
    }

    pub fn with_chunk_size(chunk_size: usize) -> Self {
        TieredVectorSequence {
            chunks: Vec::new(),
            rotation: Vec::new(),
            size: 0,
            chunk_size,
        }
    }

    /// Convert logical index to (chunk_index, offset_in_chunk)
    fn get_chunk_and_offset(&self, index: usize) -> Result<(usize, usize), String> {
        if index >= self.size {
            return Err(format!("Index {} out of range [0, {})", index, self.size));
        }

        let mut current_index = 0;
        for (_logical_chunk, &physical_chunk) in self.rotation.iter().enumerate() {
            let chunk_size = self.chunks[physical_chunk].len();

            if index < current_index + chunk_size {
                let offset = index - current_index;
                return Ok((physical_chunk, offset));
            }

            current_index += chunk_size;
        }

        Err(format!("Index {} not found in chunks", index))
    }

    /// Split a chunk that's grown too large
    fn split_chunk(&mut self, logical_chunk_idx: usize) {
        let physical_idx = self.rotation[logical_chunk_idx];
        let mid = self.chunks[physical_idx].len() / 2;

        // Split the chunk
        let new_chunk = self.chunks[physical_idx].split_off(mid);

        // Add new physical chunk
        let new_physical_idx = self.chunks.len();
        self.chunks.push(new_chunk);

        // Insert into rotation table
        self.rotation.insert(logical_chunk_idx + 1, new_physical_idx);
    }

    /// Remove an empty chunk
    fn remove_chunk(&mut self, physical_idx: usize) {
        // Find and remove from rotation table
        if let Some(pos) = self.rotation.iter().position(|&x| x == physical_idx) {
            self.rotation.remove(pos);
        }

        // Clear the chunk (don't remove to avoid reindexing)
        self.chunks[physical_idx].clear();
    }
}

impl<T: Clone> Default for TieredVectorSequence<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Debug> Sequence<T> for TieredVectorSequence<T> {
    fn len(&self) -> usize {
        self.size
    }

    fn get(&self, index: usize) -> Result<T, String> {
        if self.size == 0 {
            return Err("Sequence is empty".to_string());
        }

        let (chunk_idx, offset) = self.get_chunk_and_offset(index)?;
        Ok(self.chunks[chunk_idx][offset].clone())
    }

    fn get_ref(&self, index: usize) -> Option<&T> {
        if self.size == 0 || index >= self.size {
            return None;
        }
        self.get_chunk_and_offset(index)
            .ok()
            .map(|(chunk_idx, offset)| &self.chunks[chunk_idx][offset])
    }

    fn set(&mut self, index: usize, value: T) -> Result<(), String> {
        if self.size == 0 {
            return Err("Sequence is empty".to_string());
        }

        let (chunk_idx, offset) = self.get_chunk_and_offset(index)?;
        self.chunks[chunk_idx][offset] = value;
        Ok(())
    }

    fn insert(&mut self, index: usize, value: T) -> Result<(), String> {
        if index > self.size {
            return Err(format!("Index {} out of range [0, {}]", index, self.size));
        }

        // Special case: empty sequence
        if self.size == 0 {
            self.chunks.push(vec![value]);
            self.rotation.push(0);
            self.size = 1;
            return Ok(());
        }

        // Special case: append
        if index == self.size {
            self.append(value);
            return Ok(());
        }

        // Find which chunk to insert into
        let mut chunk_idx = index / self.chunk_size;
        let mut offset = index % self.chunk_size;

        if chunk_idx >= self.rotation.len() {
            chunk_idx = self.rotation.len() - 1;
            offset = self.chunks[self.rotation[chunk_idx]].len();
        }

        let physical_chunk = self.rotation[chunk_idx];

        // Insert into the chunk
        self.chunks[physical_chunk].insert(offset, value);
        self.size += 1;

        // If chunk is too large, split it
        if self.chunks[physical_chunk].len() > 2 * self.chunk_size {
            self.split_chunk(chunk_idx);
        }

        Ok(())
    }

    fn delete(&mut self, index: usize) -> Result<T, String> {
        if index >= self.size {
            return Err(format!("Index {} out of range [0, {})", index, self.size));
        }

        let (chunk_idx, offset) = self.get_chunk_and_offset(index)?;
        let value = self.chunks[chunk_idx].remove(offset);
        self.size -= 1;

        // If chunk is empty, remove it
        if self.chunks[chunk_idx].is_empty() {
            self.remove_chunk(chunk_idx);
        }

        Ok(value)
    }

    fn append(&mut self, value: T) {
        if self.size == 0 {
            self.chunks.push(vec![value]);
            self.rotation.push(0);
            self.size = 1;
            return;
        }

        // Add to last chunk
        let last_logical = self.rotation.len() - 1;
        let last_physical = self.rotation[last_logical];

        self.chunks[last_physical].push(value);
        self.size += 1;

        // Split if needed
        if self.chunks[last_physical].len() > 2 * self.chunk_size {
            self.split_chunk(last_logical);
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        Box::new(SequenceIterator {
            sequence: self,
            index: 0,
        })
    }
}

struct SequenceIterator<'a, T: Clone> {
    sequence: &'a TieredVectorSequence<T>,
    index: usize,
}

impl<'a, T: Clone + Debug> Iterator for SequenceIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.sequence.size {
            None
        } else {
            let result = self.sequence.get(self.index).ok();
            self.index += 1;
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_sequence_basic() {
        let mut seq = ArraySequence::<i32>::new();
        seq.append(10);
        seq.append(20);
        seq.append(30);

        assert_eq!(seq.len(), 3);
        assert_eq!(seq.get(0).unwrap(), 10);
        assert_eq!(seq.get(1).unwrap(), 20);
        assert_eq!(seq.get(2).unwrap(), 30);
    }

    #[test]
    fn test_array_sequence_insert() {
        let mut seq = ArraySequence::<i32>::new();
        seq.append(10);
        seq.append(30);
        seq.insert(1, 20).unwrap();

        assert_eq!(seq.len(), 3);
        assert_eq!(seq.get(0).unwrap(), 10);
        assert_eq!(seq.get(1).unwrap(), 20);
        assert_eq!(seq.get(2).unwrap(), 30);
    }

    #[test]
    fn test_tiered_vector_basic() {
        let mut seq = TieredVectorSequence::<i32>::new();
        seq.append(10);
        seq.append(20);
        seq.append(30);

        assert_eq!(seq.len(), 3);
        assert_eq!(seq.get(0).unwrap(), 10);
        assert_eq!(seq.get(1).unwrap(), 20);
        assert_eq!(seq.get(2).unwrap(), 30);
    }

    #[test]
    fn test_tiered_vector_insert() {
        let mut seq = TieredVectorSequence::<i32>::with_chunk_size(4);
        for i in 0..10 {
            seq.append(i);
        }

        seq.insert(5, 99).unwrap();
        assert_eq!(seq.len(), 11);
        assert_eq!(seq.get(5).unwrap(), 99);
        assert_eq!(seq.get(6).unwrap(), 5);
    }

    #[test]
    fn test_array_sequence_delete() {
        let mut seq = ArraySequence::<i32>::new();
        seq.append(10);
        seq.append(20);
        seq.append(30);
        seq.append(40);

        let deleted = seq.delete(1).unwrap();
        assert_eq!(deleted, 20);
        assert_eq!(seq.len(), 3);
        assert_eq!(seq.get(0).unwrap(), 10);
        assert_eq!(seq.get(1).unwrap(), 30);
        assert_eq!(seq.get(2).unwrap(), 40);
    }

    #[test]
    fn test_tiered_vector_delete() {
        let mut seq = TieredVectorSequence::<i32>::with_chunk_size(4);
        for i in 0..10 {
            seq.append(i * 10);
        }

        let deleted = seq.delete(5).unwrap();
        assert_eq!(deleted, 50);
        assert_eq!(seq.len(), 9);
        assert_eq!(seq.get(4).unwrap(), 40);
        assert_eq!(seq.get(5).unwrap(), 60);
        assert_eq!(seq.get(8).unwrap(), 90);
    }
}
