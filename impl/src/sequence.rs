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

/// Tiered Vector implementation using sqrt decomposition.
///
/// Complexity guarantees:
/// - O(log √N) random access via binary search on block boundaries
///   (practically O(1) for reasonable N, e.g., log(1000) ≈ 10 comparisons)
/// - O(√N) insert/delete (shift within block + update cumulative indices)
/// - Good cache locality for sequential access within blocks
///
/// The data structure maintains:
/// - `blocks`: Vector of blocks, each containing approximately √N elements
/// - `block_starts`: Cumulative index array where block_starts[i] is the
///   global index where block i begins. Enables O(log √N) binary search.
/// - Automatic splitting when blocks exceed 2√N elements
/// - Automatic merging when blocks fall below √N/4 elements
#[derive(Debug, Clone)]
pub struct TieredVectorSequence<T: Clone> {
    blocks: Vec<Vec<T>>,
    block_starts: Vec<usize>,  // block_starts[i] = starting global index of block i
    size: usize,
}

impl<T: Clone> TieredVectorSequence<T> {
    /// Minimum block size to prevent excessive fragmentation
    const MIN_BLOCK_SIZE: usize = 16;
    /// Maximum block size to bound worst-case insert/delete
    const MAX_BLOCK_SIZE: usize = 4096;

    pub fn new() -> Self {
        TieredVectorSequence {
            blocks: Vec::new(),
            block_starts: Vec::new(),
            size: 0,
        }
    }

    /// For API compatibility - chunk_size hint is used for initial block sizing
    pub fn with_chunk_size(_chunk_size_hint: usize) -> Self {
        Self::new()
    }

    /// Calculate ideal block size as √N, clamped to reasonable bounds
    fn ideal_block_size(&self) -> usize {
        if self.size == 0 {
            return Self::MIN_BLOCK_SIZE;
        }
        let sqrt = (self.size as f64).sqrt() as usize;
        sqrt.clamp(Self::MIN_BLOCK_SIZE, Self::MAX_BLOCK_SIZE)
    }

    /// Find which block contains the given index using binary search.
    /// Returns (block_index, offset_within_block).
    ///
    /// Complexity: O(log(number of blocks)) = O(log √N) = O(½ log N)
    fn find_block(&self, index: usize) -> Result<(usize, usize), String> {
        if index >= self.size {
            return Err(format!("Index {} out of range [0, {})", index, self.size));
        }

        if self.blocks.is_empty() {
            return Err("Sequence is empty".to_string());
        }

        // Binary search: find the rightmost block where block_starts[i] <= index
        let block_idx = match self.block_starts.binary_search(&index) {
            Ok(i) => i,           // Exact match: index is at start of block i
            Err(i) => i.saturating_sub(1),  // index is somewhere in block i-1
        };

        let offset = index - self.block_starts[block_idx];

        // Sanity check
        debug_assert!(offset < self.blocks[block_idx].len(),
            "Offset {} >= block len {} for index {}",
            offset, self.blocks[block_idx].len(), index);

        Ok((block_idx, offset))
    }

    /// Update block_starts for all blocks after the given index.
    /// Called after insert (+1) or delete (-1) to maintain cumulative indices.
    fn update_block_starts_after(&mut self, block_idx: usize, delta: isize) {
        for i in (block_idx + 1)..self.block_starts.len() {
            self.block_starts[i] = (self.block_starts[i] as isize + delta) as usize;
        }
    }

    /// Split a block that has grown too large (> 2 * ideal_size).
    /// Maintains the sqrt(N) block size invariant.
    fn maybe_split_block(&mut self, block_idx: usize) {
        let ideal = self.ideal_block_size();
        let threshold = 2 * ideal;

        if self.blocks[block_idx].len() <= threshold {
            return;
        }

        let mid = self.blocks[block_idx].len() / 2;
        let new_block = self.blocks[block_idx].split_off(mid);
        let new_block_start = self.block_starts[block_idx] + self.blocks[block_idx].len();

        // Insert new block right after current one
        self.blocks.insert(block_idx + 1, new_block);
        self.block_starts.insert(block_idx + 1, new_block_start);
    }

    /// Merge a block that has become too small (< ideal_size / 4) with a neighbor.
    /// Prevents excessive fragmentation from many deletions.
    fn maybe_merge_block(&mut self, block_idx: usize) {
        if self.blocks.len() <= 1 {
            return;
        }

        // Don't merge if block is empty (will be removed separately)
        if self.blocks[block_idx].is_empty() {
            return;
        }

        let ideal = self.ideal_block_size();
        let threshold = ideal / 4;

        if self.blocks[block_idx].len() >= threshold {
            return;
        }

        // Prefer merging with the smaller neighbor to keep blocks balanced
        let merge_with_next = block_idx + 1 < self.blocks.len()
            && (block_idx == 0
                || self.blocks[block_idx + 1].len() <= self.blocks[block_idx - 1].len());

        if merge_with_next && block_idx + 1 < self.blocks.len() {
            let combined = self.blocks[block_idx].len() + self.blocks[block_idx + 1].len();
            if combined <= 2 * ideal {
                let next_block = self.blocks.remove(block_idx + 1);
                self.block_starts.remove(block_idx + 1);
                self.blocks[block_idx].extend(next_block);
                return;
            }
        }

        if block_idx > 0 {
            let combined = self.blocks[block_idx - 1].len() + self.blocks[block_idx].len();
            if combined <= 2 * ideal {
                let current_block = self.blocks.remove(block_idx);
                self.block_starts.remove(block_idx);
                self.blocks[block_idx - 1].extend(current_block);
            }
        }
    }

    /// Remove an empty block entirely (proper cleanup, no memory leaks)
    fn remove_empty_block(&mut self, block_idx: usize) {
        if block_idx < self.blocks.len() && self.blocks[block_idx].is_empty() {
            self.blocks.remove(block_idx);
            self.block_starts.remove(block_idx);
        }
    }

    /// Rebalance all blocks to have approximately equal size.
    /// Call this periodically for optimal performance after many operations.
    /// Complexity: O(N)
    pub fn rebalance(&mut self) {
        if self.size == 0 {
            self.blocks.clear();
            self.block_starts.clear();
            return;
        }

        // Collect all elements
        let all_elements: Vec<T> = self.blocks
            .iter()
            .flat_map(|b| b.iter().cloned())
            .collect();

        // Calculate new block configuration
        let block_size = self.ideal_block_size();
        let num_blocks = (self.size + block_size - 1) / block_size;

        // Redistribute evenly
        self.blocks.clear();
        self.block_starts.clear();

        for i in 0..num_blocks {
            let start = i * block_size;
            let end = ((i + 1) * block_size).min(self.size);
            self.blocks.push(all_elements[start..end].to_vec());
            self.block_starts.push(start);
        }
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
        let (block_idx, offset) = self.find_block(index)?;
        Ok(self.blocks[block_idx][offset].clone())
    }

    fn get_ref(&self, index: usize) -> Option<&T> {
        if index >= self.size {
            return None;
        }
        self.find_block(index)
            .ok()
            .map(|(block_idx, offset)| &self.blocks[block_idx][offset])
    }

    fn set(&mut self, index: usize, value: T) -> Result<(), String> {
        let (block_idx, offset) = self.find_block(index)?;
        self.blocks[block_idx][offset] = value;
        Ok(())
    }

    fn insert(&mut self, index: usize, value: T) -> Result<(), String> {
        if index > self.size {
            return Err(format!("Index {} out of range [0, {}]", index, self.size));
        }

        // Special case: empty sequence - create first block
        if self.blocks.is_empty() {
            self.blocks.push(vec![value]);
            self.block_starts.push(0);
            self.size = 1;
            return Ok(());
        }

        // Special case: append to end
        if index == self.size {
            self.append(value);
            return Ok(());
        }

        // Find the block containing this index
        let (block_idx, offset) = self.find_block(index)?;

        // Insert within the block - O(block_size) = O(√N)
        self.blocks[block_idx].insert(offset, value);
        self.size += 1;

        // Update cumulative indices for subsequent blocks - O(num_blocks) = O(√N)
        self.update_block_starts_after(block_idx, 1);

        // Split if block is too large
        self.maybe_split_block(block_idx);

        Ok(())
    }

    fn delete(&mut self, index: usize) -> Result<T, String> {
        if index >= self.size {
            return Err(format!("Index {} out of range [0, {})", index, self.size));
        }

        let (block_idx, offset) = self.find_block(index)?;

        // Remove from block - O(block_size) = O(√N)
        let value = self.blocks[block_idx].remove(offset);
        self.size -= 1;

        // Update cumulative indices - O(num_blocks) = O(√N)
        self.update_block_starts_after(block_idx, -1);

        // Handle empty or small blocks
        if self.blocks[block_idx].is_empty() {
            self.remove_empty_block(block_idx);
        } else {
            self.maybe_merge_block(block_idx);
        }

        Ok(value)
    }

    fn append(&mut self, value: T) {
        // Special case: empty sequence
        if self.blocks.is_empty() {
            self.blocks.push(vec![value]);
            self.block_starts.push(0);
            self.size = 1;
            return;
        }

        // Add to last block - O(1) amortized
        let last_block_idx = self.blocks.len() - 1;
        self.blocks[last_block_idx].push(value);
        self.size += 1;

        // Split if needed
        self.maybe_split_block(last_block_idx);
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        // Efficient iterator that walks blocks sequentially
        Box::new(TieredVectorIterator {
            blocks: &self.blocks,
            block_idx: 0,
            elem_idx: 0,
            remaining: self.size,
        })
    }
}

/// Efficient iterator for TieredVectorSequence.
/// Walks blocks sequentially for O(N) total iteration (not O(N log N)).
struct TieredVectorIterator<'a, T: Clone> {
    blocks: &'a Vec<Vec<T>>,
    block_idx: usize,
    elem_idx: usize,
    remaining: usize,
}

impl<'a, T: Clone> Iterator for TieredVectorIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        // Find next valid element
        while self.block_idx < self.blocks.len() {
            if self.elem_idx < self.blocks[self.block_idx].len() {
                let value = self.blocks[self.block_idx][self.elem_idx].clone();
                self.elem_idx += 1;
                self.remaining -= 1;
                return Some(value);
            }
            // Move to next block
            self.block_idx += 1;
            self.elem_idx = 0;
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a, T: Clone> ExactSizeIterator for TieredVectorIterator<'a, T> {}

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

    // =========================================================================
    // Comprehensive TieredVector Tests
    // =========================================================================

    /// Test that verifies correct indexing after multiple insertions
    /// This would have caught the bug where insert used division instead of find_block
    #[test]
    fn test_tiered_vector_insert_correctness_after_splits() {
        let mut seq = TieredVectorSequence::<i32>::new();

        // Insert 100 elements
        for i in 0..100 {
            seq.append(i);
        }

        // Insert at various positions and verify ALL elements are still correct
        seq.insert(25, 9990).unwrap();  // Insert in first quarter (len=101)
        seq.insert(50, 9991).unwrap();  // Insert in middle (len=102)
        seq.insert(75, 9992).unwrap();  // Insert in third quarter (len=103)

        assert_eq!(seq.len(), 103);

        // Verify inserted elements are exactly where we put them
        assert_eq!(seq.get(25).unwrap(), 9990);
        assert_eq!(seq.get(50).unwrap(), 9991);
        assert_eq!(seq.get(75).unwrap(), 9992);

        // Verify original elements are in correct positions
        assert_eq!(seq.get(0).unwrap(), 0);
        assert_eq!(seq.get(24).unwrap(), 24);
        assert_eq!(seq.get(26).unwrap(), 25);  // Was at 25, shifted by first insert
        assert_eq!(seq.get(51).unwrap(), 49);  // Was at 49, shifted by inserts at 25 and 50
        assert_eq!(seq.get(76).unwrap(), 73);  // Was at 73, shifted by all three inserts
        assert_eq!(seq.get(102).unwrap(), 99);
    }

    /// Test insert at every position to verify correctness
    #[test]
    fn test_tiered_vector_insert_at_all_positions() {
        for insert_pos in 0..=20 {
            let mut seq = TieredVectorSequence::<i32>::new();

            // Create sequence [0, 1, 2, ..., 19]
            for i in 0..20 {
                seq.append(i);
            }

            // Insert 999 at position insert_pos
            seq.insert(insert_pos, 999).unwrap();

            assert_eq!(seq.len(), 21, "Failed for insert_pos={}", insert_pos);

            // Verify all elements
            for i in 0..21 {
                let expected = if i < insert_pos {
                    i as i32
                } else if i == insert_pos {
                    999
                } else {
                    (i - 1) as i32
                };
                assert_eq!(
                    seq.get(i).unwrap(),
                    expected,
                    "Mismatch at index {} after inserting at {}",
                    i,
                    insert_pos
                );
            }
        }
    }

    /// Test that delete maintains correct indices
    #[test]
    fn test_tiered_vector_delete_maintains_indices() {
        let mut seq = TieredVectorSequence::<i32>::new();

        // Create sequence [0, 10, 20, ..., 190]
        for i in 0..20 {
            seq.append(i * 10);
        }

        // Delete from middle
        let deleted = seq.delete(10).unwrap();
        assert_eq!(deleted, 100);
        assert_eq!(seq.len(), 19);

        // Verify elements shifted correctly
        assert_eq!(seq.get(9).unwrap(), 90);
        assert_eq!(seq.get(10).unwrap(), 110);  // Was at index 11
        assert_eq!(seq.get(18).unwrap(), 190);

        // Delete from beginning
        let deleted = seq.delete(0).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(seq.get(0).unwrap(), 10);

        // Delete from end
        let deleted = seq.delete(seq.len() - 1).unwrap();
        assert_eq!(deleted, 190);
    }

    /// Test with non-uniform block sizes (simulating real-world usage)
    #[test]
    fn test_tiered_vector_mixed_operations() {
        let mut seq = TieredVectorSequence::<i32>::new();

        // Interleave appends and inserts to create non-uniform blocks
        for i in 0..50 {
            seq.append((i * 2) as i32);  // Even numbers
        }

        // Insert odd numbers
        for i in 0..50 {
            seq.insert(i * 2 + 1, (i * 2 + 1) as i32).unwrap();  // Insert at odd positions
        }

        assert_eq!(seq.len(), 100);

        // Verify sequence is [0, 1, 2, 3, ..., 99]
        for i in 0..100 {
            assert_eq!(
                seq.get(i).unwrap(),
                i as i32,
                "Mismatch at index {} after mixed operations",
                i
            );
        }
    }

    /// Test iterator correctness
    #[test]
    fn test_tiered_vector_iterator() {
        let mut seq = TieredVectorSequence::<i32>::new();

        for i in 0..100 {
            seq.append(i);
        }

        // Verify iterator returns all elements in order
        let collected: Vec<i32> = seq.iter().collect();
        assert_eq!(collected.len(), 100);
        for (i, val) in collected.iter().enumerate() {
            assert_eq!(*val, i as i32);
        }
    }

    /// Test rebalance operation
    #[test]
    fn test_tiered_vector_rebalance() {
        let mut seq = TieredVectorSequence::<i32>::new();

        // Create unbalanced structure through many deletions
        for i in 0..100 {
            seq.append(i);
        }

        // Delete every other element
        for i in (0..50).rev() {
            seq.delete(i * 2).unwrap();
        }

        assert_eq!(seq.len(), 50);

        // Rebalance
        seq.rebalance();

        // Verify all elements still accessible and correct
        let expected: Vec<i32> = (0..100).filter(|x| x % 2 == 1).collect();
        for (i, expected_val) in expected.iter().enumerate() {
            assert_eq!(seq.get(i).unwrap(), *expected_val);
        }
    }

    /// Test edge case: single element operations
    #[test]
    fn test_tiered_vector_single_element() {
        let mut seq = TieredVectorSequence::<i32>::new();

        seq.append(42);
        assert_eq!(seq.len(), 1);
        assert_eq!(seq.get(0).unwrap(), 42);

        seq.set(0, 99).unwrap();
        assert_eq!(seq.get(0).unwrap(), 99);

        let deleted = seq.delete(0).unwrap();
        assert_eq!(deleted, 99);
        assert_eq!(seq.len(), 0);
    }

    /// Test edge case: empty sequence
    #[test]
    fn test_tiered_vector_empty() {
        let seq = TieredVectorSequence::<i32>::new();

        assert_eq!(seq.len(), 0);
        assert!(seq.is_empty());
        assert!(seq.get(0).is_err());
        assert!(seq.get_ref(0).is_none());
    }

    /// Test that get_ref returns correct references
    #[test]
    fn test_tiered_vector_get_ref() {
        let mut seq = TieredVectorSequence::<i32>::new();

        for i in 0..50 {
            seq.append(i * 10);
        }

        // Test get_ref at various positions
        assert_eq!(seq.get_ref(0), Some(&0));
        assert_eq!(seq.get_ref(25), Some(&250));
        assert_eq!(seq.get_ref(49), Some(&490));
        assert_eq!(seq.get_ref(50), None);  // Out of bounds
    }

    /// Test large scale operations to verify O(√N) complexity doesn't break
    #[test]
    fn test_tiered_vector_large_scale() {
        let mut seq = TieredVectorSequence::<i32>::new();
        let n = 10000;

        // Append N elements
        for i in 0..n {
            seq.append(i);
        }
        assert_eq!(seq.len(), n as usize);

        // Verify random access works correctly
        assert_eq!(seq.get(0).unwrap(), 0);
        assert_eq!(seq.get(n as usize / 2).unwrap(), n / 2);
        assert_eq!(seq.get(n as usize - 1).unwrap(), n - 1);

        // Insert in middle
        seq.insert(n as usize / 2, 99999).unwrap();
        assert_eq!(seq.get(n as usize / 2).unwrap(), 99999);
        assert_eq!(seq.get(n as usize / 2 + 1).unwrap(), n / 2);

        // Delete from middle
        seq.delete(n as usize / 2).unwrap();
        assert_eq!(seq.get(n as usize / 2).unwrap(), n / 2);
    }

    /// Test set operation
    #[test]
    fn test_tiered_vector_set() {
        let mut seq = TieredVectorSequence::<i32>::new();

        for i in 0..20 {
            seq.append(i);
        }

        // Set various positions
        seq.set(0, 100).unwrap();
        seq.set(10, 200).unwrap();
        seq.set(19, 300).unwrap();

        assert_eq!(seq.get(0).unwrap(), 100);
        assert_eq!(seq.get(10).unwrap(), 200);
        assert_eq!(seq.get(19).unwrap(), 300);

        // Other elements unchanged
        assert_eq!(seq.get(1).unwrap(), 1);
        assert_eq!(seq.get(9).unwrap(), 9);
    }

    /// Test insert at beginning (index 0)
    #[test]
    fn test_tiered_vector_insert_at_beginning() {
        let mut seq = TieredVectorSequence::<i32>::new();

        for i in 0..20 {
            seq.append(i + 1);  // [1, 2, 3, ..., 20]
        }

        seq.insert(0, 0).unwrap();  // Insert 0 at beginning

        assert_eq!(seq.len(), 21);
        for i in 0..21 {
            assert_eq!(seq.get(i).unwrap(), i as i32);
        }
    }

    /// Test insert at end (same as append)
    #[test]
    fn test_tiered_vector_insert_at_end() {
        let mut seq = TieredVectorSequence::<i32>::new();

        for i in 0..20 {
            seq.append(i);
        }

        seq.insert(20, 20).unwrap();  // Insert at end

        assert_eq!(seq.len(), 21);
        for i in 0..21 {
            assert_eq!(seq.get(i).unwrap(), i as i32);
        }
    }

    /// Test error handling
    #[test]
    fn test_tiered_vector_error_handling() {
        let mut seq = TieredVectorSequence::<i32>::new();

        // Get on empty
        assert!(seq.get(0).is_err());

        // Insert out of bounds
        assert!(seq.insert(1, 42).is_err());

        seq.append(1);
        seq.append(2);

        // Get out of bounds
        assert!(seq.get(2).is_err());
        assert!(seq.get(100).is_err());

        // Delete out of bounds
        assert!(seq.delete(2).is_err());
        assert!(seq.delete(100).is_err());

        // Set out of bounds
        assert!(seq.set(2, 99).is_err());
    }
}
