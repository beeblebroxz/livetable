//! Packed NULL flags. Array storage shifts words; tiered storage uses circular
//! bit blocks so middle edits do not introduce an O(N) scan into FastUpdates.

pub(super) enum NullBitmap {
    Array { words: Vec<u64>, len: usize },
    Tiered(TieredBits),
}

impl NullBitmap {
    pub(super) fn new(tiered: bool) -> Self {
        if tiered {
            Self::Tiered(TieredBits::new())
        } else {
            Self::Array {
                words: Vec::new(),
                len: 0,
            }
        }
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Array { len, .. } => *len,
            Self::Tiered(bits) => bits.len,
        }
    }

    #[inline]
    pub(super) fn get(&self, index: usize) -> Option<bool> {
        if index >= self.len() {
            return None;
        }
        Some(match self {
            Self::Array { words, .. } => words[index / 64] & (1 << (index % 64)) != 0,
            Self::Tiered(bits) => bits.get(index),
        })
    }

    pub(super) fn set(&mut self, index: usize, value: bool) {
        assert!(index < self.len());
        match self {
            Self::Array { words, .. } => set_bit(&mut words[index / 64], index % 64, value),
            Self::Tiered(bits) => bits.set(index, value),
        }
    }

    pub(super) fn push(&mut self, value: bool) {
        match self {
            Self::Array { words, len } => {
                if len.is_multiple_of(64) {
                    words.push(0);
                }
                set_bit(&mut words[*len / 64], *len % 64, value);
                *len += 1;
            }
            Self::Tiered(bits) => bits.push(value),
        }
    }

    pub(super) fn insert(&mut self, index: usize, value: bool) {
        assert!(index <= self.len());
        if index == self.len() {
            self.push(value);
            return;
        }
        match self {
            Self::Array { words, len } => {
                if len.is_multiple_of(64) {
                    words.push(0);
                }
                let word = index / 64;
                let bit = index % 64;
                let low = (1u64 << bit) - 1;
                let old = words[word];
                let mut carry = old >> 63;
                words[word] = (old & low) | ((old & !low) << 1) | ((value as u64) << bit);
                for next in &mut words[word + 1..] {
                    let old = *next;
                    *next = (old << 1) | carry;
                    carry = old >> 63;
                }
                *len += 1;
            }
            Self::Tiered(bits) => bits.insert(index, value),
        }
    }

    pub(super) fn delete(&mut self, index: usize) {
        assert!(index < self.len());
        match self {
            Self::Array { words, len } => {
                let word = index / 64;
                let low = (1u64 << (index % 64)) - 1;
                let carry = words.get(word + 1).copied().unwrap_or(0) << 63;
                words[word] = (words[word] & low) | ((words[word] >> 1) & !low) | carry;
                for i in word + 1..words.len() {
                    let carry = words.get(i + 1).copied().unwrap_or(0) << 63;
                    words[i] = (words[i] >> 1) | carry;
                }
                *len -= 1;
                if len.is_multiple_of(64) {
                    words.pop();
                }
            }
            Self::Tiered(bits) => bits.delete(index),
        }
    }
}

#[inline]
fn set_bit(word: &mut u64, bit: usize, value: bool) {
    let mask = 1u64 << bit;
    *word = (*word & !mask) | ((value as u64) << bit);
}

struct BitBlock {
    words: Box<[u64]>,
    head: usize,
}

impl BitBlock {
    fn new(bits: usize) -> Self {
        Self {
            words: vec![0; bits / 64].into_boxed_slice(),
            head: 0,
        }
    }

    fn get(&self, index: usize, size: usize) -> bool {
        let physical = (self.head + index) & (size - 1);
        self.words[physical / 64] & (1 << (physical % 64)) != 0
    }

    fn set(&mut self, index: usize, value: bool, size: usize) {
        let physical = (self.head + index) & (size - 1);
        set_bit(&mut self.words[physical / 64], physical % 64, value);
    }

    // Read/write up to a word across a circular word boundary. These helpers
    // let local shifts move 64 bits at a time while preserving unrelated bits.
    fn read_word(&self, index: usize, size: usize) -> u64 {
        let physical = (self.head + index) & (size - 1);
        let word = physical / 64;
        let bit = physical % 64;
        let low = self.words[word] >> bit;
        if bit == 0 {
            low
        } else {
            low | (self.words[(word + 1) & (self.words.len() - 1)] << (64 - bit))
        }
    }

    fn write_bits(&mut self, index: usize, count: usize, value: u64, size: usize) {
        debug_assert!((1..=64).contains(&count));
        let physical = (self.head + index) & (size - 1);
        let word = physical / 64;
        let bit = physical % 64;
        let first = count.min(64 - bit);
        let mask = (u64::MAX >> (64 - first)) << bit;
        self.words[word] = (self.words[word] & !mask) | ((value << bit) & mask);
        if first < count {
            let next = (word + 1) & (self.words.len() - 1);
            let mask = u64::MAX >> (64 - (count - first));
            self.words[next] = (self.words[next] & !mask) | ((value >> first) & mask);
        }
    }

    fn shift_right(&mut self, offset: usize, used: usize, size: usize) {
        let mut end = used;
        while end > offset + 1 {
            let count = 64.min(end - offset - 1);
            let start = end - count;
            let value = self.read_word(start - 1, size);
            self.write_bits(start, count, value, size);
            end = start;
        }
    }

    fn shift_left(&mut self, offset: usize, used: usize, size: usize) {
        let mut start = offset;
        while start < used - 1 {
            let count = 64.min(used - 1 - start);
            let value = self.read_word(start + 1, size);
            self.write_bits(start, count, value, size);
            start += count;
        }
    }

    // Push at the front of a full block, returning the displaced last bit.
    fn push_front(&mut self, value: bool, size: usize) -> bool {
        let carry = self.get(size - 1, size);
        self.head = self.head.wrapping_sub(1) & (size - 1);
        self.set(0, value, size);
        carry
    }

    fn pop_front(&mut self, size: usize) -> bool {
        let value = self.get(0, size);
        self.head = (self.head + 1) & (size - 1);
        value
    }
}

// All blocks except the last hold B bits. B is a power of two near sqrt(N),
// allowing direct O(1) addressing. Edits shift at most B bits in one block and
// rotate O(N/B) subsequent blocks. Occasional O(N) resizing is amortized, with
// hysteresis to prevent alternating inserts/deletes from repeatedly resizing.
pub(super) struct TieredBits {
    blocks: Vec<BitBlock>,
    block_bits: usize,
    len: usize,
}

impl TieredBits {
    fn new() -> Self {
        Self {
            blocks: Vec::new(),
            block_bits: 64,
            len: 0,
        }
    }

    fn get(&self, index: usize) -> bool {
        self.blocks[index / self.block_bits].get(index % self.block_bits, self.block_bits)
    }

    fn set(&mut self, index: usize, value: bool) {
        self.blocks[index / self.block_bits].set(index % self.block_bits, value, self.block_bits);
    }

    fn resize(&mut self, block_bits: usize) {
        let mut next = Self {
            blocks: Vec::new(),
            block_bits,
            len: 0,
        };
        for i in 0..self.len {
            next.push(self.get(i));
        }
        *self = next;
    }

    fn push(&mut self, value: bool) {
        if self.len / self.block_bits >= self.block_bits {
            self.resize(self.block_bits * 2);
        }
        if self.len.is_multiple_of(self.block_bits) {
            self.blocks.push(BitBlock::new(self.block_bits));
        }
        self.set(self.len, value);
        self.len += 1;
    }

    fn insert(&mut self, index: usize, value: bool) {
        // Allocate/grow before locating the insertion block: growth changes B.
        self.push(false);
        let b = self.block_bits;
        let block = index / b;
        let offset = index % b;
        let used = b.min(self.len - block * b);
        let first = &mut self.blocks[block];
        let mut carry = first.get(used - 1, b);
        first.shift_right(offset, used, b);
        first.set(offset, value, b);
        for next in &mut self.blocks[block + 1..] {
            carry = next.push_front(carry, b);
        }
    }

    fn delete(&mut self, index: usize) {
        let b = self.block_bits;
        let block = index / b;
        let offset = index % b;
        let used = b.min(self.len - block * b);
        let first = &mut self.blocks[block];
        first.shift_left(offset, used, b);
        first.set(used - 1, false, b);
        for i in block + 1..self.blocks.len() {
            let carry = self.blocks[i].pop_front(b);
            self.blocks[i - 1].set(b - 1, carry, b);
        }
        self.len -= 1;
        if self.len.is_multiple_of(b) {
            self.blocks.pop();
        } else {
            self.set(self.len, false);
        }
        if b > 64 && self.len / b < b / 8 {
            self.resize(b / 2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(bits: &NullBitmap, expected: &[bool]) {
        assert_eq!(bits.len(), expected.len());
        for (i, value) in expected.iter().enumerate() {
            assert_eq!(
                bits.get(i),
                Some(*value),
                "index {i}, len {}",
                expected.len()
            );
        }
        assert_eq!(bits.get(expected.len()), None);
        if let NullBitmap::Array { words, len } = bits {
            assert_eq!(words.len(), len.div_ceil(64));
            if !len.is_multiple_of(64) {
                assert_eq!(words.last().unwrap() >> (len % 64), 0);
            }
        }
    }

    #[test]
    fn circular_word_reads_and_masked_writes_preserve_neighbors() {
        for size in [64, 128, 512] {
            for head in [0, 1, 31, 63, size - 1] {
                for start in [0, 1, 31, 63, 64, 65, size - 1] {
                    if start >= size {
                        continue;
                    }
                    for count in [1, 31, 63, 64] {
                        let count = count.min(size - start);
                        let mut block = BitBlock::new(size);
                        block.head = head;
                        let mut expected: Vec<bool> = (0..size).map(|i| i % 3 == 0).collect();
                        for (i, value) in expected.iter().enumerate() {
                            block.set(i, *value, size);
                        }
                        let word = block.read_word(start, size);
                        for bit in 0..64 {
                            assert_eq!((word >> bit) & 1 != 0, expected[(start + bit) % size]);
                        }
                        let value = 0xa5c3_196e_b740_d28fu64;
                        block.write_bits(start, count, value, size);
                        for bit in 0..count {
                            expected[start + bit] = (value >> bit) & 1 != 0;
                        }
                        for (i, value) in expected.iter().enumerate() {
                            assert_eq!(
                                block.get(i, size),
                                *value,
                                "size {size}, head {head}, start {start}, count {count}, bit {i}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn word_block_and_resize_boundaries() {
        for tiered in [false, true] {
            let mut bits = NullBitmap::new(tiered);
            let mut expected = Vec::new();
            for i in 0..17_000 {
                let value = i % 3 == 0;
                bits.push(value);
                expected.push(value);
            }
            check(&bits, &expected);
            for index in [0, 1, 63, 64, 65, 127, 128, 255, 4095, 4096, 16_383, 17_000] {
                bits.insert(index, true);
                expected.insert(index, true);
                check(&bits, &expected);
                bits.delete(index);
                expected.remove(index);
                check(&bits, &expected);
            }
            // Shrink through multiple tier-size changes, then reuse emptied storage.
            while !expected.is_empty() {
                let index = expected.len() / 2;
                bits.delete(index);
                expected.remove(index);
                if expected.len().is_multiple_of(127) {
                    check(&bits, &expected);
                }
            }
            check(&bits, &expected);
            for i in 0..5000 {
                bits.insert(0, i % 2 == 0);
                expected.insert(0, i % 2 == 0);
            }
            check(&bits, &expected);
        }
    }

    #[test]
    fn randomized_edits_match_boolean_vector() {
        for tiered in [false, true] {
            let mut bits = NullBitmap::new(tiered);
            let mut expected = Vec::new();
            let mut state = 0x12345678u64;
            for step in 0..25_000 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let value = state & 128 != 0;
                let index = (state >> 16) as usize % (expected.len() + 1);
                match state % 4 {
                    0 | 1 => {
                        bits.insert(index, value);
                        expected.insert(index, value);
                    }
                    2 if index < expected.len() => {
                        bits.delete(index);
                        expected.remove(index);
                    }
                    3 if index < expected.len() => {
                        bits.set(index, value);
                        expected[index] = value;
                    }
                    _ => {
                        bits.push(value);
                        expected.push(value);
                    }
                }
                if step % 137 == 0 {
                    check(&bits, &expected);
                }
            }
            check(&bits, &expected);
        }
    }
}
