//! Type is selected once per column, not stored alongside every cell. Public
//! reads/writes still use ColumnValue; interned strings use only an ID buffer.
use super::{ColumnType, ColumnValue};
use crate::interner::StringId;
use crate::sequence::{ArraySequence, Sequence, TieredVectorSequence};
use std::fmt::Debug;

fn sequence<T: Clone>(tiered: bool) -> Buffer<T> {
    if tiered {
        Buffer::Tiered(TieredVectorSequence::new())
    } else {
        Buffer::Array(ArraySequence::new())
    }
}

// Concrete backend dispatch lets numeric access inline through the selected
// sequence. The public Sequence trait remains unchanged for other callers.
pub(super) enum Buffer<T: Clone> {
    Array(ArraySequence<T>),
    Tiered(TieredVectorSequence<T>),
}

macro_rules! with_buffer {
    ($this:expr, $seq:ident, $body:expr) => {
        match $this {
            Buffer::Array($seq) => $body,
            Buffer::Tiered($seq) => $body,
        }
    };
}

impl<T: Clone + Debug + Send> Sequence<T> for Buffer<T> {
    #[inline]
    fn len(&self) -> usize {
        with_buffer!(self, seq, seq.len())
    }
    #[inline]
    fn get(&self, index: usize) -> Result<T, String> {
        with_buffer!(self, seq, seq.get(index))
    }
    #[inline]
    fn get_ref(&self, index: usize) -> Option<&T> {
        with_buffer!(self, seq, seq.get_ref(index))
    }
    #[inline]
    fn set(&mut self, index: usize, value: T) -> Result<(), String> {
        with_buffer!(self, seq, seq.set(index, value))
    }
    #[inline]
    fn insert(&mut self, index: usize, value: T) -> Result<(), String> {
        with_buffer!(self, seq, seq.insert(index, value))
    }
    #[inline]
    fn delete(&mut self, index: usize) -> Result<T, String> {
        with_buffer!(self, seq, seq.delete(index))
    }
    #[inline]
    fn append(&mut self, value: T) {
        with_buffer!(self, seq, seq.append(value))
    }
    fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        with_buffer!(self, seq, seq.iter())
    }
}

macro_rules! typed_storage {
    ($($variant:ident: $ty:ty),+ $(,)?) => {
        pub(super) enum ColumnData {
            $($variant(Buffer<$ty>),)+
            StringIds(Buffer<StringId>),
        }

        impl ColumnData {
            pub(super) fn new(ty: ColumnType, tiered: bool, interned: bool) -> Self {
                if ty == ColumnType::String && interned {
                    return Self::StringIds(sequence(tiered));
                }
                match ty {
                    $(ColumnType::$variant => Self::$variant(sequence(tiered)),)+
                }
            }

            pub(super) fn len(&self) -> usize {
                match self {
                    $(Self::$variant(seq) => seq.len(),)+
                    Self::StringIds(seq) => seq.len(),
                }
            }

            pub(super) fn get(&self, index: usize) -> Result<ColumnValue, String> {
                match self {
                    $(Self::$variant(seq) => seq.get(index).map(ColumnValue::$variant),)+
                    Self::StringIds(_) => unreachable!("IDs must be resolved by the column"),
                }
            }

            // Callers validate type and bounds before entering these mutation
            // paths. Built-in sequences can then fail only on a broken invariant.
            pub(super) fn set(&mut self, index: usize, value: ColumnValue) {
                match (self, value) {
                    $((Self::$variant(seq), ColumnValue::$variant(value)) =>
                        seq.set(index, value).expect("column prevalidated index"),)+
                    _ => unreachable!("column prevalidated type"),
                }
            }

            pub(super) fn insert(&mut self, index: usize, value: ColumnValue) {
                match (self, value) {
                    $((Self::$variant(seq), ColumnValue::$variant(value)) =>
                        seq.insert(index, value).expect("column prevalidated index"),)+
                    _ => unreachable!("column prevalidated type"),
                }
            }

            pub(super) fn append(&mut self, value: ColumnValue) {
                match (self, value) {
                    $((Self::$variant(seq), ColumnValue::$variant(value)) => seq.append(value),)+
                    _ => unreachable!("column prevalidated type"),
                }
            }

            pub(super) fn delete(&mut self, index: usize) -> ColumnValue {
                match self {
                    $(Self::$variant(seq) => ColumnValue::$variant(
                        seq.delete(index).expect("column prevalidated index")
                    ),)+
                    Self::StringIds(_) => unreachable!("IDs must be released by the column"),
                }
            }
        }
    };
}

typed_storage!(
    Int32: i32,
    Int64: i64,
    Float32: f32,
    Float64: f64,
    String: String,
    Bool: bool,
    Date: i32,
    DateTime: i64,
);

impl ColumnData {
    #[inline]
    pub(super) fn get_f64(&self, index: usize) -> Option<f64> {
        match self {
            Self::Int32(seq) => seq.get_ref(index).map(|v| *v as f64),
            Self::Int64(seq) => seq.get_ref(index).map(|v| *v as f64),
            Self::Float32(seq) => seq.get_ref(index).map(|v| *v as f64),
            Self::Float64(seq) => seq.get_ref(index).copied(),
            // Dates, datetimes, and bools are intentionally not numeric here.
            _ => None,
        }
    }
}
