//! LiveTable View Implementation
//!
//! Views are read-only derived tables that automatically propagate changes
//! from parent tables. Each view type lives in its own submodule; shared
//! key types used by joins and aggregates live here in the module root.

use crate::column::ColumnValue;
use std::collections::HashMap;

mod aggregate;
pub mod aggregate_support;
mod computed;
mod filter;
mod join;
mod projection;
mod sorted;
mod tickable;

pub use aggregate::AggregateView;
pub use aggregate_support::AggregateFunction;
pub use computed::ComputedView;
pub use filter::FilterView;
pub use join::{JoinType, JoinView};
pub use projection::ProjectionView;
pub use sorted::{SortKey, SortOrder, SortedView};
pub use tickable::TickableTable;

#[cfg(test)]
mod tests;

type RowPredicate = dyn Fn(&HashMap<String, ColumnValue>) -> bool;
type ComputeFunction = dyn Fn(&HashMap<String, ColumnValue>) -> ColumnValue;

/// Typed, hashable composite-key component for joins.
///
/// Replaces the previous String-based serialization (which used `format!("{:?}", v)`
/// for non-Int/String variants — not a stable format — and `\x00` separators,
/// which collided when string data contained `\x00`).
///
/// Float variants store IEEE 754 bit patterns so `Eq`/`Hash` derive cleanly.
/// Per SQL semantics, NaN never equals anything: builders return `None` for any
/// NaN-bearing row, which excludes it from joins.
///
/// Also reused by `aggregate_support::GroupKey`, which applies a different
/// float policy (NaN/-0.0 canonicalized instead of excluded — see there).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum JoinKeyPart {
    Int32(i32),
    Int64(i64),
    Float32Bits(u32),
    Float64Bits(u64),
    String(String),
    Bool(bool),
    Date(i32),
    DateTime(i64),
}

type JoinKey = Vec<JoinKeyPart>;

/// Convert a single ColumnValue into a JoinKeyPart. Returns None if the value
/// cannot participate in equality-based joins (NULL or NaN floats).
fn column_value_to_join_key_part(value: &ColumnValue) -> Option<JoinKeyPart> {
    match value {
        ColumnValue::Null => None,
        ColumnValue::Int32(v) => Some(JoinKeyPart::Int32(*v)),
        ColumnValue::Int64(v) => Some(JoinKeyPart::Int64(*v)),
        ColumnValue::Float32(v) if v.is_nan() => None,
        ColumnValue::Float32(v) => Some(JoinKeyPart::Float32Bits(v.to_bits())),
        ColumnValue::Float64(v) if v.is_nan() => None,
        ColumnValue::Float64(v) => Some(JoinKeyPart::Float64Bits(v.to_bits())),
        ColumnValue::String(s) => Some(JoinKeyPart::String(s.clone())),
        ColumnValue::Bool(b) => Some(JoinKeyPart::Bool(*b)),
        ColumnValue::Date(d) => Some(JoinKeyPart::Date(*d)),
        ColumnValue::DateTime(dt) => Some(JoinKeyPart::DateTime(*dt)),
    }
}

/// Same as `column_value_to_join_key_part` but consumes an owned value
/// (saves a String clone on the string fast path).
fn column_value_into_join_key_part(value: ColumnValue) -> Option<JoinKeyPart> {
    match value {
        ColumnValue::Null => None,
        ColumnValue::Int32(v) => Some(JoinKeyPart::Int32(v)),
        ColumnValue::Int64(v) => Some(JoinKeyPart::Int64(v)),
        ColumnValue::Float32(v) if v.is_nan() => None,
        ColumnValue::Float32(v) => Some(JoinKeyPart::Float32Bits(v.to_bits())),
        ColumnValue::Float64(v) if v.is_nan() => None,
        ColumnValue::Float64(v) => Some(JoinKeyPart::Float64Bits(v.to_bits())),
        ColumnValue::String(s) => Some(JoinKeyPart::String(s)),
        ColumnValue::Bool(b) => Some(JoinKeyPart::Bool(b)),
        ColumnValue::Date(d) => Some(JoinKeyPart::Date(d)),
        ColumnValue::DateTime(dt) => Some(JoinKeyPart::DateTime(dt)),
    }
}
