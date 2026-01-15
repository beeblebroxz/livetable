/// LiveTable - High-Performance Columnar Table System
///
/// A high-performance columnar table system with reactive views and incremental updates.
/// This implementation demonstrates high-performance tabular data structures with
/// strong type safety and zero-cost abstractions.

pub mod sequence;
pub mod column;
pub mod table;
pub mod view;
pub mod changeset;
pub mod interner;

pub use sequence::{ArraySequence, Sequence, TieredVectorSequence};
pub use column::{Column, ColumnType, ColumnValue};
pub use table::{Schema, Table};
pub use view::{FilterView, ProjectionView, ComputedView, JoinView, JoinType, SortedView, SortKey, SortOrder};
pub use changeset::{Changeset, TableChange, IncrementalView, IndexAdjuster};
pub use interner::{StringInterner, StringId, InternerStats};

// Python bindings - only when python feature is enabled
#[cfg(feature = "python")]
mod python_bindings;
#[cfg(feature = "python")]
pub use python_bindings::*;

// WebSocket server modules - only when server feature is enabled
#[cfg(feature = "server")]
pub mod messages;
#[cfg(feature = "server")]
pub mod websocket;
#[cfg(feature = "server")]
pub mod server;

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::rc::Rc;
    use std::cell::RefCell;
    use std::collections::HashMap;

    #[test]
    fn test_complete_workflow() {
        // Create a sales table
        let schema = Schema::new(vec![
            ("product".to_string(), ColumnType::String, false),
            ("quantity".to_string(), ColumnType::Int32, false),
            ("price".to_string(), ColumnType::Float64, false),
        ]);

        let table = Rc::new(RefCell::new(Table::new("sales".to_string(), schema)));

        // Add data
        {
            let mut t = table.borrow_mut();

            let mut row1 = HashMap::new();
            row1.insert("product".to_string(), ColumnValue::String("Widget".to_string()));
            row1.insert("quantity".to_string(), ColumnValue::Int32(10));
            row1.insert("price".to_string(), ColumnValue::Float64(9.99));
            t.append_row(row1).unwrap();

            let mut row2 = HashMap::new();
            row2.insert("product".to_string(), ColumnValue::String("Gadget".to_string()));
            row2.insert("quantity".to_string(), ColumnValue::Int32(5));
            row2.insert("price".to_string(), ColumnValue::Float64(19.99));
            t.append_row(row2).unwrap();

            let mut row3 = HashMap::new();
            row3.insert("product".to_string(), ColumnValue::String("Doohickey".to_string()));
            row3.insert("quantity".to_string(), ColumnValue::Int32(15));
            row3.insert("price".to_string(), ColumnValue::Float64(4.99));
            t.append_row(row3).unwrap();
        }

        // Create computed view with total
        let computed_view = Rc::new(RefCell::new(ComputedView::new(
            "sales_with_total".to_string(),
            table.clone(),
            "total".to_string(),
            |row| {
                let qty = match row.get("quantity") {
                    Some(ColumnValue::Int32(q)) => *q as f64,
                    _ => 0.0,
                };
                let price = match row.get("price") {
                    Some(ColumnValue::Float64(p)) => *p,
                    _ => 0.0,
                };
                ColumnValue::Float64(qty * price)
            },
        )));

        // Verify we have 3 rows with computed totals
        assert_eq!(computed_view.borrow().len(), 3);

        // Check computed values
        let total0 = computed_view.borrow().get_value(0, "total").unwrap().as_f64().unwrap();
        let total1 = computed_view.borrow().get_value(1, "total").unwrap().as_f64().unwrap();
        let total2 = computed_view.borrow().get_value(2, "total").unwrap().as_f64().unwrap();

        assert!((total0 - 99.90).abs() < 0.01); // Widget: 10 * 9.99 = 99.90
        assert!((total1 - 99.95).abs() < 0.01); // Gadget: 5 * 19.99 = 99.95
        assert!((total2 - 74.85).abs() < 0.01); // Doohickey: 15 * 4.99 = 74.85

        // Create a filter view on top of the computed view
        // Note: We'd need to refactor FilterView to work with ComputedView, not just Table
        // For now, test that the basic workflow works

        // Verify full row includes computed column
        let row = computed_view.borrow().get_row(0).unwrap();
        assert_eq!(row.get("product").unwrap().as_string(), Some("Widget"));
        assert_eq!(row.get("quantity").unwrap().as_i32(), Some(10));
        assert!((row.get("total").unwrap().as_f64().unwrap() - 99.90).abs() < 0.01);
    }
}
