use super::aggregate_support::ColumnAggState;
use super::*;
use crate::column::ColumnType;
use crate::table::{Schema, Table};
use std::cell::RefCell;
use std::rc::Rc;

// === ColumnAggState Percentile Tests ===

#[test]
fn test_column_agg_state_percentile() {
    let mut state = ColumnAggState::new(true); // needs_sorted = true
                                               // Values: 10, 20, 30, 40, 50
    for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
        state.add_value(v);
    }

    // Median (P50) of [10,20,30,40,50] = 30.0
    let median = state.percentile(0.5).unwrap();
    assert!((median - 30.0).abs() < 1e-9);

    // P0 = 10.0 (minimum)
    let p0 = state.percentile(0.0).unwrap();
    assert!((p0 - 10.0).abs() < 1e-9);

    // P100 = 50.0 (maximum)
    let p100 = state.percentile(1.0).unwrap();
    assert!((p100 - 50.0).abs() < 1e-9);

    // P25 = 20.0
    let p25 = state.percentile(0.25).unwrap();
    assert!((p25 - 20.0).abs() < 1e-9);

    // P75 = 40.0
    let p75 = state.percentile(0.75).unwrap();
    assert!((p75 - 40.0).abs() < 1e-9);
}

#[test]
fn test_column_agg_state_percentile_interpolation() {
    let mut state = ColumnAggState::new(true);
    // Even number of values: 10, 20, 30, 40
    for v in [10.0, 20.0, 30.0, 40.0] {
        state.add_value(v);
    }
    // Median of [10,20,30,40] = interpolation at index 1.5 = 25.0
    let median = state.percentile(0.5).unwrap();
    assert!((median - 25.0).abs() < 1e-9);
}

#[test]
fn test_column_agg_state_percentile_single_value() {
    let mut state = ColumnAggState::new(true);
    state.add_value(42.0);
    // Any percentile of a single value = that value
    assert!((state.percentile(0.0).unwrap() - 42.0).abs() < 1e-9);
    assert!((state.percentile(0.5).unwrap() - 42.0).abs() < 1e-9);
    assert!((state.percentile(1.0).unwrap() - 42.0).abs() < 1e-9);
}

#[test]
fn test_column_agg_state_percentile_empty() {
    let state = ColumnAggState::new(true);
    assert!(state.percentile(0.5).is_none());
}

#[test]
fn test_column_agg_state_no_sorted_when_not_needed() {
    let mut state = ColumnAggState::new(false); // needs_sorted = false
    state.add_value(10.0);
    assert!(state.sorted_values.is_none());
    assert!(state.percentile(0.5).is_none());
}

// === AggregateView Percentile Integration Tests ===

#[test]
fn test_aggregate_view_percentile() {
    let schema = Schema::new(vec![
        ("region".to_string(), ColumnType::String, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));
    {
        let mut t = table.borrow_mut();
        // North: 10, 20, 30, 40, 50  (median=30, p90=46)
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            let mut row = HashMap::new();
            row.insert(
                "region".to_string(),
                ColumnValue::String("North".to_string()),
            );
            row.insert("amount".to_string(), ColumnValue::Float64(v));
            t.append_row(row).unwrap();
        }
        // South: 100, 200  (median=150)
        for v in [100.0, 200.0] {
            let mut row = HashMap::new();
            row.insert(
                "region".to_string(),
                ColumnValue::String("South".to_string()),
            );
            row.insert("amount".to_string(), ColumnValue::Float64(v));
            t.append_row(row).unwrap();
        }
    }

    let agg = AggregateView::new(
        "by_region".to_string(),
        table.clone(),
        vec!["region".to_string()],
        vec![
            (
                "median_amount".to_string(),
                "amount".to_string(),
                AggregateFunction::Median,
            ),
            (
                "p90_amount".to_string(),
                "amount".to_string(),
                AggregateFunction::Percentile(0.9),
            ),
        ],
    )
    .unwrap();

    assert_eq!(agg.len(), 2);

    for i in 0..agg.len() {
        let row = agg.get_row(i).unwrap();
        match row.get("region").unwrap() {
            ColumnValue::String(s) if s == "North" => {
                let median = match row.get("median_amount").unwrap() {
                    ColumnValue::Float64(v) => *v,
                    _ => panic!("Expected Float64"),
                };
                assert!((median - 30.0).abs() < 1e-9);
            }
            ColumnValue::String(s) if s == "South" => {
                let median = match row.get("median_amount").unwrap() {
                    ColumnValue::Float64(v) => *v,
                    _ => panic!("Expected Float64"),
                };
                assert!((median - 150.0).abs() < 1e-9);
            }
            _ => panic!("Unexpected region"),
        }
    }
}

#[test]
fn test_aggregate_view_percentile_incremental() {
    let schema = Schema::new(vec![
        ("group".to_string(), ColumnType::String, false),
        ("val".to_string(), ColumnType::Float64, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));
    {
        let mut t = table.borrow_mut();
        for v in [10.0, 20.0, 30.0] {
            let mut row = HashMap::new();
            row.insert("group".to_string(), ColumnValue::String("A".to_string()));
            row.insert("val".to_string(), ColumnValue::Float64(v));
            t.append_row(row).unwrap();
        }
    }

    let mut agg = AggregateView::new(
        "test_agg".to_string(),
        table.clone(),
        vec!["group".to_string()],
        vec![(
            "median_val".to_string(),
            "val".to_string(),
            AggregateFunction::Median,
        )],
    )
    .unwrap();

    // Median of [10, 20, 30] = 20.0
    let row = agg.get_row(0).unwrap();
    let median = match row.get("median_val").unwrap() {
        ColumnValue::Float64(v) => *v,
        _ => panic!("Expected Float64"),
    };
    assert!((median - 20.0).abs() < 1e-9);

    // Add a value and sync
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("group".to_string(), ColumnValue::String("A".to_string()));
        row.insert("val".to_string(), ColumnValue::Float64(40.0));
        t.append_row(row).unwrap();
    }
    agg.sync();

    // Median of [10, 20, 30, 40] = 25.0
    let row = agg.get_row(0).unwrap();
    let median = match row.get("median_val").unwrap() {
        ColumnValue::Float64(v) => *v,
        _ => panic!("Expected Float64"),
    };
    assert!((median - 25.0).abs() < 1e-9);
}

// === Existing Tests ===

#[test]
fn test_filter_view() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("value".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    // Add some rows
    {
        let mut t = table.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("value".to_string(), ColumnValue::Int32(10));
        t.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("value".to_string(), ColumnValue::Int32(25));
        t.append_row(row2).unwrap();

        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), ColumnValue::Int32(3));
        row3.insert("value".to_string(), ColumnValue::Int32(30));
        t.append_row(row3).unwrap();
    }

    // Create filter view: value > 20
    let view = FilterView::new("filtered".to_string(), table.clone(), |row| {
        if let Some(ColumnValue::Int32(v)) = row.get("value") {
            *v > 20
        } else {
            false
        }
    });

    assert_eq!(view.len(), 2);
    assert_eq!(view.get_value(0, "id").unwrap().as_i32(), Some(2));
    assert_eq!(view.get_value(1, "id").unwrap().as_i32(), Some(3));
}

#[test]
fn test_filter_view_propagation() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("active".to_string(), ColumnType::Bool, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    // Add initial rows
    {
        let mut t = table.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("active".to_string(), ColumnValue::Bool(true));
        t.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("active".to_string(), ColumnValue::Bool(false));
        t.append_row(row2).unwrap();
    }

    let mut view = FilterView::new("active_only".to_string(), table.clone(), |row| {
        if let Some(ColumnValue::Bool(active)) = row.get("active") {
            *active
        } else {
            false
        }
    });

    assert_eq!(view.len(), 1);

    // Add another active row
    {
        let mut t = table.borrow_mut();
        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), ColumnValue::Int32(3));
        row3.insert("active".to_string(), ColumnValue::Bool(true));
        t.append_row(row3).unwrap();
    }

    // Refresh view to see new row
    view.refresh();
    assert_eq!(view.len(), 2);
}

#[test]
fn test_projection_view() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
        ("secret".to_string(), ColumnType::String, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row.insert(
            "secret".to_string(),
            ColumnValue::String("password123".to_string()),
        );
        t.append_row(row).unwrap();
    }

    // Create projection without secret column
    let view = ProjectionView::new(
        "public".to_string(),
        table.clone(),
        vec!["id".to_string(), "name".to_string()],
    )
    .unwrap();

    assert_eq!(view.len(), 1);

    let row = view.get_row(0).unwrap();
    assert_eq!(row.get("id").unwrap().as_i32(), Some(1));
    assert_eq!(row.get("name").unwrap().as_string(), Some("Alice"));
    assert!(!row.contains_key("secret")); // Secret column not in projection
}

#[test]
fn test_view_readonly() {
    let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    let view = FilterView::new("readonly".to_string(), table.clone(), |_| true);

    // Views don't have mutation methods - they're read-only by design
    // This test just verifies the view exists and works
    assert_eq!(view.len(), 0);
}

#[test]
fn test_computed_view() {
    let schema = Schema::new(vec![
        ("price".to_string(), ColumnType::Float64, false),
        ("quantity".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("sales".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("price".to_string(), ColumnValue::Float64(10.5));
        row.insert("quantity".to_string(), ColumnValue::Int32(3));
        t.append_row(row).unwrap();
    }

    // Create computed view with total column
    let view = ComputedView::new(
        "with_total".to_string(),
        table.clone(),
        "total".to_string(),
        |row| {
            let price = match row.get("price") {
                Some(ColumnValue::Float64(p)) => *p,
                _ => 0.0,
            };
            let qty = match row.get("quantity") {
                Some(ColumnValue::Int32(q)) => *q as f64,
                _ => 0.0,
            };
            ColumnValue::Float64(price * qty)
        },
    );

    assert_eq!(view.len(), 1);
    assert_eq!(view.get_value(0, "total").unwrap().as_f64(), Some(31.5));

    // Check full row includes computed column
    let row = view.get_row(0).unwrap();
    assert_eq!(row.get("price").unwrap().as_f64(), Some(10.5));
    assert_eq!(row.get("quantity").unwrap().as_i32(), Some(3));
    assert_eq!(row.get("total").unwrap().as_f64(), Some(31.5));
}

#[test]
fn test_left_join() {
    // Create users table
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));

    {
        let mut u = users.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert("user_id".to_string(), ColumnValue::Int32(1));
        row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        u.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("user_id".to_string(), ColumnValue::Int32(2));
        row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        u.append_row(row2).unwrap();

        let mut row3 = HashMap::new();
        row3.insert("user_id".to_string(), ColumnValue::Int32(3));
        row3.insert(
            "name".to_string(),
            ColumnValue::String("Charlie".to_string()),
        );
        u.append_row(row3).unwrap();
    }

    // Create orders table
    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new(
        "orders".to_string(),
        orders_schema,
    )));

    {
        let mut o = orders.borrow_mut();
        // Order for Alice
        let mut row1 = HashMap::new();
        row1.insert("order_id".to_string(), ColumnValue::Int32(101));
        row1.insert("user_id".to_string(), ColumnValue::Int32(1));
        row1.insert("amount".to_string(), ColumnValue::Float64(99.99));
        o.append_row(row1).unwrap();

        // Another order for Alice
        let mut row2 = HashMap::new();
        row2.insert("order_id".to_string(), ColumnValue::Int32(102));
        row2.insert("user_id".to_string(), ColumnValue::Int32(1));
        row2.insert("amount".to_string(), ColumnValue::Float64(49.99));
        o.append_row(row2).unwrap();

        // Order for Charlie
        let mut row3 = HashMap::new();
        row3.insert("order_id".to_string(), ColumnValue::Int32(103));
        row3.insert("user_id".to_string(), ColumnValue::Int32(3));
        row3.insert("amount".to_string(), ColumnValue::Float64(199.99));
        o.append_row(row3).unwrap();

        // Bob has no orders
    }

    // Left join users with orders
    let joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Left,
    )
    .unwrap();

    // Should have 4 rows: Alice (2 orders), Bob (0 orders = 1 row with nulls), Charlie (1 order)
    assert_eq!(joined.len(), 4);

    // Check Alice's first order
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    assert_eq!(row0.get("right_order_id").unwrap().as_i32(), Some(101));
    assert_eq!(row0.get("right_amount").unwrap().as_f64(), Some(99.99));

    // Check Alice's second order
    let row1 = joined.get_row(1).unwrap();
    assert_eq!(row1.get("name").unwrap().as_string(), Some("Alice"));
    assert_eq!(row1.get("right_order_id").unwrap().as_i32(), Some(102));

    // Check Bob (no orders - should have nulls)
    let row2 = joined.get_row(2).unwrap();
    assert_eq!(row2.get("name").unwrap().as_string(), Some("Bob"));
    assert!(row2.get("right_order_id").unwrap().is_null());
    assert!(row2.get("right_amount").unwrap().is_null());

    // Check Charlie's order
    let row3 = joined.get_row(3).unwrap();
    assert_eq!(row3.get("name").unwrap().as_string(), Some("Charlie"));
    assert_eq!(row3.get("right_order_id").unwrap().as_i32(), Some(103));
}

#[test]
fn test_inner_join() {
    // Create same tables as left join test
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));

    {
        let mut u = users.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert("user_id".to_string(), ColumnValue::Int32(1));
        row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        u.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("user_id".to_string(), ColumnValue::Int32(2));
        row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        u.append_row(row2).unwrap();
    }

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new(
        "orders".to_string(),
        orders_schema,
    )));

    {
        let mut o = orders.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert("order_id".to_string(), ColumnValue::Int32(101));
        row1.insert("user_id".to_string(), ColumnValue::Int32(1));
        row1.insert("amount".to_string(), ColumnValue::Float64(99.99));
        o.append_row(row1).unwrap();
    }

    // Inner join - only Alice should appear (Bob has no orders)
    let joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Inner,
    )
    .unwrap();

    assert_eq!(joined.len(), 1);

    let row = joined.get_row(0).unwrap();
    assert_eq!(row.get("name").unwrap().as_string(), Some("Alice"));
    assert_eq!(row.get("right_order_id").unwrap().as_i32(), Some(101));
}

#[test]
fn test_join_refresh() {
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));

    {
        let mut u = users.borrow_mut();
        let mut row = HashMap::new();
        row.insert("user_id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        u.append_row(row).unwrap();
    }

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new(
        "orders".to_string(),
        orders_schema,
    )));

    let mut joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Left,
    )
    .unwrap();

    // Initially, Alice has no orders (left join shows 1 row with nulls)
    assert_eq!(joined.len(), 1);

    // Add an order for Alice
    {
        let mut o = orders.borrow_mut();
        let mut row = HashMap::new();
        row.insert("order_id".to_string(), ColumnValue::Int32(101));
        row.insert("user_id".to_string(), ColumnValue::Int32(1));
        o.append_row(row).unwrap();
    }

    // Before refresh, still shows old data
    assert_eq!(joined.len(), 1);

    // After refresh, should show the new order
    joined.refresh();
    assert_eq!(joined.len(), 1);

    let row = joined.get_row(0).unwrap();
    assert_eq!(row.get("right_order_id").unwrap().as_i32(), Some(101));
}

#[test]
fn test_join_sync_advances_cursors_and_is_idempotent() {
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    users
        .borrow_mut()
        .append_row(HashMap::from([
            ("user_id".to_string(), ColumnValue::Int32(1)),
            ("name".to_string(), ColumnValue::String("Alice".to_string())),
        ]))
        .unwrap();

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new(
        "orders".to_string(),
        orders_schema,
    )));

    let mut joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Left,
    )
    .unwrap();

    orders
        .borrow_mut()
        .append_row(HashMap::from([
            ("order_id".to_string(), ColumnValue::Int32(101)),
            ("user_id".to_string(), ColumnValue::Int32(1)),
        ]))
        .unwrap();

    assert!(joined.sync());
    assert_eq!(joined.len(), 1);
    assert_eq!(
        joined.get_value(0, "right_order_id").unwrap().as_i32(),
        Some(101)
    );

    let first_cursors = joined.last_processed_change_count();
    assert!(!joined.sync());
    assert_eq!(joined.len(), 1);
    assert_eq!(joined.last_processed_change_count(), first_cursors);
}

#[test]
fn test_join_sync_preserves_full_rebuild_order_on_left_insert() {
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    {
        let mut users_ref = users.borrow_mut();
        users_ref
            .append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(1)),
                ("name".to_string(), ColumnValue::String("Alice".to_string())),
            ]))
            .unwrap();
        users_ref
            .append_row(HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(2)),
                ("name".to_string(), ColumnValue::String("Bob".to_string())),
            ]))
            .unwrap();
    }

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new(
        "orders".to_string(),
        orders_schema,
    )));
    {
        let mut orders_ref = orders.borrow_mut();
        orders_ref
            .append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(101)),
                ("user_id".to_string(), ColumnValue::Int32(1)),
            ]))
            .unwrap();
        orders_ref
            .append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(301)),
                ("user_id".to_string(), ColumnValue::Int32(3)),
            ]))
            .unwrap();
        orders_ref
            .append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(201)),
                ("user_id".to_string(), ColumnValue::Int32(2)),
            ]))
            .unwrap();
    }

    let mut joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Inner,
    )
    .unwrap();

    users
        .borrow_mut()
        .insert_row(
            1,
            HashMap::from([
                ("user_id".to_string(), ColumnValue::Int32(3)),
                ("name".to_string(), ColumnValue::String("Carol".to_string())),
            ]),
        )
        .unwrap();

    assert!(joined.sync());
    let incremental_rows: Vec<(String, i32)> = (0..joined.len())
        .map(|idx| {
            let row = joined.get_row(idx).unwrap();
            (
                row.get("name").unwrap().as_string().unwrap().to_string(),
                row.get("right_order_id").unwrap().as_i32().unwrap(),
            )
        })
        .collect();

    joined.refresh();
    let rebuilt_rows: Vec<(String, i32)> = (0..joined.len())
        .map(|idx| {
            let row = joined.get_row(idx).unwrap();
            (
                row.get("name").unwrap().as_string().unwrap().to_string(),
                row.get("right_order_id").unwrap().as_i32().unwrap(),
            )
        })
        .collect();

    assert_eq!(incremental_rows, rebuilt_rows);
    assert_eq!(
        incremental_rows,
        vec![
            ("Alice".to_string(), 101),
            ("Carol".to_string(), 301),
            ("Bob".to_string(), 201),
        ]
    );
}

#[test]
fn test_join_sync_preserves_full_rebuild_order_on_right_insert() {
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    users
        .borrow_mut()
        .append_row(HashMap::from([
            ("user_id".to_string(), ColumnValue::Int32(1)),
            ("name".to_string(), ColumnValue::String("Alice".to_string())),
        ]))
        .unwrap();

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new(
        "orders".to_string(),
        orders_schema,
    )));
    {
        let mut orders_ref = orders.borrow_mut();
        orders_ref
            .append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(101)),
                ("user_id".to_string(), ColumnValue::Int32(1)),
            ]))
            .unwrap();
        orders_ref
            .append_row(HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(301)),
                ("user_id".to_string(), ColumnValue::Int32(1)),
            ]))
            .unwrap();
    }

    let mut joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Inner,
    )
    .unwrap();

    orders
        .borrow_mut()
        .insert_row(
            1,
            HashMap::from([
                ("order_id".to_string(), ColumnValue::Int32(201)),
                ("user_id".to_string(), ColumnValue::Int32(1)),
            ]),
        )
        .unwrap();

    assert!(joined.sync());
    let incremental_order_ids: Vec<i32> = (0..joined.len())
        .map(|idx| {
            joined
                .get_value(idx, "right_order_id")
                .unwrap()
                .as_i32()
                .unwrap()
        })
        .collect();

    joined.refresh();
    let rebuilt_order_ids: Vec<i32> = (0..joined.len())
        .map(|idx| {
            joined
                .get_value(idx, "right_order_id")
                .unwrap()
                .as_i32()
                .unwrap()
        })
        .collect();

    assert_eq!(incremental_order_ids, rebuilt_order_ids);
    assert_eq!(incremental_order_ids, vec![101, 201, 301]);
}

// === Incremental Propagation Tests ===

#[test]
fn test_filter_view_incremental_insert() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("value".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    // Add initial rows
    {
        let mut t = table.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("value".to_string(), ColumnValue::Int32(10));
        t.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("value".to_string(), ColumnValue::Int32(30));
        t.append_row(row2).unwrap();
    }

    // Create filter view: value > 20
    let mut view = FilterView::new("filtered".to_string(), table.clone(), |row| {
        if let Some(ColumnValue::Int32(v)) = row.get("value") {
            *v > 20
        } else {
            false
        }
    });

    // Clear initial changes so we can test incremental
    table.borrow_mut().clear_changeset();

    assert_eq!(view.len(), 1); // Only row with value=30

    // Add a new row that matches the filter
    {
        let mut t = table.borrow_mut();
        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), ColumnValue::Int32(3));
        row3.insert("value".to_string(), ColumnValue::Int32(50));
        t.append_row(row3).unwrap();
    }

    // Use incremental sync
    let changed = view.sync();
    assert!(changed);
    assert_eq!(view.len(), 2);
    assert_eq!(view.get_value(1, "id").unwrap().as_i32(), Some(3));

    // Add a row that doesn't match the filter
    table.borrow_mut().clear_changeset();
    {
        let mut t = table.borrow_mut();
        let mut row4 = HashMap::new();
        row4.insert("id".to_string(), ColumnValue::Int32(4));
        row4.insert("value".to_string(), ColumnValue::Int32(15)); // < 20
        t.append_row(row4).unwrap();
    }

    view.sync();
    assert_eq!(view.len(), 2); // Still 2, new row didn't match
}

#[test]
fn test_filter_view_incremental_delete() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("value".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    // Add rows
    {
        let mut t = table.borrow_mut();
        for i in 1..=5 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            row.insert("value".to_string(), ColumnValue::Int32(i * 10));
            t.append_row(row).unwrap();
        }
    }

    // Filter: value > 20 (rows 3, 4, 5)
    let mut view = FilterView::new("filtered".to_string(), table.clone(), |row| {
        if let Some(ColumnValue::Int32(v)) = row.get("value") {
            *v > 20
        } else {
            false
        }
    });

    table.borrow_mut().clear_changeset();
    assert_eq!(view.len(), 3);

    // Delete row at index 2 (id=3, value=30) - this is in the filter
    {
        table.borrow_mut().delete_row(2).unwrap();
    }

    view.sync();
    assert_eq!(view.len(), 2); // Now only rows 4 and 5 remain
}

#[test]
fn test_filter_view_incremental_update() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("value".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("value".to_string(), ColumnValue::Int32(10)); // < 20
        t.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("value".to_string(), ColumnValue::Int32(30)); // > 20
        t.append_row(row2).unwrap();
    }

    let mut view = FilterView::new("filtered".to_string(), table.clone(), |row| {
        if let Some(ColumnValue::Int32(v)) = row.get("value") {
            *v > 20
        } else {
            false
        }
    });

    table.borrow_mut().clear_changeset();
    assert_eq!(view.len(), 1);

    // Update row 0's value to 25 (now matches filter)
    {
        table
            .borrow_mut()
            .set_value(0, "value", ColumnValue::Int32(25))
            .unwrap();
    }

    view.sync();
    assert_eq!(view.len(), 2); // Both rows now match

    // Update row 1's value to 15 (no longer matches filter)
    table.borrow_mut().clear_changeset();
    {
        table
            .borrow_mut()
            .set_value(1, "value", ColumnValue::Int32(15))
            .unwrap();
    }

    view.sync();
    assert_eq!(view.len(), 1); // Only row 0 matches now
}

#[test]
fn test_table_changeset_tracking() {
    let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);

    let mut table = Table::new("test".to_string(), schema);

    // Initially no changes
    assert!(!table.has_pending_changes());
    assert_eq!(table.changeset_generation(), 0);

    // Append creates a change
    let mut row = HashMap::new();
    row.insert("id".to_string(), ColumnValue::Int32(1));
    table.append_row(row).unwrap();

    assert!(table.has_pending_changes());
    assert_eq!(table.changeset().len(), 1);

    // Clear changeset
    table.clear_changeset();
    assert!(!table.has_pending_changes());
    assert_eq!(table.changeset_generation(), 1);

    // Update creates a change
    table.set_value(0, "id", ColumnValue::Int32(2)).unwrap();
    assert!(table.has_pending_changes());

    // Drain returns changes and increments generation
    let changes = table.drain_changes();
    assert_eq!(changes.len(), 1);
    assert!(!table.has_pending_changes());
    assert_eq!(table.changeset_generation(), 2);
}

// === SortedView Tests ===

#[test]
fn test_sorted_view_basic() {
    let schema = Schema::new(vec![
        ("name".to_string(), ColumnType::String, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert(
            "name".to_string(),
            ColumnValue::String("Charlie".to_string()),
        );
        row1.insert("score".to_string(), ColumnValue::Int32(75));
        t.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row2.insert("score".to_string(), ColumnValue::Int32(92));
        t.append_row(row2).unwrap();

        let mut row3 = HashMap::new();
        row3.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        row3.insert("score".to_string(), ColumnValue::Int32(85));
        t.append_row(row3).unwrap();
    }

    // Sort by name ascending
    let sorted = SortedView::new(
        "by_name".to_string(),
        table.clone(),
        vec![SortKey::ascending("name")],
    )
    .unwrap();

    assert_eq!(sorted.len(), 3);
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Bob")
    );
    assert_eq!(
        sorted.get_value(2, "name").unwrap().as_string(),
        Some("Charlie")
    );
}

#[test]
fn test_sorted_view_descending() {
    let schema = Schema::new(vec![
        ("name".to_string(), ColumnType::String, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row1.insert("score".to_string(), ColumnValue::Int32(75));
        t.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        row2.insert("score".to_string(), ColumnValue::Int32(92));
        t.append_row(row2).unwrap();

        let mut row3 = HashMap::new();
        row3.insert(
            "name".to_string(),
            ColumnValue::String("Charlie".to_string()),
        );
        row3.insert("score".to_string(), ColumnValue::Int32(85));
        t.append_row(row3).unwrap();
    }

    // Sort by score descending (highest first)
    let sorted = SortedView::new(
        "by_score_desc".to_string(),
        table.clone(),
        vec![SortKey::descending("score")],
    )
    .unwrap();

    assert_eq!(sorted.len(), 3);
    assert_eq!(sorted.get_value(0, "score").unwrap().as_i32(), Some(92)); // Bob
    assert_eq!(sorted.get_value(1, "score").unwrap().as_i32(), Some(85)); // Charlie
    assert_eq!(sorted.get_value(2, "score").unwrap().as_i32(), Some(75)); // Alice
}

#[test]
fn test_sorted_view_multi_column() {
    let schema = Schema::new(vec![
        ("department".to_string(), ColumnType::String, false),
        ("name".to_string(), ColumnType::String, false),
        ("salary".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("employees".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        // Engineering - Alice
        let mut row = HashMap::new();
        row.insert(
            "department".to_string(),
            ColumnValue::String("Engineering".to_string()),
        );
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row.insert("salary".to_string(), ColumnValue::Int32(100000));
        t.append_row(row).unwrap();

        // Sales - Bob
        let mut row = HashMap::new();
        row.insert(
            "department".to_string(),
            ColumnValue::String("Sales".to_string()),
        );
        row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        row.insert("salary".to_string(), ColumnValue::Int32(80000));
        t.append_row(row).unwrap();

        // Engineering - Charlie
        let mut row = HashMap::new();
        row.insert(
            "department".to_string(),
            ColumnValue::String("Engineering".to_string()),
        );
        row.insert(
            "name".to_string(),
            ColumnValue::String("Charlie".to_string()),
        );
        row.insert("salary".to_string(), ColumnValue::Int32(90000));
        t.append_row(row).unwrap();

        // Sales - Diana
        let mut row = HashMap::new();
        row.insert(
            "department".to_string(),
            ColumnValue::String("Sales".to_string()),
        );
        row.insert("name".to_string(), ColumnValue::String("Diana".to_string()));
        row.insert("salary".to_string(), ColumnValue::Int32(85000));
        t.append_row(row).unwrap();
    }

    // Sort by department (asc), then by salary (desc)
    let sorted = SortedView::new(
        "by_dept_salary".to_string(),
        table.clone(),
        vec![
            SortKey::ascending("department"),
            SortKey::descending("salary"),
        ],
    )
    .unwrap();

    assert_eq!(sorted.len(), 4);

    // Engineering first (Alice 100k, then Charlie 90k)
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Charlie")
    );

    // Sales second (Diana 85k, then Bob 80k)
    assert_eq!(
        sorted.get_value(2, "name").unwrap().as_string(),
        Some("Diana")
    );
    assert_eq!(
        sorted.get_value(3, "name").unwrap().as_string(),
        Some("Bob")
    );
}

#[test]
fn test_sorted_view_with_nulls() {
    let schema = Schema::new(vec![
        ("name".to_string(), ColumnType::String, false),
        ("age".to_string(), ColumnType::Int32, true), // nullable
    ]);

    let table = Rc::new(RefCell::new(Table::new("people".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row.insert("age".to_string(), ColumnValue::Int32(30));
        t.append_row(row).unwrap();

        let mut row = HashMap::new();
        row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        row.insert("age".to_string(), ColumnValue::Null);
        t.append_row(row).unwrap();

        let mut row = HashMap::new();
        row.insert(
            "name".to_string(),
            ColumnValue::String("Charlie".to_string()),
        );
        row.insert("age".to_string(), ColumnValue::Int32(25));
        t.append_row(row).unwrap();
    }

    // Sort by age ascending (nulls last by default)
    let sorted = SortedView::new(
        "by_age".to_string(),
        table.clone(),
        vec![SortKey::ascending("age")],
    )
    .unwrap();

    assert_eq!(sorted.len(), 3);
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Charlie")
    ); // 25
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Alice")
    ); // 30
    assert_eq!(
        sorted.get_value(2, "name").unwrap().as_string(),
        Some("Bob")
    ); // null

    // Sort by age ascending (nulls first)
    let sorted_nulls_first = SortedView::new(
        "by_age_nulls_first".to_string(),
        table.clone(),
        vec![SortKey::new("age", SortOrder::Ascending, true)],
    )
    .unwrap();

    assert_eq!(
        sorted_nulls_first.get_value(0, "name").unwrap().as_string(),
        Some("Bob")
    ); // null
    assert_eq!(
        sorted_nulls_first.get_value(1, "name").unwrap().as_string(),
        Some("Charlie")
    ); // 25
    assert_eq!(
        sorted_nulls_first.get_value(2, "name").unwrap().as_string(),
        Some("Alice")
    ); // 30
}

#[test]
fn test_sorted_view_incremental_insert() {
    let schema = Schema::new(vec![
        ("name".to_string(), ColumnType::String, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        row.insert("score".to_string(), ColumnValue::Int32(85));
        t.append_row(row).unwrap();

        let mut row = HashMap::new();
        row.insert("name".to_string(), ColumnValue::String("Diana".to_string()));
        row.insert("score".to_string(), ColumnValue::Int32(95));
        t.append_row(row).unwrap();
    }

    let mut sorted = SortedView::new(
        "by_name".to_string(),
        table.clone(),
        vec![SortKey::ascending("name")],
    )
    .unwrap();

    table.borrow_mut().clear_changeset();
    assert_eq!(sorted.len(), 2);
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Bob")
    );
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Diana")
    );

    // Add Alice (should go first alphabetically)
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row.insert("score".to_string(), ColumnValue::Int32(92));
        t.append_row(row).unwrap();
    }

    let changed = sorted.sync();
    assert!(changed);
    assert_eq!(sorted.len(), 3);
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Bob")
    );
    assert_eq!(
        sorted.get_value(2, "name").unwrap().as_string(),
        Some("Diana")
    );

    // Add Charlie (should go between Bob and Diana)
    table.borrow_mut().clear_changeset();
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert(
            "name".to_string(),
            ColumnValue::String("Charlie".to_string()),
        );
        row.insert("score".to_string(), ColumnValue::Int32(80));
        t.append_row(row).unwrap();
    }

    sorted.sync();
    assert_eq!(sorted.len(), 4);
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Bob")
    );
    assert_eq!(
        sorted.get_value(2, "name").unwrap().as_string(),
        Some("Charlie")
    );
    assert_eq!(
        sorted.get_value(3, "name").unwrap().as_string(),
        Some("Diana")
    );
}

#[test]
fn test_sorted_view_incremental_delete() {
    let schema = Schema::new(vec![
        ("name".to_string(), ColumnType::String, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        for (name, score) in [("Alice", 92), ("Bob", 85), ("Charlie", 80), ("Diana", 95)] {
            let mut row = HashMap::new();
            row.insert("name".to_string(), ColumnValue::String(name.to_string()));
            row.insert("score".to_string(), ColumnValue::Int32(score));
            t.append_row(row).unwrap();
        }
    }

    let mut sorted = SortedView::new(
        "by_name".to_string(),
        table.clone(),
        vec![SortKey::ascending("name")],
    )
    .unwrap();

    table.borrow_mut().clear_changeset();
    assert_eq!(sorted.len(), 4);

    // Delete Bob (parent index 1)
    table.borrow_mut().delete_row(1).unwrap();

    sorted.sync();
    assert_eq!(sorted.len(), 3);
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Charlie")
    );
    assert_eq!(
        sorted.get_value(2, "name").unwrap().as_string(),
        Some("Diana")
    );
}

#[test]
fn test_sorted_view_incremental_update() {
    let schema = Schema::new(vec![
        ("name".to_string(), ColumnType::String, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("students".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row.insert("score".to_string(), ColumnValue::Int32(70));
        t.append_row(row).unwrap();

        let mut row = HashMap::new();
        row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        row.insert("score".to_string(), ColumnValue::Int32(80));
        t.append_row(row).unwrap();

        let mut row = HashMap::new();
        row.insert(
            "name".to_string(),
            ColumnValue::String("Charlie".to_string()),
        );
        row.insert("score".to_string(), ColumnValue::Int32(90));
        t.append_row(row).unwrap();
    }

    // Sort by score ascending
    let mut sorted = SortedView::new(
        "by_score".to_string(),
        table.clone(),
        vec![SortKey::ascending("score")],
    )
    .unwrap();

    table.borrow_mut().clear_changeset();

    // Initial order: Alice (70), Bob (80), Charlie (90)
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Alice")
    );
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Bob")
    );
    assert_eq!(
        sorted.get_value(2, "name").unwrap().as_string(),
        Some("Charlie")
    );

    // Update Alice's score to 95 (should move to end)
    table
        .borrow_mut()
        .set_value(0, "score", ColumnValue::Int32(95))
        .unwrap();

    sorted.sync();

    // New order: Bob (80), Charlie (90), Alice (95)
    assert_eq!(
        sorted.get_value(0, "name").unwrap().as_string(),
        Some("Bob")
    );
    assert_eq!(
        sorted.get_value(1, "name").unwrap().as_string(),
        Some("Charlie")
    );
    assert_eq!(
        sorted.get_value(2, "name").unwrap().as_string(),
        Some("Alice")
    );
}

#[test]
fn test_sorted_view_parent_index() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("value".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        // Parent indices: 0=100, 1=50, 2=75
        for (id, value) in [(1, 100), (2, 50), (3, 75)] {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(id));
            row.insert("value".to_string(), ColumnValue::Int32(value));
            t.append_row(row).unwrap();
        }
    }

    let sorted = SortedView::new(
        "by_value".to_string(),
        table.clone(),
        vec![SortKey::ascending("value")],
    )
    .unwrap();

    // Sorted order by value: 50 (parent 1), 75 (parent 2), 100 (parent 0)
    assert_eq!(sorted.get_parent_index(0), Some(1)); // 50
    assert_eq!(sorted.get_parent_index(1), Some(2)); // 75
    assert_eq!(sorted.get_parent_index(2), Some(0)); // 100
    assert_eq!(sorted.get_parent_index(3), None); // out of range
}

#[test]
fn test_sorted_view_empty_table() {
    let schema = Schema::new(vec![("name".to_string(), ColumnType::String, false)]);

    let table = Rc::new(RefCell::new(Table::new("empty".to_string(), schema)));

    let sorted = SortedView::new(
        "sorted_empty".to_string(),
        table.clone(),
        vec![SortKey::ascending("name")],
    )
    .unwrap();

    assert_eq!(sorted.len(), 0);
    assert!(sorted.is_empty());
}

#[test]
fn test_sorted_view_invalid_column() {
    let schema = Schema::new(vec![("name".to_string(), ColumnType::String, false)]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    let result = SortedView::new(
        "invalid".to_string(),
        table.clone(),
        vec![SortKey::ascending("nonexistent")],
    );

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));
}

#[test]
fn test_sorted_view_no_sort_keys() {
    let schema = Schema::new(vec![("name".to_string(), ColumnType::String, false)]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    let result = SortedView::new("invalid".to_string(), table.clone(), vec![]);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("At least one sort key"));
}

// === Changeset Compaction Tests ===

#[test]
fn test_filter_view_sync_incremental_with_cursor() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("value".to_string(), ColumnType::Int32, false),
    ]);
    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    // Add initial rows
    {
        let mut t = table.borrow_mut();
        for i in 0..3 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            row.insert("value".to_string(), ColumnValue::Int32(i * 10));
            t.append_row(row).unwrap();
        }
    }

    let mut view = FilterView::new("filtered".to_string(), table.clone(), |row| {
        row.get("value")
            .and_then(|v| v.as_i32())
            .map(|v| v >= 10)
            .unwrap_or(false)
    });

    assert_eq!(view.len(), 2); // rows with value 10, 20
    let initial_cursor = view.last_processed_change_count();
    assert_eq!(initial_cursor, 3); // Processed 3 initial inserts

    // Add a new matching row
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(3));
        row.insert("value".to_string(), ColumnValue::Int32(30));
        t.append_row(row).unwrap();
    }

    // Sync should process only the new change
    let modified = view.sync();
    assert!(modified);
    assert_eq!(view.len(), 3);
    assert_eq!(view.last_processed_change_count(), 4);
}

#[test]
fn test_filter_view_sync_fallback_to_rebuild() {
    let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);
    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    // Add rows
    {
        let mut t = table.borrow_mut();
        for i in 0..3 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            t.append_row(row).unwrap();
        }
    }

    let mut view = FilterView::new("all".to_string(), table.clone(), |_| true);
    assert_eq!(view.len(), 3);

    // Compact all changes away
    table.borrow_mut().compact_changeset(100);

    // Add more rows
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(10));
        t.append_row(row).unwrap();
    }

    // Sync should fallback to rebuild (returns true)
    let modified = view.sync();
    assert!(modified);
    assert_eq!(view.len(), 4);
}

#[test]
fn test_sorted_view_sync_with_cursor() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);
    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    // Add initial rows
    {
        let mut t = table.borrow_mut();
        for (id, score) in [(1, 50), (2, 30), (3, 70)] {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(id));
            row.insert("score".to_string(), ColumnValue::Int32(score));
            t.append_row(row).unwrap();
        }
    }

    let mut view = SortedView::new(
        "sorted".to_string(),
        table.clone(),
        vec![SortKey::descending("score")],
    )
    .unwrap();

    assert_eq!(view.len(), 3);
    assert_eq!(view.get_value(0, "score").unwrap().as_i32(), Some(70));
    let initial_cursor = view.last_processed_change_count();

    // Add a new highest score
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(4));
        row.insert("score".to_string(), ColumnValue::Int32(100));
        t.append_row(row).unwrap();
    }

    let modified = view.sync();
    assert!(modified);
    assert_eq!(view.len(), 4);
    assert_eq!(view.get_value(0, "score").unwrap().as_i32(), Some(100));
    assert!(view.last_processed_change_count() > initial_cursor);
}

#[test]
fn test_sorted_view_sync_fallback_to_rebuild() {
    let schema = Schema::new(vec![("value".to_string(), ColumnType::Int32, false)]);
    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        for v in [30, 10, 20] {
            let mut row = HashMap::new();
            row.insert("value".to_string(), ColumnValue::Int32(v));
            t.append_row(row).unwrap();
        }
    }

    let mut view = SortedView::new(
        "sorted".to_string(),
        table.clone(),
        vec![SortKey::ascending("value")],
    )
    .unwrap();

    assert_eq!(view.get_value(0, "value").unwrap().as_i32(), Some(10));

    // Compact changes away
    table.borrow_mut().compact_changeset(100);

    // Add new row
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("value".to_string(), ColumnValue::Int32(5));
        t.append_row(row).unwrap();
    }

    // Sync falls back to rebuild
    let modified = view.sync();
    assert!(modified);
    assert_eq!(view.len(), 4);
    assert_eq!(view.get_value(0, "value").unwrap().as_i32(), Some(5));
}

#[test]
fn test_multiple_syncs_accumulate_correctly() {
    let schema = Schema::new(vec![("value".to_string(), ColumnType::Int32, false)]);
    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("value".to_string(), ColumnValue::Int32(10));
        t.append_row(row).unwrap();
    }

    let mut view = FilterView::new("all".to_string(), table.clone(), |_| true);
    assert_eq!(view.len(), 1);
    assert_eq!(view.last_processed_change_count(), 1);

    // Add row and sync
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("value".to_string(), ColumnValue::Int32(20));
        t.append_row(row).unwrap();
    }
    view.sync();
    assert_eq!(view.len(), 2);
    assert_eq!(view.last_processed_change_count(), 2);

    // Add another row and sync again
    {
        let mut t = table.borrow_mut();
        let mut row = HashMap::new();
        row.insert("value".to_string(), ColumnValue::Int32(30));
        t.append_row(row).unwrap();
    }
    view.sync();
    assert_eq!(view.len(), 3);
    assert_eq!(view.last_processed_change_count(), 3);
}

#[test]
fn test_view_sync_after_partial_compaction() {
    let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);
    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));

    // Add 5 rows
    {
        let mut t = table.borrow_mut();
        for i in 0..5 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            t.append_row(row).unwrap();
        }
    }

    let mut view = FilterView::new("all".to_string(), table.clone(), |_| true);
    assert_eq!(view.len(), 5);
    assert_eq!(view.last_processed_change_count(), 5);

    // Compact first 3 changes (view has already processed them)
    table.borrow_mut().compact_changeset(3);

    // Add more rows
    {
        let mut t = table.borrow_mut();
        for i in 5..8 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ColumnValue::Int32(i));
            t.append_row(row).unwrap();
        }
    }

    // Sync should still work because view's cursor (5) >= base_index (3)
    let modified = view.sync();
    assert!(modified);
    assert_eq!(view.len(), 8);
    assert_eq!(view.last_processed_change_count(), 8);
}

// === RIGHT and FULL OUTER Join Tests ===

#[test]
fn test_right_join() {
    // Users: Alice/1, Bob/2, Charlie/3
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    {
        let mut u = users.borrow_mut();
        u.append_row(HashMap::from([
            ("user_id".to_string(), ColumnValue::Int32(1)),
            ("name".to_string(), ColumnValue::String("Alice".to_string())),
        ]))
        .unwrap();
        u.append_row(HashMap::from([
            ("user_id".to_string(), ColumnValue::Int32(2)),
            ("name".to_string(), ColumnValue::String("Bob".to_string())),
        ]))
        .unwrap();
        u.append_row(HashMap::from([
            ("user_id".to_string(), ColumnValue::Int32(3)),
            (
                "name".to_string(),
                ColumnValue::String("Charlie".to_string()),
            ),
        ]))
        .unwrap();
    }

    // Orders: 101/1 (Alice), 102/4 (Dave — no matching user)
    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new(
        "orders".to_string(),
        orders_schema,
    )));
    {
        let mut o = orders.borrow_mut();
        o.append_row(HashMap::from([
            ("order_id".to_string(), ColumnValue::Int32(101)),
            ("user_id".to_string(), ColumnValue::Int32(1)),
        ]))
        .unwrap();
        o.append_row(HashMap::from([
            ("order_id".to_string(), ColumnValue::Int32(102)),
            ("user_id".to_string(), ColumnValue::Int32(4)),
        ]))
        .unwrap();
    }

    let joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Right,
    )
    .unwrap();

    // RIGHT JOIN: all right rows. Alice matched (order 101), Dave unmatched (order 102).
    assert_eq!(joined.len(), 2);

    // Row 0: Alice matched with order 101
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    assert_eq!(row0.get("right_order_id").unwrap().as_i32(), Some(101));

    // Row 1: Unmatched right row (order 102, user_id=4) — left columns are NULL
    let row1 = joined.get_row(1).unwrap();
    assert!(row1.get("name").unwrap().is_null());
    assert!(row1.get("user_id").unwrap().is_null());
    assert_eq!(row1.get("right_order_id").unwrap().as_i32(), Some(102));
    assert_eq!(row1.get("right_user_id").unwrap().as_i32(), Some(4));
}

#[test]
fn test_full_join() {
    // Users: Alice/1, Bob/2
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    {
        let mut u = users.borrow_mut();
        u.append_row(HashMap::from([
            ("user_id".to_string(), ColumnValue::Int32(1)),
            ("name".to_string(), ColumnValue::String("Alice".to_string())),
        ]))
        .unwrap();
        u.append_row(HashMap::from([
            ("user_id".to_string(), ColumnValue::Int32(2)),
            ("name".to_string(), ColumnValue::String("Bob".to_string())),
        ]))
        .unwrap();
    }

    // Orders: 101/1 (Alice), 102/4 (Dave — no matching user)
    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new(
        "orders".to_string(),
        orders_schema,
    )));
    {
        let mut o = orders.borrow_mut();
        o.append_row(HashMap::from([
            ("order_id".to_string(), ColumnValue::Int32(101)),
            ("user_id".to_string(), ColumnValue::Int32(1)),
        ]))
        .unwrap();
        o.append_row(HashMap::from([
            ("order_id".to_string(), ColumnValue::Int32(102)),
            ("user_id".to_string(), ColumnValue::Int32(4)),
        ]))
        .unwrap();
    }

    let joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // FULL JOIN: 3 rows — Alice matched, Bob unmatched left, Dave unmatched right
    assert_eq!(joined.len(), 3);

    // Row 0: Alice matched with order 101
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    assert_eq!(row0.get("right_order_id").unwrap().as_i32(), Some(101));

    // Row 1: Bob unmatched (left only)
    let row1 = joined.get_row(1).unwrap();
    assert_eq!(row1.get("name").unwrap().as_string(), Some("Bob"));
    assert!(row1.get("right_order_id").unwrap().is_null());

    // Row 2: Dave unmatched (right only, user_id=4)
    let row2 = joined.get_row(2).unwrap();
    assert!(row2.get("name").unwrap().is_null());
    assert_eq!(row2.get("right_order_id").unwrap().as_i32(), Some(102));
    assert_eq!(row2.get("right_user_id").unwrap().as_i32(), Some(4));
}

#[test]
fn test_right_join_multiple_matches() {
    // Two left rows with same key, one right row
    let left_schema = Schema::new(vec![
        ("key".to_string(), ColumnType::Int32, false),
        ("val".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut l = left.borrow_mut();
        l.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(1)),
            ("val".to_string(), ColumnValue::String("A".to_string())),
        ]))
        .unwrap();
        l.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(1)),
            ("val".to_string(), ColumnValue::String("B".to_string())),
        ]))
        .unwrap();
    }

    let right_schema = Schema::new(vec![
        ("key".to_string(), ColumnType::Int32, false),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut r = right.borrow_mut();
        r.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(1)),
            ("data".to_string(), ColumnValue::String("X".to_string())),
        ]))
        .unwrap();
    }

    let joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "key".to_string(),
        "key".to_string(),
        JoinType::Right,
    )
    .unwrap();

    // RIGHT JOIN: one right row matches two left rows -> 2 result rows
    assert_eq!(joined.len(), 2);

    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("val").unwrap().as_string(), Some("A"));
    assert_eq!(row0.get("right_data").unwrap().as_string(), Some("X"));

    let row1 = joined.get_row(1).unwrap();
    assert_eq!(row1.get("val").unwrap().as_string(), Some("B"));
    assert_eq!(row1.get("right_data").unwrap().as_string(), Some("X"));
}

#[test]
fn test_full_join_no_matches() {
    // Disjoint keys: left has {1,2}, right has {3,4}
    let left_schema = Schema::new(vec![
        ("key".to_string(), ColumnType::Int32, false),
        ("lval".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut l = left.borrow_mut();
        l.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(1)),
            ("lval".to_string(), ColumnValue::String("A".to_string())),
        ]))
        .unwrap();
        l.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(2)),
            ("lval".to_string(), ColumnValue::String("B".to_string())),
        ]))
        .unwrap();
    }

    let right_schema = Schema::new(vec![
        ("key".to_string(), ColumnType::Int32, false),
        ("rval".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut r = right.borrow_mut();
        r.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(3)),
            ("rval".to_string(), ColumnValue::String("X".to_string())),
        ]))
        .unwrap();
        r.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(4)),
            ("rval".to_string(), ColumnValue::String("Y".to_string())),
        ]))
        .unwrap();
    }

    let joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "key".to_string(),
        "key".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // FULL JOIN with disjoint keys: 4 rows, all cross-columns are NULL
    assert_eq!(joined.len(), 4);

    // Left rows first (unmatched)
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("lval").unwrap().as_string(), Some("A"));
    assert!(row0.get("right_rval").unwrap().is_null());

    let row1 = joined.get_row(1).unwrap();
    assert_eq!(row1.get("lval").unwrap().as_string(), Some("B"));
    assert!(row1.get("right_rval").unwrap().is_null());

    // Right rows after (unmatched)
    let row2 = joined.get_row(2).unwrap();
    assert!(row2.get("lval").unwrap().is_null());
    assert_eq!(row2.get("right_rval").unwrap().as_string(), Some("X"));

    let row3 = joined.get_row(3).unwrap();
    assert!(row3.get("lval").unwrap().is_null());
    assert_eq!(row3.get("right_rval").unwrap().as_string(), Some("Y"));
}

#[test]
fn test_full_join_null_key_rows() {
    // Both tables have NULL-key rows. FULL -> 3 rows
    // Left: (1, "Alice"), (NULL, "Ghost")
    // Right: (1, "Order1"), (NULL, "Phantom")
    let left_schema = Schema::new(vec![
        ("key".to_string(), ColumnType::Int32, true),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut l = left.borrow_mut();
        l.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(1)),
            ("name".to_string(), ColumnValue::String("Alice".to_string())),
        ]))
        .unwrap();
        l.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Null),
            ("name".to_string(), ColumnValue::String("Ghost".to_string())),
        ]))
        .unwrap();
    }

    let right_schema = Schema::new(vec![
        ("key".to_string(), ColumnType::Int32, true),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut r = right.borrow_mut();
        r.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Int32(1)),
            (
                "data".to_string(),
                ColumnValue::String("Order1".to_string()),
            ),
        ]))
        .unwrap();
        r.append_row(HashMap::from([
            ("key".to_string(), ColumnValue::Null),
            (
                "data".to_string(),
                ColumnValue::String("Phantom".to_string()),
            ),
        ]))
        .unwrap();
    }

    let joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "key".to_string(),
        "key".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // FULL JOIN: 3 rows
    // - Alice matched with Order1: (Some(0), Some(0))
    // - Ghost (NULL key, unmatched left): (Some(1), None)
    // - Phantom (NULL key, unmatched right): (None, Some(1))
    assert_eq!(joined.len(), 3);

    // Row 0: Alice matched
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    assert_eq!(row0.get("right_data").unwrap().as_string(), Some("Order1"));

    // Row 1: Ghost (NULL key, left only)
    let row1 = joined.get_row(1).unwrap();
    assert_eq!(row1.get("name").unwrap().as_string(), Some("Ghost"));
    assert!(row1.get("right_data").unwrap().is_null());

    // Row 2: Phantom (NULL key, right only)
    let row2 = joined.get_row(2).unwrap();
    assert!(row2.get("name").unwrap().is_null());
    assert_eq!(row2.get("right_data").unwrap().as_string(), Some("Phantom"));
}

// === RIGHT/FULL JOIN incremental sync tests ===

/// Helper: collect join_index from a JoinView by reading rows and extracting
/// a comparable tuple for each row. Returns Vec of (Option<left_key>, Option<right_key>).
fn collect_join_rows(joined: &JoinView) -> Vec<(Option<i32>, Option<i32>)> {
    (0..joined.len())
        .map(|idx| {
            let row = joined.get_row(idx).unwrap();
            let left_id = match row.get("id").unwrap() {
                ColumnValue::Null => None,
                v => Some(v.as_i32().unwrap()),
            };
            let right_id = match row.get("right_id").unwrap() {
                ColumnValue::Null => None,
                v => Some(v.as_i32().unwrap()),
            };
            (left_id, right_id)
        })
        .collect()
}

fn make_left_table() -> Rc<RefCell<Table>> {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    Rc::new(RefCell::new(Table::new("left".to_string(), schema)))
}

fn make_right_table() -> Rc<RefCell<Table>> {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("val".to_string(), ColumnType::String, false),
    ]);
    Rc::new(RefCell::new(Table::new("right".to_string(), schema)))
}

fn left_row(id: i32, name: &str) -> HashMap<String, ColumnValue> {
    HashMap::from([
        ("id".to_string(), ColumnValue::Int32(id)),
        ("name".to_string(), ColumnValue::String(name.to_string())),
    ])
}

fn right_row(id: i32, val: &str) -> HashMap<String, ColumnValue> {
    HashMap::from([
        ("id".to_string(), ColumnValue::Int32(id)),
        ("val".to_string(), ColumnValue::String(val.to_string())),
    ])
}

#[test]
fn test_right_join_sync_left_insert() {
    // Start: empty left, 2 right rows. RIGHT JOIN => 2 unmatched right rows.
    // Insert left row matching right id=1. Sync. Verify matches rebuild.
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
        r.append_row(right_row(2, "R2")).unwrap();
    }

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    )
    .unwrap();

    // Initial state: 2 unmatched right rows
    assert_eq!(joined.len(), 2);

    // Insert left row matching right id=1
    left.borrow_mut().append_row(left_row(1, "L1")).unwrap();
    assert!(joined.sync());

    let synced = collect_join_rows(&joined);

    // Rebuild from scratch and compare
    joined.refresh();
    let rebuilt = collect_join_rows(&joined);

    assert_eq!(synced, rebuilt);
    // Expected: (Some(1), Some(1)) matched, then (None, Some(2)) unmatched right
    assert_eq!(synced, vec![(Some(1), Some(1)), (None, Some(2))]);
}

#[test]
fn test_right_join_sync_right_insert() {
    // Start: 1 left row (id=1), empty right. RIGHT JOIN => empty.
    // Insert 2 right rows: id=1 (matching), id=99 (not matching). Sync.
    let left = make_left_table();
    let right = make_right_table();
    left.borrow_mut().append_row(left_row(1, "L1")).unwrap();

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    )
    .unwrap();

    assert_eq!(joined.len(), 0);

    // Insert matching and non-matching right rows
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
        r.append_row(right_row(99, "R99")).unwrap();
    }
    assert!(joined.sync());

    let synced = collect_join_rows(&joined);

    joined.refresh();
    let rebuilt = collect_join_rows(&joined);

    assert_eq!(synced, rebuilt);
    // Expected: (Some(1), Some(1)) matched, then (None, Some(99)) unmatched right
    assert_eq!(synced, vec![(Some(1), Some(1)), (None, Some(99))]);
}

#[test]
fn test_full_join_sync_left_insert() {
    // Start: empty left, 1 right row (id=5). FULL JOIN => 1 unmatched right row.
    // Insert 2 left rows: id=5 (matching), id=10 (not matching). Sync.
    let left = make_left_table();
    let right = make_right_table();
    right.borrow_mut().append_row(right_row(5, "R5")).unwrap();

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // Initial: 1 unmatched right row (None, Some(0))
    assert_eq!(joined.len(), 1);

    // Insert 2 left rows
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(5, "L5")).unwrap();
        l.append_row(left_row(10, "L10")).unwrap();
    }
    assert!(joined.sync());

    let synced = collect_join_rows(&joined);

    joined.refresh();
    let rebuilt = collect_join_rows(&joined);

    assert_eq!(synced, rebuilt);
    // Expected: (Some(5), Some(5)) matched, (Some(10), None) unmatched left
    assert_eq!(synced, vec![(Some(5), Some(5)), (Some(10), None)]);
}

#[test]
fn test_full_join_sync_right_insert() {
    // Start: 1 left row (id=3), empty right. FULL JOIN => 1 unmatched left row.
    // Insert 2 right rows: id=3 (matching), id=7 (not matching). Sync.
    let left = make_left_table();
    let right = make_right_table();
    left.borrow_mut().append_row(left_row(3, "L3")).unwrap();

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // Initial: 1 unmatched left row (Some(0), None)
    assert_eq!(joined.len(), 1);

    // Insert matching and non-matching right rows
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(3, "R3")).unwrap();
        r.append_row(right_row(7, "R7")).unwrap();
    }
    assert!(joined.sync());

    let synced = collect_join_rows(&joined);

    joined.refresh();
    let rebuilt = collect_join_rows(&joined);

    assert_eq!(synced, rebuilt);
    // Expected: (Some(3), Some(3)) matched, then (None, Some(7)) unmatched right
    assert_eq!(synced, vec![(Some(3), Some(3)), (None, Some(7))]);
}

#[test]
fn test_full_join_unmatched_becomes_matched() {
    // Start: left(id=1), right(id=2) — no overlap.
    // FULL JOIN has 2 rows: (Some(0), None) and (None, Some(0)).
    // Insert left(id=2) which matches the previously-unmatched right row.
    // Sync. Verify (None, Some(0)) is replaced with (Some(1), Some(0)).
    let left = make_left_table();
    let right = make_right_table();
    left.borrow_mut().append_row(left_row(1, "L1")).unwrap();
    right.borrow_mut().append_row(right_row(2, "R2")).unwrap();

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // Initial: 2 rows — (Some(0), None) unmatched left, (None, Some(0)) unmatched right
    assert_eq!(joined.len(), 2);

    // Insert left row that matches the right row's id=2
    left.borrow_mut().append_row(left_row(2, "L2")).unwrap();
    assert!(joined.sync());

    let synced = collect_join_rows(&joined);

    joined.refresh();
    let rebuilt = collect_join_rows(&joined);

    assert_eq!(synced, rebuilt);
    // Expected: (Some(1), None) for left id=1, (Some(2), Some(2)) for matched pair
    assert_eq!(synced, vec![(Some(1), None), (Some(2), Some(2))]);
}

#[test]
fn test_right_join_rebuild_after_delete() {
    // RIGHT JOIN, delete from left, sync triggers rebuild. Verify correct result.
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
        l.append_row(left_row(2, "L2")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
        r.append_row(right_row(3, "R3")).unwrap();
    }

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    )
    .unwrap();

    // Initial: (Some(0), Some(0)) for id=1 matched, (None, Some(1)) for id=3 unmatched
    assert_eq!(joined.len(), 2);

    // Delete left row id=1 (index 0)
    left.borrow_mut().delete_row(0).unwrap();
    assert!(joined.sync()); // Should trigger rebuild

    let result = collect_join_rows(&joined);
    // After delete: left has only id=2, right has id=1 and id=3
    // RIGHT JOIN: both right rows are unmatched
    assert_eq!(result, vec![(None, Some(1)), (None, Some(3))]);
}

#[test]
fn test_full_join_rebuild_after_delete() {
    // FULL JOIN, delete from right, sync triggers rebuild. Verify correct result.
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
        l.append_row(left_row(2, "L2")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(2, "R2")).unwrap();
        r.append_row(right_row(3, "R3")).unwrap();
    }

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // Initial: (Some(0), None) for left id=1 unmatched,
    //          (Some(1), Some(0)) for id=2 matched,
    //          (None, Some(1)) for right id=3 unmatched
    assert_eq!(joined.len(), 3);

    // Delete right row id=2 (index 0)
    right.borrow_mut().delete_row(0).unwrap();
    assert!(joined.sync()); // Should trigger rebuild

    let result = collect_join_rows(&joined);
    // After delete: left has id=1 and id=2, right has only id=3
    // FULL JOIN: left id=1 unmatched, left id=2 unmatched, right id=3 unmatched
    assert_eq!(
        result,
        vec![(Some(1), None), (Some(2), None), (None, Some(3))]
    );
}

// === Incremental left-delete regression tests (fix #9) ===

/// INNER join, delete left row with a match. Entry removed; following
/// left indices shift down by 1.
#[test]
fn test_inner_join_incremental_left_delete() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
        l.append_row(left_row(2, "L2")).unwrap();
        l.append_row(left_row(3, "L3")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
        r.append_row(right_row(2, "R2")).unwrap();
        r.append_row(right_row(3, "R3")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(joined.len(), 3);

    // Delete left row id=2 (parent index 1) — the middle match
    left.borrow_mut().delete_row(1).unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // After delete: left has id=1, id=3; right unchanged
    // INNER: id=1 ↔ id=1, id=3 ↔ id=3 (id=2 was shifted down to where id=3 was)
    assert_eq!(result, vec![(Some(1), Some(1)), (Some(3), Some(3))]);
}

/// LEFT join, delete a left row that had a None-right placeholder.
/// Placeholder removed; following left indices shift down.
#[test]
fn test_left_join_incremental_left_delete_placeholder() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
        l.append_row(left_row(2, "L2")).unwrap(); // no right match
        l.append_row(left_row(3, "L3")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
        r.append_row(right_row(3, "R3")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Left,
    )
    .unwrap();
    assert_eq!(joined.len(), 3); // (1,1), (2,None), (3,3)

    // Delete left row id=2 (parent index 1) — the placeholder
    left.borrow_mut().delete_row(1).unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // After delete: left has id=1, id=3; right has id=1, id=3
    // LEFT: id=1 ↔ id=1, id=3 ↔ id=3 (id=3 was parent_idx 2, now 1)
    assert_eq!(result, vec![(Some(1), Some(1)), (Some(3), Some(3))]);
}

/// LEFT join, delete a left row with one of two matches to the same right
/// row. The right row is NOT orphaned because another left still matches.
#[test]
fn test_right_join_incremental_left_delete_no_orphan_when_other_left_matches() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1a")).unwrap();
        l.append_row(left_row(1, "L1b")).unwrap(); // both match right id=1
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    )
    .unwrap();
    assert_eq!(joined.len(), 2); // (1, 1), (1, 1) — same right id=1

    // Delete first left row
    left.borrow_mut().delete_row(0).unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // After delete: left has id=1 (was L1b), right has id=1
    // RIGHT JOIN: should be (1, 1) — NOT orphaned because the remaining
    // left row still matches the right.
    assert_eq!(result, vec![(Some(1), Some(1))]);
}

/// LEFT join, delete a right row that was the only match for a left row.
/// The left row should resurrect as (Some(left_idx), None) placeholder
/// at its sorted position.
#[test]
fn test_left_join_incremental_right_delete_resurrects_orphan() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
        l.append_row(left_row(2, "L2")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
        r.append_row(right_row(2, "R2")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Left,
    )
    .unwrap();
    assert_eq!(joined.len(), 2); // (1,1), (2,2)

    // Delete right row id=2 (parent index 1) — was the only match for left id=2
    right.borrow_mut().delete_row(1).unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // Left id=2 must resurrect as (Some(1), None) at its sorted position
    assert_eq!(result, vec![(Some(1), Some(1)), (Some(2), None)]);
}

// === Incremental left-key-update regression tests (fix #9) ===

/// INNER join, change a left row's key to one that matches a different
/// right row. Old match removed; new match inserted.
#[test]
fn test_inner_join_incremental_left_key_update_to_new_match() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
        r.append_row(right_row(2, "R2")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(joined.len(), 1); // left id=1 ↔ right id=1

    // Change left id from 1 to 2 — should match the OTHER right row
    left.borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(2))
        .unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    assert_eq!(result, vec![(Some(2), Some(2))]);
}

/// LEFT join, change a left key so no right matches. Result becomes
/// (Some(left_idx), None) placeholder. RIGHT/FULL would also resurrect
/// the previously-matched right row as orphan — covered by next test.
#[test]
fn test_left_join_incremental_left_key_update_to_no_match() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Left,
    )
    .unwrap();
    assert_eq!(joined.len(), 1);

    // Change left id to 99 — no right with id=99
    left.borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(99))
        .unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // LEFT: row appears as (Some(99), None) placeholder
    assert_eq!(result, vec![(Some(99), None)]);
}

/// FULL join, change a left key. Previous right match becomes orphan;
/// new left key matches a different right row.
#[test]
fn test_full_join_incremental_left_key_update_orphan_and_match() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
        r.append_row(right_row(2, "R2")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    )
    .unwrap();
    // Initial: (Some(0), Some(0)) for id=1, (None, Some(1)) for id=2 unmatched
    assert_eq!(joined.len(), 2);

    // Change left id from 1 to 2
    left.borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(2))
        .unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // Right id=1 (was matched) now orphaned; left now matches right id=2
    // (which removes the orphan placeholder it had)
    assert_eq!(result, vec![(Some(2), Some(2)), (None, Some(1))]);
}

// === Incremental right-key-update regression tests (fix #9) ===

/// INNER join, change a right row's key to match a different left row.
/// Old match removed; new match inserted.
#[test]
fn test_inner_join_incremental_right_key_update_to_new_match() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
        l.append_row(left_row(2, "L2")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(joined.len(), 1); // left id=1 ↔ right id=1

    // Change right id from 1 to 2 — should now match left id=2
    right
        .borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(2))
        .unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    assert_eq!(result, vec![(Some(2), Some(2))]);
}

/// FULL join, change a right key so its previous left match becomes orphan
/// (resurrects as Some(l), None placeholder), and the new key matches
/// nothing (the right itself appears as a (None, Some) orphan).
#[test]
fn test_full_join_incremental_right_key_update_creates_two_orphans() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    )
    .unwrap();
    assert_eq!(joined.len(), 1); // (1, 1)

    // Change right id from 1 to 99 — no left has id=99
    right
        .borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(99))
        .unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // FULL: left id=1 resurrects as (Some(1), None); right id=99 becomes (None, Some(99))
    assert_eq!(result, vec![(Some(1), None), (None, Some(99))]);
}

// === Rust-side tick() registry tests (fix #10) ===

/// Native Rust callers can register a FilterView with a TickableTable
/// wrapper and have tick() auto-sync it after mutations — no manual
/// sync() calls needed. Mirrors what PyTable already does for Python users.
#[test]
fn test_table_tick_propagates_to_filter_view() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("v".to_string(), ColumnType::Int32, false),
    ]);
    let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
    let tickable = TickableTable::new(table.clone());

    // Seed 2 rows
    for &(id, v) in &[(1, 10), (2, 20)] {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(id));
        row.insert("v".to_string(), ColumnValue::Int32(v));
        table.borrow_mut().append_row(row).unwrap();
    }

    let view = Rc::new(RefCell::new(FilterView::new(
        "f".to_string(),
        table.clone(),
        |row| row.get("v").and_then(|v| v.as_i32()).unwrap_or(0) >= 15,
    )));
    tickable.register_filter(&view);

    assert_eq!(view.borrow().len(), 1);

    // Append a matching row WITHOUT calling sync() manually.
    let mut row = HashMap::new();
    row.insert("id".to_string(), ColumnValue::Int32(3));
    row.insert("v".to_string(), ColumnValue::Int32(30));
    table.borrow_mut().append_row(row).unwrap();

    let synced = tickable.tick();
    assert!(synced >= 1, "tick should have synced at least one view");
    assert_eq!(view.borrow().len(), 2);
}

/// Registry must drop dead Weak references so dropped views don't leak.
#[test]
fn test_table_tick_prunes_dropped_views() {
    let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);
    let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
    let tickable = TickableTable::new(table.clone());

    {
        let view = Rc::new(RefCell::new(FilterView::new(
            "f".to_string(),
            table.clone(),
            |_| true,
        )));
        tickable.register_filter(&view);
        assert_eq!(tickable.registered_view_count(), 1);
        // view drops at end of block
    }

    // Mutate so tick has something to do
    let mut row = HashMap::new();
    row.insert("id".to_string(), ColumnValue::Int32(1));
    table.borrow_mut().append_row(row).unwrap();

    // tick() should prune the dead Weak and return 0 syncs
    assert_eq!(tickable.tick(), 0);
    assert_eq!(tickable.registered_view_count(), 0);
}

/// SortedView via TickableTable: register, mutate parent, tick — sorted
/// position of the new row must reflect in the view without any manual
/// sync() call.
#[test]
fn test_table_tick_propagates_to_sorted_view() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);
    let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
    let tickable = TickableTable::new(table.clone());

    for &(id, score) in &[(1, 50), (2, 30), (3, 70)] {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(id));
        row.insert("score".to_string(), ColumnValue::Int32(score));
        table.borrow_mut().append_row(row).unwrap();
    }

    let view = Rc::new(RefCell::new(
        SortedView::new(
            "s".to_string(),
            table.clone(),
            vec![SortKey::descending("score")],
        )
        .unwrap(),
    ));
    tickable.register_sorted(&view);

    assert_eq!(view.borrow().len(), 3);
    // Initial DESC order by score: 70, 50, 30
    assert_eq!(
        view.borrow().get_value(0, "score").unwrap().as_i32(),
        Some(70)
    );

    // Append a row that should sort to the very top
    let mut row = HashMap::new();
    row.insert("id".to_string(), ColumnValue::Int32(4));
    row.insert("score".to_string(), ColumnValue::Int32(100));
    table.borrow_mut().append_row(row).unwrap();

    let synced = tickable.tick();
    assert!(synced >= 1);
    assert_eq!(view.borrow().len(), 4);
    // New row (score=100) must be at view index 0 after auto-sync.
    assert_eq!(
        view.borrow().get_value(0, "score").unwrap().as_i32(),
        Some(100)
    );
}

/// AggregateView via TickableTable: register, mutate parent, tick —
/// aggregate state must reflect the new row.
#[test]
fn test_table_tick_propagates_to_aggregate_view() {
    let schema = Schema::new(vec![
        ("region".to_string(), ColumnType::String, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);
    let table = Rc::new(RefCell::new(Table::new("sales".to_string(), schema)));
    let tickable = TickableTable::new(table.clone());

    for &(region, amount) in &[("North", 100.0_f64), ("South", 200.0), ("North", 150.0)] {
        let mut row = HashMap::new();
        row.insert(
            "region".to_string(),
            ColumnValue::String(region.to_string()),
        );
        row.insert("amount".to_string(), ColumnValue::Float64(amount));
        table.borrow_mut().append_row(row).unwrap();
    }

    let view = Rc::new(RefCell::new(
        AggregateView::new(
            "by_region".to_string(),
            table.clone(),
            vec!["region".to_string()],
            vec![(
                "total".to_string(),
                "amount".to_string(),
                AggregateFunction::Sum,
            )],
        )
        .unwrap(),
    ));
    tickable.register_aggregate(&view);

    assert_eq!(view.borrow().len(), 2); // 2 groups: North, South

    // Append a row in a NEW region — should add a third group on next tick.
    let mut row = HashMap::new();
    row.insert(
        "region".to_string(),
        ColumnValue::String("West".to_string()),
    );
    row.insert("amount".to_string(), ColumnValue::Float64(99.0));
    table.borrow_mut().append_row(row).unwrap();

    let synced = tickable.tick();
    assert!(synced >= 1);
    assert_eq!(view.borrow().len(), 3); // 3 groups now: North, South, West
}

/// Heterogeneous registry: a Filter, a Sorted, AND an Aggregate on the
/// SAME table — one tick() syncs all three. Verifies the
/// `Box<dyn FnMut>`-based registry isn't accidentally specialized to
/// one view type.
#[test]
fn test_table_tick_with_mixed_view_types() {
    let schema = Schema::new(vec![
        ("region".to_string(), ColumnType::String, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);
    let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
    let tickable = TickableTable::new(table.clone());

    for &(region, score) in &[("A", 10), ("B", 50), ("A", 30)] {
        let mut row = HashMap::new();
        row.insert(
            "region".to_string(),
            ColumnValue::String(region.to_string()),
        );
        row.insert("score".to_string(), ColumnValue::Int32(score));
        table.borrow_mut().append_row(row).unwrap();
    }

    let filter = Rc::new(RefCell::new(FilterView::new(
        "f".to_string(),
        table.clone(),
        |row| row.get("score").and_then(|v| v.as_i32()).unwrap_or(0) >= 20,
    )));
    let sorted = Rc::new(RefCell::new(
        SortedView::new(
            "s".to_string(),
            table.clone(),
            vec![SortKey::descending("score")],
        )
        .unwrap(),
    ));
    let aggregate = Rc::new(RefCell::new(
        AggregateView::new(
            "a".to_string(),
            table.clone(),
            vec!["region".to_string()],
            vec![(
                "total".to_string(),
                "score".to_string(),
                AggregateFunction::Sum,
            )],
        )
        .unwrap(),
    ));

    tickable.register_filter(&filter);
    tickable.register_sorted(&sorted);
    tickable.register_aggregate(&aggregate);

    assert_eq!(tickable.registered_view_count(), 3);
    assert_eq!(filter.borrow().len(), 2); // scores >= 20: {50, 30}
    assert_eq!(sorted.borrow().len(), 3);
    assert_eq!(aggregate.borrow().len(), 2); // groups A, B

    // Single mutation that affects all three views distinctly:
    // - filter: new row (score=80) matches → len 2 → 3
    // - sorted: new row joins → len 3 → 4, sorted to top (score=80 highest)
    // - aggregate: still 2 regions (B), but B's sum changes
    let mut row = HashMap::new();
    row.insert("region".to_string(), ColumnValue::String("B".to_string()));
    row.insert("score".to_string(), ColumnValue::Int32(80));
    table.borrow_mut().append_row(row).unwrap();

    let synced = tickable.tick();
    assert_eq!(synced, 3, "all 3 registered views should sync in one tick");

    assert_eq!(filter.borrow().len(), 3);
    assert_eq!(sorted.borrow().len(), 4);
    assert_eq!(
        sorted.borrow().get_value(0, "score").unwrap().as_i32(),
        Some(80)
    );
    assert_eq!(aggregate.borrow().len(), 2);
}

/// tick() must call compact_changeset(min_cursor) so memory does not
/// grow unbounded across long-running streams. Verify by reading the
/// changeset's base_index and pending length directly.
#[test]
fn test_table_tick_compacts_changeset() {
    let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);
    let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
    let tickable = TickableTable::new(table.clone());

    // Register a view so tick has something to advance the cursor for.
    let view = Rc::new(RefCell::new(FilterView::new(
        "f".to_string(),
        table.clone(),
        |_| true,
    )));
    tickable.register_filter(&view);

    for i in 0..5 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(i));
        table.borrow_mut().append_row(row).unwrap();
    }

    // Before tick: 5 pending changes, base_index=0 (none compacted yet).
    assert_eq!(table.borrow().changeset().len(), 5);
    assert_eq!(table.borrow().changeset().base_index(), 0);
    assert_eq!(table.borrow().changeset().total_len(), 5);

    let synced = tickable.tick();
    assert!(synced >= 1);

    // After tick: changes all processed; compaction must have advanced
    // base_index to 5, leaving the pending vec empty. total_len is
    // still 5 (it's monotonic across compactions).
    assert_eq!(table.borrow().changeset().len(), 0);
    assert_eq!(table.borrow().changeset().base_index(), 5);
    assert_eq!(table.borrow().changeset().total_len(), 5);
}

/// JoinView must be registerable on BOTH parent TickableTables; tick on
/// either parent must propagate to the join. Mirrors the JoinLeft/
/// JoinRight variants on PyTable.
#[test]
fn test_table_tick_propagates_to_join_view_from_both_parents() {
    let left = make_left_table();
    let right = make_right_table();
    let left_tickable = TickableTable::new(left.clone());
    let right_tickable = TickableTable::new(right.clone());

    // Seed initial match: left id=1 ↔ right id=1
    left.borrow_mut().append_row(left_row(1, "L1")).unwrap();
    right.borrow_mut().append_row(right_row(1, "R1")).unwrap();

    let join = Rc::new(RefCell::new(
        JoinView::new(
            "j".to_string(),
            left.clone(),
            right.clone(),
            "id".to_string(),
            "id".to_string(),
            JoinType::Inner,
        )
        .unwrap(),
    ));
    left_tickable.register_join_as_left(&join);
    right_tickable.register_join_as_right(&join);

    assert_eq!(join.borrow().len(), 1);

    // Append on BOTH sides; the frame-mixing guard in sync() handles this
    // batch via rebuild — a single tick must add the new match exactly once.
    left.borrow_mut().append_row(left_row(2, "L2")).unwrap();
    right.borrow_mut().append_row(right_row(2, "R2")).unwrap();
    assert!(left_tickable.tick() >= 1);
    // Right tick may be a no-op (changes already consumed via left tick)
    // or sync 1 (if right's changeset still has the pending insert) —
    // both are fine. The size invariant is what matters.
    right_tickable.tick();
    assert_eq!(join.borrow().len(), 2);
}

// === Incremental JoinView edge cases (fix #9 — extended coverage) ===

/// 1:N delete on RIGHT join: deleting the single left row that matched
/// three right rows must orphan all three, preserved in right_idx ASC
/// order to match rebuild output.
#[test]
fn test_right_join_incremental_left_delete_orphans_multiple_rights_ordered() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        l.append_row(left_row(1, "L1")).unwrap();
    }
    {
        let mut r = right.borrow_mut();
        r.append_row(right_row(1, "R1a")).unwrap();
        r.append_row(right_row(1, "R1b")).unwrap();
        r.append_row(right_row(1, "R1c")).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    )
    .unwrap();
    assert_eq!(joined.len(), 3); // left id=1 matches each of 3 right rows

    // Delete left id=1 — all three rights become orphaned
    left.borrow_mut().delete_row(0).unwrap();
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // Orphans must be ordered by right_idx ASC (matches rebuild order)
    assert_eq!(
        result,
        vec![(None, Some(1)), (None, Some(1)), (None, Some(1))]
    );
}

/// Bulk delete: delete 3 left rows back-to-back, sync once. Verifies
/// cumulative index shifts compose correctly across changeset entries
/// processed in a single batch.
#[test]
fn test_inner_join_incremental_bulk_left_delete_in_one_sync() {
    let left = make_left_table();
    let right = make_right_table();
    {
        let mut l = left.borrow_mut();
        for id in 1..=5 {
            l.append_row(left_row(id, "L")).unwrap();
        }
    }
    {
        let mut r = right.borrow_mut();
        for id in 1..=5 {
            r.append_row(right_row(id, "R")).unwrap();
        }
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(joined.len(), 5);

    // Delete first three left rows in sequence WITHOUT syncing between
    {
        let mut l = left.borrow_mut();
        l.delete_row(0).unwrap(); // id=1 gone, ids 2,3,4,5 now at indices 0..4
        l.delete_row(0).unwrap(); // id=2 gone, ids 3,4,5 at 0..3
        l.delete_row(0).unwrap(); // id=3 gone, ids 4,5 at 0..2
    }
    assert!(joined.sync());

    let result = collect_join_rows(&joined);
    // After all deletes: left has id=4, id=5; right unchanged
    assert_eq!(result, vec![(Some(4), Some(4)), (Some(5), Some(5))]);
}

/// LEFT join, key-update from a NULL key (was a placeholder) to a value
/// that matches a right row. The placeholder should be replaced with the
/// actual match.
#[test]
fn test_left_join_incremental_left_key_update_null_to_match() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, true),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
    let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));
    {
        // Left starts with NULL id — placeholder in LEFT join
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Null);
        row.insert(
            "name".to_string(),
            ColumnValue::String("L_null".to_string()),
        );
        left.borrow_mut().append_row(row).unwrap();
    }
    {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(7));
        row.insert("name".to_string(), ColumnValue::String("R7".to_string()));
        right.borrow_mut().append_row(row).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Left,
    )
    .unwrap();
    assert_eq!(joined.len(), 1);
    let initial = joined.get_row(0).unwrap();
    assert!(initial.get("right_id").unwrap().is_null());

    // Change left id from NULL to 7 — should now match
    left.borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(7))
        .unwrap();
    assert!(joined.sync());

    let row = joined.get_row(0).unwrap();
    assert_eq!(row.get("id").unwrap().as_i32(), Some(7));
    assert_eq!(row.get("right_id").unwrap().as_i32(), Some(7));
    assert_eq!(row.get("right_name").unwrap().as_string(), Some("R7"));
}

/// LEFT join, key-update from a value (matched) to NULL. The matched
/// entry should be replaced with a (Some(left_idx), None) placeholder.
#[test]
fn test_left_join_incremental_left_key_update_match_to_null() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, true),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
    let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));
    {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(5));
        row.insert("name".to_string(), ColumnValue::String("L5".to_string()));
        left.borrow_mut().append_row(row).unwrap();
    }
    {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(5));
        row.insert("name".to_string(), ColumnValue::String("R5".to_string()));
        right.borrow_mut().append_row(row).unwrap();
    }

    let mut joined = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Left,
    )
    .unwrap();
    assert_eq!(joined.len(), 1);

    // Change left id from 5 to NULL — match disappears, placeholder appears
    left.borrow_mut()
        .set_value(0, "id", ColumnValue::Null)
        .unwrap();
    assert!(joined.sync());

    assert_eq!(joined.len(), 1);
    let row = joined.get_row(0).unwrap();
    assert!(row.get("id").unwrap().is_null());
    assert!(row.get("right_id").unwrap().is_null());
}

/// Multi-column join with a delete on the LEFT side. Exercises the
/// composite-key path (Vec<JoinKeyPart> rather than single key).
#[test]
fn test_inner_join_multi_column_incremental_left_delete() {
    let schema = Schema::new(vec![
        ("a".to_string(), ColumnType::Int32, false),
        ("b".to_string(), ColumnType::String, false),
        ("payload".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
    let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));

    let mkrow = |a: i32, b: &str, payload: &str| {
        let mut row = HashMap::new();
        row.insert("a".to_string(), ColumnValue::Int32(a));
        row.insert("b".to_string(), ColumnValue::String(b.to_string()));
        row.insert(
            "payload".to_string(),
            ColumnValue::String(payload.to_string()),
        );
        row
    };

    // Left: (1,"x"), (1,"y"), (2,"x")
    // Right: (1,"x"), (1,"y")
    // Expected matches: L0↔R0, L1↔R1 (INNER)
    left.borrow_mut().append_row(mkrow(1, "x", "L0")).unwrap();
    left.borrow_mut().append_row(mkrow(1, "y", "L1")).unwrap();
    left.borrow_mut().append_row(mkrow(2, "x", "L2")).unwrap();
    right.borrow_mut().append_row(mkrow(1, "x", "R0")).unwrap();
    right.borrow_mut().append_row(mkrow(1, "y", "R1")).unwrap();

    let mut joined = JoinView::new_multi(
        "j".to_string(),
        left.clone(),
        right.clone(),
        vec!["a".to_string(), "b".to_string()],
        vec!["a".to_string(), "b".to_string()],
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(joined.len(), 2);

    // Delete left row 0 → only L1↔R1 remains; indices shift
    left.borrow_mut().delete_row(0).unwrap();
    assert!(joined.sync());

    assert_eq!(joined.len(), 1);
    let row = joined.get_row(0).unwrap();
    assert_eq!(row.get("payload").unwrap().as_string(), Some("L1"));
    assert_eq!(row.get("right_payload").unwrap().as_string(), Some("R1"));
}

/// Multi-column join with a key-column update. Updating one part of a
/// composite key must rebuild the typed JoinKey and match correctly.
#[test]
fn test_inner_join_multi_column_incremental_key_update() {
    let schema = Schema::new(vec![
        ("a".to_string(), ColumnType::Int32, false),
        ("b".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
    let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));

    let mkrow = |a: i32, b: &str| {
        let mut row = HashMap::new();
        row.insert("a".to_string(), ColumnValue::Int32(a));
        row.insert("b".to_string(), ColumnValue::String(b.to_string()));
        row
    };

    left.borrow_mut().append_row(mkrow(1, "x")).unwrap();
    right.borrow_mut().append_row(mkrow(1, "x")).unwrap();
    right.borrow_mut().append_row(mkrow(1, "y")).unwrap();

    let mut joined = JoinView::new_multi(
        "j".to_string(),
        left.clone(),
        right.clone(),
        vec!["a".to_string(), "b".to_string()],
        vec!["a".to_string(), "b".to_string()],
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(joined.len(), 1); // L0 matches R0 (1,x)

    // Update left b from "x" to "y" — should now match R1, not R0
    left.borrow_mut()
        .set_value(0, "b", ColumnValue::String("y".to_string()))
        .unwrap();
    assert!(joined.sync());

    assert_eq!(joined.len(), 1);
    let row = joined.get_row(0).unwrap();
    assert_eq!(row.get("a").unwrap().as_i32(), Some(1));
    assert_eq!(row.get("b").unwrap().as_string(), Some("y"));
    assert_eq!(row.get("right_a").unwrap().as_i32(), Some(1));
    assert_eq!(row.get("right_b").unwrap().as_string(), Some("y"));
}

/// Convergence test: apply a mixed sequence of inserts, deletes, and key
/// updates to TWO parallel table pairs; on one pair sync incrementally
/// after each change, on the other build a fresh JoinView at the end.
/// Both must produce byte-identical result rows.
#[test]
fn test_full_join_incremental_converges_to_rebuild() {
    fn build_pair() -> (Rc<RefCell<Table>>, Rc<RefCell<Table>>) {
        (make_left_table(), make_right_table())
    }

    let (left_a, right_a) = build_pair();
    let (left_b, right_b) = build_pair();

    // Seed both pairs identically
    for &(id, name) in &[(1, "L1"), (2, "L2"), (3, "L3")] {
        left_a.borrow_mut().append_row(left_row(id, name)).unwrap();
        left_b.borrow_mut().append_row(left_row(id, name)).unwrap();
    }
    for &(id, val) in &[(1, "R1"), (2, "R2a"), (2, "R2b"), (4, "R4")] {
        right_a.borrow_mut().append_row(right_row(id, val)).unwrap();
        right_b.borrow_mut().append_row(right_row(id, val)).unwrap();
    }

    // Build incremental view on pair A; rebuild on pair B at end.
    let mut joined_a = JoinView::new(
        "a".to_string(),
        left_a.clone(),
        right_a.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // Mixed sequence: insert left, delete right, update left key, insert right,
    // update right key, delete left, insert at end. Each op is mirrored to
    // both pairs so the rebuilt JoinView at the end sees the same state.

    left_a.borrow_mut().append_row(left_row(5, "L5")).unwrap();
    left_b.borrow_mut().append_row(left_row(5, "L5")).unwrap();
    joined_a.sync();

    right_a.borrow_mut().delete_row(0).unwrap();
    right_b.borrow_mut().delete_row(0).unwrap();
    joined_a.sync();

    left_a
        .borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(99))
        .unwrap();
    left_b
        .borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(99))
        .unwrap();
    joined_a.sync();

    right_a
        .borrow_mut()
        .append_row(right_row(99, "R99"))
        .unwrap();
    right_b
        .borrow_mut()
        .append_row(right_row(99, "R99"))
        .unwrap();
    joined_a.sync();

    right_a
        .borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(2))
        .unwrap();
    right_b
        .borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(2))
        .unwrap();
    joined_a.sync();

    left_a.borrow_mut().delete_row(2).unwrap();
    left_b.borrow_mut().delete_row(2).unwrap();
    joined_a.sync();

    left_a
        .borrow_mut()
        .append_row(left_row(2, "L2_new"))
        .unwrap();
    left_b
        .borrow_mut()
        .append_row(left_row(2, "L2_new"))
        .unwrap();
    joined_a.sync();

    // Fresh rebuild on pair B.
    let joined_b = JoinView::new(
        "b".to_string(),
        left_b.clone(),
        right_b.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    )
    .unwrap();

    // Compare result rows (same length, same (left_id, right_id) sequence).
    let a_rows = collect_join_rows(&joined_a);
    let b_rows = collect_join_rows(&joined_b);
    assert_eq!(
        a_rows.len(),
        b_rows.len(),
        "incremental row count diverged from rebuild"
    );
    assert_eq!(
        a_rows, b_rows,
        "incremental result diverged from rebuild after mixed-op sequence"
    );
}

// === Tail-insert fast-path regression tests (fix #12) ===

/// Bulk-append 50 rows after creating a FilterView and sync; verify all
/// matching rows appear in correct order. Exercises the no-shift path.
#[test]
fn test_filter_view_bulk_tail_insert_ordering() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("v".to_string(), ColumnType::Int32, false),
    ]);
    let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));

    // Seed: ids 0..5 with v=id*2 — predicate v >= 4 selects ids 2..5
    for i in 0..5 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(i));
        row.insert("v".to_string(), ColumnValue::Int32(i * 2));
        table.borrow_mut().append_row(row).unwrap();
    }

    let mut view = FilterView::new("f".to_string(), table.clone(), |row| {
        row.get("v").and_then(|v| v.as_i32()).unwrap_or(0) >= 4
    });
    assert_eq!(view.len(), 3); // ids 2, 3, 4

    // Bulk append 50 more rows at the tail. Predicate matches all (v >= 4).
    for i in 5..55 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(i));
        row.insert("v".to_string(), ColumnValue::Int32(i * 2));
        table.borrow_mut().append_row(row).unwrap();
    }
    assert!(view.sync());

    // View should contain ids 2..55 (53 rows), parent indices in order.
    assert_eq!(view.len(), 53);
    for (view_idx, expected_parent_idx) in (2..55).enumerate() {
        let row = view.get_row(view_idx).unwrap();
        assert_eq!(row.get("id").unwrap().as_i32(), Some(expected_parent_idx));
    }
}

/// Bulk-append 20 rows to a SortedView (sorted DESC). Verify sort
/// invariant holds after the no-shift tail-insert fast path.
#[test]
fn test_sorted_view_bulk_tail_insert_ordering() {
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("score".to_string(), ColumnType::Int32, false),
    ]);
    let table = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));

    // Seed 5 rows
    for i in 0..5 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(i));
        row.insert("score".to_string(), ColumnValue::Int32(i * 10));
        table.borrow_mut().append_row(row).unwrap();
    }

    let mut view = SortedView::new(
        "s".to_string(),
        table.clone(),
        vec![SortKey::descending("score")],
    )
    .unwrap();
    assert_eq!(view.len(), 5);

    // Bulk-append 20 more, intentionally with varying scores
    for i in 0..20 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(100 + i));
        // Scores in a pattern that interleaves with existing
        row.insert("score".to_string(), ColumnValue::Int32((i * 7) % 50));
        table.borrow_mut().append_row(row).unwrap();
    }
    assert!(view.sync());

    assert_eq!(view.len(), 25);

    // Verify sort invariant: scores are monotonically non-increasing
    let mut prev: Option<i32> = None;
    for i in 0..view.len() {
        let s = view.get_value(i, "score").unwrap().as_i32().unwrap();
        if let Some(p) = prev {
            assert!(
                p >= s,
                "Sort invariant violated at view index {}: {} < {}",
                i,
                p,
                s
            );
        }
        prev = Some(s);
    }
}

// === Typed join key tests (fix #1: replace string serialization) ===

/// Two rows whose String parts contain `\x00` must not collide via
/// composite-key serialization. With the old String-based scheme,
/// keys (\"a\\x00b\", \"c\") and (\"a\", \"b\\x00c\") both produced
/// \"a\\x00b\\x00c\". Typed keys ([\"a\\x00b\", \"c\"]) vs ([\"a\", \"b\\x00c\"])
/// are structurally distinct.
#[test]
fn test_join_composite_key_null_byte_collision() {
    let schema = Schema::new(vec![
        ("k1".to_string(), ColumnType::String, false),
        ("k2".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
    let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));

    // Left: (k1="a\x00b", k2="c")
    let mut lrow = HashMap::new();
    lrow.insert("k1".to_string(), ColumnValue::String("a\x00b".to_string()));
    lrow.insert("k2".to_string(), ColumnValue::String("c".to_string()));
    left.borrow_mut().append_row(lrow).unwrap();

    // Right: (k1="a", k2="b\x00c") - DIFFERENT row, but old code thought they match
    let mut rrow = HashMap::new();
    rrow.insert("k1".to_string(), ColumnValue::String("a".to_string()));
    rrow.insert("k2".to_string(), ColumnValue::String("b\x00c".to_string()));
    right.borrow_mut().append_row(rrow).unwrap();

    let joined = JoinView::new_multi(
        "j".to_string(),
        left,
        right,
        vec!["k1".to_string(), "k2".to_string()],
        vec!["k1".to_string(), "k2".to_string()],
        JoinType::Inner,
    )
    .unwrap();

    // INNER join: must be empty (keys differ structurally)
    assert_eq!(
        joined.len(),
        0,
        "Composite key collision: rows with \\x00-containing parts incorrectly matched"
    );
}

/// Float64 join keys must work without relying on format!("{:?}", value),
/// which is not a stability-guaranteed format. Same f64 bit patterns join;
/// NaN never joins (SQL semantics).
#[test]
fn test_join_float64_keys() {
    let schema = Schema::new(vec![
        ("k".to_string(), ColumnType::Float64, false),
        ("label".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("L".to_string(), schema.clone())));
    let right = Rc::new(RefCell::new(Table::new("R".to_string(), schema)));

    // Left: 1.5, 2.5, NaN
    for (k, label) in [(1.5_f64, "L_one"), (2.5, "L_two"), (f64::NAN, "L_nan")] {
        let mut row = HashMap::new();
        row.insert("k".to_string(), ColumnValue::Float64(k));
        row.insert("label".to_string(), ColumnValue::String(label.to_string()));
        left.borrow_mut().append_row(row).unwrap();
    }
    // Right: 1.5, NaN
    for (k, label) in [(1.5_f64, "R_one"), (f64::NAN, "R_nan")] {
        let mut row = HashMap::new();
        row.insert("k".to_string(), ColumnValue::Float64(k));
        row.insert("label".to_string(), ColumnValue::String(label.to_string()));
        right.borrow_mut().append_row(row).unwrap();
    }

    let joined = JoinView::new(
        "j".to_string(),
        left,
        right,
        "k".to_string(),
        "k".to_string(),
        JoinType::Inner,
    )
    .unwrap();

    // Only 1.5 matches; NaN must NEVER match (SQL semantics)
    assert_eq!(
        joined.len(),
        1,
        "Expected exactly one Float64 match (1.5↔1.5); NaN must not match itself"
    );
    let row = joined.get_row(0).unwrap();
    assert_eq!(row.get("label").unwrap().as_string(), Some("L_one"));
    assert_eq!(row.get("right_label").unwrap().as_string(), Some("R_one"));
}

// === Regression coverage for hot-path optimizations in JoinView left-insert ===

/// Covers the LEFT "no-match incremental insert" branch where a new left
/// row arrives with no corresponding right row. The new row must appear
/// in join_index in left-index order, paired with `None` on the right.
/// Exercises view.rs:970 (else-if branch) — the consumer of the outer
/// `insert_pos` computation we are about to move into the branch.
#[test]
fn test_left_join_incremental_insert_no_right_match() {
    let left = make_left_table();
    let right = make_right_table();

    // Right has only id=1
    right.borrow_mut().append_row(right_row(1, "R1")).unwrap();
    // Left starts with id=1 (matches)
    left.borrow_mut().append_row(left_row(1, "L1")).unwrap();

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Left,
    )
    .unwrap();
    assert_eq!(joined.len(), 1);

    // Incremental: add a left row with NO match in right
    left.borrow_mut().append_row(left_row(2, "L2")).unwrap();
    assert!(joined.sync());

    // Expected: (left.id=1, right.id=1), (left.id=2, right=None)
    let result = collect_join_rows(&joined);
    assert_eq!(result, vec![(Some(1), Some(1)), (Some(2), None)]);
}

// === Cross-side and same-side frame mixing in JoinView::sync ===

/// Both parents mutated, then ONE sync handles both changesets (exactly
/// what TickableTable::tick produces). The pair added for the new left
/// row must reference the new right row at its real index — not get
/// re-shifted by the right-insert pass.
#[test]
fn test_inner_join_sync_both_side_appends_same_batch() {
    let left = make_left_table();
    let right = make_right_table();
    left.borrow_mut().append_row(left_row(1, "L1")).unwrap();
    right.borrow_mut().append_row(right_row(1, "R1")).unwrap();

    let mut join = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(collect_join_rows(&join), vec![(Some(1), Some(1))]);

    left.borrow_mut().append_row(left_row(2, "L2")).unwrap();
    right.borrow_mut().append_row(right_row(2, "R2")).unwrap();
    join.sync();

    assert_eq!(
        collect_join_rows(&join),
        vec![(Some(1), Some(1)), (Some(2), Some(2))]
    );
}

/// The original double-count repro: two non-tail left inserts (the second
/// shifts the first) plus a matching right insert in the same batch. The
/// match for the shifted new left row must appear exactly once.
#[test]
fn test_inner_join_sync_shifted_new_left_not_double_counted() {
    let left = make_left_table();
    let right = make_right_table();

    let mut join = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(join.len(), 0);

    // id=10 lands at index 0, then id=20 inserts at 0 shifting id=10 to 1
    left.borrow_mut().insert_row(0, left_row(10, "A")).unwrap();
    left.borrow_mut().insert_row(0, left_row(20, "B")).unwrap();
    right.borrow_mut().append_row(right_row(10, "R")).unwrap();
    join.sync();

    assert_eq!(collect_join_rows(&join), vec![(Some(10), Some(10))]);
}

/// Same-side frame mixing: a key update recorded BEFORE an insert in the
/// same batch. The update handler reads the row by its recorded index from
/// the live table — where the later insert has already shifted it.
#[test]
fn test_inner_join_sync_key_update_then_insert_same_batch() {
    let left = make_left_table();
    let right = make_right_table();
    left.borrow_mut().append_row(left_row(1, "X")).unwrap();
    right.borrow_mut().append_row(right_row(5, "R5")).unwrap();

    let mut join = JoinView::new(
        "j".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    )
    .unwrap();
    assert_eq!(join.len(), 0); // id=1 has no right match

    // Update row 0's key to 5 (now matches right), THEN insert a new row
    // at index 0 — the updated row's live index becomes 1.
    left.borrow_mut()
        .set_value(0, "id", ColumnValue::Int32(5))
        .unwrap();
    left.borrow_mut().insert_row(0, left_row(7, "new")).unwrap();
    join.sync();

    assert_eq!(collect_join_rows(&join), vec![(Some(5), Some(5))]);
}

// === NaN handling in aggregates ===

#[test]
fn test_aggregate_nan_values_excluded_like_null() {
    let schema = Schema::new(vec![
        ("region".to_string(), ColumnType::String, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));
    {
        let mut t = table.borrow_mut();
        for v in [10.0, f64::NAN, 20.0] {
            let mut row = HashMap::new();
            row.insert(
                "region".to_string(),
                ColumnValue::String("North".to_string()),
            );
            row.insert("amount".to_string(), ColumnValue::Float64(v));
            t.append_row(row).unwrap();
        }
    }

    let mut agg = AggregateView::new(
        "by_region".to_string(),
        table.clone(),
        vec!["region".to_string()],
        vec![
            (
                "total".to_string(),
                "amount".to_string(),
                AggregateFunction::Sum,
            ),
            (
                "average".to_string(),
                "amount".to_string(),
                AggregateFunction::Avg,
            ),
            (
                "cnt".to_string(),
                "amount".to_string(),
                AggregateFunction::Count,
            ),
            (
                "lo".to_string(),
                "amount".to_string(),
                AggregateFunction::Min,
            ),
            (
                "hi".to_string(),
                "amount".to_string(),
                AggregateFunction::Max,
            ),
            (
                "med".to_string(),
                "amount".to_string(),
                AggregateFunction::Median,
            ),
        ],
    )
    .unwrap();

    let get_f64 = |row: &HashMap<String, ColumnValue>, col: &str| -> f64 {
        match row.get(col).unwrap() {
            ColumnValue::Float64(v) => *v,
            other => panic!("Expected Float64 for {}, got {:?}", col, other),
        }
    };

    assert_eq!(agg.len(), 1);
    let row = agg.get_row(0).unwrap();
    assert!((get_f64(&row, "total") - 30.0).abs() < 1e-9);
    assert!((get_f64(&row, "average") - 15.0).abs() < 1e-9);
    assert_eq!(row.get("cnt").unwrap(), &ColumnValue::Int64(2));
    assert!((get_f64(&row, "lo") - 10.0).abs() < 1e-9);
    assert!((get_f64(&row, "hi") - 20.0).abs() < 1e-9);
    assert!((get_f64(&row, "med") - 15.0).abs() < 1e-9);

    // Deleting the NaN row must not corrupt the running state
    table.borrow_mut().delete_row(1).unwrap();
    agg.sync();
    let row = agg.get_row(0).unwrap();
    assert!((get_f64(&row, "total") - 30.0).abs() < 1e-9);
    assert_eq!(row.get("cnt").unwrap(), &ColumnValue::Int64(2));
    assert!((get_f64(&row, "med") - 15.0).abs() < 1e-9);

    // Subsequent inserts keep working on the same state
    {
        let mut row = HashMap::new();
        row.insert(
            "region".to_string(),
            ColumnValue::String("North".to_string()),
        );
        row.insert("amount".to_string(), ColumnValue::Float64(40.0));
        table.borrow_mut().append_row(row).unwrap();
    }
    agg.sync();
    let row = agg.get_row(0).unwrap();
    assert!((get_f64(&row, "total") - 70.0).abs() < 1e-9);
    assert_eq!(row.get("cnt").unwrap(), &ColumnValue::Int64(3));
    assert!((get_f64(&row, "med") - 20.0).abs() < 1e-9);
}

#[test]
fn test_aggregate_group_key_negative_zero_groups_with_positive_zero() {
    let schema = Schema::new(vec![
        ("price".to_string(), ColumnType::Float64, false),
        ("qty".to_string(), ColumnType::Int32, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));
    {
        let mut t = table.borrow_mut();
        for (p, q) in [(0.0_f64, 1), (-0.0_f64, 2)] {
            let mut row = HashMap::new();
            row.insert("price".to_string(), ColumnValue::Float64(p));
            row.insert("qty".to_string(), ColumnValue::Int32(q));
            t.append_row(row).unwrap();
        }
    }

    let agg = AggregateView::new(
        "by_price".to_string(),
        table.clone(),
        vec!["price".to_string()],
        vec![(
            "total_qty".to_string(),
            "qty".to_string(),
            AggregateFunction::Sum,
        )],
    )
    .unwrap();

    // 0.0 == -0.0, so they must form a single group
    assert_eq!(agg.len(), 1);
    let row = agg.get_row(0).unwrap();
    match row.get("total_qty").unwrap() {
        ColumnValue::Float64(v) => assert!((*v - 3.0).abs() < 1e-9),
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_aggregate_group_key_nan_single_group_distinct_from_null() {
    let schema = Schema::new(vec![
        ("bucket".to_string(), ColumnType::Float64, true),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("test".to_string(), schema)));
    {
        let mut t = table.borrow_mut();
        for (b, a) in [
            (ColumnValue::Float64(f64::NAN), 1.0),
            (ColumnValue::Float64(f64::NAN), 2.0),
            (ColumnValue::Null, 5.0),
        ] {
            let mut row = HashMap::new();
            row.insert("bucket".to_string(), b);
            row.insert("amount".to_string(), ColumnValue::Float64(a));
            t.append_row(row).unwrap();
        }
    }

    let agg = AggregateView::new(
        "by_bucket".to_string(),
        table.clone(),
        vec!["bucket".to_string()],
        vec![(
            "total".to_string(),
            "amount".to_string(),
            AggregateFunction::Sum,
        )],
    )
    .unwrap();

    // All NaNs group together (Postgres-style); NULL stays its own group
    assert_eq!(agg.len(), 2);
    let mut nan_total = None;
    let mut null_total = None;
    for i in 0..agg.len() {
        let row = agg.get_row(i).unwrap();
        let total = match row.get("total").unwrap() {
            ColumnValue::Float64(v) => *v,
            other => panic!("Expected Float64, got {:?}", other),
        };
        match row.get("bucket").unwrap() {
            ColumnValue::Float64(v) if v.is_nan() => nan_total = Some(total),
            ColumnValue::Null => null_total = Some(total),
            other => panic!("Unexpected bucket value: {:?}", other),
        }
    }
    assert!((nan_total.unwrap() - 3.0).abs() < 1e-9);
    assert!((null_total.unwrap() - 5.0).abs() < 1e-9);
}

// === ReadableTable composition: views over views ===

fn make_sales() -> Rc<RefCell<Table>> {
    let schema = Schema::new(vec![
        ("region".to_string(), ColumnType::String, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);
    let t = Rc::new(RefCell::new(Table::new("sales".to_string(), schema)));
    for (r, a) in [
        ("N", 50.0),
        ("S", 150.0),
        ("N", 300.0),
        ("S", 80.0),
        ("N", 120.0),
    ] {
        t.borrow_mut().append_row(sales_row(r, a)).unwrap();
    }
    t
}

fn sales_row(region: &str, amount: f64) -> HashMap<String, ColumnValue> {
    HashMap::from([
        (
            "region".to_string(),
            ColumnValue::String(region.to_string()),
        ),
        ("amount".to_string(), ColumnValue::Float64(amount)),
    ])
}

fn big_amount(row: &HashMap<String, ColumnValue>) -> bool {
    matches!(row.get("amount"), Some(ColumnValue::Float64(a)) if *a >= 100.0)
}

#[test]
fn test_filter_then_sort_composition() {
    let table = make_sales();
    let filter = Rc::new(RefCell::new(FilterView::new(
        "big".to_string(),
        table.clone(),
        big_amount,
    )));
    let mut sorted = SortedView::new(
        "big_sorted".to_string(),
        filter.clone(),
        vec![SortKey::descending("amount")],
    )
    .unwrap();

    let amounts = |s: &SortedView| -> Vec<f64> {
        (0..s.len())
            .map(|i| s.get_value(i, "amount").unwrap().as_f64().unwrap())
            .collect()
    };
    assert_eq!(amounts(&sorted), vec![300.0, 150.0, 120.0]);

    // Mutate the ROOT: one qualifying row, one not.
    table
        .borrow_mut()
        .append_row(sales_row("S", 500.0))
        .unwrap();
    table.borrow_mut().append_row(sales_row("N", 10.0)).unwrap();
    filter.borrow_mut().sync();
    assert!(sorted.sync());
    assert_eq!(amounts(&sorted), vec![500.0, 300.0, 150.0, 120.0]);

    // Nothing new: child sync must be a no-op.
    assert!(!sorted.sync());
}

#[test]
fn test_sort_then_filter_composition() {
    let table = make_sales();
    let sorted = Rc::new(RefCell::new(
        SortedView::new(
            "by_amount".to_string(),
            table.clone(),
            vec![SortKey::ascending("amount")],
        )
        .unwrap(),
    ));
    let mut filter = FilterView::new(
        "cheap".to_string(),
        sorted.clone(),
        |row| matches!(row.get("amount"), Some(ColumnValue::Float64(a)) if *a < 200.0),
    );

    let amounts = |f: &FilterView| -> Vec<f64> {
        (0..f.len())
            .map(|i| f.get_value(i, "amount").unwrap().as_f64().unwrap())
            .collect()
    };
    // Ascending [50, 80, 120, 150, 300] filtered <200 keeps sorted order.
    assert_eq!(amounts(&filter), vec![50.0, 80.0, 120.0, 150.0]);

    table.borrow_mut().append_row(sales_row("N", 60.0)).unwrap();
    sorted.borrow_mut().sync();
    assert!(filter.sync());
    assert_eq!(amounts(&filter), vec![50.0, 60.0, 80.0, 120.0, 150.0]);
}

#[test]
fn test_filter_then_group_by_composition() {
    let table = make_sales();
    let filter = Rc::new(RefCell::new(FilterView::new(
        "big".to_string(),
        table.clone(),
        big_amount,
    )));
    let mut agg = AggregateView::new(
        "by_region".to_string(),
        filter.clone(),
        vec!["region".to_string()],
        vec![(
            "total".to_string(),
            "amount".to_string(),
            AggregateFunction::Sum,
        )],
    )
    .unwrap();

    let totals = |a: &AggregateView| -> HashMap<String, f64> {
        (0..a.len())
            .map(|i| {
                let row = a.get_row(i).unwrap();
                (
                    row.get("region").unwrap().as_string().unwrap().to_string(),
                    row.get("total").unwrap().as_f64().unwrap(),
                )
            })
            .collect()
    };
    // Filtered (>=100): S 150, N 300, N 120.
    let t = totals(&agg);
    assert_eq!(t["N"], 420.0);
    assert_eq!(t["S"], 150.0);

    table
        .borrow_mut()
        .append_row(sales_row("S", 200.0))
        .unwrap();
    filter.borrow_mut().sync();
    assert!(agg.sync());
    let t = totals(&agg);
    assert_eq!(t["N"], 420.0);
    assert_eq!(t["S"], 350.0);
}

#[test]
fn test_filter_then_filter_composition() {
    let table = make_sales();
    let big = Rc::new(RefCell::new(FilterView::new(
        "big".to_string(),
        table.clone(),
        big_amount,
    )));
    let mut north_big = FilterView::new(
        "north_big".to_string(),
        big.clone(),
        |row| matches!(row.get("region"), Some(ColumnValue::String(r)) if r == "N"),
    );

    // big: [S 150, N 300, N 120] -> north: [300, 120]
    assert_eq!(north_big.len(), 2);

    table
        .borrow_mut()
        .append_row(sales_row("N", 700.0))
        .unwrap();
    big.borrow_mut().sync();
    assert!(north_big.sync());
    assert_eq!(north_big.len(), 3);
    assert_eq!(
        north_big.get_value(2, "amount").unwrap().as_f64().unwrap(),
        700.0
    );
}

#[test]
fn test_join_with_view_parent() {
    let table = make_sales();
    let filter = Rc::new(RefCell::new(FilterView::new(
        "big".to_string(),
        table.clone(),
        big_amount,
    )));

    let target_schema = Schema::new(vec![
        ("region".to_string(), ColumnType::String, false),
        ("target".to_string(), ColumnType::Float64, false),
    ]);
    let targets = Rc::new(RefCell::new(Table::new(
        "targets".to_string(),
        target_schema,
    )));
    targets
        .borrow_mut()
        .append_row(HashMap::from([
            ("region".to_string(), ColumnValue::String("N".to_string())),
            ("target".to_string(), ColumnValue::Float64(1000.0)),
        ]))
        .unwrap();

    let mut join = JoinView::new(
        "j".to_string(),
        filter.clone(),
        targets.clone(),
        "region".to_string(),
        "region".to_string(),
        JoinType::Inner,
    )
    .unwrap();

    // Filtered big rows in N: 300 and 120 -> 2 join rows.
    assert_eq!(join.len(), 2);

    table
        .borrow_mut()
        .append_row(sales_row("N", 800.0))
        .unwrap();
    filter.borrow_mut().sync();
    assert!(join.sync());
    assert_eq!(join.len(), 3);
    assert!(!join.sync());
}

#[test]
fn test_tickable_propagates_through_view_chain() {
    let table = make_sales();
    let tickable = TickableTable::new(table.clone());

    let filter = Rc::new(RefCell::new(FilterView::new(
        "big".to_string(),
        table.clone(),
        big_amount,
    )));
    let sorted = Rc::new(RefCell::new(
        SortedView::new(
            "big_sorted".to_string(),
            filter.clone(),
            vec![SortKey::descending("amount")],
        )
        .unwrap(),
    ));
    // Registration order is creation order = topological order.
    tickable.register_filter(&filter);
    tickable.register_sorted(&sorted);

    table
        .borrow_mut()
        .append_row(sales_row("S", 900.0))
        .unwrap();
    assert!(tickable.tick() >= 1);

    assert_eq!(filter.borrow().len(), 4);
    let s = sorted.borrow();
    assert_eq!(s.len(), 4);
    assert_eq!(s.get_value(0, "amount").unwrap().as_f64().unwrap(), 900.0);
}
