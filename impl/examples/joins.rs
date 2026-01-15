/// Join Operations Example
///
/// This example demonstrates:
/// - Left Join: All rows from left table, matched rows from right
/// - Inner Join: Only rows that exist in both tables
/// - Handling nulls in left joins
/// - Refreshing joins after data changes

use livetable::{ColumnType, ColumnValue, JoinType, JoinView, Schema, Table};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

fn main() {
    println!("=== LiveTable Join Operations Example ===\n");

    // 1. Create users table
    println!("1. Creating users table...");
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
        ("email".to_string(), ColumnType::String, false),
    ]);

    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));

    {
        let mut u = users.borrow_mut();

        let users_data = vec![
            (1, "Alice", "alice@example.com"),
            (2, "Bob", "bob@example.com"),
            (3, "Charlie", "charlie@example.com"),
            (4, "Diana", "diana@example.com"),
        ];

        for (id, name, email) in users_data {
            let mut row = HashMap::new();
            row.insert("user_id".to_string(), ColumnValue::Int32(id));
            row.insert("name".to_string(), ColumnValue::String(name.to_string()));
            row.insert("email".to_string(), ColumnValue::String(email.to_string()));
            u.append_row(row).unwrap();
        }
    }

    println!("   Added {} users\n", users.borrow().len());

    // 2. Create orders table
    println!("2. Creating orders table...");
    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
        ("product".to_string(), ColumnType::String, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);

    let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));

    {
        let mut o = orders.borrow_mut();

        let orders_data = vec![
            (101, 1, "Laptop", 999.99),
            (102, 1, "Mouse", 29.99),
            (103, 3, "Keyboard", 79.99),
            (104, 3, "Monitor", 399.99),
            (105, 3, "Headphones", 149.99),
            // Note: Bob (user_id=2) and Diana (user_id=4) have no orders
        ];

        for (order_id, user_id, product, amount) in orders_data {
            let mut row = HashMap::new();
            row.insert("order_id".to_string(), ColumnValue::Int32(order_id));
            row.insert("user_id".to_string(), ColumnValue::Int32(user_id));
            row.insert("product".to_string(), ColumnValue::String(product.to_string()));
            row.insert("amount".to_string(), ColumnValue::Float64(amount));
            o.append_row(row).unwrap();
        }
    }

    println!("   Added {} orders\n", orders.borrow().len());

    // 3. Left Join - Show all users with their orders (if any)
    println!("3. Left Join: All users with their orders");
    println!("   (Users without orders will show NULL for order columns)\n");

    let left_join = JoinView::new(
        "users_orders_left".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Left,
    )
    .unwrap();

    println!("   Total rows in left join: {}\n", left_join.len());

    for i in 0..left_join.len() {
        let row = left_join.get_row(i).unwrap();
        let name = row.get("name").unwrap().as_string().unwrap();
        let product = match row.get("right_product") {
            Some(ColumnValue::String(p)) => p.to_string(),
            Some(ColumnValue::Null) => "NO ORDERS".to_string(),
            _ => "N/A".to_string(),
        };
        let amount = match row.get("right_amount") {
            Some(ColumnValue::Float64(a)) => format!("${:.2}", a),
            Some(ColumnValue::Null) => "-".to_string(),
            _ => "N/A".to_string(),
        };

        println!("      {} - {} ({})", name, product, amount);
    }
    println!();

    // 4. Inner Join - Show only users who have orders
    println!("4. Inner Join: Only users with orders");
    println!("   (Bob and Diana excluded)\n");

    let inner_join = JoinView::new(
        "users_orders_inner".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Inner,
    )
    .unwrap();

    println!("   Total rows in inner join: {}\n", inner_join.len());

    for i in 0..inner_join.len() {
        let row = inner_join.get_row(i).unwrap();
        let name = row.get("name").unwrap().as_string().unwrap();
        let product = row.get("right_product").unwrap().as_string().unwrap();
        let amount = row.get("right_amount").unwrap().as_f64().unwrap();

        println!("      {} - {} (${:.2})", name, product, amount);
    }
    println!();

    // 5. Calculate totals per user using left join
    println!("5. Calculating total spending per user:");
    println!("   (Using left join to include users with $0 spent)\n");

    let mut user_totals: HashMap<String, f64> = HashMap::new();

    for i in 0..left_join.len() {
        let row = left_join.get_row(i).unwrap();
        let name = row.get("name").unwrap().as_string().unwrap().to_string();
        let amount = match row.get("right_amount") {
            Some(ColumnValue::Float64(a)) => *a,
            _ => 0.0,
        };

        *user_totals.entry(name).or_insert(0.0) += amount;
    }

    for (name, total) in &user_totals {
        println!("      {} has spent: ${:.2}", name, total);
    }
    println!();

    // 6. Demonstrate join refresh after adding data
    println!("6. Adding new order and refreshing join...");

    {
        let mut o = orders.borrow_mut();
        let mut new_order = HashMap::new();
        new_order.insert("order_id".to_string(), ColumnValue::Int32(106));
        new_order.insert("user_id".to_string(), ColumnValue::Int32(2)); // Bob's first order!
        new_order.insert("product".to_string(), ColumnValue::String("Tablet".to_string()));
        new_order.insert("amount".to_string(), ColumnValue::Float64(599.99));
        o.append_row(new_order).unwrap();
    }

    println!("   Added order for Bob (user_id=2)");
    println!("   Before refresh: {} rows", left_join.len());

    let mut refreshed_join = JoinView::new(
        "refreshed".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Left,
    )
    .unwrap();

    println!("   After refresh: {} rows", refreshed_join.len());
    println!();

    // Show Bob's order
    for i in 0..refreshed_join.len() {
        let row = refreshed_join.get_row(i).unwrap();
        let name = row.get("name").unwrap().as_string().unwrap();
        if name == "Bob" {
            if let Some(ColumnValue::String(product)) = row.get("right_product") {
                println!("   Bob's order: {} - ${:.2}",
                    product,
                    row.get("right_amount").unwrap().as_f64().unwrap()
                );
            }
        }
    }
    println!();

    // 7. Join on different column names
    println!("7. Join with different column names:");
    println!("   (Joining users.user_id with orders.user_id)\n");

    // This works because we specify both left_key and right_key
    let custom_join = JoinView::new(
        "custom".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),  // Column in users table
        "user_id".to_string(),  // Column in orders table (same name, but could be different)
        JoinType::Left,
    )
    .unwrap();

    println!("   Join successful with {} rows", custom_join.len());
    println!();

    // 8. Summary
    println!("8. Summary:");
    println!("   Left Join:  {} rows (includes all users)", left_join.len());
    println!("   Inner Join: {} rows (only users with orders)", inner_join.len());
    println!();
    println!("   Left joins preserve all rows from the left table.");
    println!("   Inner joins only keep rows that match in both tables.");
    println!("   Right table columns are prefixed with 'right_' to avoid conflicts.");

    println!("\n=== Example Complete ===");
}
