/// Views Example
///
/// This example demonstrates:
/// - Creating FilterView to filter rows
/// - Creating ProjectionView to select columns
/// - Creating ComputedView to add calculated columns
/// - Combining multiple views

use livetable::{ColumnType, ColumnValue, ComputedView, FilterView, ProjectionView, Schema, Table};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

fn main() {
    println!("=== LiveTable Views Example ===\n");

    // 1. Create a sales table
    println!("1. Creating sales table...");
    let schema = Schema::new(vec![
        ("product".to_string(), ColumnType::String, false),
        ("category".to_string(), ColumnType::String, false),
        ("price".to_string(), ColumnType::Float64, false),
        ("quantity".to_string(), ColumnType::Int32, false),
        ("discount".to_string(), ColumnType::Float64, false),
    ]);

    let table = Rc::new(RefCell::new(Table::new("sales".to_string(), schema)));

    // Add sample data
    {
        let mut t = table.borrow_mut();

        let items = vec![
            ("Laptop", "Electronics", 999.99, 5, 0.1),
            ("Mouse", "Electronics", 29.99, 20, 0.0),
            ("Desk", "Furniture", 299.99, 3, 0.15),
            ("Chair", "Furniture", 199.99, 8, 0.1),
            ("Monitor", "Electronics", 399.99, 10, 0.05),
        ];

        for (product, category, price, quantity, discount) in items {
            let mut row = HashMap::new();
            row.insert("product".to_string(), ColumnValue::String(product.to_string()));
            row.insert("category".to_string(), ColumnValue::String(category.to_string()));
            row.insert("price".to_string(), ColumnValue::Float64(price));
            row.insert("quantity".to_string(), ColumnValue::Int32(quantity));
            row.insert("discount".to_string(), ColumnValue::Float64(discount));
            t.append_row(row).unwrap();
        }
    }

    println!("   Added {} products\n", table.borrow().len());

    // 2. FilterView - Show only Electronics
    println!("2. Creating FilterView for Electronics...");
    let electronics_view = FilterView::new(
        "electronics".to_string(),
        table.clone(),
        |row| {
            if let Some(ColumnValue::String(cat)) = row.get("category") {
                cat == "Electronics"
            } else {
                false
            }
        },
    );

    println!("   Electronics items: {}", electronics_view.len());
    for i in 0..electronics_view.len() {
        let row = electronics_view.get_row(i).unwrap();
        println!(
            "      - {}",
            row.get("product").unwrap().as_string().unwrap()
        );
    }
    println!();

    // 3. ProjectionView - Show only product and price
    println!("3. Creating ProjectionView (product, price only)...");
    let price_list = ProjectionView::new(
        "price_list".to_string(),
        table.clone(),
        vec!["product".to_string(), "price".to_string()],
    )
    .unwrap();

    println!("   Price list:");
    for i in 0..price_list.len() {
        let row = price_list.get_row(i).unwrap();
        println!(
            "      {} - ${}",
            row.get("product").unwrap().as_string().unwrap(),
            row.get("price").unwrap().as_f64().unwrap()
        );
    }
    println!();

    // 4. ComputedView - Add total revenue column
    println!("4. Creating ComputedView with revenue calculation...");
    let with_revenue = ComputedView::new(
        "with_revenue".to_string(),
        table.clone(),
        "revenue".to_string(),
        |row| {
            let price = match row.get("price") {
                Some(ColumnValue::Float64(p)) => *p,
                _ => 0.0,
            };
            let qty = match row.get("quantity") {
                Some(ColumnValue::Int32(q)) => *q as f64,
                _ => 0.0,
            };
            let discount = match row.get("discount") {
                Some(ColumnValue::Float64(d)) => *d,
                _ => 0.0,
            };

            let revenue = price * qty * (1.0 - discount);
            ColumnValue::Float64(revenue)
        },
    );

    println!("   Revenue by product:");
    for i in 0..with_revenue.len() {
        let row = with_revenue.get_row(i).unwrap();
        println!(
            "      {}: ${:.2}",
            row.get("product").unwrap().as_string().unwrap(),
            row.get("revenue").unwrap().as_f64().unwrap()
        );
    }
    println!();

    // 5. Filter high-revenue items
    println!("5. Filtering high-revenue items (> $1000)...");
    let mut high_revenue = FilterView::new(
        "high_revenue".to_string(),
        table.clone(),
        |row| {
            // Note: This filters on base table, not computed view
            // In a real scenario, we'd compute revenue here too
            let price = match row.get("price") {
                Some(ColumnValue::Float64(p)) => *p,
                _ => 0.0,
            };
            let qty = match row.get("quantity") {
                Some(ColumnValue::Int32(q)) => *q as f64,
                _ => 0.0,
            };
            let discount = match row.get("discount") {
                Some(ColumnValue::Float64(d)) => *d,
                _ => 0.0,
            };
            price * qty * (1.0 - discount) > 1000.0
        },
    );

    println!("   High-revenue items: {}", high_revenue.len());
    for i in 0..high_revenue.len() {
        let row = high_revenue.get_row(i).unwrap();
        println!(
            "      {}",
            row.get("product").unwrap().as_string().unwrap()
        );
    }
    println!();

    // 6. Demonstrate view refresh after adding data
    println!("6. Adding new product and refreshing view...");
    {
        let mut t = table.borrow_mut();
        let mut new_row = HashMap::new();
        new_row.insert("product".to_string(), ColumnValue::String("Keyboard".to_string()));
        new_row.insert("category".to_string(), ColumnValue::String("Electronics".to_string()));
        new_row.insert("price".to_string(), ColumnValue::Float64(79.99));
        new_row.insert("quantity".to_string(), ColumnValue::Int32(15));
        new_row.insert("discount".to_string(), ColumnValue::Float64(0.0));
        t.append_row(new_row).unwrap();
    }

    println!("   Table now has {} items", table.borrow().len());
    println!("   View before refresh: {} items", high_revenue.len());

    high_revenue.refresh();
    println!("   View after refresh: {} items", high_revenue.len());

    println!("\n=== Example Complete ===");
}
