/// Basic Table Operations Example
///
/// This example demonstrates:
/// - Creating a table with a schema
/// - Adding, updating, and deleting rows
/// - Querying data from the table

use livetable::{ColumnType, ColumnValue, Schema, Table};
use std::collections::HashMap;

fn main() {
    println!("=== LiveTable Basic Table Example ===\n");

    // 1. Create a schema
    println!("1. Creating schema...");
    let schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
        ("email".to_string(), ColumnType::String, false),
        ("age".to_string(), ColumnType::Int32, true), // Nullable
    ]);
    println!("   Schema created with {} columns\n", schema.len());

    // 2. Create a table
    println!("2. Creating table...");
    let mut users = Table::new("users".to_string(), schema);
    println!("   Table '{}' created\n", users.name());

    // 3. Add rows
    println!("3. Adding rows...");

    let mut row1 = HashMap::new();
    row1.insert("id".to_string(), ColumnValue::Int32(1));
    row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
    row1.insert("email".to_string(), ColumnValue::String("alice@example.com".to_string()));
    row1.insert("age".to_string(), ColumnValue::Int32(30));
    users.append_row(row1).unwrap();

    let mut row2 = HashMap::new();
    row2.insert("id".to_string(), ColumnValue::Int32(2));
    row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
    row2.insert("email".to_string(), ColumnValue::String("bob@example.com".to_string()));
    row2.insert("age".to_string(), ColumnValue::Null); // Null age
    users.append_row(row2).unwrap();

    let mut row3 = HashMap::new();
    row3.insert("id".to_string(), ColumnValue::Int32(3));
    row3.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
    row3.insert("email".to_string(), ColumnValue::String("charlie@example.com".to_string()));
    row3.insert("age".to_string(), ColumnValue::Int32(25));
    users.append_row(row3).unwrap();

    println!("   Added {} rows\n", users.len());

    // 4. Query data
    println!("4. Querying data...");
    for i in 0..users.len() {
        let row = users.get_row(i).unwrap();
        println!(
            "   Row {}: {} - {} (age: {})",
            i,
            row.get("name").unwrap().as_string().unwrap(),
            row.get("email").unwrap().as_string().unwrap(),
            match row.get("age").unwrap() {
                ColumnValue::Int32(age) => age.to_string(),
                ColumnValue::Null => "N/A".to_string(),
                _ => "unknown".to_string(),
            }
        );
    }
    println!();

    // 5. Get specific value
    println!("5. Getting specific value...");
    let name = users.get_value(0, "name").unwrap();
    println!("   User 0 name: {}\n", name.as_string().unwrap());

    // 6. Update a value
    println!("6. Updating value...");
    users.set_value(1, "age", ColumnValue::Int32(28)).unwrap();
    println!("   Updated Bob's age to 28\n");

    // 7. Insert a row in the middle
    println!("7. Inserting row at index 1...");
    let mut new_row = HashMap::new();
    new_row.insert("id".to_string(), ColumnValue::Int32(4));
    new_row.insert("name".to_string(), ColumnValue::String("Diana".to_string()));
    new_row.insert("email".to_string(), ColumnValue::String("diana@example.com".to_string()));
    new_row.insert("age".to_string(), ColumnValue::Int32(35));
    users.insert_row(1, new_row).unwrap();
    println!("   Inserted Diana at position 1");
    println!("   Table now has {} rows\n", users.len());

    // 8. Delete a row
    println!("8. Deleting row at index 2...");
    let deleted = users.delete_row(2).unwrap();
    println!(
        "   Deleted: {}",
        deleted.get("name").unwrap().as_string().unwrap()
    );
    println!("   Table now has {} rows\n", users.len());

    // 9. Iterate over all rows
    println!("9. Final table contents:");
    for (i, row) in users.iter_rows().enumerate() {
        println!(
            "   Row {}: ID={}, Name={}",
            i,
            row.get("id").unwrap().as_i32().unwrap(),
            row.get("name").unwrap().as_string().unwrap()
        );
    }

    println!("\n=== Example Complete ===");
}
