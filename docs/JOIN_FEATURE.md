# Join Operations - Feature Documentation

## Overview

The LiveTable Rust implementation now supports **join operations** between tables, allowing you to combine data from multiple tables based on matching column values.

---

## Supported Join Types

### 1. Left Join
- **Includes**: All rows from left table
- **Matches**: Corresponding rows from right table where keys match
- **No match**: Right columns are filled with `NULL`
- **Use case**: "Show all users and their orders (if any)"

### 2. Inner Join
- **Includes**: Only rows that exist in both tables
- **Matches**: Only rows where keys match in both tables
- **No match**: Row is excluded
- **Use case**: "Show only users who have placed orders"

---

## Quick Start

```rust
use livetable::{Table, Schema, ColumnType, JoinView, JoinType};
use std::rc::Rc;
use std::cell::RefCell;

// Create tables
let users = Rc::new(RefCell::new(Table::new(...)));
let orders = Rc::new(RefCell::new(Table::new(...)));

// Left join
let joined = JoinView::new(
    "user_orders".to_string(),
    users.clone(),
    orders.clone(),
    "user_id".to_string(),  // Column in left table
    "user_id".to_string(),  // Column in right table
    JoinType::Left,
).unwrap();

// Access joined data
for i in 0..joined.len() {
    let row = joined.get_row(i).unwrap();
    // row contains columns from both tables
    // right table columns are prefixed with "right_"
}
```

---

## API Reference

### JoinView

```rust
pub struct JoinView {
    // ...
}
```

#### Constructor

```rust
pub fn new(
    name: String,
    left_table: Rc<RefCell<Table>>,
    right_table: Rc<RefCell<Table>>,
    left_key: String,
    right_key: String,
    join_type: JoinType,
) -> Result<Self, String>
```

**Parameters:**
- `name` - Name for the join view
- `left_table` - Left table (shared reference)
- `right_table` - Right table (shared reference)
- `left_key` - Column name in left table to join on
- `right_key` - Column name in right table to join on
- `join_type` - `JoinType::Left` or `JoinType::Inner`

**Returns:**
- `Ok(JoinView)` if successful
- `Err(String)` if columns don't exist

#### Methods

```rust
pub fn len(&self) -> usize
pub fn is_empty(&self) -> bool
pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String>
pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String>
pub fn refresh(&mut self)  // Rebuild join after table changes
pub fn name(&self) -> &str
pub fn join_type(&self) -> JoinType
```

### JoinType

```rust
pub enum JoinType {
    Left,   // All rows from left, matched rows from right
    Inner,  // Only rows that match in both tables
}
```

---

## Column Naming

To avoid conflicts, columns from the right table are prefixed with `right_`:

```rust
// Left table columns: user_id, name, email
// Right table columns: order_id, user_id, amount

// Joined row contains:
// - user_id, name, email (from left)
// - right_order_id, right_user_id, right_amount (from right)
```

**Accessing columns:**
```rust
let row = joined.get_row(0)?;

// Left table columns (no prefix)
let name = row.get("name").unwrap();
let email = row.get("email").unwrap();

// Right table columns (prefixed with "right_")
let order_id = row.get("right_order_id").unwrap();
let amount = row.get("right_amount").unwrap();
```

---

## Examples

### Example 1: Basic Left Join

```rust
use livetable::{Table, Schema, ColumnType, ColumnValue, JoinView, JoinType};
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

// Create users table
let users_schema = Schema::new(vec![
    ("user_id".to_string(), ColumnType::Int32, false),
    ("name".to_string(), ColumnType::String, false),
]);
let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));

// Add users
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

// Create orders table
let orders_schema = Schema::new(vec![
    ("order_id".to_string(), ColumnType::Int32, false),
    ("user_id".to_string(), ColumnType::Int32, false),
    ("amount".to_string(), ColumnType::Float64, false),
]);
let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));

// Add orders (only for Alice)
{
    let mut o = orders.borrow_mut();

    let mut row = HashMap::new();
    row.insert("order_id".to_string(), ColumnValue::Int32(101));
    row.insert("user_id".to_string(), ColumnValue::Int32(1));
    row.insert("amount".to_string(), ColumnValue::Float64(99.99));
    o.append_row(row).unwrap();
}

// Left join
let joined = JoinView::new(
    "user_orders".to_string(),
    users.clone(),
    orders.clone(),
    "user_id".to_string(),
    "user_id".to_string(),
    JoinType::Left,
).unwrap();

// Result: 2 rows
// - Alice with order_id=101, amount=99.99
// - Bob with order_id=NULL, amount=NULL
assert_eq!(joined.len(), 2);
```

### Example 2: Inner Join

```rust
// Same setup as above, but use Inner join
let joined = JoinView::new(
    "user_orders".to_string(),
    users.clone(),
    orders.clone(),
    "user_id".to_string(),
    "user_id".to_string(),
    JoinType::Inner,  // Inner instead of Left
).unwrap();

// Result: 1 row
// - Only Alice (Bob excluded because he has no orders)
assert_eq!(joined.len(), 1);

let row = joined.get_row(0).unwrap();
assert_eq!(row.get("name").unwrap().as_string(), Some("Alice"));
assert_eq!(row.get("right_order_id").unwrap().as_i32(), Some(101));
```

### Example 3: Handling NULLs in Left Join

```rust
// Using left join from Example 1
for i in 0..joined.len() {
    let row = joined.get_row(i).unwrap();
    let name = row.get("name").unwrap().as_string().unwrap();

    match row.get("right_amount") {
        Some(ColumnValue::Float64(amount)) => {
            println!("{} spent ${:.2}", name, amount);
        }
        Some(ColumnValue::Null) => {
            println!("{} has no orders", name);
        }
        _ => {}
    }
}

// Output:
// Alice spent $99.99
// Bob has no orders
```

### Example 4: Multiple Matches (One-to-Many)

```rust
// If Alice has 2 orders:
// - Order 101: $99.99
// - Order 102: $49.99

// Left join will produce 2 rows for Alice (one per order)
let joined = JoinView::new(..., JoinType::Left).unwrap();

// Result: 3 rows total
// - Alice with order 101
// - Alice with order 102
// - Bob with NULL
assert_eq!(joined.len(), 3);
```

### Example 5: Refresh After Changes

```rust
let mut joined = JoinView::new(..., JoinType::Left).unwrap();

// Initially: Alice has 1 order, Bob has 0
assert_eq!(joined.len(), 2);

// Add order for Bob
{
    let mut o = orders.borrow_mut();
    let mut row = HashMap::new();
    row.insert("order_id".to_string(), ColumnValue::Int32(102));
    row.insert("user_id".to_string(), ColumnValue::Int32(2));
    row.insert("amount".to_string(), ColumnValue::Float64(59.99));
    o.append_row(row).unwrap();
}

// Refresh to see new data
joined.refresh();

// Now: Alice has 1 order, Bob has 1 order
assert_eq!(joined.len(), 2);

let row1 = joined.get_row(1).unwrap();
assert_eq!(row1.get("name").unwrap().as_string(), Some("Bob"));
assert_eq!(row1.get("right_order_id").unwrap().as_i32(), Some(102));
```

### Example 6: Aggregating Joined Data

```rust
// Calculate total spending per user
use std::collections::HashMap;

let joined = JoinView::new(..., JoinType::Left).unwrap();

let mut totals: HashMap<String, f64> = HashMap::new();

for i in 0..joined.len() {
    let row = joined.get_row(i).unwrap();
    let name = row.get("name").unwrap().as_string().unwrap().to_string();

    let amount = match row.get("right_amount") {
        Some(ColumnValue::Float64(a)) => *a,
        _ => 0.0,  // NULL or missing = $0
    };

    *totals.entry(name).or_insert(0.0) += amount;
}

// totals now contains: {"Alice": 99.99, "Bob": 0.0}
```

---

## Performance

### Time Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Construction | O(N + M) | N = left rows, M = right rows |
| get_row | O(1) | Index lookup |
| get_value | O(1) | Index lookup |
| refresh | O(N + M) | Rebuilds join index |

### Space Complexity

| Component | Space | Notes |
|-----------|-------|-------|
| Join Index | O(R) | R = result rows |
| Right Index | O(M) | Temporary during build |

### Optimization Tips

1. **Use Inner Join when possible** - Smaller result set
2. **Index right table first** - Better for large right tables
3. **Refresh sparingly** - Only when tables change
4. **Materialize if static** - Convert to Table if data doesn't change

---

## Comparison with SQL

### Left Join
```sql
-- SQL
SELECT * FROM users
LEFT JOIN orders ON users.user_id = orders.user_id

-- LiveTable
JoinView::new(
    "result".to_string(),
    users, orders,
    "user_id".to_string(), "user_id".to_string(),
    JoinType::Left
)
```

### Inner Join
```sql
-- SQL
SELECT * FROM users
INNER JOIN orders ON users.user_id = orders.user_id

-- LiveTable
JoinView::new(
    "result".to_string(),
    users, orders,
    "user_id".to_string(), "user_id".to_string(),
    JoinType::Inner
)
```

---

## Known Limitations

### Not Yet Supported

❌ **Right Join** - Use Left Join with swapped tables
❌ **Full Outer Join** - Not implemented
❌ **Cross Join** - Not implemented
❌ **Self Join** - Would need to clone table
❌ **Multiple Join Conditions** - Only single column joins
❌ **Join on expressions** - Only exact column matches

### Workarounds

**Right Join:**
```rust
// Instead of: RIGHT JOIN orders
// Do: LEFT JOIN with swapped arguments
JoinView::new(..., orders, users, ..., JoinType::Left)
```

**Multiple Conditions:**
```rust
// Filter after joining
let joined = JoinView::new(...).unwrap();
let filtered = FilterView::new(..., |row| {
    // Additional conditions here
});
```

---

## Future Enhancements

Planned for future versions:

- [ ] Right Join support
- [ ] Full Outer Join
- [ ] Cross Join
- [ ] Multiple join keys
- [ ] Join hints for optimization
- [ ] Parallel join execution
- [ ] Join on computed columns

---

## Testing

Run join tests:
```bash
cargo test --lib test_left_join
cargo test --lib test_inner_join
cargo test --lib test_join_refresh
```

Run join example:
```bash
cargo run --example joins
```

---

## See Also

- [API_GUIDE.md](API_GUIDE.md) - Complete API documentation
- [examples/joins.rs](examples/joins.rs) - Comprehensive examples
- [src/view.rs](src/view.rs) - Implementation details

---

*Added in version 0.1.0*
*Last updated: 2025-01-15*
