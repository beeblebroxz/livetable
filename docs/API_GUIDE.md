# LiveTable Rust API Guide

Complete guide to using the LiveTable high-performance table database system in Rust.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [API Reference](#api-reference)
4. [Examples](#examples)
5. [Performance Tuning](#performance-tuning)
6. [Best Practices](#best-practices)

---

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
livetable = { path = "path/to/livetable" }
```

### Basic Usage

```rust
use livetable::{Table, Schema, ColumnType, ColumnValue};
use std::collections::HashMap;

// Create a schema
let schema = Schema::new(vec![
    ("id".to_string(), ColumnType::Int32, false),
    ("name".to_string(), ColumnType::String, false),
]);

// Create a table
let mut table = Table::new("users".to_string(), schema);

// Add data
let mut row = HashMap::new();
row.insert("id".to_string(), ColumnValue::Int32(1));
row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
table.append_row(row).unwrap();

// Query data
let value = table.get_value(0, "name").unwrap();
assert_eq!(value.as_string(), Some("Alice"));
```

---

## Core Concepts

### 1. Sequences

Sequences are the low-level storage layer. Two implementations:

- **ArraySequence**: Simple contiguous array
  - O(1) random access (fastest)
  - O(N) insert/delete
  - Minimal memory overhead

- **TieredVectorSequence**: Chunked storage with indirection
  - O(1) random access (small overhead)
  - O(sqrt(N)) insert/delete
  - ~2*sqrt(N) memory overhead

### 2. Columns

Typed containers built on sequences. Support 6 types:

- `Int32` / `Int64` - Signed integers
- `Float32` / `Float64` - Floating point
- `String` - UTF-8 strings
- `Bool` - Boolean values
- `Null` - For nullable columns

### 3. Tables

Collections of columns with a schema. Provide:

- Row-level operations
- Type safety
- Nullable support
- Iterator support

### 4. Views

Read-only derived tables:

- **FilterView** - Filter rows by predicate
- **ProjectionView** - Select specific columns
- **ComputedView** - Add calculated columns

---

## API Reference

### Schema

#### Constructor

```rust
pub fn new(columns: Vec<(String, ColumnType, bool)>) -> Self
```

Creates a schema with column definitions. Each tuple is:
- `String` - Column name
- `ColumnType` - Data type
- `bool` - Nullable (true = nullable)

#### Methods

```rust
pub fn len(&self) -> usize
pub fn is_empty(&self) -> bool
pub fn get_column_names(&self) -> Vec<&str>
pub fn get_column_index(&self, name: &str) -> Option<usize>
pub fn get_column_info(&self, index: usize) -> Option<(&str, ColumnType, bool)>
```

### Table

#### Constructors

```rust
pub fn new(name: String, schema: Schema) -> Self
pub fn new_with_options(name: String, schema: Schema, use_tiered_vector: bool) -> Self
```

Create a table. Use `new_with_options` with `use_tiered_vector=true` for better insert/delete performance.

#### Query Methods

```rust
pub fn len(&self) -> usize
pub fn is_empty(&self) -> bool
pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String>
pub fn get_row(&self, row: usize) -> Result<HashMap<String, ColumnValue>, String>
pub fn iter_rows(&self) -> TableRowIterator
```

#### Mutation Methods

```rust
pub fn append_row(&mut self, row: HashMap<String, ColumnValue>) -> Result<(), String>
pub fn insert_row(&mut self, index: usize, row: HashMap<String, ColumnValue>) -> Result<(), String>
pub fn delete_row(&mut self, index: usize) -> Result<HashMap<String, ColumnValue>, String>
pub fn set_value(&mut self, row: usize, column: &str, value: ColumnValue) -> Result<(), String>
```

### FilterView

```rust
pub fn new<F>(name: String, parent: Rc<RefCell<Table>>, predicate: F) -> Self
where
    F: Fn(&HashMap<String, ColumnValue>) -> bool + 'static
```

Create a filtered view. The predicate function receives each row and returns true/false.

```rust
pub fn len(&self) -> usize
pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String>
pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String>
pub fn refresh(&mut self)  // Call after parent table changes
```

### ProjectionView

```rust
pub fn new(
    name: String,
    parent: Rc<RefCell<Table>>,
    columns: Vec<String>
) -> Result<Self, String>
```

Create a projection (column selection) view.

```rust
pub fn len(&self) -> usize
pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String>
pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String>
pub fn columns(&self) -> &[String]
```

### ComputedView

```rust
pub fn new<F>(
    name: String,
    parent: Rc<RefCell<Table>>,
    computed_col_name: String,
    compute_func: F,
) -> Self
where
    F: Fn(&HashMap<String, ColumnValue>) -> ColumnValue + 'static
```

Create a view with a computed column.

```rust
pub fn len(&self) -> usize
pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String>
pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String>
```

### ColumnValue

Enum representing all supported types:

```rust
pub enum ColumnValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bool(bool),
    Null,
}
```

#### Helper Methods

```rust
pub fn as_i32(&self) -> Option<i32>
pub fn as_i64(&self) -> Option<i64>
pub fn as_f32(&self) -> Option<f32>
pub fn as_f64(&self) -> Option<f64>
pub fn as_string(&self) -> Option<&str>
pub fn as_bool(&self) -> Option<bool>
pub fn is_null(&self) -> bool
```

---

## Examples

### Example 1: Basic CRUD Operations

```rust
use livetable::{Table, Schema, ColumnType, ColumnValue};
use std::collections::HashMap;

let schema = Schema::new(vec![
    ("id".to_string(), ColumnType::Int32, false),
    ("name".to_string(), ColumnType::String, false),
    ("active".to_string(), ColumnType::Bool, false),
]);

let mut table = Table::new("users".to_string(), schema);

// CREATE
let mut row = HashMap::new();
row.insert("id".to_string(), ColumnValue::Int32(1));
row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
row.insert("active".to_string(), ColumnValue::Bool(true));
table.append_row(row).unwrap();

// READ
let value = table.get_value(0, "name").unwrap();
println!("Name: {}", value.as_string().unwrap());

// UPDATE
table.set_value(0, "active", ColumnValue::Bool(false)).unwrap();

// DELETE
let deleted = table.delete_row(0).unwrap();
```

### Example 2: Nullable Columns

```rust
let schema = Schema::new(vec![
    ("id".to_string(), ColumnType::Int32, false),
    ("email".to_string(), ColumnType::String, true),  // Nullable
]);

let mut table = Table::new("users".to_string(), schema);

let mut row = HashMap::new();
row.insert("id".to_string(), ColumnValue::Int32(1));
row.insert("email".to_string(), ColumnValue::Null);  // No email
table.append_row(row).unwrap();

// Check for null
let email = table.get_value(0, "email").unwrap();
if email.is_null() {
    println!("No email provided");
}
```

### Example 3: FilterView

```rust
use std::rc::Rc;
use std::cell::RefCell;
use livetable::FilterView;

let table = Rc::new(RefCell::new(Table::new("products".to_string(), schema)));

// Add products...

// Filter for expensive items
let expensive = FilterView::new(
    "expensive".to_string(),
    table.clone(),
    |row| {
        if let Some(ColumnValue::Float64(price)) = row.get("price") {
            *price > 100.0
        } else {
            false
        }
    }
);

println!("Expensive items: {}", expensive.len());
```

### Example 4: ProjectionView

```rust
let public_view = ProjectionView::new(
    "public".to_string(),
    table.clone(),
    vec!["id".to_string(), "name".to_string()],  // Exclude sensitive columns
).unwrap();

// Can only access projected columns
let row = public_view.get_row(0).unwrap();
assert!(row.contains_key("id"));
assert!(row.contains_key("name"));
assert!(!row.contains_key("password"));  // Excluded
```

### Example 5: ComputedView

```rust
let with_total = ComputedView::new(
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
    }
);

let total = with_total.get_value(0, "total").unwrap();
```

### Example 6: Iteration

```rust
// Iterate over all rows
for (i, row) in table.iter_rows().enumerate() {
    println!("Row {}: {:?}", i, row);
}

// Or manually
for i in 0..table.len() {
    let row = table.get_row(i).unwrap();
    // Process row...
}
```

### Example 7: Performance Optimization

```rust
// For insert-heavy workloads, use TieredVector
let mut table = Table::new_with_options(
    "logs".to_string(),
    schema,
    true  // use_tiered_vector
);

// Now inserts in the middle are O(sqrt(N)) instead of O(N)
for i in 0..10000 {
    table.insert_row(table.len() / 2, row.clone()).unwrap();
}
```

---

## Performance Tuning

### Choosing Storage Backend

#### Use Array-based (default) when:
- ✅ Read-heavy workloads (99%+ reads)
- ✅ Append-only operations
- ✅ Memory constrained
- ✅ Want fastest possible random access

#### Use TieredVector when:
- ✅ Frequent random inserts/deletes
- ✅ Large tables (> 10,000 rows) with mixed operations
- ✅ Can tolerate ~10-20% memory overhead
- ✅ Need predictable insert performance

### Performance Characteristics

| Operation | Array | TieredVector |
|-----------|-------|--------------|
| Random Access | 500 ps | 2-40 ns |
| Append | ~280 ns | ~560 ns |
| Insert (middle) | O(N) | O(sqrt(N)) |
| Delete | O(N) | O(sqrt(N)) |
| Memory | Minimal | +O(sqrt(N)) |

### Tips

1. **Batch Operations**: Group multiple inserts/deletes
   ```rust
   // Good
   for row in rows {
       table.append_row(row)?;
   }
   ```

2. **Reuse HashMaps**: Avoid allocating for each row
   ```rust
   let mut row = HashMap::new();
   for i in 0..n {
       row.clear();
       row.insert("id".to_string(), ColumnValue::Int32(i));
       table.append_row(row.clone())?;
   }
   ```

3. **Use Views Wisely**: Views add overhead
   - Refresh FilterView only when needed
   - Chain views sparingly
   - Consider materializing frequently-accessed views

4. **Pattern Matching**: Use `match` for type extraction
   ```rust
   // Good
   match value {
       ColumnValue::Int32(i) => println!("{}", i),
       ColumnValue::String(s) => println!("{}", s),
       _ => {}
   }

   // Less efficient
   if let Some(i) = value.as_i32() {
       println!("{}", i);
   } else if let Some(s) = value.as_string() {
       println!("{}", s);
   }
   ```

---

## Best Practices

### 1. Error Handling

Always handle `Result` types:

```rust
// Good
match table.append_row(row) {
    Ok(()) => println!("Success"),
    Err(e) => eprintln!("Error: {}", e),
}

// Or use ? operator
table.append_row(row)?;
```

### 2. Schema Design

```rust
// Good: Clear names, appropriate types
let schema = Schema::new(vec![
    ("user_id".to_string(), ColumnType::Int64, false),
    ("email".to_string(), ColumnType::String, false),
    ("age".to_string(), ColumnType::Int32, true),  // Optional
]);

// Avoid: Unclear names, wrong types
let schema = Schema::new(vec![
    ("id".to_string(), ColumnType::String, false),  // IDs should be numeric
    ("data".to_string(), ColumnType::String, false),  // Too generic
]);
```

### 3. View Ownership

Use `Rc<RefCell<>>` for shared table access:

```rust
use std::rc::Rc;
use std::cell::RefCell;

let table = Rc::new(RefCell::new(Table::new(...)));

// Multiple views can reference the same table
let view1 = FilterView::new(..., table.clone(), ...);
let view2 = ProjectionView::new(..., table.clone(), ...);

// Modify table through RefCell
{
    let mut t = table.borrow_mut();
    t.append_row(row)?;
}

// Refresh views if needed
view1.refresh();
```

### 4. Type Safety

Leverage Rust's type system:

```rust
// Extract values safely
let value = table.get_value(0, "age")?;
if let ColumnValue::Int32(age) = value {
    if age >= 18 {
        println!("Adult");
    }
}

// Or use helper methods
if let Some(age) = value.as_i32() {
    println!("Age: {}", age);
}
```

### 5. Memory Management

```rust
// Good: Drop large tables when done
{
    let mut temp_table = Table::new(...);
    // Use table...
}  // Table dropped here, memory freed

// Good: Clear views you no longer need
drop(expensive_view);
```

### 6. Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_operations() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
        ]);
        let mut table = Table::new("test".to_string(), schema);

        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(42));
        table.append_row(row).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(
            table.get_value(0, "id").unwrap().as_i32(),
            Some(42)
        );
    }
}
```

---

## Common Patterns

### Pattern 1: ETL Pipeline

```rust
// Extract from source
let raw_data = load_csv("data.csv");

// Transform with computed view
let transformed = ComputedView::new(
    "transformed".to_string(),
    raw_data.clone(),
    "normalized_price".to_string(),
    |row| {
        // Normalization logic
        ColumnValue::Float64(normalize(row))
    }
);

// Load with filtering
let valid_only = FilterView::new(
    "valid".to_string(),
    raw_data.clone(),
    |row| validate(row)
);
```

### Pattern 2: Data Validation

```rust
let errors = FilterView::new(
    "errors".to_string(),
    table.clone(),
    |row| {
        // Validation rules
        match (row.get("email"), row.get("age")) {
            (Some(ColumnValue::String(e)), Some(ColumnValue::Int32(a))) => {
                !e.contains('@') || *a < 0
            }
            _ => true,  // Missing required fields
        }
    }
);

if errors.len() > 0 {
    println!("Found {} validation errors", errors.len());
}
```

### Pattern 3: Reporting

```rust
// Create summary views
let summary = ComputedView::new(
    "summary".to_string(),
    sales.clone(),
    "profit_margin".to_string(),
    |row| {
        let revenue = row.get("revenue").unwrap().as_f64().unwrap();
        let cost = row.get("cost").unwrap().as_f64().unwrap();
        ColumnValue::Float64((revenue - cost) / revenue * 100.0)
    }
);

// Filter high performers
let top_performers = FilterView::new(
    "top".to_string(),
    sales.clone(),
    |row| {
        if let Some(ColumnValue::Float64(margin)) = row.get("profit_margin") {
            *margin > 20.0
        } else {
            false
        }
    }
);
```

---

## Limitations & Roadmap

### Current Limitations

1. **No GroupBy/Aggregations** (yet)
   - Workaround: Manual aggregation in application code

2. **Manual View Refresh**
   - Views don't auto-update when parent changes
   - Must call `refresh()` on FilterView

3. **Single-threaded**
   - No parallel operations
   - Future: Rayon integration

### Planned Features

- ✨ GroupBy and aggregations
- ✨ Join operations
- ✨ Sorting and indexing
- ✨ Persistence (save/load)
- ✨ SQL-like query interface
- ✨ Parallel operations

---

## API Stability

Current version: **0.1.0** (Alpha)

- Core APIs (Table, Schema, Sequence, Column) are stable
- View APIs may change as we add auto-propagation
- No breaking changes to storage format planned

---

## Getting Help

- **Examples**: See `examples/` directory
- **Tests**: Check `src/*/tests` modules
- **Benchmarks**: Run `cargo bench`
- **Documentation**: Run `cargo doc --open`

---

## Performance Comparison

LiveTable Rust vs Python implementation:

- **10-1000x faster** across most operations
- **Sub-nanosecond** random access (500 picoseconds!)
- **Better scaling** characteristics
- **Lower memory** usage (3-6x less)

See [PERFORMANCE_COMPARISON.md](PERFORMANCE_COMPARISON.md) for detailed benchmarks.
