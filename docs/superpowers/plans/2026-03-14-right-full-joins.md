# RIGHT/FULL OUTER Joins + tick() Integration — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add RIGHT and FULL OUTER join types to JoinView and integrate JoinView with tick() auto-propagation.

**Architecture:** Extend `join_index` from `Vec<(usize, Option<usize>)>` to `Vec<(Option<usize>, Option<usize>)>`. Add `Right` and `Full` variants to `JoinType`. Register JoinViews with both parent tables using `JoinLeft`/`JoinRight` variants in `RegisteredView` so `tick()` propagates changes and compacts changesets correctly. Expose `.sync()` to Python.

**Tech Stack:** Rust, PyO3, maturin, pytest

**Spec:** `docs/superpowers/specs/2026-03-14-right-full-joins-design.md`

---

## Chunk 1: Rust Core — JoinType, Data Model, Rebuild Algorithm

### Task 1: Add Right and Full variants to JoinType enum

**Files:**
- Modify: `impl/src/view.rs:348-355` (JoinType enum)

- [ ] **Step 1: Add new variants to JoinType**

In `impl/src/view.rs`, replace lines 348-355:

```rust
/// Join type specification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Left join: All rows from left table, matched rows from right (nulls if no match)
    Left,
    /// Inner join: Only rows that match in both tables
    Inner,
    /// Right join: All rows from right table, matched rows from left (nulls if no match)
    Right,
    /// Full outer join: All rows from both tables (nulls where no match)
    Full,
}
```

- [ ] **Step 2: Fix exhaustive match warnings**

The new variants will cause `match` exhaustiveness errors. Find all `match self.join_type` blocks in `view.rs`. There are two:

1. `rebuild_index()` at line 630-638 — will be updated in Task 3
2. `sync()` at line 888 — will be updated in Task 6

For now, add temporary `Right | Full => todo!()` arms to both so the code compiles:

At line 630, after `JoinType::Inner => {}`:
```rust
JoinType::Right => {
    // Skip left row if no right match (will be included from right side)
    todo!("RIGHT join rebuild")
}
JoinType::Full => {
    // Include left row with null right values
    todo!("FULL join rebuild")
}
```

At line 888, after `} else if self.join_type == JoinType::Left {`:
No changes needed yet — this uses `==` not `match`.

- [ ] **Step 3: Run Rust tests to verify compilation**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib -- --test-threads=1 test_inner_join 2>&1 | tail -5`
Expected: PASS (inner join doesn't hit the todo!() arms)

- [ ] **Step 4: Commit**

```bash
cd /Users/abhishekgulati/projects/livetable
git add impl/src/view.rs
git commit -m "feat: add Right and Full variants to JoinType enum"
```

---

### Task 2: Change join_index to (Option<usize>, Option<usize>)

**Files:**
- Modify: `impl/src/view.rs:396-415` (JoinView struct)
- Modify: `impl/src/view.rs:671-694` (insert position helpers)
- Modify: `impl/src/view.rs:710-769` (get_row, get_value)
- Modify: `impl/src/view.rs:786-791` (last_processed_change_count)

- [ ] **Step 1: Write failing tests for right/full join get_row with NULL left columns**

Add at end of `#[cfg(test)] mod tests` in `impl/src/view.rs` (after existing join tests, around line 3387):

```rust
#[test]
fn test_right_join() {
    // Users: Alice(1), Bob(2), Charlie(3)
    // Orders: order for Alice(1), order for Dave(4)
    // RIGHT JOIN → Alice+order, Dave+order (Bob and Charlie excluded)
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    {
        let mut u = users.borrow_mut();
        let mut r = HashMap::new();
        r.insert("user_id".to_string(), ColumnValue::Int32(1));
        r.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        u.append_row(r).unwrap();
        let mut r = HashMap::new();
        r.insert("user_id".to_string(), ColumnValue::Int32(2));
        r.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        u.append_row(r).unwrap();
        let mut r = HashMap::new();
        r.insert("user_id".to_string(), ColumnValue::Int32(3));
        r.insert("name".to_string(), ColumnValue::String("Charlie".to_string()));
        u.append_row(r).unwrap();
    }

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));
    {
        let mut o = orders.borrow_mut();
        let mut r = HashMap::new();
        r.insert("order_id".to_string(), ColumnValue::Int32(101));
        r.insert("user_id".to_string(), ColumnValue::Int32(1));
        r.insert("amount".to_string(), ColumnValue::Float64(99.99));
        o.append_row(r).unwrap();
        // Dave (user_id=4) has no user record
        let mut r = HashMap::new();
        r.insert("order_id".to_string(), ColumnValue::Int32(102));
        r.insert("user_id".to_string(), ColumnValue::Int32(4));
        r.insert("amount".to_string(), ColumnValue::Float64(49.99));
        o.append_row(r).unwrap();
    }

    let joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Right,
    ).unwrap();

    // RIGHT JOIN: 2 rows — Alice matched, Dave unmatched
    assert_eq!(joined.len(), 2);

    // Row 0: Alice matched with order 101
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    assert_eq!(row0.get("right_order_id").unwrap().as_i32(), Some(101));

    // Row 1: Dave's order — no user match, left columns are NULL
    let row1 = joined.get_row(1).unwrap();
    assert!(row1.get("name").unwrap().is_null());
    assert!(row1.get("user_id").unwrap().is_null());
    assert_eq!(row1.get("right_order_id").unwrap().as_i32(), Some(102));
    assert_eq!(row1.get("right_amount").unwrap().as_f64(), Some(49.99));
}

#[test]
fn test_full_join() {
    // Users: Alice(1), Bob(2)
    // Orders: order for Alice(1), order for Dave(4)
    // FULL JOIN → Alice+order, Bob+NULL, NULL+Dave's order
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    {
        let mut u = users.borrow_mut();
        let mut r = HashMap::new();
        r.insert("user_id".to_string(), ColumnValue::Int32(1));
        r.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        u.append_row(r).unwrap();
        let mut r = HashMap::new();
        r.insert("user_id".to_string(), ColumnValue::Int32(2));
        r.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        u.append_row(r).unwrap();
    }

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
        ("amount".to_string(), ColumnType::Float64, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));
    {
        let mut o = orders.borrow_mut();
        let mut r = HashMap::new();
        r.insert("order_id".to_string(), ColumnValue::Int32(101));
        r.insert("user_id".to_string(), ColumnValue::Int32(1));
        r.insert("amount".to_string(), ColumnValue::Float64(99.99));
        o.append_row(r).unwrap();
        let mut r = HashMap::new();
        r.insert("order_id".to_string(), ColumnValue::Int32(102));
        r.insert("user_id".to_string(), ColumnValue::Int32(4));
        r.insert("amount".to_string(), ColumnValue::Float64(49.99));
        o.append_row(r).unwrap();
    }

    let joined = JoinView::new(
        "user_orders".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Full,
    ).unwrap();

    // FULL JOIN: 3 rows — Alice matched, Bob unmatched left, Dave unmatched right
    assert_eq!(joined.len(), 3);

    // Row 0: Alice matched
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    assert_eq!(row0.get("right_order_id").unwrap().as_i32(), Some(101));

    // Row 1: Bob unmatched — right columns NULL
    let row1 = joined.get_row(1).unwrap();
    assert_eq!(row1.get("name").unwrap().as_string(), Some("Bob"));
    assert!(row1.get("right_order_id").unwrap().is_null());

    // Row 2: Dave unmatched — left columns NULL
    let row2 = joined.get_row(2).unwrap();
    assert!(row2.get("name").unwrap().is_null());
    assert_eq!(row2.get("right_order_id").unwrap().as_i32(), Some(102));
    assert_eq!(row2.get("right_amount").unwrap().as_f64(), Some(49.99));
}

#[test]
fn test_right_join_multiple_matches() {
    // Users: Alice(1), Alice2(1) — two users with same ID
    // Orders: order for user 1
    // RIGHT JOIN → both Alices matched to the order
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    {
        let mut u = users.borrow_mut();
        let mut r = HashMap::new();
        r.insert("user_id".to_string(), ColumnValue::Int32(1));
        r.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        u.append_row(r).unwrap();
        let mut r = HashMap::new();
        r.insert("user_id".to_string(), ColumnValue::Int32(1));
        r.insert("name".to_string(), ColumnValue::String("Alice2".to_string()));
        u.append_row(r).unwrap();
    }

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));
    {
        let mut o = orders.borrow_mut();
        let mut r = HashMap::new();
        r.insert("order_id".to_string(), ColumnValue::Int32(101));
        r.insert("user_id".to_string(), ColumnValue::Int32(1));
        o.append_row(r).unwrap();
    }

    let joined = JoinView::new(
        "test".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Right,
    ).unwrap();

    // Both left rows match the single right row
    assert_eq!(joined.len(), 2);
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    let row1 = joined.get_row(1).unwrap();
    assert_eq!(row1.get("name").unwrap().as_string(), Some("Alice2"));
}

#[test]
fn test_full_join_no_matches() {
    // Users: Alice(1)
    // Orders: order for Dave(4)
    // FULL JOIN with no overlap → 2 rows, all cross-columns NULL
    let users_schema = Schema::new(vec![
        ("user_id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
    {
        let mut u = users.borrow_mut();
        let mut r = HashMap::new();
        r.insert("user_id".to_string(), ColumnValue::Int32(1));
        r.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        u.append_row(r).unwrap();
    }

    let orders_schema = Schema::new(vec![
        ("order_id".to_string(), ColumnType::Int32, false),
        ("user_id".to_string(), ColumnType::Int32, false),
    ]);
    let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));
    {
        let mut o = orders.borrow_mut();
        let mut r = HashMap::new();
        r.insert("order_id".to_string(), ColumnValue::Int32(201));
        r.insert("user_id".to_string(), ColumnValue::Int32(4));
        o.append_row(r).unwrap();
    }

    let joined = JoinView::new(
        "test".to_string(),
        users.clone(),
        orders.clone(),
        "user_id".to_string(),
        "user_id".to_string(),
        JoinType::Full,
    ).unwrap();

    assert_eq!(joined.len(), 2);

    // Row 0: Alice, no right match
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    assert!(row0.get("right_order_id").unwrap().is_null());

    // Row 1: Dave's order, no left match
    let row1 = joined.get_row(1).unwrap();
    assert!(row1.get("name").unwrap().is_null());
    assert_eq!(row1.get("right_order_id").unwrap().as_i32(), Some(201));
}

#[test]
fn test_full_join_null_key_rows() {
    // Left table has a NULL key row, right table has a NULL key row
    // FULL JOIN: NULL keys match nothing, but both rows should appear
    let left_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, true),
        ("val".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut t = left.borrow_mut();
        let mut r = HashMap::new();
        r.insert("id".to_string(), ColumnValue::Int32(1));
        r.insert("val".to_string(), ColumnValue::String("A".to_string()));
        t.append_row(r).unwrap();
        let mut r = HashMap::new();
        r.insert("id".to_string(), ColumnValue::Null);
        r.insert("val".to_string(), ColumnValue::String("B".to_string()));
        t.append_row(r).unwrap();
    }

    let right_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, true),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut t = right.borrow_mut();
        let mut r = HashMap::new();
        r.insert("id".to_string(), ColumnValue::Int32(1));
        r.insert("data".to_string(), ColumnValue::String("X".to_string()));
        t.append_row(r).unwrap();
        let mut r = HashMap::new();
        r.insert("id".to_string(), ColumnValue::Null);
        r.insert("data".to_string(), ColumnValue::String("Y".to_string()));
        t.append_row(r).unwrap();
    }

    let joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    ).unwrap();

    // Row 0: id=1 matched
    // Row 1: left NULL key, unmatched (Some(1), None)
    // Row 2: right NULL key, unmatched (None, Some(1))
    assert_eq!(joined.len(), 3);

    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("val").unwrap().as_string(), Some("A"));
    assert_eq!(row0.get("right_data").unwrap().as_string(), Some("X"));

    let row1 = joined.get_row(1).unwrap();
    assert_eq!(row1.get("val").unwrap().as_string(), Some("B"));
    assert!(row1.get("right_data").unwrap().is_null());

    let row2 = joined.get_row(2).unwrap();
    assert!(row2.get("val").unwrap().is_null());
    assert_eq!(row2.get("right_data").unwrap().as_string(), Some("Y"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib -- test_right_join test_full_join 2>&1 | tail -10`
Expected: FAIL (todo!() panics or compilation errors)

- [ ] **Step 3: Change join_index type**

In `impl/src/view.rs`, change line 406 from:
```rust
    join_index: Vec<(usize, Option<usize>)>,
```
to:
```rust
    join_index: Vec<(Option<usize>, Option<usize>)>,
```

Update the doc comment on line 405 from:
```rust
    /// Cached joined rows: (left_row_index, optional_right_row_index)
```
to:
```rust
    /// Cached joined rows: (optional_left_row_index, optional_right_row_index)
    /// Ordering: all Some(left) entries first (ascending left_idx), then None-left entries (ascending right_idx)
```

- [ ] **Step 4: Update rebuild_index() for all four join types**

Replace `rebuild_index()` (lines 592-648) with:

```rust
    fn rebuild_index(&mut self) {
        self.join_index.clear();

        let left = self.left_table.borrow();
        let right = self.right_table.borrow();

        // Pre-compute column indices for join keys (done once, not per row)
        let left_col_indices: Vec<usize> = self
            .left_keys
            .iter()
            .filter_map(|k| left.schema().get_column_index(k))
            .collect();
        let right_col_indices: Vec<usize> = self
            .right_keys
            .iter()
            .filter_map(|k| right.schema().get_column_index(k))
            .collect();

        // Phase 2: Build right lookup
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();
        for i in 0..right.len() {
            if let Some(key_str) = Self::build_key_from_indices(&right, i, &right_col_indices) {
                right_index.entry(key_str).or_insert_with(Vec::new).push(i);
            }
        }

        // Track which right rows got matched (for RIGHT/FULL)
        let mut matched_right: HashSet<usize> = HashSet::new();

        // Phase 3: Scan left rows
        for i in 0..left.len() {
            match Self::build_key_from_indices(&left, i, &left_col_indices) {
                Some(key_str) => {
                    if let Some(matching_indices) = right_index.get(&key_str) {
                        for &right_idx in matching_indices {
                            self.join_index.push((Some(i), Some(right_idx)));
                            matched_right.insert(right_idx);
                        }
                    } else {
                        // No match — non-NULL key but no right row
                        match self.join_type {
                            JoinType::Left | JoinType::Full => {
                                self.join_index.push((Some(i), None));
                            }
                            JoinType::Inner | JoinType::Right => {}
                        }
                    }
                }
                None => {
                    // NULL key — matches nothing
                    match self.join_type {
                        JoinType::Left | JoinType::Full => {
                            self.join_index.push((Some(i), None));
                        }
                        JoinType::Inner | JoinType::Right => {}
                    }
                }
            }
        }

        // Phase 4: Append unmatched right rows (RIGHT/FULL only)
        match self.join_type {
            JoinType::Right | JoinType::Full => {
                for i in 0..right.len() {
                    if !matched_right.contains(&i) {
                        self.join_index.push((None, Some(i)));
                    }
                }
            }
            JoinType::Left | JoinType::Inner => {}
        }

        // Update generation trackers
        self.left_last_synced = left.changeset_generation();
        self.right_last_synced = right.changeset_generation();
        self.left_last_processed_change_count = left.changeset().total_len();
        self.right_last_processed_change_count = right.changeset().total_len();
    }
```

Add `use std::collections::HashSet;` at the top of the file if not already present.

- [ ] **Step 5: Update get_row() for Option left index**

Replace `get_row()` (lines 710-742) with:

```rust
    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        if index >= self.join_index.len() {
            return Err(format!("Index {} out of range [0, {})", index, self.len()));
        }

        let (left_idx_opt, right_idx_opt) = self.join_index[index];

        let left_schema = self.left_table.borrow().schema().clone();
        let right_schema = self.right_table.borrow().schema().clone();
        let mut result = HashMap::with_capacity(left_schema.len() + right_schema.len());

        // Add all columns from left table (or nulls if no left match)
        if let Some(left_idx) = left_idx_opt {
            let left_row = self.left_table.borrow().get_row(left_idx)?;
            result.extend(left_row);
        } else {
            for col_name in left_schema.get_column_names() {
                result.insert(col_name.to_string(), ColumnValue::Null);
            }
        }

        // Add columns from right table (or nulls if no match)
        if let Some(right_idx) = right_idx_opt {
            let right_row = self.right_table.borrow().get_row(right_idx)?;
            for (col_name, value) in right_row {
                result.insert(format!("right_{}", col_name), value);
            }
        } else {
            for col_name in right_schema.get_column_names() {
                result.insert(format!("right_{}", col_name), ColumnValue::Null);
            }
        }

        Ok(result)
    }
```

- [ ] **Step 6: Update get_value() for Option left index**

Replace `get_value()` (lines 745-769) with:

```rust
    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        if row >= self.join_index.len() {
            return Err(format!("Row {} out of range [0, {})", row, self.len()));
        }

        let (left_idx_opt, right_idx_opt) = self.join_index[row];

        if let Some(right_column) = column.strip_prefix("right_") {
            if self
                .right_table
                .borrow()
                .schema()
                .get_column_index(right_column)
                .is_none()
            {
                return Err(format!("Column '{}' not found in joined view", column));
            }

            return match right_idx_opt {
                Some(right_idx) => self.right_table.borrow().get_value(right_idx, right_column),
                None => Ok(ColumnValue::Null),
            };
        }

        // Left column
        match left_idx_opt {
            Some(left_idx) => self.left_table.borrow().get_value(left_idx, column),
            None => {
                // Verify column exists in left schema
                if self
                    .left_table
                    .borrow()
                    .schema()
                    .get_column_index(column)
                    .is_none()
                {
                    return Err(format!("Column '{}' not found in joined view", column));
                }
                Ok(ColumnValue::Null)
            }
        }
    }
```

- [ ] **Step 7: Update find_left_insert_position and find_right_insert_position**

Replace lines 671-694 with:

```rust
    fn find_left_insert_position(&self, left_idx: usize) -> usize {
        self.join_index
            .iter()
            .position(|(existing_left, _)| match existing_left {
                Some(l) => *l > left_idx,
                None => true, // None-left entries are always after all Some entries
            })
            .unwrap_or(self.join_index.len())
    }

    fn find_right_insert_position(&self, left_idx: usize, right_idx: usize) -> usize {
        self.join_index
            .iter()
            .position(|(existing_left, existing_right)| {
                match existing_left {
                    None => return true, // None-left entries are always after
                    Some(l) => {
                        if *l > left_idx {
                            return true;
                        }
                        if *l < left_idx {
                            return false;
                        }
                        // Same left index — order by right index
                        match existing_right {
                            Some(existing_right_idx) => *existing_right_idx > right_idx,
                            None => true,
                        }
                    }
                }
            })
            .unwrap_or(self.join_index.len())
    }
```

- [ ] **Step 8: Fix all remaining type errors in sync()**

In `sync()` (lines 798-968), the `join_index` iteration now yields `(Option<usize>, Option<usize>)`. Update the left-table insert handling (line 871):

Change:
```rust
for (left_idx, _) in self.join_index.iter_mut() {
    if *left_idx >= *index {
        *left_idx += 1;
    }
}
```
To:
```rust
for (left_idx_opt, _) in self.join_index.iter_mut() {
    if let Some(left_idx) = left_idx_opt {
        if *left_idx >= *index {
            *left_idx += 1;
        }
    }
}
```

Update the insert call (line 885):
Change `(*index, Some(right_idx))` to `(Some(*index), Some(right_idx))`

Update line 889 (left join no-match):
Change `(*index, None)` to `(Some(*index), None)`

Update right-table insert handling (line 904):
Change:
```rust
for (_, right_opt) in self.join_index.iter_mut() {
```
(This is already fine — `right_opt` is still `&mut Option<usize>`)

Update line 936 (finding existing null match):
Change:
```rust
.position(|(l, r)| *l == left_idx && r.is_none());
```
To:
```rust
.position(|(l, r)| *l == Some(left_idx) && r.is_none());
```

Update line 939:
Change `(left_idx, Some(*right_idx))` to `(Some(left_idx), Some(*right_idx))`

Update line 944 and 950:
Change `(left_idx, Some(*right_idx))` to `(Some(left_idx), Some(*right_idx))`

- [ ] **Step 9: Fix existing test destructuring patterns**

Existing tests that destructure `join_index` directly won't be affected since they use `get_row()`. But if there are any internal tests that access `join_index` directly, update them. Check by compiling.

- [ ] **Step 10: Run all tests**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib 2>&1 | tail -20`
Expected: All existing tests PASS, new tests (test_right_join, test_full_join, etc.) PASS

- [ ] **Step 11: Commit**

```bash
cd /Users/abhishekgulati/projects/livetable
git add impl/src/view.rs
git commit -m "feat: implement RIGHT and FULL OUTER joins in Rust core

Change join_index to (Option<usize>, Option<usize>).
Unified 4-phase rebuild with matched_right tracking.
Update get_row/get_value for symmetric NULL handling.
Fix NULL-key left row bug (previously silently dropped)."
```

---

## Chunk 2: Incremental Sync + Delete Rebuild Tests

### Task 3: Extend incremental sync for RIGHT/FULL joins

**Files:**
- Modify: `impl/src/view.rs` — `sync()` method (lines 798-968)

- [ ] **Step 1: Write failing sync tests**

Add to the test module in `impl/src/view.rs`:

```rust
#[test]
fn test_right_join_sync_left_insert() {
    let left_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));

    let right_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut r = right.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("data".to_string(), ColumnValue::String("X".to_string()));
        r.append_row(row).unwrap();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(2));
        row.insert("data".to_string(), ColumnValue::String("Y".to_string()));
        r.append_row(row).unwrap();
    }

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    ).unwrap();

    // Initially: 2 unmatched right rows
    assert_eq!(joined.len(), 2);

    // Insert a left row matching right id=1
    {
        let mut l = left.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        l.append_row(row).unwrap();
    }

    assert!(joined.sync());

    // Verify incremental matches full rebuild
    let mut rebuilt = JoinView::new(
        "rebuilt".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    ).unwrap();

    assert_eq!(joined.len(), rebuilt.len());
    for i in 0..joined.len() {
        assert_eq!(joined.get_row(i).unwrap(), rebuilt.get_row(i).unwrap(),
            "Row {} mismatch between sync and rebuild", i);
    }
}

#[test]
fn test_right_join_sync_right_insert() {
    let left_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut l = left.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        l.append_row(row).unwrap();
    }

    let right_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    ).unwrap();

    // Initially: empty (RIGHT join with no right rows)
    assert_eq!(joined.len(), 0);

    // Insert right rows — one matching, one not
    {
        let mut r = right.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("data".to_string(), ColumnValue::String("X".to_string()));
        r.append_row(row).unwrap();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(99));
        row.insert("data".to_string(), ColumnValue::String("Z".to_string()));
        r.append_row(row).unwrap();
    }

    assert!(joined.sync());

    let rebuilt = JoinView::new(
        "rebuilt".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    ).unwrap();

    assert_eq!(joined.len(), rebuilt.len());
    for i in 0..joined.len() {
        assert_eq!(joined.get_row(i).unwrap(), rebuilt.get_row(i).unwrap(),
            "Row {} mismatch", i);
    }
}

#[test]
fn test_full_join_sync_left_insert() {
    let left_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));

    let right_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut r = right.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("data".to_string(), ColumnValue::String("X".to_string()));
        r.append_row(row).unwrap();
    }

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    ).unwrap();

    // Initially: 1 unmatched right row
    assert_eq!(joined.len(), 1);

    // Insert left row that matches, and one that doesn't
    {
        let mut l = left.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        l.append_row(row).unwrap();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(99));
        row.insert("name".to_string(), ColumnValue::String("Zoe".to_string()));
        l.append_row(row).unwrap();
    }

    assert!(joined.sync());

    let rebuilt = JoinView::new(
        "rebuilt".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    ).unwrap();

    assert_eq!(joined.len(), rebuilt.len());
    for i in 0..joined.len() {
        assert_eq!(joined.get_row(i).unwrap(), rebuilt.get_row(i).unwrap(),
            "Row {} mismatch", i);
    }
}

#[test]
fn test_full_join_sync_right_insert() {
    let left_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut l = left.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        l.append_row(row).unwrap();
    }

    let right_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    ).unwrap();

    // Initially: 1 unmatched left row
    assert_eq!(joined.len(), 1);

    // Insert matching and non-matching right rows
    {
        let mut r = right.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("data".to_string(), ColumnValue::String("X".to_string()));
        r.append_row(row).unwrap();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(99));
        row.insert("data".to_string(), ColumnValue::String("Z".to_string()));
        r.append_row(row).unwrap();
    }

    assert!(joined.sync());

    let rebuilt = JoinView::new(
        "rebuilt".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    ).unwrap();

    assert_eq!(joined.len(), rebuilt.len());
    for i in 0..joined.len() {
        assert_eq!(joined.get_row(i).unwrap(), rebuilt.get_row(i).unwrap(),
            "Row {} mismatch", i);
    }
}

#[test]
fn test_full_join_unmatched_becomes_matched() {
    // Start with unmatched rows on both sides, then insert matching rows
    let left_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut l = left.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        l.append_row(row).unwrap();
    }

    let right_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut r = right.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(2));
        row.insert("data".to_string(), ColumnValue::String("Y".to_string()));
        r.append_row(row).unwrap();
    }

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    ).unwrap();

    // Initially: Alice unmatched left, right_id=2 unmatched right
    assert_eq!(joined.len(), 2);

    // Insert left row matching right id=2 → (None, Some(0)) becomes (Some(1), Some(0))
    {
        let mut l = left.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(2));
        row.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        l.append_row(row).unwrap();
    }

    assert!(joined.sync());

    let rebuilt = JoinView::new(
        "rebuilt".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    ).unwrap();

    assert_eq!(joined.len(), rebuilt.len());
    for i in 0..joined.len() {
        assert_eq!(joined.get_row(i).unwrap(), rebuilt.get_row(i).unwrap(),
            "Row {} mismatch", i);
    }
}

#[test]
fn test_right_join_rebuild_after_delete() {
    let left_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut l = left.borrow_mut();
        let mut r = HashMap::new();
        r.insert("id".to_string(), ColumnValue::Int32(1));
        r.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        l.append_row(r).unwrap();
        let mut r = HashMap::new();
        r.insert("id".to_string(), ColumnValue::Int32(2));
        r.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        l.append_row(r).unwrap();
    }

    let right_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut r = right.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("data".to_string(), ColumnValue::String("X".to_string()));
        r.append_row(row).unwrap();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(3));
        row.insert("data".to_string(), ColumnValue::String("Z".to_string()));
        r.append_row(row).unwrap();
    }

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Right,
    ).unwrap();

    assert_eq!(joined.len(), 2); // Alice matched, id=3 unmatched

    // Delete Alice from left
    left.borrow_mut().delete_row(0).unwrap();

    assert!(joined.sync()); // Should trigger full rebuild

    // Now both right rows should be unmatched
    assert_eq!(joined.len(), 2);
    let row0 = joined.get_row(0).unwrap();
    assert!(row0.get("name").unwrap().is_null()); // No left match
    assert_eq!(row0.get("right_data").unwrap().as_string(), Some("X"));
}

#[test]
fn test_full_join_rebuild_after_delete() {
    let left_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("name".to_string(), ColumnType::String, false),
    ]);
    let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema)));
    {
        let mut l = left.borrow_mut();
        let mut r = HashMap::new();
        r.insert("id".to_string(), ColumnValue::Int32(1));
        r.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        l.append_row(r).unwrap();
    }

    let right_schema = Schema::new(vec![
        ("id".to_string(), ColumnType::Int32, false),
        ("data".to_string(), ColumnType::String, false),
    ]);
    let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema)));
    {
        let mut r = right.borrow_mut();
        let mut row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int32(1));
        row.insert("data".to_string(), ColumnValue::String("X".to_string()));
        r.append_row(row).unwrap();
    }

    let mut joined = JoinView::new(
        "test".to_string(),
        left.clone(),
        right.clone(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Full,
    ).unwrap();

    assert_eq!(joined.len(), 1); // Alice matched

    // Delete right row
    right.borrow_mut().delete_row(0).unwrap();

    assert!(joined.sync()); // Full rebuild

    // Only Alice remains, unmatched
    assert_eq!(joined.len(), 1);
    let row0 = joined.get_row(0).unwrap();
    assert_eq!(row0.get("name").unwrap().as_string(), Some("Alice"));
    assert!(row0.get("right_data").unwrap().is_null());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib -- test_right_join_sync test_full_join_sync test_full_join_unmatched test_right_join_rebuild test_full_join_rebuild 2>&1 | tail -10`
Expected: FAIL

- [ ] **Step 3: Update sync() for RIGHT/FULL incremental handling**

In the `sync()` method, update the left-table insert handling. After the existing block that handles matching (around line 882-891), extend the no-match branch:

Change:
```rust
                    } else if self.join_type == JoinType::Left {
                        self.join_index.insert(insert_pos, (Some(*index), None));
                        modified = true;
                    }
```
To:
```rust
                    } else if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
                        self.join_index.insert(insert_pos, (Some(*index), None));
                        modified = true;
                    }
```

Add NEW code after matching right indices for LEFT/FULL inserts — handle the case where a new left row matches a previously-unmatched right row `(None, Some(right))`:

After inserting matched entries, add:
```rust
                    if let Some(matching_indices) = lookup.get(&key_str) {
                        // Check if any matched right rows were previously unmatched (RIGHT/FULL)
                        if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
                            for &right_idx in matching_indices {
                                // Find and remove (None, Some(right_idx)) from tail
                                if let Some(pos) = self.join_index.iter().position(
                                    |(l, r)| l.is_none() && *r == Some(right_idx)
                                ) {
                                    self.join_index.remove(pos);
                                }
                            }
                        }
                        let insert_pos = self.find_left_insert_position(*index);
                        for (offset, &right_idx) in matching_indices.iter().enumerate() {
                            self.join_index
                                .insert(insert_pos + offset, (Some(*index), Some(right_idx)));
                            modified = true;
                        }
```

For right-table inserts, update the existing code to handle RIGHT/FULL. Change the `if self.join_type == JoinType::Left` check to also handle RIGHT/FULL:

When a new right row matches existing left rows:
- LEFT: replace `(Some(left), None)` with `(Some(left), Some(right))` in-place (existing)
- RIGHT/FULL: same logic works (matched left rows already have Some(left))
- For the no-match case, add RIGHT/FULL handling:

After the left-row scan loop for right inserts, add:
```rust
                    // No left match found — append unmatched right row (RIGHT/FULL only)
                    let found_match = /* track whether any match was found */;
                    if !found_match && (self.join_type == JoinType::Right || self.join_type == JoinType::Full) {
                        self.join_index.push((None, Some(*right_idx)));
                        modified = true;
                    }
```

Note: The exact implementation may need to track `found_match` by checking if any left row actually matched during the inner loop. Refactor the loop to set a boolean flag.

- [ ] **Step 4: Run all sync tests**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib -- test_right_join test_full_join 2>&1 | tail -20`
Expected: ALL PASS

- [ ] **Step 5: Run ALL existing Rust tests to verify no regression**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib 2>&1 | tail -10`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/abhishekgulati/projects/livetable
git add impl/src/view.rs
git commit -m "feat: incremental sync and delete-rebuild for RIGHT/FULL joins

Extend sync() to handle right-unmatched→matched transitions.
Add RIGHT/FULL no-match handling for both left and right inserts.
Add 7 new tests covering sync and rebuild-after-delete."
```

---

## Chunk 3: Python Bindings + tick() Integration

### Task 4: Add Right/Full to PyJoinType and simplified API

**Files:**
- Modify: `impl/src/python_bindings.rs:2279-2307` (PyJoinType)
- Modify: `impl/src/python_bindings.rs:1330-1393` (join() method)

- [ ] **Step 1: Add RIGHT and FULL to PyJoinType**

In `python_bindings.rs`, after the `INNER()` classattr (line 2299), add:

```rust
    #[classattr]
    fn RIGHT() -> Self {
        PyJoinType {
            inner: RustJoinType::Right,
        }
    }

    #[classattr]
    fn FULL() -> Self {
        PyJoinType {
            inner: RustJoinType::Full,
        }
    }
```

Update `__repr__` (line 2301-2306) to handle new variants:

```rust
    fn __repr__(&self) -> String {
        match self.inner {
            RustJoinType::Left => "JoinType.LEFT".to_string(),
            RustJoinType::Inner => "JoinType.INNER".to_string(),
            RustJoinType::Right => "JoinType.RIGHT".to_string(),
            RustJoinType::Full => "JoinType.FULL".to_string(),
        }
    }
```

- [ ] **Step 2: Update join() simplified API for new types**

In `python_bindings.rs`, update the `how` parameter handling (lines 1362-1371):

```rust
        let join_type = match how.to_lowercase().as_str() {
            "left" => RustJoinType::Left,
            "inner" => RustJoinType::Inner,
            "right" => RustJoinType::Right,
            "full" | "outer" | "full_outer" => RustJoinType::Full,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unknown join type '{}'. Use 'left', 'inner', 'right', 'full', or 'outer'",
                    how
                )))
            }
        };
```

Update the docstring (line 1313) to mention new types:
```rust
    ///     how: Join type - "left", "inner", "right", "full", or "outer" (default: "left")
```

- [ ] **Step 3: Add .sync() to PyJoinView**

In `python_bindings.rs`, after the `refresh()` method (line 2436-2438), add:

```rust
    /// Incrementally sync the join view with parent table changes.
    /// Returns True if any changes were applied.
    /// For complex changes (deletes, key updates), falls back to full rebuild.
    fn sync(&mut self) -> bool {
        self.inner.borrow_mut().sync()
    }
```

- [ ] **Step 4: Verify compilation**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/abhishekgulati/projects/livetable
git add impl/src/python_bindings.rs
git commit -m "feat: expose RIGHT/FULL joins and .sync() in Python bindings

Add JoinType.RIGHT, JoinType.FULL.
Accept how='right'/'full'/'outer'/'full_outer' in .join().
Expose .sync() on PyJoinView for incremental updates."
```

---

### Task 5: Integrate JoinView with tick() auto-propagation

**Files:**
- Modify: `impl/src/python_bindings.rs:611-644` (RegisteredView, ActiveRegisteredView, impls)
- Modify: `impl/src/python_bindings.rs:1795-1842` (tick())
- Modify: `impl/src/python_bindings.rs:1380-1393` (join() — register view)

- [ ] **Step 1: Add JoinLeft/JoinRight variants to RegisteredView**

In `python_bindings.rs`, update `RegisteredView` enum (lines 612-618):

```rust
enum RegisteredView {
    Filter(Weak<RefCell<PyFilterViewInner>>),
    Sorted(Weak<RefCell<RustSortedView>>),
    Aggregate(Weak<RefCell<RustAggregateView>>),
    JoinLeft(Weak<RefCell<RustJoinView>>),
    JoinRight(Weak<RefCell<RustJoinView>>),
}
```

Update `ActiveRegisteredView` (lines 620-624):

```rust
enum ActiveRegisteredView {
    Filter(Rc<RefCell<PyFilterViewInner>>),
    Sorted(Rc<RefCell<RustSortedView>>),
    Aggregate(Rc<RefCell<RustAggregateView>>),
    JoinLeft(Rc<RefCell<RustJoinView>>),
    JoinRight(Rc<RefCell<RustJoinView>>),
}
```

Update `is_alive()` (lines 627-633):

```rust
    fn is_alive(&self) -> bool {
        match self {
            RegisteredView::Filter(inner) => inner.strong_count() > 0,
            RegisteredView::Sorted(inner) => inner.strong_count() > 0,
            RegisteredView::Aggregate(inner) => inner.strong_count() > 0,
            RegisteredView::JoinLeft(inner) => inner.strong_count() > 0,
            RegisteredView::JoinRight(inner) => inner.strong_count() > 0,
        }
    }
```

Update `upgrade()` (lines 635-643):

```rust
    fn upgrade(&self) -> Option<ActiveRegisteredView> {
        match self {
            RegisteredView::Filter(inner) => inner.upgrade().map(ActiveRegisteredView::Filter),
            RegisteredView::Sorted(inner) => inner.upgrade().map(ActiveRegisteredView::Sorted),
            RegisteredView::Aggregate(inner) => {
                inner.upgrade().map(ActiveRegisteredView::Aggregate)
            }
            RegisteredView::JoinLeft(inner) => {
                inner.upgrade().map(ActiveRegisteredView::JoinLeft)
            }
            RegisteredView::JoinRight(inner) => {
                inner.upgrade().map(ActiveRegisteredView::JoinRight)
            }
        }
    }
```

- [ ] **Step 2: Update tick() to handle JoinLeft/JoinRight**

In `tick()` (lines 1818-1835), add cases for the new variants:

```rust
                ActiveRegisteredView::JoinLeft(inner) => {
                    inner.borrow_mut().sync();
                    let (left_cursor, _) = inner.borrow().last_processed_change_count();
                    min_cursor = min_cursor.min(left_cursor);
                    synced_count += 1;
                }
                ActiveRegisteredView::JoinRight(inner) => {
                    inner.borrow_mut().sync();
                    let (_, right_cursor) = inner.borrow().last_processed_change_count();
                    min_cursor = min_cursor.min(right_cursor);
                    synced_count += 1;
                }
```

- [ ] **Step 3: Register JoinView with both parents in join()**

In the `join()` method (lines 1380-1393), change from returning the bare JoinView to registering it with both parents:

Replace:
```rust
        let join = RustJoinView::new_multi(
            name,
            self.inner.clone(),
            other.inner.clone(),
            left_keys,
            right_keys,
            join_type,
        )
        .map_err(|e| PyValueError::new_err(e))?;

        Ok(PyJoinView {
            inner: Rc::new(RefCell::new(join)),
        })
```

With:
```rust
        let join = RustJoinView::new_multi(
            name,
            self.inner.clone(),
            other.inner.clone(),
            left_keys,
            right_keys,
            join_type,
        )
        .map_err(|e| PyValueError::new_err(e))?;

        let join_rc = Rc::new(RefCell::new(join));

        // Register with both parent tables for tick() propagation
        self.registered_views.borrow_mut().push(
            RegisteredView::JoinLeft(Rc::downgrade(&join_rc))
        );
        other.registered_views.borrow_mut().push(
            RegisteredView::JoinRight(Rc::downgrade(&join_rc))
        );

        Ok(PyJoinView {
            inner: join_rc,
        })
```

- [ ] **Step 4: Verify compilation and run all Rust tests**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib 2>&1 | tail -10`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/abhishekgulati/projects/livetable
git add impl/src/python_bindings.rs
git commit -m "feat: integrate JoinView with tick() auto-propagation

Register JoinView with both parent tables using JoinLeft/JoinRight
variants. Each parent reads its own cursor for changeset compaction.
Sync is idempotent — second parent's tick() is a no-op."
```

---

## Chunk 4: Build, Python Tests, Documentation

### Task 6: Build and install Python package

**Files:**
- Run: `impl/install.sh`

- [ ] **Step 1: Build and install**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && ./install.sh`
Expected: Build succeeds, wheel installed

- [ ] **Step 2: Quick smoke test**

Run: `cd /Users/abhishekgulati/projects/livetable && python3 -c "import livetable; print(dir(livetable.JoinType))"`
Expected: Output includes `FULL`, `INNER`, `LEFT`, `RIGHT`

---

### Task 7: Write Python tests

**Files:**
- Create: `tests/python/test_right_full_joins.py`

- [ ] **Step 1: Write the test file**

Create `tests/python/test_right_full_joins.py`:

```python
"""Tests for RIGHT and FULL OUTER joins."""
import pytest
import livetable


def make_users_orders():
    """Create standard test tables.

    Users: Alice(1), Bob(2), Charlie(3)
    Orders: order 101 for Alice(1), order 102 for Dave(4)
    """
    schema_users = livetable.Schema([
        ("user_id", livetable.ColumnType.INT32, False),
        ("name", livetable.ColumnType.STRING, False),
    ])
    schema_orders = livetable.Schema([
        ("order_id", livetable.ColumnType.INT32, False),
        ("user_id", livetable.ColumnType.INT32, False),
        ("amount", livetable.ColumnType.FLOAT64, False),
    ])
    users = livetable.Table("users", schema_users)
    users.append_row({"user_id": 1, "name": "Alice"})
    users.append_row({"user_id": 2, "name": "Bob"})
    users.append_row({"user_id": 3, "name": "Charlie"})

    orders = livetable.Table("orders", schema_orders)
    orders.append_row({"order_id": 101, "user_id": 1, "amount": 99.99})
    orders.append_row({"order_id": 102, "user_id": 4, "amount": 49.99})

    return users, orders


class TestRightJoin:
    def test_right_join_basic(self):
        users, orders = make_users_orders()
        joined = users.join(orders, on="user_id", how="right")

        # RIGHT JOIN: Alice matched + Dave unmatched = 2 rows
        assert len(joined) == 2

        row0 = joined[0]
        assert row0["name"] == "Alice"
        assert row0["right_order_id"] == 101

        row1 = joined[1]
        assert row1["name"] is None  # No left match for Dave
        assert row1["right_order_id"] == 102
        assert row1["right_amount"] == 49.99

    def test_right_join_constructor(self):
        users, orders = make_users_orders()
        joined = livetable.JoinView(
            "test", users, orders, "user_id", "user_id",
            livetable.JoinType.RIGHT
        )
        assert len(joined) == 2

    def test_right_join_multi_column(self):
        schema_left = livetable.Schema([
            ("year", livetable.ColumnType.INT32, False),
            ("month", livetable.ColumnType.INT32, False),
            ("val", livetable.ColumnType.STRING, False),
        ])
        schema_right = livetable.Schema([
            ("year", livetable.ColumnType.INT32, False),
            ("month", livetable.ColumnType.INT32, False),
            ("data", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", schema_left)
        left.append_row({"year": 2024, "month": 1, "val": "A"})

        right = livetable.Table("right", schema_right)
        right.append_row({"year": 2024, "month": 1, "data": "X"})
        right.append_row({"year": 2024, "month": 2, "data": "Y"})

        joined = left.join(right, on=["year", "month"], how="right")
        assert len(joined) == 2  # One matched, one unmatched right


class TestFullJoin:
    def test_full_join_basic(self):
        users, orders = make_users_orders()
        joined = users.join(orders, on="user_id", how="full")

        # FULL JOIN: Alice matched + Bob unmatched + Charlie unmatched + Dave unmatched = 4
        assert len(joined) == 4

        # Alice matched
        assert joined[0]["name"] == "Alice"
        assert joined[0]["right_order_id"] == 101

        # Bob unmatched
        assert joined[1]["name"] == "Bob"
        assert joined[1]["right_order_id"] is None

        # Charlie unmatched
        assert joined[2]["name"] == "Charlie"
        assert joined[2]["right_order_id"] is None

        # Dave unmatched
        assert joined[3]["name"] is None
        assert joined[3]["right_order_id"] == 102

    def test_full_join_outer_alias(self):
        users, orders = make_users_orders()
        joined_full = users.join(orders, on="user_id", how="full")
        joined_outer = users.join(orders, on="user_id", how="outer")
        assert len(joined_full) == len(joined_outer)

    def test_full_join_full_outer_alias(self):
        users, orders = make_users_orders()
        joined = users.join(orders, on="user_id", how="full_outer")
        assert len(joined) == 4

    def test_full_join_constructor(self):
        users, orders = make_users_orders()
        joined = livetable.JoinView(
            "test", users, orders, "user_id", "user_id",
            livetable.JoinType.FULL
        )
        assert len(joined) == 4

    def test_full_join_null_keys(self):
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, True),
            ("val", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", schema)
        left.append_row({"id": 1, "val": "A"})
        left.append_row({"id": None, "val": "B"})

        right_schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, True),
            ("data", livetable.ColumnType.STRING, False),
        ])
        right = livetable.Table("right", right_schema)
        right.append_row({"id": 1, "data": "X"})
        right.append_row({"id": None, "data": "Y"})

        joined = left.join(right, on="id", how="full")
        # id=1 matched, left NULL unmatched, right NULL unmatched
        assert len(joined) == 3


class TestTickPropagation:
    def test_join_tick_propagation(self):
        """JoinView updates when either parent calls tick()."""
        schema_left = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        schema_right = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("data", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", schema_left)
        right = livetable.Table("right", schema_right)
        right.append_row({"id": 1, "data": "X"})

        joined = left.join(right, on="id", how="full")
        assert len(joined) == 1  # Only unmatched right row

        # Add a matching left row
        left.append_row({"id": 1, "name": "Alice"})
        left.tick()

        # JoinView should be updated
        assert len(joined) == 1  # Now matched
        assert joined[0]["name"] == "Alice"
        assert joined[0]["right_data"] == "X"

    def test_join_registered_view_count(self):
        """JoinView registers with both parent tables."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("val", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", schema)
        right = livetable.Table("right", schema)

        _ = left.join(right, on="id")

        assert left.registered_view_count() >= 1
        assert right.registered_view_count() >= 1


class TestSyncExposed:
    def test_join_sync_exposed(self):
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("val", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", schema)
        right = livetable.Table("right", schema)

        joined = left.join(right, on="id")

        # No changes — sync returns False
        assert joined.sync() == False

        left.append_row({"id": 1, "val": "A"})
        assert joined.sync() == True


class TestIterationAndSlicing:
    def test_right_full_iteration(self):
        users, orders = make_users_orders()
        joined = users.join(orders, on="user_id", how="full")

        rows = [row for row in joined]
        assert len(rows) == 4

    def test_right_full_slicing(self):
        users, orders = make_users_orders()
        joined = users.join(orders, on="user_id", how="full")

        # Positive index
        assert joined[0]["name"] == "Alice"

        # Negative index
        last = joined[-1]
        assert last["right_order_id"] == 102

        # Slice
        middle = joined[1:3]
        assert len(middle) == 2
```

- [ ] **Step 2: Run the Python tests**

Run: `cd /Users/abhishekgulati/projects/livetable && python3 -m pytest tests/python/test_right_full_joins.py -v 2>&1 | tail -30`
Expected: ALL PASS

- [ ] **Step 3: Run ALL Python tests to verify no regression**

Run: `cd /Users/abhishekgulati/projects/livetable && python3 -m pytest tests/python/ -v 2>&1 | tail -20`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
cd /Users/abhishekgulati/projects/livetable
git add tests/python/test_right_full_joins.py
git commit -m "test: add Python tests for RIGHT/FULL joins and tick() integration

11 tests covering: basic right/full joins, constructor API,
multi-column keys, NULL keys, tick propagation, sync exposure,
iteration, slicing, and outer/full_outer aliases."
```

---

### Task 8: Update documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `docs/PYTHON_BINDINGS_README.md`
- Modify: `docs/ORIGINAL_VISION.md`

- [ ] **Step 1: Update CLAUDE.md**

In `CLAUDE.md`, update the Python API Usage section. Add to the joins section (after line 182):

```python
# RIGHT JOIN - all rows from right table
joined = students.join(grades, on="id", how="right")

# FULL OUTER JOIN - all rows from both tables
joined = students.join(grades, on="id", how="full")
joined = students.join(grades, on="id", how="outer")       # alias
joined = students.join(grades, on="id", how="full_outer")   # alias

# Explicit constructors
joined = livetable.JoinView("result", t1, t2, "id", "id", livetable.JoinType.RIGHT)
joined = livetable.JoinView("result", t1, t2, "id", "id", livetable.JoinType.FULL)

# Incremental sync (also available on JoinView)
joined.sync()     # Incremental update, returns bool
joined.refresh()  # Full rebuild
```

Update Architecture section — change the note about JoinView:
- Remove or update the comment "JoinView is more complex (two parents) - not auto-registered for now"
- Note: "JoinView registers with both parents via JoinLeft/JoinRight variants"

Update the Key Patterns bullet about WebSocket protocol:
- Add: `JoinView registers with both parent tables for tick() propagation`

- [ ] **Step 2: Update docs/ORIGINAL_VISION.md**

Change line 207:
```markdown
- [ ] RIGHT and FULL OUTER joins
```
to:
```markdown
- [x] RIGHT and FULL OUTER joins
```

- [ ] **Step 3: Update README.md**

Add RIGHT/FULL joins to the features list and update the join example section.

- [ ] **Step 4: Update docs/PYTHON_BINDINGS_README.md**

Add API reference for `JoinType.RIGHT`, `JoinType.FULL`, `how="right"`, `how="full"`, `how="outer"`, `how="full_outer"`, and the `.sync()` method.

- [ ] **Step 5: Commit**

```bash
cd /Users/abhishekgulati/projects/livetable
git add CLAUDE.md README.md docs/ORIGINAL_VISION.md docs/PYTHON_BINDINGS_README.md
git commit -m "docs: document RIGHT/FULL OUTER joins and tick() integration

Update all four documentation files per the documentation checklist.
Mark RIGHT/FULL OUTER joins as complete in ORIGINAL_VISION.md."
```

---

### Task 9: Run full test suite

**Files:** None (verification only)

- [ ] **Step 1: Run all Rust tests**

Run: `cd /Users/abhishekgulati/projects/livetable/impl && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib 2>&1 | tail -10`
Expected: ALL PASS

- [ ] **Step 2: Run all Python tests**

Run: `cd /Users/abhishekgulati/projects/livetable && python3 -m pytest tests/python/ -v 2>&1 | tail -20`
Expected: ALL PASS

- [ ] **Step 3: Run integration tests**

Run: `cd /Users/abhishekgulati/projects/livetable && python3 -m pytest tests/integration/ -v 2>&1 | tail -20`
Expected: ALL PASS
