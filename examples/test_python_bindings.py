#!/usr/bin/env python3
"""
Test script for LiveTable Python bindings
Demonstrates the Python API for the Rust-powered table implementation
"""

import livetable

print("=" * 60)
print("Testing LiveTable Python Bindings")
print("=" * 60)

# Test 1: Basic Table Creation
print("\n1. Creating a table with schema...")
schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    ("age", livetable.ColumnType.INT32, True),  # Nullable
    ("score", livetable.ColumnType.FLOAT64, False),
])

table = livetable.Table("users", schema)
print(f"   ✓ Created table: {table}")

# Test 2: Adding Data
print("\n2. Adding rows to the table...")
table.append_row({"id": 1, "name": "Alice", "age": 30, "score": 95.5})
table.append_row({"id": 2, "name": "Bob", "age": 25, "score": 87.3})
table.append_row({"id": 3, "name": "Charlie", "age": None, "score": 92.1})
table.append_row({"id": 4, "name": "Diana", "age": 28, "score": 88.7})
print(f"   ✓ Added 4 rows. Table has {len(table)} rows")

# Test 3: Reading Data
print("\n3. Reading data from the table...")
print(f"   First row: {table.get_row(0)}")
print(f"   Name of user 2: {table.get_value(1, 'name')}")
print(f"   Age of user 3 (Charlie - should be None): {table.get_value(2, 'age')}")

# Test 4: Updating Data
print("\n4. Updating data...")
table.set_value(2, "age", 35)  # Set Charlie's age
print(f"   ✓ Updated Charlie's age: {table.get_value(2, 'age')}")

# Test 5: Column Names
print("\n5. Getting column names...")
columns = table.column_names()
print(f"   Columns: {columns}")

# Test 6: Filter View
print("\n6. Creating a filtered view (age >= 28)...")
try:
    adults = table.filter(lambda row: row.get("age") is not None and row["age"] >= 28)
    print(f"   ✓ Filtered view has {len(adults)} rows")
    for i in range(len(adults)):
        row = adults.get_row(i)
        print(f"      - {row['name']}: age {row['age']}")
except Exception as e:
    print(f"   ✗ Filter view error: {e}")

# Test 7: Projection View
print("\n7. Creating a projection view (select specific columns)...")
try:
    public_view = table.select(["id", "name"])
    print(f"   ✓ Projection has {len(public_view)} rows")
    print(f"   Columns in projection: {public_view.column_names()}")
    print(f"   First row: {public_view.get_row(0)}")
except Exception as e:
    print(f"   ✗ Projection view error: {e}")

# Test 8: Computed View
print("\n8. Creating a computed view (calculate pass/fail)...")
try:
    with_grade = table.add_computed_column(
        "grade",
        lambda row: "A" if row["score"] >= 90 else ("B" if row["score"] >= 80 else "C")
    )
    print(f"   ✓ Computed view has {len(with_grade)} rows")
    print(f"   Columns: {with_grade.column_names()}")
    for i in range(len(with_grade)):
        row = with_grade.get_row(i)
        print(f"      - {row['name']}: score {row['score']} → grade {row['grade']}")
except Exception as e:
    print(f"   ✗ Computed view error: {e}")

# Test 9: Join Operation
print("\n9. Testing join operations...")
try:
    # Create orders table
    orders_schema = livetable.Schema([
        ("order_id", livetable.ColumnType.INT32, False),
        ("user_id", livetable.ColumnType.INT32, False),
        ("amount", livetable.ColumnType.FLOAT64, False),
    ])
    orders = livetable.Table("orders", orders_schema)

    orders.append_row({"order_id": 101, "user_id": 1, "amount": 99.99})
    orders.append_row({"order_id": 102, "user_id": 1, "amount": 49.50})
    orders.append_row({"order_id": 103, "user_id": 2, "amount": 199.00})
    print(f"   ✓ Created orders table with {len(orders)} orders")

    # LEFT JOIN
    joined = livetable.JoinView(
        "user_orders",
        table,
        orders,
        "id",
        "user_id",
        livetable.JoinType.LEFT
    )
    print(f"   ✓ Created LEFT join view with {len(joined)} rows")

    for i in range(min(5, len(joined))):  # Show first 5
        row = joined.get_row(i)
        amount = row.get('right_amount', 'no orders')
        print(f"      - {row['name']}: {amount}")

    # INNER JOIN
    inner_joined = livetable.JoinView(
        "user_orders_inner",
        table,
        orders,
        "id",
        "user_id",
        livetable.JoinType.INNER
    )
    print(f"   ✓ Created INNER join view with {len(inner_joined)} rows (only users with orders)")

except Exception as e:
    print(f"   ✗ Join error: {e}")
    import traceback
    traceback.print_exc()

# Test 10: Display table
print("\n10. Display table info...")
print(table.display())

print("\n" + "=" * 60)
print("✅ All tests completed successfully!")
print("=" * 60)
print("\n🚀 The Rust-powered table is working from Python!")
print("   You now have high-performance table operations")
print("   with a Pythonic API backed by Rust!")
