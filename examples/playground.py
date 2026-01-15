#!/usr/bin/env python3
"""
LiveTable Playground - Interactive exploration of Rust-powered tables
Play with high-performance table operations from Python!
"""

import livetable

# =============================================================================
# STARTER EXAMPLE - A simple table to get you started
# =============================================================================

print("🎮 LiveTable Playground - Let's build some tables!")
print("=" * 70)

# Create a users table
print("\n📊 Creating a users table...")
users_schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    ("age", livetable.ColumnType.INT32, True),       # Nullable
    ("email", livetable.ColumnType.STRING, False),
    ("score", livetable.ColumnType.FLOAT64, False),
    ("active", livetable.ColumnType.BOOL, False),
])

users = livetable.Table("users", users_schema)

# Add some sample data
print("➕ Adding sample users...")
users.append_row({
    "id": 1,
    "name": "Alice",
    "age": 30,
    "email": "alice@example.com",
    "score": 95.5,
    "active": True
})

users.append_row({
    "id": 2,
    "name": "Bob",
    "age": 25,
    "email": "bob@example.com",
    "score": 87.3,
    "active": True
})

users.append_row({
    "id": 3,
    "name": "Charlie",
    "age": None,  # NULL age
    "email": "charlie@example.com",
    "score": 92.1,
    "active": False
})

users.append_row({
    "id": 4,
    "name": "Diana",
    "age": 28,
    "email": "diana@example.com",
    "score": 88.7,
    "active": True
})

users.append_row({
    "id": 5,
    "name": "Eve",
    "age": 35,
    "email": "eve@example.com",
    "score": 97.2,
    "active": True
})

print(f"✅ Created table with {len(users)} users")
print(users.display())

# =============================================================================
# YOUR PLAYGROUND - Experiment below!
# =============================================================================

print("\n" + "=" * 70)
print("💡 YOUR PLAYGROUND - Try these examples or create your own!")
print("=" * 70)

# Example 1: Basic querying
print("\n--- Example 1: Basic Querying ---")
print("First user:", users[0])
print("Bob's email:", users.get_value(1, "email"))
print("Charlie's age (should be None):", users.get_value(2, "age"))

# Example 2: Update data
print("\n--- Example 2: Update Data ---")
print("Setting Charlie's age to 32...")
users.set_value(2, "age", 32)
print("Charlie's new age:", users.get_value(2, "age"))

# Example 3: Filter views
print("\n--- Example 3: Filter Views ---")

# Filter: Active users only
active_users = users.filter(lambda row: row["active"])
print(f"Active users: {len(active_users)}")
for i in range(len(active_users)):
    row = active_users[i]
    print(f"  - {row['name']} (score: {row['score']})")

# Filter: High scorers (score >= 90)
high_scorers = users.filter(lambda row: row["score"] >= 90)
print(f"\nHigh scorers (>= 90): {len(high_scorers)}")
for i in range(len(high_scorers)):
    row = high_scorers[i]
    print(f"  - {row['name']}: {row['score']}")

# Filter: Users with known age >= 30
mature_users = users.filter(lambda row: row.get("age") is not None and row["age"] >= 30)
print(f"\nUsers aged 30+: {len(mature_users)}")
for i in range(len(mature_users)):
    row = mature_users[i]
    print(f"  - {row['name']}: {row['age']}")

# Example 4: Projection views
print("\n--- Example 4: Projection Views ---")

# Select only public columns
public_view = users.select(["id", "name", "score"])
print(f"Public view columns: {public_view.column_names()}")
print("First row:", public_view[0])

# Example 5: Computed columns
print("\n--- Example 5: Computed Columns ---")

# Add grade column
with_grade = users.add_computed_column(
    "grade",
    lambda row: "A+" if row["score"] >= 95 else (
                "A" if row["score"] >= 90 else (
                "B" if row["score"] >= 80 else "C"))
)

print("Users with grades:")
for i in range(len(with_grade)):
    row = with_grade[i]
    print(f"  {row['name']}: {row['score']} → {row['grade']}")

# Add age group column
with_age_group = users.add_computed_column(
    "age_group",
    lambda row: "Unknown" if row.get("age") is None else (
                "Young" if row["age"] < 30 else (
                "Middle" if row["age"] < 40 else "Senior"))
)

print("\nUsers with age groups:")
for i in range(len(with_age_group)):
    row = with_age_group[i]
    age_str = str(row['age']) if row['age'] is not None else "Unknown"
    print(f"  {row['name']}: age {age_str} → {row['age_group']}")

# Example 6: Joins
print("\n--- Example 6: Join Operations ---")

# Create an orders table
orders_schema = livetable.Schema([
    ("order_id", livetable.ColumnType.INT32, False),
    ("user_id", livetable.ColumnType.INT32, False),
    ("product", livetable.ColumnType.STRING, False),
    ("amount", livetable.ColumnType.FLOAT64, False),
])

orders = livetable.Table("orders", orders_schema)

# Add some orders
orders.append_row({"order_id": 101, "user_id": 1, "product": "Laptop", "amount": 999.99})
orders.append_row({"order_id": 102, "user_id": 1, "product": "Mouse", "amount": 29.99})
orders.append_row({"order_id": 103, "user_id": 2, "product": "Keyboard", "amount": 79.99})
orders.append_row({"order_id": 104, "user_id": 4, "product": "Monitor", "amount": 299.99})
orders.append_row({"order_id": 105, "user_id": 5, "product": "Headphones", "amount": 149.99})

print(f"Created {len(orders)} orders")

# LEFT JOIN - all users with their orders (if any)
user_orders_left = livetable.JoinView(
    "user_orders_left",
    users,
    orders,
    "id",
    "user_id",
    livetable.JoinType.LEFT
)

print(f"\nLEFT JOIN (all users): {len(user_orders_left)} rows")
for i in range(len(user_orders_left)):
    row = user_orders_left[i]
    product = row.get('right_product')
    amount = row.get('right_amount')
    if product:
        print(f"  {row['name']}: {product} (${amount:.2f})")
    else:
        print(f"  {row['name']}: (no orders)")

# INNER JOIN - only users who have placed orders
user_orders_inner = livetable.JoinView(
    "user_orders_inner",
    users,
    orders,
    "id",
    "user_id",
    livetable.JoinType.INNER
)

print(f"\nINNER JOIN (users with orders only): {len(user_orders_inner)} rows")
for i in range(len(user_orders_inner)):
    row = user_orders_inner[i]
    print(f"  {row['name']}: {row['right_product']} (${row['right_amount']:.2f})")

# =============================================================================
# 🎯 YOUR TURN - Try these challenges!
# =============================================================================

print("\n" + "=" * 70)
print("🎯 CHALLENGES - Try these!")
print("=" * 70)

print("""
1. Add 3 more users with different data
2. Create a filter for inactive users only
3. Find the average score of active users (manual calculation)
4. Create a projection with only name and email
5. Add a computed column that shows if score is above average
6. Create more orders and join to see purchase history
7. Filter the joined view to show only orders over $100
8. Update a user's status and see the filter view update automatically

Uncomment and write your code below!
""")

# Challenge 1: Add more users
# users.append_row({...})

# Challenge 2: Filter inactive users
# inactive = users.filter(lambda row: ...)
# print(f"Inactive users: {len(inactive)}")

# Challenge 3: Calculate average score
# total_score = 0
# count = 0
# for i in range(len(users)):
#     ...
# avg_score = total_score / count if count > 0 else 0
# print(f"Average score: {avg_score:.2f}")

# Challenge 4: Projection
# contact_info = users.select([...])

# Challenge 5: Above average computed column
# above_avg = users.add_computed_column(
#     "above_average",
#     lambda row: ...
# )

# Challenge 6: More orders
# orders.append_row({...})

# Challenge 7: Filter joined view for expensive orders
# expensive_orders = user_orders_left.filter(
#     lambda row: row.get('right_amount') and row['right_amount'] > 100
# )

# Challenge 8: Update and observe
# users.set_value(0, "active", False)
# active_users.refresh()  # Refresh the filter view
# print(f"Active users after update: {len(active_users)}")

# =============================================================================
# 🔬 EXPERIMENT ZONE - Create your own tables and queries!
# =============================================================================

print("\n" + "=" * 70)
print("🔬 EXPERIMENT ZONE - Build your own!")
print("=" * 70)

print("""
Create your own tables! Ideas:
- Products catalog with prices and inventory
- Blog posts with authors and tags
- Tasks/TODOs with priorities and due dates
- Music library with songs, artists, albums
- Student grades with subjects and scores

Example template:

my_schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    # Add more columns...
])

my_table = livetable.Table("my_table", my_schema)
my_table.append_row({...})

# Then query, filter, join!
""")

# Your experiments here!
# ----------------------

# Example: Products table
# products_schema = livetable.Schema([
#     ("id", livetable.ColumnType.INT32, False),
#     ("name", livetable.ColumnType.STRING, False),
#     ("price", livetable.ColumnType.FLOAT64, False),
#     ("in_stock", livetable.ColumnType.BOOL, False),
# ])
# products = livetable.Table("products", products_schema)


# =============================================================================
# 🚀 PERFORMANCE TEST - See how fast it is!
# =============================================================================

print("\n" + "=" * 70)
print("🚀 PERFORMANCE TEST")
print("=" * 70)

import time

# Create a larger table
print("Creating a table with 10,000 rows...")
perf_schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("value", livetable.ColumnType.FLOAT64, False),
])
perf_table = livetable.Table("performance_test", perf_schema)

start = time.time()
for i in range(10000):
    perf_table.append_row({"id": i, "value": float(i * 1.5)})
end = time.time()

print(f"✅ Inserted 10,000 rows in {(end-start)*1000:.2f} ms")
print(f"   ({10000/(end-start):.0f} rows/second)")

# Query performance
start = time.time()
for i in range(1000):
    _ = perf_table.get_value(i, "value")
end = time.time()

print(f"✅ Retrieved 1,000 values in {(end-start)*1000:.2f} ms")
print(f"   ({1000/(end-start):.0f} queries/second)")

# Filter performance
start = time.time()
filtered = perf_table.filter(lambda row: row["value"] > 5000)
filtered_count = len(filtered)
end = time.time()

print(f"✅ Filtered 10,000 rows in {(end-start)*1000:.2f} ms")
print(f"   Found {filtered_count} matching rows")

print("\n🎉 Playground ready! Start experimenting above! 🎉")
print("=" * 70)
