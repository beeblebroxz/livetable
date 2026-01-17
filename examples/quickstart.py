#!/usr/bin/env python3
"""
LiveTable - Quick Start Guide
The fastest way to get started with Rust-powered tables in Python!
"""

from datetime import date
import livetable

print("🚀 LiveTable Quick Start")
print("=" * 60)

# =============================================================================
# 1. CREATE A TABLE
# =============================================================================
print("\n📊 Step 1: Create a table")
print("-" * 60)

# Define schema: (column_name, type, nullable)
schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    ("age", livetable.ColumnType.INT32, True),  # Can be NULL
    ("score", livetable.ColumnType.FLOAT64, False),
    ("test_date", livetable.ColumnType.DATE, True),
])

# Create table
table = livetable.Table("students", schema)
print("✅ Created table:", table)

# =============================================================================
# 2. ADD DATA
# =============================================================================
print("\n➕ Step 2: Add data")
print("-" * 60)

# Add rows as dictionaries
table.append_row({"id": 1, "name": "Alice", "age": 20, "score": 95.5, "test_date": date.today()})
table.append_row({"id": 2, "name": "Bob", "age": 22, "score": 87.3, "test_date": date.today()})
table.append_row({"id": 3, "name": "Charlie", "age": None, "score": 92.1, "test_date": date.today()})

print(f"✅ Added 3 rows. Table now has {len(table)} rows")

# =============================================================================
# 3. QUERY DATA
# =============================================================================
print("\n🔍 Step 3: Query data")
print("-" * 60)

# Get a full row
row = table[0]
print(f"First row: {row}")

# Get a specific value
name = table.get_value(1, "name")
print(f"Second student's name: {name}")

# Check NULL values
age = table.get_value(2, "age")
print(f"Charlie's age: {age}")  # Will be None

# =============================================================================
# 4. UPDATE DATA
# =============================================================================
print("\n✏️  Step 4: Update data")
print("-" * 60)

# Update a value
table.set_value(2, "age", 21)
new_age = table.get_value(2, "age")
print(f"✅ Updated Charlie's age to: {new_age}")

# =============================================================================
# 5. FILTER DATA (with Python lambda!)
# =============================================================================
print("\n🔎 Step 5: Filter data")
print("-" * 60)

# Filter with a lambda function
high_scorers = table.filter(lambda row: row["score"] >= 90)
print(f"Students with score >= 90: {len(high_scorers)}")

for i in range(len(high_scorers)):
    row = high_scorers[i]
    print(f"  - {row['name']}: {row['score']}")

# =============================================================================
# 6. SELECT COLUMNS (Projection)
# =============================================================================
print("\n📋 Step 6: Select specific columns")
print("-" * 60)

# Create a view with only certain columns
summary = table.select(["name", "score"])
print(f"Summary columns: {summary.column_names()}")
print(f"First row: {summary[0]}")

# =============================================================================
# 7. COMPUTED COLUMNS
# =============================================================================
print("\n🧮 Step 7: Add computed columns")
print("-" * 60)

# Add a grade column based on score
with_grade = table.add_computed_column(
    "grade",
    lambda row: "A" if row["score"] >= 90 else "B" if row["score"] >= 80 else "C"
)

print("Students with grades:")
for i in range(len(with_grade)):
    row = with_grade[i]
    print(f"  {row['name']}: {row['score']} → {row['grade']}")

# =============================================================================
# 8. JOIN TABLES
# =============================================================================
print("\n🔗 Step 8: Join tables")
print("-" * 60)

# Create a second table
enrollments_schema = livetable.Schema([
    ("student_id", livetable.ColumnType.INT32, False),
    ("course", livetable.ColumnType.STRING, False),
])
enrollments = livetable.Table("enrollments", enrollments_schema)

enrollments.append_row({"student_id": 1, "course": "Math"})
enrollments.append_row({"student_id": 1, "course": "Physics"})
enrollments.append_row({"student_id": 2, "course": "Chemistry"})

# LEFT JOIN - all students with their courses
joined = livetable.JoinView(
    "student_courses",
    table,
    enrollments,
    "id",
    "student_id",
    livetable.JoinType.LEFT
)

print(f"Joined view has {len(joined)} rows:")
for i in range(len(joined)):
    row = joined[i]
    course = row.get('right_course', 'No enrollment')
    print(f"  {row['name']}: {course}")

# =============================================================================
# 9. SIMPLIFIED API (bonus!)
# =============================================================================
print("\n⚡ Step 9: Simplified API")
print("-" * 60)

# Simplified sort (instead of SortedView constructor)
sorted_table = table.sort("score", descending=True)
print(f"Top scorer: {sorted_table[0]['name']} ({sorted_table[0]['score']})")

# Simplified join (instead of JoinView constructor)
joined_simple = table.join(enrollments, left_on="id", right_on="student_id")
print(f"Simple join: {len(joined_simple)} rows")

# GROUP BY with aggregations
by_course = enrollments.group_by("course", agg=[
    ("count", "student_id", "count"),
])
print("Enrollments by course:")
for i in range(len(by_course)):
    row = by_course[i]
    print(f"  {row['course']}: {int(row['count'])} students")

# =============================================================================
# 10. REACTIVE UPDATES with tick()
# =============================================================================
print("\n🔄 Step 10: Reactive updates with tick()")
print("-" * 60)

# Views created with simplified API are auto-registered for tick()
filtered = table.filter(lambda r: r["score"] >= 90)
sorted_view = table.sort("score", descending=True)
print(f"Registered views: {table.registered_view_count()}")

# Add new data
table.append_row({"id": 4, "name": "Diana", "age": 19, "score": 98.0, "test_date": date.today()})
print(f"Added Diana (score: 98)")

# Before tick, views haven't updated yet
print(f"Filtered count (stale): {len(filtered)}")

# tick() propagates changes to all registered views at once
synced = table.tick()
print(f"After tick(): synced {synced} views")
print(f"Filtered count (fresh): {len(filtered)}")
print(f"New top scorer: {sorted_view[0]['name']}")

# =============================================================================
# 🎓 YOU'RE READY!
# =============================================================================
print("\n" + "=" * 60)
print("🎓 Quick Start Complete!")
print("=" * 60)
print("""
You now know how to:
✅ Create tables with schemas
✅ Add and query data
✅ Update values
✅ Filter with lambda functions
✅ Select columns (projection)
✅ Add computed columns
✅ Join tables
✅ Use simplified API (sort, join, group_by)
✅ Propagate changes with tick()

Next steps:
1. Watch reactive propagation: python3 demo_reactive_propagation.py
2. Experiment in the playground: python3 playground.py
3. Try real-time streaming: python3 streaming_publisher.py (with WebSocket server)
4. Read the full guide: docs/PYTHON_BINDINGS_README.md

Happy coding with Rust-powered tables! 🚀
""")
