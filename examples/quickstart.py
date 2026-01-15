#!/usr/bin/env python3
"""
LiveTable - Quick Start Guide
The fastest way to get started with Rust-powered tables in Python!
"""

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
table.append_row({"id": 1, "name": "Alice", "age": 20, "score": 95.5})
table.append_row({"id": 2, "name": "Bob", "age": 22, "score": 87.3})
table.append_row({"id": 3, "name": "Charlie", "age": None, "score": 92.1})

print(f"✅ Added 3 rows. Table now has {len(table)} rows")

# =============================================================================
# 3. QUERY DATA
# =============================================================================
print("\n🔍 Step 3: Query data")
print("-" * 60)

# Get a full row
row = table.get_row(0)
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
    row = high_scorers.get_row(i)
    print(f"  - {row['name']}: {row['score']}")

# =============================================================================
# 6. SELECT COLUMNS (Projection)
# =============================================================================
print("\n📋 Step 6: Select specific columns")
print("-" * 60)

# Create a view with only certain columns
summary = table.select(["name", "score"])
print(f"Summary columns: {summary.column_names()}")
print(f"First row: {summary.get_row(0)}")

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
    row = with_grade.get_row(i)
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
    row = joined.get_row(i)
    course = row.get('right_course', 'No enrollment')
    print(f"  {row['name']}: {course}")

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

Next steps:
1. Try the comprehensive examples: python3 test_python_bindings.py
2. Experiment in the playground: python3 playground.py
3. Read the guide: PYTHON_BINDINGS_README.md

Happy coding with Rust-powered tables! 🚀
""")
