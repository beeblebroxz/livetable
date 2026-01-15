#!/usr/bin/env python3
"""
Live Table Demo - Continuously updates the LiveTable to demonstrate real-time UI updates.

This script performs random operations on the table:
- Inserts new rows with random data
- Updates existing cells with new values
- Deletes random rows
- All changes are broadcast to connected WebSocket clients in real-time!

Usage:
    python live_demo.py

Then open http://localhost:5173 in multiple browser tabs to see synchronized updates!
"""

import livetable
import time
import random
from datetime import datetime

# Sample data for generating random entries
NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
DEPARTMENTS = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Support"]
CITIES = ["New York", "San Francisco", "London", "Tokyo", "Berlin", "Sydney", "Toronto"]

def create_random_row():
    """Generate a random person record."""
    return {
        "name": random.choice(NAMES),
        "age": random.randint(22, 65),
        "department": random.choice(DEPARTMENTS),
        "salary": random.randint(50000, 150000),
        "city": random.choice(CITIES),
    }

def main():
    print("🚀 Starting Live Table Demo")
    print("=" * 60)
    print("Open http://localhost:5173 in your browser to watch updates!")
    print("Open multiple tabs to see real-time synchronization")
    print("=" * 60)
    print()

    # Create a new table
    print("📋 Creating table with initial data...")
    table = livetable.Table()

    # Add columns
    table.add_column("name", "String")
    table.add_column("age", "Integer")
    table.add_column("department", "String")
    table.add_column("salary", "Integer")
    table.add_column("city", "String")

    # Insert a few initial rows
    initial_rows = [
        {"name": "Alice", "age": 30, "department": "Engineering", "salary": 120000, "city": "San Francisco"},
        {"name": "Bob", "age": 25, "department": "Sales", "salary": 85000, "city": "New York"},
        {"name": "Charlie", "age": 35, "department": "Marketing", "salary": 95000, "city": "London"},
    ]

    for row in initial_rows:
        table.insert(row)

    print(f"✅ Initial table created with {table.row_count()} rows")
    print()

    iteration = 0

    try:
        while True:
            iteration += 1
            timestamp = datetime.now().strftime("%H:%M:%S")

            # Decide what operation to perform (weighted probabilities)
            operation = random.choices(
                ["insert", "update", "delete", "insert"],  # Favor inserts
                weights=[40, 30, 20, 10],
                k=1
            )[0]

            current_rows = table.row_count()

            if operation == "insert":
                new_row = create_random_row()
                table.insert(new_row)
                print(f"[{timestamp}] #{iteration:3d} ➕ INSERT: {new_row['name']}, {new_row['age']}, {new_row['department']} (Total rows: {current_rows + 1})")

            elif operation == "update" and current_rows > 0:
                row_idx = random.randint(0, current_rows - 1)
                column = random.choice(["age", "salary", "department", "city"])

                if column == "age":
                    new_value = random.randint(22, 65)
                elif column == "salary":
                    new_value = random.randint(50000, 150000)
                elif column == "department":
                    new_value = random.choice(DEPARTMENTS)
                else:  # city
                    new_value = random.choice(CITIES)

                table.update(row_idx, column, new_value)
                print(f"[{timestamp}] #{iteration:3d} ✏️  UPDATE: Row {row_idx}, {column} = {new_value}")

            elif operation == "delete" and current_rows > 1:  # Keep at least 1 row
                row_idx = random.randint(0, current_rows - 1)
                table.delete(row_idx)
                print(f"[{timestamp}] #{iteration:3d} ❌ DELETE: Row {row_idx} (Total rows: {current_rows - 1})")

            else:
                # Fallback to insert if we can't delete/update
                new_row = create_random_row()
                table.insert(new_row)
                print(f"[{timestamp}] #{iteration:3d} ➕ INSERT: {new_row['name']}, {new_row['age']}, {new_row['department']} (Total rows: {current_rows + 1})")

            # Sleep for a short time to make updates visible
            time.sleep(0.5)  # 2 operations per second

    except KeyboardInterrupt:
        print()
        print("=" * 60)
        print("👋 Demo stopped by user")
        print(f"Final row count: {table.row_count()}")
        print("=" * 60)

if __name__ == "__main__":
    main()
