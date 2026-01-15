#!/usr/bin/env python3
"""
Scratch pad - Quick experiments with livetable
Edit this file and run it for fast iteration!
"""

import livetable

# =============================================================================
# YOUR CODE HERE - Experiment freely!
# =============================================================================

# Example: Create a simple table
schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
])

table = livetable.Table("test", schema)
table.append_row({"id": 1, "name": "Test"})

print("Table created:", table)
print("First row:", table.get_row(0))

# Add your experiments below:
# ----------------------------





# =============================================================================
# Tip: Run this with: python3 scratch.py
# =============================================================================
