# Getting Started with LiveTable Rust Python Bindings

Welcome! Your Python playground is all set up and ready to go! 🎉

## 🚀 Quick Start (30 seconds)

```bash
# Option 1: Use the launcher (easiest!)
./run.sh

# Option 2: Run directly
python3 quickstart.py      # Learn basics in 5 minutes
python3 playground.py      # Interactive examples
python3 scratch.py         # Your blank canvas
```

## 📁 Files Ready for You

| File | Purpose | When to Use |
|------|---------|-------------|
| **quickstart.py** | 5-minute tutorial | First time using livetable |
| **playground.py** | Interactive examples + challenges | Learning and experimenting |
| **scratch.py** | Blank template | Quick tests and experiments |
| **test_python_bindings.py** | Full test suite | See all features in action |
| **run.sh** | Launcher menu | Easy access to everything |

## 🎯 Recommended Path

### 1. Start Here (5 minutes)
```bash
python3 quickstart.py
```
This will show you the basics:
- Creating tables
- Adding/querying data
- Filtering with lambdas
- Joining tables

### 2. Explore Examples (15 minutes)
```bash
python3 playground.py
```
This has:
- Working examples you can modify
- Challenges to try
- Performance tests
- Experiment zone

### 3. Start Building (your time!)
```bash
# Edit scratch.py with your favorite editor
code scratch.py    # VS Code
vim scratch.py     # Vim
nano scratch.py    # Nano

# Run it
python3 scratch.py
```

## 💡 Quick Reference

### Create a Table
```python
import livetable

schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
    ("age", livetable.ColumnType.INT32, True),  # nullable
])

table = livetable.Table("users", schema)
```

### Add Data
```python
table.append_row({"id": 1, "name": "Alice", "age": 30})
table.append_row({"id": 2, "name": "Bob", "age": None})  # NULL age
```

### Query Data
```python
# Get a row
row = table.get_row(0)
print(row)  # {'id': 1, 'name': 'Alice', 'age': 30}

# Get a specific value
name = table.get_value(0, "name")
print(name)  # Alice
```

### Filter (with Python lambdas!)
```python
adults = table.filter(lambda row: row.get("age") and row["age"] >= 18)
print(f"Found {len(adults)} adults")
```

### Project (select columns)
```python
summary = table.select(["name", "age"])
```

### Computed Columns
```python
with_status = table.add_computed_column(
    "status",
    lambda row: "Adult" if row.get("age") and row["age"] >= 18 else "Minor"
)
```

### Join Tables
```python
joined = livetable.JoinView(
    "user_orders",
    users,
    orders,
    "id",        # column in users
    "user_id",   # column in orders
    livetable.JoinType.LEFT
)
```

## 📚 Available Data Types

```python
livetable.ColumnType.INT32      # 32-bit integer
livetable.ColumnType.INT64      # 64-bit integer
livetable.ColumnType.FLOAT32    # 32-bit float
livetable.ColumnType.FLOAT64    # 64-bit float (Python's float)
livetable.ColumnType.STRING     # String
livetable.ColumnType.BOOL       # Boolean
```

## 🎮 Interactive Mode

Want to experiment in a REPL?

```bash
./run.sh
# Choose option 4: Python REPL

# Or directly:
python3 -i -c "import livetable"
```

Then try:
```python
>>> schema = livetable.Schema([("id", livetable.ColumnType.INT32, False)])
>>> table = livetable.Table("test", schema)
>>> table.append_row({"id": 1})
>>> table.get_row(0)
{'id': 1}
```

## 📖 Full Documentation

- [PYTHON_BINDINGS_README.md](PYTHON_BINDINGS_README.md) - Complete API reference
- [IMPLEMENTATION_COMPLETE.md](IMPLEMENTATION_COMPLETE.md) - Technical details

## 🎯 Example Project Ideas

Try building:

1. **Contact Manager**
   - Table: contacts (id, name, email, phone)
   - Filter by name
   - Search by email

2. **Task Tracker**
   - Table: tasks (id, title, priority, done)
   - Filter: incomplete tasks
   - Computed: days_until_due

3. **Product Catalog**
   - Table: products (id, name, price, stock)
   - Table: categories (id, name)
   - Join: products with categories
   - Filter: in_stock items

4. **Student Grades**
   - Table: students (id, name, class)
   - Table: grades (student_id, subject, score)
   - Join: students with grades
   - Computed: average score

5. **Blog System**
   - Table: posts (id, title, author_id, views)
   - Table: authors (id, name, email)
   - Join: posts with authors
   - Filter: popular posts (views > 1000)

## 🔥 Performance Tips

The Rust backend is FAST:
- ✅ Use filters instead of manual iteration
- ✅ Views are cheap (zero-copy)
- ✅ Joins are optimized (O(N+M))
- ✅ Computed columns calculated on-demand

Try the performance test in playground.py to see!

## 🆘 Need Help?

1. **Check the examples**: Most questions answered in `playground.py`
2. **Read the API docs**: See `PYTHON_BINDINGS_README.md`
3. **Run the tests**: `python3 test_python_bindings.py` shows all features

## 🎉 You're All Set!

Pick a file and start experimenting:

```bash
# Beginner? Start here
python3 quickstart.py

# Want to explore? Try this
python3 playground.py

# Ready to build? Use this
python3 scratch.py
```

Happy coding with Rust-powered tables! 🚀
