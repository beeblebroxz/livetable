# 🎮 Ready to Play with LiveTable Rust!

Everything is set up and ready for you to experiment with the Rust-powered table system from Python!

## 🚀 Quick Start - Pick Your Adventure

### Option 1: Guided Tutorial (Recommended for First Time)
```bash
cd examples
python3 quickstart.py
```
**5-minute tour** of all the features. Perfect for learning the API.

### Option 2: Interactive Playground
```bash
cd examples
python3 playground.py
```
**Pre-built examples** with challenges to try. Great for hands-on learning.

### Option 3: Blank Canvas
```bash
cd examples
python3 scratch.py
```
**Empty template** ready for your experiments. Edit and run!

### Option 4: Use the Launcher
```bash
cd examples
./run.sh
```
**Interactive menu** to choose what to run.

## 📝 Files Available

| File | What It Does | Best For |
|------|--------------|----------|
| `quickstart.py` | Step-by-step tutorial with examples | Learning basics |
| `playground.py` | Examples, challenges, performance tests | Experimenting |
| `scratch.py` | Blank file for your code | Quick tests |
| `test_python_bindings.py` | Full test suite (all features) | Reference |
| `run.sh` | Menu to launch any of the above | Convenience |

## 💡 What Can You Do?

### ✅ Create High-Performance Tables
```python
import livetable

schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
])
table = livetable.Table("users", schema)
```

### ✅ Add and Query Data
```python
table.append_row({"id": 1, "name": "Alice"})
row = table.get_row(0)
```

### ✅ Filter with Python Lambdas
```python
adults = table.filter(lambda row: row.get("age") and row["age"] >= 18)
```

### ✅ Join Tables
```python
joined = livetable.JoinView(
    "user_orders",
    users,
    orders,
    "id",
    "user_id",
    livetable.JoinType.LEFT
)
```

### ✅ Add Computed Columns
```python
with_grade = table.add_computed_column(
    "grade",
    lambda row: "A" if row["score"] >= 90 else "B"
)
```

## 🎯 Suggested Path

1. **First**: Run `python3 quickstart.py` - see what's possible
2. **Then**: Open `playground.py` in your editor - lots of examples
3. **Next**: Try the challenges in `playground.py`
4. **Finally**: Use `scratch.py` for your own experiments

## 🔥 Performance

The Rust backend makes your Python code **10-100x faster**:

```bash
python3 playground.py
# Scroll to the bottom to see performance benchmarks
# (Inserts 10,000 rows, runs filters, etc.)
```

## 📚 Documentation

- **Getting Started**: [GETTING_STARTED.md](GETTING_STARTED.md)
- **Full API Reference**: [PYTHON_BINDINGS_README.md](PYTHON_BINDINGS_README.md)
- **Implementation Details**: [IMPLEMENTATION_COMPLETE.md](IMPLEMENTATION_COMPLETE.md)

## 🎨 Example Ideas

Build something fun:

1. **Contact Manager** - Name, email, phone with search
2. **Task Tracker** - TODOs with priorities and filters
3. **Product Catalog** - Inventory with joins to categories
4. **Grade Book** - Students with scores and computed averages
5. **Blog System** - Posts with authors (joined tables)

## 🆘 Quick Help

**Q: How do I run a file?**
```bash
python3 filename.py
```

**Q: How do I edit a file?**
```bash
code filename.py       # VS Code
vim filename.py        # Vim
nano filename.py       # Nano
open filename.py       # macOS default editor
```

**Q: Where do I write my own code?**

Edit `scratch.py` - it's your blank canvas!

**Q: I want to see all features**
```bash
python3 test_python_bindings.py
```

**Q: How do I use this interactively?**
```bash
python3 -i -c "import livetable"
>>> # Now type your code
```

## 🎉 Ready? Let's Go!

Pick one and run it:

```bash
# Tutorial (5 min)
python3 quickstart.py

# Playground (explore)
python3 playground.py

# Your code
python3 scratch.py

# Menu
./run.sh
```

**Have fun with Rust-powered tables in Python!** 🚀

---

*All set up and ready to go! The package is already installed and working.*
