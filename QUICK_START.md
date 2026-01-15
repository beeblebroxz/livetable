# LiveTable - Quick Start Guide

## What Do You Want to Do?

### I'm New - Show Me the Basics
```bash
cd examples
python3 quickstart.py
```
**5-minute interactive tutorial**

---

### I Want to Experiment
```bash
cd examples
python3 playground.py
# or
python3 scratch.py  # blank template
```
**Interactive examples and blank canvas**

---

### I Want to Build/Install
```bash
cd impl
./install.sh
```
**Builds and installs the Python package**

---

### I Want Documentation
```bash
# Read these files:
docs/PYTHON_BINDINGS_README.md  # Python API reference
docs/API_GUIDE.md               # Complete API docs
README.md                       # Project overview
```

---

### I Want to See All Features
```bash
cd examples
python3 test_python_bindings.py
```
**Comprehensive feature demonstration**

---

## Project Layout

```
livetable/
├── README.md              # Project overview
├── QUICK_START.md         # This file
├── docs/                  # All documentation
├── examples/              # Try these!
│   ├── quickstart.py      # Start here
│   ├── playground.py      # Interactive examples
│   └── scratch.py         # Your canvas
├── impl/                  # Rust implementation + Python bindings
│   ├── src/               # Rust source code
│   ├── build.sh           # Build script
│   └── install.sh         # Build + install
├── frontend/              # React frontend
└── benchmarks/            # Performance tests
```

---

## One-Liner Commands

```bash
# Build and install Rust package
cd impl && ./install.sh

# Run quick tutorial
cd examples && python3 quickstart.py

# Interactive playground
cd examples && python3 playground.py

# Start experimenting
cd examples && python3 scratch.py

# See all features
cd examples && python3 test_python_bindings.py
```

---

## Quick Code Example

```python
import livetable

# Create table
schema = livetable.Schema([
    ("id", livetable.ColumnType.INT32, False),
    ("name", livetable.ColumnType.STRING, False),
])
table = livetable.Table("users", schema)

# Add data
table.append_row({"id": 1, "name": "Alice"})

# Query
row = table.get_row(0)
print(row)  # {'id': 1, 'name': 'Alice'}

# Filter with lambda
adults = table.filter(lambda row: row.get("age", 0) >= 18)
```

---

## Next Steps

1. **First time?** Run `cd examples && python3 quickstart.py`
2. **Want to play?** Run `cd examples && python3 playground.py`
3. **Ready to code?** Edit `examples/scratch.py`

**Full docs:** [docs/PYTHON_BINDINGS_README.md](docs/PYTHON_BINDINGS_README.md)
