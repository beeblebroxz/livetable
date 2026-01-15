# LiveTable Test Suite

Comprehensive test suite for the livetable Rust-powered table system.

## Test Structure

```
tests/
├── pytest.ini                  # Pytest configuration
├── README.md                   # This file
├── run_all.sh                  # Run all tests (Rust + Python)
├── python/                     # Python unit tests
│   ├── test_table_operations.py    # CRUD operations
│   ├── test_views.py              # Views and filtering
│   └── test_bindings.py           # Legacy comprehensive test
└── integration/                # Integration tests
    └── test_end_to_end.py         # Real-world workflows
```

## Running Tests

### All Tests (Recommended)

```bash
cd tests
./run_all.sh
```

This runs:
1. Rust unit tests (23 tests in impl/src/)
2. Python unit tests (pytest)
3. Integration tests

### Python Tests Only

```bash
# From tests/ directory
pytest

# Specific test file
pytest python/test_table_operations.py

# Specific test class
pytest python/test_views.py::TestFilterView

# Specific test
pytest python/test_views.py::TestFilterView::test_filter_basic

# Verbose output
pytest -v

# Show print statements
pytest -s
```

### Rust Tests Only

```bash
cd ../impl
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib
```

## Test Categories

### Python Unit Tests (`python/`)

**test_table_operations.py** - Core table functionality
- Table creation with schemas
- Inserting rows (single, multiple, with nulls)
- Reading data (get_row, get_value, column_names)
- Updating values (set_value, update to null)
- Deleting rows

**test_views.py** - View operations
- FilterView (basic filters, null handling)
- ProjectionView (column selection)
- ComputedView (dynamic columns)
- JoinView (LEFT and INNER joins)
- View chaining (filter + project, filter + compute)

**test_bindings.py** - Legacy comprehensive test
- All features in one file (142 lines)
- Useful for quick smoke testing

### Integration Tests (`integration/`)

**test_end_to_end.py** - Real-world scenarios
- Contact Manager workflow
- E-commerce analytics with joins
- Student gradebook with computed grades
- Blog system with multiple tables
- Performance scenario with 1000+ rows

### Rust Tests (`../impl/src/`)

Located in Rust source files with `#[cfg(test)]` modules:
- **sequence.rs** - Storage backends (6 tests)
- **column.rs** - Column operations (3 tests)
- **table.rs** - Table operations (5 tests)
- **view.rs** - Views and joins (8 tests)
- **lib.rs** - Integration workflow (1 test)

## Test Coverage

### Current Coverage

| Component | Tests | Status |
|-----------|-------|--------|
| Rust Core | 23 | ✅ Passing |
| Python Bindings | 40+ | ✅ Passing |
| Integration | 6 workflows | ✅ Passing |

### What's Tested

✅ Table creation and schemas
✅ CRUD operations (Create, Read, Update, Delete)
✅ All column types (INT32, INT64, FLOAT32, FLOAT64, STRING, BOOL)
✅ Nullable columns
✅ FilterView with Python lambdas
✅ ProjectionView (column selection)
✅ ComputedView (dynamic columns)
✅ JoinView (LEFT and INNER joins)
✅ View chaining
✅ Real-world workflows
✅ Performance with 1000+ rows

### What's NOT Tested (Yet)

⚠️ Type conversion edge cases (bool vs int, f64 vs f32)
⚠️ Error handling (invalid schemas, type mismatches)
⚠️ Schema validation edge cases
⚠️ Concurrent access patterns
⚠️ Memory stress tests
⚠️ Python bindings code (`python_bindings.rs` has 0 unit tests)

## Writing New Tests

### Python Unit Test Template

```python
import pytest
import livetable

class TestYourFeature:
    """Test description"""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema)
        return table

    def test_something(self, sample_table):
        """Test a specific behavior"""
        sample_table.append_row({"id": 1, "name": "Alice"})
        assert len(sample_table) == 1
```

### Rust Test Template

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        // Arrange
        let schema = Schema::new(vec![...]);
        let mut table = Table::new("test".to_string(), schema);

        // Act
        table.append_row(...);

        // Assert
        assert_eq!(table.len(), 1);
    }
}
```

## Continuous Integration

To add CI (GitHub Actions example):

```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Install Rust
        uses: actions-rs/toolchain@v1
      - name: Install Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install pytest maturin
          cd impl && maturin develop
      - name: Run tests
        run: cd tests && ./run_all.sh
```

## Performance Testing

For performance benchmarks (separate from unit tests):

```bash
cd ../benchmarks
python3 benchmark.py
```

Or Rust benchmarks:

```bash
cd ../impl
cargo bench
```

## Troubleshooting

**"Module livetable not found"**
```bash
cd ../impl
./install.sh
```

**"PyO3 version error"**
```bash
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test
```

**Tests fail after code changes**
```bash
# Rebuild and reinstall
cd ../impl
./install.sh
cd ../tests
pytest
```

## Contributing Tests

When adding new features:

1. Write Rust unit tests in the source file
2. Write Python unit tests in `python/test_*.py`
3. Add integration test if it's a complex feature
4. Run all tests before submitting PR
5. Aim for >80% code coverage

## Questions?

- See [../docs/PYTHON_BINDINGS_README.md](../docs/PYTHON_BINDINGS_README.md) for API reference
- See [../examples/](../examples/) for usage examples
- Run `pytest --help` for pytest options
