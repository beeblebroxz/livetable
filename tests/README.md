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
1. Rust `clippy` for the core library, `server`, and `python` features
2. Rust library tests with the `server` feature enabled
3. Python unit tests
4. Integration tests
5. Frontend lint, Vitest, and production build

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
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib --features server
```

### Rust Lint Only

```bash
cd ../impl
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features server -- -D warnings
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo clippy --all-targets --features python -- -D warnings
```

### Frontend Checks Only

```bash
cd ../frontend
npm run lint
npm run test
npm run build
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
- **sequence.rs** - Storage backends
- **column.rs** - Column operations
- **table.rs** - Table operations
- **view.rs** - Views and incremental propagation
- **websocket.rs** - WebSocket protocol and JSON conversion
- **lib.rs** - Integration workflow

## Test Coverage

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
✅ WebSocket row mutation semantics
✅ Real-world workflows
✅ Performance with 1000+ rows

### What's NOT Tested (Yet)

⚠️ Full browser E2E coverage across real sockets and tabs
⚠️ Multi-client race conditions at browser level
⚠️ Memory stress tests
⚠️ Performance regression thresholds in CI

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

GitHub Actions now lives in [.github/workflows/ci.yml](../.github/workflows/ci.yml).
It runs:

1. Rust `clippy` with `-D warnings` for the core library
2. Rust `clippy` with `-D warnings` for the `server` feature
3. Rust `clippy` with `-D warnings` for the `python` feature
4. Rust tests with the `server` feature enabled
   The workflow exercises the core library plus server/WebSocket paths.
5. Python package build plus pytest suite on Python 3.12
6. Frontend lint, Vitest, and production build

### Toolchain Split

The project intentionally validates different surfaces with different tools:

1. Core, server, and python Rust feature sets are linted directly with Cargo.
2. Python bindings are built as a wheel with `maturin` and validated with `pytest`.
3. Frontend behavior is validated with ESLint, Vitest, and `vite build`.

This avoids relying on `cargo test --all-features` for the PyO3 extension target, which is less stable across local Python installations. When running Cargo commands locally against newer Python runtimes, set `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`.

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
