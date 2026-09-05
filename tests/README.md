# LiveTable Test Suite

Comprehensive test suite for the livetable Rust-powered table system.

## Test Structure (selected files)

```
tests/
├── pytest.ini                  # Pytest configuration
├── README.md                   # This file
├── run_all.sh                  # Standard Rust/Python/frontend checks
├── python/                     # Python unit tests
│   ├── test_table_operations.py    # CRUD operations
│   ├── test_typed_storage.py       # Native buffers, NULL masks, and pipeline compatibility
│   ├── test_views.py              # Views and filtering
│   ├── test_filter_batches.py     # Mixed filter batches and callback failures
│   ├── test_sorted_batches.py     # Sorted batches and downstream grouping
│   ├── test_view_composition.py   # Supported Python chains
│   └── test_bindings.py           # Legacy comprehensive test
└── integration/                # Integration tests
    └── test_end_to_end.py         # Real-world workflows
```

## Running Tests

### Standard Local Checks

```bash
cd tests
./run_all.sh
```

Rebuild/install the Python extension after Rust changes (see the
[build instructions](../docs/PYTHON_BINDINGS_README.md#building-from-source)).
The runner only installs it if importing `livetable` fails; an existing import
is not proof that it contains the latest source. Use an isolated virtualenv.

This runs:

1. Rust `clippy` for the core library, `server`, and `python` features
2. Rust library tests with the `server` feature enabled
3. Rust filter and sorted pipeline contract tests
4. Python unit and integration tests
5. Frontend lint, Vitest, and production build

The script intentionally keeps the long randomized fuzz suite and the
real-server WebSocket test separate. Run those with the commands below before a
protocol or propagation release.

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
# From the repository root
cd impl
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib --features server

# Randomized forward-propagation tests
cargo test --test forward_prop_fuzz

# Filter pipeline correctness, bounded work, history, and compaction contracts
cargo test --features server --test filter_pipeline

# Sorted-coordinate replay, move batches, downstream consumers, and bounded reads
cargo test --features server --test sorted_pipeline

# Real TCP/WebSocket protocol-v3 integration test
cargo test --features server --test protocol_v3_websocket
```

### Rust Lint Only

```bash
# From the repository root
cd impl
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features server -- -D warnings
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo clippy --all-targets --features python -- -D warnings
```

### Frontend Checks Only

```bash
# From the repository root
cd frontend
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
- JoinView basics (LEFT and INNER); RIGHT/FULL and composite keys have separate tests

**test_view_composition.py**, **test_filter_batches.py**, **test_sorted_batches.py**
- Supported filter → sort → aggregate and filter → aggregate chains
- Mixed batches, nullable values, stable ties, and explicit-constructor behavior
- Filter callback failure/retry without partial state commits

**test_bindings.py** - Legacy comprehensive test
- A legacy collection of binding checks
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
- **column/layout_tests.rs** and **column/bitmap.rs** - Native widths, NULL word/block boundaries, randomized storage models, interner ownership, and failure atomicity
- **table.rs** - Table operations
- **view/tests.rs** - Views and incremental propagation (module rooted at `view.rs`)
- **websocket.rs** - WebSocket protocol and JSON conversion
- **engine.rs** - Server table ownership, stable row IDs, and per-connection pipelines
- **pipeline_spec.rs** - Protocol pipeline validation and view construction
- **lib.rs** - Integration workflow

Rust integration tests under `../impl/tests/`:

- **forward_prop_fuzz.rs** - Differential randomized view propagation
- **filter_pipeline.rs** - Filter output coordinates, bounded work, history, and compaction
- **sorted_pipeline.rs** - Sorted-coordinate replay, move batches, and bounded source reads
- **protocol_v3_websocket.rs** - Real WebSocket deltas/snapshots, lost-final-update repair, and connection/generation isolation
- **engine/delivery_tests.rs** - Reconstructed clients vs fresh pipelines across mixed batches, bounded-history fallback, and node recovery

## Test Coverage

### What's Tested

✅ Table creation and schemas
✅ CRUD operations (Create, Read, Update, Delete)
✅ All column types (INT32, INT64, FLOAT32, FLOAT64, STRING, BOOL, DATE, DATETIME)
✅ Nullable columns
✅ FilterView with Python lambdas
✅ ProjectionView (column selection)
✅ ComputedView (dynamic columns)
✅ JoinView (LEFT, INNER, RIGHT, FULL, composite keys, incremental sync)
✅ SortedView and AggregateView incremental propagation
✅ `tick()` view registration and changeset compaction
✅ View chaining
✅ WebSocket row mutation semantics and snapshot/delta sequencing
✅ Protocol-v3 pipeline deltas/snapshots and checkpoint-based repair across real TCP/WebSocket connections
✅ Atomic client batches, stale/duplicate/gapped deliveries, bounded repair retries, generation cleanup, and delta-driven rendering
✅ Real-world workflows
✅ Performance with 1000+ rows

### What's NOT Tested (Yet)

⚠️ Full browser E2E coverage across real sockets and tabs
⚠️ Multi-client race conditions in a real browser/runtime, beyond hook-level fake socket tests
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
4. Rust library tests with the `server` feature enabled
5. Filter/sorted pipeline contracts and randomized forward-propagation tests
6. The protocol-v3 real-WebSocket integration test
7. Python package build plus pytest suite on Python 3.12
8. Frontend lint, Vitest, and production build

### Toolchain Split

The project intentionally validates different surfaces with different tools:

1. Core, server, and python Rust feature sets are linted directly with Cargo.
2. Python bindings are built as a wheel with `maturin` and validated with `pytest`.
3. Frontend behavior is validated with ESLint, Vitest, and `vite build`.

This avoids relying on `cargo test --all-features` for the PyO3 extension target, which is less stable across local Python installations. When running Cargo commands locally against newer Python runtimes, set `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`.

## Performance Testing

For allocator-counted column memory, numeric/string scans, and middle edits, see
the [typed storage harness and report](../docs/TYPED_COLUMN_STORAGE.md).

For performance benchmarks (separate from unit tests):

```bash
# From the repository root
cd benchmarks
python3 benchmark_vs_pandas.py
```

Or Rust benchmarks:

```bash
# From the repository root
cd impl
cargo bench
```

For end-to-end Rust mutation-plus-tick workloads, use the dedicated
[filter pipeline](../docs/INCREMENTAL_FILTER_PIPELINE.md#reproducible-benchmark)
and [sorted pipeline](../docs/INCREMENTAL_SORTED_PIPELINE.md#reproducible-benchmark)
harnesses. Their reports record commit/environment-specific timings; they are
not Python or browser benchmarks, nor CI performance thresholds.

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
4. Run the standard checks plus any relevant fuzz/protocol integration target before submitting a PR
5. Add focused regression coverage for every corrected failure mode

## Questions?

- See [../docs/PYTHON_BINDINGS_README.md](../docs/PYTHON_BINDINGS_README.md) for API reference
- See [../examples/](../examples/) for usage examples
- Run `pytest --help` for pytest options
