"""
Tests for bulk operations (append_rows).

This module tests the efficient bulk insertion of rows.
"""

import pytest
import livetable


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    return livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("name", livetable.ColumnType.STRING, False),
        ("score", livetable.ColumnType.FLOAT64, True),
    ])


@pytest.fixture
def empty_table(sample_schema):
    """Create an empty table for testing."""
    return livetable.Table("test", sample_schema)


# ============================================================================
# Basic Bulk Insert Tests
# ============================================================================

class TestBulkInsertBasic:
    """Tests for basic bulk insert functionality."""

    def test_append_rows_basic(self, empty_table):
        """append_rows should insert multiple rows at once."""
        rows = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
            {"id": 3, "name": "Charlie", "score": 92.0},
        ]
        count = empty_table.append_rows(rows)

        assert count == 3
        assert len(empty_table) == 3

    def test_append_rows_returns_count(self, empty_table):
        """append_rows should return the number of rows inserted."""
        rows = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
        ]
        count = empty_table.append_rows(rows)

        assert count == 2

    def test_append_rows_empty_list(self, empty_table):
        """append_rows with empty list should return 0."""
        count = empty_table.append_rows([])

        assert count == 0
        assert len(empty_table) == 0

    def test_append_rows_values_correct(self, empty_table):
        """Values inserted via append_rows should be correct."""
        rows = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": None},
        ]
        empty_table.append_rows(rows)

        row0 = empty_table.get_row(0)
        assert row0["id"] == 1
        assert row0["name"] == "Alice"
        assert row0["score"] == 95.5

        row1 = empty_table.get_row(1)
        assert row1["id"] == 2
        assert row1["name"] == "Bob"
        assert row1["score"] is None


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestBulkInsertErrors:
    """Tests for error handling in bulk insert."""

    def test_append_rows_missing_column(self, empty_table):
        """append_rows should error if a row is missing a column."""
        rows = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob"},  # Missing 'score'
        ]

        with pytest.raises(ValueError, match="Missing"):
            empty_table.append_rows(rows)

        # No rows should be inserted on error
        assert len(empty_table) == 0

    def test_append_rows_invalid_row_type(self, empty_table):
        """append_rows should error if row is not a dict."""
        rows = [
            {"id": 1, "name": "Alice", "score": 95.5},
            [1, "Bob", 87.0],  # List instead of dict
        ]

        with pytest.raises(ValueError, match="dictionary"):
            empty_table.append_rows(rows)

    def test_append_rows_type_mismatch(self, empty_table):
        """append_rows should error on type mismatch."""
        rows = [
            {"id": "not an int", "name": "Alice", "score": 95.5},
        ]

        # Type mismatches cause a PanicException (BaseException subclass)
        with pytest.raises(BaseException):
            empty_table.append_rows(rows)


# ============================================================================
# Large Data Tests
# ============================================================================

class TestBulkInsertLarge:
    """Tests for bulk insert with larger datasets."""

    def test_append_rows_1000_rows(self, sample_schema):
        """append_rows should handle 1000 rows efficiently."""
        table = livetable.Table("large", sample_schema)

        rows = [
            {"id": i, "name": f"User{i}", "score": float(i % 100)}
            for i in range(1000)
        ]

        count = table.append_rows(rows)

        assert count == 1000
        assert len(table) == 1000

        # Verify some values
        assert table.get_row(0)["id"] == 0
        assert table.get_row(999)["id"] == 999

    def test_append_rows_multiple_batches(self, sample_schema):
        """Multiple append_rows calls should accumulate correctly."""
        table = livetable.Table("batched", sample_schema)

        for batch in range(10):
            rows = [
                {"id": batch * 100 + i, "name": f"User{batch}-{i}", "score": float(i)}
                for i in range(100)
            ]
            table.append_rows(rows)

        assert len(table) == 1000


# ============================================================================
# Integration Tests
# ============================================================================

class TestBulkInsertIntegration:
    """Integration tests for bulk insert with other features."""

    def test_append_rows_with_filter(self, empty_table):
        """Filtered views should work after bulk insert."""
        rows = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
            {"id": 3, "name": "Charlie", "score": 92.0},
        ]
        empty_table.append_rows(rows)

        high_scorers = empty_table.filter(lambda r: r["score"] >= 90)
        assert len(high_scorers) == 2

    def test_append_rows_with_iteration(self, empty_table):
        """Iteration should work after bulk insert."""
        rows = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
        ]
        empty_table.append_rows(rows)

        names = [row["name"] for row in empty_table]
        assert names == ["Alice", "Bob"]

    def test_append_rows_with_aggregation(self, empty_table):
        """Aggregations should work after bulk insert."""
        rows = [
            {"id": 1, "name": "Alice", "score": 90.0},
            {"id": 2, "name": "Bob", "score": 80.0},
            {"id": 3, "name": "Charlie", "score": 100.0},
        ]
        empty_table.append_rows(rows)

        assert empty_table.sum("score") == 270.0
        assert empty_table.avg("score") == 90.0
        assert empty_table.min("score") == 80.0
        assert empty_table.max("score") == 100.0
