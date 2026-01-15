#!/usr/bin/env python3
"""
Unit tests for basic table operations
Tests CRUD operations on tables
"""

import pytest
import livetable


class TestTableCreation:
    """Test table creation and schema validation"""

    def test_create_simple_table(self):
        """Create a table with basic schema"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("users", schema)
        assert len(table) == 0
        assert table.column_names() == ["id", "name"]

    def test_create_table_with_nullable_columns(self):
        """Create a table with nullable columns"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("age", livetable.ColumnType.INT32, True),  # Nullable
        ])
        table = livetable.Table("test", schema)
        assert len(table) == 0

    def test_create_table_with_all_types(self):
        """Create a table with all supported column types"""
        schema = livetable.Schema([
            ("col_int32", livetable.ColumnType.INT32, False),
            ("col_int64", livetable.ColumnType.INT64, False),
            ("col_float32", livetable.ColumnType.FLOAT32, False),
            ("col_float64", livetable.ColumnType.FLOAT64, False),
            ("col_string", livetable.ColumnType.STRING, False),
            ("col_bool", livetable.ColumnType.BOOL, False),
        ])
        table = livetable.Table("all_types", schema)
        assert len(table.column_names()) == 6


class TestTableInsert:
    """Test inserting rows into tables"""

    @pytest.fixture
    def simple_table(self):
        """Create a simple table for testing"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.FLOAT64, False),
        ])
        return livetable.Table("test", schema)

    def test_insert_single_row(self, simple_table):
        """Insert a single row"""
        simple_table.append_row({"id": 1, "name": "Alice", "score": 95.5})
        assert len(simple_table) == 1
        row = simple_table.get_row(0)
        assert row["id"] == 1
        assert row["name"] == "Alice"
        assert row["score"] == 95.5

    def test_insert_multiple_rows(self, simple_table):
        """Insert multiple rows"""
        simple_table.append_row({"id": 1, "name": "Alice", "score": 95.5})
        simple_table.append_row({"id": 2, "name": "Bob", "score": 87.3})
        simple_table.append_row({"id": 3, "name": "Charlie", "score": 92.1})
        assert len(simple_table) == 3

    def test_insert_with_null_values(self):
        """Insert rows with null values in nullable columns"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("age", livetable.ColumnType.INT32, True),  # Nullable
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "age": 30})
        table.append_row({"id": 2, "age": None})

        assert table.get_value(0, "age") == 30
        assert table.get_value(1, "age") is None


class TestTableRead:
    """Test reading data from tables"""

    @pytest.fixture
    def populated_table(self):
        """Create a table with test data"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("age", livetable.ColumnType.INT32, True),
        ])
        table = livetable.Table("users", schema)
        table.append_row({"id": 1, "name": "Alice", "age": 30})
        table.append_row({"id": 2, "name": "Bob", "age": 25})
        table.append_row({"id": 3, "name": "Charlie", "age": None})
        return table

    def test_get_row(self, populated_table):
        """Get a complete row by index"""
        row = populated_table.get_row(1)
        assert row["id"] == 2
        assert row["name"] == "Bob"
        assert row["age"] == 25

    def test_get_value(self, populated_table):
        """Get a specific cell value"""
        assert populated_table.get_value(0, "name") == "Alice"
        assert populated_table.get_value(1, "age") == 25
        assert populated_table.get_value(2, "age") is None

    def test_column_names(self, populated_table):
        """Get column names"""
        columns = populated_table.column_names()
        assert columns == ["id", "name", "age"]


class TestTableUpdate:
    """Test updating data in tables"""

    @pytest.fixture
    def editable_table(self):
        """Create a table for update tests"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "value": 100})
        table.append_row({"id": 2, "value": 200})
        return table

    def test_update_single_value(self, editable_table):
        """Update a single cell value"""
        editable_table.set_value(0, "value", 999)
        assert editable_table.get_value(0, "value") == 999

    def test_update_to_null(self):
        """Update a value to null in nullable column"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("optional", livetable.ColumnType.INT32, True),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "optional": 42})

        table.set_value(0, "optional", None)
        assert table.get_value(0, "optional") is None


class TestTableDelete:
    """Test deleting rows from tables"""

    def test_delete_row(self):
        """Delete a row by index"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "name": "Alice"})
        table.append_row({"id": 2, "name": "Bob"})
        table.append_row({"id": 3, "name": "Charlie"})

        table.delete_row(1)  # Delete Bob
        assert len(table) == 2
        assert table.get_value(0, "name") == "Alice"
        assert table.get_value(1, "name") == "Charlie"  # Charlie moved down


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
