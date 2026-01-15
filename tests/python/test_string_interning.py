#!/usr/bin/env python3
"""
Unit tests for string interning functionality
Tests memory optimization for repeated string values
"""

import pytest
import livetable


class TestStringInterningBasic:
    """Test basic string interning functionality"""

    def test_create_table_with_interning(self):
        """Create a table with string interning enabled"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema, use_string_interning=True)
        assert table.uses_string_interning() == True

    def test_create_table_without_interning(self):
        """Create a table without string interning (default)"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema)
        assert table.uses_string_interning() == False
        assert table.interner_stats() is None

    def test_interner_stats_basic(self):
        """Get basic interner statistics"""
        schema = livetable.Schema([
            ("category", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("products", schema, use_string_interning=True)

        # Add rows with repeated strings
        table.append_row({"category": "Electronics"})
        table.append_row({"category": "Clothing"})
        table.append_row({"category": "Electronics"})  # Duplicate
        table.append_row({"category": "Electronics"})  # Another duplicate

        stats = table.interner_stats()
        assert stats is not None
        assert stats["unique_strings"] == 2  # Electronics and Clothing
        assert stats["total_references"] == 4  # 3 Electronics + 1 Clothing


class TestStringInterningDeduplication:
    """Test that string deduplication works correctly"""

    def test_repeated_strings_deduplicated(self):
        """Verify repeated strings share the same storage"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("status", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("orders", schema, use_string_interning=True)

        # Add 100 rows with only 3 unique status values
        for i in range(100):
            status = ["pending", "processing", "completed"][i % 3]
            table.append_row({"id": i, "status": status})

        assert len(table) == 100

        stats = table.interner_stats()
        assert stats["unique_strings"] == 3
        assert stats["total_references"] == 100

    def test_values_remain_correct_with_interning(self):
        """Verify that values are still correct when using interning"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema, use_string_interning=True)

        names = ["Alice", "Bob", "Alice", "Charlie", "Bob", "Alice"]
        for name in names:
            table.append_row({"name": name})

        # Verify all values are correct
        for i, expected in enumerate(names):
            assert table.get_value(i, "name") == expected


class TestStringInterningOperations:
    """Test string interning with various table operations"""

    @pytest.fixture
    def interned_table(self):
        """Create a table with string interning and test data"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("category", livetable.ColumnType.STRING, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("products", schema, use_string_interning=True)
        table.append_row({"id": 1, "category": "Electronics", "name": "Phone"})
        table.append_row({"id": 2, "category": "Electronics", "name": "Laptop"})
        table.append_row({"id": 3, "category": "Clothing", "name": "Shirt"})
        return table

    def test_update_value_with_interning(self, interned_table):
        """Update a value and verify interning updates correctly"""
        # Initially: 2 Electronics, 1 Clothing
        stats_before = interned_table.interner_stats()

        # Change first Electronics to Clothing
        interned_table.set_value(0, "category", "Clothing")

        # Now: 1 Electronics, 2 Clothing
        assert interned_table.get_value(0, "category") == "Clothing"
        assert interned_table.get_value(1, "category") == "Electronics"
        assert interned_table.get_value(2, "category") == "Clothing"

        stats_after = interned_table.interner_stats()
        # Still 2 unique categories
        assert stats_after["unique_strings"] >= 2

    def test_delete_row_with_interning(self, interned_table):
        """Delete a row and verify interning updates correctly"""
        # Delete the only Electronics with Laptop
        interned_table.delete_row(1)

        assert len(interned_table) == 2
        assert interned_table.get_value(0, "name") == "Phone"
        assert interned_table.get_value(1, "name") == "Shirt"

        # Stats should update
        stats = interned_table.interner_stats()
        assert stats is not None

    def test_insert_row_with_interning(self, interned_table):
        """Insert a row with a new unique string"""
        interned_table.append_row({"id": 4, "category": "Books", "name": "Novel"})

        assert len(interned_table) == 4
        assert interned_table.get_value(3, "category") == "Books"

        stats = interned_table.interner_stats()
        # Should now have more unique strings
        assert stats["unique_strings"] >= 3


class TestStringInterningMultipleColumns:
    """Test string interning across multiple string columns"""

    def test_multiple_string_columns_share_interner(self):
        """Multiple string columns should share the same interner"""
        schema = livetable.Schema([
            ("first_name", livetable.ColumnType.STRING, False),
            ("last_name", livetable.ColumnType.STRING, False),
            ("city", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("people", schema, use_string_interning=True)

        # Add rows where strings repeat across columns
        table.append_row({"first_name": "John", "last_name": "Smith", "city": "London"})
        table.append_row({"first_name": "Jane", "last_name": "Smith", "city": "Paris"})
        table.append_row({"first_name": "John", "last_name": "Doe", "city": "London"})

        stats = table.interner_stats()
        # "John" appears twice, "Smith" appears twice, "London" appears twice
        # Unique: John, Jane, Smith, Doe, London, Paris = 6
        assert stats["unique_strings"] == 6
        # Total refs: 3 rows * 3 columns = 9
        assert stats["total_references"] == 9


class TestStringInterningEdgeCases:
    """Test edge cases for string interning"""

    def test_empty_string_interning(self):
        """Test interning with empty strings"""
        schema = livetable.Schema([
            ("value", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema, use_string_interning=True)

        table.append_row({"value": ""})
        table.append_row({"value": ""})
        table.append_row({"value": "not empty"})

        assert table.get_value(0, "value") == ""
        assert table.get_value(1, "value") == ""
        assert table.get_value(2, "value") == "not empty"

        stats = table.interner_stats()
        assert stats["unique_strings"] == 2  # "" and "not empty"

    def test_special_characters_interning(self):
        """Test interning with special characters"""
        schema = livetable.Schema([
            ("text", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema, use_string_interning=True)

        special_strings = [
            "hello\nworld",
            "tab\there",
            "unicode: 日本語",
            "emoji: 🎉",
            "hello\nworld",  # Duplicate
        ]

        for s in special_strings:
            table.append_row({"text": s})

        assert table.get_value(0, "text") == "hello\nworld"
        assert table.get_value(2, "text") == "unicode: 日本語"
        assert table.get_value(3, "text") == "emoji: 🎉"

        stats = table.interner_stats()
        assert stats["unique_strings"] == 4  # 5 strings, 1 duplicate


class TestStringInterningWithTieredVector:
    """Test string interning with tiered vector storage"""

    def test_interning_with_tiered_vector(self):
        """Test that interning works with tiered vector storage"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table(
            "test", schema,
            use_tiered_vector=True,
            use_string_interning=True
        )

        table.append_row({"id": 1, "name": "Alice"})
        table.append_row({"id": 2, "name": "Bob"})
        table.append_row({"id": 3, "name": "Alice"})

        assert table.uses_string_interning() == True
        assert len(table) == 3
        assert table.get_value(0, "name") == "Alice"
        assert table.get_value(2, "name") == "Alice"

        stats = table.interner_stats()
        assert stats["unique_strings"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
