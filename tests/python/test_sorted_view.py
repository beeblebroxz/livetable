#!/usr/bin/env python3
"""
Unit tests for SortedView functionality
Tests sorting with ascending/descending order, multi-column sorts, and incremental updates
"""

import pytest
import livetable


class TestSortedViewBasic:
    """Test basic sorted view functionality"""

    def test_create_sorted_view_ascending(self):
        """Create a sorted view with ascending order"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("students", schema)

        table.append_row({"name": "Charlie", "score": 75})
        table.append_row({"name": "Alice", "score": 92})
        table.append_row({"name": "Bob", "score": 85})

        sorted_view = livetable.SortedView(
            "by_name",
            table,
            [livetable.SortKey.ascending("name")]
        )

        assert len(sorted_view) == 3
        assert sorted_view.get_value(0, "name") == "Alice"
        assert sorted_view.get_value(1, "name") == "Bob"
        assert sorted_view.get_value(2, "name") == "Charlie"

    def test_create_sorted_view_descending(self):
        """Create a sorted view with descending order"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("students", schema)

        table.append_row({"name": "Alice", "score": 75})
        table.append_row({"name": "Bob", "score": 92})
        table.append_row({"name": "Charlie", "score": 85})

        sorted_view = livetable.SortedView(
            "by_score_desc",
            table,
            [livetable.SortKey.descending("score")]
        )

        assert len(sorted_view) == 3
        assert sorted_view.get_value(0, "score") == 92  # Bob
        assert sorted_view.get_value(1, "score") == 85  # Charlie
        assert sorted_view.get_value(2, "score") == 75  # Alice


class TestSortedViewMultiColumn:
    """Test multi-column sorting"""

    def test_sort_by_two_columns(self):
        """Sort by department (asc) then salary (desc)"""
        schema = livetable.Schema([
            ("department", livetable.ColumnType.STRING, False),
            ("name", livetable.ColumnType.STRING, False),
            ("salary", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("employees", schema)

        # Engineering team
        table.append_row({"department": "Engineering", "name": "Alice", "salary": 100000})
        table.append_row({"department": "Sales", "name": "Bob", "salary": 80000})
        table.append_row({"department": "Engineering", "name": "Charlie", "salary": 90000})
        table.append_row({"department": "Sales", "name": "Diana", "salary": 85000})

        sorted_view = livetable.SortedView(
            "by_dept_salary",
            table,
            [
                livetable.SortKey.ascending("department"),
                livetable.SortKey.descending("salary"),
            ]
        )

        assert len(sorted_view) == 4

        # Engineering first (Alice 100k, then Charlie 90k)
        assert sorted_view.get_value(0, "name") == "Alice"
        assert sorted_view.get_value(0, "department") == "Engineering"
        assert sorted_view.get_value(1, "name") == "Charlie"
        assert sorted_view.get_value(1, "department") == "Engineering"

        # Sales second (Diana 85k, then Bob 80k)
        assert sorted_view.get_value(2, "name") == "Diana"
        assert sorted_view.get_value(2, "department") == "Sales"
        assert sorted_view.get_value(3, "name") == "Bob"
        assert sorted_view.get_value(3, "department") == "Sales"


class TestSortedViewWithNulls:
    """Test sorting with NULL values"""

    def test_nulls_last_default(self):
        """By default, NULLs should be sorted last"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("age", livetable.ColumnType.INT32, True),  # nullable
        ])
        table = livetable.Table("people", schema)

        table.append_row({"name": "Alice", "age": 30})
        table.append_row({"name": "Bob", "age": None})
        table.append_row({"name": "Charlie", "age": 25})

        sorted_view = livetable.SortedView(
            "by_age",
            table,
            [livetable.SortKey.ascending("age")]
        )

        assert sorted_view.get_value(0, "name") == "Charlie"  # 25
        assert sorted_view.get_value(1, "name") == "Alice"    # 30
        assert sorted_view.get_value(2, "name") == "Bob"      # null (last)

    def test_nulls_first(self):
        """NULLs can be sorted first with nulls_first=True"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("age", livetable.ColumnType.INT32, True),  # nullable
        ])
        table = livetable.Table("people", schema)

        table.append_row({"name": "Alice", "age": 30})
        table.append_row({"name": "Bob", "age": None})
        table.append_row({"name": "Charlie", "age": 25})

        sorted_view = livetable.SortedView(
            "by_age_nulls_first",
            table,
            [livetable.SortKey("age", livetable.SortOrder.ASCENDING, nulls_first=True)]
        )

        assert sorted_view.get_value(0, "name") == "Bob"      # null (first)
        assert sorted_view.get_value(1, "name") == "Charlie"  # 25
        assert sorted_view.get_value(2, "name") == "Alice"    # 30


class TestSortedViewIncrementalUpdates:
    """Test incremental update propagation for sorted views"""

    def test_incremental_insert(self):
        """New rows should be inserted in correct sorted position"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("students", schema)

        table.append_row({"name": "Bob", "score": 85})
        table.append_row({"name": "Diana", "score": 95})

        sorted_view = livetable.SortedView(
            "by_name",
            table,
            [livetable.SortKey.ascending("name")]
        )

        table.clear_changeset()

        # Initial state
        assert len(sorted_view) == 2
        assert sorted_view.get_value(0, "name") == "Bob"
        assert sorted_view.get_value(1, "name") == "Diana"

        # Add Alice (should go first)
        table.append_row({"name": "Alice", "score": 92})
        sorted_view.sync()

        assert len(sorted_view) == 3
        assert sorted_view.get_value(0, "name") == "Alice"
        assert sorted_view.get_value(1, "name") == "Bob"
        assert sorted_view.get_value(2, "name") == "Diana"

        # Add Charlie (should go between Bob and Diana)
        table.clear_changeset()
        table.append_row({"name": "Charlie", "score": 80})
        sorted_view.sync()

        assert len(sorted_view) == 4
        assert sorted_view.get_value(0, "name") == "Alice"
        assert sorted_view.get_value(1, "name") == "Bob"
        assert sorted_view.get_value(2, "name") == "Charlie"
        assert sorted_view.get_value(3, "name") == "Diana"

    def test_incremental_delete(self):
        """Deleted rows should be removed from sorted view"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("students", schema)

        table.append_row({"name": "Alice", "score": 92})
        table.append_row({"name": "Bob", "score": 85})
        table.append_row({"name": "Charlie", "score": 80})
        table.append_row({"name": "Diana", "score": 95})

        sorted_view = livetable.SortedView(
            "by_name",
            table,
            [livetable.SortKey.ascending("name")]
        )

        table.clear_changeset()
        assert len(sorted_view) == 4

        # Delete Bob (parent index 1)
        table.delete_row(1)
        sorted_view.sync()

        assert len(sorted_view) == 3
        assert sorted_view.get_value(0, "name") == "Alice"
        assert sorted_view.get_value(1, "name") == "Charlie"
        assert sorted_view.get_value(2, "name") == "Diana"

    def test_incremental_update_sort_key(self):
        """Updates to sort key columns should reposition rows"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("students", schema)

        table.append_row({"name": "Alice", "score": 70})
        table.append_row({"name": "Bob", "score": 80})
        table.append_row({"name": "Charlie", "score": 90})

        sorted_view = livetable.SortedView(
            "by_score",
            table,
            [livetable.SortKey.ascending("score")]
        )

        table.clear_changeset()

        # Initial order: Alice (70), Bob (80), Charlie (90)
        assert sorted_view.get_value(0, "name") == "Alice"
        assert sorted_view.get_value(1, "name") == "Bob"
        assert sorted_view.get_value(2, "name") == "Charlie"

        # Update Alice's score to 95 (should move to end)
        table.set_value(0, "score", 95)
        sorted_view.sync()

        # New order: Bob (80), Charlie (90), Alice (95)
        assert sorted_view.get_value(0, "name") == "Bob"
        assert sorted_view.get_value(1, "name") == "Charlie"
        assert sorted_view.get_value(2, "name") == "Alice"


class TestSortedViewOperations:
    """Test various sorted view operations"""

    def test_get_row(self):
        """Get a complete row from sorted view"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("students", schema)

        table.append_row({"name": "Bob", "score": 85})
        table.append_row({"name": "Alice", "score": 92})

        sorted_view = livetable.SortedView(
            "by_name",
            table,
            [livetable.SortKey.ascending("name")]
        )

        row = sorted_view.get_row(0)
        assert row["name"] == "Alice"
        assert row["score"] == 92

    def test_get_parent_index(self):
        """Get the parent table index for a sorted view position"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("test", schema)

        # Parent indices: 0=100, 1=50, 2=75
        table.append_row({"id": 1, "value": 100})
        table.append_row({"id": 2, "value": 50})
        table.append_row({"id": 3, "value": 75})

        sorted_view = livetable.SortedView(
            "by_value",
            table,
            [livetable.SortKey.ascending("value")]
        )

        # Sorted order by value: 50 (parent 1), 75 (parent 2), 100 (parent 0)
        assert sorted_view.get_parent_index(0) == 1  # 50
        assert sorted_view.get_parent_index(1) == 2  # 75
        assert sorted_view.get_parent_index(2) == 0  # 100

    def test_refresh(self):
        """Refresh should rebuild the sorted index"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema)

        table.append_row({"name": "Bob"})
        table.append_row({"name": "Alice"})

        sorted_view = livetable.SortedView(
            "by_name",
            table,
            [livetable.SortKey.ascending("name")]
        )

        assert sorted_view.get_value(0, "name") == "Alice"

        # Add Charlie without sync
        table.append_row({"name": "Charlie"})

        # Refresh to rebuild
        sorted_view.refresh()

        assert len(sorted_view) == 3
        assert sorted_view.get_value(2, "name") == "Charlie"


class TestSortedViewEdgeCases:
    """Test edge cases for sorted views"""

    def test_empty_table(self):
        """Sorted view on empty table"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("empty", schema)

        sorted_view = livetable.SortedView(
            "sorted_empty",
            table,
            [livetable.SortKey.ascending("name")]
        )

        assert len(sorted_view) == 0
        assert sorted_view.is_empty()

    def test_single_row(self):
        """Sorted view with single row"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("single", schema)
        table.append_row({"name": "Only"})

        sorted_view = livetable.SortedView(
            "sorted_single",
            table,
            [livetable.SortKey.ascending("name")]
        )

        assert len(sorted_view) == 1
        assert sorted_view.get_value(0, "name") == "Only"

    def test_duplicate_values(self):
        """Sorted view with duplicate sort key values"""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("duplicates", schema)

        table.append_row({"name": "Alice", "score": 80})
        table.append_row({"name": "Bob", "score": 80})
        table.append_row({"name": "Charlie", "score": 80})

        sorted_view = livetable.SortedView(
            "by_score",
            table,
            [livetable.SortKey.ascending("score")]
        )

        # All scores are equal, view should have all 3 rows
        assert len(sorted_view) == 3


class TestSortKeyAPI:
    """Test SortKey class API"""

    def test_sort_key_properties(self):
        """Test SortKey getters"""
        key = livetable.SortKey("score", livetable.SortOrder.DESCENDING, nulls_first=True)

        assert key.column == "score"
        assert key.nulls_first == True

    def test_sort_key_repr(self):
        """Test SortKey string representation"""
        key = livetable.SortKey.ascending("name")
        repr_str = repr(key)

        assert "name" in repr_str
        assert "ASCENDING" in repr_str

    def test_sort_key_default_order(self):
        """SortKey with no order should default to ascending"""
        key = livetable.SortKey("name")
        # Should work without errors - defaults to ascending
        assert key.column == "name"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
