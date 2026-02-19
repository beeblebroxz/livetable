#!/usr/bin/env python3
"""
Tests for Pythonic indexing features:
- table[idx] syntax (positive and negative)
- table[start:stop] slicing
- table[-1] negative indexing
- Consistency across tables and views
"""

import pytest
import livetable


@pytest.fixture
def sample_table():
    """Create a table with sample data for testing."""
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("name", livetable.ColumnType.STRING, False),
        ("score", livetable.ColumnType.FLOAT64, False),
    ])
    table = livetable.Table("test", schema)
    table.append_row({"id": 1, "name": "Alice", "score": 95.0})
    table.append_row({"id": 2, "name": "Bob", "score": 87.0})
    table.append_row({"id": 3, "name": "Charlie", "score": 92.0})
    table.append_row({"id": 4, "name": "Diana", "score": 78.0})
    table.append_row({"id": 5, "name": "Eve", "score": 99.0})
    return table


class TestPositiveIndexing:
    """Tests for table[idx] with positive indices."""

    def test_first_element(self, sample_table):
        """Access first element with table[0]."""
        row = sample_table[0]
        assert row["id"] == 1
        assert row["name"] == "Alice"
        assert row["score"] == 95.0

    def test_middle_element(self, sample_table):
        """Access middle element."""
        row = sample_table[2]
        assert row["id"] == 3
        assert row["name"] == "Charlie"

    def test_last_element_positive(self, sample_table):
        """Access last element with positive index."""
        row = sample_table[4]
        assert row["id"] == 5
        assert row["name"] == "Eve"

    def test_index_out_of_range(self, sample_table):
        """Accessing beyond bounds raises IndexError."""
        with pytest.raises(IndexError):
            _ = sample_table[10]

    def test_index_equals_length(self, sample_table):
        """Index equal to length raises IndexError."""
        with pytest.raises(IndexError):
            _ = sample_table[5]  # Length is 5, valid indices are 0-4


class TestNegativeIndexing:
    """Tests for table[-idx] negative indexing."""

    def test_last_element(self, sample_table):
        """table[-1] returns last element."""
        row = sample_table[-1]
        assert row["id"] == 5
        assert row["name"] == "Eve"

    def test_second_to_last(self, sample_table):
        """table[-2] returns second-to-last element."""
        row = sample_table[-2]
        assert row["id"] == 4
        assert row["name"] == "Diana"

    def test_first_via_negative(self, sample_table):
        """table[-len] returns first element."""
        row = sample_table[-5]
        assert row["id"] == 1
        assert row["name"] == "Alice"

    def test_negative_out_of_range(self, sample_table):
        """Negative index beyond length raises IndexError."""
        with pytest.raises(IndexError):
            _ = sample_table[-6]  # Only 5 elements

    def test_negative_consistency(self, sample_table):
        """Negative indices are consistent with positive."""
        for i in range(len(sample_table)):
            pos = sample_table[i]
            neg = sample_table[i - len(sample_table)]
            assert pos["id"] == neg["id"]
            assert pos["name"] == neg["name"]


class TestSlicing:
    """Tests for table[start:stop] slicing."""

    def test_slice_from_start(self, sample_table):
        """table[:2] returns first two elements."""
        result = sample_table[:2]
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2

    def test_slice_to_end(self, sample_table):
        """table[3:] returns last two elements."""
        result = sample_table[3:]
        assert len(result) == 2
        assert result[0]["id"] == 4
        assert result[1]["id"] == 5

    def test_slice_middle(self, sample_table):
        """table[1:4] returns middle elements."""
        result = sample_table[1:4]
        assert len(result) == 3
        assert result[0]["id"] == 2
        assert result[1]["id"] == 3
        assert result[2]["id"] == 4

    def test_slice_full_table(self, sample_table):
        """table[:] returns all elements."""
        result = sample_table[:]
        assert len(result) == 5

    def test_slice_empty(self, sample_table):
        """table[2:2] returns empty list."""
        result = sample_table[2:2]
        assert len(result) == 0

    def test_slice_negative_start(self, sample_table):
        """table[-2:] returns last two elements."""
        result = sample_table[-2:]
        assert len(result) == 2
        assert result[0]["id"] == 4
        assert result[1]["id"] == 5

    def test_slice_negative_end(self, sample_table):
        """table[:-2] returns all but last two."""
        result = sample_table[:-2]
        assert len(result) == 3
        assert result[-1]["id"] == 3

    def test_slice_both_negative(self, sample_table):
        """table[-3:-1] returns elements from -3 to -2."""
        result = sample_table[-3:-1]
        assert len(result) == 2
        assert result[0]["id"] == 3
        assert result[1]["id"] == 4

    def test_slice_out_of_bounds_clipped(self, sample_table):
        """Slice beyond bounds is clipped (Python behavior)."""
        result = sample_table[2:100]
        assert len(result) == 3  # Elements 2, 3, 4

    def test_slice_reversed_empty(self, sample_table):
        """Slice with start > stop returns empty."""
        result = sample_table[4:2]
        assert len(result) == 0


class TestViewIndexing:
    """Tests for indexing on views (not just tables)."""

    def test_filter_view_negative_index(self, sample_table):
        """FilterView supports negative indexing."""
        filtered = sample_table.filter(lambda r: r["score"] >= 90)
        # Should have: Alice(95), Charlie(92), Eve(99) = 3 elements
        last = filtered[-1]
        assert last["name"] == "Eve"

    def test_filter_view_slice(self, sample_table):
        """FilterView supports slicing."""
        filtered = sample_table.filter(lambda r: r["score"] >= 90)
        result = filtered[:2]
        assert len(result) == 2

    def test_sorted_view_negative_index(self, sample_table):
        """SortedView supports negative indexing."""
        sorted_view = sample_table.sort("score", descending=True)
        last = sorted_view[-1]  # Lowest score
        assert last["name"] == "Diana"
        assert last["score"] == 78.0

    def test_sorted_view_slice(self, sample_table):
        """SortedView supports slicing."""
        sorted_view = sample_table.sort("score", descending=True)
        top3 = sorted_view[:3]
        assert len(top3) == 3
        assert top3[0]["name"] == "Eve"  # 99
        assert top3[1]["name"] == "Alice"  # 95
        assert top3[2]["name"] == "Charlie"  # 92

    def test_projection_view_reverse_slice_with_step(self, sample_table):
        """ProjectionView supports reverse slicing with step."""
        projected = sample_table.select(["id"])
        result = projected[::-2]
        assert [row["id"] for row in result] == [5, 3, 1]

    def test_computed_view_reverse_slice_with_step(self, sample_table):
        """ComputedView supports reverse slicing with step."""
        computed = sample_table.add_computed_column("double_id", lambda r: r["id"] * 2)
        result = computed[::-2]
        assert [row["id"] for row in result] == [5, 3, 1]
        assert [row["double_id"] for row in result] == [10, 6, 2]

    def test_join_view_reverse_slice_with_step(self, sample_table):
        """JoinView supports reverse slicing with step."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("group", livetable.ColumnType.STRING, False),
        ])
        right = livetable.Table("right", schema)
        right.append_row({"id": 1, "group": "A"})
        right.append_row({"id": 2, "group": "B"})
        right.append_row({"id": 3, "group": "C"})
        right.append_row({"id": 4, "group": "D"})
        right.append_row({"id": 5, "group": "E"})

        joined = sample_table.join(right, on="id")
        result = joined[::-2]
        assert [row["id"] for row in result] == [5, 3, 1]
        assert [row["right_group"] for row in result] == ["E", "C", "A"]

    def test_sorted_view_reverse_slice_with_step(self, sample_table):
        """SortedView supports reverse slicing with step."""
        sorted_view = sample_table.sort("id")
        result = sorted_view[::-2]
        assert [row["id"] for row in result] == [5, 3, 1]


class TestEmptyTable:
    """Tests for indexing on empty tables."""

    def test_empty_table_index_raises(self):
        """Indexing empty table raises IndexError."""
        schema = livetable.Schema([("id", livetable.ColumnType.INT32, False)])
        table = livetable.Table("empty", schema)
        with pytest.raises(IndexError):
            _ = table[0]

    def test_empty_table_negative_raises(self):
        """Negative indexing empty table raises IndexError."""
        schema = livetable.Schema([("id", livetable.ColumnType.INT32, False)])
        table = livetable.Table("empty", schema)
        with pytest.raises(IndexError):
            _ = table[-1]

    def test_empty_table_slice_empty(self):
        """Slicing empty table returns empty list."""
        schema = livetable.Schema([("id", livetable.ColumnType.INT32, False)])
        table = livetable.Table("empty", schema)
        result = table[:]
        assert len(result) == 0


class TestSingleElementTable:
    """Tests for single-element edge cases."""

    def test_single_element_positive(self):
        """Single element accessible at index 0."""
        schema = livetable.Schema([("id", livetable.ColumnType.INT32, False)])
        table = livetable.Table("single", schema)
        table.append_row({"id": 42})
        assert table[0]["id"] == 42

    def test_single_element_negative(self):
        """Single element accessible at index -1."""
        schema = livetable.Schema([("id", livetable.ColumnType.INT32, False)])
        table = livetable.Table("single", schema)
        table.append_row({"id": 42})
        assert table[-1]["id"] == 42

    def test_single_element_both_same(self):
        """table[0] and table[-1] return same for single element."""
        schema = livetable.Schema([("id", livetable.ColumnType.INT32, False)])
        table = livetable.Table("single", schema)
        table.append_row({"id": 42})
        assert table[0]["id"] == table[-1]["id"]


class TestIndexingEquivalence:
    """Tests that table[idx] equals table.get_row(idx)."""

    def test_getitem_equals_get_row(self, sample_table):
        """table[i] returns same as table.get_row(i)."""
        for i in range(len(sample_table)):
            getitem = sample_table[i]
            getrow = sample_table.get_row(i)
            assert getitem["id"] == getrow["id"]
            assert getitem["name"] == getrow["name"]
            assert getitem["score"] == getrow["score"]
