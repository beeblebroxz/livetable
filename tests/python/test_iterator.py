"""
Tests for Python iterator protocol support.

This module tests the `for row in table:` syntax for all table and view types.
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
def sample_table(sample_schema):
    """Create a sample table with test data."""
    table = livetable.Table("students", sample_schema)
    table.append_row({"id": 1, "name": "Alice", "score": 95.5})
    table.append_row({"id": 2, "name": "Bob", "score": 87.0})
    table.append_row({"id": 3, "name": "Charlie", "score": 92.0})
    return table


# ============================================================================
# Table Iterator Tests
# ============================================================================

class TestTableIterator:
    """Tests for iterating over Table objects."""

    def test_table_iteration_basic(self, sample_table):
        """Table should be iterable with for loop."""
        rows = list(sample_table)
        assert len(rows) == 3

    def test_table_iteration_values(self, sample_table):
        """Iterated rows should contain correct values."""
        rows = list(sample_table)

        # Check first row
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["score"] == 95.5

        # Check last row
        assert rows[2]["id"] == 3
        assert rows[2]["name"] == "Charlie"

    def test_table_iteration_empty(self, sample_schema):
        """Empty table should yield no rows."""
        empty_table = livetable.Table("empty", sample_schema)
        rows = list(empty_table)
        assert rows == []

    def test_table_multiple_iterations(self, sample_table):
        """Table should be iterable multiple times."""
        first_pass = list(sample_table)
        second_pass = list(sample_table)
        assert first_pass == second_pass

    def test_table_iteration_with_break(self, sample_table):
        """Early break should work correctly."""
        count = 0
        for row in sample_table:
            count += 1
            if count == 2:
                break
        assert count == 2

    def test_table_iteration_comprehension(self, sample_table):
        """List comprehension should work."""
        names = [row["name"] for row in sample_table]
        assert names == ["Alice", "Bob", "Charlie"]


# ============================================================================
# FilterView Iterator Tests
# ============================================================================

class TestFilterViewIterator:
    """Tests for iterating over FilterView objects."""

    def test_filter_view_iteration(self, sample_table):
        """FilterView should be iterable."""
        high_scorers = sample_table.filter(lambda r: r["score"] >= 90)
        rows = list(high_scorers)
        assert len(rows) == 2
        assert all(row["score"] >= 90 for row in rows)

    def test_filter_view_empty_result(self, sample_table):
        """FilterView with no matches should yield empty."""
        no_matches = sample_table.filter(lambda r: r["score"] > 100)
        rows = list(no_matches)
        assert rows == []

    def test_filter_view_all_match(self, sample_table):
        """FilterView matching all rows."""
        all_rows = sample_table.filter(lambda r: r["score"] > 0)
        rows = list(all_rows)
        assert len(rows) == 3


# ============================================================================
# ProjectionView Iterator Tests
# ============================================================================

class TestProjectionViewIterator:
    """Tests for iterating over ProjectionView objects."""

    def test_projection_view_iteration(self, sample_table):
        """ProjectionView should be iterable."""
        names_scores = sample_table.select(["name", "score"])
        rows = list(names_scores)
        assert len(rows) == 3
        # Only selected columns should be present
        assert set(rows[0].keys()) == {"name", "score"}

    def test_projection_single_column(self, sample_table):
        """ProjectionView with single column."""
        names_only = sample_table.select(["name"])
        rows = list(names_only)
        assert len(rows) == 3
        assert set(rows[0].keys()) == {"name"}


# ============================================================================
# ComputedView Iterator Tests
# ============================================================================

class TestComputedViewIterator:
    """Tests for iterating over ComputedView objects."""

    def test_computed_view_iteration(self, sample_table):
        """ComputedView should be iterable."""
        with_grade = sample_table.add_computed_column(
            "grade",
            lambda r: "A" if r["score"] >= 90 else "B"
        )
        rows = list(with_grade)
        assert len(rows) == 3
        # Should have computed column
        assert "grade" in rows[0]
        assert rows[0]["grade"] == "A"  # Alice scored 95.5
        assert rows[1]["grade"] == "B"  # Bob scored 87.0


# ============================================================================
# SortedView Iterator Tests
# ============================================================================

class TestSortedViewIterator:
    """Tests for iterating over SortedView objects."""

    def test_sorted_view_iteration_desc(self, sample_table):
        """SortedView should be iterable in sort order."""
        sorted_view = livetable.SortedView(
            "by_score",
            sample_table,
            [livetable.SortKey.descending("score")]
        )
        rows = list(sorted_view)
        assert len(rows) == 3
        # Should be sorted by score descending
        assert rows[0]["name"] == "Alice"   # 95.5
        assert rows[1]["name"] == "Charlie" # 92.0
        assert rows[2]["name"] == "Bob"     # 87.0

    def test_sorted_view_iteration_asc(self, sample_table):
        """SortedView ascending order."""
        sorted_view = livetable.SortedView(
            "by_score_asc",
            sample_table,
            [livetable.SortKey.ascending("score")]
        )
        rows = list(sorted_view)
        assert rows[0]["name"] == "Bob"     # 87.0
        assert rows[2]["name"] == "Alice"   # 95.5


# ============================================================================
# JoinView Iterator Tests
# ============================================================================

class TestJoinViewIterator:
    """Tests for iterating over JoinView objects."""

    def test_join_view_iteration(self):
        """JoinView should be iterable."""
        # Create users table
        users_schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        users = livetable.Table("users", users_schema)
        users.append_row({"id": 1, "name": "Alice"})
        users.append_row({"id": 2, "name": "Bob"})

        # Create orders table
        orders_schema = livetable.Schema([
            ("order_id", livetable.ColumnType.INT32, False),
            ("user_id", livetable.ColumnType.INT32, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        orders = livetable.Table("orders", orders_schema)
        orders.append_row({"order_id": 101, "user_id": 1, "amount": 99.99})
        orders.append_row({"order_id": 102, "user_id": 2, "amount": 25.00})

        # Join
        joined = livetable.JoinView(
            "user_orders",
            users,
            orders,
            "id",
            "user_id",
            livetable.JoinType.INNER
        )

        rows = list(joined)
        assert len(rows) == 2
        # Check both left and right columns are present
        assert "name" in rows[0]
        assert "right_amount" in rows[0]


# ============================================================================
# AggregateView Iterator Tests
# ============================================================================

class TestAggregateViewIterator:
    """Tests for iterating over AggregateView objects."""

    def test_aggregate_view_iteration(self, sample_table):
        """AggregateView should be iterable."""
        agg = livetable.AggregateView(
            "stats",
            sample_table,
            [],  # No group by - aggregate all
            [
                ("total_score", "score", livetable.AggregateFunction.SUM),
                ("avg_score", "score", livetable.AggregateFunction.AVG),
                ("count", "score", livetable.AggregateFunction.COUNT),
            ]
        )

        rows = list(agg)
        assert len(rows) == 1
        assert rows[0]["total_score"] == 95.5 + 87.0 + 92.0
        assert rows[0]["count"] == 3

    def test_aggregate_view_grouped(self):
        """AggregateView with GROUP BY."""
        schema = livetable.Schema([
            ("dept", livetable.ColumnType.STRING, False),
            ("salary", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("employees", schema)
        table.append_row({"dept": "Engineering", "salary": 100000.0})
        table.append_row({"dept": "Engineering", "salary": 120000.0})
        table.append_row({"dept": "Sales", "salary": 80000.0})

        agg = livetable.AggregateView(
            "by_dept",
            table,
            ["dept"],
            [("avg_salary", "salary", livetable.AggregateFunction.AVG)]
        )

        rows = list(agg)
        assert len(rows) == 2

        # Find each group
        eng_row = next(r for r in rows if r["dept"] == "Engineering")
        sales_row = next(r for r in rows if r["dept"] == "Sales")

        assert eng_row["avg_salary"] == 110000.0
        assert sales_row["avg_salary"] == 80000.0


# ============================================================================
# Integration Tests
# ============================================================================

class TestIteratorIntegration:
    """Integration tests for iterator protocol."""

    def test_chained_operations(self, sample_table):
        """Chained view operations should all be iterable."""
        # Filter -> Project -> Iterate
        high_scorers = sample_table.filter(lambda r: r["score"] >= 90)
        names = high_scorers  # Note: can't chain .select() on FilterView

        result = [row["name"] for row in high_scorers]
        assert "Alice" in result
        assert "Charlie" in result
        assert "Bob" not in result

    def test_sum_via_iteration(self, sample_table):
        """Can use iteration to compute aggregates manually."""
        total = sum(row["score"] for row in sample_table)
        assert total == 95.5 + 87.0 + 92.0

    def test_enumerate_works(self, sample_table):
        """enumerate() should work with iterators."""
        for i, row in enumerate(sample_table):
            assert row["id"] == i + 1

    def test_zip_works(self, sample_table):
        """zip() should work with iterators."""
        names = ["Alice", "Bob", "Charlie"]
        for row, expected_name in zip(sample_table, names):
            assert row["name"] == expected_name
