#!/usr/bin/env python3
"""
Unit tests for view operations
Tests FilterView, ProjectionView, ComputedView, and JoinView
"""

import pytest
import livetable


class TestFilterView:
    """Test filter view functionality"""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table for filtering"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("age", livetable.ColumnType.INT32, True),
            ("score", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("users", schema)
        table.append_row({"id": 1, "name": "Alice", "age": 30, "score": 95.5})
        table.append_row({"id": 2, "name": "Bob", "age": 25, "score": 87.3})
        table.append_row({"id": 3, "name": "Charlie", "age": 35, "score": 92.1})
        table.append_row({"id": 4, "name": "Diana", "age": None, "score": 88.7})
        return table

    def test_filter_basic(self, sample_table):
        """Filter rows based on a simple condition"""
        adults = sample_table.filter(lambda row: row.get("age") is not None and row["age"] >= 30)
        assert len(adults) == 2  # Alice and Charlie
        assert adults.get_value(0, "name") == "Alice"
        assert adults.get_value(1, "name") == "Charlie"

    def test_filter_with_score(self, sample_table):
        """Filter based on score"""
        high_scorers = sample_table.filter(lambda row: row["score"] >= 90)
        assert len(high_scorers) == 2  # Alice and Charlie

    def test_filter_returns_empty(self, sample_table):
        """Filter that returns no results"""
        result = sample_table.filter(lambda row: row["score"] > 100)
        assert len(result) == 0

    def test_filter_with_null_handling(self, sample_table):
        """Filter handling null values properly"""
        with_age = sample_table.filter(lambda row: row.get("age") is not None)
        assert len(with_age) == 3  # Everyone except Diana


class TestProjectionView:
    """Test projection view (column selection)"""

    @pytest.fixture
    def sample_table(self):
        """Create a table with multiple columns"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("email", livetable.ColumnType.STRING, False),
            ("age", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("users", schema)
        table.append_row({"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30})
        table.append_row({"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25})
        return table

    def test_select_subset_of_columns(self, sample_table):
        """Select a subset of columns"""
        public_view = sample_table.select(["id", "name"])
        assert public_view.column_names() == ["id", "name"]
        assert len(public_view) == 2

        row = public_view.get_row(0)
        assert "id" in row
        assert "name" in row
        assert "email" not in row
        assert "age" not in row

    def test_select_single_column(self, sample_table):
        """Select a single column"""
        names_only = sample_table.select(["name"])
        assert names_only.column_names() == ["name"]
        assert names_only.get_value(0, "name") == "Alice"


class TestComputedView:
    """Test computed view (dynamic columns)"""

    @pytest.fixture
    def sample_table(self):
        """Create a table for computed columns"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("students", schema)
        table.append_row({"id": 1, "name": "Alice", "score": 95.5})
        table.append_row({"id": 2, "name": "Bob", "score": 87.3})
        table.append_row({"id": 3, "name": "Charlie", "score": 92.1})
        return table

    def test_add_computed_grade(self, sample_table):
        """Add a computed grade column"""
        with_grade = sample_table.add_computed_column(
            "grade",
            lambda row: "A" if row["score"] >= 90 else "B"
        )

        assert "grade" in with_grade.column_names()
        assert with_grade.get_value(0, "grade") == "A"  # Alice
        assert with_grade.get_value(1, "grade") == "B"  # Bob
        assert with_grade.get_value(2, "grade") == "A"  # Charlie

    def test_computed_column_from_multiple_fields(self, sample_table):
        """Compute a column using multiple source fields"""
        with_summary = sample_table.add_computed_column(
            "summary",
            lambda row: f"{row['name']}: {row['score']}"
        )

        assert with_summary.get_value(0, "summary") == "Alice: 95.5"

    def test_computed_column_numeric(self, sample_table):
        """Compute a numeric column"""
        with_bonus = sample_table.add_computed_column(
            "bonus",
            lambda row: row["score"] * 0.1
        )

        bonus = with_bonus.get_value(0, "bonus")
        assert abs(bonus - 9.55) < 0.01  # Alice's bonus


class TestJoinView:
    """Test join operations"""

    @pytest.fixture
    def users_table(self):
        """Create a users table"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("users", schema)
        table.append_row({"id": 1, "name": "Alice"})
        table.append_row({"id": 2, "name": "Bob"})
        table.append_row({"id": 3, "name": "Charlie"})
        return table

    @pytest.fixture
    def orders_table(self):
        """Create an orders table"""
        schema = livetable.Schema([
            ("order_id", livetable.ColumnType.INT32, False),
            ("user_id", livetable.ColumnType.INT32, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("orders", schema)
        table.append_row({"order_id": 101, "user_id": 1, "amount": 99.99})
        table.append_row({"order_id": 102, "user_id": 1, "amount": 49.50})
        table.append_row({"order_id": 103, "user_id": 2, "amount": 199.00})
        # Note: user_id 3 (Charlie) has no orders
        return table

    def test_left_join(self, users_table, orders_table):
        """Test LEFT join - includes all users even without orders"""
        joined = livetable.JoinView(
            "user_orders",
            users_table,
            orders_table,
            "id",
            "user_id",
            livetable.JoinType.LEFT
        )

        # Should include all users (3) + their orders
        # Alice: 2 orders, Bob: 1 order, Charlie: 1 row with None
        assert len(joined) == 4

        # Check that Charlie appears with None values
        charlie_row = None
        for i in range(len(joined)):
            row = joined.get_row(i)
            if row.get("name") == "Charlie":
                charlie_row = row
                break

        assert charlie_row is not None
        assert charlie_row["right_order_id"] is None
        assert charlie_row["right_amount"] is None

    def test_inner_join(self, users_table, orders_table):
        """Test INNER join - only users with orders"""
        joined = livetable.JoinView(
            "user_orders",
            users_table,
            orders_table,
            "id",
            "user_id",
            livetable.JoinType.INNER
        )

        # Should only include users with orders
        # Alice: 2 orders, Bob: 1 order = 3 total rows
        assert len(joined) == 3

        # Verify Charlie is not in results
        for i in range(len(joined)):
            row = joined.get_row(i)
            assert row.get("name") != "Charlie"

    def test_join_column_access(self, users_table, orders_table):
        """Test accessing columns from both tables in join"""
        joined = livetable.JoinView(
            "user_orders",
            users_table,
            orders_table,
            "id",
            "user_id",
            livetable.JoinType.INNER
        )

        row = joined.get_row(0)  # Alice's first order

        # Can access columns from both tables
        assert "name" in row  # from users table
        assert "right_order_id" in row  # from orders table (prefixed with "right_")
        assert "right_amount" in row  # from orders table (prefixed with "right_")

        assert row["name"] == "Alice"
        assert row["right_amount"] in [99.99, 49.50]


# Note: View chaining (calling .select() on a FilterView) is not currently supported
# Views must be created from the base Table object


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
