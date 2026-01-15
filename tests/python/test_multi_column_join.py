"""
Tests for multi-column joins.

This module tests JoinView with multiple join keys (composite keys).
"""

import pytest
import livetable


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def sales_schema():
    """Schema for sales table with composite key (year, month, region)."""
    return livetable.Schema([
        ("year", livetable.ColumnType.INT32, False),
        ("month", livetable.ColumnType.INT32, False),
        ("region", livetable.ColumnType.STRING, False),
        ("sales_amount", livetable.ColumnType.FLOAT64, False),
    ])


@pytest.fixture
def targets_schema():
    """Schema for targets table with composite key."""
    return livetable.Schema([
        ("target_year", livetable.ColumnType.INT32, False),
        ("target_month", livetable.ColumnType.INT32, False),
        ("target_region", livetable.ColumnType.STRING, False),
        ("target_amount", livetable.ColumnType.FLOAT64, False),
    ])


@pytest.fixture
def sales_table(sales_schema):
    """Create sales table with test data."""
    table = livetable.Table("sales", sales_schema)
    table.append_rows([
        {"year": 2024, "month": 1, "region": "North", "sales_amount": 1000.0},
        {"year": 2024, "month": 1, "region": "South", "sales_amount": 1500.0},
        {"year": 2024, "month": 2, "region": "North", "sales_amount": 1100.0},
        {"year": 2024, "month": 2, "region": "South", "sales_amount": 1600.0},
    ])
    return table


@pytest.fixture
def targets_table(targets_schema):
    """Create targets table with test data."""
    table = livetable.Table("targets", targets_schema)
    table.append_rows([
        {"target_year": 2024, "target_month": 1, "target_region": "North", "target_amount": 900.0},
        {"target_year": 2024, "target_month": 1, "target_region": "South", "target_amount": 1400.0},
        {"target_year": 2024, "target_month": 2, "target_region": "North", "target_amount": 1000.0},
        # No target for South in month 2 - for testing left join
    ])
    return table


# ============================================================================
# Basic Multi-Column Join Tests
# ============================================================================

class TestMultiColumnJoinBasic:
    """Tests for basic multi-column join functionality."""

    def test_multi_column_inner_join(self, sales_table, targets_table):
        """Inner join on multiple columns should match composite keys."""
        joined = livetable.JoinView(
            "sales_vs_targets",
            sales_table,
            targets_table,
            ["year", "month", "region"],
            ["target_year", "target_month", "target_region"],
            livetable.JoinType.INNER,
        )

        # Should only match 3 rows (North Jan, South Jan, North Feb)
        assert len(joined) == 3

        # Verify first row data
        row0 = joined.get_row(0)
        assert row0["year"] == 2024
        assert row0["month"] == 1
        assert row0["region"] == "North"
        assert row0["sales_amount"] == 1000.0
        assert row0["right_target_amount"] == 900.0

    def test_multi_column_left_join(self, sales_table, targets_table):
        """Left join should include unmatched rows with NULL right values."""
        joined = livetable.JoinView(
            "sales_vs_targets",
            sales_table,
            targets_table,
            ["year", "month", "region"],
            ["target_year", "target_month", "target_region"],
            livetable.JoinType.LEFT,
        )

        # Should include all 4 sales rows
        assert len(joined) == 4

        # Find the unmatched row (South Feb)
        unmatched = None
        for i in range(len(joined)):
            row = joined.get_row(i)
            if row["month"] == 2 and row["region"] == "South":
                unmatched = row
                break

        assert unmatched is not None
        assert unmatched["right_target_amount"] is None

    def test_two_column_join(self):
        """Test joining on exactly two columns."""
        # Create orders table
        orders_schema = livetable.Schema([
            ("customer_id", livetable.ColumnType.INT32, False),
            ("order_date", livetable.ColumnType.STRING, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        orders = livetable.Table("orders", orders_schema)
        orders.append_rows([
            {"customer_id": 1, "order_date": "2024-01-15", "amount": 100.0},
            {"customer_id": 1, "order_date": "2024-01-20", "amount": 150.0},
            {"customer_id": 2, "order_date": "2024-01-15", "amount": 200.0},
        ])

        # Create discounts table
        discounts_schema = livetable.Schema([
            ("cust_id", livetable.ColumnType.INT32, False),
            ("discount_date", livetable.ColumnType.STRING, False),
            ("discount_pct", livetable.ColumnType.FLOAT64, False),
        ])
        discounts = livetable.Table("discounts", discounts_schema)
        discounts.append_rows([
            {"cust_id": 1, "discount_date": "2024-01-15", "discount_pct": 10.0},
            {"cust_id": 2, "discount_date": "2024-01-15", "discount_pct": 15.0},
        ])

        # Join on customer_id and date
        joined = livetable.JoinView(
            "orders_with_discounts",
            orders,
            discounts,
            ["customer_id", "order_date"],
            ["cust_id", "discount_date"],
            livetable.JoinType.INNER,
        )

        # Only 2 orders have matching discounts
        assert len(joined) == 2


# ============================================================================
# Backward Compatibility Tests
# ============================================================================

class TestSingleColumnJoinBackwardCompat:
    """Tests to ensure single-column joins still work."""

    def test_single_column_string_key(self):
        """Single string key should work (backward compatible)."""
        left_schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", left_schema)
        left.append_row({"id": 1, "name": "Alice"})
        left.append_row({"id": 2, "name": "Bob"})

        right_schema = livetable.Schema([
            ("ref_id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.FLOAT64, False),
        ])
        right = livetable.Table("right", right_schema)
        right.append_row({"ref_id": 1, "value": 100.0})
        right.append_row({"ref_id": 2, "value": 200.0})

        # Using single string keys (backward compatible)
        joined = livetable.JoinView(
            "joined",
            left,
            right,
            "id",        # Single string, not list
            "ref_id",    # Single string, not list
            livetable.JoinType.INNER,
        )

        assert len(joined) == 2
        assert joined.get_row(0)["name"] == "Alice"
        assert joined.get_row(0)["right_value"] == 100.0

    def test_single_column_list_key(self):
        """Single-item list should work the same as string."""
        left_schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", left_schema)
        left.append_row({"id": 1, "name": "Alice"})

        right_schema = livetable.Schema([
            ("ref_id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.FLOAT64, False),
        ])
        right = livetable.Table("right", right_schema)
        right.append_row({"ref_id": 1, "value": 100.0})

        # Using single-item lists
        joined = livetable.JoinView(
            "joined",
            left,
            right,
            ["id"],        # List with one item
            ["ref_id"],    # List with one item
            livetable.JoinType.INNER,
        )

        assert len(joined) == 1


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestMultiColumnJoinErrors:
    """Tests for error handling in multi-column joins."""

    def test_key_count_mismatch(self):
        """Should error if left and right key counts don't match."""
        left_schema = livetable.Schema([
            ("a", livetable.ColumnType.INT32, False),
            ("b", livetable.ColumnType.INT32, False),
        ])
        left = livetable.Table("left", left_schema)

        right_schema = livetable.Schema([
            ("x", livetable.ColumnType.INT32, False),
        ])
        right = livetable.Table("right", right_schema)

        with pytest.raises(ValueError, match="mismatch"):
            livetable.JoinView(
                "joined",
                left,
                right,
                ["a", "b"],  # Two keys
                ["x"],       # One key
                livetable.JoinType.INNER,
            )

    def test_missing_left_column(self):
        """Should error if left key column doesn't exist."""
        left_schema = livetable.Schema([
            ("a", livetable.ColumnType.INT32, False),
        ])
        left = livetable.Table("left", left_schema)

        right_schema = livetable.Schema([
            ("x", livetable.ColumnType.INT32, False),
        ])
        right = livetable.Table("right", right_schema)

        with pytest.raises(ValueError, match="Left table missing column"):
            livetable.JoinView(
                "joined",
                left,
                right,
                ["nonexistent"],
                ["x"],
                livetable.JoinType.INNER,
            )

    def test_missing_right_column(self):
        """Should error if right key column doesn't exist."""
        left_schema = livetable.Schema([
            ("a", livetable.ColumnType.INT32, False),
        ])
        left = livetable.Table("left", left_schema)

        right_schema = livetable.Schema([
            ("x", livetable.ColumnType.INT32, False),
        ])
        right = livetable.Table("right", right_schema)

        with pytest.raises(ValueError, match="Right table missing column"):
            livetable.JoinView(
                "joined",
                left,
                right,
                ["a"],
                ["nonexistent"],
                livetable.JoinType.INNER,
            )

    def test_empty_keys_error(self):
        """Should error if no keys provided."""
        left_schema = livetable.Schema([
            ("a", livetable.ColumnType.INT32, False),
        ])
        left = livetable.Table("left", left_schema)

        right_schema = livetable.Schema([
            ("x", livetable.ColumnType.INT32, False),
        ])
        right = livetable.Table("right", right_schema)

        with pytest.raises(ValueError, match="At least one"):
            livetable.JoinView(
                "joined",
                left,
                right,
                [],  # Empty keys
                [],
                livetable.JoinType.INNER,
            )


# ============================================================================
# Integration Tests
# ============================================================================

class TestMultiColumnJoinIntegration:
    """Integration tests for multi-column joins."""

    def test_iteration_over_joined_rows(self, sales_table, targets_table):
        """Iteration should work over multi-column joined view."""
        joined = livetable.JoinView(
            "sales_vs_targets",
            sales_table,
            targets_table,
            ["year", "month", "region"],
            ["target_year", "target_month", "target_region"],
            livetable.JoinType.INNER,
        )

        regions = [row["region"] for row in joined]
        assert "North" in regions
        assert "South" in regions

    def test_join_with_null_values_in_key(self):
        """Rows with NULL in join key should not match."""
        left_schema = livetable.Schema([
            ("a", livetable.ColumnType.INT32, True),  # Nullable
            ("b", livetable.ColumnType.INT32, False),
            ("val", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", left_schema)
        left.append_row({"a": 1, "b": 2, "val": "match"})
        left.append_row({"a": None, "b": 2, "val": "no_match"})  # NULL in key

        right_schema = livetable.Schema([
            ("x", livetable.ColumnType.INT32, True),
            ("y", livetable.ColumnType.INT32, False),
            ("data", livetable.ColumnType.STRING, False),
        ])
        right = livetable.Table("right", right_schema)
        right.append_row({"x": 1, "y": 2, "data": "found"})
        right.append_row({"x": None, "y": 2, "data": "not_found"})

        joined = livetable.JoinView(
            "joined",
            left,
            right,
            ["a", "b"],
            ["x", "y"],
            livetable.JoinType.INNER,
        )

        # Only one row should match (1, 2) - rows with NULL shouldn't match
        assert len(joined) == 1
        assert joined.get_row(0)["val"] == "match"

    def test_many_to_many_multi_column(self):
        """Multi-column join should handle many-to-many relationships."""
        left_schema = livetable.Schema([
            ("a", livetable.ColumnType.INT32, False),
            ("b", livetable.ColumnType.INT32, False),
            ("left_val", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", left_schema)
        left.append_rows([
            {"a": 1, "b": 2, "left_val": "L1"},
            {"a": 1, "b": 2, "left_val": "L2"},  # Same key as L1
        ])

        right_schema = livetable.Schema([
            ("x", livetable.ColumnType.INT32, False),
            ("y", livetable.ColumnType.INT32, False),
            ("right_val", livetable.ColumnType.STRING, False),
        ])
        right = livetable.Table("right", right_schema)
        right.append_rows([
            {"x": 1, "y": 2, "right_val": "R1"},
            {"x": 1, "y": 2, "right_val": "R2"},  # Same key as R1
        ])

        joined = livetable.JoinView(
            "joined",
            left,
            right,
            ["a", "b"],
            ["x", "y"],
            livetable.JoinType.INNER,
        )

        # Should produce 2x2 = 4 rows (cartesian product for matching keys)
        assert len(joined) == 4

        # Collect all combinations
        combinations = set()
        for i in range(len(joined)):
            row = joined.get_row(i)
            combinations.add((row["left_val"], row["right_right_val"]))

        expected = {("L1", "R1"), ("L1", "R2"), ("L2", "R1"), ("L2", "R2")}
        assert combinations == expected
