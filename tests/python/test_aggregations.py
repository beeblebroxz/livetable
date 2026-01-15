#!/usr/bin/env python3
"""
Unit tests for aggregation operations
Tests Table.sum/avg/min/max/count_non_null and AggregateView with GROUP BY
"""

import pytest
import livetable


class TestTableAggregations:
    """Test simple aggregation methods on Table"""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table with numeric data"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("amount", livetable.ColumnType.FLOAT64, True),
        ])
        table = livetable.Table("sales", schema)
        table.append_row({"id": 1, "name": "Alice", "amount": 100.0})
        table.append_row({"id": 2, "name": "Bob", "amount": 200.0})
        table.append_row({"id": 3, "name": "Charlie", "amount": 150.0})
        return table

    @pytest.fixture
    def table_with_nulls(self):
        """Create a table with some null values"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.FLOAT64, True),
        ])
        table = livetable.Table("data", schema)
        table.append_row({"id": 1, "value": 10.0})
        table.append_row({"id": 2, "value": None})
        table.append_row({"id": 3, "value": 30.0})
        table.append_row({"id": 4, "value": None})
        table.append_row({"id": 5, "value": 20.0})
        return table

    @pytest.fixture
    def empty_table(self):
        """Create an empty table"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.FLOAT64, True),
        ])
        return livetable.Table("empty", schema)

    def test_sum_basic(self, sample_table):
        """Test basic sum calculation"""
        total = sample_table.sum("amount")
        assert abs(total - 450.0) < 0.01

    def test_avg_basic(self, sample_table):
        """Test basic average calculation"""
        avg = sample_table.avg("amount")
        assert avg is not None
        assert abs(avg - 150.0) < 0.01

    def test_min_basic(self, sample_table):
        """Test basic min calculation"""
        min_val = sample_table.min("amount")
        assert min_val is not None
        assert abs(min_val - 100.0) < 0.01

    def test_max_basic(self, sample_table):
        """Test basic max calculation"""
        max_val = sample_table.max("amount")
        assert max_val is not None
        assert abs(max_val - 200.0) < 0.01

    def test_count_non_null_all_present(self, sample_table):
        """Test count_non_null when all values are present"""
        count = sample_table.count_non_null("amount")
        assert count == 3

    def test_sum_with_nulls(self, table_with_nulls):
        """Test sum skips null values"""
        total = table_with_nulls.sum("value")
        assert abs(total - 60.0) < 0.01  # 10 + 30 + 20

    def test_avg_with_nulls(self, table_with_nulls):
        """Test avg skips null values"""
        avg = table_with_nulls.avg("value")
        assert avg is not None
        assert abs(avg - 20.0) < 0.01  # (10 + 30 + 20) / 3

    def test_count_non_null_with_nulls(self, table_with_nulls):
        """Test count_non_null excludes nulls"""
        count = table_with_nulls.count_non_null("value")
        assert count == 3  # Only 3 non-null values

    def test_min_with_nulls(self, table_with_nulls):
        """Test min skips null values"""
        min_val = table_with_nulls.min("value")
        assert min_val is not None
        assert abs(min_val - 10.0) < 0.01

    def test_max_with_nulls(self, table_with_nulls):
        """Test max skips null values"""
        max_val = table_with_nulls.max("value")
        assert max_val is not None
        assert abs(max_val - 30.0) < 0.01

    def test_sum_empty_table(self, empty_table):
        """Test sum on empty table returns 0"""
        total = empty_table.sum("value")
        assert total == 0.0

    def test_avg_empty_table(self, empty_table):
        """Test avg on empty table returns None"""
        avg = empty_table.avg("value")
        assert avg is None

    def test_min_empty_table(self, empty_table):
        """Test min on empty table returns None"""
        min_val = empty_table.min("value")
        assert min_val is None

    def test_max_empty_table(self, empty_table):
        """Test max on empty table returns None"""
        max_val = empty_table.max("value")
        assert max_val is None

    def test_count_non_null_empty_table(self, empty_table):
        """Test count_non_null on empty table returns 0"""
        count = empty_table.count_non_null("value")
        assert count == 0

    def test_sum_integer_column(self):
        """Test sum works on integer columns"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("count", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("items", schema)
        table.append_row({"id": 1, "count": 5})
        table.append_row({"id": 2, "count": 10})
        table.append_row({"id": 3, "count": 15})

        total = table.sum("count")
        assert abs(total - 30.0) < 0.01

    def test_aggregation_invalid_column(self, sample_table):
        """Test aggregation on non-existent column raises error"""
        with pytest.raises(Exception):
            sample_table.sum("nonexistent")


class TestAggregateFunction:
    """Test AggregateFunction enum"""

    def test_enum_values_exist(self):
        """Test that all enum values are accessible"""
        assert livetable.AggregateFunction.SUM is not None
        assert livetable.AggregateFunction.COUNT is not None
        assert livetable.AggregateFunction.AVG is not None
        assert livetable.AggregateFunction.MIN is not None
        assert livetable.AggregateFunction.MAX is not None


class TestAggregateView:
    """Test AggregateView with GROUP BY functionality"""

    @pytest.fixture
    def sales_table(self):
        """Create a sales table with regions"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("region", livetable.ColumnType.STRING, False),
            ("product", livetable.ColumnType.STRING, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("sales", schema)
        table.append_row({"id": 1, "region": "North", "product": "Widget", "amount": 100.0})
        table.append_row({"id": 2, "region": "South", "product": "Gadget", "amount": 200.0})
        table.append_row({"id": 3, "region": "North", "product": "Gadget", "amount": 150.0})
        table.append_row({"id": 4, "region": "South", "product": "Widget", "amount": 75.0})
        table.append_row({"id": 5, "region": "North", "product": "Widget", "amount": 50.0})
        return table

    def test_aggregate_view_basic_grouping(self, sales_table):
        """Test basic GROUP BY with SUM"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [("total", "amount", livetable.AggregateFunction.SUM)]
        )

        assert len(agg) == 2  # North and South

        # Find North and South totals
        totals = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            totals[row["region"]] = row["total"]

        assert abs(totals["North"] - 300.0) < 0.01  # 100 + 150 + 50
        assert abs(totals["South"] - 275.0) < 0.01  # 200 + 75

    def test_aggregate_view_count(self, sales_table):
        """Test COUNT aggregation"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [("num_sales", "amount", livetable.AggregateFunction.COUNT)]
        )

        counts = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            counts[row["region"]] = row["num_sales"]

        assert counts["North"] == 3
        assert counts["South"] == 2

    def test_aggregate_view_avg(self, sales_table):
        """Test AVG aggregation"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [("avg_amount", "amount", livetable.AggregateFunction.AVG)]
        )

        avgs = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            avgs[row["region"]] = row["avg_amount"]

        assert abs(avgs["North"] - 100.0) < 0.01  # (100 + 150 + 50) / 3
        assert abs(avgs["South"] - 137.5) < 0.01  # (200 + 75) / 2

    def test_aggregate_view_min_max(self, sales_table):
        """Test MIN and MAX aggregations"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [
                ("min_amount", "amount", livetable.AggregateFunction.MIN),
                ("max_amount", "amount", livetable.AggregateFunction.MAX),
            ]
        )

        for i in range(len(agg)):
            row = agg.get_row(i)
            if row["region"] == "North":
                assert abs(row["min_amount"] - 50.0) < 0.01
                assert abs(row["max_amount"] - 150.0) < 0.01
            elif row["region"] == "South":
                assert abs(row["min_amount"] - 75.0) < 0.01
                assert abs(row["max_amount"] - 200.0) < 0.01

    def test_aggregate_view_multiple_aggregations(self, sales_table):
        """Test multiple aggregations in one view"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [
                ("total", "amount", livetable.AggregateFunction.SUM),
                ("avg_amount", "amount", livetable.AggregateFunction.AVG),
                ("num_sales", "amount", livetable.AggregateFunction.COUNT),
            ]
        )

        for i in range(len(agg)):
            row = agg.get_row(i)
            if row["region"] == "North":
                assert abs(row["total"] - 300.0) < 0.01
                assert abs(row["avg_amount"] - 100.0) < 0.01
                assert row["num_sales"] == 3

    def test_aggregate_view_multiple_group_by_columns(self, sales_table):
        """Test GROUP BY with multiple columns"""
        agg = livetable.AggregateView(
            "by_region_product",
            sales_table,
            ["region", "product"],
            [("total", "amount", livetable.AggregateFunction.SUM)]
        )

        # 4 groups: North-Widget, North-Gadget, South-Widget, South-Gadget
        assert len(agg) == 4

        # Find specific combinations
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            key = (row["region"], row["product"])
            results[key] = row["total"]

        assert abs(results[("North", "Widget")] - 150.0) < 0.01  # 100 + 50
        assert abs(results[("North", "Gadget")] - 150.0) < 0.01
        assert abs(results[("South", "Widget")] - 75.0) < 0.01
        assert abs(results[("South", "Gadget")] - 200.0) < 0.01

    def test_aggregate_view_column_names(self, sales_table):
        """Test column_names returns correct columns"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [
                ("total", "amount", livetable.AggregateFunction.SUM),
                ("count", "amount", livetable.AggregateFunction.COUNT),
            ]
        )

        cols = agg.column_names()
        assert "region" in cols
        assert "total" in cols
        assert "count" in cols

    def test_aggregate_view_get_value(self, sales_table):
        """Test get_value for individual cell access"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [("total", "amount", livetable.AggregateFunction.SUM)]
        )

        # Access by row index and column name
        region = agg.get_value(0, "region")
        total = agg.get_value(0, "total")

        assert region in ["North", "South"]
        assert total is not None


class TestAggregateViewIncremental:
    """Test incremental updates for AggregateView"""

    @pytest.fixture
    def sales_table(self):
        """Create a mutable sales table"""
        schema = livetable.Schema([
            ("region", livetable.ColumnType.STRING, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("sales", schema)
        table.append_row({"region": "North", "amount": 100.0})
        table.append_row({"region": "South", "amount": 200.0})
        return table

    def test_sync_after_insert(self, sales_table):
        """Test sync updates aggregates after insert"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [("total", "amount", livetable.AggregateFunction.SUM)]
        )

        # Initial totals
        initial_totals = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            initial_totals[row["region"]] = row["total"]

        assert abs(initial_totals["North"] - 100.0) < 0.01
        assert abs(initial_totals["South"] - 200.0) < 0.01

        # Add new row
        sales_table.append_row({"region": "North", "amount": 50.0})

        # Sync and check updated totals
        agg.sync()

        updated_totals = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            updated_totals[row["region"]] = row["total"]

        assert abs(updated_totals["North"] - 150.0) < 0.01  # 100 + 50
        assert abs(updated_totals["South"] - 200.0) < 0.01

    def test_sync_creates_new_group(self, sales_table):
        """Test sync creates new group when new key value appears"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [("total", "amount", livetable.AggregateFunction.SUM)]
        )

        assert len(agg) == 2  # North and South

        # Add row with new region
        sales_table.append_row({"region": "East", "amount": 75.0})
        agg.sync()

        assert len(agg) == 3  # Now includes East

        # Find East total
        for i in range(len(agg)):
            row = agg.get_row(i)
            if row["region"] == "East":
                assert abs(row["total"] - 75.0) < 0.01
                break
        else:
            pytest.fail("East region not found")

    def test_refresh_rebuilds_from_scratch(self, sales_table):
        """Test refresh rebuilds entire aggregation"""
        agg = livetable.AggregateView(
            "by_region",
            sales_table,
            ["region"],
            [("total", "amount", livetable.AggregateFunction.SUM)]
        )

        # Add multiple rows
        sales_table.append_row({"region": "North", "amount": 50.0})
        sales_table.append_row({"region": "East", "amount": 100.0})

        # Refresh (full rebuild)
        agg.refresh()

        assert len(agg) == 3

        totals = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            totals[row["region"]] = row["total"]

        assert abs(totals["North"] - 150.0) < 0.01
        assert abs(totals["South"] - 200.0) < 0.01
        assert abs(totals["East"] - 100.0) < 0.01


class TestAggregateViewEdgeCases:
    """Test edge cases for AggregateView"""

    def test_empty_table(self):
        """Test AggregateView on empty table"""
        schema = livetable.Schema([
            ("region", livetable.ColumnType.STRING, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("empty", schema)

        agg = livetable.AggregateView(
            "by_region",
            table,
            ["region"],
            [("total", "amount", livetable.AggregateFunction.SUM)]
        )

        assert len(agg) == 0

    def test_single_row(self):
        """Test AggregateView with single row"""
        schema = livetable.Schema([
            ("region", livetable.ColumnType.STRING, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("single", schema)
        table.append_row({"region": "North", "amount": 100.0})

        agg = livetable.AggregateView(
            "by_region",
            table,
            ["region"],
            [
                ("total", "amount", livetable.AggregateFunction.SUM),
                ("avg_amount", "amount", livetable.AggregateFunction.AVG),
                ("count", "amount", livetable.AggregateFunction.COUNT),
            ]
        )

        assert len(agg) == 1
        row = agg.get_row(0)
        assert row["region"] == "North"
        assert abs(row["total"] - 100.0) < 0.01
        assert abs(row["avg_amount"] - 100.0) < 0.01
        assert row["count"] == 1

    def test_all_same_group(self):
        """Test when all rows belong to same group"""
        schema = livetable.Schema([
            ("region", livetable.ColumnType.STRING, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("same_group", schema)
        table.append_row({"region": "North", "amount": 100.0})
        table.append_row({"region": "North", "amount": 200.0})
        table.append_row({"region": "North", "amount": 300.0})

        agg = livetable.AggregateView(
            "by_region",
            table,
            ["region"],
            [("total", "amount", livetable.AggregateFunction.SUM)]
        )

        assert len(agg) == 1
        row = agg.get_row(0)
        assert row["region"] == "North"
        assert abs(row["total"] - 600.0) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
