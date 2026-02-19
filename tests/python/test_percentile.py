#!/usr/bin/env python3
"""Tests for Percentile and Median aggregation functions."""

import pytest
import livetable


@pytest.fixture
def scores_table():
    """Table with numeric data for percentile testing."""
    schema = livetable.Schema([
        ("group", livetable.ColumnType.STRING, False),
        ("score", livetable.ColumnType.FLOAT64, False),
    ])
    table = livetable.Table("scores", schema)
    # Group A: 10, 20, 30, 40, 50
    for v in [10.0, 20.0, 30.0, 40.0, 50.0]:
        table.append_row({"group": "A", "score": v})
    # Group B: 100, 200
    for v in [100.0, 200.0]:
        table.append_row({"group": "B", "score": v})
    return table


class TestPercentileExplicitAPI:
    """Test Percentile and Median via explicit AggregateFunction constructors."""

    def test_median_enum_exists(self):
        assert livetable.AggregateFunction.MEDIAN is not None

    def test_percentile_constructor(self):
        p95 = livetable.AggregateFunction.PERCENTILE(0.95)
        assert p95 is not None

    def test_percentile_invalid_value(self):
        with pytest.raises(Exception):
            livetable.AggregateFunction.PERCENTILE(1.5)
        with pytest.raises(Exception):
            livetable.AggregateFunction.PERCENTILE(-0.1)

    def test_aggregate_view_median(self, scores_table):
        agg = livetable.AggregateView(
            "by_group", scores_table, ["group"],
            [("median_score", "score", livetable.AggregateFunction.MEDIAN)],
        )
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["median_score"]

        assert abs(results["A"] - 30.0) < 0.01   # median of [10,20,30,40,50]
        assert abs(results["B"] - 150.0) < 0.01   # median of [100,200]

    def test_aggregate_view_percentile(self, scores_table):
        agg = livetable.AggregateView(
            "by_group", scores_table, ["group"],
            [("p25", "score", livetable.AggregateFunction.PERCENTILE(0.25))],
        )
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["p25"]

        assert abs(results["A"] - 20.0) < 0.01  # P25 of [10,20,30,40,50]


class TestPercentileSimplifiedAPI:
    """Test Percentile and Median via group_by string shorthands."""

    def test_median_string(self, scores_table):
        agg = scores_table.group_by("group", agg=[
            ("med", "score", "median"),
        ])
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["med"]
        assert abs(results["A"] - 30.0) < 0.01

    def test_p95_shorthand(self, scores_table):
        agg = scores_table.group_by("group", agg=[
            ("p95_score", "score", "p95"),
        ])
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["p95_score"]
        # P95 of [10,20,30,40,50]: idx=0.95*4=3.8, lerp(40,50,0.8) = 48.0
        assert abs(results["A"] - 48.0) < 0.01

    def test_all_shorthand_names(self, scores_table):
        """All shorthand names parse without error."""
        for name in ["p25", "p50", "p75", "p90", "p95", "p99", "median"]:
            agg = scores_table.group_by("group", agg=[
                ("result", "score", name),
            ])
            assert len(agg) == 2

    def test_percentile_explicit_string(self, scores_table):
        agg = scores_table.group_by("group", agg=[
            ("p10", "score", "percentile(0.1)"),
        ])
        results = {}
        for i in range(len(agg)):
            row = agg.get_row(i)
            results[row["group"]] = row["p10"]
        # P10 of [10,20,30,40,50]: idx=0.1*4=0.4, lerp(10,20,0.4) = 14.0
        assert abs(results["A"] - 14.0) < 0.01

    def test_invalid_percentile_string(self, scores_table):
        with pytest.raises(Exception):
            scores_table.group_by("group", agg=[
                ("bad", "score", "percentile(2.0)"),
            ])


class TestPercentileIncremental:
    """Test incremental updates with percentile aggregations."""

    def test_tick_updates_percentile(self):
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)
        for v in [10.0, 20.0, 30.0]:
            table.append_row({"grp": "A", "val": v})

        agg = table.group_by("grp", agg=[("med", "val", "median")])

        # Median of [10,20,30] = 20.0
        row = agg.get_row(0)
        assert abs(row["med"] - 20.0) < 0.01

        # Add value and tick
        table.append_row({"grp": "A", "val": 40.0})
        table.tick()

        # Median of [10,20,30,40] = 25.0
        row = agg.get_row(0)
        assert abs(row["med"] - 25.0) < 0.01

    def test_sync_updates_percentile(self):
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)
        for v in [10.0, 20.0, 30.0]:
            table.append_row({"grp": "A", "val": v})

        agg = livetable.AggregateView(
            "test", table, ["grp"],
            [("med", "val", livetable.AggregateFunction.MEDIAN)],
        )

        # Add and sync
        table.append_row({"grp": "A", "val": 40.0})
        agg.sync()

        row = agg.get_row(0)
        assert abs(row["med"] - 25.0) < 0.01


class TestPercentileEdgeCases:
    """Edge cases for percentile computations."""

    def test_single_row_group(self):
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)
        table.append_row({"grp": "A", "val": 42.0})

        agg = table.group_by("grp", agg=[("med", "val", "median")])
        row = agg.get_row(0)
        assert abs(row["med"] - 42.0) < 0.01

    def test_empty_table(self):
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)

        agg = table.group_by("grp", agg=[("med", "val", "median")])
        assert len(agg) == 0

    def test_mixed_aggregations(self):
        """Percentile works alongside SUM, AVG, etc."""
        schema = livetable.Schema([
            ("grp", livetable.ColumnType.STRING, False),
            ("val", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("data", schema)
        for v in [10.0, 20.0, 30.0]:
            table.append_row({"grp": "A", "val": v})

        agg = table.group_by("grp", agg=[
            ("total", "val", "sum"),
            ("med", "val", "median"),
            ("p95", "val", "p95"),
            ("avg_val", "val", "avg"),
        ])

        row = agg.get_row(0)
        assert abs(row["total"] - 60.0) < 0.01
        assert abs(row["med"] - 20.0) < 0.01
        assert abs(row["avg_val"] - 20.0) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
