"""Tests for view-over-view composition (ReadableTable DAG)."""

import livetable
import pytest


def make_sales():
    schema = livetable.Schema([
        ("region", livetable.ColumnType.STRING, False),
        ("amount", livetable.ColumnType.FLOAT64, False),
    ])
    table = livetable.Table("sales", schema)
    table.append_rows([
        {"region": "N", "amount": 50.0},
        {"region": "S", "amount": 150.0},
        {"region": "N", "amount": 300.0},
        {"region": "S", "amount": 80.0},
        {"region": "N", "amount": 120.0},
    ])
    return table


class TestFilterChaining:
    def test_filter_then_sort(self):
        table = make_sales()
        big = table.filter(lambda row: row["amount"] >= 100)
        by_amount = big.sort("amount", descending=True)

        amounts = [row["amount"] for row in by_amount]
        assert amounts == [300.0, 150.0, 120.0]

    def test_filter_then_group_by(self):
        table = make_sales()
        big = table.filter(lambda row: row["amount"] >= 100)
        grouped = big.group_by("region", agg=[("total", "amount", "sum")])

        totals = {row["region"]: row["total"] for row in grouped}
        assert totals == {"N": 420.0, "S": 150.0}

    def test_chained_views_update_on_tick(self):
        table = make_sales()
        big = table.filter(lambda row: row["amount"] >= 100)
        by_amount = big.sort("amount", descending=True)
        grouped = big.group_by("region", agg=[("total", "amount", "sum")])

        table.append_row({"region": "S", "amount": 900.0})
        table.tick()

        amounts = [row["amount"] for row in by_amount]
        assert amounts == [900.0, 300.0, 150.0, 120.0]

        totals = {row["region"]: row["total"] for row in grouped}
        assert totals == {"N": 420.0, "S": 1050.0}

    def test_chained_views_are_registered(self):
        table = make_sales()
        big = table.filter(lambda row: row["amount"] >= 100)
        assert table.registered_view_count() == 1

        sorted_view = big.sort("amount")
        grouped = big.group_by("region", agg=[("cnt", "amount", "count")])
        assert table.registered_view_count() == 3

        # Keep references alive so registration is not pruned.
        assert len(sorted_view) == 3
        assert len(grouped) == 2

    def test_chained_sort_validates_columns(self):
        table = make_sales()
        big = table.filter(lambda row: row["amount"] >= 100)
        with pytest.raises(ValueError, match="not found"):
            big.sort("nonexistent")

    def test_deletion_propagates_through_chain(self):
        table = make_sales()
        big = table.filter(lambda row: row["amount"] >= 100)
        by_amount = big.sort("amount", descending=True)

        # Delete the 300.0 row (index 2 in the root table).
        table.delete_row(2)
        table.tick()

        amounts = [row["amount"] for row in by_amount]
        assert amounts == [150.0, 120.0]
