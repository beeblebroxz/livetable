"""
Tests for the tick() method and automatic view propagation.
"""
import pytest
import livetable


@pytest.fixture
def sales_table():
    """Create a sample sales table for testing."""
    schema = livetable.Schema([
        ("region", livetable.ColumnType.STRING, False),
        ("product", livetable.ColumnType.STRING, False),
        ("amount", livetable.ColumnType.INT32, False),
    ])
    table = livetable.Table("sales", schema)
    table.append_row({"region": "West", "product": "Widget", "amount": 250})
    table.append_row({"region": "East", "product": "Gadget", "amount": 1200})
    table.append_row({"region": "West", "product": "Premium", "amount": 800})
    table.append_row({"region": "North", "product": "Basic", "amount": 150})
    return table


class TestTickBasics:
    """Test basic tick() functionality."""

    def test_tick_returns_synced_count(self, sales_table):
        """tick() should return the number of views synced."""
        # No views registered yet
        count = sales_table.tick()
        assert count == 0

    def test_registered_view_count(self, sales_table):
        """registered_view_count() should return number of registered views."""
        assert sales_table.registered_view_count() == 0

        # Create a filter view
        _ = sales_table.filter(lambda row: row["amount"] > 500)
        assert sales_table.registered_view_count() == 1

        # Create a sorted view
        _ = sales_table.sort("amount")
        assert sales_table.registered_view_count() == 2

        # Create an aggregate view
        _ = sales_table.group_by("region", agg=[("total", "amount", "sum")])
        assert sales_table.registered_view_count() == 3


class TestFilterViewWithTick:
    """Test tick() with filter views."""

    def test_filter_view_auto_syncs_on_tick(self, sales_table):
        """Filter views should update when tick() is called."""
        # Create filter for high-value sales
        filtered = sales_table.filter(lambda row: row["amount"] > 500)
        assert len(filtered) == 2  # East (1200) and West Premium (800)

        # Add a new high-value sale
        sales_table.append_row({"region": "South", "product": "Deluxe", "amount": 1500})

        # Before tick, filter is stale
        assert len(filtered) == 2

        # After tick, filter should be updated
        count = sales_table.tick()
        assert count == 1
        assert len(filtered) == 3

        # Verify the new row is included
        rows = [filtered[i] for i in range(len(filtered))]
        amounts = [row["amount"] for row in rows]
        assert 1500 in amounts

    def test_filter_excludes_non_matching_rows(self, sales_table):
        """Filter should exclude rows that don't match after tick."""
        filtered = sales_table.filter(lambda row: row["amount"] > 500)
        assert len(filtered) == 2

        # Add a low-value sale (should be excluded)
        sales_table.append_row({"region": "South", "product": "Basic", "amount": 100})
        sales_table.tick()

        # Filter should still have only 2 rows
        assert len(filtered) == 2


class TestSortedViewWithTick:
    """Test tick() with sorted views."""

    def test_sorted_view_auto_syncs_on_tick(self, sales_table):
        """Sorted views should update when tick() is called."""
        # Create sorted view by amount descending
        sorted_view = sales_table.sort("amount", descending=True)

        # Initial order
        assert sorted_view[0]["amount"] == 1200  # East Gadget
        assert sorted_view[1]["amount"] == 800   # West Premium

        # Add a new top seller
        sales_table.append_row({"region": "West", "product": "Ultra", "amount": 2000})

        # After tick, sorted view should be updated
        sales_table.tick()

        # New top seller should be first
        assert sorted_view[0]["amount"] == 2000
        assert sorted_view[0]["product"] == "Ultra"


class TestAggregateViewWithTick:
    """Test tick() with aggregate views."""

    def test_aggregate_view_auto_syncs_on_tick(self, sales_table):
        """Aggregate views should update when tick() is called."""
        # Create aggregate view by region
        agg = sales_table.group_by("region", agg=[
            ("total", "amount", "sum"),
            ("count", "amount", "count"),
        ])

        # Initial aggregation
        initial_len = len(agg)
        west_total = None
        for i in range(len(agg)):
            row = agg[i]
            if row["region"] == "West":
                west_total = row["total"]
                break
        assert west_total == 1050  # 250 + 800

        # Add another West sale
        sales_table.append_row({"region": "West", "product": "Extra", "amount": 500})

        # After tick, aggregate should be updated
        sales_table.tick()

        # Find West total again
        new_west_total = None
        for i in range(len(agg)):
            row = agg[i]
            if row["region"] == "West":
                new_west_total = row["total"]
                break
        assert new_west_total == 1550  # 250 + 800 + 500

    def test_aggregate_new_group_appears_on_tick(self, sales_table):
        """New groups should appear in aggregate view after tick()."""
        agg = sales_table.group_by("region", agg=[("total", "amount", "sum")])

        initial_regions = {agg[i]["region"] for i in range(len(agg))}
        assert "South" not in initial_regions

        # Add a sale from a new region
        sales_table.append_row({"region": "South", "product": "Deluxe", "amount": 1500})
        sales_table.tick()

        # South should now appear
        new_regions = {agg[i]["region"] for i in range(len(agg))}
        assert "South" in new_regions


class TestMultipleViewsWithTick:
    """Test tick() with multiple views."""

    def test_tick_updates_all_views(self, sales_table):
        """tick() should update all registered views at once."""
        # Create multiple views
        filtered = sales_table.filter(lambda row: row["amount"] > 500)
        sorted_view = sales_table.sort("amount", descending=True)
        agg = sales_table.group_by("region", agg=[("total", "amount", "sum")])

        assert sales_table.registered_view_count() == 3

        # Add a new high-value sale
        sales_table.append_row({"region": "South", "product": "Mega", "amount": 3000})

        # Single tick should update all
        count = sales_table.tick()
        assert count == 3

        # Verify filter updated
        assert len(filtered) == 3

        # Verify sorted updated
        assert sorted_view[0]["amount"] == 3000

        # Verify aggregate updated
        south_total = None
        for i in range(len(agg)):
            if agg[i]["region"] == "South":
                south_total = agg[i]["total"]
                break
        assert south_total == 3000


class TestTickIdempotence:
    """Test that tick() is idempotent."""

    def test_double_tick_no_op(self, sales_table):
        """Calling tick() twice without changes should be safe."""
        filtered = sales_table.filter(lambda row: row["amount"] > 500)
        sales_table.append_row({"region": "South", "product": "Deluxe", "amount": 1500})

        # First tick
        count1 = sales_table.tick()
        len1 = len(filtered)

        # Second tick (should sync again but no actual change)
        count2 = sales_table.tick()
        len2 = len(filtered)

        # Results should be the same
        assert len1 == len2 == 3


class TestViewCreationMethods:
    """Test that different view creation methods work correctly."""

    def test_filter_creates_registered_view(self, sales_table):
        """filter() should create a registered view."""
        assert sales_table.registered_view_count() == 0
        _ = sales_table.filter(lambda row: row["amount"] > 500)
        assert sales_table.registered_view_count() == 1

    def test_sort_creates_registered_view(self, sales_table):
        """sort() should create a registered view."""
        assert sales_table.registered_view_count() == 0
        _ = sales_table.sort("amount")
        assert sales_table.registered_view_count() == 1

    def test_group_by_creates_registered_view(self, sales_table):
        """group_by() should create a registered view."""
        assert sales_table.registered_view_count() == 0
        _ = sales_table.group_by("region", agg=[("total", "amount", "sum")])
        assert sales_table.registered_view_count() == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
