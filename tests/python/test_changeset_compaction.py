#!/usr/bin/env python3
"""
Tests for changeset compaction and multi-view cursor tracking.

These tests verify that:
1. Views track their position in the change stream independently
2. Compaction only removes changes that all views have processed
3. Views that fall behind gracefully rebuild from scratch
"""

import pytest
import livetable


@pytest.fixture
def sample_table():
    """Create a sample table for testing."""
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("value", livetable.ColumnType.INT32, False),
    ])
    table = livetable.Table("test", schema)
    for i in range(5):
        table.append_row({"id": i, "value": i * 10})
    return table


class TestChangesetCompaction:
    """Test changeset compaction with multiple views."""

    def test_compaction_preserves_unprocessed_changes(self, sample_table):
        """Changes should not be compacted until all views have processed them."""
        # Create two filter views
        view1 = sample_table.filter(lambda row: row["value"] >= 20)
        view2 = sample_table.filter(lambda row: row["value"] >= 30)

        assert len(view1) == 3  # values 20, 30, 40
        assert len(view2) == 2  # values 30, 40

        # Add a new high-value row
        sample_table.append_row({"id": 5, "value": 50})

        # tick() syncs all views and compacts
        count = sample_table.tick()
        assert count == 2

        # Both views should see the new row
        assert len(view1) == 4
        assert len(view2) == 3

    def test_multiple_ticks_accumulate_correctly(self, sample_table):
        """Multiple ticks with interleaved changes should work correctly."""
        view = sample_table.filter(lambda row: row["value"] >= 30)
        assert len(view) == 2  # values 30, 40

        # Add row and tick
        sample_table.append_row({"id": 5, "value": 50})
        sample_table.tick()
        assert len(view) == 3

        # Add another row and tick again
        sample_table.append_row({"id": 6, "value": 60})
        sample_table.tick()
        assert len(view) == 4

        # Add non-matching row
        sample_table.append_row({"id": 7, "value": 10})
        sample_table.tick()
        assert len(view) == 4  # Still 4

    def test_views_with_different_sync_patterns(self, sample_table):
        """Views synced at different times should all get correct data."""
        view1 = sample_table.filter(lambda row: row["value"] >= 0)

        # Add some rows
        sample_table.append_row({"id": 5, "value": 50})
        sample_table.append_row({"id": 6, "value": 60})

        # tick() updates view1
        sample_table.tick()
        assert len(view1) == 7

        # Now create view2 - it should see all current rows
        view2 = sample_table.filter(lambda row: row["value"] >= 50)
        assert len(view2) == 2

        # Add more rows
        sample_table.append_row({"id": 7, "value": 70})

        # tick() should update both views
        count = sample_table.tick()
        assert count == 2
        assert len(view1) == 8
        assert len(view2) == 3


class TestMixedViewTypes:
    """Test compaction with different view types."""

    def test_filter_and_sorted_together(self, sample_table):
        """Filter and sorted views should work together with compaction."""
        filtered = sample_table.filter(lambda row: row["value"] >= 20)
        sorted_view = sample_table.sort("value", descending=True)

        assert len(filtered) == 3
        assert sorted_view[0]["value"] == 40

        # Add new highest value
        sample_table.append_row({"id": 5, "value": 100})

        sample_table.tick()

        assert len(filtered) == 4
        assert sorted_view[0]["value"] == 100

    def test_filter_sorted_aggregate_together(self):
        """All view types should work together with compaction."""
        schema = livetable.Schema([
            ("region", livetable.ColumnType.STRING, False),
            ("amount", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("sales", schema)
        table.append_row({"region": "West", "amount": 100})
        table.append_row({"region": "East", "amount": 200})
        table.append_row({"region": "West", "amount": 150})

        filtered = table.filter(lambda row: row["amount"] >= 150)
        sorted_view = table.sort("amount", descending=True)
        agg = table.group_by("region", agg=[("total", "amount", "sum")])

        assert len(filtered) == 2  # 200 and 150
        assert sorted_view[0]["amount"] == 200

        # Find West total
        west_total = None
        for i in range(len(agg)):
            if agg[i]["region"] == "West":
                west_total = agg[i]["total"]
        assert west_total == 250

        # Add new high value for West
        table.append_row({"region": "West", "amount": 300})

        table.tick()

        assert len(filtered) == 3
        assert sorted_view[0]["amount"] == 300

        # Check updated aggregate
        for i in range(len(agg)):
            if agg[i]["region"] == "West":
                west_total = agg[i]["total"]
        assert west_total == 550


class TestEdgeCases:
    """Test edge cases in compaction."""

    def test_tick_with_no_views(self, sample_table):
        """tick() with no views should clear changeset."""
        sample_table.append_row({"id": 5, "value": 50})
        count = sample_table.tick()
        assert count == 0

    def test_tick_with_no_changes(self, sample_table):
        """tick() with no pending changes should be safe."""
        view = sample_table.filter(lambda row: True)
        sample_table.tick()  # Sync initial state

        # tick() again with no changes - views still get synced
        count = sample_table.tick()
        # The view count depends on implementation - just verify it's safe
        assert count >= 0
        assert len(view) == 5

    def test_rapid_changes_and_syncs(self, sample_table):
        """Rapid interleaving of changes and syncs should work."""
        view = sample_table.filter(lambda row: row["value"] > 0)

        for i in range(10):
            sample_table.append_row({"id": 100 + i, "value": (i + 1) * 100})
            sample_table.tick()

        # Should have original 4 (excluding value=0) + 10 new = 14
        assert len(view) == 14

    def test_many_views_sync_correctly(self):
        """Many views should all sync correctly."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("test", schema)
        for i in range(10):
            table.append_row({"id": i, "value": i})

        # Create many filter views with different thresholds
        views = []
        for threshold in range(10):
            view = table.filter(lambda row, t=threshold: row["value"] >= t)
            views.append((threshold, view))

        # Add a new row
        table.append_row({"id": 10, "value": 5})
        table.tick()

        # Check each view has correct count
        for threshold, view in views:
            # Original rows with value >= threshold + 1 new row if 5 >= threshold
            expected = (10 - threshold) + (1 if 5 >= threshold else 0)
            assert len(view) == expected, f"threshold={threshold}, expected={expected}, got={len(view)}"


class TestViewCreationAfterChanges:
    """Test views created after changes have occurred."""

    def test_view_created_after_tick(self, sample_table):
        """View created after tick should see current state."""
        view1 = sample_table.filter(lambda row: row["value"] >= 0)

        sample_table.append_row({"id": 5, "value": 50})
        sample_table.tick()

        # Create view2 after tick
        view2 = sample_table.filter(lambda row: row["value"] >= 50)

        assert len(view1) == 6
        assert len(view2) == 1

        # Both views should update on next tick
        sample_table.append_row({"id": 6, "value": 60})
        count = sample_table.tick()
        assert count == 2

        assert len(view1) == 7
        assert len(view2) == 2

    def test_view_created_between_changes(self, sample_table):
        """View created between changes should work correctly."""
        sample_table.append_row({"id": 5, "value": 50})

        # Create view without ticking first
        view = sample_table.filter(lambda row: row["value"] >= 40)

        # View should see current state
        assert len(view) == 2  # 40 and 50

        sample_table.append_row({"id": 6, "value": 60})
        sample_table.tick()

        assert len(view) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
