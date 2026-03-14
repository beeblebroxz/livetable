"""Tests for RIGHT and FULL OUTER joins."""
import pytest
import livetable


def make_users_orders():
    """Create standard test tables.

    Users: Alice(1), Bob(2), Charlie(3)
    Orders: order 101 for Alice(1), order 102 for Dave(4)
    """
    schema_users = livetable.Schema([
        ("user_id", livetable.ColumnType.INT32, False),
        ("name", livetable.ColumnType.STRING, False),
    ])
    schema_orders = livetable.Schema([
        ("order_id", livetable.ColumnType.INT32, False),
        ("user_id", livetable.ColumnType.INT32, False),
        ("amount", livetable.ColumnType.FLOAT64, False),
    ])
    users = livetable.Table("users", schema_users)
    users.append_row({"user_id": 1, "name": "Alice"})
    users.append_row({"user_id": 2, "name": "Bob"})
    users.append_row({"user_id": 3, "name": "Charlie"})

    orders = livetable.Table("orders", schema_orders)
    orders.append_row({"order_id": 101, "user_id": 1, "amount": 99.99})
    orders.append_row({"order_id": 102, "user_id": 4, "amount": 49.99})

    return users, orders


class TestRightJoin:
    """Tests for RIGHT JOIN functionality."""

    def test_right_join_basic(self):
        """RIGHT join keeps all right rows; unmatched left columns are NULL."""
        users, orders = make_users_orders()
        joined = users.join(
            orders, left_on="user_id", right_on="user_id", how="right"
        )
        # Right table has 2 rows: order 101 (user_id=1, matched to Alice)
        # and order 102 (user_id=4, no matching user -> NULL name)
        assert len(joined) == 2

        rows = [joined[i] for i in range(len(joined))]
        # Find the matched row (Alice)
        alice_rows = [r for r in rows if r["name"] == "Alice"]
        assert len(alice_rows) == 1
        assert alice_rows[0]["right_order_id"] == 101
        assert abs(alice_rows[0]["right_amount"] - 99.99) < 0.01

        # Find the unmatched row (Dave's order, no user match)
        null_rows = [r for r in rows if r["name"] is None]
        assert len(null_rows) == 1
        assert null_rows[0]["right_order_id"] == 102
        assert abs(null_rows[0]["right_amount"] - 49.99) < 0.01

    def test_right_join_constructor(self):
        """JoinView constructor with JoinType.RIGHT produces correct length."""
        users, orders = make_users_orders()
        joined = livetable.JoinView(
            "right_join",
            users,
            orders,
            "user_id",
            "user_id",
            livetable.JoinType.RIGHT,
        )
        assert len(joined) == 2

    def test_right_join_multi_column(self):
        """RIGHT join on composite key."""
        schema_a = livetable.Schema([
            ("year", livetable.ColumnType.INT32, False),
            ("month", livetable.ColumnType.INT32, False),
            ("sales", livetable.ColumnType.FLOAT64, False),
        ])
        schema_b = livetable.Schema([
            ("year", livetable.ColumnType.INT32, False),
            ("month", livetable.ColumnType.INT32, False),
            ("target", livetable.ColumnType.FLOAT64, False),
        ])

        actual = livetable.Table("actual", schema_a)
        actual.append_row({"year": 2024, "month": 1, "sales": 1000.0})

        targets = livetable.Table("targets", schema_b)
        targets.append_row({"year": 2024, "month": 1, "target": 900.0})
        targets.append_row({"year": 2024, "month": 2, "target": 1200.0})

        joined = actual.join(targets, on=["year", "month"], how="right")
        # Right has 2 rows; only (2024,1) matches left
        assert len(joined) == 2

        rows = [joined[i] for i in range(len(joined))]
        matched = [r for r in rows if r["sales"] is not None]
        unmatched = [r for r in rows if r["sales"] is None]
        assert len(matched) == 1
        assert matched[0]["right_target"] == 900.0
        assert len(unmatched) == 1
        assert unmatched[0]["right_target"] == 1200.0


class TestFullJoin:
    """Tests for FULL OUTER JOIN functionality."""

    def test_full_join_basic(self):
        """FULL join keeps all rows from both sides."""
        users, orders = make_users_orders()
        joined = users.join(
            orders, left_on="user_id", right_on="user_id", how="full"
        )
        # Alice matched (1 row), Bob unmatched left (1), Charlie unmatched left (1),
        # Dave's order unmatched right (1) = 4 rows total
        assert len(joined) == 4

        rows = [joined[i] for i in range(len(joined))]
        names = [r["name"] for r in rows]
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" in names
        # Dave's order has NULL name
        assert None in names

    def test_full_join_outer_alias(self):
        """how='outer' is an alias for FULL join."""
        users, orders = make_users_orders()
        joined = users.join(
            orders, left_on="user_id", right_on="user_id", how="outer"
        )
        assert len(joined) == 4

    def test_full_join_full_outer_alias(self):
        """how='full_outer' is an alias for FULL join."""
        users, orders = make_users_orders()
        joined = users.join(
            orders, left_on="user_id", right_on="user_id", how="full_outer"
        )
        assert len(joined) == 4

    def test_full_join_constructor(self):
        """JoinView constructor with JoinType.FULL produces correct length."""
        users, orders = make_users_orders()
        joined = livetable.JoinView(
            "full_join",
            users,
            orders,
            "user_id",
            "user_id",
            livetable.JoinType.FULL,
        )
        assert len(joined) == 4

    def test_full_join_null_keys(self):
        """FULL join where both tables have NULL key rows."""
        schema = livetable.Schema([
            ("key", livetable.ColumnType.INT32, True),
            ("val", livetable.ColumnType.STRING, False),
        ])
        left = livetable.Table("left", schema)
        left.append_row({"key": 1, "val": "L1"})
        left.append_row({"key": None, "val": "L_null"})

        right = livetable.Table("right", schema)
        right.append_row({"key": 1, "val": "R1"})
        right.append_row({"key": None, "val": "R_null"})

        joined = left.join(right, on="key", how="full")
        # key=1 matches (1 row), left NULL key unmatched (1), right NULL key unmatched (1)
        # NULL != NULL in join semantics, so 3 rows
        assert len(joined) == 3


class TestTickPropagation:
    """Tests for tick() propagation to JoinViews."""

    def test_join_tick_propagation(self):
        """tick() on left table propagates changes to FULL join view."""
        users, orders = make_users_orders()
        joined = users.join(
            orders, left_on="user_id", right_on="user_id", how="full"
        )
        assert len(joined) == 4

        # Add a new user that matches Dave's order (user_id=4)
        users.append_row({"user_id": 4, "name": "Dave"})
        users.tick()

        # Now Dave's order should match, total still 4:
        # Alice(1), Bob(unmatched), Charlie(unmatched), Dave(4) matched
        assert len(joined) == 4
        rows = [joined[i] for i in range(len(joined))]
        names = [r["name"] for r in rows]
        assert "Dave" in names
        # No more NULL names (all right rows now matched)
        assert None not in names

    def test_join_registered_view_count(self):
        """Both parent tables register the join view."""
        users, orders = make_users_orders()
        _joined = users.join(
            orders, left_on="user_id", right_on="user_id", how="full"
        )
        assert users.registered_view_count() >= 1
        assert orders.registered_view_count() >= 1


class TestSyncExposed:
    """Tests for JoinView.sync() method."""

    def test_join_sync_exposed(self):
        """sync() returns False when no changes, True after appending."""
        users, orders = make_users_orders()
        joined = livetable.JoinView(
            "sync_test",
            users,
            orders,
            "user_id",
            "user_id",
            livetable.JoinType.FULL,
        )
        # Initial sync should return False (no pending changes)
        assert joined.sync() is False

        # Add a row and sync
        users.append_row({"user_id": 5, "name": "Eve"})
        result = joined.sync()
        assert result is True


class TestIterationAndSlicing:
    """Tests for iteration and indexing on RIGHT/FULL join views."""

    def test_right_full_iteration(self):
        """for row in joined works for FULL join."""
        users, orders = make_users_orders()
        joined = users.join(
            orders, left_on="user_id", right_on="user_id", how="full"
        )
        count = 0
        for row in joined:
            assert isinstance(row, dict)
            count += 1
        assert count == 4

    def test_right_full_slicing(self):
        """Indexing and slicing work on FULL join views."""
        users, orders = make_users_orders()
        joined = users.join(
            orders, left_on="user_id", right_on="user_id", how="full"
        )
        # Positive index
        row0 = joined[0]
        assert isinstance(row0, dict)

        # Negative index
        row_last = joined[-1]
        assert isinstance(row_last, dict)

        # Slicing
        sliced = joined[1:3]
        assert len(sliced) == 2
        assert all(isinstance(r, dict) for r in sliced)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
