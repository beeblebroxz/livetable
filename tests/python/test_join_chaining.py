"""Python chaining over joins: filter/sort/group_by on a JoinView, kept current
by tick() on either joined table. See
docs/superpowers/specs/2026-09-27-python-join-chaining-design.md."""
import gc

import pytest

import livetable


def make_tables():
    """Orders (oid, cust, amount) and customers (cid, name, tier).

    Order 4's customer (9) does not exist, so LEFT joins keep it unmatched.
    """
    customers = livetable.Table("customers", livetable.Schema([
        ("cid", livetable.ColumnType.INT32, False),
        ("name", livetable.ColumnType.STRING, False),
        ("tier", livetable.ColumnType.INT32, False),
    ]))
    for cid, name, tier in [(1, "Ada", 1), (2, "Bo", 2), (3, "Cy", 1)]:
        customers.append_row({"cid": cid, "name": name, "tier": tier})
    orders = livetable.Table("orders", livetable.Schema([
        ("oid", livetable.ColumnType.INT32, False),
        ("cust", livetable.ColumnType.INT32, False),
        ("amount", livetable.ColumnType.FLOAT64, False),
    ]))
    for oid, cust, amount in [(1, 1, 10.0), (2, 2, 60.0), (3, 1, 75.0), (4, 9, 90.0), (5, 3, 20.0)]:
        orders.append_row({"oid": oid, "cust": cust, "amount": amount})
    return orders, customers


def join(orders, customers, how="left"):
    return orders.join(customers, left_on="cust", right_on="cid", how=how)


def oracle_rows(orders, customers):
    """A from-scratch LEFT join. Explicit joins are not tick-registered."""
    fresh = livetable.JoinView(
        "oracle", orders, customers, "cust", "cid", livetable.JoinType.LEFT
    )
    return list(fresh)


def canon(rows):
    """Order-free comparison: join output order may differ from a rebuild."""
    return sorted(repr(sorted(row.items())) for row in rows)


def big(row):
    return row["amount"] >= 50.0


def tier_one(row):
    return row["right_tier"] == 1


def test_filter_over_join_updates_on_left_tick():
    orders, customers = make_tables()
    rich = join(orders, customers).filter(big)
    orders.append_row({"oid": 6, "cust": 2, "amount": 55.0})
    orders.set_value(0, "amount", 99.0)
    orders.tick()
    assert canon(rich) == canon(r for r in oracle_rows(orders, customers) if big(r))
    assert len(rich) == 5


def test_filter_over_join_updates_on_right_only_tick():
    orders, customers = make_tables()
    tier1 = join(orders, customers).filter(tier_one)
    customers.set_value(1, "tier", 1)  # Bo moves to tier 1
    customers.tick()
    assert canon(tier1) == canon(r for r in oracle_rows(orders, customers) if tier_one(r))
    assert sorted(row["oid"] for row in tier1) == [1, 2, 3, 5]


def test_predicate_sees_right_columns_and_none_when_unmatched():
    orders, customers = make_tables()
    seen = []
    join(orders, customers).filter(lambda row: seen.append(row) or True)
    assert [row for row in seen if row["oid"] == 4] == [{
        "oid": 4, "cust": 9, "amount": 90.0,
        "right_cid": None, "right_name": None, "right_tier": None,
    }]
    assert {row["right_name"] for row in seen if row["oid"] != 4} == {"Ada", "Bo", "Cy"}


def test_filter_reads_keep_error_types():
    orders, customers = make_tables()
    rich = join(orders, customers).filter(big)
    with pytest.raises(IndexError):
        rich[len(rich)]
    with pytest.raises(IndexError):
        rich.get_row(len(rich))
    with pytest.raises(KeyError):
        rich.get_value(0, "missing")
    assert rich.get_value(0, "right_name") == rich[0]["right_name"]


def test_iteration_raises_when_either_joined_table_mutates():
    orders, customers = make_tables()
    rich = join(orders, customers).filter(big)
    with pytest.raises(RuntimeError):
        for _ in rich:
            customers.set_value(0, "name", "Ada2")


def test_failing_predicate_is_retryable():
    orders, customers = make_tables()
    broken = {"on": False}

    def predicate(row):
        if broken["on"]:
            raise ValueError("boom")
        return big(row)

    rich = join(orders, customers).filter(predicate)
    broken["on"] = True
    orders.set_value(0, "amount", 80.0)
    with pytest.raises(ValueError):
        orders.tick()
    broken["on"] = False
    orders.tick()
    assert canon(rich) == canon(r for r in oracle_rows(orders, customers) if big(r))


def test_predicate_mutating_the_right_table_raises():
    orders, customers = make_tables()
    armed = {"on": False}

    def predicate(row):
        if armed["on"]:
            customers.append_row({"cid": 7, "name": "Zed", "tier": 3})
        return big(row)

    rich = join(orders, customers).filter(predicate)  # keep it registered
    armed["on"] = True
    orders.set_value(0, "amount", 80.0)
    with pytest.raises(RuntimeError):
        orders.tick()
    assert len(rich) == 3


def test_ticks_compact_both_tables():
    orders, customers = make_tables()
    rich = join(orders, customers).filter(big)
    orders.set_value(1, "amount", 5.0)
    customers.set_value(0, "name", "Ada2")
    orders.tick()
    customers.tick()
    # A join-coordinate cursor treated as a root cursor would hold these back.
    assert orders.pending_changes_count() == 0
    assert customers.pending_changes_count() == 0
    assert canon(rich) == canon(r for r in oracle_rows(orders, customers) if big(r))


def test_no_output_join_edit_keeps_filter_history():
    orders, customers = make_tables()
    joined = join(orders, customers, how="inner")
    rich = joined.filter(big)
    ranked = rich.sort("amount")
    orders.set_value(3, "amount", 91.0)  # order 4 has no customer: no INNER output
    assert joined.sync() is False
    assert rich.sync() is False
    # Without the recorded parent version the sort would refresh and return True.
    assert ranked.sync() is False


def test_filter_over_a_stale_join_refreshes():
    orders, customers = make_tables()
    joined = join(orders, customers)
    rich = joined.filter(big)
    orders.append_row({"oid": 6, "cust": 2, "amount": 65.0})
    # The join is stale (no history): a version-checked refresh, not a replay.
    assert rich.sync() is True
    assert 6 not in [row["oid"] for row in rich], "the join has not seen the insert"
    joined.sync()
    # The stale refresh left no cursor, so the filter rebaselines.
    assert rich.sync() is True
    assert canon(rich) == canon(r for r in oracle_rows(orders, customers) if big(r))
