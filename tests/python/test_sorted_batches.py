"""Independent Python-model checks for sorted-view batch propagation."""
from functools import cmp_to_key
import random

import livetable
import pytest


def make_table(storage):
    return livetable.Table("source", livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("rank", livetable.ColumnType.INT32, True),
        ("region", livetable.ColumnType.STRING, False),
        ("amount", livetable.ColumnType.INT32, True),
    ]), storage=storage)


def included(row):
    return row["amount"] is not None and row["amount"] >= 0


def grouped(parent):
    return parent.group_by("region", agg=[
        ("total", "amount", "sum"), ("count", "amount", "count"),
    ])


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
@pytest.mark.parametrize("sync_method", ["tick", "sync"])
@pytest.mark.parametrize("descending", [False, True])
def test_mixed_batches_match_stable_python_sort_and_group(storage, sync_method,
                                                         descending):
    rng = random.Random(7182)
    table = make_table(storage)
    model = []
    filtered = table.filter(included)
    # The simplified sort API puts NULL first; native contract tests also
    # exercise explicit NULL-last keys and filters downstream of a sort.
    ranked = filtered.sort(["rank", "region"], descending=[descending, True])
    totals = grouped(ranked)

    def compare(a, b):
        av, bv = a["rank"], b["rank"]
        if av is None and bv is not None:
            return -1
        if bv is None and av is not None:
            return 1
        if av is not None and bv is not None and av != bv:
            result = (av > bv) - (av < bv)
            return -result if descending else result
        # No unique-ID tie breaker: equal keys must preserve parent order.
        return (b["region"] > a["region"]) - (b["region"] < a["region"])

    next_id = 0
    for batch in range(100):
        for _ in range(rng.randint(1, 8)):
            op = rng.choice(["insert", "update", "update", "delete"])
            if not model or op == "insert":
                row = {"id": next_id, "rank": rng.choice([None, 0, 1, 2]),
                       "region": rng.choice(["A", "B", "C"]),
                       "amount": rng.choice([None, -1, 0, 10, 20])}
                next_id += 1
                index = rng.randrange(len(model) + 1)
                table.insert_row(index, row)
                model.insert(index, row.copy())
            elif op == "delete":
                index = rng.randrange(len(model))
                table.delete_row(index)
                model.pop(index)
            else:
                index = rng.randrange(len(model))
                column = rng.choice(["rank", "region", "amount"])
                choices = {"rank": [None, 0, 1, 2], "region": ["A", "B", "C"],
                           "amount": [None, -1, 0, 10, 20]}
                value = rng.choice(choices[column])
                table.set_value(index, column, value)
                model[index][column] = value
        if sync_method == "tick":
            table.tick()
        else:
            filtered.sync()
            ranked.sync()
            totals.sync()
            # Once every consumer has caught up, losing root history is safe.
            table.clear_changeset()
        expected = sorted(filter(included, model), key=cmp_to_key(compare))
        context = f"batch={batch}, storage={storage}, sync={sync_method}"
        assert list(ranked) == expected, context
        expected_totals = {}
        for row in expected:
            values = expected_totals.setdefault(row["region"], [0, 0])
            values[0] += row["amount"]
            values[1] += 1
        assert {r["region"]: [r["total"], r["count"]] for r in totals} == expected_totals, context
        assert ranked.sync() is False
        assert totals.sync() is False


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
def test_sorted_history_fallbacks_and_stale_child_construction(storage):
    table = make_table(storage)
    table.append_rows([
        {"id": 0, "rank": 0, "region": "A", "amount": 10},
        {"id": 1, "rank": 1, "region": "B", "amount": 20},
    ])
    ranked = table.sort("rank")
    fast = grouped(ranked)
    slow = grouped(ranked)
    for value in range(30, 40):
        table.set_value(0, "amount", value)
        ranked.sync()
        fast.sync()
    slow.sync()
    assert list(fast) == list(slow)

    table.set_value(0, "amount", 50)
    table.set_value(0, "rank", 100)
    early = grouped(ranked)
    ranked.sync()
    early.sync()
    assert {r["region"]: r["total"] for r in early} == {"A": 50, "B": 20}

    for value in range(300):
        table.set_value(0, "amount", value)
    table.tick()  # large-batch fallback, all registered consumers catch up
    for view in [early, fast, slow]:
        assert {r["region"]: r["total"] for r in view} == {"A": 299, "B": 20}
    ranked.refresh()
    assert early.sync() is True
    table.set_value(0, "amount", 1000)
    table.clear_changeset()
    ranked.sync()
    early.sync()
    assert {r["region"]: r["total"] for r in early} == {"A": 1000, "B": 20}


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
def test_first_sorted_move_keeps_nullable_integer_groups_correct(storage):
    table = livetable.Table("integers", livetable.Schema([
        ("key", livetable.ColumnType.INT32, True),
        ("amount", livetable.ColumnType.INT32, False),
    ]), storage=storage)
    table.append_rows([{"key": None, "amount": 10}, {"key": 1, "amount": 20},
                       {"key": 2, "amount": 30}])
    ranked = table.sort("key")
    totals = ranked.group_by("key", agg=[("total", "amount", "sum")])
    table.set_value(0, "key", 0)
    table.set_value(1, "key", None)
    table.tick()
    assert {r["key"]: r["total"] for r in totals} == {0: 10, None: 20, 2: 30}
    ranked.refresh()
    totals.sync()
    assert {r["key"]: r["total"] for r in totals} == {0: 10, None: 20, 2: 30}


def test_grouping_explicit_sort_registers_parent_once_and_preserves_lifetime():
    import gc

    table = make_table("fast_reads")
    table.append_rows([
        {"id": 0, "rank": None, "region": "A", "amount": 10},
        {"id": 1, "rank": 1, "region": "B", "amount": 20},
    ])
    ranked = livetable.SortedView("explicit", table, [livetable.SortKey.ascending("rank")])
    assert table.registered_view_count() == 0
    first = grouped(ranked)
    second = grouped(ranked)
    assert table.registered_view_count() == 3, "sort must only register once"
    table.set_value(0, "rank", 0)
    table.set_value(0, "amount", 30)
    table.tick()
    assert list(ranked) == list(table)
    del ranked
    gc.collect()
    table.set_value(0, "amount", 40)
    table.tick()
    for view in [first, second]:
        assert {r["region"]: r["total"] for r in view} == {"A": 40, "B": 20}
    del view, first, second
    gc.collect()
    assert table.registered_view_count() == 0
