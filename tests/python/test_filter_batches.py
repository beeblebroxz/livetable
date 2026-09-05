"""Regression and differential checks for Python filters across mutation batches."""

import random

import livetable
import pytest


def make_table(rows, storage):
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("region", livetable.ColumnType.STRING, False),
        ("amount", livetable.ColumnType.INT32, True),
    ])
    table = livetable.Table("sales", schema, storage=storage)
    table.append_rows(rows)
    return table


def matches(row):
    return row["amount"] is not None and row["amount"] >= 5


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
@pytest.mark.parametrize("sync_method", ["tick", "sync"])
@pytest.mark.parametrize("initial_amount, updated_amount, shift", [
    pytest.param(0, 10, "delete", id="enter-before-delete"),
    pytest.param(0, 10, "insert", id="enter-before-insert"),
    pytest.param(10, 0, "insert", id="leave-before-insert"),
    pytest.param(None, 10, "delete", id="null-before-delete"),
])
def test_filter_update_before_row_shift(storage, sync_method, initial_amount,
                                       updated_amount, shift):
    table = make_table([
        {"id": 1, "region": "West", "amount": initial_amount},
        {"id": 2, "region": "East", "amount": initial_amount},
    ], storage)
    filtered = table.filter(matches)

    table.set_value(1, "amount", updated_amount)
    if shift == "delete":
        table.delete_row(0)
    else:
        table.insert_row(0, {"id": 3, "region": "North", "amount": 0})

    if sync_method == "tick":
        table.tick()
    else:
        filtered.sync()

    assert list(filtered) == [row for row in table if matches(row)]
    assert filtered.sync() is False

    # The batch cursor must also allow a later single update to propagate.
    table.set_value(len(table) - 1, "amount", 20)
    table.tick()
    assert list(filtered) == [row for row in table if matches(row)]
    assert table.tick() == 0


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
@pytest.mark.parametrize("seed", [0, 1, 2])
def test_batched_filter_pipeline_matches_python_model(storage, seed):
    rng = random.Random(seed)
    rows = []
    table = make_table(rows, storage)
    filtered = table.filter(matches)
    ranked = filtered.sort(["amount", "id"], descending=[True, False])
    grouped = filtered.group_by("region", agg=[
        ("total", "amount", "sum"),
        ("count", "amount", "count"),
    ])
    next_id = 1

    for batch in range(100):
        for _ in range(rng.randint(1, 6)):
            operation = rng.choice(["append", "insert", "update", "delete"])
            if not rows or operation in ("append", "insert"):
                row = {
                    "id": next_id,
                    "region": rng.choice(["West", "East", "North"]),
                    "amount": rng.choice([None, 0, 4, 5, 10, 20]),
                }
                next_id += 1
                if operation == "insert":
                    index = rng.randrange(len(rows) + 1)
                    table.insert_row(index, row)
                    rows.insert(index, row.copy())
                else:
                    table.append_row(row)
                    rows.append(row.copy())
            elif operation == "update":
                index = rng.randrange(len(rows))
                column = rng.choice(["amount", "region"])
                choices = ([None, 0, 4, 5, 10, 20] if column == "amount"
                           else ["West", "East", "North"])
                value = rng.choice(choices)
                table.set_value(index, column, value)
                rows[index][column] = value
            else:
                index = rng.randrange(len(rows))
                table.delete_row(index)
                del rows[index]

        table.tick()
        context = f"seed={seed}, batch={batch}, storage={storage}"
        expected = [row for row in rows if matches(row)]
        assert list(table) == rows, context
        assert list(filtered) == expected, context
        assert list(ranked) == sorted(
            expected, key=lambda row: (-row["amount"], row["id"])
        ), context

        totals = {}
        counts = {}
        for row in expected:
            region = row["region"]
            totals[region] = totals.get(region, 0) + row["amount"]
            counts[region] = counts.get(region, 0) + 1
        assert {row["region"]: row["total"] for row in grouped} == totals, context
        assert {row["region"]: row["count"] for row in grouped} == counts, context
        assert table.tick() == 0, context


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
@pytest.mark.parametrize("batch_size", [2, 300])
def test_filter_batch_can_retry_after_predicate_error(storage, batch_size):
    table = make_table([
        {"id": 1, "region": "West", "amount": 0},
        {"id": 2, "region": "East", "amount": 0},
    ], storage)
    fail = False

    def predicate(row):
        if fail and row["id"] == 2:
            raise ValueError("predicate failed")
        return matches(row)

    filtered = table.filter(predicate)
    grouped = filtered.group_by("region", agg=[("total", "amount", "sum")])
    for index in range(batch_size):
        table.set_value(index % 2, "amount", 10 if index % 2 == 0 else 20)
    fail = True
    with pytest.raises(ValueError, match="predicate failed"):
        table.tick()
    assert len(filtered) == 0, "failed callbacks must not partially publish membership"
    assert len(grouped) == 0

    fail = False
    table.tick()
    assert list(filtered) == list(table)
    assert {row["region"]: row["total"] for row in grouped} == {"West": 10, "East": 20}
    assert table.tick() == 0


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
def test_small_batches_evaluate_only_changed_rows(storage):
    table = make_table([
        {"id": index, "region": "West", "amount": 10}
        for index in range(1000)
    ], storage)
    calls = []

    def predicate(row):
        calls.append(row["id"])
        return matches(row)

    filtered = table.filter(predicate)
    grouped = filtered.group_by("region", agg=[("total", "amount", "sum")])
    calls.clear()
    table.set_value(0, "amount", 20)
    table.set_value(1, "amount", 30)
    table.tick()
    assert calls == [0, 1]
    assert grouped[0]["total"] == 10030


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
def test_child_created_before_parent_sync_has_safe_baseline(storage):
    table = make_table([{"id": 1, "region": "West", "amount": 10}], storage)
    filtered = table.filter(matches)
    table.set_value(0, "amount", 20)
    grouped = filtered.group_by("region", agg=[("total", "amount", "sum")])
    table.tick()
    assert grouped[0]["total"] == 20


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
def test_lagging_aggregate_and_explicit_refresh_recover(storage):
    table = make_table([{"id": 1, "region": "West", "amount": 10}], storage)
    threshold = 5
    filtered = table.filter(lambda row: row["amount"] >= threshold)
    fast = filtered.group_by("region", agg=[("total", "amount", "sum")])
    slow = filtered.group_by("region", agg=[("total", "amount", "sum")])
    for amount in [20, 30, 40]:
        table.set_value(0, "amount", amount)
        filtered.sync()
        fast.sync()
    slow.sync()
    assert list(fast) == list(slow) == [{"region": "West", "total": 40}]

    threshold = 50
    filtered.refresh()
    fast.sync()
    slow.sync()
    assert list(fast) == list(slow) == []


def test_direct_filter_sync_rejects_parent_mutation_in_callback():
    table = make_table([{"id": 1, "region": "West", "amount": 0}], "fast_reads")
    mutate = False

    def predicate(row):
        if mutate:
            table.set_value(0, "amount", 30)
        return matches(row)

    filtered = table.filter(predicate)
    table.set_value(0, "amount", 10)
    mutate = True
    with pytest.raises(RuntimeError, match="mutated during"):
        filtered.sync()
    assert len(filtered) == 0
    mutate = False
    table.tick()
    assert list(filtered) == [{"id": 1, "region": "West", "amount": 30}]


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
def test_nullable_integer_group_keys_survive_refresh(storage):
    schema = livetable.Schema([
        ("bucket", livetable.ColumnType.INT32, True),
        ("amount", livetable.ColumnType.INT32, False),
    ])
    table = livetable.Table("nullable_groups", schema, storage=storage)
    table.append_rows([{"bucket": None, "amount": 10}, {"bucket": 1, "amount": 20}])
    filtered = table.filter(matches)
    grouped = filtered.group_by("bucket", agg=[("total", "amount", "sum")])
    assert {row["bucket"]: row["total"] for row in grouped} == {None: 10, 1: 20}
    table.set_value(0, "amount", 15)
    table.tick()
    expected = {None: 15, 1: 20}
    assert {row["bucket"]: row["total"] for row in grouped} == expected
    grouped.refresh()
    assert {row["bucket"]: row["total"] for row in grouped} == expected
