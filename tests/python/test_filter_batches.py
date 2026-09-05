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
def test_filter_batch_can_retry_after_predicate_error(storage):
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
    table.set_value(0, "amount", 10)
    table.set_value(1, "amount", 20)
    fail = True
    with pytest.raises(ValueError, match="predicate failed"):
        table.tick()

    fail = False
    table.tick()
    assert list(filtered) == list(table)
    assert table.tick() == 0
