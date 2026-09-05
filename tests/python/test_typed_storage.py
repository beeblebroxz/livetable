"""Public-API regression coverage for native buffers and packed NULL flags."""
from collections import defaultdict
from datetime import date, datetime, timedelta
import random

import livetable
import pytest


def make_table(storage, interned, nullable):
    types = livetable.ColumnType
    schema = livetable.Schema([
        ("small", types.INT32, nullable),
        ("large", types.INT64, nullable),
        ("single", types.FLOAT32, nullable),
        ("score", types.FLOAT64, nullable),
        ("text", types.STRING, nullable),
        ("flag", types.BOOL, nullable),
        ("day", types.DATE, nullable),
        ("moment", types.DATETIME, nullable),
    ])
    return livetable.Table("typed", schema, storage=storage, use_string_interning=interned)


def make_row(n, nullable):
    row = {
        "small": n - 1000,
        "large": (1 << 54) + n,
        "single": (n % 13) * 0.5,
        "score": (n % 17) - 8.25,
        "text": ["", "repeated", "雪🌱", "a\0b"][n % 4],
        "flag": n % 2 == 0,
        "day": date(1969, 1, 1) + timedelta(days=n),
        "moment": datetime(1969, 12, 31, 23, 59, 59) + timedelta(milliseconds=n),
    }
    if nullable:
        for offset, key in enumerate(row):
            if (n + offset) % 7 == 0:
                row[key] = None
    return row


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
@pytest.mark.parametrize("interned", [False, True])
@pytest.mark.parametrize("nullable", [False, True])
def test_typed_rows_and_pipeline_follow_mixed_mutations(storage, interned, nullable):
    table = make_table(storage, interned, nullable)
    model = [make_row(n, nullable) for n in range(260)]
    table.append_rows(model)
    filtered = table.filter(lambda r: r["score"] is not None and r["score"] >= 0)
    ranked = filtered.sort("score", descending=True)
    totals = ranked.group_by("text", agg=[("total", "score", "sum"), ("count", "score", "count")])

    def check():
        table.tick()
        assert list(table) == model
        qualifying = [r for r in model if r["score"] is not None and r["score"] >= 0]
        assert list(filtered) == qualifying
        assert list(ranked) == sorted(qualifying, key=lambda r: r["score"], reverse=True)
        expected = defaultdict(lambda: [0.0, 0])
        for row in qualifying:
            expected[row["text"]][0] += row["score"]
            expected[row["text"]][1] += 1
        assert {r["text"]: [r["total"], r["count"]] for r in totals} == dict(expected)
        if interned:
            assert table.interner_stats()["total_references"] == sum(r["text"] is not None for r in model)

    check()
    for n, index in enumerate([0, 1, 63, 64, 65, 127, 128, 260]):
        row = make_row(n + 1000, nullable)
        table.insert_row(index, row)
        model.insert(index, row)
    check()
    rng = random.Random(12345)
    for step in range(250):
        index = rng.randrange(len(model))
        new_row = make_row(2000 + step, nullable)
        operation = rng.randrange(4)
        if operation == 0:
            table.insert_row(index, new_row)
            model.insert(index, new_row)
        elif operation == 1:
            table.delete_row(index)
            model.pop(index)
        elif operation == 2:
            column = rng.choice(list(new_row))
            table.set_value(index, column, new_row[column])
            model[index] = {**model[index], column: new_row[column]}
        else:
            table.append_row(new_row)
            model.append(new_row)
        if step % 17 == 0:
            check()
    check()
    # Delete across bitmap block boundaries; rebuild after an oversized batch.
    while model:
        index = len(model) // 2
        table.delete_row(index)
        model.pop(index)
    check()
    row = make_row(5000, nullable)
    table.append_row(row)
    model.append(row)
    check()


@pytest.mark.parametrize("storage", ["fast_reads", "fast_updates"])
@pytest.mark.parametrize("interned", [False, True])
def test_rejected_typed_writes_preserve_rows_flags_and_references(storage, interned):
    table = make_table(storage, interned, True)
    model = [make_row(n, True) for n in range(65)]
    table.append_rows(model)
    before_stats = table.interner_stats()
    iterator = iter(table)
    good = make_row(100, True)
    bad = {**good, "small": "wrong type"}
    for operation in [
        lambda: table.append_row(bad),
        lambda: table.append_rows([good, bad]),
        lambda: table.insert_row(0, bad),
        lambda: table.insert_row(len(model) + 1, good),
        lambda: table.set_value(0, "small", "wrong type"),
        lambda: table.set_value(len(model), "text", "not interned on failure"),
        lambda: table.delete_row(len(model)),
    ]:
        with pytest.raises((ValueError, TypeError, IndexError, OverflowError)):
            operation()
        assert list(table) == model
        assert table.interner_stats() == before_stats
    assert next(iterator) == model[0], "rejected writes must not advance the table version"
