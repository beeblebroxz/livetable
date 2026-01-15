"""Tests for DATE and DATETIME column types."""

import pytest
from datetime import date, datetime
import livetable


class TestDateColumnType:
    """Tests for DATE column type."""

    def test_date_column_basic(self):
        """Test basic date column operations."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("birth_date", livetable.ColumnType.DATE, False),
        ])
        table = livetable.Table("people", schema)

        # Add rows with date objects
        table.append_row({"id": 1, "birth_date": date(1990, 5, 15)})
        table.append_row({"id": 2, "birth_date": date(2000, 12, 25)})
        table.append_row({"id": 3, "birth_date": date(1970, 1, 1)})  # Unix epoch

        assert len(table) == 3

        # Read back values
        row0 = table.get_row(0)
        assert row0["birth_date"] == date(1990, 5, 15)

        row1 = table.get_row(1)
        assert row1["birth_date"] == date(2000, 12, 25)

        row2 = table.get_row(2)
        assert row2["birth_date"] == date(1970, 1, 1)

    def test_date_column_nullable(self):
        """Test nullable date column."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("date", livetable.ColumnType.DATE, True),  # Nullable
        ])
        table = livetable.Table("events", schema)

        table.append_row({"id": 1, "date": date(2023, 6, 15)})
        table.append_row({"id": 2, "date": None})

        row0 = table.get_row(0)
        assert row0["date"] == date(2023, 6, 15)

        row1 = table.get_row(1)
        assert row1["date"] is None

    def test_date_column_from_datetime(self):
        """Test that datetime objects work for date columns."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("date", livetable.ColumnType.DATE, False),
        ])
        table = livetable.Table("test", schema)

        # datetime should be converted to date (time part discarded)
        table.append_row({"id": 1, "date": datetime(2023, 6, 15, 10, 30, 0)})

        row = table.get_row(0)
        assert row["date"] == date(2023, 6, 15)

    def test_date_column_from_int(self):
        """Test that integer (days since epoch) works for date columns."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("date", livetable.ColumnType.DATE, False),
        ])
        table = livetable.Table("test", schema)

        # 0 = 1970-01-01, 19523 = 2023-06-15
        table.append_row({"id": 1, "date": 0})
        table.append_row({"id": 2, "date": 19523})

        row0 = table.get_row(0)
        assert row0["date"] == date(1970, 1, 1)

        row1 = table.get_row(1)
        assert row1["date"] == date(2023, 6, 15)

    def test_date_before_epoch(self):
        """Test dates before 1970."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("date", livetable.ColumnType.DATE, False),
        ])
        table = livetable.Table("historical", schema)

        table.append_row({"id": 1, "date": date(1955, 3, 14)})  # Einstein's death
        table.append_row({"id": 2, "date": date(1900, 1, 1)})

        row0 = table.get_row(0)
        assert row0["date"] == date(1955, 3, 14)

        row1 = table.get_row(1)
        assert row1["date"] == date(1900, 1, 1)


class TestDateTimeColumnType:
    """Tests for DATETIME column type."""

    def test_datetime_column_basic(self):
        """Test basic datetime column operations."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("timestamp", livetable.ColumnType.DATETIME, False),
        ])
        table = livetable.Table("events", schema)

        dt1 = datetime(2023, 6, 15, 10, 30, 0)
        dt2 = datetime(1970, 1, 1, 0, 0, 0)
        dt3 = datetime(2000, 12, 31, 23, 59, 59)

        table.append_row({"id": 1, "timestamp": dt1})
        table.append_row({"id": 2, "timestamp": dt2})
        table.append_row({"id": 3, "timestamp": dt3})

        assert len(table) == 3

        row0 = table.get_row(0)
        assert row0["timestamp"] == dt1

        row1 = table.get_row(1)
        assert row1["timestamp"] == dt2

        row2 = table.get_row(2)
        assert row2["timestamp"] == dt3

    def test_datetime_column_nullable(self):
        """Test nullable datetime column."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("timestamp", livetable.ColumnType.DATETIME, True),
        ])
        table = livetable.Table("logs", schema)

        table.append_row({"id": 1, "timestamp": datetime(2023, 6, 15, 10, 0, 0)})
        table.append_row({"id": 2, "timestamp": None})

        row0 = table.get_row(0)
        assert row0["timestamp"] == datetime(2023, 6, 15, 10, 0, 0)

        row1 = table.get_row(1)
        assert row1["timestamp"] is None

    def test_datetime_with_milliseconds(self):
        """Test datetime preserves milliseconds."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("timestamp", livetable.ColumnType.DATETIME, False),
        ])
        table = livetable.Table("precise", schema)

        # Create datetime with microseconds (will be rounded to milliseconds)
        dt = datetime(2023, 6, 15, 10, 30, 45, 123000)  # 123ms
        table.append_row({"id": 1, "timestamp": dt})

        row = table.get_row(0)
        result = row["timestamp"]
        assert result.year == 2023
        assert result.month == 6
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 45
        # Microseconds should be 123000 (123ms)
        assert result.microsecond == 123000

    def test_datetime_from_date(self):
        """Test that date objects work for datetime columns (midnight)."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("timestamp", livetable.ColumnType.DATETIME, False),
        ])
        table = livetable.Table("test", schema)

        table.append_row({"id": 1, "timestamp": date(2023, 6, 15)})

        row = table.get_row(0)
        assert row["timestamp"] == datetime(2023, 6, 15, 0, 0, 0)


class TestDateTimeSerialization:
    """Tests for CSV/JSON serialization of date/datetime types."""

    def test_date_to_csv(self):
        """Test date column serializes to ISO format in CSV."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("date", livetable.ColumnType.DATE, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "date": date(2023, 6, 15)})
        table.append_row({"id": 2, "date": date(1970, 1, 1)})

        csv = table.to_csv()
        assert "2023-06-15" in csv
        assert "1970-01-01" in csv

    def test_date_from_csv(self):
        """Test date parsing from CSV."""
        csv = "id,date\n1,2023-06-15\n2,1970-01-01"
        table = livetable.Table.from_csv("test", csv)

        assert len(table) == 2
        row0 = table.get_row(0)
        assert row0["date"] == date(2023, 6, 15)

    def test_datetime_to_csv(self):
        """Test datetime column serializes to ISO format in CSV."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("timestamp", livetable.ColumnType.DATETIME, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "timestamp": datetime(2023, 6, 15, 10, 30, 0)})

        csv = table.to_csv()
        assert "2023-06-15T10:30:00" in csv

    def test_datetime_from_csv(self):
        """Test datetime parsing from CSV."""
        csv = "id,timestamp\n1,2023-06-15T10:30:00\n2,1970-01-01T00:00:00"
        table = livetable.Table.from_csv("test", csv)

        assert len(table) == 2
        row0 = table.get_row(0)
        assert row0["timestamp"] == datetime(2023, 6, 15, 10, 30, 0)

    def test_date_to_json(self):
        """Test date column serializes to ISO format in JSON."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("date", livetable.ColumnType.DATE, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "date": date(2023, 6, 15)})

        json_str = table.to_json()
        assert "2023-06-15" in json_str

    def test_date_from_json(self):
        """Test date parsing from JSON."""
        json_str = '[{"id": 1, "date": "2023-06-15"}, {"id": 2, "date": "1970-01-01"}]'
        table = livetable.Table.from_json("test", json_str)

        assert len(table) == 2
        row0 = table.get_row(0)
        assert row0["date"] == date(2023, 6, 15)

    def test_datetime_to_json(self):
        """Test datetime column serializes to ISO format in JSON."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("timestamp", livetable.ColumnType.DATETIME, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "timestamp": datetime(2023, 6, 15, 10, 30, 0)})

        json_str = table.to_json()
        assert "2023-06-15T10:30:00" in json_str

    def test_datetime_from_json(self):
        """Test datetime parsing from JSON."""
        json_str = '[{"id": 1, "ts": "2023-06-15T10:30:00"}]'
        table = livetable.Table.from_json("test", json_str)

        row = table.get_row(0)
        assert row["ts"] == datetime(2023, 6, 15, 10, 30, 0)


class TestDateTimeWithViews:
    """Tests for date/datetime columns with views."""

    def test_date_with_sorted_view(self):
        """Test sorting by date column."""
        schema = livetable.Schema([
            ("name", livetable.ColumnType.STRING, False),
            ("date", livetable.ColumnType.DATE, False),
        ])
        table = livetable.Table("events", schema)

        table.append_row({"name": "C", "date": date(2023, 6, 15)})
        table.append_row({"name": "A", "date": date(2020, 1, 1)})
        table.append_row({"name": "B", "date": date(2021, 12, 31)})

        sorted_view = livetable.SortedView(
            "sorted",
            table,
            [livetable.SortKey.ascending("date")]
        )

        # Should be sorted by date: A (2020), B (2021), C (2023)
        row0 = sorted_view.get_row(0)
        row1 = sorted_view.get_row(1)
        row2 = sorted_view.get_row(2)

        assert row0["name"] == "A"
        assert row1["name"] == "B"
        assert row2["name"] == "C"

    def test_datetime_with_aggregate_view(self):
        """Test grouping by date part of datetime."""
        schema = livetable.Schema([
            ("date", livetable.ColumnType.DATE, False),
            ("value", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("sales", schema)

        table.append_row({"date": date(2023, 6, 15), "value": 100.0})
        table.append_row({"date": date(2023, 6, 15), "value": 200.0})
        table.append_row({"date": date(2023, 6, 16), "value": 150.0})

        agg = livetable.AggregateView(
            "daily_totals",
            table,
            ["date"],
            [("total", "value", livetable.AggregateFunction.SUM)]
        )

        assert len(agg) == 2


class TestColumnTypeAttributes:
    """Test that DATE and DATETIME are exposed as ColumnType attributes."""

    def test_column_type_date_exists(self):
        """Test ColumnType.DATE exists and is usable."""
        assert hasattr(livetable.ColumnType, 'DATE')
        assert repr(livetable.ColumnType.DATE) == "ColumnType.DATE"

    def test_column_type_datetime_exists(self):
        """Test ColumnType.DATETIME exists and is usable."""
        assert hasattr(livetable.ColumnType, 'DATETIME')
        assert repr(livetable.ColumnType.DATETIME) == "ColumnType.DATETIME"
