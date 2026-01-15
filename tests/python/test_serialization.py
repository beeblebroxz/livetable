"""
Tests for CSV/JSON serialization features in livetable.

Tests cover:
- to_csv() - Export table to CSV string
- to_json() - Export table to JSON string
- from_csv() - Import table from CSV string
- from_json() - Import table from JSON string
- Round-trip tests (export → import → verify)
- Edge cases (special characters, nulls, type inference)
"""

import pytest
import json
import livetable


# ============================================================================
# CSV Export Tests
# ============================================================================

class TestToCsv:
    """Tests for table.to_csv()"""

    def test_to_csv_basic(self):
        """Basic CSV export with simple data"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "name": "Alice"})
        table.append_row({"id": 2, "name": "Bob"})

        csv = table.to_csv()
        lines = csv.strip().split('\n')

        assert len(lines) == 3
        assert lines[0] == "id,name"
        assert lines[1] == "1,Alice"
        assert lines[2] == "2,Bob"

    def test_to_csv_with_all_types(self):
        """CSV export with all supported column types"""
        schema = livetable.Schema([
            ("int32_col", livetable.ColumnType.INT32, False),
            ("int64_col", livetable.ColumnType.INT64, False),
            ("float64_col", livetable.ColumnType.FLOAT64, False),
            ("string_col", livetable.ColumnType.STRING, False),
            ("bool_col", livetable.ColumnType.BOOL, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({
            "int32_col": 42,
            "int64_col": 9999999999,
            "float64_col": 3.14159,
            "string_col": "hello",
            "bool_col": True,
        })

        csv = table.to_csv()
        lines = csv.strip().split('\n')

        assert len(lines) == 2
        # Check header
        assert "int32_col" in lines[0]
        assert "int64_col" in lines[0]
        # Check data row contains expected values
        assert "42" in lines[1]
        assert "9999999999" in lines[1]
        assert "3.14159" in lines[1]
        assert "hello" in lines[1]
        assert "true" in lines[1]

    def test_to_csv_with_nulls(self):
        """CSV export with NULL values (should become empty strings)"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.STRING, True),  # nullable
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "value": "present"})
        table.append_row({"id": 2, "value": None})

        csv = table.to_csv()
        lines = csv.strip().split('\n')

        assert len(lines) == 3
        assert lines[1] == "1,present"
        assert lines[2] == "2,"  # NULL becomes empty

    def test_to_csv_with_special_characters(self):
        """CSV export with strings containing commas, quotes, and newlines"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("text", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "text": "hello, world"})  # comma
        table.append_row({"id": 2, "text": 'say "hi"'})  # quotes
        table.append_row({"id": 3, "text": "line1\nline2"})  # newline

        csv = table.to_csv()

        # Strings with special chars should be quoted
        assert '"hello, world"' in csv
        assert '"say ""hi"""' in csv  # double quotes escaped
        assert '"line1\nline2"' in csv

    def test_to_csv_empty_table(self):
        """CSV export of empty table should return just the header"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema)

        csv = table.to_csv()
        lines = csv.strip().split('\n')

        assert len(lines) == 1
        assert lines[0] == "id,name"


# ============================================================================
# JSON Export Tests
# ============================================================================

class TestToJson:
    """Tests for table.to_json()"""

    def test_to_json_basic(self):
        """Basic JSON export with simple data"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "name": "Alice"})
        table.append_row({"id": 2, "name": "Bob"})

        json_str = table.to_json()
        data = json.loads(json_str)

        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[0]["name"] == "Alice"
        assert data[1]["id"] == 2
        assert data[1]["name"] == "Bob"

    def test_to_json_all_types(self):
        """JSON export with all supported column types"""
        schema = livetable.Schema([
            ("int32_col", livetable.ColumnType.INT32, False),
            ("int64_col", livetable.ColumnType.INT64, False),
            ("float64_col", livetable.ColumnType.FLOAT64, False),
            ("string_col", livetable.ColumnType.STRING, False),
            ("bool_col", livetable.ColumnType.BOOL, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({
            "int32_col": 42,
            "int64_col": 9999999999,
            "float64_col": 3.14159,
            "string_col": "hello",
            "bool_col": True,
        })

        json_str = table.to_json()
        data = json.loads(json_str)

        assert len(data) == 1
        row = data[0]
        assert row["int32_col"] == 42
        assert row["int64_col"] == 9999999999
        assert abs(row["float64_col"] - 3.14159) < 0.00001
        assert row["string_col"] == "hello"
        assert row["bool_col"] is True

    def test_to_json_with_nulls(self):
        """JSON export with NULL values"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.INT32, True),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "value": 100})
        table.append_row({"id": 2, "value": None})

        json_str = table.to_json()
        data = json.loads(json_str)

        assert data[0]["value"] == 100
        assert data[1]["value"] is None

    def test_to_json_empty_table(self):
        """JSON export of empty table should return empty array"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("test", schema)

        json_str = table.to_json()
        data = json.loads(json_str)

        assert isinstance(data, list)
        assert len(data) == 0


# ============================================================================
# CSV Import Tests
# ============================================================================

class TestFromCsv:
    """Tests for Table.from_csv()"""

    def test_from_csv_basic(self):
        """Basic CSV import"""
        csv = "id,name\n1,Alice\n2,Bob"
        table = livetable.Table.from_csv("test", csv)

        assert len(table) == 2
        assert table.get_row(0)["id"] == 1
        assert table.get_row(0)["name"] == "Alice"
        assert table.get_row(1)["id"] == 2
        assert table.get_row(1)["name"] == "Bob"

    def test_from_csv_type_inference_int(self):
        """CSV import should infer integer types"""
        csv = "small,large\n42,9999999999"
        table = livetable.Table.from_csv("test", csv)

        row = table.get_row(0)
        assert row["small"] == 42
        assert row["large"] == 9999999999

    def test_from_csv_type_inference_float(self):
        """CSV import should infer float types"""
        csv = "value\n3.14159"
        table = livetable.Table.from_csv("test", csv)

        row = table.get_row(0)
        assert abs(row["value"] - 3.14159) < 0.00001

    def test_from_csv_type_inference_bool(self):
        """CSV import should infer boolean types"""
        csv = "flag\ntrue\nfalse\nTRUE\nFALSE"
        table = livetable.Table.from_csv("test", csv)

        assert table.get_row(0)["flag"] is True
        assert table.get_row(1)["flag"] is False
        assert table.get_row(2)["flag"] is True
        assert table.get_row(3)["flag"] is False

    def test_from_csv_type_inference_string(self):
        """CSV import should infer string types for non-numeric data"""
        csv = "name\nhello\nworld"
        table = livetable.Table.from_csv("test", csv)

        assert table.get_row(0)["name"] == "hello"
        assert table.get_row(1)["name"] == "world"

    def test_from_csv_with_nulls(self):
        """CSV import should handle empty values as NULL"""
        csv = "id,value\n1,present\n2,"
        table = livetable.Table.from_csv("test", csv)

        assert table.get_row(0)["value"] == "present"
        assert table.get_row(1)["value"] is None

    def test_from_csv_with_quoted_fields(self):
        """CSV import should handle quoted fields"""
        csv = 'id,text\n1,"hello, world"\n2,"say ""hi"""'
        table = livetable.Table.from_csv("test", csv)

        assert table.get_row(0)["text"] == "hello, world"
        assert table.get_row(1)["text"] == 'say "hi"'

    def test_from_csv_empty_error(self):
        """CSV import should error on empty input"""
        with pytest.raises(ValueError, match="empty"):
            livetable.Table.from_csv("test", "")

    def test_from_csv_header_only(self):
        """CSV with only header should create empty table"""
        csv = "id,name"
        table = livetable.Table.from_csv("test", csv)

        assert len(table) == 0
        assert "id" in table.column_names()
        assert "name" in table.column_names()


# ============================================================================
# JSON Import Tests
# ============================================================================

class TestFromJson:
    """Tests for Table.from_json()"""

    def test_from_json_basic(self):
        """Basic JSON import"""
        json_str = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
        table = livetable.Table.from_json("test", json_str)

        assert len(table) == 2
        assert table.get_row(0)["id"] == 1
        assert table.get_row(0)["name"] == "Alice"
        assert table.get_row(1)["id"] == 2
        assert table.get_row(1)["name"] == "Bob"

    def test_from_json_type_inference(self):
        """JSON import should infer correct types"""
        json_str = '[{"int_val": 42, "float_val": 3.14, "str_val": "hello", "bool_val": true}]'
        table = livetable.Table.from_json("test", json_str)

        row = table.get_row(0)
        assert row["int_val"] == 42
        assert abs(row["float_val"] - 3.14) < 0.01
        assert row["str_val"] == "hello"
        assert row["bool_val"] is True

    def test_from_json_with_nulls(self):
        """JSON import should handle null values"""
        json_str = '[{"id": 1, "value": 100}, {"id": 2, "value": null}]'
        table = livetable.Table.from_json("test", json_str)

        assert table.get_row(0)["value"] == 100
        assert table.get_row(1)["value"] is None

    def test_from_json_empty_array_error(self):
        """JSON import should error on empty array"""
        with pytest.raises(ValueError, match="empty"):
            livetable.Table.from_json("test", "[]")

    def test_from_json_invalid_json_error(self):
        """JSON import should error on invalid JSON"""
        with pytest.raises(ValueError, match="parse"):
            livetable.Table.from_json("test", "not valid json")

    def test_from_json_not_array_error(self):
        """JSON import should error if root is not an array"""
        with pytest.raises(ValueError, match="sequence|array"):
            livetable.Table.from_json("test", '{"id": 1}')


# ============================================================================
# Round-trip Tests
# ============================================================================

class TestRoundtrip:
    """Tests for export → import → verify"""

    def test_roundtrip_csv(self):
        """CSV round-trip should preserve data"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.FLOAT64, True),
        ])
        original = livetable.Table("original", schema)
        original.append_row({"id": 1, "name": "Alice", "score": 95.5})
        original.append_row({"id": 2, "name": "Bob", "score": 87.0})
        original.append_row({"id": 3, "name": "Charlie", "score": None})

        # Round-trip
        csv = original.to_csv()
        restored = livetable.Table.from_csv("restored", csv)

        # Verify
        assert len(restored) == len(original)
        for i in range(len(original)):
            orig_row = original.get_row(i)
            rest_row = restored.get_row(i)
            assert rest_row["id"] == orig_row["id"]
            assert rest_row["name"] == orig_row["name"]
            if orig_row["score"] is None:
                assert rest_row["score"] is None
            else:
                assert abs(rest_row["score"] - orig_row["score"]) < 0.01

    def test_roundtrip_json(self):
        """JSON round-trip should preserve data"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("active", livetable.ColumnType.BOOL, False),
            ("score", livetable.ColumnType.FLOAT64, True),
        ])
        original = livetable.Table("original", schema)
        original.append_row({"id": 1, "name": "Alice", "active": True, "score": 95.5})
        original.append_row({"id": 2, "name": "Bob", "active": False, "score": None})

        # Round-trip
        json_str = original.to_json()
        restored = livetable.Table.from_json("restored", json_str)

        # Verify
        assert len(restored) == len(original)
        for i in range(len(original)):
            orig_row = original.get_row(i)
            rest_row = restored.get_row(i)
            assert rest_row["id"] == orig_row["id"]
            assert rest_row["name"] == orig_row["name"]
            assert rest_row["active"] == orig_row["active"]
            if orig_row["score"] is None:
                assert rest_row["score"] is None
            else:
                assert abs(rest_row["score"] - orig_row["score"]) < 0.01

    def test_roundtrip_special_strings(self):
        """Round-trip should preserve special characters in strings"""
        schema = livetable.Schema([
            ("text", livetable.ColumnType.STRING, False),
        ])
        original = livetable.Table("original", schema)
        original.append_row({"text": "hello, world"})
        original.append_row({"text": 'say "hi"'})
        original.append_row({"text": "line1\nline2"})

        # CSV round-trip
        csv = original.to_csv()
        restored_csv = livetable.Table.from_csv("restored", csv)

        for i in range(len(original)):
            assert restored_csv.get_row(i)["text"] == original.get_row(i)["text"]

        # JSON round-trip
        json_str = original.to_json()
        restored_json = livetable.Table.from_json("restored", json_str)

        for i in range(len(original)):
            assert restored_json.get_row(i)["text"] == original.get_row(i)["text"]


# ============================================================================
# Integration Tests
# ============================================================================

class TestIntegration:
    """Integration tests combining serialization with other features"""

    def test_serialize_after_filter(self):
        """Can serialize a table after filtering"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("score", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("test", schema)
        table.append_row({"id": 1, "score": 95.0})
        table.append_row({"id": 2, "score": 75.0})
        table.append_row({"id": 3, "score": 85.0})

        # Can still serialize the original table
        csv = table.to_csv()
        assert "1," in csv
        assert "2," in csv
        assert "3," in csv

    def test_aggregations_on_imported_table(self):
        """Can run aggregations on imported table"""
        csv = "id,score\n1,90\n2,80\n3,70"
        table = livetable.Table.from_csv("test", csv)

        total = table.sum("score")
        avg = table.avg("score")

        assert total == 240.0
        assert abs(avg - 80.0) < 0.01

    def test_modify_imported_table(self):
        """Can modify an imported table"""
        json_str = '[{"id": 1, "name": "Alice"}]'
        table = livetable.Table.from_json("test", json_str)

        # Add a row
        table.append_row({"id": 2, "name": "Bob"})

        assert len(table) == 2
        assert table.get_row(1)["name"] == "Bob"

        # Export again
        new_json = table.to_json()
        data = json.loads(new_json)
        assert len(data) == 2
