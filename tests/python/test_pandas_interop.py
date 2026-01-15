"""
Tests for Pandas DataFrame interoperability.

This module tests conversion between livetable Tables and pandas DataFrames.
"""

import pytest
import livetable

# Try to import pandas, skip tests if not available
pandas = pytest.importorskip("pandas")
import numpy as np


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def sample_table():
    """Create a sample table with test data."""
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("name", livetable.ColumnType.STRING, False),
        ("score", livetable.ColumnType.FLOAT64, True),
    ])
    table = livetable.Table("students", schema)
    table.append_row({"id": 1, "name": "Alice", "score": 95.5})
    table.append_row({"id": 2, "name": "Bob", "score": 87.0})
    table.append_row({"id": 3, "name": "Charlie", "score": None})
    return table


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame."""
    return pandas.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.5, 30.5],
    })


# ============================================================================
# to_pandas Tests
# ============================================================================

class TestToPandas:
    """Tests for converting Tables to DataFrames."""

    def test_to_pandas_basic(self, sample_table):
        """to_pandas should return a pandas DataFrame."""
        df = sample_table.to_pandas()

        assert isinstance(df, pandas.DataFrame)
        assert len(df) == 3

    def test_to_pandas_column_names(self, sample_table):
        """DataFrame should have correct column names."""
        df = sample_table.to_pandas()

        assert set(df.columns) == {"id", "name", "score"}

    def test_to_pandas_values(self, sample_table):
        """DataFrame should have correct values."""
        df = sample_table.to_pandas()

        assert df.iloc[0]["id"] == 1
        assert df.iloc[0]["name"] == "Alice"
        assert df.iloc[0]["score"] == 95.5

    def test_to_pandas_null_values(self, sample_table):
        """NULL values should become NaN in DataFrame."""
        df = sample_table.to_pandas()

        # Row with NULL score
        assert pandas.isna(df.iloc[2]["score"])

    def test_to_pandas_empty_table(self):
        """Empty table should produce empty DataFrame."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
        ])
        empty_table = livetable.Table("empty", schema)

        df = empty_table.to_pandas()

        assert len(df) == 0
        assert "id" in df.columns

    def test_to_pandas_all_types(self):
        """All column types should convert correctly."""
        # Note: FLOAT32 not tested because Python floats are always float64
        schema = livetable.Schema([
            ("int32_col", livetable.ColumnType.INT32, False),
            ("int64_col", livetable.ColumnType.INT64, False),
            ("float64_col", livetable.ColumnType.FLOAT64, False),
            ("string_col", livetable.ColumnType.STRING, False),
            ("bool_col", livetable.ColumnType.BOOL, False),
        ])
        table = livetable.Table("all_types", schema)
        table.append_row({
            "int32_col": 42,
            "int64_col": 9999999999,
            "float64_col": 2.718281828,
            "string_col": "hello",
            "bool_col": True,
        })

        df = table.to_pandas()

        assert df.iloc[0]["int32_col"] == 42
        assert df.iloc[0]["int64_col"] == 9999999999
        assert abs(df.iloc[0]["float64_col"] - 2.718281828) < 0.0001
        assert df.iloc[0]["string_col"] == "hello"
        assert df.iloc[0]["bool_col"] == True


# ============================================================================
# from_pandas Tests
# ============================================================================

class TestFromPandas:
    """Tests for creating Tables from DataFrames."""

    def test_from_pandas_basic(self, sample_dataframe):
        """from_pandas should create a Table from DataFrame."""
        table = livetable.Table.from_pandas("test", sample_dataframe)

        assert len(table) == 3

    def test_from_pandas_values(self, sample_dataframe):
        """Table should have correct values from DataFrame."""
        table = livetable.Table.from_pandas("test", sample_dataframe)

        row0 = table.get_row(0)
        assert row0["id"] == 1
        assert row0["name"] == "Alice"
        assert row0["value"] == 10.5

    def test_from_pandas_with_nan(self):
        """NaN values in DataFrame should become NULL."""
        df = pandas.DataFrame({
            "id": [1, 2, 3],
            "score": [95.5, np.nan, 87.0],
        })

        table = livetable.Table.from_pandas("with_nan", df)

        assert table.get_row(0)["score"] == 95.5
        assert table.get_row(1)["score"] is None  # NaN -> NULL
        assert table.get_row(2)["score"] == 87.0

    def test_from_pandas_type_inference_int(self):
        """Integer columns should be inferred correctly."""
        df = pandas.DataFrame({"val": pandas.array([1, 2, 3], dtype="int64")})

        table = livetable.Table.from_pandas("int_test", df)

        # Should be able to read as int
        assert table.get_row(0)["val"] == 1

    def test_from_pandas_type_inference_float(self):
        """Float columns should be inferred correctly."""
        df = pandas.DataFrame({"val": [1.5, 2.5, 3.5]})

        table = livetable.Table.from_pandas("float_test", df)

        assert table.get_row(0)["val"] == 1.5

    def test_from_pandas_type_inference_string(self):
        """String columns should be inferred correctly."""
        df = pandas.DataFrame({"val": ["a", "b", "c"]})

        table = livetable.Table.from_pandas("string_test", df)

        assert table.get_row(0)["val"] == "a"

    def test_from_pandas_type_inference_bool(self):
        """Boolean columns should be inferred correctly."""
        df = pandas.DataFrame({"val": [True, False, True]})

        table = livetable.Table.from_pandas("bool_test", df)

        assert table.get_row(0)["val"] == True
        assert table.get_row(1)["val"] == False

    def test_from_pandas_empty_dataframe(self):
        """Empty DataFrame should produce empty Table."""
        df = pandas.DataFrame({"id": pandas.array([], dtype="int64")})

        table = livetable.Table.from_pandas("empty", df)

        assert len(table) == 0

    def test_from_pandas_no_columns_error(self):
        """DataFrame with no columns should raise error."""
        df = pandas.DataFrame()

        with pytest.raises(ValueError, match="no columns"):
            livetable.Table.from_pandas("empty", df)


# ============================================================================
# Round-trip Tests
# ============================================================================

class TestRoundTrip:
    """Tests for Table -> DataFrame -> Table conversion."""

    def test_roundtrip_preserves_data(self, sample_table):
        """Round-trip should preserve all data."""
        df = sample_table.to_pandas()
        restored = livetable.Table.from_pandas("restored", df)

        assert len(restored) == len(sample_table)

        for i in range(len(sample_table)):
            original = sample_table.get_row(i)
            restored_row = restored.get_row(i)

            for key in original:
                orig_val = original[key]
                rest_val = restored_row[key]

                # Handle NULL/NaN comparison
                if orig_val is None:
                    assert rest_val is None or pandas.isna(rest_val)
                else:
                    assert rest_val == orig_val

    def test_roundtrip_multiple_times(self, sample_table):
        """Multiple round-trips should preserve data."""
        current = sample_table

        for _ in range(3):
            df = current.to_pandas()
            current = livetable.Table.from_pandas("round", df)

        # Verify final table matches original
        assert len(current) == len(sample_table)


# ============================================================================
# Integration Tests
# ============================================================================

class TestPandasIntegration:
    """Integration tests with pandas operations."""

    def test_pandas_operations_after_to_pandas(self, sample_table):
        """Standard pandas operations should work on converted DataFrame."""
        df = sample_table.to_pandas()

        # Filtering
        high_scores = df[df["score"] >= 90]
        assert len(high_scores) == 1

        # Aggregation
        mean_score = df["score"].mean()
        assert abs(mean_score - 91.25) < 0.01  # (95.5 + 87.0) / 2

        # Sorting
        sorted_df = df.sort_values("score", ascending=False)
        assert sorted_df.iloc[0]["name"] == "Alice"

    def test_livetable_operations_after_from_pandas(self, sample_dataframe):
        """LiveTable operations should work on converted Table."""
        table = livetable.Table.from_pandas("test", sample_dataframe)

        # Filtering
        filtered = table.filter(lambda r: r["value"] >= 20)
        assert len(filtered) == 2

        # Aggregation
        total = table.sum("value")
        assert total == 61.5

        # Iteration
        names = [row["name"] for row in table]
        assert names == ["Alice", "Bob", "Charlie"]
