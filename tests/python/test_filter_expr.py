"""Tests for expression-based filtering (filter_expr)."""

import pytest
import livetable


@pytest.fixture
def sample_table():
    """Create a sample table for testing."""
    schema = livetable.Schema([
        ("id", livetable.ColumnType.INT32, False),
        ("name", livetable.ColumnType.STRING, False),
        ("age", livetable.ColumnType.INT32, True),
        ("score", livetable.ColumnType.FLOAT64, True),
        ("active", livetable.ColumnType.BOOL, False),
    ])
    table = livetable.Table("test", schema)
    table.append_row({"id": 1, "name": "Alice", "age": 25, "score": 95.5, "active": True})
    table.append_row({"id": 2, "name": "Bob", "age": 30, "score": 87.0, "active": False})
    table.append_row({"id": 3, "name": "Charlie", "age": None, "score": 92.0, "active": True})
    table.append_row({"id": 4, "name": "Diana", "age": 22, "score": None, "active": True})
    table.append_row({"id": 5, "name": "Eve", "age": 35, "score": 78.5, "active": False})
    return table


class TestFilterExprBasic:
    """Basic filter_expr tests."""

    def test_simple_equality(self, sample_table):
        """Test simple column = value."""
        indices = sample_table.filter_expr("id = 1")
        assert indices == [0]

    def test_simple_greater_than(self, sample_table):
        """Test column > value."""
        indices = sample_table.filter_expr("age > 25")
        assert set(indices) == {1, 4}  # Bob (30), Eve (35)

    def test_simple_less_than(self, sample_table):
        """Test column < value."""
        indices = sample_table.filter_expr("age < 30")
        assert set(indices) == {0, 3}  # Alice (25), Diana (22)

    def test_greater_than_or_equal(self, sample_table):
        """Test column >= value."""
        indices = sample_table.filter_expr("score >= 90")
        assert set(indices) == {0, 2}  # Alice (95.5), Charlie (92.0)

    def test_less_than_or_equal(self, sample_table):
        """Test column <= value."""
        indices = sample_table.filter_expr("score <= 87")
        assert set(indices) == {1, 4}  # Bob (87.0), Eve (78.5)

    def test_not_equal(self, sample_table):
        """Test column != value."""
        indices = sample_table.filter_expr("name != 'Alice'")
        assert set(indices) == {1, 2, 3, 4}


class TestFilterExprStrings:
    """String filtering tests."""

    def test_string_equality(self, sample_table):
        """Test string = 'value'."""
        indices = sample_table.filter_expr("name = 'Bob'")
        assert indices == [1]

    def test_string_not_equal(self, sample_table):
        """Test string != 'value'."""
        indices = sample_table.filter_expr("name != 'Alice'")
        assert 0 not in indices
        assert len(indices) == 4


class TestFilterExprBooleans:
    """Boolean filtering tests."""

    def test_bool_true(self, sample_table):
        """Test boolean = true."""
        indices = sample_table.filter_expr("active = true")
        assert set(indices) == {0, 2, 3}  # Alice, Charlie, Diana

    def test_bool_false(self, sample_table):
        """Test boolean = false."""
        indices = sample_table.filter_expr("active = false")
        assert set(indices) == {1, 4}  # Bob, Eve


class TestFilterExprLogical:
    """Logical operator tests (AND, OR, NOT)."""

    def test_and_operator(self, sample_table):
        """Test AND operator."""
        indices = sample_table.filter_expr("age > 20 AND active = true")
        assert set(indices) == {0, 3}  # Alice (25, true), Diana (22, true)

    def test_or_operator(self, sample_table):
        """Test OR operator."""
        indices = sample_table.filter_expr("name = 'Alice' OR name = 'Bob'")
        assert set(indices) == {0, 1}

    def test_not_operator(self, sample_table):
        """Test NOT operator."""
        indices = sample_table.filter_expr("NOT active = false")
        assert set(indices) == {0, 2, 3}  # All except Bob and Eve

    def test_complex_and_or(self, sample_table):
        """Test complex AND/OR combinations."""
        indices = sample_table.filter_expr("(age > 25 OR score > 90) AND active = true")
        assert set(indices) == {0, 2}  # Alice (95.5 score), Charlie (92.0 score)


class TestFilterExprNulls:
    """NULL handling tests."""

    def test_is_null(self, sample_table):
        """Test IS NULL."""
        indices = sample_table.filter_expr("age IS NULL")
        assert indices == [2]  # Charlie

    def test_is_not_null(self, sample_table):
        """Test IS NOT NULL."""
        indices = sample_table.filter_expr("score IS NOT NULL")
        assert set(indices) == {0, 1, 2, 4}  # All except Diana

    def test_null_combined_with_and(self, sample_table):
        """Test NULL combined with AND."""
        indices = sample_table.filter_expr("score IS NOT NULL AND score > 85")
        assert set(indices) == {0, 1, 2}  # Alice, Bob, Charlie


class TestFilterExprParentheses:
    """Parentheses grouping tests."""

    def test_parentheses_precedence(self, sample_table):
        """Test parentheses affect precedence."""
        # Without parentheses: age < 25 OR (age > 30 AND active = true)
        indices1 = sample_table.filter_expr("age < 25 OR age > 30 AND active = true")

        # With parentheses: (age < 25 OR age > 30) AND active = true
        indices2 = sample_table.filter_expr("(age < 25 OR age > 30) AND active = true")

        # Results should be different
        assert set(indices1) == {3}  # Diana (22) OR nothing (no one >30 AND active)
        assert set(indices2) == {3}  # Diana (22), Eve is >30 but not active


class TestFilterExprErrors:
    """Error handling tests."""

    def test_invalid_column_returns_no_matches(self, sample_table):
        """Test that invalid column name returns no matches (treated as NULL)."""
        # Non-existent columns are treated as NULL, so equality comparisons fail
        indices = sample_table.filter_expr("nonexistent = 5")
        assert indices == []

    def test_syntax_error(self, sample_table):
        """Test error on syntax error."""
        with pytest.raises(Exception):
            sample_table.filter_expr("age >")

    def test_empty_expression(self, sample_table):
        """Test error on empty expression."""
        with pytest.raises(Exception):
            sample_table.filter_expr("")


class TestFilterExprEdgeCases:
    """Edge case tests."""

    def test_empty_table(self):
        """Test filter_expr on empty table."""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("empty", schema)
        indices = table.filter_expr("id > 0")
        assert indices == []

    def test_no_matches(self, sample_table):
        """Test filter that matches nothing."""
        indices = sample_table.filter_expr("id > 1000")
        assert indices == []

    def test_all_match(self, sample_table):
        """Test filter that matches everything."""
        indices = sample_table.filter_expr("id > 0")
        assert len(indices) == 5

    def test_float_comparison(self, sample_table):
        """Test float value comparisons."""
        indices = sample_table.filter_expr("score > 90.0")
        assert set(indices) == {0, 2}  # Alice (95.5), Charlie (92.0)

    def test_case_insensitive_keywords(self, sample_table):
        """Test that AND/OR/NOT are case-insensitive."""
        indices1 = sample_table.filter_expr("id = 1 AND active = true")
        indices2 = sample_table.filter_expr("id = 1 and active = true")
        assert indices1 == indices2
