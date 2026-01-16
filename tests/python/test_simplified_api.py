"""
Tests for the simplified API: sort(), join(), group_by()
"""

import pytest
import livetable


class TestSort:
    """Tests for table.sort() method"""

    def setup_method(self):
        """Create test table"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.FLOAT64, True),
        ])
        self.table = livetable.Table("students", schema)
        self.table.append_row({"id": 1, "name": "Alice", "score": 95.5})
        self.table.append_row({"id": 2, "name": "Bob", "score": 87.0})
        self.table.append_row({"id": 3, "name": "Charlie", "score": 92.0})
        self.table.append_row({"id": 4, "name": "Diana", "score": None})

    def test_sort_single_column_ascending(self):
        """Sort by single column ascending (default)"""
        sorted_table = self.table.sort("name")
        assert len(sorted_table) == 4
        assert sorted_table[0]["name"] == "Alice"
        assert sorted_table[1]["name"] == "Bob"
        assert sorted_table[2]["name"] == "Charlie"
        assert sorted_table[3]["name"] == "Diana"

    def test_sort_single_column_descending(self):
        """Sort by single column descending"""
        sorted_table = self.table.sort("score", descending=True)
        # NULL should be first (nulls_first=True)
        assert sorted_table[0]["score"] is None
        assert sorted_table[1]["score"] == 95.5
        assert sorted_table[2]["score"] == 92.0
        assert sorted_table[3]["score"] == 87.0

    def test_sort_multiple_columns(self):
        """Sort by multiple columns with mixed order"""
        # Add duplicate scores for better test
        self.table.append_row({"id": 5, "name": "Eve", "score": 92.0})

        sorted_table = self.table.sort(["score", "name"], descending=[True, False])
        # Should sort by score desc, then name asc
        assert sorted_table[0]["score"] is None  # NULL first
        assert sorted_table[1]["score"] == 95.5
        # Two with 92.0 should be sorted by name asc
        assert sorted_table[2]["name"] == "Charlie"
        assert sorted_table[3]["name"] == "Eve"

    def test_sort_iteration(self):
        """Sorted view supports iteration"""
        sorted_table = self.table.sort("id")
        ids = [row["id"] for row in sorted_table]
        assert ids == [1, 2, 3, 4]

    def test_sort_indexing(self):
        """Sorted view supports indexing and slicing"""
        sorted_table = self.table.sort("id", descending=True)
        # Descending: 4, 3, 2, 1
        assert sorted_table[0]["id"] == 4
        assert sorted_table[-1]["id"] == 1

        # Slicing
        slice_result = sorted_table[1:3]
        assert len(slice_result) == 2
        assert slice_result[0]["id"] == 3

    def test_sort_invalid_column(self):
        """Error on invalid column"""
        with pytest.raises(ValueError):
            self.table.sort("nonexistent")

    def test_sort_descending_list_length_mismatch(self):
        """Error when descending list length doesn't match columns"""
        with pytest.raises(ValueError):
            self.table.sort(["name", "score"], descending=[True])


class TestJoin:
    """Tests for table.join() method"""

    def setup_method(self):
        """Create test tables"""
        # Students table
        student_schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
        ])
        self.students = livetable.Table("students", student_schema)
        self.students.append_row({"id": 1, "name": "Alice"})
        self.students.append_row({"id": 2, "name": "Bob"})
        self.students.append_row({"id": 3, "name": "Charlie"})

        # Enrollments table
        enrollment_schema = livetable.Schema([
            ("student_id", livetable.ColumnType.INT32, False),
            ("course", livetable.ColumnType.STRING, False),
        ])
        self.enrollments = livetable.Table("enrollments", enrollment_schema)
        self.enrollments.append_row({"student_id": 1, "course": "Math"})
        self.enrollments.append_row({"student_id": 1, "course": "Science"})
        self.enrollments.append_row({"student_id": 2, "course": "History"})
        # Note: No enrollment for student 3

    def test_join_with_on(self):
        """Join using 'on' parameter (same column name in both)"""
        # Need a table with matching column name
        grades_schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("grade", livetable.ColumnType.STRING, False),
        ])
        grades = livetable.Table("grades", grades_schema)
        grades.append_row({"id": 1, "grade": "A"})
        grades.append_row({"id": 2, "grade": "B"})

        joined = self.students.join(grades, on="id")
        assert len(joined) == 3  # Left join, all students
        assert joined[0]["name"] == "Alice"
        # Right table columns are prefixed with 'right_'
        assert joined[0]["right_grade"] == "A"

    def test_join_with_left_on_right_on(self):
        """Join using left_on/right_on parameters"""
        joined = self.students.join(
            self.enrollments,
            left_on="id",
            right_on="student_id"
        )
        # Student 1: 2 enrollments, Student 2: 1 enrollment, Student 3: 0 (NULL)
        assert len(joined) >= 3

    def test_join_inner(self):
        """Inner join excludes non-matching rows"""
        joined = self.students.join(
            self.enrollments,
            left_on="id",
            right_on="student_id",
            how="inner"
        )
        # Only students with enrollments (right columns prefixed)
        for row in joined:
            assert row["right_course"] is not None

    def test_join_left_default(self):
        """Default join is LEFT"""
        joined = self.students.join(
            self.enrollments,
            left_on="id",
            right_on="student_id"
        )
        # All students should be present
        names = [row["name"] for row in joined]
        assert "Charlie" in names  # Even though no enrollment

    def test_join_iteration(self):
        """Join view supports iteration"""
        joined = self.students.join(
            self.enrollments,
            left_on="id",
            right_on="student_id"
        )
        count = sum(1 for _ in joined)
        assert count == len(joined)

    def test_join_missing_keys(self):
        """Error when no keys specified"""
        with pytest.raises(ValueError, match="Must specify"):
            self.students.join(self.enrollments)

    def test_join_conflicting_keys(self):
        """Error when both on and left_on/right_on specified"""
        with pytest.raises(ValueError, match="Cannot specify both"):
            self.students.join(
                self.enrollments,
                on="id",
                left_on="id",
                right_on="student_id"
            )

    def test_join_invalid_how(self):
        """Error on invalid join type"""
        with pytest.raises(ValueError, match="Unknown join type"):
            self.students.join(
                self.enrollments,
                left_on="id",
                right_on="student_id",
                how="outer"
            )

    def test_join_multi_column(self):
        """Join on multiple columns"""
        # Create tables with composite keys
        sales_schema = livetable.Schema([
            ("year", livetable.ColumnType.INT32, False),
            ("month", livetable.ColumnType.INT32, False),
            ("amount", livetable.ColumnType.FLOAT64, False),
        ])
        sales = livetable.Table("sales", sales_schema)
        sales.append_row({"year": 2024, "month": 1, "amount": 1000.0})
        sales.append_row({"year": 2024, "month": 2, "amount": 1500.0})

        targets_schema = livetable.Schema([
            ("year", livetable.ColumnType.INT32, False),
            ("month", livetable.ColumnType.INT32, False),
            ("target", livetable.ColumnType.FLOAT64, False),
        ])
        targets = livetable.Table("targets", targets_schema)
        targets.append_row({"year": 2024, "month": 1, "target": 900.0})
        targets.append_row({"year": 2024, "month": 2, "target": 1200.0})

        joined = sales.join(targets, on=["year", "month"])
        assert len(joined) == 2
        for row in joined:
            assert row["amount"] is not None
            # Right table columns are prefixed with 'right_'
            assert row["right_target"] is not None


class TestGroupBy:
    """Tests for table.group_by() method"""

    def setup_method(self):
        """Create test table"""
        schema = livetable.Schema([
            ("department", livetable.ColumnType.STRING, False),
            ("employee", livetable.ColumnType.STRING, False),
            ("salary", livetable.ColumnType.FLOAT64, False),
        ])
        self.table = livetable.Table("employees", schema)
        self.table.append_row({"department": "Engineering", "employee": "Alice", "salary": 100000.0})
        self.table.append_row({"department": "Engineering", "employee": "Bob", "salary": 90000.0})
        self.table.append_row({"department": "Sales", "employee": "Charlie", "salary": 80000.0})
        self.table.append_row({"department": "Sales", "employee": "Diana", "salary": 85000.0})
        self.table.append_row({"department": "HR", "employee": "Eve", "salary": 70000.0})

    def test_group_by_sum(self):
        """Group by with SUM aggregation"""
        grouped = self.table.group_by("department", agg=[
            ("total_salary", "salary", "sum")
        ])

        # Find Engineering department
        eng_total = None
        for row in grouped:
            if row["department"] == "Engineering":
                eng_total = row["total_salary"]
                break

        assert eng_total == 190000.0  # 100000 + 90000

    def test_group_by_multiple_aggregations(self):
        """Group by with multiple aggregations"""
        grouped = self.table.group_by("department", agg=[
            ("total", "salary", "sum"),
            ("average", "salary", "avg"),
            ("headcount", "salary", "count"),
        ])

        for row in grouped:
            if row["department"] == "Sales":
                assert row["total"] == 165000.0  # 80000 + 85000
                assert row["average"] == 82500.0  # (80000 + 85000) / 2
                assert row["headcount"] == 2
                break

    def test_group_by_min_max(self):
        """Group by with MIN and MAX"""
        grouped = self.table.group_by("department", agg=[
            ("min_salary", "salary", "min"),
            ("max_salary", "salary", "max"),
        ])

        for row in grouped:
            if row["department"] == "Engineering":
                assert row["min_salary"] == 90000.0
                assert row["max_salary"] == 100000.0
                break

    def test_group_by_iteration(self):
        """Grouped view supports iteration"""
        grouped = self.table.group_by("department", agg=[
            ("total", "salary", "sum")
        ])

        departments = [row["department"] for row in grouped]
        assert len(departments) == 3  # Engineering, Sales, HR

    def test_group_by_multiple_columns(self):
        """Group by multiple columns"""
        # Add year to make multi-column grouping meaningful
        schema = livetable.Schema([
            ("year", livetable.ColumnType.INT32, False),
            ("department", livetable.ColumnType.STRING, False),
            ("revenue", livetable.ColumnType.FLOAT64, False),
        ])
        table = livetable.Table("revenue", schema)
        table.append_row({"year": 2023, "department": "Sales", "revenue": 1000.0})
        table.append_row({"year": 2023, "department": "Sales", "revenue": 2000.0})
        table.append_row({"year": 2024, "department": "Sales", "revenue": 3000.0})

        grouped = table.group_by(["year", "department"], agg=[
            ("total", "revenue", "sum")
        ])

        # Should have 2 groups: (2023, Sales) and (2024, Sales)
        assert len(grouped) == 2

    def test_group_by_function_aliases(self):
        """Test function name aliases (avg/average/mean, etc.)"""
        # These should all work
        grouped1 = self.table.group_by("department", agg=[("x", "salary", "avg")])
        grouped2 = self.table.group_by("department", agg=[("x", "salary", "average")])
        grouped3 = self.table.group_by("department", agg=[("x", "salary", "mean")])

        # All should produce same results
        assert len(grouped1) == len(grouped2) == len(grouped3)

    def test_group_by_invalid_function(self):
        """Error on invalid aggregation function"""
        with pytest.raises(ValueError, match="Unknown aggregation function"):
            self.table.group_by("department", agg=[
                ("x", "salary", "median")  # Not supported yet
            ])


class TestApiConsistency:
    """Tests verifying the new API works consistently with old API"""

    def setup_method(self):
        """Create test table"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("score", livetable.ColumnType.FLOAT64, False),
        ])
        self.table = livetable.Table("test", schema)
        self.table.append_row({"id": 1, "name": "Alice", "score": 95.5})
        self.table.append_row({"id": 2, "name": "Bob", "score": 87.0})

    def test_sort_equals_sorted_view(self):
        """table.sort() produces same result as SortedView"""
        # New API
        sorted1 = self.table.sort("score", descending=True)

        # Old API
        sorted2 = livetable.SortedView(
            "test_sorted",
            self.table,
            [livetable.SortKey.descending("score")]
        )

        # Same results
        assert len(sorted1) == len(sorted2)
        for i in range(len(sorted1)):
            assert sorted1[i]["id"] == sorted2[i]["id"]

    def test_old_api_still_works(self):
        """Original View constructors still work"""
        # SortedView
        sv = livetable.SortedView(
            "sorted",
            self.table,
            [livetable.SortKey.ascending("name")]
        )
        assert len(sv) == 2

        # AggregateView
        av = livetable.AggregateView(
            "agg",
            self.table,
            ["name"],
            [("total", "score", livetable.AggregateFunction.SUM)]
        )
        assert len(av) == 2
