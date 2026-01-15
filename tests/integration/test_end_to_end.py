#!/usr/bin/env python3
"""
End-to-end integration tests
Tests realistic workflows combining multiple features
"""

import pytest
import livetable


class TestContactManager:
    """Test a complete contact manager workflow"""

    def test_contact_manager_workflow(self):
        """Build a simple contact manager with search and updates"""
        # Create contacts table
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("email", livetable.ColumnType.STRING, False),
            ("phone", livetable.ColumnType.STRING, True),
            ("company", livetable.ColumnType.STRING, True),
        ])
        contacts = livetable.Table("contacts", schema)

        # Add contacts
        contacts.append_row({
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "phone": "555-0001",
            "company": "TechCorp"
        })
        contacts.append_row({
            "id": 2,
            "name": "Bob Smith",
            "email": "bob@test.com",
            "phone": None,
            "company": "StartupXYZ"
        })
        contacts.append_row({
            "id": 3,
            "name": "Charlie Brown",
            "email": "charlie@demo.com",
            "phone": "555-0003",
            "company": None
        })

        # Search for contacts with phones
        with_phones = contacts.filter(lambda row: row.get("phone") is not None)
        assert len(with_phones) == 2

        # Get contact summary (name + email only)
        summary = contacts.select(["name", "email"])
        assert len(summary) == 3
        assert "phone" not in summary.column_names()

        # Add computed "display name" column
        with_display = contacts.add_computed_column(
            "display",
            lambda row: f"{row['name']} ({row['company']})" if row.get("company") else row["name"]
        )

        assert with_display.get_value(0, "display") == "Alice Johnson (TechCorp)"
        assert with_display.get_value(2, "display") == "Charlie Brown"


class TestEcommerceAnalytics:
    """Test an e-commerce analytics workflow"""

    def test_sales_analytics(self):
        """Analyze sales data with joins and computed columns"""
        # Create products table
        products_schema = livetable.Schema([
            ("product_id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("category", livetable.ColumnType.STRING, False),
            ("price", livetable.ColumnType.FLOAT64, False),
        ])
        products = livetable.Table("products", products_schema)

        products.append_row({"product_id": 1, "name": "Laptop", "category": "Electronics", "price": 999.99})
        products.append_row({"product_id": 2, "name": "Mouse", "category": "Electronics", "price": 29.99})
        products.append_row({"product_id": 3, "name": "Desk", "category": "Furniture", "price": 299.99})

        # Create orders table
        orders_schema = livetable.Schema([
            ("order_id", livetable.ColumnType.INT32, False),
            ("product_id", livetable.ColumnType.INT32, False),
            ("quantity", livetable.ColumnType.INT32, False),
            ("discount", livetable.ColumnType.FLOAT64, False),
        ])
        orders = livetable.Table("orders", orders_schema)

        orders.append_row({"order_id": 101, "product_id": 1, "quantity": 2, "discount": 0.1})
        orders.append_row({"order_id": 102, "product_id": 2, "quantity": 5, "discount": 0.0})
        orders.append_row({"order_id": 103, "product_id": 1, "quantity": 1, "discount": 0.05})

        # Join orders with products
        order_details = livetable.JoinView(
            "order_details",
            orders,
            products,
            "product_id",
            "product_id",
            livetable.JoinType.INNER
        )

        assert len(order_details) == 3

        # Verify we can access columns from both tables
        row0 = order_details.get_row(0)
        assert "quantity" in row0  # from orders
        assert "right_price" in row0  # from products (with right_ prefix)

        # Manually calculate total for verification
        total = row0["right_price"] * row0["quantity"] * (1 - row0["discount"])
        expected_total = 999.99 * 2 * 0.9
        assert abs(total - expected_total) < 0.01


class TestGradebook:
    """Test a student gradebook workflow"""

    def test_gradebook_with_computed_grades(self):
        """Manage student grades with automatic letter grades"""
        # Create students table
        students_schema = livetable.Schema([
            ("student_id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("midterm", livetable.ColumnType.FLOAT64, False),
            ("final", livetable.ColumnType.FLOAT64, False),
            ("homework", livetable.ColumnType.FLOAT64, False),
        ])
        students = livetable.Table("students", students_schema)

        students.append_row({"student_id": 1, "name": "Alice", "midterm": 95.0, "final": 92.0, "homework": 98.0})
        students.append_row({"student_id": 2, "name": "Bob", "midterm": 78.0, "final": 85.0, "homework": 88.0})
        students.append_row({"student_id": 3, "name": "Charlie", "midterm": 88.0, "final": 90.0, "homework": 85.0})

        # Compute overall average (weighted: midterm 30%, final 40%, homework 30%)
        with_avg = students.add_computed_column(
            "average",
            lambda row: row["midterm"] * 0.3 + row["final"] * 0.4 + row["homework"] * 0.3
        )

        # Verify averages
        alice_row = with_avg.get_row(0)
        assert "average" in alice_row
        alice_avg = alice_row["average"]
        expected_avg = 95.0 * 0.3 + 92.0 * 0.4 + 98.0 * 0.3
        assert abs(alice_avg - expected_avg) < 0.01

        # Manually check grades
        def letter_grade(avg):
            if avg >= 90:
                return "A"
            elif avg >= 80:
                return "B"
            elif avg >= 70:
                return "C"
            else:
                return "F"

        assert letter_grade(alice_avg) == "A"

        bob_row = with_avg.get_row(1)
        bob_avg = bob_row["average"]
        assert letter_grade(bob_avg) == "B"


class TestBlogSystem:
    """Test a blog system with users, posts, and comments"""

    def test_blog_post_joins(self):
        """Test multi-table blog system"""
        # Create authors table
        authors_schema = livetable.Schema([
            ("author_id", livetable.ColumnType.INT32, False),
            ("name", livetable.ColumnType.STRING, False),
            ("email", livetable.ColumnType.STRING, False),
        ])
        authors = livetable.Table("authors", authors_schema)

        authors.append_row({"author_id": 1, "name": "Alice Writer", "email": "alice@blog.com"})
        authors.append_row({"author_id": 2, "name": "Bob Blogger", "email": "bob@blog.com"})

        # Create posts table
        posts_schema = livetable.Schema([
            ("post_id", livetable.ColumnType.INT32, False),
            ("author_id", livetable.ColumnType.INT32, False),
            ("title", livetable.ColumnType.STRING, False),
            ("views", livetable.ColumnType.INT32, False),
        ])
        posts = livetable.Table("posts", posts_schema)

        posts.append_row({"post_id": 1, "author_id": 1, "title": "First Post", "views": 100})
        posts.append_row({"post_id": 2, "author_id": 1, "title": "Second Post", "views": 250})
        posts.append_row({"post_id": 3, "author_id": 2, "title": "Bob's Post", "views": 150})

        # Join posts with authors
        posts_with_authors = livetable.JoinView(
            "posts_with_authors",
            posts,
            authors,
            "author_id",
            "author_id",
            livetable.JoinType.INNER
        )

        assert len(posts_with_authors) == 3

        # Verify we can access columns from both tables
        row0 = posts_with_authors.get_row(0)
        assert "title" in row0  # from posts
        assert "right_name" in row0  # from authors (with right_ prefix)
        assert "views" in row0  # from posts

        # Manually create summary
        summary = f"{row0['title']} by {row0['right_name']} ({row0['views']} views)"
        assert "by" in summary
        assert "views" in summary


class TestPerformanceScenario:
    """Test performance with larger datasets"""

    def test_bulk_insert_and_filter(self):
        """Insert many rows and perform filtering"""
        schema = livetable.Schema([
            ("id", livetable.ColumnType.INT32, False),
            ("value", livetable.ColumnType.INT32, False),
        ])
        table = livetable.Table("large_table", schema)

        # Insert 1000 rows
        for i in range(1000):
            table.append_row({"id": i, "value": i * 2})

        assert len(table) == 1000

        # Filter for even values
        evens = table.filter(lambda row: row["value"] % 4 == 0)
        assert len(evens) == 500

        # Get specific row
        row_500 = table.get_row(500)
        assert row_500["id"] == 500
        assert row_500["value"] == 1000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
