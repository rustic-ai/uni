# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for Query DSL builder."""

import pytest

from uni_pydantic import (
    FilterExpr,
    FilterOp,
    PropertyProxy,
    Relationship,
    UniNode,
)


class Person(UniNode):
    """Test model for queries."""

    name: str
    age: int | None = None
    email: str | None = None
    active: bool = True


class TestPropertyProxy:
    """Tests for PropertyProxy filter expressions."""

    def test_equality(self):
        """Test equality filter."""
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy == "Alice"
        assert isinstance(expr, FilterExpr)
        assert expr.property_name == "name"
        assert expr.op == FilterOp.EQ
        assert expr.value == "Alice"

    def test_inequality(self):
        """Test inequality filter."""
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy != "Bob"
        assert expr.op == FilterOp.NE
        assert expr.value == "Bob"

    def test_less_than(self):
        """Test less than filter."""
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy < 30
        assert expr.op == FilterOp.LT
        assert expr.value == 30

    def test_less_than_or_equal(self):
        """Test less than or equal filter."""
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy <= 30
        assert expr.op == FilterOp.LE

    def test_greater_than(self):
        """Test greater than filter."""
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy > 18
        assert expr.op == FilterOp.GT

    def test_greater_than_or_equal(self):
        """Test greater than or equal filter."""
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy >= 18
        assert expr.op == FilterOp.GE

    def test_in_list(self):
        """Test IN filter."""
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy.in_([25, 30, 35])
        assert expr.op == FilterOp.IN
        assert expr.value == [25, 30, 35]

    def test_not_in_list(self):
        """Test NOT IN filter."""
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy.not_in([25, 30])
        assert expr.op == FilterOp.NOT_IN

    def test_like_pattern(self):
        """Test LIKE (regex) filter."""
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy.like("^A.*")
        assert expr.op == FilterOp.LIKE
        assert expr.value == "^A.*"

    def test_is_null(self):
        """Test IS NULL filter."""
        proxy = PropertyProxy[str]("email", Person)
        expr = proxy.is_null()
        assert expr.op == FilterOp.IS_NULL
        assert expr.value is None

    def test_is_not_null(self):
        """Test IS NOT NULL filter."""
        proxy = PropertyProxy[str]("email", Person)
        expr = proxy.is_not_null()
        assert expr.op == FilterOp.IS_NOT_NULL

    def test_starts_with(self):
        """Test STARTS WITH filter."""
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy.starts_with("Al")
        assert expr.op == FilterOp.STARTS_WITH
        assert expr.value == "Al"

    def test_ends_with(self):
        """Test ENDS WITH filter."""
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy.ends_with("ice")
        assert expr.op == FilterOp.ENDS_WITH

    def test_contains(self):
        """Test CONTAINS filter."""
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy.contains("lic")
        assert expr.op == FilterOp.CONTAINS


class TestFilterExpr:
    """Tests for FilterExpr Cypher generation."""

    def test_equality_cypher(self):
        """Test equality Cypher generation."""
        expr = FilterExpr("name", FilterOp.EQ, "Alice")
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.name = $p1"
        assert params == {"p1": "Alice"}

    def test_comparison_cypher(self):
        """Test comparison Cypher generation."""
        expr = FilterExpr("age", FilterOp.GE, 18)
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.age >= $p1"
        assert params == {"p1": 18}

    def test_in_cypher(self):
        """Test IN Cypher generation."""
        expr = FilterExpr("age", FilterOp.IN, [25, 30])
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.age IN $p1"
        assert params == {"p1": [25, 30]}

    def test_is_null_cypher(self):
        """Test IS NULL Cypher generation."""
        expr = FilterExpr("email", FilterOp.IS_NULL)
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.email IS NULL"
        assert params == {}

    def test_starts_with_cypher(self):
        """Test STARTS WITH Cypher generation."""
        expr = FilterExpr("name", FilterOp.STARTS_WITH, "Al")
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.name STARTS WITH $p1"
        assert params == {"p1": "Al"}


# Note: Full QueryBuilder tests require a database session
# These would be integration tests
class TestQueryBuilderUnit:
    """Unit tests for QueryBuilder (no database)."""

    def test_filter_expr_creation(self):
        """Test that filter expressions can be created."""
        name_filter = PropertyProxy[str]("name", Person) == "Alice"
        age_filter = PropertyProxy[int]("age", Person) >= 18

        assert name_filter.property_name == "name"
        assert age_filter.property_name == "age"

    def test_multiple_filters(self):
        """Test combining multiple filters."""
        filters = [
            PropertyProxy[str]("name", Person) == "Alice",
            PropertyProxy[int]("age", Person) >= 18,
            PropertyProxy[str]("email", Person).is_not_null(),
        ]

        assert len(filters) == 3
        assert all(isinstance(f, FilterExpr) for f in filters)
