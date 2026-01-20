# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Query DSL builder for type-safe graph queries."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    cast,
)

from .base import UniNode
from .exceptions import QueryError
from .fields import RelationshipDescriptor

if TYPE_CHECKING:
    from .session import UniSession

NodeT = TypeVar("NodeT", bound=UniNode)
T = TypeVar("T")


class FilterOp(Enum):
    """Filter operation types."""

    EQ = "="
    NE = "<>"
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "=~"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    STARTS_WITH = "STARTS WITH"
    ENDS_WITH = "ENDS WITH"
    CONTAINS = "CONTAINS"


@dataclass
class FilterExpr:
    """A filter expression for a query."""

    property_name: str
    op: FilterOp
    value: Any = None

    def to_cypher(self, node_var: str, param_name: str) -> tuple[str, dict[str, Any]]:
        """Convert to Cypher WHERE clause fragment."""
        prop = f"{node_var}.{self.property_name}"

        if self.op == FilterOp.IS_NULL:
            return f"{prop} IS NULL", {}
        elif self.op == FilterOp.IS_NOT_NULL:
            return f"{prop} IS NOT NULL", {}
        elif self.op == FilterOp.IN:
            return f"{prop} IN ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.NOT_IN:
            return f"NOT {prop} IN ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.LIKE:
            return f"{prop} =~ ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.STARTS_WITH:
            return f"{prop} STARTS WITH ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.ENDS_WITH:
            return f"{prop} ENDS WITH ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.CONTAINS:
            return f"{prop} CONTAINS ${param_name}", {param_name: self.value}
        else:
            return f"{prop} {self.op.value} ${param_name}", {param_name: self.value}


class PropertyProxy(Generic[T]):
    """
    Proxy for model properties that enables filter expressions.

    Used in query builder to create type-safe filter conditions.

    Example:
        >>> query.filter(Person.age >= 18)
        >>> query.filter(Person.name.starts_with("A"))
    """

    def __init__(self, property_name: str, model: type[UniNode]) -> None:
        self._property_name = property_name
        self._model = model

    def __eq__(self, other: Any) -> FilterExpr:  # type: ignore[override]
        return FilterExpr(self._property_name, FilterOp.EQ, other)

    def __ne__(self, other: Any) -> FilterExpr:  # type: ignore[override]
        return FilterExpr(self._property_name, FilterOp.NE, other)

    def __lt__(self, other: Any) -> FilterExpr:
        return FilterExpr(self._property_name, FilterOp.LT, other)

    def __le__(self, other: Any) -> FilterExpr:
        return FilterExpr(self._property_name, FilterOp.LE, other)

    def __gt__(self, other: Any) -> FilterExpr:
        return FilterExpr(self._property_name, FilterOp.GT, other)

    def __ge__(self, other: Any) -> FilterExpr:
        return FilterExpr(self._property_name, FilterOp.GE, other)

    def in_(self, values: Sequence[T]) -> FilterExpr:
        """Check if value is in a list."""
        return FilterExpr(self._property_name, FilterOp.IN, list(values))

    def not_in(self, values: Sequence[T]) -> FilterExpr:
        """Check if value is not in a list."""
        return FilterExpr(self._property_name, FilterOp.NOT_IN, list(values))

    def like(self, pattern: str) -> FilterExpr:
        """Match a regex pattern."""
        return FilterExpr(self._property_name, FilterOp.LIKE, pattern)

    def is_null(self) -> FilterExpr:
        """Check if value is null."""
        return FilterExpr(self._property_name, FilterOp.IS_NULL)

    def is_not_null(self) -> FilterExpr:
        """Check if value is not null."""
        return FilterExpr(self._property_name, FilterOp.IS_NOT_NULL)

    def starts_with(self, prefix: str) -> FilterExpr:
        """Check if string starts with prefix."""
        return FilterExpr(self._property_name, FilterOp.STARTS_WITH, prefix)

    def ends_with(self, suffix: str) -> FilterExpr:
        """Check if string ends with suffix."""
        return FilterExpr(self._property_name, FilterOp.ENDS_WITH, suffix)

    def contains(self, substring: str) -> FilterExpr:
        """Check if string contains substring."""
        return FilterExpr(self._property_name, FilterOp.CONTAINS, substring)


class ModelProxy(Generic[NodeT]):
    """
    Proxy for model classes that provides property proxies.

    Enables type-safe property access in query filters.

    Example:
        >>> Person.name  # Returns PropertyProxy for 'name'
        >>> query.filter(Person.age >= 18)
    """

    def __init__(self, model: type[NodeT]) -> None:
        self._model = model

    def __getattr__(self, name: str) -> PropertyProxy[Any]:
        if name.startswith("_"):
            raise AttributeError(name)
        return PropertyProxy(name, self._model)


@dataclass
class OrderByClause:
    """An ORDER BY clause."""

    property_name: str
    descending: bool = False


@dataclass
class TraversalStep:
    """A relationship traversal step."""

    edge_type: str
    direction: Literal["outgoing", "incoming", "both"]
    target_label: str | None = None


@dataclass
class VectorSearchConfig:
    """Configuration for vector similarity search."""

    property_name: str
    query_vector: list[float]
    k: int
    threshold: float | None = None


class QueryBuilder(Generic[NodeT]):
    """
    Type-safe query builder for graph queries.

    Provides a fluent API for building Cypher queries with
    type checking and IDE autocomplete support.

    Example:
        >>> adults = (
        ...     session.query(Person)
        ...     .filter(Person.age >= 18)
        ...     .order_by(Person.name)
        ...     .limit(10)
        ...     .all()
        ... )
    """

    def __init__(
        self,
        session: UniSession,
        model: type[NodeT],
    ) -> None:
        self._session = session
        self._model = model
        self._filters: list[FilterExpr] = []
        self._order_by: list[OrderByClause] = []
        self._limit: int | None = None
        self._skip: int | None = None
        self._distinct: bool = False
        self._traversals: list[TraversalStep] = []
        self._eager_load: list[str] = []
        self._vector_search: VectorSearchConfig | None = None
        self._param_counter = 0

    def _next_param(self) -> str:
        """Generate a unique parameter name."""
        self._param_counter += 1
        return f"p{self._param_counter}"

    def filter(self, expr: FilterExpr) -> QueryBuilder[NodeT]:
        """
        Add a filter condition.

        Args:
            expr: A filter expression created from model properties.

        Returns:
            Self for method chaining.

        Example:
            >>> query.filter(Person.age >= 18)
            >>> query.filter(Person.name.starts_with("A"))
        """
        self._filters.append(expr)
        return self

    def filter_by(self, **kwargs: Any) -> QueryBuilder[NodeT]:
        """
        Add equality filters by keyword arguments.

        Args:
            **kwargs: Property name to value mappings.

        Returns:
            Self for method chaining.

        Example:
            >>> query.filter_by(name="Alice", active=True)
        """
        for prop, value in kwargs.items():
            self._filters.append(FilterExpr(prop, FilterOp.EQ, value))
        return self

    def order_by(
        self,
        prop: PropertyProxy[Any] | str,
        descending: bool = False,
    ) -> QueryBuilder[NodeT]:
        """
        Add an ORDER BY clause.

        Args:
            prop: Property to order by (PropertyProxy or string).
            descending: Whether to sort in descending order.

        Returns:
            Self for method chaining.

        Example:
            >>> query.order_by(Person.name)
            >>> query.order_by(Person.age, descending=True)
        """
        if isinstance(prop, PropertyProxy):
            name = prop._property_name
        else:
            name = prop
        self._order_by.append(OrderByClause(name, descending))
        return self

    def limit(self, n: int) -> QueryBuilder[NodeT]:
        """
        Limit the number of results.

        Args:
            n: Maximum number of results.

        Returns:
            Self for method chaining.
        """
        self._limit = n
        return self

    def skip(self, n: int) -> QueryBuilder[NodeT]:
        """
        Skip the first n results.

        Args:
            n: Number of results to skip.

        Returns:
            Self for method chaining.
        """
        self._skip = n
        return self

    def distinct(self) -> QueryBuilder[NodeT]:
        """
        Return only distinct results.

        Returns:
            Self for method chaining.
        """
        self._distinct = True
        return self

    def traverse(
        self,
        relationship: RelationshipDescriptor[Any] | str,
        target_model: type[UniNode] | None = None,
    ) -> QueryBuilder[NodeT]:
        """
        Traverse a relationship to related nodes.

        Args:
            relationship: Relationship descriptor or edge type string.
            target_model: Optional target model type for type hints.

        Returns:
            Self for method chaining.

        Example:
            >>> query.filter(Person.name == "Alice").traverse(Person.friends)
        """
        if isinstance(relationship, RelationshipDescriptor):
            edge_type = relationship.config.edge_type
            direction = relationship.config.direction
        else:
            edge_type = relationship
            direction = "outgoing"

        target_label = target_model.__label__ if target_model else None
        self._traversals.append(
            TraversalStep(edge_type, direction, target_label)
        )
        return self

    def eager_load(self, *relationships: RelationshipDescriptor[Any] | str) -> QueryBuilder[NodeT]:
        """
        Eager load relationships to avoid N+1 queries.

        Args:
            *relationships: Relationships to load eagerly.

        Returns:
            Self for method chaining.

        Example:
            >>> query.eager_load(Person.friends, Person.works_at)
        """
        for rel in relationships:
            if isinstance(rel, RelationshipDescriptor):
                self._eager_load.append(rel.field_name)
            else:
                self._eager_load.append(rel)
        return self

    def vector_search(
        self,
        prop: PropertyProxy[Any] | str,
        query_vector: list[float],
        k: int = 10,
        threshold: float | None = None,
    ) -> QueryBuilder[NodeT]:
        """
        Perform vector similarity search.

        Args:
            prop: Vector property to search.
            query_vector: Query vector for similarity matching.
            k: Number of nearest neighbors to return.
            threshold: Optional distance threshold.

        Returns:
            Self for method chaining.

        Example:
            >>> similar = query.vector_search(Person.embedding, vec, k=10)
        """
        if isinstance(prop, PropertyProxy):
            name = prop._property_name
        else:
            name = prop

        self._vector_search = VectorSearchConfig(
            property_name=name,
            query_vector=query_vector,
            k=k,
            threshold=threshold,
        )
        return self

    def _build_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build the Cypher query string and parameters."""
        label = self._model.__label__
        params: dict[str, Any] = {}

        # Handle vector search separately
        if self._vector_search:
            return self._build_vector_search_cypher()

        # Build MATCH clause with traversals
        if self._traversals:
            match_pattern = self._build_traversal_pattern()
        else:
            match_pattern = f"(n:{label})"

        cypher = f"MATCH {match_pattern}"

        # Build WHERE clause
        if self._filters:
            where_parts = []
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("n", param_name)
                where_parts.append(clause)
                params.update(clause_params)
            cypher += " WHERE " + " AND ".join(where_parts)

        # Build RETURN clause
        return_var = "n" if not self._traversals else self._get_final_var()
        if self._distinct:
            cypher += f" RETURN DISTINCT {return_var}"
        else:
            cypher += f" RETURN {return_var}"

        # Add ORDER BY
        if self._order_by:
            order_parts = []
            for o in self._order_by:
                order_str = f"{return_var}.{o.property_name}"
                if o.descending:
                    order_str += " DESC"
                order_parts.append(order_str)
            cypher += " ORDER BY " + ", ".join(order_parts)

        # Add SKIP and LIMIT
        if self._skip is not None:
            cypher += f" SKIP {self._skip}"
        if self._limit is not None:
            cypher += f" LIMIT {self._limit}"

        return cypher, params

    def _build_traversal_pattern(self) -> str:
        """Build MATCH pattern for relationship traversals."""
        label = self._model.__label__
        parts = [f"(n:{label})"]

        for i, step in enumerate(self._traversals):
            var = f"n{i + 1}"
            edge_var = f"r{i}"

            # Build edge pattern based on direction
            if step.direction == "outgoing":
                edge_pattern = f"-[{edge_var}:{step.edge_type}]->"
            elif step.direction == "incoming":
                edge_pattern = f"<-[{edge_var}:{step.edge_type}]-"
            else:  # both
                edge_pattern = f"-[{edge_var}:{step.edge_type}]-"

            # Build target node pattern
            if step.target_label:
                node_pattern = f"({var}:{step.target_label})"
            else:
                node_pattern = f"({var})"

            parts.append(edge_pattern + node_pattern)

        return "".join(parts)

    def _get_final_var(self) -> str:
        """Get the variable name for the final node in traversals."""
        if self._traversals:
            return f"n{len(self._traversals)}"
        return "n"

    def _build_vector_search_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build Cypher for vector search."""
        label = self._model.__label__
        vs = self._vector_search
        assert vs is not None

        # Use CALL db.idx.vector.query procedure
        params = {"query_vec": vs.query_vector}

        cypher = f"CALL db.idx.vector.query('{label}', '{vs.property_name}', $query_vec, {vs.k}) YIELD vid, distance"
        cypher += f" MATCH (n:{label}) WHERE id(n) = vid"

        # Add threshold filter
        if vs.threshold is not None:
            cypher += f" AND distance <= {vs.threshold}"

        # Add other filters
        if self._filters:
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("n", param_name)
                cypher += f" AND {clause}"
                params.update(clause_params)

        cypher += " RETURN n, distance ORDER BY distance"

        if self._limit:
            cypher += f" LIMIT {self._limit}"

        return cypher, params

    def all(self) -> list[NodeT]:
        """
        Execute the query and return all results.

        Returns:
            List of model instances.
        """
        cypher, params = self._build_cypher()
        results = self._session._db.query(cypher, params)

        instances = []
        for row in results:
            # Get the node data from the result
            node_data = row.get("n") or row.get(self._get_final_var())
            if node_data is None:
                continue

            # Note: row.get("distance") available for vector search results

            # Convert to model instance
            instance = self._session._result_to_model(node_data, self._model)
            if instance:
                instances.append(instance)

        # Handle eager loading
        if self._eager_load and instances:
            self._session._eager_load_relationships(instances, self._eager_load)

        return instances

    def first(self) -> NodeT | None:
        """
        Execute the query and return the first result.

        Returns:
            First model instance or None if no results.
        """
        # Optimize by limiting to 1
        original_limit = self._limit
        self._limit = 1
        results = self.all()
        self._limit = original_limit
        return results[0] if results else None

    def one(self) -> NodeT:
        """
        Execute the query and return exactly one result.

        Raises:
            QueryError: If no results or more than one result.

        Returns:
            The single model instance.
        """
        results = self.limit(2).all()
        if not results:
            raise QueryError("Query returned no results")
        if len(results) > 1:
            raise QueryError("Query returned more than one result")
        return results[0]

    def count(self) -> int:
        """
        Execute the query and return the count of results.

        Returns:
            Number of matching records.
        """
        label = self._model.__label__
        params: dict[str, Any] = {}

        cypher = f"MATCH (n:{label})"

        if self._filters:
            where_parts = []
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("n", param_name)
                where_parts.append(clause)
                params.update(clause_params)
            cypher += " WHERE " + " AND ".join(where_parts)

        cypher += " RETURN count(n) as count"

        results = self._session._db.query(cypher, params)
        return cast(int, results[0]["count"]) if results else 0

    def exists(self) -> bool:
        """
        Check if any matching records exist.

        Returns:
            True if at least one record matches.
        """
        return self.limit(1).count() > 0

    def delete(self) -> int:
        """
        Delete all matching records.

        Returns:
            Number of deleted records.

        Note:
            This also deletes relationships connected to the nodes.
        """
        label = self._model.__label__
        params: dict[str, Any] = {}

        cypher = f"MATCH (n:{label})"

        if self._filters:
            where_parts = []
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("n", param_name)
                where_parts.append(clause)
                params.update(clause_params)
            cypher += " WHERE " + " AND ".join(where_parts)

        cypher += " DETACH DELETE n"

        return cast(int, self._session._db.execute(cypher))

    def update(self, **kwargs: Any) -> int:
        """
        Update all matching records.

        Args:
            **kwargs: Properties to update.

        Returns:
            Number of updated records.
        """
        label = self._model.__label__
        params: dict[str, Any] = {}

        cypher = f"MATCH (n:{label})"

        if self._filters:
            where_parts = []
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("n", param_name)
                where_parts.append(clause)
                params.update(clause_params)
            cypher += " WHERE " + " AND ".join(where_parts)

        # Build SET clause
        set_parts = []
        for prop, value in kwargs.items():
            param_name = self._next_param()
            set_parts.append(f"n.{prop} = ${param_name}")
            params[param_name] = value

        cypher += " SET " + ", ".join(set_parts)

        return cast(int, self._session._db.execute(cypher))
