# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
"""
Type stubs for the Uni Graph Database Python bindings.

Uni is an embedded, multi-model graph database with OpenCypher queries,
vector search, and columnar analytics.
"""

from typing import Any, Optional


# =============================================================================
# Data Classes
# =============================================================================

class VectorMatch:
    """Result from a vector similarity search."""

    vid: int
    """Vertex ID of the matched node."""

    distance: float
    """Distance/similarity score (lower is more similar for L2)."""


class SnapshotInfo:
    """Metadata about a database snapshot."""

    snapshot_id: str
    """Unique identifier for this snapshot."""

    name: Optional[str]
    """Optional user-provided name."""

    created_at: str
    """ISO 8601 timestamp when snapshot was created."""

    vertex_count: int
    """Number of vertices in the snapshot."""

    edge_count: int
    """Number of edges in the snapshot."""


class LabelInfo:
    """Information about a vertex label."""

    name: str
    """Label name."""

    is_document: bool
    """Whether this is a document label."""

    count: int
    """Approximate vertex count."""

    properties: list["PropertyInfo"]
    """List of properties defined on this label."""

    indexes: list["IndexInfo"]
    """List of indexes on this label."""


class PropertyInfo:
    """Information about a property."""

    name: str
    """Property name."""

    data_type: str
    """Data type (e.g., 'String', 'Int64', 'Vector{128}')."""

    nullable: bool
    """Whether null values are allowed."""

    is_indexed: bool
    """Whether an index exists on this property."""


class IndexInfo:
    """Information about an index."""

    name: str
    """Index name."""

    index_type: str
    """Index type (e.g., 'btree', 'hash', 'vector')."""

    properties: list[str]
    """Properties covered by this index."""

    status: str
    """Index status (e.g., 'Ready', 'Building')."""


class ConstraintInfo:
    """Information about a constraint."""

    name: str
    """Constraint name."""

    constraint_type: str
    """Constraint type."""

    properties: list[str]
    """Properties covered by this constraint."""

    enabled: bool
    """Whether the constraint is enabled."""


class BulkStats:
    """Statistics from a bulk loading operation."""

    vertices_inserted: int
    """Number of vertices inserted."""

    edges_inserted: int
    """Number of edges inserted."""

    duration_secs: float
    """Total duration in seconds."""


class IndexRebuildTask:
    """Information about an index rebuild task."""

    task_id: str
    """Task identifier."""

    label: str
    """Label being indexed."""

    status: str
    """Task status."""

    error: Optional[str]
    """Error message if failed."""


class CompactionStats:
    """Statistics from a compaction operation."""

    files_compacted: int
    """Number of files compacted."""

    bytes_before: int
    """Size before compaction."""

    bytes_after: int
    """Size after compaction."""

    duration_secs: float
    """Duration in seconds."""

    crdt_merges: int
    """Number of CRDT merge operations."""


# =============================================================================
# Builder Classes
# =============================================================================

class DatabaseBuilder:
    """Builder for creating or opening a Uni database."""

    @staticmethod
    def open(path: str) -> "DatabaseBuilder":
        """
        Open a database, creating it if it doesn't exist.

        Args:
            path: Path to the database directory.

        Returns:
            A DatabaseBuilder for further configuration.
        """
        ...

    @staticmethod
    def create(path: str) -> "DatabaseBuilder":
        """
        Create a new database. Fails if it already exists.

        Args:
            path: Path to the database directory.

        Returns:
            A DatabaseBuilder for further configuration.

        Raises:
            OSError: If the database already exists.
        """
        ...

    @staticmethod
    def open_existing(path: str) -> "DatabaseBuilder":
        """
        Open an existing database. Fails if it doesn't exist.

        Args:
            path: Path to the database directory.

        Returns:
            A DatabaseBuilder for further configuration.

        Raises:
            OSError: If the database doesn't exist.
        """
        ...

    @staticmethod
    def temporary() -> "DatabaseBuilder":
        """
        Create a temporary in-memory database.

        Returns:
            A DatabaseBuilder for further configuration.
        """
        ...

    def cache_size(self, bytes: int) -> "DatabaseBuilder":
        """
        Set the cache size in bytes.

        Args:
            bytes: Cache size in bytes.

        Returns:
            Self for method chaining.
        """
        ...

    def parallelism(self, n: int) -> "DatabaseBuilder":
        """
        Set the parallelism level for query execution.

        Args:
            n: Number of parallel threads.

        Returns:
            Self for method chaining.
        """
        ...

    def build(self) -> "Database":
        """
        Build and return the configured database.

        Returns:
            The opened Database instance.
        """
        ...


class QueryBuilder:
    """Builder for parameterized queries."""

    def param(self, name: str, value: Any) -> "QueryBuilder":
        """
        Add a query parameter.

        Args:
            name: Parameter name (without $).
            value: Parameter value.

        Returns:
            Self for method chaining.
        """
        ...

    def params(self, params: dict[str, Any]) -> None:
        """
        Add multiple query parameters.

        Args:
            params: Dictionary of parameter names to values.
        """
        ...

    def fetch_all(self) -> list[dict[str, Any]]:
        """
        Execute the query and return all results.

        Returns:
            List of result rows as dictionaries.
        """
        ...


class SchemaBuilder:
    """Builder for defining database schema."""

    def label(self, name: str) -> "LabelBuilder":
        """
        Start defining a new label.

        Args:
            name: Label name.

        Returns:
            A LabelBuilder for further configuration.
        """
        ...

    def edge_type(
        self, name: str, from_labels: list[str], to_labels: list[str]
    ) -> "EdgeTypeBuilder":
        """
        Start defining a new edge type.

        Args:
            name: Edge type name.
            from_labels: Labels that can be source vertices.
            to_labels: Labels that can be target vertices.

        Returns:
            An EdgeTypeBuilder for further configuration.
        """
        ...

    def apply(self) -> None:
        """Apply all pending schema changes."""
        ...


class LabelBuilder:
    """Builder for defining a vertex label."""

    def document(self) -> "LabelBuilder":
        """
        Mark this as a document label.

        Returns:
            Self for method chaining.
        """
        ...

    def property(self, name: str, data_type: str) -> "LabelBuilder":
        """
        Add a required property.

        Args:
            name: Property name.
            data_type: Data type (string, int, float, bool, vector:N).

        Returns:
            Self for method chaining.
        """
        ...

    def property_nullable(self, name: str, data_type: str) -> "LabelBuilder":
        """
        Add an optional (nullable) property.

        Args:
            name: Property name.
            data_type: Data type.

        Returns:
            Self for method chaining.
        """
        ...

    def vector(self, name: str, dimensions: int) -> "LabelBuilder":
        """
        Add a vector property.

        Args:
            name: Property name.
            dimensions: Vector dimensions.

        Returns:
            Self for method chaining.
        """
        ...

    def index(self, property: str, index_type: str) -> "LabelBuilder":
        """
        Add an index on a property.

        Args:
            property: Property name.
            index_type: Index type (btree, hash, vector).

        Returns:
            Self for method chaining.
        """
        ...

    def done(self) -> SchemaBuilder:
        """
        Finish this label and return to SchemaBuilder.

        Returns:
            The parent SchemaBuilder.
        """
        ...

    def apply(self) -> None:
        """Apply this label immediately."""
        ...


class EdgeTypeBuilder:
    """Builder for defining an edge type."""

    def property(self, name: str, data_type: str) -> "EdgeTypeBuilder":
        """
        Add a required property.

        Args:
            name: Property name.
            data_type: Data type.

        Returns:
            Self for method chaining.
        """
        ...

    def property_nullable(self, name: str, data_type: str) -> "EdgeTypeBuilder":
        """
        Add an optional property.

        Args:
            name: Property name.
            data_type: Data type.

        Returns:
            Self for method chaining.
        """
        ...

    def done(self) -> SchemaBuilder:
        """
        Finish this edge type and return to SchemaBuilder.

        Returns:
            The parent SchemaBuilder.
        """
        ...

    def apply(self) -> None:
        """Apply this edge type immediately."""
        ...


class SessionBuilder:
    """Builder for creating query sessions."""

    def set(self, key: str, value: Any) -> "SessionBuilder":
        """
        Set a session variable.

        Args:
            key: Variable name.
            value: Variable value.

        Returns:
            Self for method chaining.
        """
        ...

    def build(self) -> "Session":
        """
        Build and return the session.

        Returns:
            The configured Session.
        """
        ...


class Session:
    """A query session with scoped variables."""

    def query(self, cypher: str) -> list[dict[str, Any]]:
        """
        Execute a read query.

        Args:
            cypher: Cypher query string.

        Returns:
            List of result rows.
        """
        ...

    def execute(self, cypher: str) -> int:
        """
        Execute a write query.

        Args:
            cypher: Cypher query string.

        Returns:
            Number of affected rows.
        """
        ...

    def get(self, key: str) -> Optional[Any]:
        """
        Get a session variable.

        Args:
            key: Variable name.

        Returns:
            Variable value or None if not set.
        """
        ...


class BulkWriterBuilder:
    """Builder for bulk data loading."""

    def defer_vector_indexes(self, defer: bool) -> "BulkWriterBuilder":
        """
        Defer vector index building until commit.

        Args:
            defer: Whether to defer.

        Returns:
            Self for method chaining.
        """
        ...

    def defer_scalar_indexes(self, defer: bool) -> "BulkWriterBuilder":
        """
        Defer scalar index building until commit.

        Args:
            defer: Whether to defer.

        Returns:
            Self for method chaining.
        """
        ...

    def batch_size(self, size: int) -> "BulkWriterBuilder":
        """
        Set the batch size for writes.

        Args:
            size: Batch size.

        Returns:
            Self for method chaining.
        """
        ...

    def async_indexes(self, enabled: bool) -> "BulkWriterBuilder":
        """
        Enable asynchronous index building.

        Args:
            enabled: Whether to enable async indexes.

        Returns:
            Self for method chaining.
        """
        ...

    def build(self) -> "BulkWriter":
        """
        Build and return the bulk writer.

        Returns:
            The configured BulkWriter.
        """
        ...


class BulkWriter:
    """High-performance bulk data loader."""

    def insert_vertices(
        self, label: str, vertices: list[dict[str, Any]]
    ) -> list[int]:
        """
        Insert multiple vertices.

        Args:
            label: Vertex label.
            vertices: List of property dictionaries.

        Returns:
            List of assigned vertex IDs.
        """
        ...

    def insert_edges(
        self,
        edge_type: str,
        edges: list[tuple[int, int, dict[str, Any]]],
    ) -> None:
        """
        Insert multiple edges.

        Args:
            edge_type: Edge type name.
            edges: List of (src_vid, dst_vid, properties) tuples.
        """
        ...

    def commit(self) -> BulkStats:
        """
        Commit all pending writes and build indexes.

        Returns:
            Statistics about the bulk load.
        """
        ...

    def abort(self) -> None:
        """Abort the bulk write operation."""
        ...


class VectorSearchBuilder:
    """Builder for vector similarity search."""

    def k(self, k: int) -> "VectorSearchBuilder":
        """
        Set the number of results to return.

        Args:
            k: Number of nearest neighbors.

        Returns:
            Self for method chaining.
        """
        ...

    def threshold(self, threshold: float) -> "VectorSearchBuilder":
        """
        Set the maximum distance threshold.

        Args:
            threshold: Maximum distance for results.

        Returns:
            Self for method chaining.
        """
        ...

    def search(self) -> list[VectorMatch]:
        """
        Execute the search.

        Returns:
            List of VectorMatch results.
        """
        ...

    def fetch_nodes(self) -> list[tuple[dict[str, Any], float]]:
        """
        Execute search and fetch full node data.

        Returns:
            List of (node_properties, distance) tuples.
        """
        ...


# =============================================================================
# Transaction
# =============================================================================

class Transaction:
    """A database transaction."""

    def query(
        self, cypher: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """
        Execute a query within this transaction.

        Args:
            cypher: Cypher query string.
            params: Optional query parameters.

        Returns:
            List of result rows.
        """
        ...

    def commit(self) -> None:
        """Commit the transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the transaction."""
        ...


# =============================================================================
# Database
# =============================================================================

class Database:
    """
    The main Uni database interface.

    Example:
        >>> db = Database("/path/to/db")
        >>> db.create_label("Person")
        >>> db.query("CREATE (n:Person {name: 'Alice'})")
        >>> results = db.query("MATCH (n:Person) RETURN n.name AS name")
    """

    def __init__(self, path: str) -> None:
        """
        Open or create a database at the given path.

        Args:
            path: Path to the database directory.
        """
        ...

    # -------------------------------------------------------------------------
    # Query Methods
    # -------------------------------------------------------------------------

    def query(
        self, cypher: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """
        Execute a Cypher query.

        Args:
            cypher: Cypher query string.
            params: Optional query parameters.

        Returns:
            List of result rows as dictionaries.

        Example:
            >>> results = db.query("MATCH (n:Person) WHERE n.age > $min RETURN n.name AS name", {"min": 21})
        """
        ...

    def execute(self, cypher: str) -> int:
        """
        Execute a write query and return affected row count.

        Args:
            cypher: Cypher query string.

        Returns:
            Number of affected rows.
        """
        ...

    def query_with(self, cypher: str) -> QueryBuilder:
        """
        Create a parameterized query builder.

        Args:
            cypher: Cypher query string with $parameters.

        Returns:
            A QueryBuilder for adding parameters.

        Example:
            >>> builder = db.query_with("MATCH (n:Person) WHERE n.name = $name RETURN n")
            >>> builder.param("name", "Alice")
            >>> results = builder.fetch_all()
        """
        ...

    def explain(self, cypher: str) -> dict[str, Any]:
        """
        Get query execution plan without running.

        Args:
            cypher: Cypher query string.

        Returns:
            Dictionary with plan_text, cost_estimates, index_usage.
        """
        ...

    def profile(self, cypher: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Execute query and return results with execution profile.

        Args:
            cypher: Cypher query string.

        Returns:
            Tuple of (results, profile_stats).
        """
        ...

    # -------------------------------------------------------------------------
    # Transaction Methods
    # -------------------------------------------------------------------------

    def begin(self) -> Transaction:
        """
        Begin a new transaction.

        Returns:
            A Transaction object.
        """
        ...

    # -------------------------------------------------------------------------
    # Session Methods
    # -------------------------------------------------------------------------

    def session(self) -> SessionBuilder:
        """
        Create a session builder.

        Returns:
            A SessionBuilder for configuration.
        """
        ...

    # -------------------------------------------------------------------------
    # Schema Methods
    # -------------------------------------------------------------------------

    def schema(self) -> SchemaBuilder:
        """
        Create a schema builder.

        Returns:
            A SchemaBuilder for defining schema.
        """
        ...

    def create_label(self, name: str) -> None:
        """
        Create a new vertex label.

        Args:
            name: Label name.
        """
        ...

    def create_edge_type(
        self, name: str, from_labels: list[str], to_labels: list[str]
    ) -> None:
        """
        Create a new edge type.

        Args:
            name: Edge type name.
            from_labels: Valid source labels.
            to_labels: Valid target labels.
        """
        ...

    def add_property(
        self, label_or_type: str, name: str, data_type: str, nullable: bool
    ) -> None:
        """
        Add a property to a label or edge type.

        Args:
            label_or_type: Label or edge type name.
            name: Property name.
            data_type: Data type (string, int, float, bool, vector:N).
            nullable: Whether null values are allowed.
        """
        ...

    def label_exists(self, name: str) -> bool:
        """Check if a label exists."""
        ...

    def edge_type_exists(self, name: str) -> bool:
        """Check if an edge type exists."""
        ...

    def list_labels(self) -> list[str]:
        """List all label names."""
        ...

    def list_edge_types(self) -> list[str]:
        """List all edge type names."""
        ...

    def get_label_info(self, name: str) -> Optional[LabelInfo]:
        """Get detailed information about a label."""
        ...

    # -------------------------------------------------------------------------
    # Index Methods
    # -------------------------------------------------------------------------

    def create_btree_index(self, label: str, property: str) -> None:
        """Create a B-tree index."""
        ...

    def create_hash_index(self, label: str, property: str) -> None:
        """Create a hash index."""
        ...

    def create_vector_index(self, label: str, property: str, metric: str) -> None:
        """
        Create a vector similarity index.

        Args:
            label: Label name.
            property: Vector property name.
            metric: Distance metric (l2, cosine, dot).
        """
        ...

    def rebuild_indexes(self, label: str, async_: bool = False) -> Optional[str]:
        """Rebuild all indexes for a label."""
        ...

    def index_rebuild_status(self) -> list[IndexRebuildTask]:
        """Get status of all index rebuild tasks."""
        ...

    def is_index_building(self, label: str) -> bool:
        """Check if indexes are being built for a label."""
        ...

    # -------------------------------------------------------------------------
    # Vector Search Methods
    # -------------------------------------------------------------------------

    def vector_search(
        self, label: str, property: str, query_vec: list[float], k: int
    ) -> list[VectorMatch]:
        """
        Perform vector similarity search.

        Args:
            label: Label to search.
            property: Vector property name.
            query_vec: Query vector.
            k: Number of results.

        Returns:
            List of VectorMatch results.
        """
        ...

    def vector_search_with(
        self, label: str, property: str, query_vec: list[float]
    ) -> VectorSearchBuilder:
        """
        Create a vector search builder.

        Args:
            label: Label to search.
            property: Vector property name.
            query_vec: Query vector.

        Returns:
            A VectorSearchBuilder for configuration.
        """
        ...

    # -------------------------------------------------------------------------
    # Bulk Loading Methods
    # -------------------------------------------------------------------------

    def bulk_writer(self) -> BulkWriterBuilder:
        """
        Create a bulk writer builder.

        Returns:
            A BulkWriterBuilder for configuration.
        """
        ...

    def bulk_insert_vertices(
        self, label: str, vertices: list[dict[str, Any]]
    ) -> list[int]:
        """Insert vertices in bulk."""
        ...

    def bulk_insert_edges(
        self,
        edge_type: str,
        edges: list[tuple[int, int, dict[str, Any]]],
    ) -> None:
        """Insert edges in bulk."""
        ...

    # -------------------------------------------------------------------------
    # Snapshot Methods
    # -------------------------------------------------------------------------

    def create_snapshot(self, name: Optional[str] = None) -> str:
        """Create a snapshot and return its ID."""
        ...

    def list_snapshots(self) -> list[SnapshotInfo]:
        """List all available snapshots."""
        ...

    def restore_snapshot(self, snapshot_id: str) -> None:
        """Restore the database to a snapshot."""
        ...

    def at_snapshot(self, snapshot_id: str) -> "Database":
        """Open a read-only view at a snapshot."""
        ...

    # -------------------------------------------------------------------------
    # Compaction Methods
    # -------------------------------------------------------------------------

    def compact_label(self, label: str) -> CompactionStats:
        """Compact storage for a label."""
        ...

    def compact_edge_type(self, edge_type: str) -> CompactionStats:
        """Compact storage for an edge type."""
        ...

    def wait_for_compaction(self) -> None:
        """Wait for all compaction to complete."""
        ...

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def flush(self) -> None:
        """Flush all pending writes to storage."""
        ...

    def uid(self, label: str, vid: int) -> Optional[str]:
        """Get the UID for a vertex ID."""
        ...
