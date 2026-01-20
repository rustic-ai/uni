# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Session management for uni-pydantic OGM."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
)
from weakref import WeakValueDictionary

from .base import UniEdge, UniNode
from .exceptions import (
    NotPersisted,
    SessionError,
    TransactionError,
)
from .fields import RelationshipDescriptor
from .hooks import (
    _AFTER_CREATE,
    _AFTER_DELETE,
    _AFTER_LOAD,
    _AFTER_UPDATE,
    _BEFORE_CREATE,
    _BEFORE_DELETE,
    _BEFORE_LOAD,
    _BEFORE_UPDATE,
    run_class_hooks,
    run_hooks,
)
from .query import QueryBuilder
from .schema import SchemaGenerator

if TYPE_CHECKING:
    import uni_db

NodeT = TypeVar("NodeT", bound=UniNode)
EdgeT = TypeVar("EdgeT", bound=UniEdge)


class UniTransaction:
    """
    Transaction context for atomic operations.

    Provides commit/rollback semantics for a group of operations.

    Example:
        >>> with session.transaction() as tx:
        ...     alice = Person(name="Alice")
        ...     tx.add(alice)
        ...     # Auto-commits on success, rolls back on exception
    """

    def __init__(self, session: UniSession) -> None:
        self._session = session
        self._tx: uni_db.Transaction | None = None
        self._pending_nodes: list[UniNode] = []
        self._pending_edges: list[tuple[UniNode, str, UniNode, UniEdge | None]] = []
        self._committed = False
        self._rolled_back = False

    def __enter__(self) -> UniTransaction:
        self._tx = self._session._db.begin()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.rollback()
            return
        if not self._committed and not self._rolled_back:
            self.commit()

    def add(self, entity: UniNode) -> None:
        """Add a node to be created in this transaction."""
        self._pending_nodes.append(entity)

    def create_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: UniEdge | None = None,
        **kwargs: Any,
    ) -> None:
        """Create an edge between two nodes in this transaction."""
        if not source.is_persisted:
            raise NotPersisted(source)
        if not target.is_persisted:
            raise NotPersisted(target)
        self._pending_edges.append((source, edge_type, target, properties))

    def commit(self) -> None:
        """Commit the transaction."""
        if self._committed:
            raise TransactionError("Transaction already committed")
        if self._rolled_back:
            raise TransactionError("Transaction already rolled back")

        if self._tx is None:
            raise TransactionError("Transaction not started")

        try:
            # Create pending nodes
            for node in self._pending_nodes:
                self._session._create_node_in_tx(node, self._tx)

            # Create pending edges
            for source, edge_type, target, props in self._pending_edges:
                self._session._create_edge_in_tx(
                    source, edge_type, target, props, self._tx
                )

            self._tx.commit()
            self._committed = True

            # Mark nodes as clean
            for node in self._pending_nodes:
                node._mark_clean()

        except Exception as e:
            self.rollback()
            raise TransactionError(f"Commit failed: {e}") from e

    def rollback(self) -> None:
        """Rollback the transaction."""
        if self._rolled_back:
            return
        if self._tx is not None:
            self._tx.rollback()
        self._rolled_back = True
        self._pending_nodes.clear()
        self._pending_edges.clear()


class UniSession:
    """
    Session for interacting with the graph database using Pydantic models.

    The session manages model registration, schema synchronization,
    and provides CRUD operations and query building.

    Example:
        >>> from uni_db import Database
        >>> from uni_pydantic import UniSession
        >>>
        >>> db = Database("./my_graph")
        >>> session = UniSession(db)
        >>> session.register(Person, Company)
        >>> session.sync_schema()
        >>>
        >>> alice = Person(name="Alice", age=30)
        >>> session.add(alice)
        >>> session.commit()
    """

    def __init__(self, db: uni_db.Database) -> None:
        self._db = db
        self._schema_gen = SchemaGenerator()
        self._identity_map: WeakValueDictionary[tuple[str, int], UniNode] = WeakValueDictionary()
        self._pending_new: list[UniNode] = []
        self._pending_delete: list[UniNode] = []

    def register(self, *models: type[UniNode] | type[UniEdge]) -> None:
        """
        Register model classes with the session.

        Registered models can be used for schema generation and queries.

        Args:
            *models: UniNode or UniEdge subclasses to register.

        Example:
            >>> session.register(Person, Company, FriendshipEdge)
        """
        self._schema_gen.register(*models)

    def sync_schema(self) -> None:
        """
        Synchronize database schema with registered models.

        Creates labels, edge types, properties, and indexes as needed.
        This is additive-only; it won't remove existing schema elements.

        Example:
            >>> session.register(Person)
            >>> session.sync_schema()  # Creates Person label with properties
        """
        self._schema_gen.apply_to_database(self._db)

    def query(self, model: type[NodeT]) -> QueryBuilder[NodeT]:
        """
        Create a query builder for the given model.

        Args:
            model: The UniNode subclass to query.

        Returns:
            A QueryBuilder for constructing queries.

        Example:
            >>> adults = session.query(Person).filter(Person.age >= 18).all()
        """
        return QueryBuilder(self, model)

    def add(self, entity: UniNode) -> None:
        """
        Add a new entity to be persisted.

        The entity will be inserted on the next commit().

        Args:
            entity: The node to add.

        Example:
            >>> alice = Person(name="Alice")
            >>> session.add(alice)
            >>> session.commit()
        """
        if entity.is_persisted:
            raise SessionError(f"Entity {entity!r} is already persisted")
        entity._session = self
        self._pending_new.append(entity)

    def add_all(self, entities: Sequence[UniNode]) -> None:
        """
        Add multiple entities to be persisted.

        Args:
            entities: Sequence of nodes to add.

        Example:
            >>> people = [Person(name=f"User{i}") for i in range(100)]
            >>> session.add_all(people)
            >>> session.commit()
        """
        for entity in entities:
            self.add(entity)

    def delete(self, entity: UniNode) -> None:
        """
        Mark an entity for deletion.

        The entity will be deleted on the next commit().

        Args:
            entity: The node to delete.

        Example:
            >>> session.delete(alice)
            >>> session.commit()
        """
        if not entity.is_persisted:
            raise NotPersisted(entity)
        self._pending_delete.append(entity)

    def get(
        self,
        model: type[NodeT],
        vid: int | None = None,
        uid: str | None = None,
        **kwargs: Any,
    ) -> NodeT | None:
        """
        Get an entity by ID or unique properties.

        Args:
            model: The model type to retrieve.
            vid: Vertex ID to look up.
            uid: Unique ID to look up.
            **kwargs: Property equality filters.

        Returns:
            The model instance or None if not found.

        Example:
            >>> person = session.get(Person, vid=12345)
            >>> person = session.get(Person, email="alice@example.com")
        """
        # Check identity map first
        if vid is not None:
            cached = self._identity_map.get((model.__label__, vid))
            if cached is not None:
                return cached  # type: ignore[return-value]

        # Build query
        label = model.__label__
        params: dict[str, Any] = {}

        if vid is not None:
            cypher = f"MATCH (n:{label}) WHERE id(n) = $vid RETURN n"
            params["vid"] = vid
        elif uid is not None:
            cypher = f"MATCH (n:{label}) WHERE n._uid = $uid RETURN n"
            params["uid"] = uid
        elif kwargs:
            conditions = [f"n.{k} = ${k}" for k in kwargs]
            cypher = f"MATCH (n:{label}) WHERE {' AND '.join(conditions)} RETURN n LIMIT 1"
            params.update(kwargs)
        else:
            raise ValueError("Must provide vid, uid, or property filters")

        results = self._db.query(cypher, params)
        if not results:
            return None

        return self._result_to_model(results[0]["n"], model)

    def refresh(self, entity: UniNode) -> None:
        """
        Refresh an entity's properties from the database.

        Args:
            entity: The entity to refresh.
        """
        if not entity.is_persisted:
            raise NotPersisted(entity)

        label = entity.__class__.__label__
        cypher = f"MATCH (n:{label}) WHERE id(n) = $vid RETURN n"
        results = self._db.query(cypher, {"vid": entity._vid})

        if not results:
            raise SessionError(f"Entity with vid={entity._vid} no longer exists")

        # Update properties
        props = results[0]["n"]
        for field_name in entity.get_property_fields():
            if field_name in props:
                setattr(entity, field_name, props[field_name])

        entity._mark_clean()

    def commit(self) -> None:
        """
        Commit all pending changes to the database.

        This persists new entities, updates dirty entities,
        and deletes marked entities.

        Example:
            >>> session.add(alice)
            >>> alice.age = 31
            >>> session.commit()
        """
        # Insert new entities
        for entity in self._pending_new:
            self._create_node(entity)

        # Update dirty entities in identity map
        for (label, vid), entity in list(self._identity_map.items()):
            if entity.is_dirty and entity.is_persisted:
                self._update_node(entity)

        # Delete marked entities
        for entity in self._pending_delete:
            self._delete_node(entity)

        # Flush to storage
        self._db.flush()

        # Clear pending lists
        self._pending_new.clear()
        self._pending_delete.clear()

    def rollback(self) -> None:
        """
        Discard all pending changes.

        Clears new entities and refreshes dirty tracked entities.
        """
        # Clear pending new
        for entity in self._pending_new:
            entity._session = None
        self._pending_new.clear()

        # Clear pending deletes
        self._pending_delete.clear()

        # Refresh dirty tracked entities
        for entity in list(self._identity_map.values()):
            if entity.is_dirty:
                self.refresh(entity)

    @contextmanager
    def transaction(self) -> Iterator[UniTransaction]:
        """
        Create a transaction context.

        Example:
            >>> with session.transaction() as tx:
            ...     tx.add(Person(name="Alice"))
            ...     tx.add(Person(name="Bob"))
        """
        tx = UniTransaction(self)
        with tx:
            yield tx

    def begin(self) -> UniTransaction:
        """
        Begin a new transaction.

        Returns:
            A UniTransaction for manual commit/rollback.

        Example:
            >>> tx = session.begin()
            >>> try:
            ...     tx.add(Person(name="Alice"))
            ...     tx.commit()
            ... except:
            ...     tx.rollback()
        """
        tx = UniTransaction(self)
        tx._tx = self._db.begin()
        return tx

    def cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        result_type: type[NodeT] | None = None,
    ) -> list[NodeT] | list[dict[str, Any]]:
        """
        Execute a raw Cypher query.

        Args:
            query: Cypher query string.
            params: Query parameters.
            result_type: Optional model type for result mapping.

        Returns:
            List of results (model instances if result_type provided).

        Example:
            >>> results = session.cypher(
            ...     "MATCH (p:Person) WHERE p.age > $age RETURN p",
            ...     params={"age": 18},
            ...     result_type=Person
            ... )
        """
        results = self._db.query(query, params)

        if result_type is None:
            return cast(list[dict[str, Any]], results)

        # Map results to model instances
        mapped = []
        for row in results:
            # Try to find node data in the row
            for key, value in row.items():
                if isinstance(value, dict) and "_vid" in str(value):
                    instance = self._result_to_model(value, result_type)
                    if instance:
                        mapped.append(instance)
                        break
            else:
                # Try the first column
                first_value = next(iter(row.values()), None)
                if isinstance(first_value, dict):
                    instance = self._result_to_model(first_value, result_type)
                    if instance:
                        mapped.append(instance)

        return mapped

    def create_edge(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: dict[str, Any] | UniEdge | None = None,
    ) -> None:
        """
        Create an edge between two nodes.

        Args:
            source: Source node.
            edge_type: Edge type name.
            target: Target node.
            properties: Optional edge properties.

        Example:
            >>> session.create_edge(alice, "FRIEND_OF", bob, {"since": date.today()})
        """
        if not source.is_persisted:
            raise NotPersisted(source)
        if not target.is_persisted:
            raise NotPersisted(target)

        src_vid = source._vid
        dst_vid = target._vid

        props: dict[str, Any] = {}
        if isinstance(properties, UniEdge):
            props = properties.to_properties()
        elif properties:
            props = properties

        # Build CREATE edge query
        props_str = ", ".join(f"{k}: ${k}" for k in props)
        if props_str:
            cypher = f"MATCH (a), (b) WHERE id(a) = $src AND id(b) = $dst CREATE (a)-[r:{edge_type} {{{props_str}}}]->(b)"
        else:
            cypher = f"MATCH (a), (b) WHERE id(a) = $src AND id(b) = $dst CREATE (a)-[r:{edge_type}]->(b)"

        params = {"src": src_vid, "dst": dst_vid, **props}
        self._db.query(cypher, params)

    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------

    def _create_node(self, entity: UniNode) -> None:
        """Create a node in the database."""
        # Run before_create hooks
        run_hooks(entity, _BEFORE_CREATE)

        label = entity.__class__.__label__
        props = entity.to_properties()

        # Build CREATE query
        props_str = ", ".join(f"{k}: ${k}" for k in props)
        cypher = f"CREATE (n:{label} {{{props_str}}}) RETURN id(n) as vid"

        results = self._db.query(cypher, props)
        if results:
            vid = results[0]["vid"]
            entity._attach_session(self, vid)

            # Add to identity map
            self._identity_map[(label, vid)] = entity

        # Run after_create hooks
        run_hooks(entity, _AFTER_CREATE)
        entity._mark_clean()

    def _create_node_in_tx(self, entity: UniNode, tx: uni_db.Transaction) -> None:
        """Create a node within a transaction."""
        run_hooks(entity, _BEFORE_CREATE)

        label = entity.__class__.__label__
        props = entity.to_properties()

        props_str = ", ".join(f"{k}: ${k}" for k in props)
        cypher = f"CREATE (n:{label} {{{props_str}}}) RETURN id(n) as vid"

        results = tx.query(cypher, props)
        if results:
            vid = results[0]["vid"]
            entity._attach_session(self, vid)
            self._identity_map[(label, vid)] = entity

        run_hooks(entity, _AFTER_CREATE)

    def _create_edge_in_tx(
        self,
        source: UniNode,
        edge_type: str,
        target: UniNode,
        properties: UniEdge | None,
        tx: uni_db.Transaction,
    ) -> None:
        """Create an edge within a transaction."""
        props = properties.to_properties() if properties else {}

        props_str = ", ".join(f"{k}: ${k}" for k in props)
        if props_str:
            cypher = f"MATCH (a), (b) WHERE id(a) = $src AND id(b) = $dst CREATE (a)-[:{edge_type} {{{props_str}}}]->(b)"
        else:
            cypher = f"MATCH (a), (b) WHERE id(a) = $src AND id(b) = $dst CREATE (a)-[:{edge_type}]->(b)"

        params = {"src": source._vid, "dst": target._vid, **props}
        tx.query(cypher, params)

    def _update_node(self, entity: UniNode) -> None:
        """Update a node in the database."""
        run_hooks(entity, _BEFORE_UPDATE)

        label = entity.__class__.__label__
        dirty_props = {name: getattr(entity, name) for name in entity._dirty}

        if not dirty_props:
            return

        set_clause = ", ".join(f"n.{k} = ${k}" for k in dirty_props)
        cypher = f"MATCH (n:{label}) WHERE id(n) = $vid SET {set_clause}"
        params = {"vid": entity._vid, **dirty_props}

        self._db.query(cypher, params)

        run_hooks(entity, _AFTER_UPDATE)
        entity._mark_clean()

    def _delete_node(self, entity: UniNode) -> None:
        """Delete a node from the database."""
        run_hooks(entity, _BEFORE_DELETE)

        label = entity.__class__.__label__
        vid = entity._vid

        # DETACH DELETE to also remove connected edges
        cypher = f"MATCH (n:{label}) WHERE id(n) = $vid DETACH DELETE n"
        self._db.execute(cypher)

        # Remove from identity map
        if vid is not None and (label, vid) in self._identity_map:
            del self._identity_map[(label, vid)]

        # Clear entity IDs
        entity._vid = None
        entity._uid = None
        entity._session = None

        run_hooks(entity, _AFTER_DELETE)

    def _result_to_model(
        self,
        data: dict[str, Any],
        model: type[NodeT],
    ) -> NodeT | None:
        """Convert a query result row to a model instance."""
        if not data:
            return None

        # Run before_load hooks
        data = run_class_hooks(model, _BEFORE_LOAD, data) or data

        # Extract vid if present
        vid = data.pop("_vid", None) or data.pop("vid", None)

        try:
            instance = cast(NodeT, model.from_properties(
                data,
                vid=vid,
                session=self,
            ))
        except Exception:
            # If validation fails, return None
            return None

        # Add to identity map if we have a vid
        if vid is not None:
            existing = self._identity_map.get((model.__label__, vid))
            if existing is not None:
                return cast(NodeT, existing)
            self._identity_map[(model.__label__, vid)] = instance

        # Run after_load hooks
        run_hooks(instance, _AFTER_LOAD)

        return instance

    def _load_relationship(
        self,
        entity: UniNode,
        descriptor: RelationshipDescriptor[Any],
    ) -> list[UniNode] | UniNode | None:
        """Load a relationship for an entity."""
        if not entity.is_persisted:
            raise NotPersisted(entity)

        config = descriptor.config
        label = entity.__class__.__label__

        # Build traversal query based on direction
        if config.direction == "outgoing":
            edge_pattern = f"-[:{config.edge_type}]->"
        elif config.direction == "incoming":
            edge_pattern = f"<-[:{config.edge_type}]-"
        else:  # both
            edge_pattern = f"-[:{config.edge_type}]-"

        cypher = f"MATCH (a:{label}){edge_pattern}(b) WHERE id(a) = $vid RETURN b"
        results = self._db.query(cypher, {"vid": entity._vid})

        # Map results to model instances
        # Note: We'd need to determine the target model type
        # For now, return raw data or try to infer from registered models
        nodes = []
        for row in results:
            node_data = row.get("b", {})
            # Try to find the model for this node
            node_label = node_data.get("_label")
            if node_label and node_label in self._schema_gen._node_models:
                model = self._schema_gen._node_models[node_label]
                instance = self._result_to_model(node_data, model)
                if instance:
                    nodes.append(instance)

        if not descriptor.is_list:
            return nodes[0] if nodes else None
        return nodes

    def _eager_load_relationships(
        self,
        entities: list[NodeT],
        relationships: list[str],
    ) -> None:
        """Eager load relationships for a list of entities."""
        if not entities:
            return

        model = type(entities[0])
        rel_configs = model.get_relationship_fields()

        for rel_name in relationships:
            if rel_name not in rel_configs:
                continue

            config = rel_configs[rel_name]
            label = model.__label__
            vids = [e._vid for e in entities if e._vid is not None]

            if not vids:
                continue

            # Build batch query
            if config.direction == "outgoing":
                edge_pattern = f"-[:{config.edge_type}]->"
            elif config.direction == "incoming":
                edge_pattern = f"<-[:{config.edge_type}]-"
            else:
                edge_pattern = f"-[:{config.edge_type}]-"

            cypher = f"MATCH (a:{label}){edge_pattern}(b) WHERE id(a) IN $vids RETURN id(a) as src_vid, b"
            results = self._db.query(cypher, {"vids": vids})

            # Group results by source vid
            by_source: dict[int, list[Any]] = {}
            for row in results:
                src_vid = row["src_vid"]
                if src_vid not in by_source:
                    by_source[src_vid] = []
                by_source[src_vid].append(row["b"])

            # Set cached values on entities
            for entity in entities:
                if entity._vid in by_source:
                    related = by_source[entity._vid]
                    # Cache on entity
                    cache_attr = f"_rel_cache_{rel_name}"
                    setattr(entity, cache_attr, related)
