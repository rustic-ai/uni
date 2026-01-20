# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Schema generation from Pydantic models to Uni database schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, get_type_hints

from .base import UniEdge, UniNode
from .exceptions import SchemaError
from .fields import get_field_config
from .types import get_vector_dimensions, is_optional, python_type_to_uni

if TYPE_CHECKING:
    import uni_db


@dataclass
class PropertySchema:
    """Schema for a single property."""

    name: str
    data_type: str
    nullable: bool = False
    index_type: str | None = None
    unique: bool = False
    tokenizer: str | None = None
    metric: str | None = None


@dataclass
class LabelSchema:
    """Schema for a vertex label."""

    name: str
    is_document: bool = False
    properties: dict[str, PropertySchema] = field(default_factory=dict)


@dataclass
class EdgeTypeSchema:
    """Schema for an edge type."""

    name: str
    from_labels: list[str] = field(default_factory=list)
    to_labels: list[str] = field(default_factory=list)
    properties: dict[str, PropertySchema] = field(default_factory=dict)


@dataclass
class DatabaseSchema:
    """Complete database schema generated from models."""

    labels: dict[str, LabelSchema] = field(default_factory=dict)
    edge_types: dict[str, EdgeTypeSchema] = field(default_factory=dict)


class SchemaGenerator:
    """Generates Uni database schema from registered models."""

    def __init__(self) -> None:
        self._node_models: dict[str, type[UniNode]] = {}
        self._edge_models: dict[str, type[UniEdge]] = {}
        self._schema: DatabaseSchema | None = None

    def register_node(self, model: type[UniNode]) -> None:
        """Register a node model for schema generation."""
        label = model.__label__
        if not label:
            raise SchemaError(f"Model {model.__name__} has no __label__", model)
        self._node_models[label] = model
        self._schema = None  # Invalidate cached schema

    def register_edge(self, model: type[UniEdge]) -> None:
        """Register an edge model for schema generation."""
        edge_type = model.__edge_type__
        if not edge_type:
            raise SchemaError(f"Model {model.__name__} has no __edge_type__", model)
        self._edge_models[edge_type] = model
        self._schema = None

    def register(self, *models: type[UniNode] | type[UniEdge]) -> None:
        """Register multiple models."""
        for model in models:
            if issubclass(model, UniEdge):
                self.register_edge(model)
            elif issubclass(model, UniNode):
                self.register_node(model)
            else:
                raise SchemaError(
                    f"Model {model.__name__} must be a subclass of UniNode or UniEdge"
                )

    def _generate_property_schema(
        self,
        model: type[UniNode] | type[UniEdge],
        field_name: str,
    ) -> PropertySchema:
        """Generate schema for a single property field."""
        field_info = model.model_fields[field_name]

        # Get type hints with forward refs resolved
        try:
            hints = get_type_hints(model)
            type_hint = hints.get(field_name, field_info.annotation)
        except Exception:
            type_hint = field_info.annotation

        # Check for nullability
        is_nullable, inner_type = is_optional(type_hint)

        # Get Uni data type
        data_type, nullable = python_type_to_uni(type_hint, nullable=is_nullable)

        # Check for vector dimensions
        vec_dims = get_vector_dimensions(inner_type if is_nullable else type_hint)
        if vec_dims:
            data_type = f"vector:{vec_dims}"

        # Get field config for index settings
        config = get_field_config(field_info)

        index_type = None
        unique = False
        tokenizer = None
        metric = None

        if config:
            index_type = config.index
            unique = config.unique
            tokenizer = config.tokenizer
            metric = config.metric

        # Auto-create vector index for Vector fields (regardless of Field config)
        if vec_dims and not index_type:
            index_type = "vector"

        return PropertySchema(
            name=field_name,
            data_type=data_type,
            nullable=nullable,
            index_type=index_type,
            unique=unique,
            tokenizer=tokenizer,
            metric=metric,
        )

    def _generate_label_schema(self, model: type[UniNode]) -> LabelSchema:
        """Generate schema for a node model."""
        label = model.__label__

        properties = {}
        for field_name in model.get_property_fields():
            prop_schema = self._generate_property_schema(model, field_name)
            properties[field_name] = prop_schema

        return LabelSchema(
            name=label,
            is_document=False,  # Can be extended later
            properties=properties,
        )

    def _generate_edge_type_schema(self, model: type[UniEdge]) -> EdgeTypeSchema:
        """Generate schema for an edge model."""
        edge_type = model.__edge_type__
        from_labels = model.get_from_labels()
        to_labels = model.get_to_labels()

        # If from/to not specified, allow any labels
        if not from_labels:
            from_labels = list(self._node_models.keys())
        if not to_labels:
            to_labels = list(self._node_models.keys())

        properties = {}
        for field_name in model.get_property_fields():
            prop_schema = self._generate_property_schema(model, field_name)
            properties[field_name] = prop_schema

        return EdgeTypeSchema(
            name=edge_type,
            from_labels=from_labels,
            to_labels=to_labels,
            properties=properties,
        )

    def generate(self) -> DatabaseSchema:
        """Generate the complete database schema."""
        if self._schema is not None:
            return self._schema

        schema = DatabaseSchema()

        # Generate label schemas
        for label, model in self._node_models.items():
            schema.labels[label] = self._generate_label_schema(model)

        # Generate edge type schemas
        edge_type_name: str
        edge_model: type[UniEdge]
        for edge_type_name, edge_model in self._edge_models.items():
            schema.edge_types[edge_type_name] = self._generate_edge_type_schema(edge_model)

        # Also generate labels from relationships in node models
        for model in self._node_models.values():
            for rel_name, rel_config in model.get_relationship_fields().items():
                edge_type = rel_config.edge_type
                if edge_type not in schema.edge_types:
                    # Create a minimal edge type schema
                    schema.edge_types[edge_type] = EdgeTypeSchema(
                        name=edge_type,
                        from_labels=list(self._node_models.keys()),
                        to_labels=list(self._node_models.keys()),
                    )

        self._schema = schema
        return schema

    def apply_to_database(self, db: uni_db.Database) -> None:
        """Apply the generated schema to a database."""
        schema = self.generate()

        # Create labels
        for label, label_schema in schema.labels.items():
            if not db.label_exists(label):
                db.create_label(label)

            # Add properties
            for prop_name, prop_schema in label_schema.properties.items():
                # Check if property already exists
                label_info = db.get_label_info(label)
                existing_props = {p.name for p in (label_info.properties if label_info else [])}

                if prop_name not in existing_props:
                    db.add_property(
                        label,
                        prop_name,
                        prop_schema.data_type,
                        prop_schema.nullable,
                    )

            # Create indexes
            for prop_name, prop_schema in label_schema.properties.items():
                if prop_schema.index_type:
                    self._create_index(db, label, prop_name, prop_schema)

        # Create edge types
        for edge_type, edge_schema in schema.edge_types.items():
            if not db.edge_type_exists(edge_type):
                db.create_edge_type(
                    edge_type,
                    edge_schema.from_labels,
                    edge_schema.to_labels,
                )

            # Add edge properties
            for prop_name, prop_schema in edge_schema.properties.items():
                # Edge property addition - simplified for now
                pass  # TODO: Add edge property support when available

    def _create_index(
        self,
        db: uni_db.Database,
        label: str,
        prop_name: str,
        prop_schema: PropertySchema,
    ) -> None:
        """Create an index on a property."""
        index_type = prop_schema.index_type
        if not index_type:
            return

        # Check if index already exists
        label_info = db.get_label_info(label)
        if label_info:
            existing_indexes = {
                (idx.properties[0] if idx.properties else None, idx.index_type)
                for idx in label_info.indexes
            }
            if (prop_name, index_type) in existing_indexes:
                return

        if index_type == "btree":
            db.create_btree_index(label, prop_name)
        elif index_type == "hash":
            db.create_hash_index(label, prop_name)
        elif index_type == "vector":
            metric = prop_schema.metric or "l2"
            db.create_vector_index(label, prop_name, metric)
        # Note: fulltext index would need different handling


def generate_schema(*models: type[UniNode] | type[UniEdge]) -> DatabaseSchema:
    """Generate a database schema from the given models."""
    generator = SchemaGenerator()
    generator.register(*models)
    return generator.generate()
