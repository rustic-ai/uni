# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for UniNode and UniEdge models."""

from datetime import date, datetime

import pytest
from pydantic import ValidationError as PydanticValidationError

from uni_pydantic import (
    Field,
    Relationship,
    UniEdge,
    UniNode,
    Vector,
    after_create,
    before_create,
)


class TestUniNode:
    """Tests for UniNode base class."""

    def test_basic_model(self):
        """Test basic model definition."""

        class Person(UniNode):
            name: str
            age: int

        person = Person(name="Alice", age=30)
        assert person.name == "Alice"
        assert person.age == 30
        assert person.__label__ == "Person"

    def test_custom_label(self):
        """Test custom label override."""

        class User(UniNode):
            __label__ = "AppUser"
            name: str

        assert User.__label__ == "AppUser"

    def test_optional_fields(self):
        """Test optional (nullable) fields."""

        class Person(UniNode):
            name: str
            nickname: str | None = None

        person = Person(name="Alice")
        assert person.name == "Alice"
        assert person.nickname is None

    def test_default_values(self):
        """Test fields with default values."""

        class Person(UniNode):
            name: str
            active: bool = True
            views: int = 0

        person = Person(name="Alice")
        assert person.active is True
        assert person.views == 0

    def test_default_factory(self):
        """Test fields with default_factory."""

        class Person(UniNode):
            name: str
            tags: list[str] = Field(default_factory=list)

        person = Person(name="Alice")
        assert person.tags == []
        person.tags.append("test")
        assert person.tags == ["test"]

    def test_vector_field(self):
        """Test Vector field."""

        class Document(UniNode):
            title: str
            embedding: Vector[128]

        vec = [0.1] * 128
        doc = Document(title="Test", embedding=vec)
        assert doc.embedding == vec

    def test_property_fields(self):
        """Test get_property_fields excludes relationships."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")

        fields = Person.get_property_fields()
        assert "name" in fields
        assert "friends" not in fields

    def test_relationship_fields(self):
        """Test get_relationship_fields."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")

        rels = Person.get_relationship_fields()
        assert "friends" in rels
        assert rels["friends"].edge_type == "FRIEND_OF"

    def test_to_properties(self):
        """Test converting model to property dict."""

        class Person(UniNode):
            name: str
            age: int | None = None

        person = Person(name="Alice", age=30)
        props = person.to_properties()
        assert props == {"name": "Alice", "age": 30}

    def test_to_properties_excludes_none(self):
        """Test that to_properties excludes None values."""

        class Person(UniNode):
            name: str
            age: int | None = None

        person = Person(name="Alice")
        props = person.to_properties()
        assert props == {"name": "Alice"}

    def test_from_properties(self):
        """Test creating model from property dict."""

        class Person(UniNode):
            name: str
            age: int

        person = Person.from_properties({"name": "Alice", "age": 30})
        assert person.name == "Alice"
        assert person.age == 30

    def test_is_persisted(self):
        """Test is_persisted property."""

        class Person(UniNode):
            name: str

        person = Person(name="Alice")
        assert person.is_persisted is False
        assert person.vid is None

    def test_dirty_tracking(self):
        """Test dirty field tracking."""

        class Person(UniNode):
            name: str
            age: int

        person = Person(name="Alice", age=30)
        person._mark_clean()
        assert person.is_dirty is False

        person.age = 31
        assert person.is_dirty is True
        assert "age" in person._dirty

    def test_validation(self):
        """Test Pydantic validation."""

        class Person(UniNode):
            name: str
            age: int

        with pytest.raises(PydanticValidationError):
            Person(name="Alice", age="not_an_int")  # type: ignore


class TestUniEdge:
    """Tests for UniEdge base class."""

    def test_basic_edge(self):
        """Test basic edge definition."""

        class Person(UniNode):
            name: str

        class FriendshipEdge(UniEdge):
            __edge_type__ = "FRIEND_OF"
            __from__ = Person
            __to__ = Person

            since: date
            strength: float = 1.0

        edge = FriendshipEdge(since=date(2020, 1, 1))
        assert edge.since == date(2020, 1, 1)
        assert edge.strength == 1.0
        assert edge.__edge_type__ == "FRIEND_OF"

    def test_edge_from_to(self):
        """Test edge from/to labels."""

        class Person(UniNode):
            __label__ = "Person"
            name: str

        class Company(UniNode):
            __label__ = "Company"
            name: str

        class WorksAtEdge(UniEdge):
            __edge_type__ = "WORKS_AT"
            __from__ = Person
            __to__ = Company

            role: str

        assert WorksAtEdge.get_from_labels() == ["Person"]
        assert WorksAtEdge.get_to_labels() == ["Company"]

    def test_edge_to_properties(self):
        """Test edge to_properties."""

        class FriendshipEdge(UniEdge):
            since: date
            strength: float = 1.0

        edge = FriendshipEdge(since=date(2020, 1, 1), strength=0.8)
        props = edge.to_properties()
        assert props == {"since": date(2020, 1, 1), "strength": 0.8}


class TestLifecycleHooks:
    """Tests for lifecycle hooks."""

    def test_before_create_hook(self):
        """Test before_create hook."""
        called = []

        class Person(UniNode):
            name: str
            created_at: datetime | None = None

            @before_create
            def set_created(self):
                called.append("before_create")
                self.created_at = datetime(2020, 1, 1)

        person = Person(name="Alice")
        # Hook marker should be set
        assert hasattr(Person.set_created, "_uni_before_create")

    def test_after_create_hook(self):
        """Test after_create hook."""
        called = []

        class Person(UniNode):
            name: str

            @after_create
            def log_created(self):
                called.append(f"created:{self.name}")

        person = Person(name="Alice")
        assert hasattr(Person.log_created, "_uni_after_create")


class TestFieldConfiguration:
    """Tests for Field configuration."""

    def test_index_field(self):
        """Test indexed field."""

        class Person(UniNode):
            email: str = Field(index="btree")

        info = Person.model_fields["email"]
        assert info.json_schema_extra is not None
        config = info.json_schema_extra.get("uni_config")
        assert config.index == "btree"

    def test_unique_field(self):
        """Test unique field."""

        class Person(UniNode):
            email: str = Field(unique=True)

        info = Person.model_fields["email"]
        config = info.json_schema_extra.get("uni_config")
        assert config.unique is True

    def test_fulltext_field(self):
        """Test fulltext indexed field."""

        class Article(UniNode):
            content: str = Field(index="fulltext", tokenizer="standard")

        info = Article.model_fields["content"]
        config = info.json_schema_extra.get("uni_config")
        assert config.index == "fulltext"
        assert config.tokenizer == "standard"

    def test_vector_metric(self):
        """Test vector field with metric."""

        class Document(UniNode):
            embedding: Vector[128] = Field(metric="cosine")

        info = Document.model_fields["embedding"]
        config = info.json_schema_extra.get("uni_config")
        assert config.metric == "cosine"


class TestRelationshipDeclaration:
    """Tests for Relationship declaration."""

    def test_basic_relationship(self):
        """Test basic relationship."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")

        rels = Person.get_relationship_fields()
        assert "friends" in rels
        assert rels["friends"].edge_type == "FRIEND_OF"
        assert rels["friends"].direction == "outgoing"

    def test_incoming_relationship(self):
        """Test incoming relationship."""

        class Person(UniNode):
            name: str
            followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

        rels = Person.get_relationship_fields()
        assert rels["followers"].direction == "incoming"

    def test_bidirectional_relationship(self):
        """Test bidirectional relationship."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

        rels = Person.get_relationship_fields()
        assert rels["friends"].direction == "both"

    def test_optional_single_relationship(self):
        """Test optional single relationship."""

        class Person(UniNode):
            name: str
            manager: "Person | None" = Relationship("REPORTS_TO")

        rels = Person.get_relationship_fields()
        assert "manager" in rels
