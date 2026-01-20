#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""
Demo script for uni-pydantic OGM.

This script demonstrates the key features of uni-pydantic:
- Model definition with UniNode and UniEdge
- Field configuration with indexes and constraints
- Relationships between nodes
- CRUD operations
- Query DSL
- Lifecycle hooks

Usage:
    cd bindings/python/uni-pydantic
    poetry install
    poetry run python examples/demo.py
"""

from datetime import date, datetime
import tempfile
import sys
from pathlib import Path

# Add parent directories to path for development
parent = Path(__file__).parent.parent
sys.path.insert(0, str(parent / "src"))
sys.path.insert(0, str(parent.parent))


def main():
    """Run the demo."""
    try:
        import uni_db
    except ImportError:
        print("Error: uni_db not available. Please ensure uni-db is installed.")
        print("Run: cd bindings/python && poetry run maturin develop")
        return 1

    from uni_pydantic import (
        UniNode,
        UniEdge,
        UniSession,
        Field,
        Relationship,
        Vector,
        before_create,
        after_create,
    )

    # =========================================================================
    # 1. Define Models
    # =========================================================================
    print("=" * 60)
    print("1. Defining Models")
    print("=" * 60)

    class Person(UniNode):
        """A person node in our social network."""

        __label__ = "Person"

        # Basic properties
        name: str
        age: int | None = None

        # Indexed and unique property
        email: str = Field(unique=True, index="btree")

        # Fulltext searchable
        bio: str | None = Field(default=None, index="fulltext")

        # Timestamps with hooks
        created_at: datetime | None = None
        updated_at: datetime | None = None

        # Relationships
        friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
        follows: list["Person"] = Relationship("FOLLOWS")
        followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")
        works_at: "Company | None" = Relationship("WORKS_AT")

        @before_create
        def set_created_at(self):
            self.created_at = datetime.now()

        @after_create
        def log_creation(self):
            print(f"  Created person: {self.name} (vid={self.vid})")

    class Company(UniNode):
        """A company node."""

        __label__ = "Company"

        name: str = Field(index="btree", unique=True)
        founded: date | None = None
        industry: str | None = None

        # Reverse relationship
        employees: list[Person] = Relationship("WORKS_AT", direction="incoming")

    class FriendshipEdge(UniEdge):
        """Edge representing friendship between people."""

        __edge_type__ = "FRIEND_OF"
        __from__ = Person
        __to__ = Person

        since: date
        strength: float = 1.0

    print("Models defined:")
    print(f"  - Person (label: {Person.__label__})")
    print(f"  - Company (label: {Company.__label__})")
    print(f"  - FriendshipEdge (type: {FriendshipEdge.__edge_type__})")
    print()

    # =========================================================================
    # 2. Create Database and Session
    # =========================================================================
    print("=" * 60)
    print("2. Creating Database and Session")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"  Database path: {tmpdir}")

        # Create database
        db = uni_db.DatabaseBuilder.open(tmpdir).build()

        # Create session and register models
        session = UniSession(db)
        session.register(Person, Company, FriendshipEdge)

        print("  Session created and models registered")
        print()

        # =====================================================================
        # 3. Sync Schema
        # =====================================================================
        print("=" * 60)
        print("3. Syncing Schema")
        print("=" * 60)

        session.sync_schema()
        print("  Schema synchronized!")
        print(f"  Labels: {db.list_labels()}")
        print(f"  Edge types: {db.list_edge_types()}")
        print()

        # =====================================================================
        # 4. Create Entities
        # =====================================================================
        print("=" * 60)
        print("4. Creating Entities")
        print("=" * 60)

        # Create people
        alice = Person(
            name="Alice",
            age=30,
            email="alice@example.com",
            bio="Software engineer who loves graphs",
        )
        bob = Person(
            name="Bob",
            age=28,
            email="bob@example.com",
            bio="Data scientist and ML enthusiast",
        )
        charlie = Person(
            name="Charlie",
            age=35,
            email="charlie@example.com",
            bio="Product manager and startup founder",
        )

        # Create companies
        acme = Company(name="Acme Corp", founded=date(2010, 1, 1), industry="Technology")
        startup = Company(name="Cool Startup", founded=date(2020, 6, 15), industry="AI")

        # Add to session
        session.add_all([alice, bob, charlie, acme, startup])
        session.commit()

        print(f"  Created {len([alice, bob, charlie])} people")
        print(f"  Created {len([acme, startup])} companies")
        print()

        # =====================================================================
        # 5. Create Relationships
        # =====================================================================
        print("=" * 60)
        print("5. Creating Relationships")
        print("=" * 60)

        # Create friendships
        session.create_edge(alice, "FRIEND_OF", bob, {"since": date(2015, 3, 1)})
        session.create_edge(bob, "FRIEND_OF", charlie, {"since": date(2018, 7, 15)})
        session.create_edge(alice, "FRIEND_OF", charlie, {"since": date(2019, 1, 1)})

        # Create follows
        session.create_edge(alice, "FOLLOWS", charlie)
        session.create_edge(bob, "FOLLOWS", alice)
        session.create_edge(charlie, "FOLLOWS", alice)

        # Create employment
        session.create_edge(alice, "WORKS_AT", acme, {"role": "Senior Engineer"})
        session.create_edge(bob, "WORKS_AT", startup, {"role": "Data Scientist"})
        session.create_edge(charlie, "WORKS_AT", startup, {"role": "CEO"})

        db.flush()
        print("  Created friendship edges")
        print("  Created follows edges")
        print("  Created works_at edges")
        print()

        # =====================================================================
        # 6. Query with DSL
        # =====================================================================
        print("=" * 60)
        print("6. Querying with DSL")
        print("=" * 60)

        # Simple query
        print("\n  All people:")
        all_people = session.query(Person).all()
        for person in all_people:
            print(f"    - {person.name} (age: {person.age})")

        # Filter query
        print("\n  People over 28:")
        from uni_pydantic.query import PropertyProxy

        adults = (
            session.query(Person)
            .filter(PropertyProxy[int]("age", Person) > 28)
            .all()
        )
        for person in adults:
            print(f"    - {person.name} (age: {person.age})")

        # Filter by keyword
        print("\n  Find Alice by email:")
        alice_found = session.get(Person, email="alice@example.com")
        if alice_found:
            print(f"    Found: {alice_found.name}")

        print()

        # =====================================================================
        # 7. Update Entities
        # =====================================================================
        print("=" * 60)
        print("7. Updating Entities")
        print("=" * 60)

        alice_found = session.get(Person, email="alice@example.com")
        if alice_found:
            print(f"  Before: Alice's age = {alice_found.age}")
            alice_found.age = 31
            session.commit()
            print(f"  After: Alice's age = {alice_found.age}")
        print()

        # =====================================================================
        # 8. Raw Cypher Query
        # =====================================================================
        print("=" * 60)
        print("8. Raw Cypher Query")
        print("=" * 60)

        results = session.cypher(
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name as name, c.name as company"
        )
        print("  Employment relationships:")
        for row in results:
            print(f"    - {row['name']} works at {row['company']}")
        print()

        # =====================================================================
        # 9. Count and Exists
        # =====================================================================
        print("=" * 60)
        print("9. Count and Exists")
        print("=" * 60)

        total_people = session.query(Person).count()
        print(f"  Total people: {total_people}")

        has_alice = session.query(Person).filter_by(name="Alice").exists()
        print(f"  Alice exists: {has_alice}")

        has_david = session.query(Person).filter_by(name="David").exists()
        print(f"  David exists: {has_david}")
        print()

        # =====================================================================
        # Done!
        # =====================================================================
        print("=" * 60)
        print("Demo completed successfully!")
        print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
