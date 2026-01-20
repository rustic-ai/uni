# uni-pydantic Design Plan

## Overview

Create a new Python package `uni-pydantic` that provides Pydantic-based OGM (Object-Graph Mapping) for the Uni graph database. This will be a separate package from `uni-db` to keep dependencies optional.

## Design Goals

1. **Type Safety**: Full IDE autocomplete and type checking for graph operations
2. **Pydantic-Native**: Leverage Pydantic v2 patterns (validators, computed fields, serialization)
3. **Schema-from-Models**: Auto-generate Uni schema from Pydantic model definitions
4. **Relationship First-Class**: Edges/relationships as declarative fields with lazy/eager loading
5. **Query DSL**: Type-safe query builder alongside raw Cypher escape hatch

## Design Decisions

- **Package location**: `bindings/python/uni-pydantic/` (monorepo alongside uni-db)
- **Pydantic version**: v2 only (modern API, better performance)
- **Async support**: Sync-only (matches current uni-db bindings, simpler adoption)
- **Schema migrations**: Additive-only initially (new properties/indexes), manual migration for breaking changes

---

## API Design

### 1. Base Classes

```python
from uni_pydantic import UniNode, UniEdge, Field, Vector, Relationship

class Person(UniNode):
    """A person node in the graph."""
    __label__ = "Person"  # Optional, defaults to class name

    # Properties with type mapping
    name: str                           # Required string
    age: int | None = None              # Nullable int64
    email: str = Field(unique=True)     # Unique constraint
    bio: str = Field(index="fulltext")  # Fulltext indexed
    embedding: Vector[1536]             # 1536-dim vector with auto-index

    # Relationships (lazy-loaded by default)
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
    works_at: "Company | None" = Relationship("WORKS_AT")


class FriendshipEdge(UniEdge):
    """Edge with properties."""
    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person

    since: date
    strength: float = 1.0


class Company(UniNode):
    __label__ = "Company"

    name: str = Field(index="btree", unique=True)
    founded: date | None = None

    # Reverse relationship
    employees: list[Person] = Relationship("WORKS_AT", direction="incoming")
```

### 2. Type Mapping (Pydantic → Uni DataType)

| Python Type | Uni DataType | Notes |
|-------------|--------------|-------|
| `str` | String | |
| `int` | Int64 | |
| `float` | Float64 | |
| `bool` | Bool | |
| `datetime` | DateTime | |
| `date` | Date | |
| `time` | Time | |
| `timedelta` | Duration | |
| `bytes` | Bytes | |
| `dict` | Json | Stored as JSON |
| `list[T]` | List(T) | Recursive |
| `Vector[N]` | Vector{N} | Custom type |
| `T \| None` | T + nullable=True | Optional fields |

### 3. Field Configuration

```python
from uni_pydantic import Field, Index

class Article(UniNode):
    # Basic indexing
    title: str = Field(index="btree")

    # Unique constraint
    slug: str = Field(unique=True)

    # Fulltext search
    content: str = Field(index="fulltext", tokenizer="standard")

    # Vector with custom metric
    embedding: Vector[768] = Field(metric="cosine")

    # Computed/generated property
    word_count: int = Field(generated="length(content)")

    # Default values
    views: int = Field(default=0)
    tags: list[str] = Field(default_factory=list)
```

### 4. Relationship Declaration

```python
from uni_pydantic import Relationship

class Person(UniNode):
    # Outgoing relationship (default)
    follows: list["Person"] = Relationship("FOLLOWS")

    # Incoming relationship
    followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

    # Bidirectional
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

    # Single relationship (not list)
    manager: "Person | None" = Relationship("REPORTS_TO")

    # Relationship with edge properties
    friendships: list[tuple["Person", FriendshipEdge]] = Relationship(
        "FRIEND_OF",
        edge_model=FriendshipEdge
    )
```

### 5. Database Session & Registration

```python
from uni_pydantic import UniSession
from uni_db import Database

db = Database.open("./my_graph")

# Create session and register models
session = UniSession(db)
session.register(Person, Company, FriendshipEdge)

# Auto-generate and apply schema
session.sync_schema()  # Creates labels, edge types, indexes
```

### 6. CRUD Operations

```python
# Create
alice = Person(name="Alice", age=30, email="alice@example.com")
session.add(alice)
session.commit()

# Read by ID
person = session.get(Person, vid=12345)

# Update
alice.age = 31
session.commit()

# Delete
session.delete(alice)
session.commit()

# Bulk create
people = [Person(name=f"User{i}") for i in range(1000)]
session.add_all(people)
session.commit()
```

### 7. Query DSL

```python
# Simple queries
people = session.query(Person).all()
alice = session.query(Person).filter(Person.name == "Alice").first()

# Chained filters
adults = (
    session.query(Person)
    .filter(Person.age >= 18)
    .filter(Person.email.is_not_null())
    .order_by(Person.name)
    .limit(10)
    .all()
)

# Relationship traversal
alice_friends = (
    session.query(Person)
    .filter(Person.name == "Alice")
    .traverse(Person.friends)  # Follow FRIEND_OF edges
    .all()
)

# Multi-hop traversal
friends_of_friends = (
    session.query(Person)
    .filter(Person.name == "Alice")
    .traverse(Person.friends)
    .traverse(Person.friends)
    .distinct()
    .all()
)

# Eager loading (avoid N+1)
people_with_companies = (
    session.query(Person)
    .eager_load(Person.works_at)
    .all()
)

# Vector search
similar = (
    session.query(Person)
    .vector_search(Person.embedding, query_vec, k=10, threshold=0.8)
    .all()
)
```

### 8. Raw Cypher Escape Hatch

```python
# Execute raw Cypher, get typed results
results: list[Person] = session.cypher(
    "MATCH (p:Person)-[:FRIEND_OF]->(f:Person) WHERE p.name = $name RETURN f",
    params={"name": "Alice"},
    result_type=Person
)

# Mixed return types
from uni_pydantic import CypherResult

results = session.cypher(
    "MATCH (p:Person)-[r:FRIEND_OF]->(f:Person) RETURN p, r, f",
    result_type=CypherResult[Person, FriendshipEdge, Person]
)
for p, rel, f in results:
    print(f"{p.name} -> {f.name} (since {rel.since})")
```

### 9. Lifecycle Hooks

```python
from uni_pydantic import before_create, after_create, before_update, before_delete

class Person(UniNode):
    name: str
    created_at: datetime | None = None

    @before_create
    def set_created_at(self):
        self.created_at = datetime.now()

    @after_create
    def log_creation(self):
        logger.info(f"Created person: {self.name}")

    @before_update
    def validate_update(self):
        if self.age and self.age < 0:
            raise ValueError("Age cannot be negative")
```

### 10. Transactions

```python
# Context manager
with session.transaction() as tx:
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    tx.add(alice)
    tx.add(bob)
    tx.create_edge(alice, "FRIEND_OF", bob, since=date.today())
    # Auto-commits on exit, rolls back on exception

# Manual control
tx = session.begin()
try:
    tx.add(Person(name="Charlie"))
    tx.commit()
except Exception:
    tx.rollback()
```

---

## Package Structure

```
uni-pydantic/
├── pyproject.toml
├── src/
│   └── uni_pydantic/
│       ├── __init__.py          # Public API exports
│       ├── base.py              # UniNode, UniEdge base classes
│       ├── fields.py            # Field, Vector, Relationship
│       ├── session.py           # UniSession, transactions
│       ├── query.py             # Query builder DSL
│       ├── schema.py            # Schema generation from models
│       ├── types.py             # Type mapping (Pydantic ↔ Uni)
│       ├── hooks.py             # Lifecycle decorators
│       └── exceptions.py        # Custom exceptions
└── tests/
    ├── test_models.py
    ├── test_queries.py
    ├── test_relationships.py
    └── test_schema_sync.py
```

---

## Implementation Phases

### Phase 1: Core Foundation
- [ ] Create package structure with Poetry
- [ ] Implement `UniNode` and `UniEdge` base classes
- [ ] Implement type mapping (Pydantic → Uni DataType)
- [ ] Implement `Field` with index/unique/nullable support
- [ ] Implement `Vector[N]` custom type

### Phase 2: Schema Generation
- [ ] Model registry and introspection
- [ ] Generate Uni schema from registered models
- [ ] `sync_schema()` to apply schema to database
- [ ] Handle schema migrations (additive only initially)

### Phase 3: Session & CRUD
- [ ] `UniSession` class wrapping `uni_db.Database`
- [ ] `add()`, `get()`, `delete()` operations
- [ ] Dirty tracking for updates
- [ ] `commit()` with batch writes
- [ ] Transaction support

### Phase 4: Relationships
- [ ] `Relationship` field descriptor
- [ ] Lazy loading on access
- [ ] Eager loading via query builder
- [ ] Edge property models
- [ ] Bidirectional relationship sync

### Phase 5: Query DSL
- [ ] `QueryBuilder` with method chaining
- [ ] Filter expressions (==, !=, <, >, in_, like)
- [ ] `traverse()` for relationship following
- [ ] `order_by()`, `limit()`, `skip()`
- [ ] `vector_search()` integration
- [ ] Result mapping to model instances

### Phase 6: Polish
- [ ] Lifecycle hooks
- [ ] Raw Cypher with typed results
- [ ] Comprehensive error messages
- [ ] Documentation and examples
- [ ] Performance optimization

---

## Verification Plan

1. **Unit Tests**: Test each component in isolation
   - Type mapping correctness
   - Field parsing and validation
   - Query builder generates correct Cypher

2. **Integration Tests**: Full round-trip with real database
   - Create models → sync schema → CRUD operations
   - Relationship traversal and loading
   - Vector search integration

3. **Example Script**: End-to-end demo
   ```bash
   cd bindings/python/uni-pydantic
   poetry install
   poetry run pytest
   poetry run python examples/demo.py
   ```
