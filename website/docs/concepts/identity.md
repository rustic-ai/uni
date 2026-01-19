# Identity Model

Uni uses a sophisticated identity system to balance performance, flexibility, and distributed computing requirements. This document explains the three identity types and their roles.

## Overview

Every entity in Uni has multiple identifiers serving different purposes:

| Identity | Bits | Purpose | Locality |
|----------|------|---------|----------|
| **VID/EID** | 64 | Internal array indexing | Local to database |
| **ext_id** | Variable | User-provided external ID | User-defined |
| **UniId** | 256 | Content-addressed provenance | Global / distributed |

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         VERTEX IDENTITY STACK                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    User-Facing (ext_id)                              │   │
│  │                    "paper_abc123"                                    │   │
│  │                    Human-readable, stable across imports             │   │
│  └───────────────────────────────┬─────────────────────────────────────┘   │
│                                  │ resolves to                             │
│  ┌───────────────────────────────▼─────────────────────────────────────┐   │
│  │                    Internal (VID)                                    │   │
│  │                    0x0001_0000_0000_002A                             │   │
│  │                    Fast array indexing, label-encoded                │   │
│  └───────────────────────────────┬─────────────────────────────────────┘   │
│                                  │ content-hashes to                       │
│  ┌───────────────────────────────▼─────────────────────────────────────┐   │
│  │                    Provenance (UniId)                             │   │
│  │                    bafkreihdwdcefgh4dqkjv67uzcmw7o...               │   │
│  │                    Content-addressed, CRDT-compatible                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Vertex ID (VID)

The Vertex ID is a 64-bit packed integer optimized for O(1) array indexing.

### Encoding

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              VID (64 bits)                                  │
├──────────────────┬─────────────────────────────────────────────────────────┤
│   label_id (16)  │                  local_offset (48)                       │
├──────────────────┴─────────────────────────────────────────────────────────┤
│                                                                            │
│   Example: 0x0001_0000_0000_002A                                           │
│            ────── ──────────────                                           │
│            label    offset                                                 │
│            (Paper)  (42)                                                   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

| Component | Bits | Range | Purpose |
|-----------|------|-------|---------|
| `label_id` | 16 | 0 - 65,535 | Identifies vertex type (label) |
| `local_offset` | 48 | 0 - 281 trillion | Per-label sequential offset |

### Usage in Code

```rust
use uni::core::Vid;

// Create a VID
let vid = Vid::new(1, 42);  // label_id=1 (Paper), offset=42

// Access components
assert_eq!(vid.label_id(), 1);
assert_eq!(vid.local_offset(), 42);
assert_eq!(vid.as_u64(), 0x0001_0000_0000_002A);

// Parse from u64
let vid = Vid::from_u64(0x0001_0000_0000_002A);
```

### Why This Design?

1. **O(1) Array Indexing**: The offset directly indexes into per-label property arrays
2. **Label Partitioning**: Queries on a single label only scan that label's data
3. **Dense Storage**: Offsets are sequential, enabling compact columnar storage
4. **Type Safety**: Label ID embedded in VID prevents cross-label confusion

### Capacity

| Component | Maximum | Practical Limit |
|-----------|---------|-----------------|
| Labels | 65,535 | Typically 10-100 |
| Vertices per label | 281 trillion | Limited by storage |
| Total vertices | 18 quintillion | Theoretical max |

---

## Edge ID (EID)

Edge IDs follow the same packed 64-bit structure as VIDs.

### Encoding

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              EID (64 bits)                                  │
├──────────────────┬─────────────────────────────────────────────────────────┤
│   type_id (16)   │                  local_offset (48)                       │
├──────────────────┴─────────────────────────────────────────────────────────┤
│                                                                            │
│   Example: 0x0002_0000_0000_0015                                           │
│            ────── ──────────────                                           │
│            type     offset                                                 │
│            (CITES)  (21)                                                   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Usage

```rust
use uni::core::Eid;

let eid = Eid::new(2, 21);  // type_id=2 (CITES), offset=21

assert_eq!(eid.type_id(), 2);
assert_eq!(eid.local_offset(), 21);
```

---

## External ID (ext_id)

The external ID is a user-provided string identifier for human readability.

### Characteristics

| Property | Description |
|----------|-------------|
| Type | UTF-8 String |
| Uniqueness | Unique within a label |
| Mutability | Immutable after creation |
| Indexing | Automatically indexed for lookups |

### Usage

```cypher
// Create with external ID
CREATE (p:Paper {id: "arxiv:2106.09685", title: "LoRA"})

// Query by external ID
MATCH (p:Paper {id: "arxiv:2106.09685"})
RETURN p.title
```

### Resolution

External IDs are resolved to VIDs at query time:

```
ext_id "arxiv:2106.09685"
         │
         ▼
    ┌─────────────────┐
    │  ext_id Index   │  (BTree index on ext_id column)
    └────────┬────────┘
             │
             ▼
    VID 0x0001_0000_0000_002A
```

---

## UniId

The UniId is a content-addressed identifier for distributed systems and provenance tracking.

### Characteristics

| Property | Description |
|----------|-------------|
| Algorithm | SHA3-256 |
| Encoding | Multibase (base32) |
| Length | 44 characters |
| Determinism | Same content → same UID |

### Format

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           UniId Structure                                │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│   Multibase prefix: 'b' (base32 lowercase)                                 │
│                                                                            │
│   Example: bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku     │
│            ─                                                               │
│            └── multibase prefix                                            │
│             ────────────────────────────────────────────────────────       │
│             └── base32 encoded SHA3-256 hash (43 chars)                    │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Generation

UniId is computed from vertex content:

```rust
use uni::core::UniId;
use sha3::{Sha3_256, Digest};

// Content to hash
let content = serde_json::json!({
    "label": "Paper",
    "properties": {
        "title": "Attention Is All You Need",
        "year": 2017
    }
});

// Compute SHA3-256
let mut hasher = Sha3_256::new();
hasher.update(content.to_string().as_bytes());
let hash = hasher.finalize();

// Create UniId
let uid = UniId::from_bytes(&hash);
println!("{}", uid.to_multibase());  // bafkrei...
```

### Use Cases

1. **Content Deduplication**: Same content always produces same UID
2. **Distributed Sync**: UIDs are globally unique without coordination
3. **Audit Trail**: Track data provenance across systems
4. **CRDT Integration**: UIDs enable conflict-free replication across distributed nodes

---

## ID Resolution

### VID Lookup by ext_id

```cypher
MATCH (p:Paper {id: "arxiv:2106.09685"})
```

Resolution path:
1. Parse pattern → extract ext_id value
2. Query ext_id index → get VID
3. Load vertex data using VID offset

### VID Lookup by UniId

```cypher
MATCH (p:Paper)
WHERE p._uid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
```

Resolution path:
1. Query UID index (separate Lance dataset)
2. Get VID from index
3. Load vertex data using VID

---

## Direction Enum

For edge traversal, Uni uses a Direction enum:

```rust
pub enum Direction {
    Outgoing,  // Source → Destination
    Incoming,  // Destination ← Source
    Both,      // Either direction
}
```

### Cypher Syntax Mapping

| Cypher Pattern | Direction |
|----------------|-----------|
| `(a)-[:TYPE]->(b)` | Outgoing from a |
| `(a)<-[:TYPE]-(b)` | Incoming to a |
| `(a)-[:TYPE]-(b)` | Both |

---

## ID Allocation

IDs are allocated by the `IdAllocator`:

```rust
pub struct IdAllocator {
    label_counters: DashMap<u16, AtomicU64>,
    edge_type_counters: DashMap<u16, AtomicU64>,
}

impl IdAllocator {
    pub fn allocate_vid(&self, label_id: u16) -> Vid {
        let offset = self.label_counters
            .entry(label_id)
            .or_insert(AtomicU64::new(0))
            .fetch_add(1, Ordering::SeqCst);
        Vid::new(label_id, offset)
    }

    pub fn allocate_eid(&self, type_id: u16) -> Eid {
        // Similar for edges
    }
}
```

**Allocation Properties:**
- Thread-safe via atomic operations
- Sequential within each label/type
- Persisted on flush for recovery
- Never reuses IDs (even after deletes)

---

## Storage Layout

### UID Index Structure

```
indexes/uid_to_vid/{label}/index.lance
├── _uid: FixedSizeBinary(32)  // SHA3-256 hash bytes
└── _vid: UInt64               // Corresponding VID
```

### Resolution Performance

| Lookup Type | Index | Complexity | Typical Latency |
|-------------|-------|------------|-----------------|
| VID direct | None | O(1) | ~10µs |
| ext_id | BTree | O(log n) | ~100µs |
| UniId | BTree | O(log n) | ~100µs |
| Full scan | None | O(n) | Varies |

---

## Best Practices

### Choosing External IDs

```
✓ Good: "user_12345", "arxiv:2106.09685", "isbn:978-0134685991"
✗ Bad: Sequential integers (use VID instead), UUIDs (use UniId)
```

### When to Use Each ID

| Use Case | Recommended ID |
|----------|----------------|
| Internal operations | VID |
| API responses | ext_id |
| Cross-system sync | UniId |
| Human debugging | ext_id |
| Array indexing | VID offset |

---

## Next Steps

- [Data Model](data-model.md) — Vertices, edges, and properties
- [Indexing](indexing.md) — Index types and configuration
- [Architecture](architecture.md) — System overview
