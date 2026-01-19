# Schema Design Guide

A well-designed schema is crucial for performance, maintainability, and query efficiency. This guide covers best practices for modeling your domain in Uni.

## Schema Design Principles

### 1. Model the Domain, Not the Queries

Design your schema around your domain entities, not specific queries:

```
✓ Good: Entities represent real-world concepts
  Paper, Author, Venue, Citation

✗ Bad: Entities shaped by specific queries
  PaperWithAuthorNames, RecentPapersByVenue
```

### 2. Use Labels for Types, Not States

Labels define entity types, not transient states:

```
✓ Good: Labels are stable types
  :Paper, :Author, :Venue

✗ Bad: Labels represent changeable states
  :PublishedPaper, :DraftPaper, :RetractedPaper
  (Use a "status" property instead)
```

### 3. Relationships Are First-Class

Graph relationships are powerful—use them:

```
✓ Good: Relationships as edges
  (paper)-[:AUTHORED_BY]->(author)
  (paper)-[:CITES]->(cited)

✗ Bad: Relationships as properties
  Paper { author_ids: ["a1", "a2"] }
```

### 4. Keep Vertices Focused

Each vertex should represent one cohesive entity:

```
✓ Good: Focused vertex
  Paper { title, year, abstract }

✗ Bad: Kitchen sink vertex
  Paper { title, year, author_name, venue_name, citation_count }
  (author_name, venue_name should be separate vertices)
```

---

## Labels

### Naming Conventions

| Convention | Example | Rationale |
|------------|---------|-----------|
| Singular nouns | `:Paper` | Represents one entity |
| PascalCase | `:ResearchPaper` | Standard convention |
| Descriptive | `:AcademicPaper` | Clear meaning |
| Avoid abbreviations | `:Organization` not `:Org` | Readable |

### Label Granularity

**Too Few Labels:**
```
// All entities in one label - hard to query efficiently
:Entity { type: "paper", ... }
:Entity { type: "author", ... }
```

**Too Many Labels:**
```
// Fragmented - complex schema, poor caching
:NeurIPSPaper, :ICMLPaper, :ICLRPaper, :ArXivPaper
```

**Just Right:**
```
// Labels represent fundamental types
:Paper { venue: "NeurIPS" }  // venue is a property
:Author
:Venue
```

### Label Hierarchy Considerations

Uni uses single-label vertices (encoded in VID). If you need hierarchies:

```
// Option 1: Property-based classification
:Paper { paper_type: "research", venue_type: "conference" }

// Option 2: Separate labels with relationships
:Paper, :ConferenceSubmission
(paper)-[:SUBMITTED_TO]->(conference)

// Option 3: Composition via edges
:Paper, :Category
(paper)-[:IN_CATEGORY]->(category)
```

---

## Edge Types

### Naming Conventions

| Convention | Example | Rationale |
|------------|---------|-----------|
| UPPER_SNAKE_CASE | `:AUTHORED_BY` | Visually distinct from labels |
| Verb phrases | `:CITES`, `:BELONGS_TO` | Describes relationship |
| Past tense or present | `:WROTE` or `:WRITES` | Consistent style |
| Active voice | `:CITES` not `:CITED_BY` | Clear direction |

### Direction Semantics

Choose direction based on typical query patterns:

```
// Natural reading direction: subject -[verb]-> object
(paper)-[:CITES]->(cited_paper)      // Paper cites another paper
(paper)-[:AUTHORED_BY]->(author)     // Paper is authored by author
(author)-[:WORKS_AT]->(institution)  // Author works at institution

// Query from either direction
MATCH (a:Author)<-[:AUTHORED_BY]-(p:Paper)  // Find author's papers
MATCH (p:Paper)-[:AUTHORED_BY]->(a:Author)  // Find paper's authors
```

### Edge Properties

Use edge properties sparingly for relationship metadata:

```json
{
  "edge_types": {
    "AUTHORED_BY": {
      "id": 1,
      "src_labels": ["Paper"],
      "dst_labels": ["Author"]
    }
  },
  "properties": {
    "AUTHORED_BY": {
      "position": { "type": "Int32" },      // Author order
      "contribution": { "type": "String" }   // Role: "lead", "contributor"
    }
  }
}
```

**When to Use Edge Properties:**
- Relationship metadata (timestamps, weights, roles)
- Data specific to the relationship, not the connected vertices

**When to Avoid Edge Properties:**
- Frequently updated data (edges are immutable)
- Large data (embeddings, documents)

---

## Property Design

### Data Type Selection

| Data Type | Use Case | Example |
|-----------|----------|---------|
| `String` | Text, identifiers | title, name, doi |
| `Int32` | Small integers | year, count |
| `Int64` | Large integers | timestamp_ms, big_count |
| `Float64` | Decimal values | price, score |
| `Bool` | Flags | is_published, is_retracted |
| `Timestamp` | Date/time | created_at, published_at |
| `Vector` | Embeddings | embedding, image_vector |
| `Json` | Semi-structured | metadata, config |

### Nullability

Be intentional about nullable properties:

```json
{
  "Paper": {
    // Required: every paper has these
    "title": { "type": "String", "nullable": false },

    // Optional: not all papers have these
    "abstract": { "type": "String", "nullable": true },
    "doi": { "type": "String", "nullable": true }
  }
}
```

### Property Naming

| Convention | Example | Notes |
|------------|---------|-------|
| snake_case | `created_at` | Consistent with JSON |
| Descriptive | `citation_count` not `cc` | Self-documenting |
| No prefixes | `title` not `paper_title` | Label provides context |

### Avoid Property Bloat

```
✓ Good: Focused properties
  Paper { title, year, venue, abstract, doi }

✗ Bad: Everything on one vertex
  Paper {
    title, year, venue, abstract, doi,
    author_names,        // Should be vertex + edge
    all_citations,       // Should be edges
    raw_pdf_bytes,       // Too large
    processing_status    // Transient state
  }
```

---

## Vector Properties

### Dimension Planning

Vector dimensions are immutable after schema creation:

```json
{
  "embedding": {
    "type": "Vector",
    "dimensions": 768  // Cannot change later
  }
}
```

**Choosing Dimensions:**

| Model Family | Typical Dimensions | Notes |
|--------------|-------------------|-------|
| Sentence Transformers | 384-768 | General text |
| OpenAI embeddings | 1536-3072 | Commercial |
| CLIP | 512-768 | Multimodal |
| Custom | Varies | Match your model |

### Multiple Embeddings

For different embedding types, use separate properties:

```json
{
  "Paper": {
    "title_embedding": { "type": "Vector", "dimensions": 384 },
    "abstract_embedding": { "type": "Vector", "dimensions": 768 },
    "figure_embedding": { "type": "Vector", "dimensions": 512 }
  }
}
```

### Embedding Versioning

When upgrading embedding models:

```json
{
  "Paper": {
    // Current
    "embedding": { "type": "Vector", "dimensions": 768 },

    // Legacy (deprecated)
    "embedding_v1": { "type": "Vector", "dimensions": 384 }
  }
}
```

---

## Document Mode

### When to Use Document Mode

Enable `is_document: true` for entities with:
- Highly variable/nested structure
- Frequently changing schema
- Semi-structured metadata

```json
{
  "labels": {
    "Paper": {
      "id": 1,
      "is_document": true
    }
  }
}
```

### Document Properties

```cypher
CREATE (p:Paper {
  title: "Research Paper",
  year: 2024,
  // Document field for flexible data
  _doc: {
    figures: [
      { id: "fig1", caption: "Architecture" },
      { id: "fig2", caption: "Results" }
    ],
    supplementary: {
      code_url: "https://github.com/...",
      datasets: ["imagenet", "coco"]
    },
    review_scores: [8, 7, 9]
  }
})
```

### JSON vs Typed Properties

| Use Typed Properties | Use JSON |
|---------------------|----------|
| Frequently queried | Rarely queried |
| Stable schema | Evolving schema |
| Needs indexing | No indexing needed |
| Performance critical | Flexibility critical |

---

## Index Planning

### Index Strategy

Plan indexes based on query patterns:

```json
{
  "indexes": {
    // Vector index for similarity search
    "paper_embeddings": {
      "type": "vector",
      "label": "Paper",
      "property": "embedding",
      "config": { "index_type": "hnsw", "metric": "cosine" }
    },

    // Scalar index for frequent filters
    "paper_year": {
      "type": "scalar",
      "label": "Paper",
      "property": "year",
      "config": { "index_type": "btree" }
    },

    // Scalar index for unique lookups
    "paper_doi": {
      "type": "scalar",
      "label": "Paper",
      "property": "doi",
      "config": { "index_type": "hash" }
    }
  }
}
```

### Index Selection Guidelines

| Query Pattern | Index Type | Example |
|---------------|------------|---------|
| `WHERE x = 5` | BTree or Hash | Year, ID |
| `WHERE x > 5` | BTree | Year ranges |
| `WHERE x IN [...]` | BTree or Bitmap | Categories |
| Vector similarity | HNSW or IVF_PQ | Embeddings |
| Text search | Full-text | Title, abstract |

---

## Schema Evolution

### Adding Properties

Safe operation—existing data gets NULL:

```json
// Before
{ "Paper": { "title": "String" } }

// After (add new property)
{ "Paper": {
    "title": "String",
    "citation_count": { "type": "Int32", "nullable": true }  // New
}}
```

### Deprecating Properties

Use state markers for gradual removal:

```json
{
  "Paper": {
    "old_field": {
      "type": "String",
      "state": "deprecated",
      "deprecated_since": "2024-01-01",
      "migration_hint": "Use new_field instead"
    },
    "new_field": { "type": "String" }
  }
}
```

### Adding Labels/Edge Types

Safe operation—new types get new ID ranges:

```json
// Add new label
{
  "labels": {
    "Paper": { "id": 1 },
    "Preprint": { "id": 2 }  // New label
  }
}
```

### Breaking Changes (Avoid)

These require data migration:
- Changing property types
- Changing vector dimensions
- Renaming labels (ID is fixed)
- Changing edge type direction semantics

---

## Example Schemas

### Academic Papers

```json
{
  "schema_version": 1,

  "labels": {
    "Paper": { "id": 1, "is_document": true },
    "Author": { "id": 2 },
    "Venue": { "id": 3 },
    "Institution": { "id": 4 }
  },

  "edge_types": {
    "CITES": { "id": 1, "src_labels": ["Paper"], "dst_labels": ["Paper"] },
    "AUTHORED_BY": { "id": 2, "src_labels": ["Paper"], "dst_labels": ["Author"] },
    "PUBLISHED_IN": { "id": 3, "src_labels": ["Paper"], "dst_labels": ["Venue"] },
    "AFFILIATED_WITH": { "id": 4, "src_labels": ["Author"], "dst_labels": ["Institution"] }
  },

  "properties": {
    "Paper": {
      "title": { "type": "String", "nullable": false },
      "abstract": { "type": "String", "nullable": true },
      "year": { "type": "Int32", "nullable": false },
      "doi": { "type": "String", "nullable": true },
      "embedding": { "type": "Vector", "dimensions": 768 }
    },
    "Author": {
      "name": { "type": "String", "nullable": false },
      "email": { "type": "String", "nullable": true },
      "orcid": { "type": "String", "nullable": true }
    },
    "Venue": {
      "name": { "type": "String", "nullable": false },
      "type": { "type": "String", "nullable": true }
    },
    "AUTHORED_BY": {
      "position": { "type": "Int32", "nullable": true }
    }
  }
}
```

### E-Commerce

```json
{
  "schema_version": 1,

  "labels": {
    "User": { "id": 1 },
    "Product": { "id": 2 },
    "Category": { "id": 3 },
    "Order": { "id": 4 }
  },

  "edge_types": {
    "PURCHASED": { "id": 1, "src_labels": ["User"], "dst_labels": ["Product"] },
    "VIEWED": { "id": 2, "src_labels": ["User"], "dst_labels": ["Product"] },
    "IN_CATEGORY": { "id": 3, "src_labels": ["Product"], "dst_labels": ["Category"] },
    "ORDERED": { "id": 4, "src_labels": ["Order"], "dst_labels": ["Product"] },
    "PLACED_BY": { "id": 5, "src_labels": ["Order"], "dst_labels": ["User"] }
  },

  "properties": {
    "User": {
      "email": { "type": "String", "nullable": false },
      "name": { "type": "String", "nullable": true },
      "preference_embedding": { "type": "Vector", "dimensions": 128 }
    },
    "Product": {
      "name": { "type": "String", "nullable": false },
      "description": { "type": "String", "nullable": true },
      "price": { "type": "Float64", "nullable": false },
      "embedding": { "type": "Vector", "dimensions": 384 }
    },
    "PURCHASED": {
      "quantity": { "type": "Int32", "nullable": false },
      "timestamp": { "type": "Timestamp", "nullable": false }
    }
  }
}
```

---

## Schema Validation Checklist

Before deploying your schema:

- [ ] All labels use singular PascalCase nouns
- [ ] All edge types use UPPER_SNAKE_CASE verbs
- [ ] All properties use snake_case
- [ ] Required properties are marked `nullable: false`
- [ ] Vector dimensions match your embedding model
- [ ] Edge type constraints match your domain rules
- [ ] Indexes planned for common query patterns
- [ ] No circular dependencies or overly complex relationships
- [ ] Document mode used only where needed
- [ ] Schema version tracked for evolution

---

## Next Steps

- [Data Ingestion](data-ingestion.md) — Import data with your schema
- [Indexing](../concepts/indexing.md) — Configure indexes
- [Cypher Querying](cypher-querying.md) — Query your schema
