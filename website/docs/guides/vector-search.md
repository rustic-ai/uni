# Vector Search Guide

Uni treats vector search as a first-class citizen, deeply integrated with the graph traversal engine. This guide covers schema design, index configuration, query patterns, and performance optimization for semantic similarity search.

## Overview

Vector search enables finding similar items based on high-dimensional embeddings:

```
Query: "papers about attention mechanisms"
         │
         ▼
    ┌───────────────────┐
    │  Embed Query      │
    │  → [0.12, -0.34,  │
    │     0.56, ...]    │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │  Vector Index     │
    │  (HNSW / IVF_PQ)  │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │  Top-K Results    │
    │  - Attention...   │
    │  - Transformer... │
    │  - BERT...        │
    └───────────────────┘
```

---

## Setting Up Vector Search

### Step 1: Define Vector Schema

Add a `Vector` type property to your schema:

```json
{
  "properties": {
    "Paper": {
      "title": { "type": "String", "nullable": false },
      "abstract": { "type": "String", "nullable": true },
      "embedding": {
        "type": "Vector",
        "dimensions": 768
      }
    },
    "Product": {
      "name": { "type": "String", "nullable": false },
      "description_embedding": {
        "type": "Vector",
        "dimensions": 384
      },
      "image_embedding": {
        "type": "Vector",
        "dimensions": 512
      }
    }
  }
}
```

**Dimension Guidelines:**

| Model | Dimensions | Use Case |
|-------|------------|----------|
| all-MiniLM-L6-v2 | 384 | General text, fast |
| BGE-base-en-v1.5 | 768 | High quality text |
| OpenAI text-embedding-3-small | 1536 | Commercial, high quality |
| CLIP ViT-B/32 | 512 | Image + text |

### Step 2: Create Vector Index

Create an index for efficient similarity search:

**HNSW (Recommended for most cases):**

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper)
ON p.embedding
OPTIONS {
  index_type: "hnsw",
  metric: "cosine",
  m: 32,
  ef_construction: 200
}
```

**IVF_PQ (For memory-constrained environments):**

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper)
ON p.embedding
OPTIONS {
  index_type: "ivf_pq",
  metric: "cosine",
  num_partitions: 1024,
  num_sub_vectors: 48
}
```

### Step 3: Import Data with Embeddings

Your import data should include embedding vectors:

```json
{"id": "paper_001", "title": "Attention Is All You Need", "embedding": [0.12, -0.34, 0.56, ...]}
{"id": "paper_002", "title": "BERT: Pre-training of Deep Bidirectional Transformers", "embedding": [0.08, -0.21, 0.42, ...]}
```

---

## Querying Vectors

### Basic KNN Search

Find the K nearest neighbors to a query vector:

```cypher
CALL db.idx.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node, distance
RETURN node.title, distance
ORDER BY distance
```

**Parameters:**
- `'Paper'`: Label to search
- `'embedding'`: Vector property name
- `$query_vector`: Query vector (list of floats)
- `10`: Number of results (K)

### With Distance Threshold

Filter results by distance:

```cypher
CALL db.idx.vector.query('Paper', 'embedding', $query_vector, 100, 0.3)
YIELD node, distance
WHERE distance < 0.2
RETURN node.title, distance
LIMIT 10
```

### Hybrid: Vector + Property Filters

Combine vector search with property filtering:

```cypher
CALL db.idx.vector.query('Paper', 'embedding', $query_vector, 50)
YIELD node AS paper, distance
WHERE paper.year >= 2020 AND paper.venue IN ['NeurIPS', 'ICML']
RETURN paper.title, paper.year, distance
ORDER BY distance
LIMIT 10
```

---

## Hybrid Graph + Vector Queries

The real power comes from combining graph traversal with vector search.

### Pattern 1: Vector Search → Graph Expansion

Find similar papers, then explore their citations:

```cypher
// Find papers similar to query
CALL db.idx.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node AS seed, distance

// Expand to citations
MATCH (seed)-[:CITES]->(cited:Paper)
RETURN seed.title AS source, cited.title AS cited_paper, distance
ORDER BY distance, cited.year DESC
```

### Pattern 2: Graph Context → Vector Search

Start from a known node, find similar neighbors:

```cypher
// Start from a specific paper
MATCH (seed:Paper {title: 'Attention Is All You Need'})

// Get its embedding
WITH seed, seed.embedding AS seed_embedding

// Find papers cited by seed that are similar to seed
MATCH (seed)-[:CITES]->(cited:Paper)
WHERE vector_similarity(seed_embedding, cited.embedding) > 0.8
RETURN cited.title, cited.year
```

### Pattern 3: Multi-Hop with Similarity Filter

Find papers in citation chain with semantic similarity:

```cypher
MATCH (start:Paper {title: 'Attention Is All You Need'})
MATCH (start)-[:CITES]->(hop1:Paper)-[:CITES]->(hop2:Paper)
WHERE vector_similarity(start.embedding, hop2.embedding) > 0.7
RETURN DISTINCT hop2.title, hop2.year
ORDER BY hop2.year DESC
LIMIT 20
```

### Pattern 4: Author's Similar Papers

Find an author's papers similar to a query:

```cypher
// Vector search for similar papers
CALL db.idx.vector.query('Paper', 'embedding', $query_vector, 100)
YIELD node AS paper, distance

// Filter to specific author
MATCH (paper)-[:AUTHORED_BY]->(a:Author {name: 'Geoffrey Hinton'})
RETURN paper.title, paper.year, distance
ORDER BY distance
LIMIT 10
```

---

## Generating Embeddings

### Using FastEmbed (Built-in)

Uni includes FastEmbed for local embedding generation:

```rust
use uni_db::embedding::{EmbeddingService, FastEmbedService, FastEmbedModel};

// Create service
let service = FastEmbedService::new(FastEmbedModel::AllMiniLML6V2)?;

// Embed text
let texts = vec!["attention mechanisms in transformers", "graph neural networks"];
let embeddings = service.embed(&texts).await?;
// embeddings: Vec<Vec<f32>> with 384 dimensions each
```

**Available Models:**

| Model | Dimensions | Speed | Quality |
|-------|------------|-------|---------|
| `AllMiniLML6V2` | 384 | Fast | Good |
| `BGESmallENV15` | 384 | Fast | Good |
| `BGEBaseENV15` | 768 | Medium | Better |
| `NomicEmbedTextV15` | 768 | Medium | Better |
| `MultilingualE5Small` | 384 | Fast | Multilingual |

### Using External APIs

For production, you might use external embedding APIs:

```python
import openai
import json

# Generate embeddings
def embed_text(text):
    response = openai.Embedding.create(
        input=text,
        model="text-embedding-3-small"
    )
    return response['data'][0]['embedding']

# Prepare JSONL with embeddings
papers = [
    {"id": "p1", "title": "Paper 1", "embedding": embed_text("Paper 1 abstract")},
    {"id": "p2", "title": "Paper 2", "embedding": embed_text("Paper 2 abstract")},
]

with open("papers.jsonl", "w") as f:
    for paper in papers:
        f.write(json.dumps(paper) + "\n")
```

---

## Distance Metrics

### Cosine Similarity

Best for normalized embeddings (most text models):

```
similarity = A · B / (||A|| × ||B||)
distance = 1 - similarity
```

- Range: 0 (identical) to 2 (opposite)
- Use when: Magnitude doesn't matter, only direction

### L2 (Euclidean) Distance

Best for embeddings where magnitude matters:

```
distance = √Σ(aᵢ - bᵢ)²
```

- Range: 0 (identical) to ∞
- Use when: Absolute position in space matters

### Dot Product

Best for unnormalized embeddings:

```
similarity = A · B
distance = -similarity (negated for ranking)
```

- Range: -∞ to +∞
- Use when: Embeddings have meaningful magnitudes

---

## Index Tuning

### HNSW Parameters

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper) ON p.embedding
OPTIONS {
  index_type: "hnsw",
  metric: "cosine",

  // Build-time parameters
  m: 32,               // Connections per node (16-64)
  ef_construction: 200, // Build-time search width (100-500)

  // Query-time parameters (set at query)
  // ef_search: 100     // Query-time search width (50-200)
}
```

**Tuning Guide:**

| Scenario | m | ef_construction | ef_search |
|----------|---|-----------------|-----------|
| Speed priority | 16 | 100 | 50 |
| Balanced | 32 | 200 | 100 |
| Recall priority | 48 | 400 | 200 |
| Maximum recall | 64 | 500 | 300 |

### IVF_PQ Parameters

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper) ON p.embedding
OPTIONS {
  index_type: "ivf_pq",
  metric: "cosine",

  num_partitions: 1024,  // √n is good start
  num_sub_vectors: 48,   // Higher = better recall
  num_probes: 50         // Query-time clusters to search
}
```

**Memory vs Recall Trade-off:**

| num_sub_vectors | Memory per vector | Recall |
|-----------------|-------------------|--------|
| 8 | 8 bytes | Lower |
| 16 | 16 bytes | Medium |
| 32 | 32 bytes | Good |
| 48 | 48 bytes | Better |
| 64 | 64 bytes | Best |

---

## Performance Optimization

### Pre-filtering Strategy

For hybrid queries, filter order matters:

```cypher
// Good: Vector search first, then filter
CALL db.idx.vector.query('Paper', 'embedding', $query_vector, 100)
YIELD node AS paper, distance
WHERE paper.year >= 2020  // Filter after vector search
RETURN paper.title, distance
LIMIT 10

// Alternative: Over-fetch then filter
CALL db.idx.vector.query('Paper', 'embedding', $query_vector, 500)
YIELD node AS paper, distance
WHERE paper.year >= 2020 AND paper.venue = 'NeurIPS'
RETURN paper.title, distance
LIMIT 10
```

### Batch Queries

For multiple queries, batch them:

```rust
// Process multiple query vectors efficiently
let queries = vec![query1, query2, query3];
let results = storage.batch_vector_search(
    "Paper",
    "embedding",
    &queries,
    10  // k per query
).await?;
```

### Caching Query Vectors

Pre-compute and cache frequent query embeddings:

```cypher
// Store computed query embedding
CREATE (q:Query {
  text: 'transformer architectures',
  embedding: $precomputed_embedding,
  created_at: datetime()
})

// Reuse later
MATCH (q:Query {text: 'transformer architectures'})
CALL db.idx.vector.query('Paper', 'embedding', q.embedding, 10)
YIELD node, distance
RETURN node.title, distance
```

---

## Use Cases

### Semantic Document Search

```cypher
// Find documents similar to a natural language query
WITH $query_embedding AS query_vec
CALL db.idx.vector.query('Document', 'content_embedding', query_vec, 20)
YIELD node AS doc, distance
RETURN doc.title, doc.summary, distance
ORDER BY distance
LIMIT 10
```

### Recommendation System

```cypher
// Find products similar to what user viewed
MATCH (u:User {id: $user_id})-[:VIEWED]->(viewed:Product)
WITH COLLECT(viewed.embedding) AS viewed_embeddings

// Average the embeddings (simplified)
WITH reduce(sum = [0.0]*384, e IN viewed_embeddings |
  [i IN range(0, 383) | sum[i] + e[i]]) AS summed,
  size(viewed_embeddings) AS count
WITH [x IN summed | x / count] AS avg_embedding

CALL db.idx.vector.query('Product', 'embedding', avg_embedding, 20)
YIELD node AS product, distance
WHERE NOT EXISTS((u)-[:VIEWED]->(product))  // Exclude already viewed
RETURN product.name, product.price, distance
LIMIT 10
```

### Duplicate Detection

```cypher
// Find near-duplicate documents
MATCH (d:Document)
CALL db.idx.vector.query('Document', 'embedding', d.embedding, 5)
YIELD node AS similar, distance
WHERE similar.id <> d.id AND distance < 0.1  // Very similar
RETURN d.title, similar.title, distance
```

### Clustering via Vector Search

```cypher
// Find clusters of similar papers
MATCH (seed:Paper)
WHERE seed.citations > 100  // Start from influential papers
CALL db.idx.vector.query('Paper', 'embedding', seed.embedding, 20)
YIELD node AS similar, distance
WHERE distance < 0.3
RETURN seed.title AS cluster_center, COLLECT(similar.title) AS cluster_members
```

---

## Troubleshooting

### Low Recall

**Symptoms:** Missing expected results

**Solutions:**
1. Increase `ef_search` (HNSW) or `num_probes` (IVF)
2. Increase K and post-filter
3. Check embedding model consistency (same model for indexing and querying)
4. Verify dimension matches

### Slow Queries

**Symptoms:** High latency on vector search

**Solutions:**
1. Reduce `ef_search` if recall is acceptable
2. Use IVF_PQ instead of HNSW for large datasets
3. Pre-filter with scalar indexes when possible
4. Ensure index is built (not building)

### Memory Issues

**Symptoms:** OOM during indexing or queries

**Solutions:**
1. Switch to IVF_PQ (compressed vectors)
2. Reduce HNSW `m` parameter
3. Shard data across multiple indexes
4. Use streaming index build

---

## Next Steps

- [Indexing](../concepts/indexing.md) — All index types and configuration
- [Performance Tuning](performance-tuning.md) — Optimization strategies
- [Data Ingestion](data-ingestion.md) — Import data with embeddings
