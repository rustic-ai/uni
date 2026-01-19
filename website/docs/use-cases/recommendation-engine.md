# Recommendation Engines

Modern recommendation systems are hybrids: they use **collaborative filtering** (graph-based: "users like you bought...") and **content-based filtering** (vector-based: "products similar to this..."). Uni handles both in a single query engine.

## Why Uni for RecSys?

| Challenge | Traditional Approach | Uni Approach |
|-----------|----------------------|--------------|
| **Cold Start** | Graph algorithms fail for new items (no edges). | **Vector Search**: Find items similar to user's interest description. |
| **Diversity** | Vector search returns near-duplicates. | **Graph Expansion**: Boost score if item is in a cluster "liked" by friends. |
| **Real-time** | Pre-compute recommendations batch (stale). | **On-demand**: Generate candidates at query time using live history. |

---

## Scenario: E-Commerce Personalization

We want to recommend products to a user based on:
1.  **Semantic Match**: Products matching their search query (Vector).
2.  **Social Proof**: Products purchased by other users who bought the *same* items as the target user (Graph).

### 1. Schema Definition

**`schema.json`**
```json
{
  "labels": {
    "User": { "id": 1 },
    "Product": { "id": 2 },
    "Category": { "id": 3 }
  },
  "edge_types": {
    "VIEWED": { "id": 1, "src_labels": ["User"], "dst_labels": ["Product"] },
    "PURCHASED": { "id": 2, "src_labels": ["User"], "dst_labels": ["Product"] },
    "IN_CATEGORY": { "id": 3, "src_labels": ["Product"], "dst_labels": ["Category"] }
  },
  "properties": {
    "Product": {
      "name": { "type": "String", "nullable": false },
      "price": { "type": "Float64", "nullable": false },
      "embedding": { "type": "Vector", "dimensions": 384 } // Image + Text embedding
    }
  },
  "indexes": [
    {
      "type": "Vector",
      "name": "product_embedding",
      "label": "Product",
      "property": "embedding",
      "index_type": { "IvfPq": { "num_partitions": 1024, "num_sub_vectors": 16 } },
      "metric": "Cosine"
    }
  ]
}
```

### 2. Configuration

We use **IVF_PQ** for the vector index to reduce memory usage, allowing us to keep a larger portion of the graph in the **Adjacency Cache**.

**`uni.toml`**
```toml
[storage]
adjacency_cache_size = 2000000 # Cache large parts of user-product graph

[executor]
# Parallelize scoring across many candidate products
worker_threads = 16 
```

### 3. Hybrid Query

This query performs a "Vector-to-Graph" re-ranking pipeline.

1.  **Candidate Generation**: Find 50 products semantically similar to the user's current search (e.g., "running shoes").
2.  **Scoring**:
    *   Base score = Vector similarity.
    *   Boost = Count of purchases by *similar* users (Collaborative Filtering signal).

```cypher
// 1. Generate Candidates (Content-Based)
CALL db.idx.vector.query('Product', 'embedding', $search_embedding, 50)
YIELD node AS product, distance

// 2. Calculate Social Score (Collaborative)
// Find users who bought this product
MATCH (other_user:User)-[:PURCHASED]->(product)

// (Optional) Ensure 'other_user' has some overlap with current user
// MATCH (current_user)-[:PURCHASED]->(:Product)<-[:PURCHASED]-(other_user) ...

// 3. Aggregate and Re-rank
RETURN 
    product.name, 
    product.price,
    distance AS semantic_score,
    COUNT(other_user) AS popularity_score,
    // Final score: similarity weighted by log of popularity
    (1.0 - distance) + LOG(1 + COUNT(other_user)) * 0.5 AS final_score
ORDER BY final_score DESC
LIMIT 10
```

### Key Advantages

*   **No ETL**: You don't need to export data to Spark to run collaborative filtering. It happens in the DB.
*   **Vector-Native**: Unlike Lucene-based search engines, the vector index is part of the query planner, allowing seamless joining with graph data.
*   **Flexibility**: You can tweak the weighting formula (`0.5` in the example) instantly without retraining models.
