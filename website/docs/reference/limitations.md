# Current Limitations

This document describes the known limitations in the current version of Uni. These are areas where functionality is either partially implemented, not yet available, or has known constraints.

---

## Storage Limitations

### Cloud Storage (S3/GCS/Azure)

**Status:** Supported (Hybrid Mode)

Uni supports cloud storage backends using a hybrid architecture: local WAL/metadata for low latency, cloud storage for bulk data.

| Backend | Status |
|---------|--------|
| Local filesystem | ✅ Fully supported |
| In-memory | ✅ Fully supported (testing) |
| Amazon S3 | ✅ Supported (hybrid mode) |
| Google Cloud Storage | ✅ Supported (hybrid mode) |
| Azure Blob Storage | ✅ Supported (hybrid mode) |
| S3-compatible (MinIO) | ✅ Supported |

**Hybrid Mode:**

Hybrid mode keeps WAL and ID allocation local while storing bulk vertex/edge data in cloud object storage:

```rust
let db = Uni::open("./local_meta")
    .hybrid("./local_meta", "s3://my-bucket/graph-data")
    .build()
    .await?;
```

**Cloud URLs in Commands:**

BACKUP, COPY, and EXPORT commands support cloud URLs:

```cypher
BACKUP TO 's3://backup-bucket/snapshot'
COPY Person FROM 'gs://data-bucket/people.parquet'
EXPORT Person TO 'az://account/container/export.parquet'
```

**Configuration:**

See [Cloud Storage Configuration](configuration.md#cloud-storage-configuration) for detailed setup instructions.

**Limitations:**

- Pure cloud mode (no local storage) is not recommended due to WAL latency
- Cloud operations may have higher latency than local storage
- Network failures can cause transient errors (automatic retry is configured)

---

## Query Language Limitations

### Regular Expression Matching

**Status:** Not Implemented

The Cypher `=~` operator for regular expression matching is not supported:

```cypher
-- NOT SUPPORTED
MATCH (p:Paper)
WHERE p.title =~ '.*Transform.*'
RETURN p
```

**Workaround:** Use `CONTAINS`, `STARTS WITH`, or `ENDS WITH` for pattern matching:

```cypher
-- Use CONTAINS instead
MATCH (p:Paper)
WHERE p.title CONTAINS 'Transform'
RETURN p

-- Or STARTS WITH / ENDS WITH
MATCH (p:Paper)
WHERE p.title STARTS WITH 'Attention'
RETURN p
```

### shortestPath Multi-Hop Patterns

**Status:** Partial Support

The `shortestPath` function only supports simple 1-hop patterns:

```cypher
-- SUPPORTED: Single relationship type with variable length
MATCH path = shortestPath((a:Person)-[:KNOWS*]-(b:Person))
RETURN path

-- NOT SUPPORTED: Range specifiers
MATCH path = shortestPath((a:Person)-[:KNOWS*1..5]-(b:Person))
RETURN path

-- NOT SUPPORTED: Multiple relationship types
MATCH path = shortestPath((a)-[:KNOWS|WORKS_WITH*]-(b))
RETURN path
```

**Technical Details:** The planner explicitly checks for exactly 3 pattern parts `[source_node, relationship, target_node]` and rejects more complex patterns.

**Workaround:** Use variable-length path patterns with explicit bounds:

```cypher
-- Find paths up to 5 hops manually
MATCH path = (a:Person {name: 'Alice'})-[:KNOWS*1..5]-(b:Person {name: 'Bob'})
RETURN path, length(path) AS hops
ORDER BY hops
LIMIT 1
```

### DELETE/SET on Matched Patterns

**Status:** Partial Support

Write operations (`DELETE`, `SET`, `REMOVE`) have some constraints:

- `DELETE` works on explicitly bound variables
- `DETACH DELETE` removes nodes and their relationships
- `SET` on properties works for simple cases

**Known Issues:**

```cypher
-- May not work as expected in complex patterns
MATCH (a)-[r]->(b)
WHERE a.name = 'Alice'
DELETE r
```

**Workaround:** Use explicit variable binding and simpler patterns for mutations.

---

## Index Limitations

### BTree Index for STARTS WITH

**Status:** Partial (Residual Evaluation Only)

While BTree indexes exist and `STARTS WITH` queries work, the query planner does not push `STARTS WITH` predicates down to BTree indexes for acceleration:

```cypher
-- Works, but uses residual evaluation (post-load filtering)
-- rather than index-accelerated prefix scan
MATCH (p:Person)
WHERE p.name STARTS WITH 'John'
RETURN p
```

**Impact:** For large datasets, `STARTS WITH` queries may be slower than expected because the index is not used for prefix matching.

**Workaround:** For performance-critical prefix searches, consider using a Hash index with exact matches, or use Full-Text Search with `CONTAINS`.

### Vector Index Limitations

**Status:** Functional with Constraints

- HNSW is the only supported vector index algorithm
- Index must be created before inserting vectors for optimal performance
- Rebuilding vector indexes on large datasets can be time-consuming

---

## Concurrency Limitations

### Single-Writer Model

**Status:** By Design

Uni uses a single-writer, multi-reader concurrency model:

- Only one write transaction can be active at a time
- Multiple read transactions can run concurrently
- Readers see a consistent snapshot and are never blocked by writers

**Implications:**

- Write throughput is limited to sequential operations
- Long-running write transactions block other writes
- Suitable for embedded/single-process deployments

**Workaround:** Use batch operations (`BulkWriter`) for high-throughput ingestion. Structure applications to minimize write transaction duration.

### No Distributed Mode

**Status:** Not Available

Uni is an embedded database and does not support distributed deployments:

- No built-in replication
- No sharding across nodes
- No distributed transactions

**Workaround:** For high-availability needs, use application-level replication or deploy behind a load balancer with read replicas using snapshot-based synchronization.

---

## Algorithm Limitations

### Graph Algorithm Scope

**Status:** Functional with Constraints

The 35 built-in graph algorithms operate on in-memory subgraphs:

- Algorithms load relevant data into memory before execution
- Very large graphs may exceed available memory
- No streaming/incremental algorithm execution

**Memory Consideration:**

```rust
// For large graphs, filter to relevant subgraph
let results = db.query(r#"
    CALL algo.pageRank(['Person'], ['KNOWS'])
    YIELD nodeId, score
    RETURN nodeId, score
    LIMIT 100
"#).await?;
```

**Workaround:** Use label and edge type filters to reduce the working set. For very large graphs, consider sampling or partitioning strategies.

---

## Schema Limitations

### No Schema Evolution for Properties

**Status:** Partial Support

While labels and edge types can be created/dropped, property schema changes have constraints:

- Adding new properties: ✅ Supported
- Removing properties: ⚠️ Marks as deprecated, data remains
- Changing property types: ❌ Not supported
- Renaming properties: ❌ Not supported

**Workaround:** Create a new property with the desired type and migrate data manually:

```cypher
// Add new property
MATCH (p:Person)
SET p.age_new = toInteger(p.age_string)

// Remove old property reference from schema
// (data remains but is no longer queryable by name)
```

---

## API Limitations

### Python API Synchronous Only

**Status:** By Design

The Python bindings are synchronous (blocking):

```python
# Python API is synchronous
db = uni.Database("./my-graph")
results = db.query("MATCH (n) RETURN n")  # Blocks until complete
```

**Rationale:** Simplifies Python integration. The underlying Rust runtime handles async internally.

**Workaround:** For async Python applications, run Uni operations in a thread pool:

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=4)

async def async_query(db, cypher):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, db.query, cypher)
```

### No Streaming Results in Python

**Status:** Not Available

Python API returns complete result sets:

```python
# Returns all results at once
results = db.query("MATCH (n) RETURN n")

# No cursor/streaming API available in Python
# (Rust has query_cursor() for streaming)
```

**Workaround:** Use `LIMIT` and `SKIP` for pagination:

```python
offset = 0
batch_size = 1000
while True:
    results = db.query(f"MATCH (n) RETURN n SKIP {offset} LIMIT {batch_size}")
    if not results:
        break
    process(results)
    offset += batch_size
```

---

## Summary Table

| Limitation | Category | Severity | Workaround Available |
|------------|----------|----------|---------------------|
| Cloud Storage (S3/GCS/Azure) | Storage | Low | Supported via hybrid mode |
| Regular Expression (`=~`) | Query | Medium | Use CONTAINS/STARTS WITH |
| shortestPath multi-hop | Query | Low | Variable-length paths |
| BTree STARTS WITH pushdown | Index | Low | Use FTS or Hash index |
| Single-writer model | Concurrency | Medium | Batch operations |
| No distributed mode | Architecture | High | Application-level replication |
| No streaming in Python | API | Low | Pagination with SKIP/LIMIT |
| Schema type changes | Schema | Medium | Manual migration |

---

## Reporting Issues

If you encounter limitations not documented here, please report them:

- GitHub Issues: [rustic-ai/uni/issues](https://github.com/rustic-ai/uni/issues)
- Include: Uni version, query/code that fails, expected vs actual behavior
