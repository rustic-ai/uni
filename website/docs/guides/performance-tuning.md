# Performance Tuning Guide

This guide covers strategies for optimizing Uni's performance across query execution, storage, indexing, and resource utilization.

## Performance Overview

Uni's performance characteristics:

| Operation | Typical Latency | Optimization Target |
|-----------|-----------------|---------------------|
| Point lookup | 2-5ms | Index usage |
| 1-hop traversal | 4-8ms | Adjacency cache |
| Vector KNN (k=10) | 1-3ms | Index tuning |
| Aggregation (1M rows) | 50-200ms | Predicate pushdown |
| Bulk insert (10K) | 5-10ms | Batch size |

---

## Query Optimization

### 1. Use Predicate Pushdown

Push filters to storage for massive I/O reduction:

```cypher
// Good: Filter pushed to Lance
MATCH (p:Paper)
WHERE p.year > 2020 AND p.venue = 'NeurIPS'
RETURN p.title

// Bad: Filter applied after full scan
MATCH (p:Paper)
WHERE p.title CONTAINS 'Transformer'  // Cannot push CONTAINS
RETURN p.title
```

**Pushable Predicates:**
- `=`, `<>`, `<`, `>`, `<=`, `>=`
- `IN [list]`
- `IS NULL`, `IS NOT NULL`
- `AND` combinations of above

**Non-Pushable Predicates:**
- `CONTAINS`, `STARTS WITH`, `ENDS WITH`
- Function calls: `lower(x) = 'value'`
- `OR` with different properties

### 2. Limit Early

Apply LIMIT as early as possible:

```cypher
// Good: Limit applied early in pipeline
MATCH (p:Paper)
WHERE p.year > 2020
RETURN p.title
ORDER BY p.year DESC
LIMIT 10

// Bad: Process all then limit
MATCH (p:Paper)-[:CITES]->(cited)
WITH p, COUNT(cited) AS citation_count
ORDER BY citation_count DESC
RETURN p.title, citation_count
LIMIT 10  // All citations computed before limit
```

### 3. Project Only Needed Properties

Don't fetch unnecessary properties:

```cypher
// Good: Only fetch needed properties
MATCH (p:Paper)
RETURN p.title, p.year

// Bad: Fetch all properties
MATCH (p:Paper)
RETURN p  // Loads all properties including large ones

// Worse: Return unused properties
MATCH (p:Paper)
RETURN p.title, p.abstract, p.embedding  // embedding loaded but unused
```

### 4. Use Indexes

Ensure indexes exist for filter properties:

```cypher
-- Check if index is used
EXPLAIN MATCH (p:Paper) WHERE p.year = 2023 RETURN p.title

-- Create index if missing
CREATE INDEX paper_year FOR (p:Paper) ON (p.year)
```

### 5. Optimize Traversal Patterns

Structure patterns for efficient execution:

```cypher
// Good: Filter before traverse
MATCH (p:Paper)
WHERE p.year > 2020
MATCH (p)-[:CITES]->(cited)
RETURN p.title, cited.title

// Good: Traverse from smaller set
MATCH (seed:Paper {title: 'Attention Is All You Need'})
MATCH (seed)-[:CITES]->(cited)
RETURN cited.title

// Bad: Full cross-product
MATCH (p1:Paper), (p2:Paper)
WHERE p1.title = p2.title  // Cartesian join
RETURN p1, p2
```

---

## Index Tuning

### Vector Index Configuration

#### HNSW Tuning

| Parameter | Default | Low Latency | High Recall |
|-----------|---------|-------------|-------------|
| `m` | 16 | 16 | 48 |
| `ef_construction` | 200 | 100 | 500 |
| `ef_search` | 100 | 50 | 200 |

```cypher
// High recall configuration
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper) ON p.embedding
OPTIONS {
  index_type: "hnsw",
  metric: "cosine",
  m: 48,
  ef_construction: 400
}
```

#### IVF_PQ Tuning

| Parameter | Default | Memory Optimized | Recall Optimized |
|-----------|---------|------------------|------------------|
| `num_partitions` | 256 | 512 | 256 |
| `num_sub_vectors` | 8 | 8 | 48 |
| `num_probes` | 20 | 10 | 50 |

### Scalar Index Selection

| Query Pattern | Index Type | Notes |
|---------------|------------|-------|
| Equality (`= value`) | Hash | O(1) lookup |
| Range (`> value`) | BTree | Range scan |
| Low cardinality | Bitmap | Efficient for categories |
| High cardinality unique | Hash | Best for IDs |

```cypher
-- Hash for exact match (faster)
CREATE INDEX paper_doi FOR (p:Paper) ON (p.doi) OPTIONS { type: "hash" }

-- BTree for range queries
CREATE INDEX paper_year FOR (p:Paper) ON (p.year) OPTIONS { type: "btree" }

-- Bitmap for categories
CREATE INDEX paper_venue FOR (p:Paper) ON (p.venue) OPTIONS { type: "bitmap" }
```

### Composite Indexes

Create composite indexes for common filter combinations:

```cypher
-- Composite index for common query pattern
CREATE INDEX paper_venue_year FOR (p:Paper) ON (p.venue, p.year)

-- Query uses the composite index
MATCH (p:Paper)
WHERE p.venue = 'NeurIPS' AND p.year > 2020
RETURN p.title
```

---

## Storage Optimization

### Batch Size Tuning

Tune batch sizes for your workload:

```bash
# Import with larger batches (more memory, faster)
uni import data --batch-size 50000 ...

# Import with smaller batches (less memory)
uni import data --batch-size 5000 ...
```

**Guidelines:**
- Increase batch size if memory allows (faster)
- Decrease if OOM errors occur
- Default (10000) is good for most cases

### L0 Buffer Configuration

Tune the in-memory write buffer:

```rust
use std::time::Duration;

let config = UniConfig {
    // Mutation-based flush (high-transaction systems)
    auto_flush_threshold: 10_000,  // Flush at 10K mutations

    // Time-based flush (low-transaction systems)
    auto_flush_interval: Some(Duration::from_secs(5)),  // Flush every 5s
    auto_flush_min_mutations: 1,  // If at least 1 mutation pending

    ..Default::default()
};
```

**Trade-offs:**
- Larger threshold: Better write throughput, higher memory, longer recovery
- Smaller threshold: Lower memory, more frequent flushes, faster recovery
- Shorter interval: Lower data-at-risk, more I/O overhead
- Longer interval: Less I/O overhead, more data-at-risk on crash

### Auto-Flush Tuning

Choose flush strategy based on workload:

| Workload | Recommended Settings | Rationale |
|----------|---------------------|-----------|
| High-transaction OLTP | `threshold: 10_000`, `interval: None` | Mutation count drives flush |
| Low-transaction | `threshold: 10_000`, `interval: 5s` | Time ensures eventual flush |
| Critical data | `threshold: 1_000`, `interval: 1s` | Minimize data at risk |
| Cost-sensitive cloud | `threshold: 50_000`, `interval: 30s` | Reduce API calls |
| Batch import | `threshold: 100_000`, `interval: None` | Maximum throughput |

```rust
// High-transaction system (default)
let config = UniConfig {
    auto_flush_threshold: 10_000,
    auto_flush_interval: Some(Duration::from_secs(5)),
    ..Default::default()
};

// Cost-sensitive cloud workload
let config = UniConfig {
    auto_flush_threshold: 50_000,
    auto_flush_interval: Some(Duration::from_secs(30)),
    auto_flush_min_mutations: 100,  // Batch up small writes
    ..Default::default()
};

// Critical data, minimize loss
let config = UniConfig {
    auto_flush_threshold: 1_000,
    auto_flush_interval: Some(Duration::from_secs(1)),
    ..Default::default()
};
```

### Compaction

Trigger compaction after bulk operations:

```bash
# Manual compaction
uni compact --path ./storage

# Compaction levels
uni compact --path ./storage --level l1  # L0 → L1 only
uni compact --path ./storage --level l2  # Full compaction
```

---

## Cache Configuration

### Adjacency Cache

The CSR adjacency cache is critical for traversal performance:

```rust
let storage = StorageManager::with_config(
    path,
    schema_manager,
    StorageConfig {
        adjacency_cache_size: 1_000_000,  // Max cached vertices
        adjacency_cache_ttl: Duration::from_secs(3600),
    }
);
```

**Sizing Guidelines:**
- Size for your "hot" working set
- Monitor cache hit ratio
- Increase if traversals are slow after warmup

### Property Cache

Configure the property LRU cache:

```rust
let prop_manager = PropertyManager::with_config(
    storage,
    schema_manager,
    PropertyConfig {
        cache_capacity: 100_000,  // Cached property entries
        batch_load_size: 1000,    // Properties per batch load
    }
);
```

---

## Query Analysis

### EXPLAIN

View the query plan without execution:

```bash
uni query "MATCH (p:Paper) WHERE p.year > 2020 RETURN p.title" \
    --explain --path ./storage
```

Output:
```
Query Plan:
├── Project [p.title]
│   └── Scan [:Paper]
│         ↳ Index: paper_year (year > 2020)
│         ↳ Pushdown: year > 2020

Estimated rows: 5,000
Index usage: BTree (paper_year)
```

### PROFILE

Execute with timing breakdown:

```bash
uni query "MATCH (p:Paper)-[:CITES]->(c) RETURN COUNT(c)" \
    --profile --path ./storage
```

Output:
```
┌───────────┐
│ COUNT(c)  │
├───────────┤
│ 45,231    │
└───────────┘

Execution Profile:
  Parse:      0.8ms
  Plan:       1.2ms
  Execute:    42.3ms
    ├── Scan:       12.1ms (28.6%)  [10,000 rows]
    ├── Traverse:   24.5ms (57.9%)  [45,231 edges]
    └── Aggregate:   5.7ms (13.5%)  [1 row]
  Total:      44.3ms
```

### Identifying Bottlenecks

| Profile Pattern | Likely Cause | Solution |
|-----------------|--------------|----------|
| High Scan time | No index, large result set | Add index, add filters |
| High Traverse time | Cold cache, many edges | Warm cache, limit hops |
| High Aggregate time | Large group count | Add LIMIT, pre-aggregate |
| High memory | Large intermediate results | Stream results, limit |

---

## Parallel Execution

### Morsel-Driven Parallelism

Uni uses morsel-driven parallelism for large queries:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PARALLEL EXECUTION                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Source Data: [────────────────────────────────────────────────]           │
│                         │                                                   │
│                         ▼                                                   │
│   Morsels:     [────] [────] [────] [────] [────] [────]                   │
│                  │       │       │       │       │       │                  │
│                  ▼       ▼       ▼       ▼       ▼       ▼                  │
│   Workers:     [W1]   [W2]   [W3]   [W4]   [W1]   [W2]                     │
│                  │       │       │       │       │       │                  │
│                  └───────┴───────┴───────┴───────┴───────┘                  │
│                                   │                                         │
│                                   ▼                                         │
│   Merge:                     [Results]                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Concurrency Configuration

```rust
let executor = Executor::with_config(
    storage,
    ExecutorConfig {
        worker_threads: 8,       // Parallel workers
        morsel_size: 4096,       // Rows per morsel
        max_concurrent_io: 16,   // Parallel I/O operations
    }
);
```

**Guidelines:**
- Set workers to CPU core count
- Increase morsel size for simpler queries
- Decrease morsel size for complex operators

---

## Memory Management

### Memory Budget

Monitor and limit memory usage:

```bash
# Monitor memory during query
RUST_LOG=uni_db=debug uni query "..." 2>&1 | grep -i memory

# Set memory limits
export UNI_MAX_MEMORY_MB=4096
```

### Reducing Memory Usage

1. **Smaller batch sizes**: `--batch-size 5000`
2. **Smaller caches**: Reduce cache capacities
3. **Stream large results**: Use SKIP/LIMIT pagination
4. **Avoid large intermediates**: Filter early

### Memory Profile

```rust
// Enable memory tracking
let storage = StorageManager::with_config(
    path,
    schema_manager,
    StorageConfig {
        enable_memory_tracking: true,
        memory_limit_bytes: 4 * 1024 * 1024 * 1024,  // 4 GB
    }
);

// Query memory stats
let stats = storage.memory_stats();
println!("Adjacency cache: {} MB", stats.adjacency_cache_mb);
println!("Property cache: {} MB", stats.property_cache_mb);
println!("L0 buffer: {} MB", stats.l0_buffer_mb);
```

---

## I/O Optimization

### Cloud Storage Configuration

Uni supports multiple cloud storage backends with automatic credential resolution:

```rust
use uni_common::CloudStorageConfig;

// Amazon S3
let db = Uni::open("./local-cache")
    .cloud_storage(CloudStorageConfig {
        url: "s3://my-bucket/graph-data".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: None,  // Use default AWS endpoint
        ..Default::default()
    })
    .build()
    .await?;

// Google Cloud Storage
let db = Uni::open("./local-cache")
    .cloud_storage(CloudStorageConfig {
        url: "gs://my-bucket/graph-data".to_string(),
        ..Default::default()
    })
    .build()
    .await?;

// S3-compatible (MinIO, LocalStack)
let db = Uni::open("./local-cache")
    .cloud_storage(CloudStorageConfig {
        url: "s3://my-bucket/graph-data".to_string(),
        endpoint: Some("http://localhost:9000".to_string()),
        ..Default::default()
    })
    .build()
    .await?;
```

### Hybrid Mode for Optimal Performance

Use hybrid mode (local + cloud) for best write latency with cloud durability:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HYBRID MODE PERFORMANCE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Operation          Local-Only    Cloud-Only    Hybrid Mode               │
│   ─────────────────────────────────────────────────────────────────────────│
│   Single write       ~50µs         ~100ms        ~50µs (local L0)          │
│   Batch 1K writes    ~550µs        ~150ms        ~550µs (local L0)         │
│   Point read (cold)  ~3ms          ~100ms        ~100ms (first access)     │
│   Point read (warm)  ~3ms          ~3ms          ~3ms (cached)             │
│   Durability         Local disk    Cloud         Cloud (after flush)       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Best Practice:** Use hybrid mode when:
- Write latency matters (< 1ms)
- Data must ultimately reside in cloud storage
- You have local SSD for the write cache

### Auto-Flush Tuning for Cloud

Optimize flush interval for cloud cost vs. durability:

| Cloud Provider | Recommended Interval | Rationale |
|----------------|---------------------|-----------|
| S3 | 5-30s | Balance PUT request costs |
| GCS | 5-30s | Similar to S3 |
| Azure Blob | 5-30s | Similar to S3 |
| Local SSD | 1-5s | No cost concern, minimize data at risk |

```rust
// Cost-optimized for cloud (fewer API calls)
let db = Uni::open("./local-cache")
    .cloud_storage(CloudStorageConfig {
        url: "s3://my-bucket/data".to_string(),
        ..Default::default()
    })
    .auto_flush_threshold(50_000)
    .auto_flush_interval(Duration::from_secs(30))
    .auto_flush_min_mutations(100)
    .build()
    .await?;
```

### Read-Ahead

Configure read-ahead for sequential scans:

```rust
let config = StorageConfig {
    read_ahead_size: 64 * 1024 * 1024,  // 64 MB
    prefetch_enabled: true,
};
```

---

## Benchmarking

### Built-in Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- vector_search

# Save baseline
cargo bench -- --save-baseline main

# Compare to baseline
cargo bench -- --baseline main
```

### Custom Benchmarks

```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn benchmark_traversal(c: &mut Criterion) {
    let storage = setup_storage();

    c.bench_function("1-hop traversal", |b| {
        b.iter(|| {
            let query = "MATCH (p:Paper)-[:CITES]->(c) RETURN COUNT(c)";
            executor.execute(query).unwrap()
        })
    });
}

criterion_group!(benches, benchmark_traversal);
criterion_main!(benches);
```

---

## Performance Checklist

Before deploying to production:

- [ ] Indexes created for filter properties
- [ ] Vector indexes tuned for recall/latency trade-off
- [ ] Batch sizes tuned for workload
- [ ] Cache sizes appropriate for working set
- [ ] Queries use pushable predicates where possible
- [ ] LIMIT applied early in query patterns
- [ ] Only needed properties projected
- [ ] Memory limits configured
- [ ] I/O timeouts set for remote storage
- [ ] Monitoring enabled for cache hit rates

---

## Next Steps

- [Architecture](../concepts/architecture.md) — Understand system internals
- [Vectorized Execution](../internals/vectorized-execution.md) — Batch processing details
- [Storage Engine](../internals/storage-engine.md) — Storage layer optimization
- [Benchmarks](../internals/benchmarks.md) — Performance metrics
