# Troubleshooting Guide

This guide covers common issues, error messages, and their solutions when working with Uni.

## Quick Diagnostics

```bash
# Check storage health
uni stats --path ./storage

# Validate schema
uni schema validate --path ./storage

# Check for corruption
uni check --path ./storage

# View recent logs
RUST_LOG=uni=debug uni query "RETURN 1" --path ./storage 2>&1 | tail -50
```

---

## Common Issues

### Installation Problems

#### Rust Version Incompatible

**Symptom:**
```
error: package `uni v0.1.0` cannot be built because it requires rustc 1.75 or newer
```

**Solution:**
```bash
# Update Rust
rustup update stable

# Verify version
rustc --version  # Should be 1.75+
```

#### Missing System Dependencies

**Symptom:**
```
error: failed to run custom build command for `openssl-sys`
```

**Solution:**
```bash
# Ubuntu/Debian
sudo apt install pkg-config libssl-dev

# macOS
brew install openssl
export OPENSSL_DIR=$(brew --prefix openssl)

# Fedora
sudo dnf install openssl-devel
```

#### Build Fails with SIMD Errors

**Symptom:**
```
error: unknown feature 'avx2'
```

**Solution:**
```bash
# Build without SIMD optimizations
cargo build --release --no-default-features

# Or specify target CPU
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

---

### Storage Issues

#### Cannot Open Storage

**Symptom:**
```
Error: Failed to open storage at ./storage: No such file or directory
```

**Solution:**
```bash
# Check path exists
ls -la ./storage

# Initialize new storage
uni import my-data --papers data.jsonl --citations edges.jsonl --output ./storage

# Or create programmatically
let storage = StorageManager::new("./storage", schema);
```

#### Corrupted Storage

**Symptom:**
```
Error: Invalid manifest at version 42: CRC mismatch
```

**Solution:**
```bash
# Check integrity
uni check --path ./storage --verbose

# Repair if possible
uni repair --path ./storage

# Rollback to previous version
uni rollback --path ./storage --version 41

# Last resort: re-import
uni import my-data --papers data.jsonl --output ./storage --force
```

#### Out of Disk Space

**Symptom:**
```
Error: Failed to write to storage: No space left on device
```

**Solution:**
```bash
# Check disk usage
df -h ./storage

# Compact storage (removes old versions)
uni compact --path ./storage --level l2

# Clean old WAL segments
uni wal clean --path ./storage --keep-count 2

# Increase disk or move storage
uni migrate --from ./storage --to /larger-disk/storage
```

#### Lock File Issues

**Symptom:**
```
Error: Storage is locked by another process
```

**Solution:**
```bash
# Check for running processes
lsof ./storage/.lock

# Force unlock (only if no other process is actually using it!)
rm ./storage/.lock

# In code, ensure proper cleanup
drop(storage);  // Explicit drop releases lock
```

---

### Query Issues

#### Parse Errors

**Symptom:**
```
Error: Parse error at line 1, column 15: unexpected token
```

**Common Causes:**

1. **Missing quotes around strings:**
   ```cypher
   // Wrong
   WHERE p.title = My Paper

   // Correct
   WHERE p.title = 'My Paper'
   ```

2. **Wrong comparison operator:**
   ```cypher
   // Wrong (SQL style)
   WHERE p.year == 2023

   // Correct (Cypher style)
   WHERE p.year = 2023
   ```

3. **Missing relationship direction:**
   ```cypher
   // Wrong
   MATCH (a)-[r]-(b)  // Ambiguous in some contexts

   // Better (explicit direction)
   MATCH (a)-[r]->(b)
   ```

#### Semantic Errors

**Symptom:**
```
Error: Unknown label 'Paper'
```

**Solution:**
```bash
# List available labels
uni schema show --path ./storage

# Check spelling/case
# Labels are case-sensitive: Paper != paper
```

**Symptom:**
```
Error: Unknown property 'year' for label 'Paper'
```

**Solution:**
```bash
# List properties for label
uni schema show --path ./storage --label Paper

# Add property to schema if needed
uni schema update --path ./storage --add-property Paper.year:Int32
```

#### Query Timeout

**Symptom:**
```
Error: Query timeout after 300 seconds
```

**Solutions:**

1. **Add LIMIT:**
   ```cypher
   MATCH (p:Paper)-[:CITES]->(cited)
   RETURN p.title, cited.title
   LIMIT 1000  -- Add limit
   ```

2. **Add filters:**
   ```cypher
   MATCH (p:Paper)-[:CITES]->(cited)
   WHERE p.year > 2020  -- Filter early
   RETURN p.title, cited.title
   ```

3. **Increase timeout:**
   ```rust
   let config = ExecutorConfig {
       timeout: Duration::from_secs(600),  // 10 minutes
       ..Default::default()
   };
   ```

4. **Check query plan:**
   ```bash
   uni query "EXPLAIN MATCH (p:Paper)..." --path ./storage
   # Look for missing indexes
   ```

#### Out of Memory

**Symptom:**
```
Error: Query execution failed: Out of memory
```

**Solutions:**

1. **Reduce result size:**
   ```cypher
   MATCH (p:Paper)
   RETURN p.title  -- Only needed columns
   LIMIT 10000     -- Reasonable limit
   ```

2. **Stream results:**
   ```rust
   let stream = executor.execute_stream(query).await?;
   while let Some(row) = stream.next().await {
       // Process one row at a time
   }
   ```

3. **Increase memory limit:**
   ```rust
   let config = ExecutorConfig {
       memory_limit: 8 * 1024 * 1024 * 1024,  // 8 GB
       ..Default::default()
   };
   ```

4. **Reduce batch size:**
   ```rust
   let config = ExecutorConfig {
       batch_size: 1024,  // Smaller batches
       morsel_size: 1024,
       ..Default::default()
   };
   ```

---

### Index Issues

#### Vector Search Returns Poor Results

**Symptom:** Results are not semantically similar to the query.

**Causes and Solutions:**

1. **Wrong embedding model:**
   ```rust
   // Ensure same model for indexing and querying
   let service = FastEmbedService::new(FastEmbedModel::BGEBaseENV15)?;
   // Use the same model everywhere!
   ```

2. **Dimension mismatch:**
   ```bash
   # Check schema dimensions
   uni schema show --path ./storage --label Paper
   # Embedding: Vector[768]

   # Ensure query vector matches
   assert_eq!(query_vec.len(), 768);
   ```

3. **Low ef_search:**
   ```cypher
   -- Increase search depth
   CALL db.idx.vector.query('Paper', 'embedding', $vec, 10, {ef_search: 200})
   ```

4. **Wrong distance metric:**
   ```bash
   # Check index metric
   uni index show --path ./storage --name paper_embeddings
   # Metric: cosine

   # Rebuild with correct metric if needed
   uni index rebuild --path ./storage --name paper_embeddings --metric l2
   ```

#### Index Not Being Used

**Symptom:** Query is slow despite having an index.

**Diagnosis:**
```bash
uni query "EXPLAIN MATCH (p:Paper) WHERE p.year > 2020 RETURN p" --path ./storage
```

**Common Causes:**

1. **Function on indexed column:**
   ```cypher
   // Index NOT used (function applied to column)
   WHERE LOWER(p.venue) = 'neurips'

   // Index used
   WHERE p.venue = 'NeurIPS'
   ```

2. **OR conditions:**
   ```cypher
   // May not use index efficiently
   WHERE p.year > 2020 OR p.venue = 'NeurIPS'

   // Better: split into UNION (if supported)
   ```

3. **Leading wildcard:**
   ```cypher
   // Cannot use index
   WHERE p.title CONTAINS 'attention'

   // Can use full-text index (if available)
   ```

4. **Low selectivity:**
   ```cypher
   // If 80% of data matches, full scan may be faster
   WHERE p.year > 2000
   ```

#### Index Build Fails

**Symptom:**
```
Error: Failed to build vector index: out of memory
```

**Solutions:**

1. **Reduce batch size:**
   ```bash
   uni index create --path ./storage --batch-size 10000 ...
   ```

2. **Use IVF_PQ instead of HNSW:**
   ```cypher
   CREATE VECTOR INDEX paper_embeddings
   FOR (p:Paper) ON p.embedding
   OPTIONS { index_type: 'ivf_pq' }  -- Less memory
   ```

3. **Build incrementally:**
   ```bash
   # Build in chunks
   uni index create --path ./storage --incremental ...
   ```

---

### Import Issues

#### Schema Inference Fails

**Symptom:**
```
Error: Cannot infer type for property 'year': mixed types
```

**Solution:**
```bash
# Provide explicit schema
uni import my-data --schema schema.json ...

# Or fix source data to have consistent types
```

#### Duplicate IDs

**Symptom:**
```
Warning: Duplicate external ID 'paper_123', keeping latest
```

**Solutions:**

1. **Deduplicate source data:**
   ```bash
   sort -u -t',' -k1,1 papers.jsonl > papers_dedup.jsonl
   ```

2. **Use merge mode:**
   ```bash
   uni import my-data --mode merge ...  # Merge duplicates
   ```

3. **Use strict mode to fail:**
   ```bash
   uni import my-data --on-duplicate error ...
   ```

#### Embedding Dimension Mismatch

**Symptom:**
```
Error: Vector dimension mismatch: expected 768, got 384
```

**Solution:**
```bash
# Check schema
uni schema show --path ./storage --label Paper

# Re-embed with correct model or update schema
uni schema update --path ./storage --property Paper.embedding:Vector[384]
# Note: This requires re-importing data
```

---

### Performance Issues

#### Slow Traversals

**Symptom:** Graph traversals take >100ms.

**Diagnosis:**
```bash
uni query "PROFILE MATCH (p:Paper)-[:CITES]->(c) RETURN COUNT(c)" --path ./storage
```

**Solutions:**

1. **Warm the adjacency cache:**
   ```rust
   storage.adjacency_cache().warm(&frequently_accessed_vids).await?;
   ```

2. **Increase cache size:**
   ```rust
   let config = StorageConfig {
       adjacency_cache_size: 5_000_000,
       ..Default::default()
   };
   ```

3. **Add LIMIT to multi-hop:**
   ```cypher
   MATCH (p:Paper)-[:CITES*1..3]->(end)
   RETURN DISTINCT end
   LIMIT 1000
   ```

#### High Memory Usage

**Symptom:** Process using more memory than expected.

**Diagnosis:**
```rust
let stats = storage.memory_stats();
println!("Adjacency cache: {} MB", stats.adjacency_cache_mb);
println!("Property cache: {} MB", stats.property_cache_mb);
println!("L0 buffer: {} MB", stats.l0_buffer_mb);
```

**Solutions:**

1. **Reduce cache sizes:**
   ```rust
   let config = StorageConfig {
       adjacency_cache_size: 500_000,
       property_cache_size: 50_000,
       ..Default::default()
   };
   ```

2. **Flush L0 more frequently:**
   ```rust
   let config = StorageConfig {
       max_l0_size: 32 * 1024 * 1024,  // 32 MB
       max_mutations_before_flush: 5_000,
       ..Default::default()
   };
   ```

3. **Use streaming queries:**
   ```rust
   let stream = executor.execute_stream(query).await?;
   // Process results without loading all into memory
   ```

---

## Error Reference

### Storage Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `StorageLocked` | Another process holds lock | Wait or force unlock |
| `CorruptedManifest` | Invalid manifest file | Rollback or repair |
| `WalCorrupted` | WAL corruption detected | Truncate WAL, may lose recent writes |
| `DiskFull` | No space available | Free space or compact |
| `VersionNotFound` | Requested version doesn't exist | Use valid version number |

### Query Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `ParseError` | Invalid Cypher syntax | Fix query syntax |
| `UnknownLabel` | Label not in schema | Check schema or fix query |
| `UnknownProperty` | Property not in schema | Check schema or fix query |
| `TypeMismatch` | Incompatible types | Fix query or data types |
| `Timeout` | Query exceeded time limit | Optimize query or increase timeout |
| `OutOfMemory` | Memory limit exceeded | Reduce result size or increase limit |

### Index Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `IndexNotFound` | Index doesn't exist | Create index first |
| `IndexBuildFailed` | Build process failed | Check logs, retry with smaller batch |
| `DimensionMismatch` | Vector size doesn't match | Use correct embedding dimensions |
| `MetricMismatch` | Distance metric incompatible | Use matching metric |

---

## Debugging Tips

### Enable Verbose Logging

```bash
# All Uni logs at debug level
RUST_LOG=uni=debug uni query "..." --path ./storage

# Specific module
RUST_LOG=uni::storage=trace,uni::query=debug uni query "..."

# Include Lance logs
RUST_LOG=uni=debug,lance=info uni query "..."
```

### Query Profiling

```bash
# Get execution profile
uni query "PROFILE MATCH (p:Paper) WHERE p.year > 2020 RETURN COUNT(p)" --path ./storage

# Output shows:
# - Time per operator
# - Rows processed
# - Index usage
# - Memory usage
```

### Storage Inspection

```bash
# List all datasets
ls -la ./storage/vertices/
ls -la ./storage/edges/
ls -la ./storage/adjacency/

# Check Lance dataset info
uni inspect --path ./storage/vertices/Paper

# View manifest
cat ./storage/manifest.json | jq .
```

### Memory Profiling

```bash
# Run with memory tracking
RUST_LOG=uni=debug UNI_TRACK_MEMORY=1 uni query "..."

# Use heaptrack (Linux)
heaptrack uni query "..."
heaptrack_gui heaptrack.uni.*.gz
```

---

## Getting Help

### Resources

- **Documentation**: [https://uni.dev/docs](https://uni.dev/docs)
- **GitHub Issues**: [https://github.com/dragonscale/uni/issues](https://github.com/dragonscale/uni/issues)
- **Discussions**: [https://github.com/dragonscale/uni/discussions](https://github.com/dragonscale/uni/discussions)

### Reporting Bugs

When reporting issues, include:

1. **Uni version:** `uni --version`
2. **Rust version:** `rustc --version`
3. **OS and version**
4. **Minimal reproduction steps**
5. **Error messages (full output)**
6. **Query plan** (if query-related): `EXPLAIN ...`
7. **Storage stats**: `uni stats --path ./storage`

---

## Next Steps

- [Configuration Reference](configuration.md) — All configuration options
- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
- [Glossary](glossary.md) — Terminology reference
