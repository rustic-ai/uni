# Configuration Reference

This document provides a comprehensive reference for all Uni configuration options, environment variables, and tuning parameters.

## Configuration Overview

Uni can be configured through:
1. **Rust API** — `StorageConfig`, `ExecutorConfig`, etc.
2. **Environment Variables** — Runtime overrides
3. **Configuration File** — `uni.toml` or JSON

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       CONFIGURATION HIERARCHY                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Defaults (Code)                                                           │
│       ↓ overridden by                                                       │
│   Configuration File (uni.toml)                                             │
│       ↓ overridden by                                                       │
│   Environment Variables (UNI_*)                                             │
│       ↓ overridden by                                                       │
│   Programmatic Config (Rust API)                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Storage Configuration

### StorageConfig

```rust
pub struct StorageConfig {
    // L0 Buffer Configuration
    pub max_l0_size: usize,
    pub max_mutations_before_flush: usize,
    pub auto_flush: bool,

    // L1 Configuration
    pub max_l1_runs: usize,
    pub l1_compaction_threshold: usize,

    // WAL Configuration
    pub wal_sync_mode: WalSyncMode,
    pub wal_segment_size: usize,
    pub wal_dir: Option<PathBuf>,

    // Cache Configuration
    pub adjacency_cache_size: usize,
    pub adjacency_cache_ttl: Duration,
    pub property_cache_size: usize,

    // I/O Configuration
    pub read_ahead_size: usize,
    pub prefetch_enabled: bool,
    pub max_open_files: usize,

    // Memory Configuration
    pub memory_limit: Option<usize>,
    pub enable_memory_tracking: bool,
}
```

### Parameter Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_l0_size` | bytes | 128 MB | Maximum L0 buffer size before flush |
| `max_mutations_before_flush` | count | 10,000 | Mutations triggering auto-flush |
| `auto_flush` | bool | true | Enable automatic flushing |
| `max_l1_runs` | count | 4 | L1 runs before compaction |
| `l1_compaction_threshold` | bytes | 256 MB | Size threshold for L1→L2 compaction |
| `wal_sync_mode` | enum | Periodic(100ms) | WAL durability mode |
| `wal_segment_size` | bytes | 64 MB | WAL segment rotation size |
| `adjacency_cache_size` | vertices | 1,000,000 | Maximum cached vertices |
| `adjacency_cache_ttl` | duration | 1 hour | Cache entry TTL |
| `property_cache_size` | entries | 100,000 | Property cache capacity |
| `read_ahead_size` | bytes | 64 MB | Sequential read prefetch size |
| `prefetch_enabled` | bool | true | Enable I/O prefetching |
| `max_open_files` | count | 1,000 | Maximum open file handles |
| `memory_limit` | bytes | None | Per-process memory limit |

### WAL Sync Modes

```rust
pub enum WalSyncMode {
    /// fsync after every write
    /// Safest, ~50% slower writes
    Sync,

    /// fsync at regular intervals
    /// Balanced durability/performance
    Periodic { interval_ms: u64 },

    /// OS-managed sync
    /// Fastest, risk of data loss on crash
    Async,
}
```

**Recommendations:**

| Use Case | Mode | Rationale |
|----------|------|-----------|
| Production (critical data) | `Sync` | Maximum durability |
| Production (balanced) | `Periodic(100)` | Good balance |
| Development | `Async` | Maximum speed |
| Batch import | `Async` | Speed, re-import on failure |

### Example Configuration

```rust
let config = StorageConfig {
    // Large buffer for batch workloads
    max_l0_size: 256 * 1024 * 1024,  // 256 MB
    max_mutations_before_flush: 50_000,
    auto_flush: true,

    // Faster compaction trigger
    max_l1_runs: 2,
    l1_compaction_threshold: 128 * 1024 * 1024,

    // Balanced WAL
    wal_sync_mode: WalSyncMode::Periodic { interval_ms: 50 },
    wal_segment_size: 32 * 1024 * 1024,

    // Large cache for traversal-heavy workloads
    adjacency_cache_size: 5_000_000,
    property_cache_size: 500_000,

    ..Default::default()
};
```

---

## Executor Configuration

### ExecutorConfig

```rust
pub struct ExecutorConfig {
    // Parallelism
    pub worker_threads: usize,
    pub morsel_size: usize,
    pub max_concurrent_io: usize,

    // Resource Limits
    pub memory_limit: usize,
    pub timeout: Duration,

    // Optimization
    pub enable_pushdown: bool,
    pub enable_late_materialize: bool,
    pub batch_size: usize,

    // Planner
    pub max_optimization_rounds: usize,
    pub cost_model: CostModel,
}
```

### Parameter Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `worker_threads` | count | CPU cores | Parallel worker count |
| `morsel_size` | rows | 4,096 | Rows per morsel |
| `max_concurrent_io` | count | 16 | Parallel I/O operations |
| `memory_limit` | bytes | 4 GB | Per-query memory limit |
| `timeout` | duration | 5 min | Query timeout |
| `enable_pushdown` | bool | true | Enable predicate pushdown |
| `enable_late_materialize` | bool | true | Enable late materialization |
| `batch_size` | rows | 4,096 | Vectorized batch size |
| `max_optimization_rounds` | count | 10 | Optimizer iterations |

### Tuning Guidelines

| Workload | worker_threads | morsel_size | batch_size |
|----------|----------------|-------------|------------|
| OLTP (simple queries) | 4 | 1,024 | 1,024 |
| OLAP (complex analytics) | CPU cores | 4,096 | 8,192 |
| Mixed | CPU cores / 2 | 2,048 | 4,096 |
| Memory constrained | 2-4 | 1,024 | 2,048 |

---

## Index Configuration

### Vector Index Options

```rust
pub struct VectorIndexConfig {
    pub name: String,
    pub index_type: VectorIndexType,
    pub metric: DistanceMetric,

    // HNSW parameters
    pub m: Option<usize>,              // Default: 32
    pub ef_construction: Option<usize>, // Default: 200
    pub ef_search: Option<usize>,       // Default: 100

    // IVF_PQ parameters
    pub num_partitions: Option<usize>,  // Default: sqrt(n)
    pub num_sub_vectors: Option<usize>, // Default: dimensions/8
    pub num_probes: Option<usize>,      // Default: 20
}
```

### HNSW Tuning

| Parameter | Low Latency | Balanced | High Recall |
|-----------|-------------|----------|-------------|
| `m` | 16 | 32 | 48-64 |
| `ef_construction` | 100 | 200 | 400-500 |
| `ef_search` | 50 | 100 | 200-300 |

**Trade-offs:**
- Higher `m` → Better recall, more memory, slower build
- Higher `ef_construction` → Better index quality, slower build
- Higher `ef_search` → Better recall at query time, slower queries

### IVF_PQ Tuning

| Parameter | Memory Optimized | Balanced | Recall Optimized |
|-----------|------------------|----------|------------------|
| `num_partitions` | 1024 | 512 | 256 |
| `num_sub_vectors` | 8 | 16 | 32-48 |
| `num_probes` | 10 | 20 | 50 |

**Trade-offs:**
- More partitions → Smaller clusters, less memory, potentially lower recall
- More sub-vectors → Better recall, more memory per vector
- More probes → Better recall at query time, slower queries

### Scalar Index Options

```rust
pub struct ScalarIndexConfig {
    pub name: String,
    pub index_type: ScalarIndexType,
}

pub enum ScalarIndexType {
    /// B-tree for range queries
    BTree,

    /// Hash for equality lookups
    Hash,

    /// Bitmap for low-cardinality columns
    Bitmap,
}
```

| Index Type | Best For | Query Types |
|------------|----------|-------------|
| BTree | Range queries, sorting | `<`, `>`, `<=`, `>=`, `BETWEEN` |
| Hash | Exact match, high cardinality | `=`, `IN` |
| Bitmap | Low cardinality (<1000 distinct) | `=`, `IN`, `AND`/`OR` |

---

## Environment Variables

All environment variables use the `UNI_` prefix.

### Storage Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `UNI_STORAGE_PATH` | path | `./storage` | Default storage path |
| `UNI_MAX_L0_SIZE_MB` | integer | 128 | L0 buffer size in MB |
| `UNI_WAL_SYNC_MODE` | string | `periodic` | WAL mode: `sync`, `periodic`, `async` |
| `UNI_WAL_SYNC_INTERVAL_MS` | integer | 100 | Periodic sync interval |
| `UNI_ADJACENCY_CACHE_SIZE` | integer | 1000000 | Cache size in vertices |
| `UNI_PROPERTY_CACHE_SIZE` | integer | 100000 | Cache size in entries |

### Executor Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `UNI_WORKER_THREADS` | integer | CPU cores | Parallel workers |
| `UNI_MORSEL_SIZE` | integer | 4096 | Rows per morsel |
| `UNI_MAX_MEMORY_MB` | integer | 4096 | Memory limit in MB |
| `UNI_QUERY_TIMEOUT_SECS` | integer | 300 | Query timeout |
| `UNI_ENABLE_PUSHDOWN` | bool | true | Predicate pushdown |

### Logging Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RUST_LOG` | string | `warn` | Log level filter |
| `UNI_LOG_FORMAT` | string | `pretty` | Log format: `pretty`, `json`, `compact` |
| `UNI_LOG_FILE` | path | None | Log to file |

### Object Store Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | string | - | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | string | - | S3 secret key |
| `AWS_REGION` | string | `us-east-1` | S3 region |
| `AWS_ENDPOINT_URL` | string | - | Custom S3 endpoint |
| `GOOGLE_APPLICATION_CREDENTIALS` | path | - | GCS service account |

### Example

```bash
# Production configuration
export UNI_STORAGE_PATH=/data/uni
export UNI_MAX_L0_SIZE_MB=256
export UNI_WAL_SYNC_MODE=periodic
export UNI_WAL_SYNC_INTERVAL_MS=50
export UNI_ADJACENCY_CACHE_SIZE=5000000
export UNI_WORKER_THREADS=16
export UNI_MAX_MEMORY_MB=8192
export RUST_LOG=uni=info,lance=warn
```

---

## Configuration File

Uni supports TOML configuration files.

### Location

Uni searches for configuration in order:
1. Path specified with `--config`
2. `./uni.toml`
3. `~/.config/uni/config.toml`
4. `/etc/uni/config.toml`

### Full Example

```toml
# uni.toml - Uni Configuration

[storage]
path = "/data/uni"
max_l0_size_mb = 256
max_mutations_before_flush = 50000
auto_flush = true
max_l1_runs = 4
l1_compaction_threshold_mb = 256

[storage.wal]
sync_mode = "periodic"  # sync, periodic, async
sync_interval_ms = 100
segment_size_mb = 64

[storage.cache]
adjacency_size = 2000000
adjacency_ttl_secs = 3600
property_size = 200000

[storage.io]
read_ahead_mb = 64
prefetch = true
max_open_files = 1000

[executor]
worker_threads = 8  # 0 = auto-detect
morsel_size = 4096
max_concurrent_io = 16
memory_limit_mb = 4096
timeout_secs = 300

[executor.optimization]
enable_pushdown = true
enable_late_materialize = true
batch_size = 4096
max_optimization_rounds = 10

[index.vector.defaults]
index_type = "hnsw"
metric = "cosine"
m = 32
ef_construction = 200
ef_search = 100

[index.scalar.defaults]
index_type = "btree"

[logging]
level = "info"  # trace, debug, info, warn, error
format = "pretty"  # pretty, json, compact
file = "/var/log/uni/uni.log"

[server]
host = "127.0.0.1"
port = 8080
max_connections = 100

[object_store.s3]
bucket = "my-bucket"
region = "us-west-2"
# Credentials from environment or IAM role
```

---

## Schema Configuration

### Schema JSON Format

**Important:** The schema JSON format used by `uni` requires internal metadata fields (`created_at`, `state`, `added_in`) for correct deserialization, even though they are often managed automatically by the system. When manually creating a schema file, you must include these fields.

```json
{
  "schema_version": 1,

  "labels": {
    "Paper": {
      "id": 1,
      "is_document": false,
      "created_at": "2024-01-01T00:00:00Z",
      "state": "Active"
    },
    "Author": {
      "id": 2,
      "is_document": false,
      "created_at": "2024-01-01T00:00:00Z",
      "state": "Active"
    }
  },

  "edge_types": {
    "CITES": {
      "id": 1,
      "src_labels": ["Paper"],
      "dst_labels": ["Paper"],
      "state": "Active"
    },
    "AUTHORED_BY": {
      "id": 2,
      "src_labels": ["Paper"],
      "dst_labels": ["Author"],
      "state": "Active"
    }
  },

  "properties": {
    "Paper": {
      "title": {
        "type": "String",
        "nullable": false,
        "added_in": 1,
        "state": "Active"
      },
      "year": {
        "type": "Int32",
        "nullable": true,
        "added_in": 1,
        "state": "Active"
      },
      "embedding": {
        "type": "Vector",
        "dimensions": 768,
        "nullable": true,
        "added_in": 1,
        "state": "Active"
      }
    },
    "Author": {
      "name": {
        "type": "String",
        "nullable": false,
        "added_in": 1,
        "state": "Active"
      }
    }
  },

  "indexes": [
    {
      "type": "Vector",
      "name": "paper_embeddings",
      "label": "Paper",
      "property": "embedding",
      "index_type": {
        "Hnsw": {
          "m": 32,
          "ef_construction": 200,
          "ef_search": 100
        }
      },
      "metric": "Cosine"
    },
    {
      "type": "Scalar",
      "name": "paper_year",
      "label": "Paper",
      "properties": ["year"],
      "index_type": "BTree"
    },
    {
      "type": "Scalar",
      "name": "composite_venue_year",
      "label": "Paper",
      "properties": ["venue", "year"],
      "index_type": "BTree"
    }
  ]
}
```

**Note on Case Sensitivity:**
The JSON schema parser is generally case-sensitive for enum values. Use PascalCase for types (e.g., `Vector`, `Scalar`, `BTree`, `Hnsw`, `Active`) as shown in the example above.

### Data Types

| Type | JSON Name | Description |
|------|-----------|-------------|
| Boolean | `Bool` | true/false |
| 32-bit integer | `Int32` | -2³¹ to 2³¹-1 |
| 64-bit integer | `Int64` | -2⁶³ to 2⁶³-1 |
| 64-bit float | `Float64` | IEEE 754 double |
| String | `String` | UTF-8 text |
| Binary | `Bytes` | Raw bytes |
| Vector | `Vector` | Float32 array (requires `dimensions`) |
| Timestamp | `Timestamp` | UTC datetime |
| JSON | `Json` | Semi-structured data |

---

## Performance Profiles

### High Throughput (Batch Processing)

```rust
let storage_config = StorageConfig {
    max_l0_size: 512 * 1024 * 1024,  // 512 MB
    max_mutations_before_flush: 100_000,
    wal_sync_mode: WalSyncMode::Async,
    adjacency_cache_size: 10_000_000,
    ..Default::default()
};

let executor_config = ExecutorConfig {
    worker_threads: num_cpus::get(),
    morsel_size: 8192,
    batch_size: 8192,
    memory_limit: 16 * 1024 * 1024 * 1024,
    ..Default::default()
};
```

### Low Latency (Interactive)

```rust
let storage_config = StorageConfig {
    max_l0_size: 32 * 1024 * 1024,  // 32 MB
    max_mutations_before_flush: 1_000,
    wal_sync_mode: WalSyncMode::Sync,
    adjacency_cache_size: 5_000_000,
    property_cache_size: 500_000,
    ..Default::default()
};

let executor_config = ExecutorConfig {
    worker_threads: 4,
    morsel_size: 1024,
    batch_size: 2048,
    timeout: Duration::from_secs(30),
    ..Default::default()
};
```

### Memory Constrained

```rust
let storage_config = StorageConfig {
    max_l0_size: 16 * 1024 * 1024,  // 16 MB
    adjacency_cache_size: 100_000,
    property_cache_size: 10_000,
    ..Default::default()
};

let executor_config = ExecutorConfig {
    worker_threads: 2,
    morsel_size: 512,
    batch_size: 1024,
    memory_limit: 512 * 1024 * 1024,  // 512 MB
    ..Default::default()
};
```

---

## Next Steps

- [Rust API Reference](rust-api.md) — Complete API documentation
- [Troubleshooting](troubleshooting.md) — Common issues and solutions
- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
