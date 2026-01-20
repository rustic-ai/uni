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

## Main Configuration

### UniConfig

```rust
pub struct UniConfig {
    /// Maximum adjacency cache size in bytes (default: 1GB)
    pub cache_size: usize,

    /// Number of worker threads for parallel execution
    pub parallelism: usize,

    /// Size of each data morsel/batch (number of rows)
    pub batch_size: usize,

    /// Maximum size of traversal frontier before pruning
    pub max_frontier_size: usize,

    /// Auto-flush threshold for L0 buffer (default: 10_000 mutations)
    pub auto_flush_threshold: usize,

    /// Auto-flush interval for L0 buffer (default: 5 seconds).
    /// Flush triggers if time elapsed AND mutation count >= auto_flush_min_mutations.
    /// Set to None to disable time-based flush.
    pub auto_flush_interval: Option<Duration>,

    /// Minimum mutations required before time-based flush triggers (default: 1).
    /// Prevents unnecessary flushes when there's minimal activity.
    pub auto_flush_min_mutations: usize,

    /// Enable write-ahead logging (default: true)
    pub wal_enabled: bool,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// Write throttling configuration
    pub throttle: WriteThrottleConfig,

    /// Optional snapshot ID to open the database at (time-travel)
    pub at_snapshot: Option<String>,

    /// File sandbox configuration for BACKUP/COPY/EXPORT commands
    pub file_sandbox: FileSandboxConfig,

    /// Default query execution timeout (default: 30s)
    pub query_timeout: Duration,

    /// Default maximum memory per query (default: 1GB)
    pub max_query_memory: usize,

    /// Object store resilience configuration
    pub object_store: ObjectStoreConfig,

    /// Background index rebuild configuration
    pub index_rebuild: IndexRebuildConfig,
}
```

### Parameter Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cache_size` | bytes | 1 GB | Maximum adjacency cache size |
| `parallelism` | count | CPU cores | Worker threads for parallel execution |
| `batch_size` | rows | 1,024 | Rows per morsel/batch |
| `max_frontier_size` | count | 1,000,000 | Maximum traversal frontier size |
| `auto_flush_threshold` | count | 10,000 | Mutations triggering auto-flush |
| `auto_flush_interval` | duration | 5s | Time-based flush interval (None to disable) |
| `auto_flush_min_mutations` | count | 1 | Minimum mutations for time-based flush |
| `wal_enabled` | bool | true | Enable write-ahead logging |
| `at_snapshot` | string | None | Open database at specific snapshot |
| `query_timeout` | duration | 30s | Default query execution timeout |
| `max_query_memory` | bytes | 1 GB | Maximum memory per query |

### CompactionConfig

```rust
pub struct CompactionConfig {
    /// Enable background compaction (default: true)
    pub enabled: bool,

    /// Max L1 runs before triggering compaction (default: 4)
    pub max_l1_runs: usize,

    /// Max L1 size in bytes before compaction (default: 256MB)
    pub max_l1_size_bytes: u64,

    /// Max age of oldest L1 run before compaction (default: 1 hour)
    pub max_l1_age: Duration,

    /// Background check interval (default: 30s)
    pub check_interval: Duration,

    /// Number of compaction worker threads (default: 1)
    pub worker_threads: usize,
}
```

### WriteThrottleConfig

```rust
pub struct WriteThrottleConfig {
    /// L1 run count to start throttling (default: 8)
    pub soft_limit: usize,

    /// L1 run count to stop writes entirely (default: 16)
    pub hard_limit: usize,

    /// Base delay when throttling (default: 10ms)
    pub base_delay: Duration,
}
```

### FileSandboxConfig (Security-Critical)

```rust
pub struct FileSandboxConfig {
    /// If true, file operations are restricted to allowed_paths
    /// MUST be enabled for server mode with untrusted clients
    pub enabled: bool,

    /// List of allowed base directories for file operations
    pub allowed_paths: Vec<PathBuf>,
}
```

**Security Note:** File sandbox MUST be enabled in server mode to prevent path traversal attacks (CWE-22).

### ObjectStoreConfig

```rust
pub struct ObjectStoreConfig {
    pub connect_timeout: Duration,    // Default: 10s
    pub read_timeout: Duration,       // Default: 30s
    pub write_timeout: Duration,      // Default: 60s
    pub max_retries: u32,             // Default: 3
    pub retry_backoff_base: Duration, // Default: 100ms
    pub retry_backoff_max: Duration,  // Default: 10s
}
```

### IndexRebuildConfig

```rust
pub struct IndexRebuildConfig {
    /// Maximum retry attempts for failed builds (default: 3)
    pub max_retries: u32,

    /// Delay between retry attempts (default: 60s)
    pub retry_delay: Duration,

    /// Check interval for pending tasks (default: 5s)
    pub worker_check_interval: Duration,
}
```

---

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
| `AWS_SESSION_TOKEN` | string | - | S3 session token (temporary credentials) |
| `AWS_REGION` | string | `us-east-1` | S3 region |
| `AWS_DEFAULT_REGION` | string | `us-east-1` | S3 region (alternative) |
| `AWS_ENDPOINT_URL` | string | - | Custom S3 endpoint (MinIO, LocalStack) |
| `GOOGLE_APPLICATION_CREDENTIALS` | path | - | GCS service account JSON path |
| `AZURE_STORAGE_ACCOUNT` | string | - | Azure storage account name |
| `AZURE_STORAGE_ACCESS_KEY` | string | - | Azure storage access key |
| `AZURE_STORAGE_SAS_TOKEN` | string | - | Azure SAS token |

---

## Cloud Storage Configuration

Uni supports cloud storage backends for bulk data storage while keeping metadata and WAL local for low latency.

### Supported Backends

| Backend | URL Scheme | Status |
|---------|------------|--------|
| Local filesystem | `/path` or `file://` | Fully supported |
| Amazon S3 | `s3://bucket/path` | Supported |
| Google Cloud Storage | `gs://bucket/path` | Supported |
| Azure Blob Storage | `az://account/container` | Supported |
| S3-compatible (MinIO) | `s3://` with custom endpoint | Supported |

### Hybrid Mode

Hybrid mode stores bulk data in cloud storage while keeping WAL and metadata local:

```rust
use uni_db::Uni;

let db = Uni::open("./local_meta")
    .hybrid("./local_meta", "s3://my-bucket/graph-data")
    .build()
    .await?;
```

**Benefits:**
- Low-latency writes via local WAL
- Scalable storage via cloud object store
- Cost-effective for large datasets

### S3 Configuration

```rust
use uni_db::Uni;
use uni_common::CloudStorageConfig;

// Using environment variables (recommended)
let config = CloudStorageConfig::s3_from_env("my-bucket");

// Or explicit configuration
let config = CloudStorageConfig::S3 {
    bucket: "my-bucket".to_string(),
    region: Some("us-west-2".to_string()),
    endpoint: None,  // Use AWS default
    access_key_id: None,  // Use env/IAM
    secret_access_key: None,
    session_token: None,
    virtual_hosted_style: true,
};

let db = Uni::open("./local")
    .hybrid("./local", "s3://my-bucket/data")
    .cloud_config(config)
    .build()
    .await?;
```

**For S3-compatible services (MinIO, LocalStack):**

```rust
let config = CloudStorageConfig::S3 {
    bucket: "test-bucket".to_string(),
    region: Some("us-east-1".to_string()),
    endpoint: Some("http://localhost:9000".to_string()),
    access_key_id: Some("minioadmin".to_string()),
    secret_access_key: Some("minioadmin".to_string()),
    session_token: None,
    virtual_hosted_style: false,  // Path-style for MinIO
};
```

### GCS Configuration

```rust
use uni_common::CloudStorageConfig;

// Using environment variable (GOOGLE_APPLICATION_CREDENTIALS)
let config = CloudStorageConfig::gcs_from_env("my-gcs-bucket");

// Or explicit configuration
let config = CloudStorageConfig::Gcs {
    bucket: "my-gcs-bucket".to_string(),
    service_account_path: Some("/path/to/service-account.json".to_string()),
    service_account_key: None,
};
```

### Azure Configuration

```rust
use uni_common::CloudStorageConfig;

// Using environment variables
let config = CloudStorageConfig::azure_from_env("my-container");

// Or explicit configuration
let config = CloudStorageConfig::Azure {
    container: "my-container".to_string(),
    account: "mystorageaccount".to_string(),
    access_key: Some("account-key".to_string()),
    sas_token: None,
};
```

### Cloud Storage with BACKUP/COPY/EXPORT

BACKUP, COPY, and EXPORT commands support cloud URLs:

```cypher
-- Backup to S3
BACKUP TO 's3://backup-bucket/uni-backup-2024'

-- Import from S3
COPY Person FROM 's3://data-bucket/people.parquet'

-- Export to GCS
EXPORT Person TO 'gs://export-bucket/people.parquet'
```

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
export RUST_LOG=uni_db=info,lance=warn
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
use std::time::Duration;

let config = UniConfig {
    // Large L0 buffer for batch writes
    auto_flush_threshold: 100_000,
    auto_flush_interval: None,  // Disable time-based flush during batch
    cache_size: 10 * 1024 * 1024 * 1024,  // 10 GB
    parallelism: num_cpus::get(),
    batch_size: 8192,
    ..Default::default()
};
```

### Low Latency (Interactive)

```rust
use std::time::Duration;

let config = UniConfig {
    // Small L0 for fast flush
    auto_flush_threshold: 1_000,
    auto_flush_interval: Some(Duration::from_secs(1)),  // Flush quickly
    auto_flush_min_mutations: 1,
    cache_size: 5 * 1024 * 1024 * 1024,  // 5 GB
    parallelism: 4,
    batch_size: 2048,
    query_timeout: Duration::from_secs(30),
    ..Default::default()
};
```

### Low-Transaction System

For systems with infrequent writes that still need timely durability:

```rust
use std::time::Duration;

let config = UniConfig {
    auto_flush_threshold: 10_000,
    // Time-based flush ensures data reaches storage
    auto_flush_interval: Some(Duration::from_secs(5)),
    auto_flush_min_mutations: 1,  // Flush even with 1 pending mutation
    ..Default::default()
};
```

### Cloud Storage (Cost Optimized)

For cloud storage backends where minimizing API calls matters:

```rust
use std::time::Duration;
use uni_common::CloudStorageConfig;

let config = UniConfig {
    // Larger batches = fewer PUT operations
    auto_flush_threshold: 50_000,
    auto_flush_interval: Some(Duration::from_secs(30)),
    auto_flush_min_mutations: 100,  // Don't flush for just a few writes
    ..Default::default()
};

let db = Uni::open("./local-cache")
    .config(config)
    .cloud_storage(CloudStorageConfig {
        url: "s3://my-bucket/data".to_string(),
        ..Default::default()
    })
    .build()
    .await?;
```

### Memory Constrained

```rust
use std::time::Duration;

let config = UniConfig {
    // Small caches
    cache_size: 512 * 1024 * 1024,  // 512 MB
    // Flush frequently to keep memory low
    auto_flush_threshold: 1_000,
    auto_flush_interval: Some(Duration::from_secs(2)),
    parallelism: 2,
    batch_size: 1024,
    max_query_memory: 256 * 1024 * 1024,  // 256 MB per query
    ..Default::default()
};
```

---

## Next Steps

- [Rust API Reference](rust-api.md) — Complete API documentation
- [Troubleshooting](troubleshooting.md) — Common issues and solutions
- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
