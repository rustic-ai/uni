# Data Ingestion Guide

This guide covers all methods for getting data into Uni, from bulk imports to streaming writes and programmatic access.

## Overview

Uni supports multiple ingestion patterns:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION PATTERNS                              │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│    BULK IMPORT      │   STREAMING WRITE   │      PROGRAMMATIC API           │
├─────────────────────┼─────────────────────┼─────────────────────────────────┤
│ • JSONL files       │ • Real-time inserts │ • Rust crate API                │
│ • CLI import        │ • HTTP API          │ • Writer interface              │
│ • One-time load     │ • Continuous        │ • Fine-grained control          │
├─────────────────────┼─────────────────────┼─────────────────────────────────┤
│ Best for:           │ Best for:           │ Best for:                       │
│ Initial data load   │ Live applications   │ Custom pipelines                │
│ Batch migrations    │ Event streaming     │ Integration code                │
└─────────────────────┴─────────────────────┴─────────────────────────────────┘
```

---

## Bulk Import (CLI)

The fastest way to load large datasets.

### Input File Format

#### Vertices (JSONL)

Each line is a JSON object representing a vertex:

```json
{"id": "paper_001", "title": "Attention Is All You Need", "year": 2017, "venue": "NeurIPS", "embedding": [0.12, -0.34, ...]}
{"id": "paper_002", "title": "BERT", "year": 2018, "venue": "NAACL", "embedding": [0.08, -0.21, ...]}
```

**Required Fields:**
- `id` (String): Unique external identifier

**Optional Fields:**
- Any property defined in your schema
- Properties not in schema are ignored

**Special Fields:**
- `_label`: Override default label (if multiple labels)

#### Edges (JSONL)

Each line is a JSON object representing an edge:

```json
{"src": "paper_002", "dst": "paper_001"}
{"src": "paper_003", "dst": "paper_001", "weight": 0.95}
```

**Required Fields:**
- `src` (String): Source vertex external ID
- `dst` (String): Destination vertex external ID

**Optional Fields:**
- `type`: Edge type (default: inferred from import config)
- Any edge property defined in schema

### Running Import

**Basic Import:**

```bash
uni import semantic-scholar \
    --papers ./data/papers.jsonl \
    --citations ./data/citations.jsonl \
    --output ./storage
```

**With Custom Schema:**

```bash
uni import my-dataset \
    --papers ./vertices.jsonl \
    --citations ./edges.jsonl \
    --schema ./schema.json \
    --output ./storage
```

**Tuned for Large Data:**

```bash
uni import wikipedia \
    --papers ./articles.jsonl \
    --citations ./links.jsonl \
    --batch-size 50000 \
    --output s3://my-bucket/wiki-graph
```

### Import Options

| Option | Default | Description |
|--------|---------|-------------|
| `--papers` | Required | Path to vertex JSONL file |
| `--citations` | Required | Path to edge JSONL file |
| `--output` | `./storage` | Output storage path |
| `--schema` | Auto-inferred | Path to schema JSON |
| `--batch-size` | 10000 | Records per batch |
| `--skip-validation` | false | Skip schema validation |
| `--force` | false | Overwrite existing storage |

### Import Process

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           IMPORT PIPELINE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   [1] SCHEMA LOADING                                                        │
│       └── Load or infer schema from data                                    │
│                                                                             │
│   [2] VERTEX PASS                                                           │
│       ├── Stream JSONL file                                                 │
│       ├── Validate properties against schema                                │
│       ├── Allocate VIDs (label_id << 48 | offset)                          │
│       ├── Build ext_id → VID mapping                                        │
│       └── Write to Lance vertex datasets (batched)                          │
│                                                                             │
│   [3] EDGE PASS                                                             │
│       ├── Stream JSONL file                                                 │
│       ├── Resolve src/dst ext_ids to VIDs                                  │
│       ├── Allocate EIDs                                                     │
│       └── Write to Lance edge datasets (batched)                            │
│                                                                             │
│   [4] ADJACENCY BUILD                                                       │
│       ├── Group edges by (edge_type, direction, source_label)              │
│       ├── Build CSR-style neighbor lists                                    │
│       └── Write to Lance adjacency datasets                                 │
│                                                                             │
│   [5] INDEX BUILD                                                           │
│       ├── Build vector indexes (HNSW/IVF) if configured                    │
│       └── Build scalar indexes if configured                                │
│                                                                             │
│   [6] SNAPSHOT                                                              │
│       └── Write manifest with all dataset versions                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Schema Definition

### Auto-Inference

If no schema is provided, Uni infers types from data:

```bash
# Schema inferred from first N records
uni import my-data --papers data.jsonl --citations edges.jsonl
```

**Inference Rules:**
- Integer → `Int64`
- Float → `Float64`
- String → `String`
- Array of floats → `Vector` (dimension from first record)
- Object → `Json`

### Manual Schema

For production, define an explicit schema:

```json
{
  "schema_version": 1,

  "labels": {
    "Paper": { "id": 1, "is_document": false },
    "Author": { "id": 2, "is_document": false }
  },

  "edge_types": {
    "CITES": {
      "id": 1,
      "src_labels": ["Paper"],
      "dst_labels": ["Paper"]
    },
    "AUTHORED_BY": {
      "id": 2,
      "src_labels": ["Paper"],
      "dst_labels": ["Author"]
    }
  },

  "properties": {
    "Paper": {
      "title": { "type": "String", "nullable": false },
      "year": { "type": "Int32", "nullable": true },
      "abstract": { "type": "String", "nullable": true },
      "embedding": { "type": "Vector", "dimensions": 768 }
    },
    "Author": {
      "name": { "type": "String", "nullable": false },
      "email": { "type": "String", "nullable": true }
    },
    "AUTHORED_BY": {
      "position": { "type": "Int32", "nullable": true }
    }
  },

  "indexes": {
    "paper_embeddings": {
      "type": "vector",
      "label": "Paper",
      "property": "embedding",
      "config": { "index_type": "hnsw", "metric": "cosine" }
    }
  }
}
```

---

## Streaming Writes (Cypher)

For real-time applications, use CREATE statements.

### Creating Vertices

```cypher
// Single vertex
CREATE (p:Paper {
  title: 'New Research Paper',
  year: 2024,
  venue: 'ArXiv'
})
RETURN p

// With external ID
CREATE (p:Paper {
  id: 'paper_new_001',
  title: 'New Research'
})
```

### Creating Edges

```cypher
// Between existing vertices
MATCH (p:Paper {id: 'paper_001'}), (a:Author {id: 'author_001'})
CREATE (p)-[:AUTHORED_BY {position: 1}]->(a)

// Create both nodes and edge
CREATE (p:Paper {title: 'New Paper'})-[:AUTHORED_BY]->(a:Author {name: 'New Author'})
```

### Batch Creates

```cypher
// Multiple vertices
UNWIND $papers AS paper
CREATE (p:Paper {
  id: paper.id,
  title: paper.title,
  year: paper.year
})

// Multiple edges
UNWIND $edges AS edge
MATCH (src:Paper {id: edge.src}), (dst:Paper {id: edge.dst})
CREATE (src)-[:CITES]->(dst)
```

### HTTP API

```bash
# Create via HTTP
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "CREATE (p:Paper {title: $title, year: $year})",
    "params": {"title": "New Paper", "year": 2024}
  }'
```

---

## Programmatic API (Rust)

For maximum control, use the Rust API directly.

### Bulk Loading with BulkWriter

The `BulkWriter` API provides high-performance bulk loading with deferred index building:

```rust
use uni::*;

#[tokio::main]
async fn main() -> Result<()> {
    let db = Uni::open("./my-graph").build().await?;

    // Create a bulk writer with deferred indexing
    let mut bulk = db.bulk_writer()
        .defer_vector_indexes(true)   // Defer vector index updates
        .defer_scalar_indexes(true)   // Defer scalar index updates
        .batch_size(10_000)           // Flush every 10K records
        .on_progress(|progress| {
            println!("{}: {} rows processed",
                progress.phase, progress.rows_processed);
        })
        .build()?;

    // Bulk insert vertices
    let vertices: Vec<HashMap<String, Value>> = papers
        .iter()
        .map(|p| {
            let mut props = HashMap::new();
            props.insert("title".into(), Value::String(p.title.clone()));
            props.insert("year".into(), Value::Int32(p.year));
            props.insert("embedding".into(), Value::Vector(p.embedding.clone()));
            props
        })
        .collect();

    let vids = bulk.insert_vertices("Paper", vertices).await?;
    println!("Inserted {} vertices", vids.len());

    // Bulk insert edges
    let edges: Vec<EdgeData> = citations
        .iter()
        .map(|c| EdgeData {
            src_vid: vid_map[&c.from],
            dst_vid: vid_map[&c.to],
            properties: HashMap::new(),
        })
        .collect();

    let eids = bulk.insert_edges("CITES", edges).await?;
    println!("Inserted {} edges", eids.len());

    // Commit and rebuild indexes
    let stats = bulk.commit().await?;
    println!("Bulk load complete:");
    println!("  Vertices: {}", stats.vertices_inserted);
    println!("  Edges: {}", stats.edges_inserted);
    println!("  Indexes rebuilt: {}", stats.indexes_rebuilt);
    println!("  Duration: {:?}", stats.duration);

    Ok(())
}
```

### BulkWriter Options

| Option | Description | Default |
|--------|-------------|---------|
| `defer_vector_indexes(bool)` | Defer vector index updates until commit | `false` |
| `defer_scalar_indexes(bool)` | Defer scalar index updates until commit | `false` |
| `batch_size(usize)` | Records per batch before flushing | `10_000` |
| `async_indexes(bool)` | Build indexes in background after commit | `false` |
| `on_progress(callback)` | Progress callback for monitoring | None |

### Progress Monitoring

```rust
let mut bulk = db.bulk_writer()
    .on_progress(|progress| {
        match progress.phase {
            BulkPhase::Inserting => {
                println!("Inserting: {}/{}",
                    progress.rows_processed,
                    progress.total_rows.unwrap_or(0));
            }
            BulkPhase::RebuildingVectorIndex { label, property } => {
                println!("Building vector index: {}.{}", label, property);
            }
            BulkPhase::RebuildingScalarIndex { label, property } => {
                println!("Building scalar index: {}.{}", label, property);
            }
            BulkPhase::Finalizing => {
                println!("Finalizing snapshot...");
            }
            _ => {}
        }
    })
    .build()?;
```

### Aborting Bulk Operations

```rust
let mut bulk = db.bulk_writer().build()?;
bulk.insert_vertices("Paper", vertices).await?;

// Something went wrong - abort without committing
bulk.abort().await?;
// No data is persisted
```

### Performance Guidelines

| Dataset Size | Recommended Settings |
|--------------|---------------------|
| < 100K | `batch_size: 5_000`, `defer_*: false` |
| 100K - 1M | `batch_size: 10_000`, `defer_*: true` |
| 1M - 10M | `batch_size: 50_000`, `defer_*: true` |
| > 10M | `batch_size: 100_000`, `defer_*: true`, `async_indexes: true` |

---

### Low-Level Writer API

For fine-grained control, use the low-level writer:

```rust
use uni::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load or create schema
    let schema_manager = Arc::new(
        SchemaManager::load(Path::new("./storage/schema.json")).await?
    );

    // Create storage manager
    let storage = Arc::new(
        StorageManager::new("./storage", schema_manager.clone())
    );

    // Create writer
    let writer = Writer::new(
        storage.clone(),
        schema_manager.clone(),
        WriteConfig::default()
    );

    Ok(())
}
```

### Insert Vertices

```rust
use uni::core::{Vid, Properties};

// Allocate VID
let label_id = schema_manager.get_label_id("Paper")?;
let vid = writer.allocate_vid(label_id)?;

// Build properties
let mut props = Properties::new();
props.insert("title".into(), Value::String("New Paper".into()));
props.insert("year".into(), Value::Int32(2024));

// Insert
writer.insert_vertex(vid, props).await?;
```

### Insert Edges

```rust
use uni::core::{Eid, Direction};

// Get VIDs (from query or allocation)
let src_vid: Vid = /* ... */;
let dst_vid: Vid = /* ... */;

// Allocate EID
let edge_type_id = schema_manager.get_edge_type_id("CITES")?;
let eid = writer.allocate_eid(edge_type_id)?;

// Build properties
let mut props = Properties::new();
props.insert("weight".into(), Value::Float64(0.95));

// Insert
writer.insert_edge(src_vid, dst_vid, eid, edge_type_id, props).await?;
```

### Batch Inserts

```rust
// Batch vertex insert
let vertices: Vec<(Vid, Properties)> = papers
    .iter()
    .map(|p| {
        let vid = writer.allocate_vid(label_id)?;
        let props = build_properties(p);
        Ok((vid, props))
    })
    .collect::<Result<Vec<_>>>()?;

writer.insert_vertices_batch(vertices).await?;

// Batch edge insert
let edges: Vec<(Vid, Vid, Eid, u16, Properties)> = /* ... */;
writer.insert_edges_batch(edges).await?;
```

### Flush to Storage

```rust
// Manual flush
writer.flush_to_l1().await?;

// Auto-flush on threshold
let config = WriteConfig {
    max_mutations_before_flush: 10000,
    auto_flush: true,
    ..Default::default()
};
```

---

## Data Transformation

### Converting CSV to JSONL

```python
import csv
import json

# Convert CSV to JSONL
with open('papers.csv', 'r') as csv_file, open('papers.jsonl', 'w') as jsonl_file:
    reader = csv.DictReader(csv_file)
    for row in reader:
        # Transform as needed
        record = {
            'id': row['paper_id'],
            'title': row['title'],
            'year': int(row['year']),
            'venue': row['venue']
        }
        jsonl_file.write(json.dumps(record) + '\n')
```

### Adding Embeddings

```python
import json
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

with open('papers_raw.jsonl', 'r') as infile, open('papers.jsonl', 'w') as outfile:
    for line in infile:
        record = json.loads(line)
        # Generate embedding from title + abstract
        text = record['title'] + ' ' + record.get('abstract', '')
        embedding = model.encode(text).tolist()
        record['embedding'] = embedding
        outfile.write(json.dumps(record) + '\n')
```

### Extracting from Database

```python
import psycopg2
import json

conn = psycopg2.connect("postgresql://...")
cursor = conn.cursor()

# Export vertices
cursor.execute("SELECT id, title, year FROM papers")
with open('papers.jsonl', 'w') as f:
    for row in cursor:
        record = {'id': str(row[0]), 'title': row[1], 'year': row[2]}
        f.write(json.dumps(record) + '\n')

# Export edges
cursor.execute("SELECT citing_id, cited_id FROM citations")
with open('citations.jsonl', 'w') as f:
    for row in cursor:
        record = {'src': str(row[0]), 'dst': str(row[1])}
        f.write(json.dumps(record) + '\n')
```

---

## Incremental Updates

### Append-Only Pattern

```bash
# Initial load
uni import initial --papers papers_v1.jsonl --citations edges_v1.jsonl --output ./storage

# Incremental update (append new data)
uni import incremental --papers papers_new.jsonl --citations edges_new.jsonl \
    --output ./storage --mode append
```

### Delta Processing

```cypher
// Add new vertices
UNWIND $new_papers AS paper
MERGE (p:Paper {id: paper.id})
SET p.title = paper.title, p.year = paper.year

// Add new edges
UNWIND $new_edges AS edge
MATCH (src:Paper {id: edge.src}), (dst:Paper {id: edge.dst})
MERGE (src)-[:CITES]->(dst)
```

---

## Validation & Error Handling

### Schema Validation

```bash
# Validate data against schema
uni validate --data papers.jsonl --schema schema.json

# Output:
# Validated 10,000 records
# Errors: 0
# Warnings: 12 (nullable fields missing)
```

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Property type mismatch` | Wrong data type | Check schema types |
| `Unknown property` | Property not in schema | Add to schema or filter |
| `Vector dimension mismatch` | Wrong embedding size | Ensure consistent dimensions |
| `Duplicate ID` | Non-unique ext_id | Deduplicate source data |
| `Missing required property` | Null in non-nullable | Fix source data |

### Error Recovery

```bash
# Continue on errors (log failures)
uni import data --papers papers.jsonl --citations edges.jsonl \
    --output ./storage \
    --on-error continue \
    --error-log errors.jsonl
```

---

## Performance Tips

### Large File Handling

```bash
# Use streaming (default)
uni import large --papers huge_file.jsonl ...

# Split large files
split -l 1000000 huge_file.jsonl chunk_
# Import chunks sequentially or in parallel
```

### Memory Management

```bash
# Tune batch size based on memory
uni import data --batch-size 5000 ...  # Lower for less memory

# Monitor memory
RUST_LOG=uni=debug uni import ... 2>&1 | grep memory
```

### Parallel Import

```bash
# If data can be partitioned
uni import shard1 --papers shard1.jsonl ... --output ./storage/shard1 &
uni import shard2 --papers shard2.jsonl ... --output ./storage/shard2 &
wait
```

---

## Next Steps

- [Schema Design](schema-design.md) — Best practices for schema definition
- [Vector Search](vector-search.md) — Working with embeddings
- [Performance Tuning](performance-tuning.md) — Optimization strategies
