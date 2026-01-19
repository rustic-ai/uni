# CLI Reference

Complete reference for the `uni` command-line interface. The CLI provides tools for data import, query execution, and server management.

## Synopsis

```bash
uni [OPTIONS] <COMMAND>
```

## Global Options

| Option | Description |
|--------|-------------|
| `-h, --help` | Print help information |
| `-V, --version` | Print version information |
| `-v, --verbose` | Enable verbose logging (can be repeated: -vv, -vvv) |
| `--quiet` | Suppress non-essential output |
| `--log-format <FORMAT>` | Log format: `text` (default), `json` |

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RUST_LOG` | Log level filter (e.g., `info`, `debug`, `uni=debug`) | `warn` |
| `UNI_STORAGE_PATH` | Default storage path | `./storage` |
| `UNI_CONFIG` | Path to configuration file | `./uni.toml` |
| `AWS_REGION` | AWS region for S3 storage | — |
| `AWS_ACCESS_KEY_ID` | AWS access key | — |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | — |

---

## Commands

### `import` — Import Data

Import vertices and edges from JSONL files into a new or existing database.

#### Synopsis

```bash
uni import <NAME> [OPTIONS]
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Dataset name identifier (e.g., `semantic-scholar`, `products`) |

#### Options

| Option | Description | Required |
|--------|-------------|----------|
| `--papers <PATH>` | Path to vertices JSONL file | Yes |
| `--citations <PATH>` | Path to edges JSONL file | Yes |
| `--output <PATH>` | Output directory for storage | No (default: `./storage`) |
| `--schema <PATH>` | Path to schema JSON file | No (auto-inferred) |
| `--batch-size <N>` | Records per batch during import | No (default: 10000) |
| `--skip-validation` | Skip schema validation (faster but risky) | No |
| `--force` | Overwrite existing storage | No |

#### Examples

**Basic import:**
```bash
uni import products \
    --papers ./data/products.jsonl \
    --citations ./data/purchases.jsonl \
    --output ./product-graph
```

**Import with custom schema:**
```bash
uni import research \
    --papers ./papers.jsonl \
    --citations ./citations.jsonl \
    --schema ./schema.json \
    --output ./research-graph
```

**Large dataset with tuned batch size:**
```bash
uni import wikipedia \
    --papers ./articles.jsonl \
    --citations ./links.jsonl \
    --batch-size 50000 \
    --output s3://my-bucket/wiki-graph
```

#### Input File Format

**Vertices JSONL:**
```json
{"id": "node_123", "label": "Product", "name": "Widget", "price": 29.99, "embedding": [0.1, 0.2, ...]}
```

- `id` (required): Unique string identifier
- `label` (optional): Vertex label (default: inferred from import name)
- Additional fields: Property key-value pairs

**Edges JSONL:**
```json
{"src": "user_456", "dst": "node_123", "type": "PURCHASED", "timestamp": "2024-01-15T10:30:00Z"}
```

- `src` (required): Source vertex ID
- `dst` (required): Destination vertex ID
- `type` (optional): Edge type (default: `RELATES_TO`)
- Additional fields: Edge properties

---

### `query` — Execute Queries

Run OpenCypher queries against a database.

#### Synopsis

```bash
uni query <STATEMENT> [OPTIONS]
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `<STATEMENT>` | Cypher query string |

#### Options

| Option | Description | Default |
|----------|-------------|---------|
| `--path <PATH>` | Storage directory path | `./storage` |

#### Examples

**Basic query with table output:**
```bash
uni query "MATCH (n:Person) RETURN n.name LIMIT 10" --path ./social-graph
```

**Output:**
```
┌──────────┐
│ n.name   │
├──────────┤
│ Alice    │
│ Bob      │
└──────────┘
```

---

### `repl` — Interactive Shell

Start the interactive UniDB shell for running Cypher queries.

#### Synopsis

```bash
uni repl [OPTIONS]
# or simply
uni
```

#### Options

| Option | Description | Default |
|----------|-------------|---------|
| `--path <PATH>` | Storage directory path | `./storage` |

#### Shell Commands

Within the REPL, you can use the following commands:

| Command | Description |
|---------|-------------|
| `help` | Show available commands |
| `clear` | Clear the screen |
| `exit`, `quit` | Exit the REPL |
| `<cypher>` | Execute a Cypher query |

---

### `snapshot` — Manage Snapshots

Create, list, and restore database snapshots for point-in-time recovery and history tracking.

#### Synopsis

```bash
uni snapshot <SUBCOMMAND> [OPTIONS]
```

#### Subcommands

**`list`** — List all available snapshots.
```bash
uni snapshot list --path ./storage
```

**`create`** — Create a new named snapshot.
```bash
uni snapshot create [--name <NAME>] --path ./storage
```

**`restore`** — Restore the database to a specific snapshot ID.
```bash
uni snapshot restore <ID> --path ./storage
```

---

### `start` — Start Server

Start the Uni HTTP API server for remote access.

#### Synopsis

```bash
uni start [OPTIONS]
```

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--port <PORT>` | HTTP port to listen on | 8080 |
| `--host <HOST>` | Host address to bind | `127.0.0.1` |
| `--path <PATH>` | Storage directory path | `./storage` |
| `--workers <N>` | Number of worker threads | CPU count |
| `--read-only` | Disable write operations | false |
| `--cors` | Enable CORS for all origins | false |

#### Examples

**Start on default port:**
```bash
uni start --path ./my-graph
```

**Production configuration:**
```bash
uni start \
    --host 0.0.0.0 \
    --port 9000 \
    --path s3://bucket/graph \
    --workers 8 \
    --read-only
```

#### API Endpoints

When the server is running:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/query` | POST | Execute Cypher query |
| `/schema` | GET | Get schema definition |
| `/stats` | GET | Database statistics |

**Query API example:**
```bash
curl -X POST http://localhost:8080/query \
    -H "Content-Type: application/json" \
    -d '{"query": "MATCH (n) RETURN COUNT(n)"}'
```

Response:
```json
{
  "results": [{"COUNT(n)": 10000}],
  "stats": {
    "rows": 1,
    "execution_time_ms": 12.5
  }
}
```

---

### Schema, Stats, and Index Management

Schema inspection, database statistics, and index management are available through Cypher queries in the REPL or via the query command:

**Schema inspection:**
```bash
# View labels
uni query "CALL db.labels() YIELD label RETURN label" --path ./graph

# View relationship types
uni query "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType" --path ./graph

# View indexes
uni query "SHOW INDEXES" --path ./graph
```

**Database statistics:**
```bash
# Count vertices and edges
uni query "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count" --path ./graph

# Count by edge type
uni query "MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS count" --path ./graph
```

**Index management:**
```bash
# Create a vector index
uni query "CREATE VECTOR INDEX paper_emb FOR (p:Paper) ON p.embedding OPTIONS {type: 'hnsw', metric: 'cosine'}" --path ./graph

# Create a scalar index
uni query "CREATE INDEX author_name FOR (a:Author) ON a.name" --path ./graph

# Drop an index
uni query "DROP INDEX paper_emb" --path ./graph
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |
| 3 | Storage error (file not found, permission denied) |
| 4 | Query error (parse error, execution failure) |
| 5 | Timeout exceeded |

---

## Shell Completion

Generate shell completions for faster CLI usage:

**Bash:**
```bash
uni completions bash > /etc/bash_completion.d/uni
```

**Zsh:**
```bash
uni completions zsh > ~/.zsh/completions/_uni
```

**Fish:**
```bash
uni completions fish > ~/.config/fish/completions/uni.fish
```

---

## See Also

- [Quick Start](quickstart.md) — Tutorial introduction
- [Cypher Querying](../guides/cypher-querying.md) — Query language reference
- [Configuration](../reference/configuration.md) — Configuration file options
