# Interactive Examples

This section contains interactive Jupyter notebooks demonstrating Uni's capabilities across various use cases.

## Available Examples

We provide examples in both **Python** and **Rust** to match your preferred development environment.

### Use Cases

| Use Case | Description | Python | Rust |
|----------|-------------|--------|------|
| **Supply Chain** | BOM explosion, cost rollup, defect tracking | [Python](python/supply_chain.ipynb) | [Rust](rust/supply_chain.ipynb) |
| **Recommendation** | Collaborative filtering, vector similarity | [Python](python/recommendation.ipynb) | [Rust](rust/recommendation.ipynb) |
| **RAG** | Knowledge graph + vector search for LLM context | [Python](python/rag.ipynb) | [Rust](rust/rag.ipynb) |
| **Fraud Detection** | Cycle detection, shared device analysis | [Python](python/fraud_detection.ipynb) | [Rust](rust/fraud_detection.ipynb) |
| **Sales Analytics** | Graph traversal with columnar aggregations | [Python](python/sales_analytics.ipynb) | [Rust](rust/sales_analytics.ipynb) |

## Running the Notebooks

### Python Notebooks

```bash
# Install dependencies
cd bindings/python
pip install -e .

# Run Jupyter
jupyter notebook examples/
```

### Rust Notebooks

Rust notebooks require the `evcxr_jupyter` kernel:

```bash
# Install the Rust Jupyter kernel
cargo install evcxr_jupyter
evcxr_jupyter --install

# Run Jupyter
jupyter notebook examples/rust/
```

## What You'll Learn

Each notebook demonstrates:

- **Schema Design** - Defining labels, edge types, and properties
- **Data Ingestion** - Bulk loading vertices and edges
- **Cypher Queries** - Pattern matching, filtering, aggregations
- **Graph Algorithms** - Traversals, path finding, cycle detection
- **Vector Search** - Semantic similarity with embeddings

## Source Code

The notebook source files are also available in the repository:

- Python: [`bindings/python/examples/`](https://github.com/rustic-ai/uni/tree/main/bindings/python/examples)
- Rust: [`examples/rust/`](https://github.com/rustic-ai/uni/tree/main/examples/rust)
