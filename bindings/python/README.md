# Uni Python Bindings

[![PyPI](https://img.shields.io/pypi/v/uni.svg)](https://pypi.org/project/uni/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Python bindings for the **Uni** embedded graph database.

Part of [The Rustic Initiative](https://www.rustic.ai) by [Dragonscale Industries Inc.](https://www.dragonscale.ai)

## Installation

You need `maturin` to build and install the bindings.

```bash
pip install maturin
maturin develop
```

## Usage

```python
import uni

# Open or create a database
db = uni.Database("./my_db")

# Create Schema
db.create_label("Person")
db.create_edge_type("KNOWS")

# Create Vector Index
db.create_vector_index("Person", "embedding", 128, "l2")

# Create Data
db.query("CREATE (n:Person {name: 'Alice', age: 30})")

# Query Data
results = db.query("MATCH (n:Person) RETURN n.name as name")
for row in results:
    print(row['name'])

# Transactions
tx = db.begin()
tx.query("CREATE (n:Person {name: 'Bob'})")
tx.commit()
```

## Development

Run tests:
```bash
# Build libuni.so for testing (if not using maturin develop)
cargo build
cp ../../target/debug/libuni.so uni.so  # Linux
# OR
cp ../../target/debug/libuni.dylib uni.so # Mac

# Run tests
python3 tests/test_basic.py
python3 tests/test_advanced.py
```

## Documentation

- [Full Documentation](https://rustic-ai.github.io/uni)
- [Python API Reference](https://rustic-ai.github.io/uni/api/python/)
- [GitHub Repository](https://github.com/rustic-ai/uni)

## License

Apache 2.0
