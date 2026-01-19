# Supply Chain & BOM Analysis

Supply chains are deeply nested graphs (Part A -> Part B -> Part C). Uni's specialized support for **recursive traversals** and **document storage** makes it an excellent fit for Bill of Materials (BOM) management and impact analysis.

## Why Uni for Supply Chain?

| Challenge | Traditional Approach | Uni Approach |
|-----------|----------------------|--------------|
| **Recursive Depth** | SQL `WITH RECURSIVE` is slow and hard to optimize. | **Vectorized Recursion**: `MATCH (n)-[:PART_OF*]->(m)` is optimized for deep paths. |
| **Variable Data** | Parts have widely varying specs (resistors vs. CPUs). Relational schemas explode. | **Document Mode**: Store distinct part specs as JSON in the same `Part` table. |
| **Analytics** | Hard to aggregage cost/weight up the tree. | **Aggregation Pushdown**: Fast summation over traversal paths. |

---

## Scenario: Product Impact Analysis

A supplier notifies you that "Part X" has a defect. You need to find:
1.  All finished products that contain Part X (recursively).
2.  The total revenue at risk.

### 1. Schema Definition

We enable **Document Mode** for `Part` to store varied technical specifications.

**`schema.json`**
```json
{
  "labels": {
    "Part": {
      "id": 1,
      "is_document": true // Allows storing arbitrary JSON in _doc
    },
    "Supplier": { "id": 2 },
    "Product": { "id": 3 }
  },
  "edge_types": {
    "ASSEMBLED_FROM": { "id": 1, "src_labels": ["Part", "Product"], "dst_labels": ["Part"] },
    "SUPPLIED_BY": { "id": 2, "src_labels": ["Part"], "dst_labels": ["Supplier"] }
  },
  "properties": {
    "Part": {
      "sku": { "type": "String", "nullable": false },
      "cost": { "type": "Float64", "nullable": false }
    },
    "Product": {
      "name": { "type": "String", "nullable": false },
      "price": { "type": "Float64", "nullable": false }
    }
  },
  "indexes": [
    {
      "type": "Scalar",
      "name": "part_sku",
      "label": "Part",
      "properties": ["sku"],
      "index_type": "Hash"
    },
    {
      "type": "Scalar",
      "name": "json_spec_voltage",
      "label": "Part",
      "properties": ["$.specs.voltage"], // JSON Path Indexing (Planned feature)
      "index_type": "BTree"
    }
  ]
}
```

### 2. Ingestion (With Documents)

When inserting parts, we put fixed fields in properties and variable fields in `_doc`.

```rust
// Rust Example
writer.insert_vertex(vid, json!({
    "sku": "RES-10K",
    "cost": 0.05,
    "_doc": {
        "type": "resistor",
        "specs": { "resistance": "10k", "tolerance": "5%" },
        "compliance": ["RoHS"]
    }
})).await?;
```

### 3. Query: BOM Explosion

Find all products affected by the defective part.

```cypher
// Start at the defective part
MATCH (defective:Part {sku: 'RES-10K'})

// Traverse UP the assembly tree (incoming ASSEMBLED_FROM edges)
// Variable length path: *1..20 levels deep
MATCH (product:Product)-[:ASSEMBLED_FROM*1..20]->(defective)

// Return unique affected products and their price
RETURN DISTINCT 
    product.name, 
    product.price
ORDER BY product.price DESC
```

### 4. Query: Cost Rollup

Calculate the total cost of a product by summing the cost of all its constituent parts.

```cypher
MATCH (p:Product {name: 'Smartphone X'})
MATCH (p)-[:ASSEMBLED_FROM*]->(part:Part)
RETURN p.name, SUM(part.cost) AS total_bom_cost
```

### Key Advantages

*   **Deep Traversal Speed**: Uni's adjacency cache ensures that each hop is a simple memory lookup, not a disk seek. BOM explosions that take seconds in SQL take milliseconds in Uni.
*   **Schema Flexibility**: You can store resistors, capacitors, screens, and batteries in the same `Part` dataset without a sparse column mess.
*   **Vector Potential**: You could even add embeddings to parts (e.g., image of the component) to find visual duplicates in your inventory.
