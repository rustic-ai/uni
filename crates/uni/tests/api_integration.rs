// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, VectorAlgo, VectorIndexCfg, VectorMetric};

#[tokio::test]
async fn test_api_vector_search() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Paper")
        .property("title", DataType::String)
        .vector("embedding", 3)
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::L2,
            }),
        )
        .apply()
        .await?;

    // Insert data
    db.execute("CREATE (:Paper {title: 'Graph DB', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    db.execute("CREATE (:Paper {title: 'Vector DB', embedding: [0.0, 1.0, 0.0]})")
        .await?;

    // Flush to make data visible to vector search (Lance)
    db.flush().await?;

    // Search
    let query_vec = vec![0.9, 0.1, 0.0];
    let results = db
        .vector_search("Paper", "embedding", &query_vec, 1)
        .await?;
    assert_eq!(results.len(), 1);

    // Fetch full nodes
    let nodes = db
        .vector_search_with("Paper", "embedding", &query_vec)
        .k(1)
        .fetch_nodes()
        .await?;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].0.get::<String>("title")?, "Graph DB");

    Ok(())
}

#[tokio::test]
async fn test_api_algorithms() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    // A -> B -> C
    db.execute("CREATE (a:Person {id: 1}), (b:Person {id: 2}), (c:Person {id: 3})")
        .await?;
    db.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}), (c:Person {id: 3}) 
                CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
    )
    .await?;

    // PageRank
    let scores = db.algo().pagerank().run().await?;
    assert_eq!(scores.len(), 3);

    // WCC
    let components = db.algo().wcc().run().await?;
    assert_eq!(components.len(), 3);
    // In A->B->C, all are in the same component in undirected view (WCC)
    let comp_id = components[0].1;
    for (_, c) in components {
        assert_eq!(c, comp_id);
    }

    Ok(())
}

#[tokio::test]
async fn test_api_transactions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Account")
        .property("balance", DataType::Int64)
        .apply()
        .await?;

    // 1. Successful transaction
    let tx = db.begin().await?;
    tx.execute("CREATE (:Account {balance: 100})").await?;
    tx.execute("CREATE (:Account {balance: 200})").await?;
    tx.commit().await?;

    let result = db
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("total")?, 300);

    // 2. Rollback transaction
    let tx = db.begin().await?;
    tx.execute("CREATE (:Account {balance: 500})").await?;
    // Data should be visible inside transaction
    let res_inner = tx
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(res_inner.rows()[0].get::<i64>("total")?, 800);

    tx.rollback().await?;

    // Data should NOT be visible after rollback
    let res_outer = db
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(res_outer.rows()[0].get::<i64>("total")?, 300);

    // 3. Closure-based transaction
    db.transaction(|tx| {
        Box::pin(async move {
            tx.execute("CREATE (:Account {balance: 1000})").await?;
            Ok(())
        })
    })
    .await?;

    let result = db
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("total")?, 1300);

    Ok(())
}

#[tokio::test]
async fn test_api_schema_and_property_query() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. Define Schema
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int32)
        .index("name", IndexType::Scalar(ScalarType::BTree))
        .label("Movie")
        .property("title", DataType::String)
        .edge_type("ACTED_IN", &["Person"], &["Movie"])
        .property("role", DataType::String)
        .apply()
        .await?;

    // 2. Insert Data using Cypher
    db.execute("CREATE (:Person {name: 'Tom Hanks', age: 68})")
        .await?;
    db.execute("CREATE (:Movie {title: 'Cast Away'})").await?;
    db.execute(
        "
        MATCH (p:Person {name: 'Tom Hanks'}), (m:Movie {title: 'Cast Away'})
        CREATE (p)-[:ACTED_IN {role: 'Chuck Noland'}]->(m)
    ",
    )
    .await?;

    // 3. Query properties
    let result = db
        .query("MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, p.age, r.role, m.title")
        .await?;
    assert_eq!(result.len(), 1);

    let row = &result.rows()[0];
    assert_eq!(row.get::<String>("p.name")?, "Tom Hanks");
    assert_eq!(row.get::<i32>("p.age")?, 68);
    assert_eq!(row.get::<String>("r.role")?, "Chuck Noland");
    assert_eq!(row.get::<String>("m.title")?, "Cast Away");

    Ok(())
}

#[tokio::test]
async fn test_api_query_flow() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema implicitly? No, need schema first for properties
    // Or we can rely on "schemaless" if supported?
    // Current Uni requires schema for properties.
    // For now, let's create a label using internal schema manager until Phase 3 (Schema API).
    // Accessing internal schema manager is possible via db.schema (it's pub(crate)).
    // Wait, integration tests are outside the crate, so they can't access pub(crate).
    // I need to use the Schema API or hacks.
    // But Schema API is Phase 3.

    // Can I run a query without schema?
    // "CREATE (n:Person {name: 'Alice'})"
    // The legacy executor/planner might require schema for "name" property.
    // Let's check if I can use raw `db.storage.schema_manager().add_label(...)`?
    // No, `db.storage` is not pub.

    // I can assume Phase 3 is next, so I can't test properties yet.
    // But I can test CREATE (n:Person) without properties if it's allowed.
    // `SchemaManager::add_label` checks existence.
    // `Planner` checks if label exists.

    // Implementation Plan Phase 3 is next.
    // I should implement Phase 3 Schema API to test this properly?
    // Or I can add a temporary helper in `Uni`?
    // Or I can just test `RETURN 1` which doesn't need schema.

    // Test 1: Simple scalar return
    let result = db.query("RETURN 1 AS num, 'hello' AS str").await?;
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];
    let num: i64 = row.get("num")?;
    let s: String = row.get("str")?;
    assert_eq!(num, 1);
    assert_eq!(s, "hello");

    // Test 2: List and Map
    let result = db.query("RETURN [1, 2, 3] AS list, {a: 1} AS map").await?;
    let row = &result.rows()[0];
    // Lists come back as Value::List
    let list: Vec<i64> = row.get("list")?;
    assert_eq!(list, vec![1, 2, 3]);

    // Map support in Executor might be limited (it might return String if not handled well)
    // But let's see. My `extract_value` handles StructArray -> Map.
    // Does legacy executor return Map? Yes, JsonValue::Object -> Map.
    // Does vectorized executor support Map construction?
    // Vectorized projection of map literal might be supported.

    // Test 3: Params
    let result = db
        .query_with("RETURN $x AS x")
        .param("x", 42)
        .fetch_all()
        .await?;
    let x: i64 = result.rows()[0].get("x")?;
    assert_eq!(x, 42);

    Ok(())
}
