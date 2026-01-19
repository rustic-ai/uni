// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use tempfile::tempdir;
use uni::{Uni, Value};

#[tokio::test]
async fn test_vectorized_create() -> Result<()> {
    let dir = tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;

    // Create Schema
    db.query("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    db.query("CREATE EDGE TYPE KNOWS () FROM Person TO Person")
        .await?;

    // 1. CREATE node
    db.query("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;

    // 2. Verify
    let results = db.query("MATCH (n:Person) RETURN n.name, n.age").await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].value("n.name"), Some(&Value::from("Alice")));
    assert_eq!(results.rows[0].value("n.age"), Some(&Value::from(30)));

    // 3. CREATE with MATCH (multiple nodes)
    db.query("MATCH (n:Person) CREATE (n)-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;

    // 4. Verify
    let results = db
        .query("MATCH (p:Person {name: 'Bob'}) RETURN count(p) as count")
        .await?;
    assert_eq!(results.rows[0].value("count"), Some(&Value::from(1)));

    Ok(())
}

#[tokio::test]
async fn test_vectorized_set_remove() -> Result<()> {
    let dir = tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;

    db.query("CREATE LABEL Person (name STRING, age INT, city STRING)")
        .await?;

    db.query("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;

    // 1. SET property
    db.query("MATCH (n:Person {name: 'Alice'}) SET n.age = 31, n.city = 'London'")
        .await?;

    // 2. Verify
    let results = db
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.age, n.city")
        .await?;
    assert_eq!(results.rows[0].value("n.age"), Some(&Value::from(31)));
    assert_eq!(
        results.rows[0].value("n.city"),
        Some(&Value::from("London"))
    );

    // 3. REMOVE property
    db.query("MATCH (n:Person {name: 'Alice'}) REMOVE n.city")
        .await?;

    // 4. Verify
    let results = db
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.city")
        .await?;
    assert_eq!(results.rows[0].value("n.city"), Some(&Value::Null));

    Ok(())
}

#[tokio::test]
async fn test_vectorized_merge() -> Result<()> {
    let dir = tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;

    db.query("CREATE LABEL Person (name STRING, created BOOL, matched BOOL)")
        .await?;

    // 1. MERGE (create)
    db.query("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true")
        .await?;

    // 2. Verify
    let results = db
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.created")
        .await?;
    assert_eq!(results.rows[0].value("n.created"), Some(&Value::from(true)));

    // 3. MERGE (match)
    db.query("MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.matched = true")
        .await?;

    // 4. Verify
    let results = db
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.created, n.matched")
        .await?;
    assert_eq!(results.rows[0].value("n.created"), Some(&Value::from(true)));
    assert_eq!(results.rows[0].value("n.matched"), Some(&Value::from(true)));

    Ok(())
}

#[tokio::test]
async fn test_vectorized_merge_relationship() -> Result<()> {
    let dir = tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;

    db.query("CREATE LABEL Person (name STRING)").await?;
    db.query("CREATE EDGE TYPE KNOWS () FROM Person TO Person")
        .await?;

    db.query("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
        .await?;

    // MERGE relationship between existing nodes
    db.query("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await?;

    // Verify
    let results = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .await?;
    assert_eq!(results.len(), 1);

    // MERGE again (should not create duplicate)
    db.query("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await?;
    let results = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) as count")
        .await?;
    assert_eq!(results.rows[0].value("count"), Some(&Value::from(1)));

    Ok(())
}
