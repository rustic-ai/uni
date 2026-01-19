// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni::Uni;

#[tokio::test]
async fn test_relationship_disjunction() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .edge_type("KNOWS", &["Person"], &["Person"])
        .edge_type("LIKES", &["Person"], &["Person"])
        .apply()
        .await?;

    db.execute("CREATE (a:Person {name: 'A'})").await?;
    db.execute("CREATE (b:Person {name: 'B'})").await?;
    db.execute("CREATE (c:Person {name: 'C'})").await?;

    db.execute("MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) CREATE (a)-[:KNOWS]->(b)")
        .await?;
    db.execute("MATCH (a:Person {name: 'A'}), (c:Person {name: 'C'}) CREATE (a)-[:LIKES]->(c)")
        .await?;

    // Query with disjunction
    let result = db
        .query("MATCH (a:Person {name: 'A'})-[r:KNOWS|LIKES]->(other) RETURN other.name")
        .await?;

    assert_eq!(result.len(), 2);
    let mut names: Vec<String> = result
        .rows
        .iter()
        .map(|r| r.get::<String>("other.name").unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["B", "C"]);

    Ok(())
}

#[tokio::test]
async fn test_yield_aliasing() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // db.labels() returns "label" column
    let _result = db
        .query("CALL db.labels() YIELD label AS l RETURN l")
        .await?;

    // We expect some labels (Person, etc. if we created them, but this is fresh DB so maybe empty or just system?)
    // Let's create one
    db.schema().label("TestLabel").apply().await?;

    let result = db
        .query("CALL db.labels() YIELD label AS l RETURN l")
        .await?;
    assert!(!result.is_empty());

    // Check if column name is "l"
    // Note: Uni query result columns might be sorted or as projected.
    // The columns in QueryResult should reflect the RETURN clause.
    let _col_idx = result
        .columns
        .iter()
        .position(|c| c == "l")
        .expect("Column 'l' not found");
    assert!(!result.rows.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_call_composition() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").apply().await?;
    db.execute("CREATE (:Person {name: 'Alice'})").await?;

    // CALL ... YIELD ... MATCH ...
    // Note: MATCH (n) WHERE n:Person is not yet fully supported for label inference in Scan.
    // Using MATCH (n:Person) instead.
    let query = "
        CALL db.labels() YIELD label
        MATCH (n:Person) WHERE label = 'Person'
        RETURN n.name, label
    ";

    let result = db.query(query).await?;
    // db.labels() returns "Person". So filter label='Person' matches.
    assert_eq!(result.len(), 1);
    let name: String = result.rows[0].get("n.name")?;
    assert_eq!(name, "Alice");

    Ok(())
}
