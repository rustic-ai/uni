// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_recursive_cte_execution() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Node (id INT)").await?;
    db.execute("CREATE EDGE TYPE CHILD () FROM Node TO Node")
        .await?;

    db.execute("CREATE (n0:Node {id: 0})").await?;
    db.execute("CREATE (n1:Node {id: 1})").await?;
    db.execute("CREATE (n2:Node {id: 2})").await?;

    db.execute("MATCH (n0:Node {id: 0}), (n1:Node {id: 1}) CREATE (n0)-[:CHILD]->(n1)")
        .await?;
    db.execute("MATCH (n1:Node {id: 1}), (n2:Node {id: 2}) CREATE (n1)-[:CHILD]->(n2)")
        .await?;

    // Query: Start at 0, follow CHILD recursively
    let query = "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Node {id: 0}) RETURN root
            UNION
            MATCH (parent:Node)-[:CHILD]->(child:Node)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Node) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ";

    let result = db.query(query).await?;
    assert_eq!(result.len(), 3);

    let rows = result.rows();
    assert_eq!(rows[0].get::<i64>("id")?, 0);
    assert_eq!(rows[1].get::<i64>("id")?, 1);
    assert_eq!(rows[2].get::<i64>("id")?, 2);

    Ok(())
}
