// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni::Uni;

#[tokio::test]
async fn test_session_variables() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Create Schema
    db.query(
        r#"
        CALL db.createLabel('User', {
            "properties": {
                "name": { "type": "STRING" },
                "tenant": { "type": "STRING" }
            }
        })
    "#,
    )
    .await?;

    db.query("CREATE (n:User {name: 'Alice', tenant: 'A'})")
        .await?;
    db.query("CREATE (n:User {name: 'Bob', tenant: 'B'})")
        .await?;

    // Create session
    let session = db.session().set("tenant_id", "A").build();

    // Query with session variable
    // $session.tenant_id should resolve to "A"
    let results = session
        .query("MATCH (n:User) WHERE n.tenant = $session.tenant_id RETURN n.name")
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<String>("n.name")?, "Alice");

    Ok(())
}
