// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_unique_constraint_enforcement() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Define label with UNIQUE constraint
    db.execute("CREATE LABEL User (email STRING UNIQUE, name STRING)")
        .await?;

    // Insert first user
    db.execute("CREATE (u:User {email: 'alice@example.com', name: 'Alice'})")
        .await?;

    // Insert second user with DIFFERENT email -> Should succeed
    db.execute("CREATE (u:User {email: 'bob@example.com', name: 'Bob'})")
        .await?;

    // Insert third user with DUPLICATE email -> Should fail
    let result = db
        .execute("CREATE (u:User {email: 'alice@example.com', name: 'Alice2'})")
        .await;

    assert!(result.is_err(), "Duplicate email insert should have failed");
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("constraint"),
        "Error should mention constraint violation: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_not_null_constraint_enforcement() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Define label with NOT NULL constraint
    db.execute("CREATE LABEL Product (id STRING, price FLOAT NOT NULL)")
        .await?;

    // Insert valid product
    db.execute("CREATE (p:Product {id: 'p1', price: 10.0})")
        .await?;

    // Insert product with MISSING price -> Should fail
    let result = db.execute("CREATE (p:Product {id: 'p2'})").await;

    assert!(
        result.is_err(),
        "Insert with missing NOT NULL property should have failed"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("null"),
        "Error should mention null/missing property: {}",
        err
    );

    // Insert product with EXPLICIT NULL price -> Should fail (if parser allows null literal for property)
    // Note: Cypher CREATE syntax usually doesn't allow explicit null for property unless passed as param?
    // CREATE (p:Product {id: 'p3', price: null}) is valid Cypher.

    // Let's test with NULL literal if parser supports it
    // db.execute("CREATE (p:Product {id: 'p3', price: null})").await ...

    Ok(())
}
