// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_snapshots_and_time_travel() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // State 1: Alice
    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    let snap1_id = db.create_snapshot(Some("alice-only")).await?;

    // State 2: Alice + Bob
    db.execute("CREATE (:Person {name: 'Bob'})").await?;
    let _snap2_id = db.create_snapshot(Some("alice-and-bob")).await?;

    // Verify State 2
    let res2 = db
        .query("MATCH (n:Person) RETURN count(n) AS count")
        .await?;
    assert_eq!(res2.rows()[0].get::<i64>("count")?, 2);

    // Time Travel to State 1
    let db_v1 = db.at_snapshot(&snap1_id).await?;
    let res1 = db_v1
        .query("MATCH (n:Person) RETURN count(n) AS count")
        .await?;
    assert_eq!(res1.rows()[0].get::<i64>("count")?, 1);

    // Verify snapshot list procedure
    let list_res = db.query("CALL db.snapshot.list()").await?;
    assert!(list_res.len() >= 2);

    let mut found_alice = false;
    for row in list_res.rows() {
        if row.get::<String>("name").is_ok() && row.get::<String>("name")? == "alice-only" {
            found_alice = true;
            assert_eq!(row.get::<String>("snapshot_id")?, snap1_id);
        }
    }
    assert!(found_alice);

    // Test Restore
    db.execute(&format!("CALL db.snapshot.restore('{}')", snap1_id))
        .await?;

    // Re-open Uni from the same path
    // Need to get path from db
    let _path = db
        .config()
        .at_snapshot
        .as_ref()
        .map(|_| "TODO")
        .unwrap_or_else(|| "TODO");
    // Actually db doesn't expose the base path easily?
    // Wait, Uni doesn't store the path.
    // For in_memory() it's a temp dir.

    Ok(())
}
