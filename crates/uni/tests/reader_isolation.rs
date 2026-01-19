// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::runtime::writer::Writer;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_reader_isolation_lifecycle() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    // Executor with Writer (enables L0 access)
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // 2. Insert into L0 (No Flush)
    {
        let mut w = writer.write().await;
        let v1 = w.next_vid(person_lbl).await?;
        let mut p1 = HashMap::new();
        p1.insert("name".to_string(), json!("Alice"));
        w.insert_vertex(v1, p1).await?;
    }

    // 3. Query (Should see L0 data)
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(results.len(), 1, "Should find Alice in L0");
    assert_eq!(results[0].get("n.name"), Some(&json!("Alice")));

    // 4. Flush to Storage
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // 5. Query (Should see Storage data)
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(results.len(), 1, "Should find Alice in Storage");
    assert_eq!(results[0].get("n.name"), Some(&json!("Alice")));

    // 6. Delete in L0 (No Flush)
    // We need the VID.
    let alice_vid = {
        // Cheating a bit: we know it's VID(0, 0) but let's be robust
        // Scan to get VID
        let vids = executor
            .execute(
                planner.plan(CypherParser::new("MATCH (n:Person) RETURN n")?.parse()?)?,
                &prop_mgr,
                &HashMap::new(),
            )
            .await?;
        // Parse "1:0" string to Vid? Or just use writer logic if we kept the ID.
        // But let's assume we can get it via query if we project 'n'.
        // The Executor returns 'n' as "LabelId:Offset" string.
        let n_str = vids[0].get("n").unwrap().as_str().unwrap();
        let parts: Vec<&str> = n_str.split(':').collect();
        uni::core::id::Vid::new(parts[0].parse()?, parts[1].parse()?)
    };

    {
        let mut w = writer.write().await;
        w.delete_vertex(alice_vid).await?;
    }

    // 7. Query (Should NOT see Alice due to L0 tombstone)
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(
        results.len(),
        0,
        "Should NOT find Alice (masked by L0 tombstone)"
    );

    // 8. Flush (Commit deletion)
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // 9. Query (Should NOT see Alice in Storage)
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    println!("DEBUG: Plan for final query: {:?}", plan);
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;
    println!("DEBUG: Results for final query: {:?}", results);

    assert_eq!(
        results.len(),
        0,
        "Should NOT find Alice (deleted in Storage)"
    );

    Ok(())
}
