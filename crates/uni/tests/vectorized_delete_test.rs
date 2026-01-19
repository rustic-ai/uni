// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::UniConfig;
use uni::core::id::Vid;
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::runtime::writer::Writer;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_delete() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));
    let cache_arc = Some(storage.adjacency_cache());

    // We need to create Writer explicitly to pass it to Executor
    let writer = Arc::new(tokio::sync::RwLock::new(
        Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            0,
            UniConfig::default(),
            cache_arc,
            None,
            None,
        )
        .await
        .unwrap(),
    ));

    // 2. Insert Data
    // Alice, Bob, Charlie
    {
        let mut w = writer.write().await;
        for i in 0..3 {
            let vid = Vid::new(person_lbl, i);
            let mut props = HashMap::new();
            props.insert(
                "name".to_string(),
                json!(match i {
                    0 => "Alice",
                    1 => "Bob",
                    2 => "Charlie",
                    _ => "",
                }),
            );
            w.insert_vertex(vid, props).await?;
        }
        w.flush_to_l1(None).await?;
    }

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 3. Test DELETE
    // MATCH (n:Person) WHERE n.name = 'Bob' DELETE n
    let cypher = "MATCH (n:Person) WHERE n.name = 'Bob' DELETE n";
    let plan = planner.plan(CypherParser::new(cypher)?.parse()?)?;

    // Execute
    let _results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // Bob should be deleted.
    // Query remaining
    let cypher_check = "MATCH (n:Person) RETURN n.name ORDER BY n.name";
    let plan_check = planner.plan(CypherParser::new(cypher_check)?.parse()?)?;
    let results_check = executor
        .execute(plan_check, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results_check.len(), 2);
    assert_eq!(results_check[0].get("n.name"), Some(&json!("Alice")));
    assert_eq!(results_check[1].get("n.name"), Some(&json!("Charlie")));

    Ok(())
}
