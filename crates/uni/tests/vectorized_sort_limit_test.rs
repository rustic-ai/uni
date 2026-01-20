// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::UniConfig;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_sort_limit() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int32, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));
    let cache_arc = Some(storage.adjacency_cache());
    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        UniConfig::default(),
        cache_arc,
        None,
        None,
    )
    .await
    .unwrap();

    // 2. Insert Data
    // Alice 25, Bob 35, Charlie 20, David 40
    let users = [("Alice", 25), ("Bob", 35), ("Charlie", 20), ("David", 40)];

    for (i, (name, age)) in users.iter().enumerate() {
        let vid = Vid::new(person_lbl, i as u64); // Assuming id matches index
        let mut props = HashMap::new();
        props.insert("name".to_string(), json!(name));
        props.insert("age".to_string(), json!(age));
        writer.insert_vertex(vid, props).await?;
    }

    writer.flush_to_l1(None).await?;

    let executor = Executor::new(storage.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Test 1: ORDER BY age ASC
    {
        let cypher = "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC";
        let plan = planner.plan(CypherParser::new(cypher)?.parse()?)?;
        let results = executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;

        assert_eq!(results.len(), 4);
        assert_eq!(
            results[0].get("n.name"),
            Some(&Value::String("Charlie".to_string()))
        );
        assert_eq!(
            results[1].get("n.name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            results[2].get("n.name"),
            Some(&Value::String("Bob".to_string()))
        );
        assert_eq!(
            results[3].get("n.name"),
            Some(&Value::String("David".to_string()))
        );
    }

    // Test 2: LIMIT
    {
        let cypher = "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC LIMIT 2";
        let plan = planner.plan(CypherParser::new(cypher)?.parse()?)?;
        let results = executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].get("n.name"),
            Some(&Value::String("Charlie".to_string()))
        );
        assert_eq!(
            results[1].get("n.name"),
            Some(&Value::String("Alice".to_string()))
        );
    }

    // Test 3: SKIP + LIMIT
    {
        let cypher = "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC SKIP 1 LIMIT 2";
        let plan = planner.plan(CypherParser::new(cypher)?.parse()?)?;
        let results = executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].get("n.name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            results[1].get("n.name"),
            Some(&Value::String("Bob".to_string()))
        );
    }

    Ok(())
}
