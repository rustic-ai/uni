// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::{Value, json};
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
async fn test_vectorized_late_materialization() -> anyhow::Result<()> {
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
    // Person 0: Alice, 30
    // Person 1: Bob, 20
    let vid0 = Vid::new(person_lbl, 0);
    let vid1 = Vid::new(person_lbl, 1);

    let mut props0 = HashMap::new();
    props0.insert("name".to_string(), json!("Alice"));
    props0.insert("age".to_string(), json!(30));
    writer.insert_vertex(vid0, props0).await?;

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), json!("Bob"));
    props1.insert("age".to_string(), json!(20));
    writer.insert_vertex(vid1, props1).await?;

    writer.flush_to_l1(None).await?;

    // 3. Run Vectorized Query
    // Query that needs Filtering on one property and Returning another.
    let cypher = "MATCH (n:Person) WHERE n.age > 25 RETURN n.name";

    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    // The executor should pick the vectorized path because this plan only has Scan, Filter, Project.
    let executor = Executor::new(storage.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // 4. Verify Results
    // Should only have Alice
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("n.name"),
        Some(&Value::String("Alice".to_string()))
    );

    Ok(())
}
