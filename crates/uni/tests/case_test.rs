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
async fn test_case_expression() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int64, true)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        UniConfig::default(),
        None,
        None,
        None,
    )
    .await
    .unwrap();

    // Alice: 10
    let vid1 = Vid::new(person_lbl, 1);
    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), json!("Alice"));
    props1.insert("age".to_string(), json!(10));
    writer.insert_vertex(vid1, props1).await?;

    // Bob: 20
    let vid2 = Vid::new(person_lbl, 2);
    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), json!("Bob"));
    props2.insert("age".to_string(), json!(20));
    writer.insert_vertex(vid2, props2).await?;

    // Charlie: 30
    let vid3 = Vid::new(person_lbl, 3);
    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), json!("Charlie"));
    props3.insert("age".to_string(), json!(30));
    writer.insert_vertex(vid3, props3).await?;

    writer.flush_to_l1(None).await?;

    let executor = Executor::new(storage.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // 1. Generic CASE
    // MATCH (n:Person) RETURN n.name, CASE WHEN n.age < 15 THEN 'Child' WHEN n.age < 25 THEN 'Teen' ELSE 'Adult' END ORDER BY n.name
    let cypher1 = "MATCH (n:Person) RETURN n.name, CASE WHEN n.age < 15 THEN 'Child' WHEN n.age < 25 THEN 'Teen' ELSE 'Adult' END ORDER BY n.name";

    let mut parser = CypherParser::new(cypher1)?;
    let query_ast = parser.parse()?;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results[0].get("n.name"), Some(&json!("Alice"))); // Child
    let val1 = results[0]
        .values()
        .find(|v| v.as_str() == Some("Child"))
        .cloned();
    assert_eq!(val1, Some(json!("Child")));

    assert_eq!(results[1].get("n.name"), Some(&json!("Bob"))); // Teen
    let val2 = results[1]
        .values()
        .find(|v| v.as_str() == Some("Teen"))
        .cloned();
    assert_eq!(val2, Some(json!("Teen")));

    assert_eq!(results[2].get("n.name"), Some(&json!("Charlie"))); // Adult
    let val3 = results[2]
        .values()
        .find(|v| v.as_str() == Some("Adult"))
        .cloned();
    assert_eq!(val3, Some(json!("Adult")));

    // 2. Simple CASE
    // MATCH (n:Person) RETURN n.name, CASE n.name WHEN 'Alice' THEN 1 WHEN 'Bob' THEN 2 ELSE 3 END ORDER BY n.name
    let cypher2 = "MATCH (n:Person) RETURN n.name, CASE n.name WHEN 'Alice' THEN 1 WHEN 'Bob' THEN 2 ELSE 3 END ORDER BY n.name";
    let mut parser = CypherParser::new(cypher2)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // Alice -> 1
    assert_eq!(results[0].get("n.name"), Some(&json!("Alice")));
    let val1 = results[0].values().find(|v| v.as_u64() == Some(1)).cloned();
    assert_eq!(val1, Some(json!(1)));

    // Bob -> 2
    assert_eq!(results[1].get("n.name"), Some(&json!("Bob")));
    let val2 = results[1].values().find(|v| v.as_u64() == Some(2)).cloned();
    assert_eq!(val2, Some(json!(2)));

    // Charlie -> 3
    assert_eq!(results[2].get("n.name"), Some(&json!("Charlie")));
    let val3 = results[2].values().find(|v| v.as_u64() == Some(3)).cloned();
    assert_eq!(val3, Some(json!(3)));

    Ok(())
}
