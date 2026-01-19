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
async fn test_null_handling_functions() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;

    // Add properties, including nullable ones
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int64, true)?; // Nullable
    schema_manager.add_property("Person", "nickname", DataType::String, true)?; // Nullable

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
    // Person 1: Alice, age 30, no nickname
    let vid1 = Vid::new(person_lbl, 1);
    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), json!("Alice"));
    props1.insert("age".to_string(), json!(30));
    writer.insert_vertex(vid1, props1).await?;

    // Person 2: Bob, no age, nickname 'Bobby'
    let vid2 = Vid::new(person_lbl, 2);
    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), json!("Bob"));
    props2.insert("nickname".to_string(), json!("Bobby"));
    writer.insert_vertex(vid2, props2).await?;

    // Person 3: Charlie, no age, no nickname
    let vid3 = Vid::new(person_lbl, 3);
    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), json!("Charlie"));
    writer.insert_vertex(vid3, props3).await?;

    writer.flush_to_l1(None).await?;

    // 3. Test COALESCE
    // MATCH (n:Person) RETURN n.name, coalesce(n.nickname, n.age, 'Unknown') ORDER BY n.name
    let cypher_coalesce =
        "MATCH (n:Person) RETURN n.name, coalesce(n.nickname, n.age, 'Unknown') ORDER BY n.name";

    let mut parser = CypherParser::new(cypher_coalesce)?;
    let query_ast = parser.parse()?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new(storage.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    let coalesce_key = "COALESCE(n.nickname, n.age, String(\"Unknown\"))";

    // Expected results sorted by name: Alice, Bob, Charlie
    // Alice: nickname=null, age=30 -> 30 (may be string "30" in vectorized engine due to mixed type promotion)
    assert_eq!(results[0].get("n.name"), Some(&json!("Alice")));
    let alice_coalesce = results[0].get(coalesce_key).unwrap();
    assert!(
        alice_coalesce == &json!(30) || alice_coalesce == &json!("30"),
        "Alice coalesce mismatch: {:?}",
        alice_coalesce
    );

    // Bob: nickname='Bobby', age=null -> 'Bobby'
    assert_eq!(results[1].get("n.name"), Some(&json!("Bob")));
    assert_eq!(results[1].get(coalesce_key), Some(&json!("Bobby")));

    // Charlie: nickname=null, age=null -> 'Unknown'
    assert_eq!(results[2].get("n.name"), Some(&json!("Charlie")));
    assert_eq!(results[2].get(coalesce_key), Some(&json!("Unknown")));

    // 4. Test nullIf
    // MATCH (n:Person) RETURN n.name, nullIf(n.name, 'Bob') ORDER BY n.name
    let cypher_nullif = "MATCH (n:Person) RETURN n.name, nullIf(n.name, 'Bob') ORDER BY n.name";

    let mut parser = CypherParser::new(cypher_nullif)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    let nullif_key = "NULLIF(n.name, String(\"Bob\"))";

    // Alice: 'Alice' != 'Bob' -> 'Alice'
    assert_eq!(results[0].get("n.name"), Some(&json!("Alice")));
    assert_eq!(results[0].get(nullif_key), Some(&json!("Alice")));

    // Bob: 'Bob' == 'Bob' -> null
    assert_eq!(results[1].get("n.name"), Some(&json!("Bob")));
    assert_eq!(results[1].get(nullif_key), Some(&Value::Null));

    // Charlie: 'Charlie' != 'Bob' -> 'Charlie'
    assert_eq!(results[2].get("n.name"), Some(&json!("Charlie")));
    assert_eq!(results[2].get(nullif_key), Some(&json!("Charlie")));

    Ok(())
}
