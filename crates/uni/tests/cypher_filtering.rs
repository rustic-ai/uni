// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use lance::dataset::WriteMode;
use serde_json::Value;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_filtering() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema & Data
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;

    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int32, false)?;
    schema_manager.add_property("Person", "active", DataType::Bool, false)?;

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Insert Persons:
    // 0: Alice, 25, true
    // 1: Bob, 35, false
    // 2: Charlie, 40, true
    let vertex_ds = storage.vertex_dataset("Person")?;
    let schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(person_lbl, 0).as_u64(),
                Vid::new(person_lbl, 1).as_u64(),
                Vid::new(person_lbl, 2).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 3].into(),
                None,
            )),
            Arc::new(BooleanArray::from(vec![false, false, false])), // _deleted
            Arc::new(UInt64Array::from(vec![1, 1, 1])),
            Arc::new(BooleanArray::from(vec![true, false, true])), // active
            Arc::new(Int32Array::from(vec![25, 35, 40])),          // age
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])), // name
        ],
    )?;
    vertex_ds.write_batch(batch, WriteMode::Overwrite).await?;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new(storage.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Test 1: Equality (Name = 'Alice')
    println!("--- Test 1: Equality ---");
    let sql = "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("n.name"),
        Some(&Value::String("Alice".to_string()))
    );

    // Test 2: Range (Age > 30) -> Bob (35), Charlie (40)
    println!("--- Test 2: Range ---");
    let sql = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);
    let names: Vec<&str> = results
        .iter()
        .map(|r| r.get("n.name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));

    // Test 3: Boolean Logic (Age > 20 AND Active = true) -> Alice (25, T), Charlie (40, T)
    println!("--- Test 3: Boolean Logic ---");
    let sql = "MATCH (n:Person) WHERE n.age > 20 AND n.active = true RETURN n.name";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);
    let names: Vec<&str> = results
        .iter()
        .map(|r| r.get("n.name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Charlie"));

    Ok(())
}
