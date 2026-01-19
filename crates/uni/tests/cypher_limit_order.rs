// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use lance::dataset::WriteMode;
use serde_json::Value;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::Vid;
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_limit_order() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema & Data
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

    // david, 40
    let vertex_ds = storage.vertex_dataset("Person")?;
    let schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(person_lbl, 0).as_u64(),
                Vid::new(person_lbl, 1).as_u64(),
                Vid::new(person_lbl, 2).as_u64(),
                Vid::new(person_lbl, 3).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 4].into(),
                None,
            )),
            Arc::new(BooleanArray::from(vec![false; 4])), // _deleted
            Arc::new(UInt64Array::from(vec![1; 4])),
            Arc::new(Int32Array::from(vec![25, 35, 20, 40])), // age
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"])), // name
        ],
    )?;
    vertex_ds.write_batch(batch, WriteMode::Overwrite).await?;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new(storage.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Test 1: LIMIT
    println!("--- Test 1: LIMIT ---");
    let sql = "MATCH (n:Person) RETURN n.name LIMIT 2";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);

    // Test 2: ORDER BY ASC
    println!("--- Test 2: ORDER BY ASC ---");
    let sql = "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age ASC";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 4);
    assert_eq!(
        results[0].get("n.name"),
        Some(&Value::String("Charlie".to_string()))
    ); // 20
    assert_eq!(
        results[1].get("n.name"),
        Some(&Value::String("Alice".to_string()))
    ); // 25
    assert_eq!(
        results[2].get("n.name"),
        Some(&Value::String("Bob".to_string()))
    ); // 35
    assert_eq!(
        results[3].get("n.name"),
        Some(&Value::String("David".to_string()))
    ); // 40

    // Test 3: ORDER BY DESC + LIMIT
    println!("--- Test 3: ORDER BY DESC + LIMIT ---");
    let sql = "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC LIMIT 2";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("n.name"),
        Some(&Value::String("David".to_string()))
    ); // 40
    assert_eq!(
        results[1].get("n.name"),
        Some(&Value::String("Bob".to_string()))
    ); // 35

    // Test 4: SKIP + LIMIT
    println!("--- Test 4: SKIP + LIMIT ---");
    // Ordered by age: Charlie(20), Alice(25), Bob(35), David(40)
    // SKIP 1 LIMIT 2 -> Alice, Bob
    let sql = "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC SKIP 1 LIMIT 2";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
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

    Ok(())
}
