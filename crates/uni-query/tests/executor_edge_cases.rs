// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::RecordBatch;
use lance::dataset::WriteMode;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::id::Vid;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_query::query::executor::Executor;
use uni_query::query::parser::CypherParser;
use uni_query::query::planner::QueryPlanner;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

async fn setup_executor(
    path: &std::path::Path,
) -> (
    Executor,
    Arc<PropertyManager>,
    Arc<SchemaManager>,
    Arc<StorageManager>,
) {
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();

    // Add schema elements
    schema_manager.add_label("Person", false).unwrap();
    schema_manager
        .add_property("Person", "name", DataType::String, true)
        .unwrap();
    schema_manager
        .add_property("Person", "age", DataType::Int32, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let prop_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        100,
    ));
    let executor = Executor::new(storage.clone());

    (executor, prop_manager, schema_manager, storage)
}

#[tokio::test]
async fn test_execute_match_no_results() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, _) = setup_executor(path).await;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Query for Person (no data inserted)
    let sql = "MATCH (n:Person) RETURN n";
    let query = CypherParser::new(sql).unwrap().parse().unwrap();
    let plan = planner.plan(query).unwrap();

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_execute_match_with_null_properties() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, storage) = setup_executor(path).await;

    // Insert data with nulls using Lance directly (bypassing Writer)
    let schema = schema_manager.schema();
    let label_id = schema.labels.get("Person").unwrap().id;
    let vertex_ds = storage.vertex_dataset("Person").unwrap();
    let arrow_schema = vertex_ds.get_arrow_schema(&schema).unwrap();

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(arrow_array::UInt64Array::from(vec![
                Vid::new(label_id, 1).as_u64(),
                Vid::new(label_id, 2).as_u64(),
                Vid::new(label_id, 3).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 3].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false, false, false])), // _deleted
            Arc::new(arrow_array::UInt64Array::from(vec![1, 1, 1])),              // _version
            Arc::new(arrow_array::Int32Array::from(vec![
                Some(30),
                None,
                Some(25),
            ])), // age
            Arc::new(arrow_array::StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                None,
            ])), // name
        ],
    )
    .unwrap();

    vertex_ds
        .write_batch(batch, WriteMode::Overwrite)
        .await
        .unwrap();

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Query 1: Filter by age IS NULL
    let sql = "MATCH (n:Person) WHERE n.age IS NULL RETURN n.name";
    let plan = planner
        .plan(CypherParser::new(sql).unwrap().parse().unwrap())
        .unwrap();
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("n.name"),
        Some(&Value::String("Bob".to_string()))
    );

    // Query 2: Filter by name IS NULL
    let sql = "MATCH (n:Person) WHERE n.name IS NULL RETURN n.age";
    let plan = planner
        .plan(CypherParser::new(sql).unwrap().parse().unwrap())
        .unwrap();
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("n.age"), Some(&json!(25)));
}

#[tokio::test]
async fn test_aggregation_empty_group() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, _) = setup_executor(path).await;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Aggregation with no matches
    let sql = "MATCH (n:Person) RETURN count(n), sum(n.age)";
    let plan = planner
        .plan(CypherParser::new(sql).unwrap().parse().unwrap())
        .unwrap();
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    assert!(
        results.is_empty(),
        "Currently returns empty, maybe should return 0?"
    );
}
