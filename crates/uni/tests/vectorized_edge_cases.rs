// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::RecordBatch;
use lance::dataset::WriteMode;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::storage::manager::StorageManager;
use uni_common::config::UniConfig;

#[tokio::test]
async fn test_vectorized_scan_empty_label() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();
    schema_manager.add_label("Empty", false).unwrap();
    schema_manager.save().await.unwrap();
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));
    let _prop_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        100,
    ));
    let executor = Executor::new(storage);

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Scan empty label
    let sql = "MATCH (n:Empty) RETURN n";
    let plan = planner
        .plan(CypherParser::new(sql).unwrap().parse().unwrap())
        .unwrap();

    let batch = executor
        .execute_vectorized(
            plan,
            &_prop_manager,
            &std::collections::HashMap::new(),
            UniConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn test_vectorized_filter_all_filtered() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();
    let lbl = schema_manager.add_label("Node", false).unwrap();
    schema_manager
        .add_property("Node", "age", DataType::Int64, false)
        .unwrap();
    schema_manager.save().await.unwrap();
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Insert data: age 10
    let ds = storage.vertex_dataset("Node").unwrap();
    let schema = ds.get_arrow_schema(&schema_manager.schema()).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::UInt64Array::from(vec![
                uni::core::id::Vid::new(lbl, 0).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false])),
            Arc::new(arrow_array::UInt64Array::from(vec![1])),
            Arc::new(arrow_array::Int64Array::from(vec![10])),
        ],
    )
    .unwrap();
    ds.write_batch(batch, WriteMode::Overwrite).await.unwrap();

    let prop_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        100,
    ));
    let executor = Executor::new(storage);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Filter age > 20 (should return empty)
    let sql = "MATCH (n:Node) WHERE n.age > 20 RETURN n";
    let plan = planner
        .plan(CypherParser::new(sql).unwrap().parse().unwrap())
        .unwrap();

    let batch = executor
        .execute_vectorized(
            plan,
            &prop_manager,
            &std::collections::HashMap::new(),
            UniConfig::default(),
        )
        .await
        .unwrap();

    // Vectorized engine might return batch with 0 rows, or batch with selection vector all false.
    // .num_rows() accounts for selection?
    // In current implementation `VectorizedBatch` num_rows() is length of arrays.
    // Selection is separate.
    // If we compact, we see 0.

    let compacted = batch.compact().unwrap();
    assert_eq!(compacted.num_rows(), 0);
}

#[tokio::test]
async fn test_vectorized_project_null_handling() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();
    let lbl = schema_manager.add_label("Node", false).unwrap();
    schema_manager
        .add_property("Node", "name", DataType::String, true)
        .unwrap();
    schema_manager.save().await.unwrap();
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Insert: name = NULL
    let ds = storage.vertex_dataset("Node").unwrap();
    let schema = ds.get_arrow_schema(&schema_manager.schema()).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::UInt64Array::from(vec![
                uni::core::id::Vid::new(lbl, 0).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false])),
            Arc::new(arrow_array::UInt64Array::from(vec![1])),
            Arc::new(arrow_array::StringArray::from(vec![None::<&str>])),
        ],
    )
    .unwrap();
    ds.write_batch(batch, WriteMode::Overwrite).await.unwrap();

    let prop_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        100,
    ));
    let executor = Executor::new(storage);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Project coalesce(name, 'Unknown')
    let sql = "MATCH (n:Node) RETURN coalesce(n.name, 'Unknown')";
    let plan = planner
        .plan(CypherParser::new(sql).unwrap().parse().unwrap())
        .unwrap();

    let batch = executor
        .execute_vectorized(
            plan,
            &prop_manager,
            &std::collections::HashMap::new(),
            UniConfig::default(),
        )
        .await
        .unwrap();

    // Verify result
    assert_eq!(batch.num_rows(), 1);

    // Find the column by inspecting keys
    let keys: Vec<_> = batch.variables.keys().collect();
    // println!("Keys: {:?}", keys);

    let col_name = keys
        .iter()
        .find(|k| k.to_lowercase().contains("coalesce"))
        .expect("Result column not found");

    let col_idx = *batch.variables.get(*col_name).unwrap();

    let col = batch.data.column(col_idx);
    let str_col = col
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(str_col.value(0), "Unknown");
}
