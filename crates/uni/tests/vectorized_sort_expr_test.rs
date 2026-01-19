// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use lance::dataset::WriteMode;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::Vid;
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::storage::manager::StorageManager;
use uni_common::config::UniConfig;

#[tokio::test]
async fn test_vectorized_sort_expression() -> anyhow::Result<()> {
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

    // Test: ORDER BY length(n.name) DESC
    // This requires evaluating a function call in the Sort operator, which is currently unsupported.
    let sql = "MATCH (n:Person) RETURN n.name ORDER BY length(n.name) DESC";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;

    // Execute directly with vectorized engine to bypass fallback
    let result = executor
        .execute_vectorized(
            plan,
            &prop_mgr,
            &std::collections::HashMap::new(),
            UniConfig::default(),
        )
        .await;

    let batch = result.expect("Vectorized execution should succeed");

    // Check results
    // Expected order (length(n.name) DESC):
    // "Charlie" (7)
    // "Alice" (5) - tie with David
    // "David" (5)
    // "Bob" (3)
    // Note: Ties are unstable in simple sort, but Alice/David order might vary.
    // Let's just check length order.

    let name_col_idx = *batch.variables.get("n.name").unwrap();
    let name_col = batch
        .data
        .column(name_col_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(batch.num_rows(), 4);
    assert_eq!(name_col.value(0), "Charlie");
    let second = name_col.value(1);
    let third = name_col.value(2);
    assert!(second == "Alice" || second == "David");
    assert!(third == "Alice" || third == "David");
    assert_eq!(name_col.value(3), "Bob");

    Ok(())
}
