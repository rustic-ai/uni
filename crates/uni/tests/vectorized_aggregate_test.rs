// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{Int32Array, RecordBatch, UInt64Array};
use lance::dataset::WriteMode;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::config::UniConfig;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_aggregate() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema & Data
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;

    schema_manager.add_property("Person", "age", DataType::Int32, false)?;
    schema_manager.add_property("Person", "group", DataType::String, false)?; // "A" or "B"
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let vertex_ds = storage.vertex_dataset("Person")?;
    let schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    // Create 4 rows: A:10, A:20, B:30, B:40
    let vids: Vec<u64> = (0..4).map(|i| Vid::new(person_lbl, i).as_u64()).collect();
    let ages: Vec<i32> = vec![10, 20, 30, 40];
    let groups = arrow_array::StringArray::from(vec!["A", "A", "B", "B"]);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vids)),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 4].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false; 4])), // _deleted
            Arc::new(UInt64Array::from(vec![1; 4])),
            Arc::new(Int32Array::from(ages)),
            Arc::new(groups),
        ],
    )?;
    vertex_ds.write_batch(batch, WriteMode::Overwrite).await?;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new(storage.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Test: MATCH (n:Person) RETURN n.group, count(n), sum(n.age)
    let sql = "MATCH (n:Person) RETURN n.group, count(n), sum(n.age)";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;

    let result = executor
        .execute_vectorized(
            plan,
            &prop_mgr,
            &std::collections::HashMap::new(),
            UniConfig::default(),
        )
        .await
        .expect("Vectorized Aggregate should succeed");

    assert_eq!(result.num_rows(), 2);
    // Note: order is not guaranteed for hash agg
    // We should find two rows.

    // Check for Group A
    // sum(age) = 10+20 = 30
    // count = 2

    // Check for Group B
    // sum(age) = 30+40 = 70
    // count = 2

    // Basic structural check
    assert!(result.variables.contains_key("n.group"));
    assert!(result.variables.contains_key("COUNT(n)"));
    assert!(result.variables.contains_key("SUM(n.age)"));

    Ok(())
}
