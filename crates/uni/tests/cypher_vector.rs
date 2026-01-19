// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder};
use arrow_array::{BooleanArray, RecordBatch, UInt64Array};
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

#[tokio::test]
async fn test_cypher_vector_search() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let storage_str = path.join("storage").to_str().unwrap().to_string();

    // 1. Setup Data
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let label_id = schema_manager.add_label("Item", false)?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(&storage_str, schema_manager.clone()));

    let ds = storage.vertex_dataset("Item")?;
    let schema = ds.get_arrow_schema(&schema_manager.schema())?;

    // Items
    let vids = UInt64Array::from(vec![
        Vid::new(label_id, 1).as_u64(),
        Vid::new(label_id, 2).as_u64(),
    ]);

    let mut uid_builder = FixedSizeBinaryBuilder::new(32);
    let dummy_uid = vec![0u8; 32];
    uid_builder.append_value(&dummy_uid).unwrap();
    uid_builder.append_value(&dummy_uid).unwrap();

    let mut vec_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    vec_builder.values().append_value(0.0);
    vec_builder.values().append_value(0.0);
    vec_builder.append(true);
    vec_builder.values().append_value(1.0);
    vec_builder.values().append_value(1.0);
    vec_builder.append(true);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(vids),
            Arc::new(uid_builder.finish()),
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(UInt64Array::from(vec![1, 1])),
            Arc::new(vec_builder.finish()),
        ],
    )?;
    ds.write_batch(batch, WriteMode::Overwrite).await?;

    // 2. Query
    let sql = "CALL db.idx.vector.query('Item', 'embedding', [0.1, 0.1], 2) YIELD node, distance RETURN node, distance";

    let mut parser = CypherParser::new(sql)?;
    let query_ast = parser.parse()?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new(storage.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &std::collections::HashMap::new())
        .await?;
    println!("Results: {:?}", results);

    assert_eq!(results.len(), 2);
    // Closest should be Item 1 (0,0)
    assert!(
        results[0]
            .get("node")
            .unwrap()
            .as_str()
            .unwrap()
            .contains(":1")
    );
    assert!(results[0].get("distance").unwrap().as_f64().unwrap() < 0.1);

    Ok(())
}
