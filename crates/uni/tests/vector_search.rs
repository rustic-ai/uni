// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder};
use arrow_array::{RecordBatch, UInt64Array};
use lance::dataset::WriteMode;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_vector_search() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _label_id = schema_manager.add_label("Item", false)?;
    // Add vector property: 2 dimensions
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(storage_str, schema_manager.clone());

    // 2. Insert Data
    // Item 1: [0.0, 0.0] (Target)
    // Item 2: [1.0, 1.0]
    // Item 3: [10.0, 10.0]

    let vertex_ds = storage.vertex_dataset("Item")?;
    let schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    let vids = UInt64Array::from(vec![1, 2, 3]);
    let versions = UInt64Array::from(vec![1, 1, 1]);
    let deleted = arrow_array::BooleanArray::from(vec![false, false, false]);

    // UIDs (dummy)
    let mut uid_builder = FixedSizeBinaryBuilder::new(32);
    let dummy_uid = vec![0u8; 32];
    for _ in 0..3 {
        uid_builder.append_value(&dummy_uid).unwrap();
    }
    let uids = uid_builder.finish();

    // Vectors: [[0,0], [1,1], [10,10]]
    let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);

    // Item 1: [0.0, 0.0]
    vector_builder.values().append_value(0.0);
    vector_builder.values().append_value(0.0);
    vector_builder.append(true);

    // Item 2: [1.0, 1.0]
    vector_builder.values().append_value(1.0);
    vector_builder.values().append_value(1.0);
    vector_builder.append(true);

    // Item 3: [10.0, 10.0]
    vector_builder.values().append_value(10.0);
    vector_builder.values().append_value(10.0);
    vector_builder.append(true);

    let vectors = vector_builder.finish();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(vids),
            Arc::new(uids),
            Arc::new(deleted),
            Arc::new(versions),
            Arc::new(vectors), // embedding
        ],
    )?;

    vertex_ds.write_batch(batch, WriteMode::Overwrite).await?;

    // 3. Search
    // Query: [0.1, 0.1]. Should match Item 1 best.
    let query = vec![0.1f32, 0.1f32];
    let results = storage
        .vector_search("Item", "embedding", &query, 2)
        .await?;

    // Expect 2 results
    assert_eq!(results.len(), 2);
    // Closest should be Item 1 (Vid 1)
    assert_eq!(results[0].0.local_offset(), 1);

    // Second closest should be Item 2 (Vid 2)
    assert_eq!(results[1].0.local_offset(), 2);

    println!("Vector Search Results: {:?}", results);

    Ok(())
}
