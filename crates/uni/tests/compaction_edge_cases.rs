// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{BooleanArray, FixedSizeBinaryArray, RecordBatch, StringArray, UInt64Array};
use lance::dataset::WriteMode;
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::Vid;
use uni::core::schema::{DataType, SchemaManager};
use uni::runtime::property_manager::PropertyManager;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_compact_vertices_with_null_props() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let lbl = schema_manager.add_label("Node", false)?;
    schema_manager.add_property("Node", "name", DataType::String, true)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Insert data directly to storage to bypass L0 and force L1 writes
    let ds = storage.vertex_dataset("Node")?;
    let arrow_schema = ds.get_arrow_schema(&schema_manager.schema())?;

    // Batch 1: VID 1, name="A"
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(lbl, 1).as_u64()])),
            Arc::new(FixedSizeBinaryArray::new(32, vec![0u8; 32].into(), None)),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("A")])),
        ],
    )?;
    ds.write_batch(batch1, WriteMode::Append).await?;

    // Batch 2: VID 2, name=NULL
    let batch2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(lbl, 2).as_u64()])),
            Arc::new(FixedSizeBinaryArray::new(32, vec![0u8; 32].into(), None)),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(UInt64Array::from(vec![2])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )?;
    ds.write_batch(batch2, WriteMode::Append).await?;

    // Compact
    let stats = storage.compact_label("Node").await?;
    assert!(stats.files_compacted >= 1);

    // Verify
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 10);
    let val1 = prop_mgr.get_vertex_prop(Vid::new(lbl, 1), "name").await?;
    assert_eq!(val1, json!("A"));

    let val2 = prop_mgr.get_vertex_prop(Vid::new(lbl, 2), "name").await?;
    assert!(val2.is_null());

    Ok(())
}

#[tokio::test]
async fn test_compact_empty_dataset() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _lbl = schema_manager.add_label("Empty", false)?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        Arc::new(schema_manager),
    ));

    // Compact empty
    let stats = storage.compact_label("Empty").await?;
    assert_eq!(stats.files_compacted, 0); // Nothing to compact or dataset created but empty

    Ok(())
}
