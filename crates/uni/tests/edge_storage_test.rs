// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{BooleanArray, RecordBatch, UInt64Array};
use lance::dataset::WriteMode;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::schema::SchemaManager;
use uni_db::storage::edge::EdgeDataset;

#[tokio::test]
async fn test_edge_storage() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person", false)?;
    schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    // 2. Create EdgeDataset
    let edge_ds = EdgeDataset::new(storage_str, "knows", "Person", "Person");
    let schema = edge_ds.get_arrow_schema(&schema_manager.schema())?;

    // 3. Write Data
    let eids = UInt64Array::from(vec![1, 2]);
    let src_vids = UInt64Array::from(vec![10, 11]);
    let dst_vids = UInt64Array::from(vec![20, 21]);
    let deleted = BooleanArray::from(vec![false, false]);
    let versions = UInt64Array::from(vec![1, 1]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(eids),
            Arc::new(src_vids),
            Arc::new(dst_vids),
            Arc::new(deleted),
            Arc::new(versions),
        ],
    )?;

    edge_ds.write_batch(batch, WriteMode::Overwrite).await?;

    // 4. Open and Verify
    let ds = edge_ds.open().await?;
    assert_eq!(ds.count_rows(None).await?, 2);

    Ok(())
}
