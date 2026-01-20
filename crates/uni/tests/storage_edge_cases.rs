// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use lance::dataset::WriteMode;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::storage::delta::Op;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_vertex_dataset_batch_writes() -> Result<()> {
    let dir = tempdir()?;
    let base_path = dir.path().to_str().unwrap();
    let schema_path = dir.path().join("schema.json");

    let schema_manager = SchemaManager::load(&schema_path).await?;
    let label_id = schema_manager.add_label("User", false)?;
    schema_manager.add_property("User", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(base_path, schema_manager.clone());
    let ds = storage.vertex_dataset("User")?;

    // Write a batch
    let vid1 = Vid::new(label_id, 1);
    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), serde_json::json!("Alice"));

    let schema = schema_manager.schema();
    let batch = ds.build_record_batch(&[(vid1, props1)], &[false], &[1], &schema)?;
    ds.write_batch(batch, WriteMode::Create).await?;

    let vid2 = Vid::new(label_id, 2);
    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), json!("Bob"));
    let batch2 = ds.build_record_batch(&[(vid2, props2)], &[false], &[1], &schema)?;
    ds.write_batch(batch2, WriteMode::Append).await?;

    // Verify count
    let lance_ds = ds.open().await?;
    assert_eq!(lance_ds.count_rows(None).await?, 2);

    Ok(())
}

#[tokio::test]
async fn test_delta_dataset_merging() -> Result<()> {
    let dir = tempdir()?;
    let base_path = dir.path().to_str().unwrap();
    let schema_path = dir.path().join("schema.json");

    let schema_manager = SchemaManager::load(&schema_path).await?;
    let tid = schema_manager.add_edge_type("KNOWS", vec!["User".into()], vec!["User".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(base_path, schema_manager.clone());
    let delta_ds = storage.delta_dataset("KNOWS", "fwd")?;

    // Write multiple runs
    let schema = schema_manager.schema();

    // Run 1: Insert E1 (ver 1)
    let entry1 = uni_db::storage::delta::L1Entry {
        src_vid: Vid::new(1, 1),
        dst_vid: Vid::new(1, 2),
        eid: uni_db::core::id::Eid::new(tid, 1),
        op: Op::Insert,
        version: 1,
        properties: HashMap::new(),
    };
    let batch1 = delta_ds.build_record_batch(std::slice::from_ref(&entry1), &schema)?;
    delta_ds.write_run(batch1).await?;

    // Run 2: Delete E1 (ver 2)
    let entry2 = uni_db::storage::delta::L1Entry {
        src_vid: Vid::new(1, 1),
        dst_vid: Vid::new(1, 2),
        eid: uni_db::core::id::Eid::new(tid, 1),
        op: Op::Delete,
        version: 2,
        properties: HashMap::new(),
    };
    let batch2 = delta_ds.build_record_batch(std::slice::from_ref(&entry2), &schema)?;
    delta_ds.write_run(batch2).await?;

    // Read deltas for src_vid
    let deltas = delta_ds.read_deltas(Vid::new(1, 1), &schema, None).await?;

    // Should get both? Or merged? read_deltas returns raw entries from runs.
    // Logic in manager merges them.
    assert_eq!(deltas.len(), 2);
    assert_eq!(deltas[0].version, 1);
    assert_eq!(deltas[1].version, 2);

    Ok(())
}
