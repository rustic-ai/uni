// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::{Eid, Vid};
use uni::core::schema::{DataType, SchemaManager};
use uni::runtime::writer::Writer;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_writer_flush() -> anyhow::Result<()> {
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

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()));

    // 2. Initialize Writer
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    // 3. Insert Edge
    let vid_a = Vid::new(1, 1);
    let vid_b = Vid::new(1, 2);
    let eid = Eid::new(1, 100);
    writer
        .insert_edge(vid_a, vid_b, 1, eid, HashMap::new())
        .await?;

    // 4. Flush to L1
    writer.flush_to_l1(None).await?;

    // 5. Verify L1 Delta Dataset
    let delta_ds = storage.delta_dataset("knows", "fwd")?;
    let schema = schema_manager.schema();
    let entries = delta_ds.read_deltas(vid_a, &schema, None).await?;

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].src_vid, vid_a);
    assert_eq!(entries[0].dst_vid, vid_b);
    assert_eq!(entries[0].eid, eid);

    Ok(())
}

#[tokio::test]
async fn test_writer_vertex_flush() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()));

    // 2. Initialize Writer
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    // 3. Insert Vertex
    let vid = Vid::new(1, 10);
    let mut props = HashMap::new();
    props.insert("name".to_string(), json!("Alice"));
    writer.insert_vertex(vid, props).await?;

    // 4. Flush to L1
    writer.flush_to_l1(None).await?;

    // 5. Verify Vertex Dataset
    let ds = storage.vertex_dataset("Person")?;
    let lance_ds = ds.open().await?;
    let count = lance_ds.count_rows(None).await?;
    assert_eq!(count, 1);

    Ok(())
}
