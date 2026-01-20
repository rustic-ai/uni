// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::BooleanArray;
use futures::TryStreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_delete_vertex_persistence() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    // 2. Insert Vertex
    let vid = writer.next_vid(person_lbl).await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), json!("Alice"));
    writer.insert_vertex(vid, props).await?;
    writer.flush_to_l1(None).await?;

    // 2. Delete vertex
    writer.delete_vertex(vid).await?;
    writer.flush_to_l1(None).await?;

    // 3. Verify in storage
    let ds = storage.vertex_dataset("Person")?.open().await?;
    let mut stream = ds.scan().try_into_stream().await?;

    let mut found_deleted = false;
    while let Some(batch) = stream.try_next().await? {
        let vid_col = batch
            .column_by_name("_vid")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        let deleted_col = batch
            .column_by_name("_deleted")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if vid_col.value(i) == vid.as_u64() && deleted_col.value(i) {
                found_deleted = true;
            }
        }
    }

    assert!(
        found_deleted,
        "Vertex should be marked as deleted in storage"
    );

    Ok(())
}
