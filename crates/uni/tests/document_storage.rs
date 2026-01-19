// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::Vid;
use uni::core::schema::{DataType, SchemaManager};
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_document_storage() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let label_id = schema_manager.add_label("Article", true)?; // is_document = true
    schema_manager.add_json_index("Article", "$.title", DataType::String)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(storage_str, schema_manager.clone());

    // 2. Insert Document
    let vid = Vid::new(label_id, 1);
    let doc = json!({
        "title": "Graph Databases",
        "author": "Alice"
    });

    storage.insert_document("Article", vid, doc).await?;

    // 3. Verify Index
    let index = storage.json_index("Article", "$.title")?;
    let vids = index.get_vids("Graph Databases").await?;

    assert_eq!(vids.len(), 1);
    assert_eq!(vids[0], vid);

    let vids_missing = index.get_vids("SQL").await?;
    assert!(vids_missing.is_empty());

    Ok(())
}
