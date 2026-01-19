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
async fn test_property_lookup_uses_vid_filter() -> anyhow::Result<()> {
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

    let vertex_ds = storage.vertex_dataset("Person")?;
    let schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    // Write rows out of VID order to ensure row index != local_offset.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(person_lbl, 1).as_u64(),
                Vid::new(person_lbl, 0).as_u64(),
            ])),
            Arc::new(FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 2].into(),
                None,
            )),
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(UInt64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec!["Bob", "Alice"])),
        ],
    )?;
    vertex_ds.write_batch(batch, WriteMode::Overwrite).await?;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 10);
    let alice_vid = Vid::new(person_lbl, 0);
    let bob_vid = Vid::new(person_lbl, 1);

    let alice_name = prop_mgr.get_vertex_prop(alice_vid, "name").await?;
    assert_eq!(alice_name, json!("Alice"));

    let bob_props = prop_mgr.get_all_vertex_props(bob_vid).await?;
    assert_eq!(bob_props.get("name"), Some(&json!("Bob")));

    Ok(())
}

#[tokio::test]
async fn test_property_lookup_not_found() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = Arc::new(SchemaManager::load(&path.join("schema.json")).await?);
    let lbl = schema_manager.add_label("Node", false)?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let prop_mgr = PropertyManager::new(storage, schema_manager, 10);
    let vid = Vid::new(lbl, 999); // Non-existent

    let val = prop_mgr.get_vertex_prop(vid, "name").await?;
    assert!(val.is_null());

    let props = prop_mgr.get_all_vertex_props(vid).await?;
    assert!(props.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_list_property_storage() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let lbl = schema_manager.add_label("Node", false)?;
    // List<String>
    schema_manager.add_property(
        "Node",
        "tags",
        DataType::List(Box::new(DataType::String)),
        false,
    )?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let vertex_ds = storage.vertex_dataset("Node")?;
    let arrow_schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    // Create List array
    let tags_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    let mut tags_builder = tags_builder;
    tags_builder.values().append_value("a");
    tags_builder.values().append_value("b");
    tags_builder.append(true);
    let tags_arr = Arc::new(tags_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(lbl, 0).as_u64()])),
            Arc::new(FixedSizeBinaryArray::new(32, vec![0u8; 32].into(), None)),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(UInt64Array::from(vec![1])),
            tags_arr,
        ],
    )?;
    vertex_ds.write_batch(batch, WriteMode::Overwrite).await?;

    let prop_mgr = PropertyManager::new(storage, schema_manager, 10);
    let vid = Vid::new(lbl, 0);

    let val = prop_mgr.get_vertex_prop(vid, "tags").await?;
    assert_eq!(val, json!(["a", "b"]));

    Ok(())
}
