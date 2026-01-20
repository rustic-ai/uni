// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_backup_and_restore() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let storage_path = path.join("storage");
    let backup_path = path.join("backup");

    // 1. Setup Data
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let label_id = schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        storage_path.to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Use Writer to ensure snapshot is created on flush
    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    // Insert data directly via Writer to simulate real usage
    {
        let mut w = writer.write().await;
        let vid = Vid::new(label_id, 1);
        let mut props = std::collections::HashMap::new();
        props.insert("name".to_string(), serde_json::json!("Alice"));
        w.insert_vertex(vid, props).await?;
        w.flush_to_l1(None).await?; // This creates a snapshot
    }

    // 2. Execute BACKUP
    let query = format!("BACKUP TO '{}'", backup_path.to_str().unwrap());

    let mut parser = CypherParser::new(&query)?;
    let ast = parser.parse()?;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(ast)?;
    let mut executor = Executor::new(storage.clone());
    executor.set_writer(writer.clone());
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let result = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].get("status").unwrap().as_str().unwrap(),
        "Backup completed"
    );

    // 3. Verify Backup
    assert!(backup_path.exists());
    assert!(backup_path.join("catalog").exists());
    assert!(backup_path.join("vertices").exists());

    // 4. Restore (Open new Uni from backup)
    let restore_schema_manager =
        SchemaManager::load(&backup_path.join("catalog/schema.json")).await?;
    let restore_schema_manager = Arc::new(restore_schema_manager);
    let restore_storage = Arc::new(StorageManager::new(
        backup_path.to_str().unwrap(),
        restore_schema_manager.clone(),
    ));

    // Check data
    let ds = restore_storage.vertex_dataset("Person")?;
    let ds = ds.open().await?;
    assert!(ds.count_rows(None).await? > 0);

    Ok(())
}
