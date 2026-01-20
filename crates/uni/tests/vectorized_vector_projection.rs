// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_vector_projection() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc", false)?;
    // vector property
    // We don't have explicit Vector type in DataType enum?
    // Usually it's just a list of floats, but stored specially if indexed.
    // Let's check DataType.
    // DataType::List(Box::new(DataType::Float))

    // Actually, for vector search we usually configure it via index config or just property.
    // Let's try creating a property that holds a vector.
    // In Uni, vectors are properties.
    schema_manager.add_property(
        "Doc",
        "embedding",
        DataType::List(Box::new(DataType::Float)),
        false,
    )?;

    schema_manager.save().await?;
    let schema = Arc::new(schema_manager.schema().clone());
    let schema_manager = Arc::new(schema_manager);

    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()));
    let writer = Arc::new(tokio::sync::RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let prop_manager = PropertyManager::new(storage.clone(), storage.schema_manager_arc(), 1024);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema);
    let params = HashMap::new();

    // 2. Create data
    let query = "CREATE (d:Doc {embedding: [0.1, 0.2, 0.3]})";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. Test Projection
    let query = "MATCH (d:Doc) RETURN d.embedding";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 1);
    let emb = res[0].get("d.embedding").unwrap();
    assert!(emb.is_array());
    let arr = emb.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_f64().unwrap(), 0.1);
    assert_eq!(arr[1].as_f64().unwrap(), 0.2);
    assert_eq!(arr[2].as_f64().unwrap(), 0.3);

    Ok(())
}
