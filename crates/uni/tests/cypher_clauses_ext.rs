// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::runtime::writer::Writer;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_unwind() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));
    let executor = Executor::new(storage.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let sql = "UNWIND [1, 2, 3] AS x RETURN x";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("x"), Some(&json!(1)));
    assert_eq!(results[1].get("x"), Some(&json!(2)));
    assert_eq!(results[2].get("x"), Some(&json!(3)));

    Ok(())
}

#[tokio::test]
async fn test_cypher_set_remove() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int32, true)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // 1. CREATE with properties
    let sql = "CREATE (n:Person {name: 'Alice', age: 30})";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    let vid;
    {
        let w = writer.read().await;
        let l0 = w.l0_manager.get_current();
        let l0 = l0.read();
        assert_eq!(l0.vertex_properties.len(), 1);
        vid = *l0.vertex_properties.keys().next().unwrap();
        let props = &l0.vertex_properties[&vid];
        assert_eq!(props.get("name"), Some(&json!("Alice")));
        assert_eq!(props.get("age"), Some(&json!(30)));
    }

    // 2. SET property
    // We need to flush so MATCH can find it.
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    let sql = "MATCH (n:Person) SET n.age = 31";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    {
        let w = writer.read().await;
        // L0 should have the update
        let l0 = w.l0_manager.get_current();
        let l0 = l0.read();
        let props = &l0.vertex_properties[&vid];
        assert_eq!(props.get("age"), Some(&json!(31)));
        assert_eq!(
            props.get("name"),
            Some(&json!("Alice")),
            "Name should be preserved"
        );
    }

    // 3. REMOVE property
    let sql = "MATCH (n:Person) REMOVE n.age";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    {
        let w = writer.read().await;
        let l0 = w.l0_manager.get_current();
        let l0 = l0.read();
        let props = &l0.vertex_properties[&vid];
        assert_eq!(props.get("age"), Some(&json!(null)));
    }

    Ok(())
}

#[tokio::test]
async fn test_cypher_with() -> anyhow::Result<()> {
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

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // Create a node
    {
        let mut w = writer.write().await;
        let vid = w.next_vid(person_lbl).await?;
        let mut props = HashMap::new();
        props.insert("name".to_string(), json!("Alice"));
        w.insert_vertex(vid, props).await?;
        w.flush_to_l1(None).await?;
    }

    // Query: MATCH (n:Person) WITH n RETURN n.name
    let sql = "MATCH (n:Person) WITH n RETURN n.name";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("n.name"), Some(&json!("Alice")));

    Ok(())
}
