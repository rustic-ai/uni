// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::runtime::writer::Writer;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_optional_scan() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    // Add "City" label but don't add any data for it (or add data and filter it out)
    schema_manager.add_label("City", false)?;
    schema_manager.add_property("City", "name", DataType::String, false)?;

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

    // 2. Create data: Alice and Bob
    let query = "CREATE (a:Person {name: 'Alice'}) CREATE (b:Person {name: 'Bob'})";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. OPTIONAL MATCH with Scan on empty label (City)
    // This should perform a CrossJoin between Person (2 rows) and City (0 rows -> 1 null row)
    // Result should be 2 rows: (Alice, null), (Bob, null)
    let query = "MATCH (p:Person) OPTIONAL MATCH (c:City) RETURN p.name, c.name ORDER BY p.name";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;

    // Verify plan is using VectorizedScan for optional part
    println!("Plan: {:?}", plan);

    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 2);

    // Check Row 1: Alice
    assert_eq!(res[0].get("p.name").unwrap().as_str().unwrap(), "Alice");
    assert!(res[0].get("c.name").unwrap().is_null());

    // Check Row 2: Bob
    assert_eq!(res[1].get("p.name").unwrap().as_str().unwrap(), "Bob");
    assert!(res[1].get("c.name").unwrap().is_null());

    Ok(())
}

#[tokio::test]
async fn test_vectorized_optional_scan_with_filter() -> anyhow::Result<()> {
    // Test case where Scan finds rows but Filter removes them
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_label("City", false)?;
    schema_manager.add_property("City", "name", DataType::String, false)?;

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

    // 2. Create data: Alice, and City 'New York'
    let query = "CREATE (a:Person {name: 'Alice'}) CREATE (c:City {name: 'New York'})";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. OPTIONAL MATCH with Filter that removes the match
    // MATCH (p:Person) OPTIONAL MATCH (c:City) WHERE c.name = 'London'
    // City 'New York' exists but doesn't match filter.
    // Result should be (Alice, null)
    let query =
        "MATCH (p:Person) OPTIONAL MATCH (c:City) WHERE c.name = 'London' RETURN p.name, c.name";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;

    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("p.name").unwrap().as_str().unwrap(), "Alice");
    assert!(res[0].get("c.name").unwrap().is_null());

    Ok(())
}
