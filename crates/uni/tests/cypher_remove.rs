// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

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
async fn test_cypher_remove() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("User", false)?;
    schema_manager.add_property("User", "name", DataType::String, false)?;
    schema_manager.add_property("User", "age", DataType::Int64, true)?;
    schema_manager.add_edge_type(
        "FOLLOWS",
        vec!["User".to_string()],
        vec!["User".to_string()],
    )?;
    schema_manager.add_property("FOLLOWS", "since", DataType::Int64, true)?;
    schema_manager.save().await?;
    let schema = Arc::new(schema_manager.schema().clone());
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

    let prop_manager = PropertyManager::new(storage.clone(), storage.schema_manager_arc(), 1024);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema);
    let params = HashMap::new();

    // 2. Create data
    let query = "CREATE (u1:User {name: 'Alice', age: 30}) CREATE (u2:User {name: 'Bob'}) CREATE (u1)-[r:FOLLOWS {since: 2020}]->(u2)";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. Remove property from Vertex
    let query = "MATCH (u:User {name: 'Alice'}) REMOVE u.age RETURN u.age";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("u.age"), Some(&serde_json::Value::Null));

    // Verify persistence (fetch again)
    let query = "MATCH (u:User {name: 'Alice'}) RETURN u.age";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("u.age"), Some(&serde_json::Value::Null));

    // 4. Remove property from Relationship
    let query = "MATCH (:User {name: 'Alice'})-[r:FOLLOWS]->(:User {name: 'Bob'}) REMOVE r.since RETURN r.since";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("r.since"), Some(&serde_json::Value::Null));

    // Verify persistence
    let query = "MATCH (:User {name: 'Alice'})-[r:FOLLOWS]->(:User {name: 'Bob'}) RETURN r.since";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("r.since"), Some(&serde_json::Value::Null));

    // 5. Try removing primary label (should fail)
    let query = "MATCH (u:User {name: 'Bob'}) REMOVE u:User";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Removing the primary label is not supported")
    );

    Ok(())
}
