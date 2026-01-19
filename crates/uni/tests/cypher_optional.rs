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
async fn test_cypher_optional_match() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
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

    // 2. Create data: Alice knows Bob. Charlie knows no one.
    let query = "CREATE (a:Person {name: 'Alice'}) CREATE (b:Person {name: 'Bob'}) CREATE (c:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(b)";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. OPTIONAL MATCH
    // Should return:
    // Alice -> Bob
    // Bob -> null
    // Charlie -> null
    let query =
        "MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS]->(friend:Person) RETURN p.name, friend.name";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // Check results
    let mut results: HashMap<String, Option<String>> = HashMap::new();
    for row in res {
        let p_name = row.get("p.name").unwrap().as_str().unwrap().to_string();
        let f_name = row
            .get("friend.name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        results.insert(p_name, f_name);
    }

    assert_eq!(results.get("Alice"), Some(&Some("Bob".to_string())));
    assert_eq!(results.get("Bob"), Some(&None));
    assert_eq!(results.get("Charlie"), Some(&None));

    Ok(())
}
