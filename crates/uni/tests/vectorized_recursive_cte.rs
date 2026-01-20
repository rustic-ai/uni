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
async fn test_vectorized_recursive_cte() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Node", false)?;
    schema_manager.add_property("Node", "id", DataType::Int64, false)?;
    schema_manager.add_edge_type("CHILD", vec!["Node".to_string()], vec!["Node".to_string()])?;
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

    // 2. Create data: 0 -> 1 -> 2
    let query = "
        CREATE (n0:Node {id: 0})
        CREATE (n1:Node {id: 1})
        CREATE (n2:Node {id: 2})
        CREATE (n0)-[:CHILD]->(n1)
        CREATE (n1)-[:CHILD]->(n2)
    ";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. Recursive CTE
    // Find all descendants of 0 (including 0)
    // We return IDs to make containment check simple
    let query = "
        WITH RECURSIVE descendants AS (
            MATCH (root:Node) WHERE root.id = 0 RETURN root.id as id
            UNION
            MATCH (parent:Node)-[:CHILD]->(child:Node)
            WHERE parent.id IN descendants
            RETURN child.id as id
        )
        MATCH (n:Node) WHERE n.id IN descendants RETURN n.id ORDER BY n.id
    ";

    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    println!("AST: {:?}", ast);
    let plan = planner.plan(ast)?;
    println!("Plan: {:?}", plan);

    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 3);
    assert_eq!(res[0].get("n.id").unwrap().as_i64().unwrap(), 0);
    assert_eq!(res[1].get("n.id").unwrap().as_i64().unwrap(), 1);
    assert_eq!(res[2].get("n.id").unwrap().as_i64().unwrap(), 2);

    Ok(())
}
