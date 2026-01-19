// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::schema::SchemaManager;
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_explain() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person", false)?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    let query = "EXPLAIN MATCH (n:Person) RETURN n";

    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(ast)?;

    let executor = Executor::new(storage.clone());
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let result = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(result.len(), 1);
    let plan_str = result[0].get("plan").unwrap().as_str().unwrap();
    println!("Plan:\n{}", plan_str);

    assert!(plan_str.contains("Scan"));
    assert!(plan_str.contains("Project"));

    Ok(())
}
