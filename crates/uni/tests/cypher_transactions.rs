// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_explicit_transactions() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person", false)?;
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
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // 2. BEGIN Transaction
    executor
        .execute(
            planner.plan(CypherParser::new("BEGIN")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;

    // 3. CREATE in transaction
    executor
        .execute(
            planner.plan(CypherParser::new("CREATE (n:Person {name: 'Alice'})")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;

    // 4. Query in transaction (Should see Alice)
    let res = executor
        .execute(
            planner.plan(CypherParser::new("MATCH (n:Person) RETURN n.name")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("n.name"), Some(&json!("Alice")));

    // 5. ROLLBACK
    executor
        .execute(
            planner.plan(CypherParser::new("ROLLBACK")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;

    // 6. Query after rollback (Should be empty)
    let res = executor
        .execute(
            planner.plan(CypherParser::new("MATCH (n:Person) RETURN n.name")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 0);

    // 7. COMMIT Transaction
    executor
        .execute(
            planner.plan(CypherParser::new("BEGIN")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    executor
        .execute(
            planner.plan(CypherParser::new("CREATE (n:Person {name: 'Bob'})")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    executor
        .execute(
            planner.plan(CypherParser::new("COMMIT")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;

    // 8. Query after commit (Should see Bob)
    let res = executor
        .execute(
            planner.plan(CypherParser::new("MATCH (n:Person) RETURN n.name")?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("n.name"), Some(&json!("Bob")));

    Ok(())
}
