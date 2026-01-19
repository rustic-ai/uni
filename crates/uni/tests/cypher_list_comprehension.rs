// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
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
async fn test_list_comprehension_literals() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
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
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // 1. Literal Mapping - transform all elements
    let cypher = "RETURN [x IN [1, 2, 3] | 10]";
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let res = results[0]
        .values()
        .next()
        .expect("Result should have one column");
    assert_eq!(res, &json!([10, 10, 10]));

    // 2. Filter - keep elements matching condition
    let cypher = "RETURN [x IN [1, 2, 3, 4] WHERE x > 2]";
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let res = results[0]
        .values()
        .next()
        .expect("Result should have one column");
    assert_eq!(res, &json!([3, 4]));

    // 3. Identity (Flat List) - no transformation
    let cypher = "RETURN [x IN [1, 2, 3] | x]";
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let res = results[0]
        .values()
        .next()
        .expect("Result should have one column");
    assert_eq!(res, &json!([1, 2, 3]));

    // 4. Property Access from Map
    let cypher = "RETURN [x IN [{a: 1}, {a: 2}] | x.a]";
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let res = results[0]
        .values()
        .next()
        .expect("Result should have one column");
    assert_eq!(res, &json!([1, 2]));

    // 5. Filter + Mapping combined
    let cypher = "RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x * 2]";
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let res = results[0]
        .values()
        .next()
        .expect("Result should have one column");
    assert_eq!(res, &json!([6, 8, 10])); // 3*2, 4*2, 5*2

    // 6. Filter that removes all elements - empty result
    let cypher = "RETURN [x IN [1, 2, 3] WHERE x > 10]";
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let res = results[0]
        .values()
        .next()
        .expect("Result should have one column");
    assert_eq!(res, &json!([]));

    // 7. Nested list comprehension
    let cypher = "RETURN [x IN [1, 2] | [y IN [10, 20] | x + y]]";
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let res = results[0]
        .values()
        .next()
        .expect("Result should have one column");
    assert_eq!(res, &json!([[11, 21], [12, 22]]));

    // 8. String operations in mapping
    let cypher = r#"RETURN [x IN ["a", "b", "c"] | upper(x)]"#;
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    let res = results[0]
        .values()
        .next()
        .expect("Result should have one column");
    assert_eq!(res, &json!(["A", "B", "C"]));

    Ok(())
}
