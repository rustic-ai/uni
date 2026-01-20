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
async fn test_vectorized_distinct_aggregate() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int64, false)?;
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

    // 2. Create data:
    // Alice (30)
    // Bob (30)
    // Alice (30) -- duplicate name and age
    // Charlie (40)

    let query = "CREATE (p1:Person {name: 'Alice', age: 30})
                 CREATE (p2:Person {name: 'Bob', age: 30})
                 CREATE (p3:Person {name: 'Alice', age: 30})
                 CREATE (p4:Person {name: 'Charlie', age: 40})";

    // Split into multiple queries to ensure they are processed
    for q in query.split("CREATE").skip(1) {
        let full_q = format!("CREATE {}", q);
        let mut parser = CypherParser::new(&full_q)?;
        let ast = parser.parse()?;
        let plan = planner.plan(ast)?;
        executor.execute(plan, &prop_manager, &params).await?;
    }

    // 3. Test DISTINCT aggregates
    // Unique names: Alice, Bob, Charlie (3)
    // Unique ages: 30, 40 (2)
    // Sum unique ages: 30 + 40 = 70

    let query = "MATCH (p:Person) RETURN count(DISTINCT p.name) as dist_names, count(p.name) as total_names, sum(DISTINCT p.age) as sum_dist_age";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res.len(), 1);

    let dist_names = res[0].get("dist_names").unwrap().as_i64().unwrap();
    let total_names = res[0].get("total_names").unwrap().as_i64().unwrap();
    let sum_dist_age = res[0].get("sum_dist_age").unwrap().as_f64().unwrap(); // SUM usually returns float

    assert_eq!(dist_names, 3);
    assert_eq!(total_names, 4);
    assert_eq!(sum_dist_age, 70.0);

    // 4. Test COLLECT(DISTINCT)
    // Note: ORDER BY causes interaction issues with Sort operator (potential separate bug in Sort re-evaluating expressions)
    // We sort manually in test.
    let query = "MATCH (p:Person) RETURN collect(DISTINCT p.name) as unique_names";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    let unique_names_val = res[0].get("unique_names").unwrap();
    let unique_names_arr = unique_names_val.as_array().unwrap();

    assert_eq!(unique_names_arr.len(), 3);
    let mut names: Vec<String> = unique_names_arr
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);

    Ok(())
}
