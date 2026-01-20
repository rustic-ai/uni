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
async fn test_vectorized_window_lag_lead() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Stock", false)?;
    schema_manager.add_property("Stock", "ticker", DataType::String, false)?;
    schema_manager.add_property("Stock", "price", DataType::Int64, false)?;
    schema_manager.add_property("Stock", "day", DataType::Int64, false)?;
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
    // AAPL: 100 (day 1), 105 (day 2), 110 (day 3)
    // GOOG: 200 (day 1), 195 (day 2)
    let query = "
        CREATE (s1:Stock {ticker: 'AAPL', price: 100, day: 1})
        CREATE (s2:Stock {ticker: 'AAPL', price: 105, day: 2})
        CREATE (s3:Stock {ticker: 'AAPL', price: 110, day: 3})
        CREATE (s4:Stock {ticker: 'GOOG', price: 200, day: 1})
        CREATE (s5:Stock {ticker: 'GOOG', price: 195, day: 2})
    ";

    // Split into multiple queries to ensure they are processed
    for q in query.split("CREATE").skip(1) {
        let full_q = format!("CREATE {}", q);
        let mut parser = CypherParser::new(&full_q)?;
        let ast = parser.parse()?;
        let plan = planner.plan(ast)?;
        executor.execute(plan, &prop_manager, &params).await?;
    }

    // 3. Test LAG (prev price)
    let query = "
        MATCH (s:Stock)
        RETURN s.ticker, s.day, s.price,
               lag(s.price) OVER (PARTITION BY s.ticker ORDER BY s.day) as prev_price
        ORDER BY s.ticker, s.day
    ";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // AAPL
    // Day 1: prev=null
    // Day 2: prev=100
    // Day 3: prev=105
    assert_eq!(res[0].get("s.ticker").unwrap().as_str().unwrap(), "AAPL");
    assert!(res[0].get("prev_price").unwrap().is_null());

    assert_eq!(res[1].get("s.ticker").unwrap().as_str().unwrap(), "AAPL");
    assert_eq!(res[1].get("prev_price").unwrap().as_i64().unwrap(), 100);

    assert_eq!(res[2].get("s.ticker").unwrap().as_str().unwrap(), "AAPL");
    assert_eq!(res[2].get("prev_price").unwrap().as_i64().unwrap(), 105);

    // GOOG
    // Day 1: prev=null
    // Day 2: prev=200
    assert_eq!(res[3].get("s.ticker").unwrap().as_str().unwrap(), "GOOG");
    assert!(res[3].get("prev_price").unwrap().is_null());

    assert_eq!(res[4].get("s.ticker").unwrap().as_str().unwrap(), "GOOG");
    assert_eq!(res[4].get("prev_price").unwrap().as_i64().unwrap(), 200);

    // 4. Test LEAD (next price) with offset
    let query = "
        MATCH (s:Stock)
        RETURN s.ticker, s.day, s.price,
               lead(s.price, 1, 0) OVER (PARTITION BY s.ticker ORDER BY s.day) as next_price
        ORDER BY s.ticker, s.day
    ";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // AAPL
    // Day 1: next=105
    // Day 2: next=110
    // Day 3: next=0 (default)
    assert_eq!(res[0].get("next_price").unwrap().as_i64().unwrap(), 105);
    assert_eq!(res[1].get("next_price").unwrap().as_i64().unwrap(), 110);
    assert_eq!(res[2].get("next_price").unwrap().as_i64().unwrap(), 0);

    Ok(())
}
