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
async fn test_vectorized_window_first_last() -> anyhow::Result<()> {
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

    // 2. Create data
    let query = "
        CREATE (s1:Stock {ticker: 'AAPL', price: 100, day: 1})
        CREATE (s2:Stock {ticker: 'AAPL', price: 105, day: 2})
        CREATE (s3:Stock {ticker: 'AAPL', price: 110, day: 3})
        CREATE (s4:Stock {ticker: 'GOOG', price: 200, day: 1})
        CREATE (s5:Stock {ticker: 'GOOG', price: 195, day: 2})
    ";
    for q in query.split("CREATE").skip(1) {
        let full_q = format!("CREATE {}", q);
        let mut parser = CypherParser::new(&full_q)?;
        let ast = parser.parse()?;
        let plan = planner.plan(ast)?;
        executor.execute(plan, &prop_manager, &params).await?;
    }

    // 3. Test FIRST_VALUE (opening price)
    let query = "
        MATCH (s:Stock)
        RETURN s.ticker, s.day,
               first_value(s.price) OVER (PARTITION BY s.ticker ORDER BY s.day) as open_price
        ORDER BY s.ticker, s.day
    ";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // AAPL: First is 100
    assert_eq!(res[0].get("s.ticker").unwrap().as_str().unwrap(), "AAPL");
    assert_eq!(res[0].get("open_price").unwrap().as_i64().unwrap(), 100);
    assert_eq!(res[1].get("open_price").unwrap().as_i64().unwrap(), 100);
    assert_eq!(res[2].get("open_price").unwrap().as_i64().unwrap(), 100);

    // GOOG: First is 200
    assert_eq!(res[3].get("s.ticker").unwrap().as_str().unwrap(), "GOOG");
    assert_eq!(res[3].get("open_price").unwrap().as_i64().unwrap(), 200);
    assert_eq!(res[4].get("open_price").unwrap().as_i64().unwrap(), 200);

    // 4. Test LAST_VALUE (closing price)
    // Note: Standard SQL LAST_VALUE uses ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW default frame.
    // Our implementation assumes UNBOUNDED FOLLOWING (partition scope) for now as per plan.
    let query = "
        MATCH (s:Stock)
        RETURN s.ticker, s.day,
               last_value(s.price) OVER (PARTITION BY s.ticker ORDER BY s.day) as close_price
        ORDER BY s.ticker, s.day
    ";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // AAPL: Last is 110
    assert_eq!(res[0].get("close_price").unwrap().as_i64().unwrap(), 110);
    assert_eq!(res[1].get("close_price").unwrap().as_i64().unwrap(), 110);
    assert_eq!(res[2].get("close_price").unwrap().as_i64().unwrap(), 110);

    // GOOG: Last is 195
    assert_eq!(res[3].get("close_price").unwrap().as_i64().unwrap(), 195);
    assert_eq!(res[4].get("close_price").unwrap().as_i64().unwrap(), 195);

    Ok(())
}
