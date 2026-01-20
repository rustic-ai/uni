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
async fn test_vectorized_window_percent_cume() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Score", false)?;
    schema_manager.add_property("Score", "val", DataType::Int64, false)?;
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

    // 2. Create data: 10, 20, 20, 30, 40 (5 rows)
    let query = "
        CREATE (s1:Score {val: 10})
        CREATE (s2:Score {val: 20})
        CREATE (s3:Score {val: 20})
        CREATE (s4:Score {val: 30})
        CREATE (s5:Score {val: 40})
    ";
    for q in query.split("CREATE").skip(1) {
        let full_q = format!("CREATE {}", q);
        let mut parser = CypherParser::new(&full_q)?;
        let ast = parser.parse()?;
        let plan = planner.plan(ast)?;
        executor.execute(plan, &prop_manager, &params).await?;
    }

    // 3. Test PERCENT_RANK and CUME_DIST
    // Val | Rank | PeerCountLE | N=5
    // 10  | 1    | 1           | PR = (1-1)/4 = 0.0, CD = 1/5 = 0.2
    // 20  | 2    | 3           | PR = (2-1)/4 = 0.25, CD = 3/5 = 0.6
    // 20  | 2    | 3           | PR = 0.25, CD = 0.6
    // 30  | 4    | 4           | PR = (4-1)/4 = 0.75, CD = 4/5 = 0.8
    // 40  | 5    | 5           | PR = (5-1)/4 = 1.0, CD = 5/5 = 1.0
    let query = "
        MATCH (s:Score)
        RETURN s.val,
               percent_rank() OVER (ORDER BY s.val) as pr,
               cume_dist() OVER (ORDER BY s.val) as cd
        ORDER BY s.val
    ";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse()?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // Row 1: 10
    assert_eq!(res[0].get("pr").unwrap().as_f64().unwrap(), 0.0);
    assert_eq!(res[0].get("cd").unwrap().as_f64().unwrap(), 0.2);

    // Row 2: 20
    assert_eq!(res[1].get("pr").unwrap().as_f64().unwrap(), 0.25);
    assert_eq!(res[1].get("cd").unwrap().as_f64().unwrap(), 0.6);

    // Row 3: 20
    assert_eq!(res[2].get("pr").unwrap().as_f64().unwrap(), 0.25);
    assert_eq!(res[2].get("cd").unwrap().as_f64().unwrap(), 0.6);

    // Row 4: 30
    assert_eq!(res[3].get("pr").unwrap().as_f64().unwrap(), 0.75);
    assert_eq!(res[3].get("cd").unwrap().as_f64().unwrap(), 0.8);

    // Row 5: 40
    assert_eq!(res[4].get("pr").unwrap().as_f64().unwrap(), 1.0);
    assert_eq!(res[4].get("cd").unwrap().as_f64().unwrap(), 1.0);

    Ok(())
}
