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
async fn test_edge_export_failure() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person", false)?;
    schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.add_property("KNOWS", "since", DataType::Int32, false)?;
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

    // 2. Insert Data
    {
        let mut w = writer.write().await;
        // Alice (1:0), Bob (1:1)
        w.insert_vertex(uni_db::common::core::id::Vid::new(1, 0), HashMap::new())
            .await?;
        w.insert_vertex(uni_db::common::core::id::Vid::new(1, 1), HashMap::new())
            .await?;

        // Edge Alice -> Bob
        let eid = uni_db::common::core::id::Eid::new(1, 0); // Type 1 (KNOWS)
        let mut props = HashMap::new();
        props.insert("since".to_string(), json!(2022));
        w.insert_edge(
            uni_db::common::core::id::Vid::new(1, 0),
            uni_db::common::core::id::Vid::new(1, 1),
            1,
            eid,
            props,
        )
        .await?;
        w.flush_to_l1(None).await?; // Flush to ensure it's in storage (optional for export logic depending on implementation)
    }

    // 3. Try Export Edges
    let export_csv = path.join("knows_export.csv");
    let cypher = format!("COPY KNOWS TO '{}'", export_csv.to_str().unwrap());

    let res = executor
        .execute(
            planner.plan(CypherParser::new(&cypher)?.parse()?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res[0].get("count").unwrap(), &json!(1));

    // Verify Exported File
    let mut rdr = csv::ReaderBuilder::new().from_path(&export_csv)?;
    let headers = rdr.headers()?.clone();

    // Header should contain _eid, _src, _dst, _type, since
    assert!(headers.iter().any(|h| h == "_eid"));
    assert!(headers.iter().any(|h| h == "_src"));
    assert!(headers.iter().any(|h| h == "_dst"));
    assert!(headers.iter().any(|h| h == "_type"));
    assert!(headers.iter().any(|h| h == "since"));

    let mut count = 0;
    for result in rdr.records() {
        let record = result?;
        let src_idx = headers.iter().position(|h| h == "_src").unwrap();
        let dst_idx = headers.iter().position(|h| h == "_dst").unwrap();
        let since_idx = headers.iter().position(|h| h == "since").unwrap();

        assert_eq!(record.get(src_idx).unwrap(), "1:0");
        assert_eq!(record.get(dst_idx).unwrap(), "1:1");
        assert_eq!(record.get(since_idx).unwrap(), "2022");
        count += 1;
    }
    assert_eq!(count, 1);
    Ok(())
}
