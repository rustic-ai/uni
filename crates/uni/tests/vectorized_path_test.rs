// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::UniConfig;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_path_binding() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;
    let knows_edge = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;

    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));
    let cache_arc = Some(storage.adjacency_cache());
    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        UniConfig::default(),
        cache_arc,
        None,
        None,
    )
    .await
    .unwrap();

    // 2. Insert Data
    // A -> B -> C
    let names = ["A", "B", "C"];
    let mut vids = Vec::new();

    for (i, name) in names.iter().enumerate() {
        let vid = Vid::new(person_lbl, i as u64);
        vids.push(vid);
        let mut props = HashMap::new();
        props.insert("name".to_string(), json!(name));
        writer.insert_vertex(vid, props).await?;
    }

    // Edges with specific EIDs
    // A(0) -> B(1), eid=10
    writer
        .insert_edge(
            vids[0],
            vids[1],
            knows_edge,
            Eid::new(knows_edge, 10),
            HashMap::new(),
        )
        .await?;
    // B(1) -> C(2), eid=11
    writer
        .insert_edge(
            vids[1],
            vids[2],
            knows_edge,
            Eid::new(knows_edge, 11),
            HashMap::new(),
        )
        .await?;

    writer.flush_to_l1(None).await?;

    // 3. Test Path Binding (Fixed Length *2)
    // MATCH (n:Person)-[p:KNOWS*2..2]->(m:Person) WHERE n.name = 'A' RETURN m.name, p
    let cypher = "MATCH (n:Person)-[p:KNOWS*2..2]->(m:Person) WHERE n.name = 'A' RETURN m.name, p";

    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new(storage.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("m.name"),
        Some(&Value::String("C".to_string()))
    );

    let p_val = results[0].get("p").unwrap();
    // p should be a Struct with { nodes: [...], relationships: [...] }
    if let Value::Object(map) = p_val {
        let rels = map
            .get("relationships")
            .expect("relationships field missing");
        if let Value::Array(arr) = rels {
            assert_eq!(arr.len(), 2);
            // We expect raw u64 values (EIDs)
            // Eid(10) and Eid(11)
            let eid10 = Eid::new(knows_edge, 10).as_u64();
            let eid11 = Eid::new(knows_edge, 11).as_u64();

            assert_eq!(arr[0].as_u64(), Some(eid10));
            assert_eq!(arr[1].as_u64(), Some(eid11));
        } else {
            panic!("relationships is not an array");
        }

        let nodes = map.get("nodes").expect("nodes field missing");
        if let Value::Array(arr) = nodes {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("nodes is not an array");
        }
    } else {
        panic!("p is not an object (Struct)");
    }

    Ok(())
}
