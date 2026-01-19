// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::UniConfig;
use uni::core::id::{Eid, Vid};
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::runtime::writer::Writer;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_multi_hop() -> anyhow::Result<()> {
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
    // A -> D -> E
    let names = ["A", "B", "C", "D", "E"];
    let mut vids = Vec::new();

    for (i, name) in names.iter().enumerate() {
        let vid = Vid::new(person_lbl, i as u64);
        vids.push(vid);
        let mut props = HashMap::new();
        props.insert("name".to_string(), json!(name));
        writer.insert_vertex(vid, props).await?;
    }

    // Edges
    // A(0) -> B(1)
    writer
        .insert_edge(
            vids[0],
            vids[1],
            knows_edge,
            Eid::new(knows_edge, 0),
            HashMap::new(),
        )
        .await?;
    // B(1) -> C(2)
    writer
        .insert_edge(
            vids[1],
            vids[2],
            knows_edge,
            Eid::new(knows_edge, 1),
            HashMap::new(),
        )
        .await?;
    // A(0) -> D(3)
    writer
        .insert_edge(
            vids[0],
            vids[3],
            knows_edge,
            Eid::new(knows_edge, 2),
            HashMap::new(),
        )
        .await?;
    // D(3) -> E(4)
    writer
        .insert_edge(
            vids[3],
            vids[4],
            knows_edge,
            Eid::new(knows_edge, 3),
            HashMap::new(),
        )
        .await?;

    writer.flush_to_l1(None).await?;

    // 3. Test 2-hop fixed
    // MATCH (n:Person)-[:KNOWS*2..2]->(m) WHERE n.name = 'A' RETURN m.name
    // Should return C and E.
    let cypher = "MATCH (n:Person)-[:KNOWS*2..2]->(m:Person) WHERE n.name = 'A' RETURN m.name";

    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new(storage.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // Debug print
    println!("Results: {:?}", results);

    assert_eq!(results.len(), 2, "Expected 2 results for 2-hop query");
    let mut found_names: Vec<String> = results
        .iter()
        .map(|r| r.get("m.name").unwrap().as_str().unwrap().to_string())
        .collect();
    found_names.sort();
    assert_eq!(found_names, vec!["C", "E"]);

    // 4. Test Variable Hop (1..2)
    // MATCH (n:Person)-[:KNOWS*1..2]->(m) WHERE n.name = 'A' RETURN m.name
    // Should return B, D (1 hop) and C, E (2 hops)
    let cypher = "MATCH (n:Person)-[:KNOWS*1..2]->(m:Person) WHERE n.name = 'A' RETURN m.name";
    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;
    let plan = planner.plan(query_ast)?;
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    assert_eq!(results.len(), 4, "Expected 4 results for 1..2 hop query");
    let mut found_names: Vec<String> = results
        .iter()
        .map(|r| r.get("m.name").unwrap().as_str().unwrap().to_string())
        .collect();
    found_names.sort();
    assert_eq!(found_names, vec!["B", "C", "D", "E"]);

    Ok(())
}
