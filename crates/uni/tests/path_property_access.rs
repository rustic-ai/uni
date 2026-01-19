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
async fn test_path_property_access() -> anyhow::Result<()> {
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
    // A -> B
    let vid_a = Vid::new(person_lbl, 0);
    let vid_b = Vid::new(person_lbl, 1);

    let mut props_a = HashMap::new();
    props_a.insert("name".to_string(), json!("Alice"));
    writer.insert_vertex(vid_a, props_a).await?;

    let mut props_b = HashMap::new();
    props_b.insert("name".to_string(), json!("Bob"));
    writer.insert_vertex(vid_b, props_b).await?;

    writer
        .insert_edge(
            vid_a,
            vid_b,
            knows_edge,
            Eid::new(knows_edge, 10),
            HashMap::new(),
        )
        .await?;

    // Keep data in L0 to verify L0 lookup in vectorized engine
    // writer.flush_to_l1().await?;

    // 3. Test Property Access on Path Node: nodes(p)[0].name
    // Use *1..2 to force path binding (Path object) instead of simple relationship binding
    // *1..1 is optimized to Traverse which binds Relationship (EID) not Path.
    let cypher =
        "MATCH (a:Person)-[p:KNOWS*1..2]->(b:Person) RETURN nodes(p)[0].name, nodes(p)[1].name";

    let mut parser = CypherParser::new(cypher)?;
    let query_ast = parser.parse()?;

    // Set RUST_LOG for debugging
    // std::env::set_var("RUST_LOG", "debug");
    // let _ = env_logger::builder().is_test(true).try_init();

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor =
        Executor::new_with_writer(storage.clone(), Arc::new(tokio::sync::RwLock::new(writer)));

    // We need to access writer to get L0 manager for prop manager?
    // Executor::new_with_writer handles context creation.
    // But we need to pass a property manager.
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await?;

    // println!("DEBUG: results[0] keys: {:?}", results[0].keys());
    // println!("DEBUG: results[0] values: {:?}", results[0]);

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("NODES(p)[Number(0)].name"),
        Some(&json!("Alice"))
    );
    assert_eq!(
        results[0].get("NODES(p)[Number(1)].name"),
        Some(&json!("Bob"))
    );

    Ok(())
}
