// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{ListBuilder, UInt64Builder};
use arrow_array::{BooleanArray, RecordBatch, UInt64Array};
use lance::dataset::WriteMode;
use serde_json::Value;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_var_length() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;
    let _knows_type =
        schema_manager.add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.add_property("Person", "id", DataType::Int32, false)?;

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Chain: A(0) -> B(1) -> C(2) -> D(3)
    let vertex_ds = storage.vertex_dataset("Person")?;
    let batch = RecordBatch::try_new(
        vertex_ds.get_arrow_schema(&schema_manager.schema())?,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(person_lbl, 0).as_u64(),
                Vid::new(person_lbl, 1).as_u64(),
                Vid::new(person_lbl, 2).as_u64(),
                Vid::new(person_lbl, 3).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 4].into(),
                None,
            )),
            Arc::new(BooleanArray::from(vec![false; 4])),
            Arc::new(UInt64Array::from(vec![1; 4])),
            Arc::new(arrow_array::Int32Array::from(vec![0, 1, 2, 3])),
        ],
    )?;
    vertex_ds.write_batch(batch, WriteMode::Overwrite).await?;

    let adj_ds = storage.adjacency_dataset("KNOWS", "Person", "fwd")?;
    let mut n_builder = ListBuilder::new(UInt64Builder::new());
    let mut e_builder = ListBuilder::new(UInt64Builder::new());

    // A->B (edge 0)
    n_builder
        .values()
        .append_value(Vid::new(person_lbl, 1).as_u64());
    n_builder.append(true);
    e_builder.values().append_value(Eid::new(0, 0).as_u64());
    e_builder.append(true);

    // B->C (edge 1)
    n_builder
        .values()
        .append_value(Vid::new(person_lbl, 2).as_u64());
    n_builder.append(true);
    e_builder.values().append_value(Eid::new(0, 1).as_u64());
    e_builder.append(true);

    // C->D (edge 2)
    n_builder
        .values()
        .append_value(Vid::new(person_lbl, 3).as_u64());
    n_builder.append(true);
    e_builder.values().append_value(Eid::new(0, 2).as_u64());
    e_builder.append(true);

    // D->
    n_builder.append(true);
    e_builder.append(true);

    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(person_lbl, 0).as_u64(),
                Vid::new(person_lbl, 1).as_u64(),
                Vid::new(person_lbl, 2).as_u64(),
                Vid::new(person_lbl, 3).as_u64(),
            ])),
            Arc::new(n_builder.finish()),
            Arc::new(e_builder.finish()),
        ],
    )?;
    adj_ds.write_chunk(batch, WriteMode::Overwrite).await?;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new(storage.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Test 1: Fixed *2 (A->C)
    println!("--- Test 1: *2 ---");
    let sql = "MATCH (a:Person)-[:KNOWS*2]->(b:Person) WHERE a.id = 0 RETURN b.id";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("b.id"), Some(&Value::Number(2.into())));

    // Test 2: Range *1..2 (A->B, A->C)
    println!("--- Test 2: *1..2 ---");
    let sql =
        "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) WHERE a.id = 0 RETURN b.id ORDER BY b.id ASC";
    let query = CypherParser::new(sql)?.parse()?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("b.id"), Some(&Value::Number(1.into())));
    assert_eq!(results[1].get("b.id"), Some(&Value::Number(2.into())));

    Ok(())
}
