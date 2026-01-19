// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, UInt64Builder};
use arrow_array::{RecordBatch, StringArray, UInt64Array};
use lance::dataset::WriteMode;
use serde_json::Value;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::{Eid, Vid};
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_query_integration() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let person_label_id = schema_manager.add_label("Person", false)?;
    let knows_type_id =
        schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()));

    // VIDs
    // A (0)
    // B (1)
    let vid_a = Vid::new(person_label_id, 0);
    let vid_b = Vid::new(person_label_id, 1);

    // EID
    let eid_ab = Eid::new(knows_type_id, 1);

    // 2. Write Data
    // 2a. Vertices
    let vertex_ds = storage.vertex_dataset("Person")?;
    // We need to write _vid, _uid, _deleted, _version, name
    // _vid: [0, 1]
    // name: ["Alice", "Bob"]

    let schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    let vids = UInt64Array::from(vec![vid_a.as_u64(), vid_b.as_u64()]);
    let names = StringArray::from(vec!["Alice", "Bob"]);
    // _uid, _deleted, _version can be null or defaults
    // But Lance requires matching schema.
    // get_arrow_schema returns fields in specific order.
    // _vid, _uid, _deleted, _version, name (sorted)

    let mut uid_builder = FixedSizeBinaryBuilder::new(32);
    let dummy_uid = vec![0u8; 32];
    uid_builder.append_value(&dummy_uid).unwrap();
    uid_builder.append_value(&dummy_uid).unwrap();
    let uids = uid_builder.finish();

    let deleted = arrow_array::BooleanArray::from(vec![false, false]);
    let versions = UInt64Array::from(vec![1, 1]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(vids),
            Arc::new(uids),     // _uid
            Arc::new(deleted),  // _deleted
            Arc::new(versions), // _version
            Arc::new(names),    // name
        ],
    )?;

    vertex_ds.write_batch(batch, WriteMode::Overwrite).await?;

    // 2b. Adjacency (A->B)
    let adj_ds = storage.adjacency_dataset("knows", "Person", "fwd")?;
    let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
    let mut edge_ids_builder = ListBuilder::new(UInt64Builder::new());

    // Row 0 (A): neighbors [B], eids [eid_ab]
    neighbors_builder.values().append_value(vid_b.as_u64());
    neighbors_builder.append(true);
    edge_ids_builder.values().append_value(eid_ab.as_u64());
    edge_ids_builder.append(true);

    // Row 1 (B): empty
    neighbors_builder.append(true);
    edge_ids_builder.append(true);

    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![vid_a.as_u64(), vid_b.as_u64()])),
            Arc::new(neighbors_builder.finish()),
            Arc::new(edge_ids_builder.finish()),
        ],
    )?;

    adj_ds.write_chunk(batch, WriteMode::Overwrite).await?;

    // 3. Execute Query
    let sql = "MATCH (a:Person)-[:knows]->(b:Person) RETURN a.name";

    let mut parser = CypherParser::new(sql)?;
    let query_ast = parser.parse()?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new(storage.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &std::collections::HashMap::new())
        .await?;

    println!("Results: {:?}", results);

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("a.name"),
        Some(&Value::String("Alice".to_string()))
    );

    Ok(())
}
