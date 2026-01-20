// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::RecordBatch;
use lance::dataset::WriteMode;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::config::UniConfig;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_undirected_traversal() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();
    let node_lbl = schema_manager.add_label("Node", false).unwrap();
    schema_manager
        .add_property("Node", "name", DataType::String, true)
        .unwrap();
    let edge_type = schema_manager
        .add_edge_type("REL", vec!["Node".into()], vec!["Node".into()])
        .unwrap();
    schema_manager.save().await.unwrap();
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Create Nodes: A(0), B(1)
    let ds = storage.vertex_dataset("Node").unwrap();
    let schema = ds.get_arrow_schema(&schema_manager.schema()).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::UInt64Array::from(vec![
                Vid::new(node_lbl, 0).as_u64(),
                Vid::new(node_lbl, 1).as_u64(),
            ])), // _vid
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 2].into(),
                None,
            )), // _uid
            Arc::new(arrow_array::BooleanArray::from(vec![false, false])), // _deleted
            Arc::new(arrow_array::UInt64Array::from(vec![1, 1])),          // _version
            Arc::new(arrow_array::StringArray::from(vec![Some("A"), Some("B")])), // name
        ],
    )
    .unwrap();
    ds.write_batch(batch, WriteMode::Overwrite).await.unwrap();

    // Create Edge: A -> B
    // DeltaDataset "REL_fwd" and "REL_bwd"
    use uni_db::storage::delta::{L1Entry, Op};
    let op = L1Entry {
        src_vid: Vid::new(node_lbl, 0),
        dst_vid: Vid::new(node_lbl, 1),
        eid: Eid::new(edge_type, 0),
        op: Op::Insert,
        version: 1,
        properties: Default::default(),
    };

    // Write Forward
    let delta_fwd = storage.delta_dataset("REL", "fwd").unwrap();
    let batch_fwd = delta_fwd
        .build_record_batch(std::slice::from_ref(&op), &schema_manager.schema())
        .unwrap();
    delta_fwd.write_run(batch_fwd).await.unwrap();

    // Write Backward
    let delta_bwd = storage.delta_dataset("REL", "bwd").unwrap();
    let batch_bwd = delta_bwd
        .build_record_batch(std::slice::from_ref(&op), &schema_manager.schema())
        .unwrap();
    delta_bwd.write_run(batch_bwd).await.unwrap();

    let prop_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        100,
    ));
    let executor = Executor::new(storage);
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Query: (B)-[]-(A)  (Undirected)
    // Starting from B (name='B'), find neighbor with name='A'
    let sql = "MATCH (b:Node {name: 'B'})-[]-(a:Node) RETURN a.name";
    let plan = planner
        .plan(CypherParser::new(sql).unwrap().parse().unwrap())
        .unwrap();

    let batch = executor
        .execute_vectorized(
            plan,
            &prop_manager,
            &std::collections::HashMap::new(),
            UniConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(batch.num_rows(), 1);
    let col = batch
        .column("a.name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "A");

    // Query: (A)-[]-(B) (Undirected, should follow outgoing as well)
    let sql2 = "MATCH (a:Node {name: 'A'})-[]-(b:Node) RETURN b.name";
    let plan2 = planner
        .plan(CypherParser::new(sql2).unwrap().parse().unwrap())
        .unwrap();
    let batch2 = executor
        .execute_vectorized(
            plan2,
            &prop_manager,
            &std::collections::HashMap::new(),
            UniConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(batch2.num_rows(), 1);
    let col2 = batch2
        .column("b.name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(col2.value(0), "B");
}
