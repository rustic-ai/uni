// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{Float64Array, RecordBatch, StringArray, UInt64Array};
use std::sync::Arc;
use uni::core::id::{Eid, UniId, Vid};
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::storage::manager::StorageManager;

// ... existing tests ...

#[tokio::test]
async fn test_regional_sales_analytics() -> anyhow::Result<()> {
    // Scenario: Region <-[:SHIPPED_TO]- Order.
    // 1. Find Region
    // 2. Traverse to Orders
    // 3. Columnar Scan of "amount" for these orders (Batch Read)
    // 4. Compute Sum (Analytics)

    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let region_lbl = schema_manager.add_label("Region", false)?;
    let order_lbl = schema_manager.add_label("Order", false)?;
    let shipped_type =
        schema_manager.add_edge_type("SHIPPED_TO", vec!["Order".into()], vec!["Region".into()])?;

    schema_manager.add_property("Region", "name", DataType::String, false)?;
    schema_manager.add_property("Order", "amount", DataType::Float64, false)?; // Float64 for currency

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Data:
    // Region: "North" (Vid 0)
    // Orders: 0..100. All shipped to "North".
    // Amount = 10.0 * (i + 1)

    // Insert Region
    let region_ds = storage.vertex_dataset("Region")?;
    let region_batch = RecordBatch::try_new(
        region_ds.get_arrow_schema(&schema_manager.schema())?,
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(region_lbl, 0).as_u64()])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false])),
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["North"])),
        ],
    )?;
    region_ds
        .write_batch(region_batch, WriteMode::Overwrite)
        .await?;

    // Insert Orders (Batch of 100)
    let order_ds = storage.vertex_dataset("Order")?;
    let num_orders = 100;

    let mut vid_builder = arrow_array::builder::UInt64Builder::new();
    let mut amount_builder = arrow_array::builder::Float64Builder::new();
    let mut uid_builder = arrow_array::builder::FixedSizeBinaryBuilder::new(32);
    let mut bool_builder = arrow_array::builder::BooleanBuilder::new();
    let mut ver_builder = arrow_array::builder::UInt64Builder::new();

    let dummy_uid = vec![0u8; 32];

    for i in 0..num_orders {
        vid_builder.append_value(Vid::new(order_lbl, i as u64).as_u64());
        amount_builder.append_value(10.0 * (i as f64 + 1.0));
        uid_builder.append_value(&dummy_uid).unwrap();
        bool_builder.append_value(false);
        ver_builder.append_value(1);
    }

    let order_batch = RecordBatch::try_new(
        order_ds.get_arrow_schema(&schema_manager.schema())?,
        vec![
            Arc::new(vid_builder.finish()),
            Arc::new(uid_builder.finish()),
            Arc::new(bool_builder.finish()),
            Arc::new(ver_builder.finish()),
            Arc::new(amount_builder.finish()),
        ],
    )?;
    order_ds
        .write_batch(order_batch, WriteMode::Overwrite)
        .await?;

    // Edges: Order(i) -> Region(0)
    // Adjacency: Region(0) <- Orders(0..99) (Incoming)
    // To traverse efficiently from Region -> Orders, we need "SHIPPED_TO" "bwd" (incoming) adjacency on Region.
    // Or "fwd" adjacency on Orders.
    // Query: MATCH (r:Region {name: "North"})<-[:SHIPPED_TO]-(o:Order) ...
    // Direction: Incoming from Region perspective.

    // We populate `adj_bwd_SHIPPED_TO_Region`.
    // Format: dst_vid (Region) -> [src_vids (Orders)]
    let adj_ds = storage.adjacency_dataset("SHIPPED_TO", "Region", "bwd")?; // bwd partition by dst_label

    let mut n_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());
    let mut e_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());

    // One row for Region 0
    for i in 0..num_orders {
        n_builder
            .values()
            .append_value(Vid::new(order_lbl, i as u64).as_u64());
        e_builder
            .values()
            .append_value(Eid::new(shipped_type, i as u64).as_u64());
    }
    n_builder.append(true);
    e_builder.append(true);

    let adj_batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(region_lbl, 0).as_u64()])),
            Arc::new(n_builder.finish()),
            Arc::new(e_builder.finish()),
        ],
    )?;
    adj_ds.write_chunk(adj_batch, WriteMode::Overwrite).await?;

    // Execution:
    // 1. Load subgraph (Region -> incoming Orders)
    let region_vid = Vid::new(region_lbl, 0);
    // Load incoming edges
    let graph = storage
        .load_subgraph(
            &[region_vid],
            &[shipped_type],
            1,
            uni::runtime::Direction::Incoming,
            None,
        )
        .await?;

    // 2. Collect Order VIDs
    let mut order_vids = Vec::new();
    for e in graph.edges() {
        let u_vid = e.src_vid;
        order_vids.push(u_vid);
    }
    assert_eq!(order_vids.len(), 100);

    // 3. Columnar Fetch: "amount"
    // Use low-level Lance access via StorageManager -> VertexDataset
    let order_ds = storage.vertex_dataset("Order")?;
    let ds = order_ds.open().await?;

    // We need offsets for `take`. Vid local_offset.
    let offsets: Vec<u64> = order_vids.iter().map(|v| v.local_offset()).collect();

    // Project "amount".
    // Lance `take` with projection.
    let schema = ds.schema().project(&["amount"])?;
    let batch = ds.take(&offsets, schema).await?;

    // 4. Compute Sum (Vectorized/Columnar)
    let amount_col = batch
        .column_by_name("amount")
        .expect("amount column missing")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Invalid type");

    let total_sales: f64 = amount_col.iter().flatten().sum();

    // Expected: 10 * (1 + 2 + ... + 100)
    // Sum 1..100 = 100*101/2 = 5050
    // Total = 50500.0
    assert_eq!(total_sales, 50500.0);

    Ok(())
}
use lance::dataset::WriteMode;
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_ecommerce_recommendation() -> anyhow::Result<()> {
    // Scenario: User -> VIEWED -> Product. Find products similar to what User viewed.
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let user_lbl = schema_manager.add_label("User", false)?;
    let prod_lbl = schema_manager.add_label("Product", false)?;
    let viewed_type =
        schema_manager.add_edge_type("VIEWED", vec!["User".into()], vec!["Product".into()])?;

    schema_manager.add_property("User", "name", DataType::String, false)?;
    schema_manager.add_property("Product", "name", DataType::String, false)?;
    schema_manager.add_property(
        "Product",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // 2. Data
    // User: Alice (vid 0)
    // Product: Laptop (vid 0) -> Vec [1.0, 0.0]
    // Product: Mouse (vid 1) -> Vec [0.9, 0.1] (Close)
    // Product: Shampoo (vid 2) -> Vec [0.0, 1.0] (Far)

    // Insert User
    let user_ds = storage.vertex_dataset("User")?;
    let user_schema = user_ds.get_arrow_schema(&schema_manager.schema())?;
    let batch = RecordBatch::try_new(
        user_schema,
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(user_lbl, 0).as_u64()])), // _vid
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32].into(),
                None,
            )), // _uid (dummy)
            Arc::new(arrow_array::BooleanArray::from(vec![false])),            // _deleted
            Arc::new(UInt64Array::from(vec![1])),                              // _version
            Arc::new(StringArray::from(vec!["Alice"])),                        // name
        ],
    )?;
    user_ds.write_batch(batch, WriteMode::Overwrite).await?;

    // Insert Products
    let prod_ds = storage.vertex_dataset("Product")?;
    let prod_schema = prod_ds.get_arrow_schema(&schema_manager.schema())?;

    let mut vec_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    // Laptop
    vec_builder.values().append_value(1.0);
    vec_builder.values().append_value(0.0);
    vec_builder.append(true);
    // Mouse
    vec_builder.values().append_value(0.9);
    vec_builder.values().append_value(0.1);
    vec_builder.append(true);
    // Shampoo
    vec_builder.values().append_value(0.0);
    vec_builder.values().append_value(1.0);
    vec_builder.append(true);

    let batch = RecordBatch::try_new(
        prod_schema,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(prod_lbl, 0).as_u64(),
                Vid::new(prod_lbl, 1).as_u64(),
                Vid::new(prod_lbl, 2).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 3].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false, false, false])),
            Arc::new(UInt64Array::from(vec![1, 1, 1])),
            Arc::new(vec_builder.finish()), // embedding
            Arc::new(StringArray::from(vec!["Laptop", "Mouse", "Shampoo"])), // name
        ],
    )?;
    prod_ds.write_batch(batch, WriteMode::Overwrite).await?;

    // Edge: Alice -> VIEWED -> Laptop
    // Adjacency for User -> Product
    let adj_ds = storage.adjacency_dataset("VIEWED", "User", "fwd")?;
    let mut n_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());
    let mut e_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());

    // Alice (0) -> [Laptop(0)]
    n_builder
        .values()
        .append_value(Vid::new(prod_lbl, 0).as_u64());
    n_builder.append(true);
    e_builder
        .values()
        .append_value(Eid::new(viewed_type, 0).as_u64());
    e_builder.append(true);

    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(user_lbl, 0).as_u64()])),
            Arc::new(n_builder.finish()),
            Arc::new(e_builder.finish()),
        ],
    )?;
    adj_ds.write_chunk(batch, WriteMode::Overwrite).await?;

    // 3. Execution Logic
    // Step A: Find products Alice viewed
    let sql = "MATCH (u:User)-[:VIEWED]->(p:Product) RETURN p.embedding";
    let mut parser = CypherParser::new(sql)?;
    let ast = parser.parse()?;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(ast)?;
    let executor = Executor::new(storage.clone());
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;
    assert_eq!(results.len(), 1);

    // Get embedding from result
    let embedding_val = results[0].get("p.embedding").unwrap();
    let embedding: Vec<f32> = serde_json::from_value(embedding_val.clone())?;

    // Step B: Vector Search using that embedding
    // Find top 2 (should be Laptop itself and Mouse)
    let similar = storage
        .vector_search("Product", "embedding", &embedding, 2)
        .await?;

    assert_eq!(similar.len(), 2);
    // 0 = Laptop (dist 0), 1 = Mouse (dist small)
    // Vid(prod_lbl, 0) and Vid(prod_lbl, 1)
    let vid_laptop = Vid::new(prod_lbl, 0);
    let vid_mouse = Vid::new(prod_lbl, 1);

    let found_vids: Vec<Vid> = similar.iter().map(|(v, _)| *v).collect();
    assert!(found_vids.contains(&vid_laptop));
    assert!(found_vids.contains(&vid_mouse));

    Ok(())
}

#[tokio::test]
async fn test_document_knowledge_graph() -> anyhow::Result<()> {
    // Scenario: Document (Paper) -> Graph (CITES) -> Document.
    // JSON Index on $.topic
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let paper_lbl = schema_manager.add_label("Paper", true)?; // is_document
    let cites_type =
        schema_manager.add_edge_type("CITES", vec!["Paper".into()], vec!["Paper".into()])?;
    schema_manager.add_json_index("Paper", "$.topic", DataType::String)?;

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Insert Papers
    // 1 (AI)
    // 2 (DB)
    // 3 (AI)
    storage
        .insert_document("Paper", Vid::new(paper_lbl, 0), json!({"topic": "AI"}))
        .await?;
    storage
        .insert_document("Paper", Vid::new(paper_lbl, 1), json!({"topic": "DB"}))
        .await?;
    storage
        .insert_document("Paper", Vid::new(paper_lbl, 2), json!({"topic": "AI"}))
        .await?;

    // Edge: 1 -> CITES -> 3
    let adj_ds = storage.adjacency_dataset("CITES", "Paper", "fwd")?;
    // Write adjacency for Paper 0 -> [Paper 2]
    let mut n_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());
    let mut e_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());

    n_builder
        .values()
        .append_value(Vid::new(paper_lbl, 2).as_u64());
    n_builder.append(true);
    e_builder
        .values()
        .append_value(Eid::new(cites_type, 0).as_u64());
    e_builder.append(true);

    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(paper_lbl, 0).as_u64()])),
            Arc::new(n_builder.finish()),
            Arc::new(e_builder.finish()),
        ],
    )?;
    adj_ds.write_chunk(batch, WriteMode::Overwrite).await?;

    // Logic: Find "AI" papers using index, then traverse.
    // 1. Index lookup
    let index = storage.json_index("Paper", "$.topic")?;
    let ai_papers = index.get_vids("AI").await?;
    assert_eq!(ai_papers.len(), 2); // 0 and 2

    // 2. Traverse from Paper 0
    let vid_0 = Vid::new(paper_lbl, 0);
    // Load subgraph 1 hop outgoing
    let graph = storage
        .load_subgraph(
            &[vid_0],
            &[cites_type],
            1,
            uni::runtime::Direction::Outgoing,
            None,
        )
        .await?;

    // Check neighbor
    let mut neighbors = Vec::new();
    for e in graph.edges() {
        neighbors.push(e.dst_vid);
    }

    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0], Vid::new(paper_lbl, 2));

    Ok(())
}

#[tokio::test]
async fn test_identity_provenance() -> anyhow::Result<()> {
    // Scenario: Resolve UID -> VID, then traverse
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let node_lbl = schema_manager.add_label("Node", false)?;
    let derived_type =
        schema_manager.add_edge_type("DERIVED_FROM", vec!["Node".into()], vec!["Node".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Node A (VID 0) -> UID A
    // Node B (VID 1) -> UID B
    // B -> DERIVED_FROM -> A

    // UIDs
    let uid_bytes_a = [0xA; 32];
    let uid_a = UniId::from_bytes(uid_bytes_a);

    let uid_bytes_b = [0xB; 32];
    let uid_b = UniId::from_bytes(uid_bytes_b);

    // Insert mappings
    storage
        .insert_vertex_with_uid("Node", Vid::new(node_lbl, 0), uid_a)
        .await?;
    storage
        .insert_vertex_with_uid("Node", Vid::new(node_lbl, 1), uid_b)
        .await?;

    // Create Adjacency B -> A
    let adj_ds = storage.adjacency_dataset("DERIVED_FROM", "Node", "fwd")?;
    let mut n_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());
    let mut e_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());

    n_builder
        .values()
        .append_value(Vid::new(node_lbl, 0).as_u64());
    n_builder.append(true); // Row 0 (VID B?? No, adjacency dataset rows are sorted by src_vid.
    e_builder
        .values()
        .append_value(Eid::new(derived_type, 0).as_u64());
    e_builder.append(true);

    // We need to write row for VID 1 (B).
    // Adjacency Chunk format: [src_vid, neighbors, eids].
    // We just write one row for B.
    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(node_lbl, 1).as_u64()])),
            Arc::new(n_builder.finish()),
            Arc::new(e_builder.finish()),
        ],
    )?;
    adj_ds.write_chunk(batch, WriteMode::Overwrite).await?;

    // Logic:
    // 1. Resolve UID B
    let vid_b_resolved = storage
        .get_vertex_by_uid(&uid_b, "Node")
        .await?
        .expect("UID B not found");
    assert_eq!(vid_b_resolved, Vid::new(node_lbl, 1));

    // 2. Load subgraph
    let graph = storage
        .load_subgraph(
            &[vid_b_resolved],
            &[derived_type],
            1,
            uni::runtime::Direction::Outgoing,
            None,
        )
        .await?;

    // 3. Verify edge to A
    let edges: Vec<_> = graph.edges().collect();
    assert_eq!(edges.len(), 1);

    let target_vid = edges[0].dst_vid;

    assert_eq!(target_vid, Vid::new(node_lbl, 0));

    Ok(())
}
