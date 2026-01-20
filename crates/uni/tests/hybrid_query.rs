// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{
    FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder, ListBuilder, UInt64Builder,
};
use arrow_array::{RecordBatch, StringArray, UInt64Array};
use lance::dataset::WriteMode;
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
async fn test_hybrid_vector_graph_query() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let paper_label_id = schema_manager.add_label("Paper", false)?;
    let author_label_id = schema_manager.add_label("Author", false)?;
    let wrote_edge_id =
        schema_manager.add_edge_type("WROTE", vec!["Author".into()], vec!["Paper".into()])?;

    schema_manager.add_property(
        "Paper",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.add_property("Paper", "title", DataType::String, false)?;
    schema_manager.add_property("Author", "name", DataType::String, false)?;

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()));
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 1000);

    // 2. Insert Data: Papers
    {
        let dataset = storage.vertex_dataset("Paper")?;
        let schema = dataset.get_arrow_schema(&schema_manager.schema())?;

        let p1 = Vid::new(paper_label_id, 0); // Offset 0
        let p2 = Vid::new(paper_label_id, 1); // Offset 1
        let vids = UInt64Array::from(vec![p1.as_u64(), p2.as_u64()]);
        let versions = UInt64Array::from(vec![1, 1]);
        let deleted = arrow_array::BooleanArray::from(vec![false, false]);

        // UIDs
        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        for _ in 0..2 {
            uid_builder.append_value([0u8; 32]).unwrap();
        }
        let uids = uid_builder.finish();

        // Vectors
        let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        // Paper 1: [0.1, 0.1]
        vector_builder.values().append_value(0.1);
        vector_builder.values().append_value(0.1);
        vector_builder.append(true);
        // Paper 2: [0.9, 0.9]
        vector_builder.values().append_value(0.9);
        vector_builder.values().append_value(0.9);
        vector_builder.append(true);
        let vectors = vector_builder.finish();

        // Titles
        let titles = StringArray::from(vec!["Vector DBs", "Cooking"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(vids),
                Arc::new(uids),
                Arc::new(deleted),
                Arc::new(versions),
                Arc::new(vectors),
                Arc::new(titles),
            ],
        )?;
        dataset.write_batch(batch, WriteMode::Overwrite).await?;
    }

    // 3. Insert Data: Authors
    {
        let dataset = storage.vertex_dataset("Author")?;
        let schema = dataset.get_arrow_schema(&schema_manager.schema())?;

        let a1 = Vid::new(author_label_id, 0); // Offset 0
        let vids = UInt64Array::from(vec![a1.as_u64()]);
        let versions = UInt64Array::from(vec![1]);
        let deleted = arrow_array::BooleanArray::from(vec![false]);

        // UIDs
        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        uid_builder.append_value([0u8; 32]).unwrap();
        let uids = uid_builder.finish();

        // Names
        let names = StringArray::from(vec!["Alice"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(vids),
                Arc::new(uids),
                Arc::new(deleted),
                Arc::new(versions),
                Arc::new(names),
            ],
        )?;
        dataset.write_batch(batch, WriteMode::Overwrite).await?;
    }

    // 4. Insert Edge: Alice (Author:0) -> WROTE -> Vector DBs (Paper:0)
    {
        let src_vid = Vid::new(author_label_id, 0);
        let dst_vid = Vid::new(paper_label_id, 0);
        let eid = Eid::new(wrote_edge_id, 1);

        // Forward: Author -> Paper
        {
            let adj_ds = storage.adjacency_dataset("WROTE", "Author", "fwd")?;
            let schema = adj_ds.get_arrow_schema();

            let src_vids = UInt64Array::from(vec![src_vid.as_u64()]);

            let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
            neighbors_builder.values().append_value(dst_vid.as_u64());
            neighbors_builder.append(true);
            let neighbors = neighbors_builder.finish();

            let mut eids_builder = ListBuilder::new(UInt64Builder::new());
            eids_builder.values().append_value(eid.as_u64());
            eids_builder.append(true);
            let eids = eids_builder.finish();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(src_vids), Arc::new(neighbors), Arc::new(eids)],
            )?;

            adj_ds.write_chunk(batch, WriteMode::Overwrite).await?;
        }

        // Backward: Paper -> Author
        {
            let adj_ds = storage.adjacency_dataset("WROTE", "Paper", "bwd")?;
            let schema = adj_ds.get_arrow_schema();

            let src_vids = UInt64Array::from(vec![dst_vid.as_u64()]); // Source is Paper

            let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
            neighbors_builder.values().append_value(src_vid.as_u64()); // Neighbor is Author
            neighbors_builder.append(true);
            let neighbors = neighbors_builder.finish();

            let mut eids_builder = ListBuilder::new(UInt64Builder::new());
            eids_builder.values().append_value(eid.as_u64());
            eids_builder.append(true);
            let eids = eids_builder.finish();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(src_vids), Arc::new(neighbors), Arc::new(eids)],
            )?;

            adj_ds.write_chunk(batch, WriteMode::Overwrite).await?;
        }
    }

    // 5. Run Query
    // We must start MATCH with the bound variable 'p' for the current planner to pick it up without Scan.
    let query_sql = "
        CALL db.idx.vector.query('Paper', 'embedding', [0.1, 0.1], 1) YIELD p, dist
        MATCH (p)<-[:WROTE]-(a:Author)
        RETURN a.name, p.title, dist
    ";

    let mut parser = CypherParser::new(query_sql)?;
    let query = parser.parse()?;

    let planner = QueryPlanner::new(schema_manager.schema_arc());
    let plan = planner.plan(query)?;

    let executor = Executor::new(storage.clone());
    let results = executor
        .execute(plan, &prop_manager, &std::collections::HashMap::new())
        .await?;

    // 6. Verify
    assert_eq!(results.len(), 1);
    let row = &results[0];

    let name = row.get("a.name").unwrap().as_str().unwrap();
    assert_eq!(name, "Alice");

    let title = row.get("p.title").unwrap().as_str().unwrap();
    assert_eq!(title, "Vector DBs");

    let dist = row.get("dist").unwrap().as_f64().unwrap();
    assert!(dist < 0.001);

    Ok(())
}

trait SchemaManagerExt {
    fn schema_arc(&self) -> Arc<uni_db::core::schema::Schema>;
}
impl SchemaManagerExt for Arc<SchemaManager> {
    fn schema_arc(&self) -> Arc<uni_db::core::schema::Schema> {
        // This is a hack because QueryPlanner needs Arc<Schema> and SchemaManager has &Schema.
        // But wait, QueryPlanner takes Arc<Schema>.
        // SchemaManager::schema() returns &Schema.
        // I need to clone the schema to get Arc<Schema> or Planner needs to change.
        // Planner: pub fn new(schema: Arc<Schema>)
        // Schema is Clone.
        Arc::new(self.schema().clone())
    }
}
