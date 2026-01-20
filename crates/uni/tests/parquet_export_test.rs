// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::RecordBatch;
use lance::dataset::WriteMode;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::query::parser::CypherParser;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_parquet_export() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let export_path = path.join("export.parquet");

    // 1. Setup Data
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let label_id = schema_manager.add_label("Person", false)?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int64, false)?;
    schema_manager.save().await?;

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Insert 2 Persons
    let ds = storage.vertex_dataset("Person")?;
    let schema = ds.get_arrow_schema(&schema_manager.schema())?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::UInt64Array::from(vec![
                Vid::new(label_id, 1).as_u64(),
                Vid::new(label_id, 2).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 64].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false, false])),
            Arc::new(arrow_array::UInt64Array::from(vec![1, 1])),
            Arc::new(arrow_array::Int64Array::from(vec![30, 25])), // age
            Arc::new(arrow_array::StringArray::from(vec!["Alice", "Bob"])), // name
        ],
    )?;
    ds.write_batch(batch, WriteMode::Overwrite).await?;

    // 2. Execute COPY TO Parquet
    // Syntax: COPY Label TO 'file' WITH {format: 'parquet'}
    let query = format!(
        "COPY Person TO '{}' WITH {{format: 'parquet'}}",
        export_path.to_str().unwrap()
    );

    let mut parser = CypherParser::new(&query)?;
    let ast = parser.parse()?;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(ast)?;
    let executor = Executor::new(storage.clone());
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let result = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await;

    // This should fail currently or just do nothing/CSV if logic is missing
    assert!(result.is_ok(), "Export failed: {:?}", result.err());

    // 3. Verify File Exists and Content
    assert!(export_path.exists(), "Parquet file not created");

    let file = std::fs::File::open(&export_path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let batch = reader.next().unwrap()?;

    assert_eq!(batch.num_rows(), 2);

    // Check columns
    // _vid, age, name (sorted alphabetically usually? No, schema order or insertion order?)
    // In execute_export, we get keys from schema properties.

    // Let's just check if we can read "name"
    let name_col = batch.column_by_name("name").expect("name column missing");
    let names = name_col
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();

    let mut name_vals: Vec<&str> = names.iter().map(|s| s.unwrap()).collect();
    name_vals.sort();
    assert_eq!(name_vals, vec!["Alice", "Bob"]);

    Ok(())
}
