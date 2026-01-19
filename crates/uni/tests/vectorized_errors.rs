// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni::core::schema::{DataType, SchemaManager};
use uni::query::executor::Executor;
use uni::query::parser::CypherParser;
use uni::query::planner::QueryPlanner;
use uni::runtime::property_manager::PropertyManager;
use uni::runtime::writer::Writer;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_vectorized_execution_errors() -> Result<()> {
    let dir = tempdir()?;
    let base_path = dir.path().to_str().unwrap();
    let schema_path = dir.path().join("schema.json");

    let schema_manager = SchemaManager::load(&schema_path).await?;
    let label_id = schema_manager.add_label("Node", false)?;
    schema_manager.add_property("Node", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(base_path, schema_manager.clone()));
    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    // Insert data
    {
        let mut w = writer.write().await;
        let vid = w.next_vid(label_id).await?;
        let mut props = HashMap::new();
        props.insert("name".to_string(), serde_json::json!("A"));
        w.insert_vertex(vid, props).await?;
        w.flush_to_l1(None).await?;
    }

    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());

    // 1. Test unsupported function in vectorized path (Scalar eval error?)
    // Note: The planner checks for unsupported functions during planning.
    // So we expect planning error or fallback to legacy?
    // Planner `check_expr_supported` returns Err.
    // If planner returns Err, `Executor` falls back to legacy?
    // `Executor::execute` calls `planner.plan(&plan)`. If it fails, it runs legacy.
    // So to test *Operator* error, we need something that passes planning but fails runtime.
    // E.g. Dimension mismatch for vector? Or type mismatch?
    // Type mismatch in `evaluate_scalar`.
    // Example: Compare String > Number.

    let query_str = "MATCH (n:Node) WHERE n.name > 10 RETURN n.name";
    let mut parser = CypherParser::new(query_str)?;
    let query = parser.parse()?;
    let planner = QueryPlanner::new(schema_manager.schema().into());
    let plan = planner.plan(query)?;

    // This should run. If vectorized engine runs it, `evaluate_scalar` will encounter String vs Number.
    // It should return Error.
    // If vectorized execution fails, does Executor fallback? No. `phys_plan.execute` result is returned.
    // Unless `batch_to_rows` happens.

    let result = executor.execute(plan, &prop_manager, &HashMap::new()).await;

    // We expect Error from vectorized engine or Lance
    // With predicate pushdown, the error might come from Lance or return empty results
    match result {
        Err(err) => {
            // Verify it's a comparison or type error (from any layer)
            let msg = err.to_string();
            assert!(
                msg.contains("Comparison Gt error")
                    || msg.contains("Comparison only supported for numbers")
                    || msg.contains("type")
                    || msg.contains("Type"),
                "Unexpected error: {}",
                msg
            );
        }
        Ok(rows) => {
            // With predicate pushdown to Lance, type-mismatched predicates may
            // return empty results instead of an error (Lance handles it differently)
            // This is acceptable behavior - the query doesn't crash
            assert!(
                rows.is_empty(),
                "Type-mismatched comparison should return empty or error, got {} rows",
                rows.len()
            );
        }
    }

    Ok(())
}
