// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Test for 100k property lookup bug

use serde_json::json;
use std::collections::HashMap;
use uni_common::core::id::Vid;

const SCHEMA_JSON: &str = r#"{
    "schema_version": 1,
    "labels": {
        "Person": {
            "id": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "state": "Active",
            "is_document": false,
            "json_indexes": []
        }
    },
    "edge_types": {
        "KNOWS": {
            "id": 1,
            "src_labels": ["Person"],
            "dst_labels": ["Person"],
            "state": "Active"
        }
    },
    "properties": {
        "Person": {
            "name": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" },
            "age": { "type": "Int32", "nullable": true, "added_in": 1, "state": "Active" },
            "embedding": { "type": { "Vector": { "dimensions": 128 } }, "nullable": true, "added_in": 1, "state": "Active" }
        }
    },
    "indexes": []
}"#;

#[tokio::test]
async fn test_100k_property_lookup() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path();

    let schema_path = path.join("schema.json");
    tokio::fs::write(&schema_path, SCHEMA_JSON).await.unwrap();

    let db = uni_db::Uni::open(path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    db.load_schema(&schema_path).await.unwrap();

    // Create 10k vertices to test (smaller scale for faster debugging)
    let num_vertices = 10_000;
    let batch_size = 5_000;

    eprintln!(
        "Creating {} vertices in batches of {}",
        num_vertices, batch_size
    );

    let mut all_vids: Vec<Vid> = Vec::new();

    for batch_start in (0..num_vertices).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_vertices);
        let mut props = Vec::new();
        for i in batch_start..batch_end {
            let embedding: Vec<f32> = (0..128).map(|x| (x + i) as f32).collect();
            let mut p: HashMap<String, serde_json::Value> = HashMap::new();
            p.insert("name".to_string(), json!(format!("Person_{}", i)));
            p.insert("age".to_string(), json!((i % 100) as i32));
            p.insert("embedding".to_string(), json!(embedding));
            props.push(p);
        }
        let vids = db.bulk_insert_vertices("Person", props).await.unwrap();
        all_vids.extend(vids);
        eprintln!("  Batch {} - {} created", batch_start, batch_end);
    }

    eprintln!("Created {} vertices total", all_vids.len());

    // Check properties BEFORE creating edges
    let pre_edge_sample = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 3")
        .await
        .unwrap();
    eprintln!("BEFORE EDGES - Sample names: {:?}", pre_edge_sample.rows);

    eprintln!("Now creating edges...");

    // Create 30k edges (same ratio as benchmark: 3 edges per vertex)
    let num_edges = 30_000usize;
    let edge_batch_size = 10_000usize;

    for batch_start in (0..num_edges).step_by(edge_batch_size) {
        let batch_end = (batch_start + edge_batch_size).min(num_edges);
        let edges: Vec<(Vid, Vid, HashMap<String, serde_json::Value>)> = (batch_start..batch_end)
            .map(|i| {
                let src = all_vids[i % all_vids.len()];
                let dst = all_vids[(i * 7 + 13) % all_vids.len()]; // pseudo-random dest
                (src, dst, HashMap::new())
            })
            .collect();
        db.bulk_insert_edges("KNOWS", edges).await.unwrap();
        eprintln!("  Edge batch {} - {} created", batch_start, batch_end);
    }

    // Check properties AFTER edges, BEFORE flush
    let post_edge_sample = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 3")
        .await
        .unwrap();
    eprintln!(
        "AFTER EDGES, BEFORE FLUSH - Sample names: {:?}",
        post_edge_sample.rows
    );

    eprintln!("Flushing...");
    db.flush().await.unwrap();

    // Check properties AFTER flush
    let post_flush_sample = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 3")
        .await
        .unwrap();
    eprintln!("AFTER FLUSH - Sample names: {:?}", post_flush_sample.rows);

    // Test count
    let count = db
        .query("MATCH (n:Person) RETURN count(n) as cnt")
        .await
        .unwrap();
    eprintln!("Total count: {:?}", count.rows.first());

    // Diagnostic: Check if properties are accessible at all
    let sample = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 5")
        .await
        .unwrap();
    eprintln!("Sample names (LIMIT 5): {:?}", sample.rows);

    // Check if Person_0 exists without filter
    let all_names = db
        .query("MATCH (n:Person) WHERE n.name IS NOT NULL RETURN n.name")
        .await
        .unwrap();
    eprintln!("Total rows with name: {}", all_names.rows.len());

    // Check if we can find Person_0 in the returned names
    let has_person_0 = all_names.rows.iter().any(|row| {
        row.values
            .iter()
            .any(|v| matches!(v, uni_db::Value::String(s) if s == "Person_0"))
    });
    eprintln!("Person_0 exists in full scan: {}", has_person_0);

    // Try a simple equality filter without the edge pattern
    let simple_lookup = db
        .query("MATCH (n:Person) WHERE n.name = 'Person_0' RETURN n.name")
        .await
        .unwrap();
    eprintln!("Simple lookup Person_0: {} rows", simple_lookup.rows.len());

    // Test lookups at different positions
    for target in [0, 100, 1000, 5000, 9999] {
        let query = format!(
            "MATCH (n:Person) WHERE n.name = 'Person_{}' RETURN n.name",
            target
        );
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.rows.len(), 1, "Expected 1 row for Person_{}", target);
    }
}
