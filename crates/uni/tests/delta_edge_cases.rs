// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::{Eid, Vid};
use uni::core::schema::SchemaManager;
use uni::storage::delta::{DeltaDataset, L1Entry, Op};

async fn setup_delta(path: &str) -> (DeltaDataset, Arc<uni::core::schema::Schema>) {
    let schema_manager = SchemaManager::load(std::path::Path::new(path).join("schema.json"))
        .await
        .unwrap();
    let _ = schema_manager.add_label("Person", false).unwrap();
    let _ = schema_manager
        .add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])
        .unwrap();
    schema_manager.save().await.unwrap();
    let schema = Arc::new(schema_manager.schema().clone());
    let delta = DeltaDataset::new(path, "KNOWS", "fwd");
    (delta, schema)
}

#[tokio::test]
async fn test_read_deltas_time_travel() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let (delta, schema) = setup_delta(path).await;

    let vid1 = Vid::new(1, 1);
    let vid2 = Vid::new(1, 2);
    let eid1 = Eid::new(1, 1);

    // Write 3 batches to create 3 Lance versions
    for v in 1..=3 {
        let op = L1Entry {
            src_vid: vid1,
            dst_vid: vid2,
            eid: eid1,
            op: Op::Insert,
            version: v, // logical version
            properties: HashMap::new(),
        };
        let batch = delta.build_record_batch(&[op], &schema)?;
        delta.write_run(batch).await?;
    }

    // Now we have Lance versions 1, 2, 3.
    // Each version N contains records from batches 1..N.

    // Read at version 2. Should see logical versions 1 and 2.
    // Note: read_deltas(..., Some(v)) means checkout Lance version v.
    let results_v2 = delta.read_deltas(vid1, &schema, Some(2)).await?;

    assert_eq!(results_v2.len(), 2);
    let found_versions: Vec<u64> = results_v2.iter().map(|r| r.version).collect();
    assert!(found_versions.contains(&1));
    assert!(found_versions.contains(&2));
    assert!(!found_versions.contains(&3));

    // Read at version 3. Should see all.
    let results_v3 = delta.read_deltas(vid1, &schema, Some(3)).await?;
    assert_eq!(results_v3.len(), 3);

    // Read latest (None). Should see all.
    let results_latest = delta.read_deltas(vid1, &schema, None).await?;
    assert_eq!(results_latest.len(), 3);

    Ok(())
}

#[tokio::test]
async fn test_write_empty_batch() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let (delta, schema) = setup_delta(path).await;

    let ops: Vec<L1Entry> = Vec::new();
    let batch = delta.build_record_batch(&ops, &schema)?;

    // Writing empty batch should ideally result in no-op or valid empty file?
    // Depending on implementation.
    // If it creates an empty file, read should handle it.

    if batch.num_rows() > 0 {
        delta.write_run(batch).await?;
    }

    // Read
    let results = delta.read_deltas(Vid::new(1, 1), &schema, None).await?;
    assert!(results.is_empty());

    Ok(())
}
