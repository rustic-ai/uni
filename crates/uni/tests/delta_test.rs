// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::{Eid, Vid};
use uni::core::schema::SchemaManager;
use uni::storage::delta::{DeltaDataset, L1Entry, Op};

#[tokio::test]
async fn test_delta_operations() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();

    // Setup Schema
    let schema_manager = SchemaManager::load(&dir.path().join("schema.json")).await?;
    let _ = schema_manager.add_label("Person", false)?;
    let _ = schema_manager.add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema = Arc::new(schema_manager.schema().clone());

    // Create DeltaDataset
    let delta = DeltaDataset::new(path, "KNOWS", "fwd");

    // Create ops
    let vid1 = Vid::new(1, 1);
    let vid2 = Vid::new(1, 2);
    let eid1 = Eid::new(1, 1);

    let op1 = L1Entry {
        src_vid: vid1,
        dst_vid: vid2,
        eid: eid1,
        op: Op::Insert,
        version: 1,
        properties: HashMap::new(),
    };

    let op2 = L1Entry {
        src_vid: vid1,
        dst_vid: vid2,
        eid: eid1,
        op: Op::Delete,
        version: 2,
        properties: HashMap::new(),
    };

    // Write deltas
    let batch = delta.build_record_batch(&[op1.clone(), op2.clone()], &schema)?;
    delta.write_run(batch).await?;

    // Read deltas for vid1
    let results = delta.read_deltas(vid1, &schema, None).await?;

    assert_eq!(results.len(), 2);

    // Verify first op
    assert_eq!(results[0].src_vid, vid1);
    assert_eq!(results[0].dst_vid, vid2);
    assert_eq!(results[0].eid, eid1);
    // Lance usually returns in insertion order if not sorted.
    // Let's check versions.

    let v1 = results
        .iter()
        .find(|r| r.version == 1)
        .expect("Version 1 not found");
    assert_eq!(v1.op, Op::Insert);

    let v2 = results
        .iter()
        .find(|r| r.version == 2)
        .expect("Version 2 not found");
    assert_eq!(v2.op, Op::Delete);

    // Test with non-existent VID
    let vid3 = Vid::new(1, 3);
    let empty_results = delta.read_deltas(vid3, &schema, None).await?;
    assert!(empty_results.is_empty());

    Ok(())
}
