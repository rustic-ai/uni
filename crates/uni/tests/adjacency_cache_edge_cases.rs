// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::{Eid, Vid};
use uni::core::schema::SchemaManager;
use uni::runtime::l0::L0Buffer;
use uni::storage::adjacency_cache::{AdjacencyCache, Direction};
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_cache_miss_fallback() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Node", false)?;
    let type_id = schema_manager.add_edge_type("REL", vec!["Node".into()], vec!["Node".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // Write edge to storage directly (bypass L0 to test cache loading from storage)
    // We need to write to DeltaDataset.
    let delta = storage.delta_dataset("REL", "fwd")?;
    let schema = schema_manager.schema();

    use uni::storage::delta::{L1Entry, Op};
    let op = L1Entry {
        src_vid: Vid::new(1, 0),
        dst_vid: Vid::new(1, 1),
        eid: Eid::new(type_id, 0),
        op: Op::Insert,
        version: 1,
        properties: Default::default(),
    };

    let batch = delta.build_record_batch(&[op], &schema)?;
    delta.write_run(batch).await?;

    let cache = AdjacencyCache::new(1000);

    // Warm cache
    cache
        .warm(&storage, type_id, Direction::Outgoing, 1, Some(1))
        .await?;

    // Check CSR
    let csr = cache
        .get_csr(type_id, Direction::Outgoing, 1)
        .expect("CSR missing");
    let neighbors = csr.get_neighbors(Vid::new(1, 0));
    assert_eq!(neighbors.0.len(), 1);
    assert_eq!(neighbors.0[0], Vid::new(1, 1));

    Ok(())
}

#[tokio::test]
async fn test_overlay_l0_neighbors() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Node", false)?;
    let type_id = schema_manager.add_edge_type("REL", vec!["Node".into()], vec!["Node".into()])?;
    schema_manager.save().await?;

    let cache = AdjacencyCache::new(1000);
    // Empty cache (no storage data)

    // Create L0 buffer with edge
    let mut l0 = L0Buffer::new(1, None);
    let src = Vid::new(1, 0);
    let dst = Vid::new(1, 1);
    let eid = Eid::new(type_id, 0);
    let _ = l0.insert_edge(src, dst, type_id, eid, Default::default());

    let l0 = RwLock::new(l0);
    let l0_guard = l0.read();

    let mut neighbors = HashMap::new();
    cache.overlay_l0_neighbors(src, type_id, Direction::Outgoing, &l0_guard, &mut neighbors);

    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[&eid], dst);

    Ok(())
}
