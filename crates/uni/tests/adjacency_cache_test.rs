// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni::core::id::{Eid, Vid};
use uni::core::schema::SchemaManager;
use uni::runtime::writer::Writer;
use uni::storage::adjacency_cache::Direction;
use uni::storage::manager::StorageManager;

#[tokio::test]
async fn test_adjacency_cache_lifecycle() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person", false)?;
    let knows_type =
        schema_manager.add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(
        path.join("storage").to_str().unwrap(),
        schema_manager.clone(),
    ));

    // 2. Insert Base Data (L2) using Writer (which writes L0, then flush)
    // We want to test Cache Warming from L2.
    // Person 0 -> Person 1
    use uni::UniConfig;

    let cache_arc = Some(storage.adjacency_cache());
    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        UniConfig::default(),
        cache_arc,
        None,
        None,
    )
    .await
    .unwrap();

    let vid0 = Vid::new(person_lbl, 0);
    let vid1 = Vid::new(person_lbl, 1);
    let eid01 = Eid::new(knows_type, 0);

    writer
        .insert_edge(vid0, vid1, knows_type, eid01, HashMap::new())
        .await?;
    writer.flush_to_l1(None).await?; // This writes to L1 (Delta) and possibly invalidates (but cache is empty)

    // Manually write to Adjacency Dataset to simulate L2 compaction (or just rely on L1 reading)
    // The current Cache implementation reads from AdjacencyDataset (L2) AND DeltaDataset (L1).
    // So flushing to L1 is sufficient for "Storage" presence.

    // 3. Warm Cache
    // We access via StorageManager
    let cache = storage.adjacency_cache();
    let dir = Direction::Outgoing;

    // warm() is async
    cache
        .warm(&storage, knows_type, dir, person_lbl, None)
        .await?;

    // 4. Verify Cache Hit
    // Directly check CSR
    let csr = cache
        .get_csr(knows_type, dir, person_lbl)
        .expect("CSR should be loaded");
    let (neighbors, eids) = csr.get_neighbors(vid0);

    // Since we only flushed to L1, and `warm` reads L2 AND L1.
    // L1 reading in `warm` was implemented to append entries.
    // So we should see the neighbor.
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0], vid1);
    assert_eq!(eids[0], eid01);

    // 5. Test L0 Overlay
    // Insert Person 0 -> Person 2 in new L0
    let vid2 = Vid::new(person_lbl, 2);
    let eid02 = Eid::new(knows_type, 1);

    writer
        .insert_edge(vid0, vid2, knows_type, eid02, HashMap::new())
        .await?;

    // Get current L0
    let l0_arc = writer.l0_manager.get_current();

    // Get neighbors with L0 overlay
    {
        let l0 = l0_arc.read();
        let neighbors_combined = cache.get_neighbors_with_l0(vid0, knows_type, dir, Some(&l0));

        // Should have 2 neighbors: 1 (from Cache/L1) and 2 (from L0)
        assert_eq!(neighbors_combined.len(), 2);
        // Order might vary, check containment
        let n_vids: Vec<Vid> = neighbors_combined.iter().map(|(v, _)| *v).collect();
        assert!(n_vids.contains(&vid1));
        assert!(n_vids.contains(&vid2));
    }

    // 6. Test Invalidation
    // Flush again. Writer should invalidate cache.
    writer.flush_to_l1(None).await?;

    // Cache should be empty/invalidated for this type
    assert!(cache.get_csr(knows_type, dir, person_lbl).is_none());

    // 7. Warm again (now both edges should be in L1)
    cache
        .warm(&storage, knows_type, dir, person_lbl, None)
        .await?;
    let csr2 = cache
        .get_csr(knows_type, dir, person_lbl)
        .expect("CSR should be re-loaded");
    let (neighbors2, _) = csr2.get_neighbors(vid0);
    assert_eq!(neighbors2.len(), 2);

    Ok(())
}
