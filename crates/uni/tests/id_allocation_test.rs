// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::sync::Arc;
use tempfile::tempdir;
use uni::runtime::id_allocator::IdAllocator;

#[tokio::test]
async fn test_id_allocation_persistence_and_restart() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let path = ObjectStorePath::from("id_allocator.json");
    let batch_size = 10;

    // --- Phase 1: First Run ---
    {
        let allocator = IdAllocator::new(store.clone(), path.clone(), batch_size).await?;

        // Allocate first VID for label 1
        let vid1 = allocator.allocate_vid(1).await?;
        assert_eq!(vid1.label_id(), 1);
        assert_eq!(vid1.local_offset(), 0);

        // Allocate second VID for label 1
        let vid2 = allocator.allocate_vid(1).await?;
        assert_eq!(vid2.label_id(), 1);
        assert_eq!(vid2.local_offset(), 1);

        // Allocate first EID for type 5
        let eid1 = allocator.allocate_eid(5).await?;
        assert_eq!(eid1.type_id(), 5);
        assert_eq!(eid1.local_offset(), 0);

        // Verify manifest file was created via object store listing?
        // Or just trust subsequent restart
    }

    // --- Phase 2: Restart (Simulate Crash/Reload) ---
    {
        // Initialize new allocator pointing to same file
        let allocator = IdAllocator::new(store.clone(), path.clone(), batch_size).await?;

        // The new allocator initializes 'current' counters from the manifest values.
        // Manifest had 10. So next VID should be 10 (skipping 2..9).
        let vid_restart = allocator.allocate_vid(1).await?;
        assert_eq!(vid_restart.label_id(), 1);
        assert_eq!(
            vid_restart.local_offset(),
            10,
            "Should skip to next batch start on restart"
        );

        // Allocating again should be sequential within the new batch
        let vid_next = allocator.allocate_vid(1).await?;
        assert_eq!(vid_next.local_offset(), 11);

        // Check EID behavior too
        let eid_restart = allocator.allocate_eid(5).await?;
        assert_eq!(eid_restart.type_id(), 5);
        assert_eq!(
            eid_restart.local_offset(),
            10,
            "Should skip to next batch start for EID"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_multiple_labels_independence() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let path = ObjectStorePath::from("mixed_allocator.json");
    let allocator = IdAllocator::new(store, path, 100).await?;

    // Label 1
    let v1 = allocator.allocate_vid(1).await?;
    assert_eq!(v1.local_offset(), 0);

    // Label 2 (Should start at 0 independently)
    let v2 = allocator.allocate_vid(2).await?;
    assert_eq!(v2.local_offset(), 0);
    assert_eq!(v2.label_id(), 2);

    // Label 1 again (Should be 1)
    let v3 = allocator.allocate_vid(1).await?;
    assert_eq!(v3.local_offset(), 1);

    Ok(())
}
