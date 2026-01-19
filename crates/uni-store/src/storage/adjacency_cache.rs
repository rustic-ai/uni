// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::l0::L0Buffer;
use crate::storage::csr::CompressedSparseRow;
use crate::storage::manager::StorageManager;
use dashmap::DashMap;
use metrics;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uni_common::core::id::{Eid, Vid};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::Outgoing => "fwd",
            Direction::Incoming => "bwd",
            Direction::Both => "both",
        }
    }
}

pub struct AdjacencyCache {
    /// CSR for each (edge_type, direction, src_label_id) triple
    csr_maps: DashMap<(u16, Direction, u16), Arc<CompressedSparseRow>>,
    /// Current memory usage
    current_bytes: AtomicUsize,
}

impl AdjacencyCache {
    pub fn new(_max_bytes: usize) -> Self {
        Self {
            csr_maps: DashMap::new(),
            current_bytes: AtomicUsize::new(0),
        }
    }

    /// Warm cache from Storage (L2 + L1) for a specific edge type and direction.
    /// This rebuilds the CSR from scratch.
    pub async fn warm(
        &self,
        storage: &StorageManager,
        edge_type_id: u16,
        direction: Direction,
        src_label_id: u16,
        version: Option<u64>,
    ) -> anyhow::Result<()> {
        let schema = storage.schema_manager().schema();

        let edge_type_name = schema
            .edge_type_name_by_id(edge_type_id)
            .ok_or_else(|| anyhow::anyhow!("Edge type {} not found", edge_type_id))?;

        let src_label_name = schema
            .label_name_by_id(src_label_id)
            .ok_or_else(|| anyhow::anyhow!("Label {} not found", src_label_id))?;

        let mut entries = Vec::new();
        let mut deleted_eids = std::collections::HashSet::new();

        // Determine which directions to read
        let directions_to_read = match direction {
            Direction::Outgoing => vec![(Direction::Outgoing, "fwd")],
            Direction::Incoming => vec![(Direction::Incoming, "bwd")],
            Direction::Both => vec![(Direction::Outgoing, "fwd"), (Direction::Incoming, "bwd")],
        };

        for (read_dir, dir_str) in directions_to_read {
            // 1. Read L2 (Adjacency Dataset)
            let adj_ds = storage.adjacency_dataset(edge_type_name, src_label_name, dir_str);

            // L2 might not exist for one direction if not flushed yet
            if let Ok(adj_ds) = adj_ds
                && let Ok(ds) = adj_ds.open_at(version).await
            {
                use arrow_array::{ListArray, UInt64Array};
                use futures::TryStreamExt;

                let mut stream = ds.scan().try_into_stream().await?;
                while let Some(batch) = stream.try_next().await? {
                    let src_col = batch
                        .column_by_name("src_vid")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    let neighbors_list = batch
                        .column_by_name("neighbors")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();
                    let eids_list = batch
                        .column_by_name("edge_ids")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();

                    for i in 0..batch.num_rows() {
                        let src_u64 = src_col.value(i);
                        let src_vid = Vid::from(src_u64);

                        // For L2, we assume src_vid matches our requested label if we opened the correct dataset.
                        // But for "Both", we are combining.
                        // If we read "bwd", src_vid is the destination.
                        // In L2, "bwd" dataset is partitioned/named by dest label?
                        // Yes, adjacency_dataset(edge, label, dir).
                        // So we are reading the correct partition.

                        let src_offset = src_vid.local_offset();

                        let neighbors_array_ref = neighbors_list.value(i);
                        let neighbors = neighbors_array_ref
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();

                        let eids_array_ref = eids_list.value(i);
                        let eids = eids_array_ref
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();

                        for j in 0..neighbors.len() {
                            entries.push((
                                src_offset,
                                Vid::from(neighbors.value(j)),
                                Eid::from(eids.value(j)),
                            ));
                        }
                    }
                }
            }

            // 2. Read L1 (Delta)
            let delta_ds = storage.delta_dataset(edge_type_name, dir_str)?;

            if let Ok(ds) = delta_ds.open_at(version).await {
                use arrow_array::{UInt8Array, UInt64Array};
                use futures::TryStreamExt;
                let mut stream = ds.scan().try_into_stream().await?;
                while let Some(batch) = stream.try_next().await? {
                    let src_col = batch
                        .column_by_name("src_vid")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    let dst_col = batch
                        .column_by_name("dst_vid")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    let eid_col = batch
                        .column_by_name("eid")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    let op_col = batch
                        .column_by_name("op")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap();
                    let version_col = batch
                        .column_by_name("_version")
                        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>().cloned());

                    for i in 0..batch.num_rows() {
                        let mut src_vid = Vid::from(src_col.value(i));
                        let mut dst_vid = Vid::from(dst_col.value(i));

                        if read_dir == Direction::Incoming {
                            std::mem::swap(&mut src_vid, &mut dst_vid);
                        }

                        if src_vid.label_id() != src_label_id {
                            continue;
                        }
                        let eid = Eid::from(eid_col.value(i));
                        let op = op_col.value(i); // 0=Insert, 1=Delete
                        let row_version = version_col.as_ref().map(|c| c.value(i)).unwrap_or(0);

                        if op == 0 {
                            entries.push((src_vid.local_offset(), dst_vid, eid));
                        } else {
                            deleted_eids.insert(eid);
                        }
                        let _ = row_version;
                    }
                }
            }
        }

        // Filter out deleted edges from entries
        if !deleted_eids.is_empty() {
            entries.retain(|(_, _, eid)| !deleted_eids.contains(eid));
        }

        // Determine max offset
        let max_offset = entries.iter().map(|(o, _, _)| *o).max().unwrap_or(0);

        // Build CSR
        let csr = CompressedSparseRow::new(max_offset as usize, entries);

        // Update stats
        let size = csr.memory_usage();
        self.current_bytes.fetch_add(size, Ordering::Relaxed);

        // Store
        self.csr_maps
            .insert((edge_type_id, direction, src_label_id), Arc::new(csr));

        Ok(())
    }

    /// Get neighbors with O(1) lookup
    pub fn get_neighbors(
        &self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
    ) -> Option<Arc<CompressedSparseRow>> {
        // Returns the CSR arc if available. Caller must query it.
        let res = self
            .csr_maps
            .get(&(edge_type, direction, vid.label_id()))
            .map(|r| r.value().clone());

        if res.is_some() {
            metrics::counter!("uni_adjacency_cache_hits_total").increment(1);
        } else {
            metrics::counter!("uni_adjacency_cache_misses_total").increment(1);
        }
        res
    }

    pub fn get_csr(
        &self,
        edge_type: u16,
        direction: Direction,
        label_id: u16,
    ) -> Option<Arc<CompressedSparseRow>> {
        self.csr_maps
            .get(&(edge_type, direction, label_id))
            .map(|r| r.value().clone())
    }

    /// Merge with L0 buffer for uncommitted edges
    pub fn get_neighbors_with_l0<'a>(
        &'a self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
        l0: Option<&'a L0Buffer>,
    ) -> Vec<(Vid, Eid)> {
        let mut neighbors_map: HashMap<Eid, Vid> = HashMap::new();

        // 1. Get from CSR
        if let Some(csr) = self.get_neighbors(vid, edge_type, direction) {
            let (n, e) = csr.get_neighbors(vid);
            for (&neighbor, &eid) in n.iter().zip(e.iter()) {
                neighbors_map.insert(eid, neighbor);
            }
        }

        // 2. Overlay L0
        if let Some(l0) = l0 {
            self.overlay_l0_neighbors(vid, edge_type, direction, l0, &mut neighbors_map);
        }

        neighbors_map.into_iter().map(|(e, n)| (n, e)).collect()
    }

    /// Merge with multiple L0 buffers for uncommitted edges.
    /// L0s should be provided oldest-first so newer writes win.
    pub fn get_neighbors_with_l0s<'a>(
        &'a self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
        l0: Option<&'a L0Buffer>,
        pending_l0s: &[&'a L0Buffer],
    ) -> Vec<(Vid, Eid)> {
        let mut neighbors_map: HashMap<Eid, Vid> = HashMap::new();

        // 1. Get from CSR
        if let Some(csr) = self.get_neighbors(vid, edge_type, direction) {
            let (n, e) = csr.get_neighbors(vid);
            for (&neighbor, &eid) in n.iter().zip(e.iter()) {
                neighbors_map.insert(eid, neighbor);
            }
        }

        // 2. Overlay pending L0s (oldest first)
        for pending_l0 in pending_l0s {
            self.overlay_l0_neighbors(vid, edge_type, direction, pending_l0, &mut neighbors_map);
        }

        // 3. Overlay current L0 (newest, wins)
        if let Some(l0) = l0 {
            self.overlay_l0_neighbors(vid, edge_type, direction, l0, &mut neighbors_map);
        }

        neighbors_map.into_iter().map(|(e, n)| (n, e)).collect()
    }

    pub fn overlay_l0_neighbors<'a>(
        &'a self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
        l0: &'a L0Buffer,
        neighbors_map: &mut HashMap<Eid, Vid>,
    ) {
        // Apply L0 inserts - convert cache Direction to simple_graph Direction
        match direction {
            Direction::Outgoing => {
                let l0_neighbors = l0.get_neighbors(
                    vid,
                    edge_type,
                    uni_common::graph::simple_graph::Direction::Outgoing,
                );
                for (neighbor, eid, _ver) in l0_neighbors {
                    neighbors_map.insert(eid, neighbor);
                }
            }
            Direction::Incoming => {
                let l0_neighbors = l0.get_neighbors(
                    vid,
                    edge_type,
                    uni_common::graph::simple_graph::Direction::Incoming,
                );
                for (neighbor, eid, _ver) in l0_neighbors {
                    neighbors_map.insert(eid, neighbor);
                }
            }
            Direction::Both => {
                // For Both, overlay both Outgoing and Incoming from L0
                let out = l0.get_neighbors(
                    vid,
                    edge_type,
                    uni_common::graph::simple_graph::Direction::Outgoing,
                );
                for (neighbor, eid, _ver) in out {
                    neighbors_map.insert(eid, neighbor);
                }
                let inc = l0.get_neighbors(
                    vid,
                    edge_type,
                    uni_common::graph::simple_graph::Direction::Incoming,
                );
                for (neighbor, eid, _ver) in inc {
                    neighbors_map.insert(eid, neighbor);
                }
            }
        };

        // Apply L0 Tombstones
        for eid in l0.tombstones.keys() {
            neighbors_map.remove(eid);
        }
    }

    pub fn overlay_l0_neighbors_with_type<'a>(
        &'a self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
        l0: &'a L0Buffer,
        neighbors_map: &mut HashMap<Eid, (Vid, u16)>,
    ) {
        // Apply L0 inserts - convert cache Direction to simple_graph Direction
        match direction {
            Direction::Outgoing => {
                let l0_neighbors = l0.get_neighbors(
                    vid,
                    edge_type,
                    uni_common::graph::simple_graph::Direction::Outgoing,
                );
                for (neighbor, eid, _ver) in l0_neighbors {
                    neighbors_map.insert(eid, (neighbor, edge_type));
                }
            }
            Direction::Incoming => {
                let l0_neighbors = l0.get_neighbors(
                    vid,
                    edge_type,
                    uni_common::graph::simple_graph::Direction::Incoming,
                );
                for (neighbor, eid, _ver) in l0_neighbors {
                    neighbors_map.insert(eid, (neighbor, edge_type));
                }
            }
            Direction::Both => {
                // For Both, overlay both Outgoing and Incoming from L0
                let out = l0.get_neighbors(
                    vid,
                    edge_type,
                    uni_common::graph::simple_graph::Direction::Outgoing,
                );
                for (neighbor, eid, _ver) in out {
                    neighbors_map.insert(eid, (neighbor, edge_type));
                }
                let inc = l0.get_neighbors(
                    vid,
                    edge_type,
                    uni_common::graph::simple_graph::Direction::Incoming,
                );
                for (neighbor, eid, _ver) in inc {
                    neighbors_map.insert(eid, (neighbor, edge_type));
                }
            }
        };

        // Apply L0 Tombstones
        for eid in l0.tombstones.keys() {
            neighbors_map.remove(eid);
        }
    }

    /// Invalidate on flush
    pub fn invalidate(&self, edge_type: u16) {
        // Remove both directions for ALL labels
        // DashMap doesn't support easy bulk removal by pattern.
        // We have to iterate and remove.
        self.csr_maps
            .retain(|(k_edge, _, _), _| *k_edge != edge_type);
    }
}
