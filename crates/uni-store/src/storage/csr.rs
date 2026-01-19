// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_common::core::id::{Eid, Vid};

/// Compressed Sparse Row (CSR) representation of adjacency
///
/// Optimized for:
/// 1. Low memory footprint (offsets are u32)
/// 2. O(1) neighbor lookup
/// 3. CPU cache locality (contiguous memory)
pub struct CompressedSparseRow {
    /// Offset into neighbors for vertex i: offsets[i]..offsets[i+1]
    /// We map Vid::local_offset() to index in this array.
    /// Note: This assumes Vid offsets are somewhat dense or we handle sparsity.
    /// For now, we'll assume we might need a map if sparse, or just a large vec if dense.
    /// Given Lance uses sequential offsets, a Vec is likely fine but might be sparse.
    ///
    /// To support sparse VIDs efficiently, we might need a `dense_id_map` or `roaring` bitmap?
    /// For MVP/Phase 1, let's assume we can allocate up to max_vid.
    offsets: Vec<u32>,

    /// Flattened neighbor list
    neighbors: Vec<Vid>,

    /// Edge IDs parallel to neighbors
    edge_ids: Vec<Eid>,
}

impl CompressedSparseRow {
    pub fn new(max_vid_offset: usize, entries: Vec<(u64, Vid, Eid)>) -> Self {
        // entries must be (src_offset, neighbor_vid, eid)
        // Sort by src_offset
        let mut sorted = entries;
        sorted.sort_by_key(|(src, _, _)| *src);

        let mut offsets = vec![0u32; max_vid_offset + 2];
        let mut neighbors = Vec::with_capacity(sorted.len());
        let mut edge_ids = Vec::with_capacity(sorted.len());

        let mut current_offset = 0;
        let mut last_src = 0;

        for (src, neighbor, eid) in sorted {
            let src_idx = src as usize;

            // Fill gaps
            if src_idx > last_src {
                for offset in offsets.iter_mut().take(src_idx + 1).skip(last_src + 1) {
                    *offset = current_offset;
                }
            }
            last_src = src_idx;

            neighbors.push(neighbor);
            edge_ids.push(eid);
            current_offset += 1;
        }

        // Fill remaining offsets
        for offset in offsets.iter_mut().skip(last_src + 1) {
            *offset = current_offset;
        }

        Self {
            offsets,
            neighbors,
            edge_ids,
        }
    }

    /// O(1) neighbor lookup
    pub fn get_neighbors(&self, vid: Vid) -> (&[Vid], &[Eid]) {
        let local = vid.local_offset() as usize;
        if local + 1 >= self.offsets.len() {
            return (&[], &[]);
        }

        let start = self.offsets[local] as usize;
        let end = self.offsets[local + 1] as usize;

        if start >= self.neighbors.len() || end > self.neighbors.len() {
            return (&[], &[]);
        }

        (&self.neighbors[start..end], &self.edge_ids[start..end])
    }

    pub fn memory_usage(&self) -> usize {
        self.offsets.len() * 4 + self.neighbors.len() * 8 + self.edge_ids.len() * 8
    }

    /// Iterate over all edges in the CSR.
    /// Returns iterator over (src_offset, dst_vid, eid).
    pub fn iter_all(&self) -> impl Iterator<Item = (u64, Vid, Eid)> + '_ {
        (0..self.offsets.len() - 1).flat_map(move |i| {
            let start = self.offsets[i] as usize;
            let end = self.offsets[i + 1] as usize;
            (start..end).map(move |j| (i as u64, self.neighbors[j], self.edge_ids[j]))
        })
    }
}
