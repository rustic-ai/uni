// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph Projection - Dense CSR representation for algorithm execution.
//!
//! A `GraphProjection` is a materialized, algorithm-optimized view of a subgraph.
//! It provides:
//! - Dense vertex indexing (0..V) for efficient array-based state
//! - CSR format for cache-friendly neighbor iteration
//! - Optional reverse edges for algorithms like PageRank
//! - Optional edge weights for weighted algorithms

use crate::algo::IdMap;
use anyhow::{Result, anyhow};
use uni_common::core::id::Vid;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::adjacency_cache::{AdjacencyCache, Direction as CacheDir};
use uni_store::storage::manager::StorageManager;

/// Edge list for CSR construction: (source_slot, destination_slot, weight) pairs.
type WeightedEdgeList = Vec<(u32, u32, f64)>;

/// Configuration for building a graph projection.
#[derive(Debug, Clone, Default)]
pub struct ProjectionConfig {
    /// Node labels to include (empty = all)
    pub node_labels: Vec<String>,
    /// Edge types to include (empty = all)
    pub edge_types: Vec<String>,
    /// Property to use as edge weight
    pub weight_property: Option<String>,
    /// Whether to build reverse edges (in_neighbors)
    pub include_reverse: bool,
}

/// Dense CSR representation optimized for algorithm execution.
#[derive(Debug)]
pub struct GraphProjection {
    /// Number of vertices in the projection
    pub(crate) vertex_count: usize,

    /// Outbound edges: CSR format
    pub(crate) out_offsets: Vec<u32>, // [V+1] vertex slot -> edge start
    pub(crate) out_neighbors: Vec<u32>, // [E] neighbor slots

    /// Inbound edges: CSR format (optional, for PageRank/SCC)
    pub(crate) in_offsets: Vec<u32>, // [V+1]
    pub(crate) in_neighbors: Vec<u32>, // [E]

    /// Optional edge weights
    pub(crate) out_weights: Option<Vec<f64>>,

    /// Identity mapping
    pub(crate) id_map: IdMap,

    /// Metadata
    pub(crate) _node_labels: Vec<String>,
    pub(crate) _edge_types: Vec<String>,
}

impl GraphProjection {
    /// Number of vertices in the projection.
    #[inline]
    pub fn vertex_count(&self) -> usize {
        self.vertex_count
    }

    /// Number of edges in the projection.
    #[inline]
    pub fn edge_count(&self) -> usize {
        self.out_neighbors.len()
    }

    /// Outbound neighbors of a vertex (by slot).
    #[inline]
    pub fn out_neighbors(&self, slot: u32) -> &[u32] {
        let start = self.out_offsets[slot as usize] as usize;
        let end = self.out_offsets[slot as usize + 1] as usize;
        &self.out_neighbors[start..end]
    }

    /// Outbound degree of a vertex.
    #[inline]
    pub fn out_degree(&self, slot: u32) -> u32 {
        self.out_offsets[slot as usize + 1] - self.out_offsets[slot as usize]
    }

    /// Inbound neighbors of a vertex (by slot).
    ///
    /// Panics if projection was built without `include_reverse`.
    #[inline]
    pub fn in_neighbors(&self, slot: u32) -> &[u32] {
        let start = self.in_offsets[slot as usize] as usize;
        let end = self.in_offsets[slot as usize + 1] as usize;
        &self.in_neighbors[start..end]
    }

    /// Inbound degree of a vertex.
    #[inline]
    pub fn in_degree(&self, slot: u32) -> u32 {
        self.in_offsets[slot as usize + 1] - self.in_offsets[slot as usize]
    }

    /// Get edge weight for outbound edge.
    ///
    /// Panics if projection was built without weights.
    #[inline]
    pub fn out_weight(&self, slot: u32, edge_idx: usize) -> f64 {
        let start = self.out_offsets[slot as usize] as usize;
        self.out_weights.as_ref().expect("no weights")[start + edge_idx]
    }

    /// Check if weights are available.
    #[inline]
    pub fn has_weights(&self) -> bool {
        self.out_weights.is_some()
    }

    /// Check if reverse edges are available.
    #[inline]
    pub fn has_reverse(&self) -> bool {
        !self.in_neighbors.is_empty()
    }

    /// Map slot back to VID.
    #[inline]
    pub fn to_vid(&self, slot: u32) -> Vid {
        self.id_map.to_vid_unchecked(slot)
    }

    /// Map VID to slot.
    #[inline]
    pub fn to_slot(&self, vid: Vid) -> Option<u32> {
        self.id_map.to_slot(vid)
    }

    /// Iterate over all vertices as (slot, vid).
    pub fn vertices(&self) -> impl Iterator<Item = (u32, Vid)> + '_ {
        self.id_map.iter()
    }

    /// Memory usage in bytes.
    pub fn memory_size(&self) -> usize {
        self.out_offsets.len() * 4
            + self.out_neighbors.len() * 4
            + self.in_offsets.len() * 4
            + self.in_neighbors.len() * 4
            + self.out_weights.as_ref().map_or(0, |w| w.len() * 8)
            + self.id_map.memory_size()
    }
}

use std::sync::Arc;

/// Builder for constructing a `GraphProjection` from storage.
pub struct ProjectionBuilder<'a> {
    storage: Arc<StorageManager>,
    cache: &'a AdjacencyCache,
    l0: Option<Arc<parking_lot::RwLock<L0Buffer>>>,
    /// L0 buffers currently being flushed (still visible to reads).
    pending_flush_l0s: Vec<Arc<parking_lot::RwLock<L0Buffer>>>,
    config: ProjectionConfig,
}

impl<'a> ProjectionBuilder<'a> {
    /// Create a new projection builder.
    pub fn new(
        storage: Arc<StorageManager>,
        cache: &'a AdjacencyCache,
        l0: Option<Arc<parking_lot::RwLock<L0Buffer>>>,
    ) -> Self {
        Self {
            storage,
            cache,
            l0,
            pending_flush_l0s: Vec::new(),
            config: ProjectionConfig::default(),
        }
    }

    /// Set pending flush L0 buffers.
    pub fn pending_flush_l0s(
        mut self,
        l0s: Vec<std::sync::Arc<parking_lot::RwLock<L0Buffer>>>,
    ) -> Self {
        self.pending_flush_l0s = l0s;
        self
    }

    /// Set node labels to include.
    pub fn node_labels(mut self, labels: &[&str]) -> Self {
        self.config.node_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set edge types to include.
    pub fn edge_types(mut self, types: &[&str]) -> Self {
        self.config.edge_types = types.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set weight property.
    pub fn weight_property(mut self, prop: &str) -> Self {
        self.config.weight_property = Some(prop.to_string());
        self
    }

    /// Include reverse edges for in_neighbors access.
    pub fn include_reverse(mut self, enabled: bool) -> Self {
        self.config.include_reverse = enabled;
        self
    }

    /// Build the projection.
    pub async fn build(self) -> Result<GraphProjection> {
        let schema = self.storage.schema_manager().schema();

        // 1. Resolve label and edge type IDs
        let (label_ids, edge_type_ids) = self.resolve_ids(&schema)?;

        // 2. Warm cache for all requested edge types
        self.warm_caches(&label_ids, &edge_type_ids).await?;

        // 3. Collect VIDs from storage and L0
        let all_vids = self.collect_vertices(&schema, &label_ids).await?;

        let mut id_map = IdMap::with_capacity(all_vids.len());
        for vid in all_vids {
            id_map.insert(vid);
        }
        let vertex_count = id_map.len();

        // 4. Collect edges from cache
        let (out_edges, in_edges) = self.collect_edges(&id_map, &edge_type_ids).await?;

        // Compact IdMap (drops hash map, enables binary search)
        id_map.compact();

        let (out_offsets, out_neighbors, out_weights) = build_csr(vertex_count, &out_edges, true);
        let (in_offsets, in_neighbors, _) = if self.config.include_reverse {
            build_csr(vertex_count, &in_edges, false)
        } else {
            (vec![0; vertex_count + 1], Vec::new(), None)
        };

        Ok(GraphProjection {
            vertex_count,
            out_offsets,
            out_neighbors,
            in_offsets,
            in_neighbors,
            out_weights,
            id_map,
            _node_labels: self.config.node_labels,
            _edge_types: self.config.edge_types,
        })
    }

    /// Resolve label and edge type IDs from configuration.
    fn resolve_ids(
        &self,
        schema: &uni_common::core::schema::Schema,
    ) -> Result<(Vec<u16>, Vec<u16>)> {
        let mut label_ids = Vec::new();
        for label_name in &self.config.node_labels {
            let meta = schema
                .labels
                .get(label_name)
                .ok_or_else(|| anyhow!("Label {} not found", label_name))?;
            label_ids.push(meta.id);
        }

        let mut edge_type_ids = Vec::new();
        for type_name in &self.config.edge_types {
            let meta = schema
                .edge_types
                .get(type_name)
                .ok_or_else(|| anyhow!("Edge type {} not found", type_name))?;
            edge_type_ids.push(meta.id);
        }

        // If empty, include all from schema
        if label_ids.is_empty() {
            label_ids = schema.labels.values().map(|m| m.id).collect();
        }
        if edge_type_ids.is_empty() {
            edge_type_ids = schema.edge_types.values().map(|m| m.id).collect();
        }

        Ok((label_ids, edge_type_ids))
    }

    /// Warm adjacency cache for all requested edge types.
    async fn warm_caches(&self, label_ids: &[u16], edge_type_ids: &[u16]) -> Result<()> {
        for &type_id in edge_type_ids {
            let edge_ver = self.storage.get_edge_version_by_id(type_id);
            for &lid in label_ids {
                self.cache
                    .warm(&self.storage, type_id, CacheDir::Outgoing, lid, edge_ver)
                    .await?;
                if self.config.include_reverse {
                    self.cache
                        .warm(&self.storage, type_id, CacheDir::Incoming, lid, edge_ver)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Collect VIDs from storage and L0 buffers.
    async fn collect_vertices(
        &self,
        schema: &uni_common::core::schema::Schema,
        label_ids: &[u16],
    ) -> Result<Vec<Vid>> {
        use arrow_array::UInt64Array;
        use futures::StreamExt;

        let mut all_vids = Vec::new();

        // Scan storage for each label
        for &lid in label_ids {
            let label_name = schema.label_name_by_id(lid).unwrap();

            let ds = self.storage.vertex_dataset(label_name)?;
            if let Ok(lance_ds) = ds.open().await {
                let mut scanner = lance_ds.scan();
                scanner.project(&["_vid"])?;
                let mut stream = scanner.try_into_stream().await?;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    let vid_col = batch
                        .column_by_name("_vid")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        all_vids.push(Vid::from(vid_col.value(i)));
                    }
                }
            }
        }

        // Overlay pending flush L0 vertices (oldest first)
        for pending_l0_arc in &self.pending_flush_l0s {
            all_vids.extend(pending_l0_arc.read().vids_for_labels(label_ids));
        }

        // Overlay current L0 vertices
        if let Some(ref l0_arc) = self.l0 {
            all_vids.extend(l0_arc.read().vids_for_labels(label_ids));
        }

        // Sort and dedup to ensure IdMap is sorted for compaction
        all_vids.sort_unstable();
        all_vids.dedup();

        Ok(all_vids)
    }

    /// Collect edges from cache with L0 overlay.
    async fn collect_edges(
        &self,
        id_map: &IdMap,
        edge_type_ids: &[u16],
    ) -> Result<(WeightedEdgeList, WeightedEdgeList)> {
        // Phase 1: Collect topology (Hold Lock)
        let mut raw_out_edges = Vec::new(); // (src_slot, dst_vid, eid)
        let mut raw_in_edges = Vec::new();

        {
            let l0_guard = self.l0.as_ref().map(|l| l.read());
            let l0_ref = l0_guard.as_deref();
            let pending_guards: Vec<_> = self.pending_flush_l0s.iter().map(|l| l.read()).collect();
            let pending_refs: Vec<&L0Buffer> = pending_guards.iter().map(|g| &**g).collect();

            for (src_slot, src_vid) in id_map.iter() {
                for &type_id in edge_type_ids {
                    // Outbound
                    let neighbors = self.cache.get_neighbors_with_l0s(
                        src_vid,
                        type_id,
                        CacheDir::Outgoing,
                        l0_ref,
                        &pending_refs,
                    );
                    for (dst_vid, eid) in neighbors {
                        raw_out_edges.push((src_slot, dst_vid, eid));
                    }

                    // Inbound
                    if self.config.include_reverse {
                        let in_neighbors = self.cache.get_neighbors_with_l0s(
                            src_vid,
                            type_id,
                            CacheDir::Incoming,
                            l0_ref,
                            &pending_refs,
                        );
                        for (dst_vid, eid) in in_neighbors {
                            raw_in_edges.push((src_slot, dst_vid, eid));
                        }
                    }
                }
            }
        } // Drop locks

        // Phase 2: Fetch weights and map destination slots (Async, No Lock)
        let pm = if self.config.weight_property.is_some() {
            Some(PropertyManager::new(
                self.storage.clone(),
                self.storage.schema_manager_arc(),
                1000,
            ))
        } else {
            None
        };
        let weight_prop = self.config.weight_property.as_deref();

        let mut out_edges = Vec::with_capacity(raw_out_edges.len());
        for (src_slot, dst_vid, eid) in raw_out_edges {
            if let Some(dst_slot) = id_map.to_slot(dst_vid) {
                let weight = if let Some(pm) = &pm {
                    if let Some(prop) = weight_prop {
                        let val = pm.get_edge_prop(eid, prop, None).await?;
                        val.as_f64().unwrap_or(1.0)
                    } else {
                        1.0
                    }
                } else {
                    1.0
                };
                out_edges.push((src_slot, dst_slot, weight));
            }
        }

        let mut in_edges = Vec::with_capacity(raw_in_edges.len());
        for (src_slot, dst_vid, eid) in raw_in_edges {
            if let Some(dst_slot) = id_map.to_slot(dst_vid) {
                let weight = if let Some(pm) = &pm {
                    if let Some(prop) = weight_prop {
                        let val = pm.get_edge_prop(eid, prop, None).await?;
                        val.as_f64().unwrap_or(1.0)
                    } else {
                        1.0
                    }
                } else {
                    1.0
                };
                in_edges.push((src_slot, dst_slot, weight));
            }
        }

        Ok((out_edges, in_edges))
    }
}

/// Build CSR from edge list.
fn build_csr(
    vertex_count: usize,
    edges: &[(u32, u32, f64)],
    include_weights: bool,
) -> (Vec<u32>, Vec<u32>, Option<Vec<f64>>) {
    if vertex_count == 0 {
        return (vec![0], Vec::new(), None);
    }

    // Count degrees
    let mut degrees = vec![0u32; vertex_count];
    for &(src, _, _) in edges {
        degrees[src as usize] += 1;
    }

    // Build offsets (prefix sum)
    let mut offsets = vec![0u32; vertex_count + 1];
    for i in 0..vertex_count {
        offsets[i + 1] = offsets[i] + degrees[i];
    }

    // Fill neighbors
    let mut neighbors = vec![0u32; edges.len()];
    let mut weights = if include_weights {
        Some(vec![0.0; edges.len()])
    } else {
        None
    };
    let mut current = offsets.clone();

    for &(src, dst, w) in edges {
        let idx = current[src as usize] as usize;
        neighbors[idx] = dst;
        if let Some(ws) = &mut weights {
            ws[idx] = w;
        }
        current[src as usize] += 1;
    }

    (offsets, neighbors, weights)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_csr() {
        // Triangle: 0 -> 1, 1 -> 2, 2 -> 0
        let edges = vec![(0, 1, 1.0), (1, 2, 1.0), (2, 0, 1.0), (0, 2, 0.5)];
        let (offsets, neighbors, weights) = build_csr(3, &edges, true);

        assert_eq!(offsets, vec![0, 2, 3, 4]);
        // Node 0 has edges to 1 and 2
        assert_eq!(&neighbors[0..2], &[1, 2]);
        if let Some(w) = weights {
            assert_eq!(&w[0..2], &[1.0, 0.5]);
        }
        // Node 1 has edge to 2
        assert_eq!(&neighbors[2..3], &[2]);
        // Node 2 has edge to 0
        assert_eq!(&neighbors[3..4], &[0]);
    }
}
