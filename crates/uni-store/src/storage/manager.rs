// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::compaction::{CompactionStats, CompactionStatus, CompactionTask};
use crate::runtime::WorkingGraph;
use crate::runtime::l0::L0Buffer;
use crate::storage::adjacency::AdjacencyDataset;
use crate::storage::delta::{DeltaDataset, Op};
use crate::storage::edge::EdgeDataset;
use crate::storage::index::UidIndex;
use crate::storage::inverted_index::InvertedIndex;
use crate::storage::json_index::JsonPathIndex;
use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use arrow_array::{Float32Array, UInt64Array};
use dashmap::DashMap;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance_linalg::distance::MetricType;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use uni_common::config::UniConfig;
use uni_common::core::id::{Eid, UniId, Vid};
use uni_common::core::schema::{DistanceMetric, IndexDefinition, SchemaManager};
use uni_common::sync::acquire_mutex;

use crate::snapshot::manager::SnapshotManager;
use crate::storage::IndexManager;
use crate::storage::adjacency_cache::AdjacencyCache;
use crate::storage::resilient_store::ResilientObjectStore;

use uni_common::core::snapshot::SnapshotManifest;

use uni_common::graph::simple_graph::Direction as GraphDirection;

/// Edge state during subgraph loading - tracks version and deletion status.
struct EdgeState {
    neighbor: Vid,
    version: u64,
    deleted: bool,
}

pub struct StorageManager {
    base_uri: String,
    #[allow(dead_code)]
    store: Arc<dyn ObjectStore>,
    schema_manager: Arc<SchemaManager>,
    snapshot_manager: Arc<SnapshotManager>,
    adjacency_cache: Arc<AdjacencyCache>,
    /// Cache of opened datasets by label name for vector search performance
    dataset_cache: DashMap<String, Arc<Dataset>>,
    pub config: UniConfig,
    pub compaction_status: Arc<Mutex<CompactionStatus>>,
    /// Optional pinned snapshot for time-travel
    pinned_snapshot: Option<SnapshotManifest>,
}

/// Helper to manage compaction_in_progress flag
struct CompactionGuard {
    status: Arc<Mutex<CompactionStatus>>,
}

impl CompactionGuard {
    fn new(status: Arc<Mutex<CompactionStatus>>) -> Option<Self> {
        let mut s = acquire_mutex(&status, "compaction_status").ok()?;
        if s.compaction_in_progress {
            return None;
        }
        s.compaction_in_progress = true;
        Some(Self {
            status: status.clone(),
        })
    }
}

impl Drop for CompactionGuard {
    fn drop(&mut self) {
        let mut s = self
            .status
            .lock()
            .expect("Compaction status lock poisoned - a thread panicked while holding it");
        s.compaction_in_progress = false;
        s.last_compaction = Some(std::time::SystemTime::now());
    }
}

impl StorageManager {
    pub fn new(base_uri: &str, schema_manager: Arc<SchemaManager>) -> Self {
        Self::new_with_config(base_uri, schema_manager, UniConfig::default())
    }

    pub fn new_with_cache(
        base_uri: &str,
        schema_manager: Arc<SchemaManager>,
        adjacency_cache_size: usize,
    ) -> Self {
        let config = UniConfig {
            cache_size: adjacency_cache_size,
            ..Default::default()
        };
        Self::new_with_config(base_uri, schema_manager, config)
    }

    pub fn new_with_config(
        base_uri: &str,
        schema_manager: Arc<SchemaManager>,
        config: UniConfig,
    ) -> Self {
        let store: Arc<dyn ObjectStore> = if base_uri.contains("://") {
            let (store, _path) =
                object_store::parse_url(&url::Url::parse(base_uri).expect("Invalid base URI"))
                    .expect("Failed to parse object store URL");
            Arc::from(store)
        } else {
            // If local path, ensure it exists
            std::fs::create_dir_all(base_uri).ok();
            Arc::new(
                LocalFileSystem::new_with_prefix(base_uri).expect("Failed to create local storage"),
            )
        };

        let resilient_store: Arc<dyn ObjectStore> = Arc::new(ResilientObjectStore::new(
            store,
            config.object_store.clone(),
        ));

        let snapshot_manager = Arc::new(SnapshotManager::new(resilient_store.clone()));
        Self {
            base_uri: base_uri.to_string(),
            store: resilient_store,
            schema_manager,
            snapshot_manager,
            adjacency_cache: Arc::new(AdjacencyCache::new(config.cache_size)),
            dataset_cache: DashMap::new(),
            config,
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            pinned_snapshot: None,
        }
    }

    pub fn pinned(&self, snapshot: SnapshotManifest) -> Self {
        Self {
            base_uri: self.base_uri.clone(),
            store: self.store.clone(),
            schema_manager: self.schema_manager.clone(),
            snapshot_manager: self.snapshot_manager.clone(),
            adjacency_cache: Arc::new(AdjacencyCache::new(self.config.cache_size)), // New cache for pinned view?
            dataset_cache: DashMap::new(),
            config: self.config.clone(),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            pinned_snapshot: Some(snapshot),
        }
    }

    pub fn get_edge_version_by_id(&self, edge_type_id: u16) -> Option<u64> {
        let schema = self.schema_manager.schema();
        let name = schema.edge_type_name_by_id(edge_type_id)?;
        self.pinned_snapshot
            .as_ref()
            .and_then(|s| s.edges.get(name).map(|es| es.lance_version))
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub fn compaction_status(&self) -> CompactionStatus {
        self.compaction_status
            .lock()
            .expect("Compaction status lock poisoned - a thread panicked while holding it")
            .clone()
    }

    pub async fn compact(&self) -> Result<CompactionStats> {
        // Manual compaction triggers unconditional full compaction
        self.execute_compaction(CompactionTask::ByRunCount).await // Treat manual as "just do it"
    }

    pub async fn compact_label(&self, label: &str) -> Result<CompactionStats> {
        let _guard = CompactionGuard::new(self.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let ds = self.vertex_dataset(label)?;
        let mut files_compacted = 0;
        let mut bytes_before = 0;

        if let Ok(mut dataset) = ds.open_raw().await {
            let before = dataset.count_rows(None).await? as u64;
            bytes_before += before;

            compact_files(&mut dataset, CompactionOptions::default(), None).await?;

            files_compacted += 1;
            self.invalidate_dataset_cache(label);
        }

        Ok(CompactionStats {
            files_compacted,
            bytes_before,
            bytes_after: 0, // optimize returns metrics but we need to map them. For now 0.
            duration: std::time::Duration::from_secs(0),
            crdt_merges: 0,
        })
    }

    pub async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats> {
        let _guard = CompactionGuard::new(self.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let mut files_compacted = 0;
        let mut bytes_before = 0;

        for dir in ["fwd", "bwd"] {
            let ds = self.delta_dataset(edge_type, dir)?;
            if let Ok(mut dataset) = ds.open_latest_raw().await {
                let before = dataset.count_rows(None).await? as u64;
                bytes_before += before;

                compact_files(&mut dataset, CompactionOptions::default(), None).await?;

                files_compacted += 1;
            }
        }

        if let Some(_cache) = &self.dataset_cache.get(edge_type) {
            // Invalidate edge cache if we cache edge datasets (we don't currently cache deltas in DashMap, but check logic)
            // StorageManager::dataset_cache seems to be for Vertex datasets only (key is label).
        }
        // But we DO cache adjacency in AdjacencyCache
        // Does compaction invalidate adjacency cache?
        // AdjacencyCache reads from L1/L2. If L1 changes (compacted), the cache might be stale if it holds direct file offsets?
        // Uni's AdjacencyCache holds `CachedAdjacency` which is `Arc<Vec<...>>`. It's data, not file pointers.
        // So it is valid until next read. Compaction doesn't change DATA content, just layout.
        // So safe to keep.

        Ok(CompactionStats {
            files_compacted,
            bytes_before,
            bytes_after: 0,
            duration: std::time::Duration::from_secs(0),
            crdt_merges: 0,
        })
    }

    pub async fn wait_for_compaction(&self) -> Result<()> {
        loop {
            let in_progress = {
                acquire_mutex(&self.compaction_status, "compaction_status")?.compaction_in_progress
            };
            if !in_progress {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    pub fn start_background_compaction(self: Arc<Self>) {
        if !self.config.compaction.enabled {
            return;
        }

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.config.compaction.check_interval);
            loop {
                interval.tick().await;

                if let Err(e) = self.update_compaction_status().await {
                    log::error!("Failed to update compaction status: {}", e);
                    continue;
                }

                if let Some(task) = self.pick_compaction_task() {
                    log::info!("Triggering background compaction: {:?}", task);
                    if let Err(e) = self.execute_compaction(task).await {
                        log::error!("Compaction failed: {}", e);
                    }
                }
            }
        });
    }

    async fn update_compaction_status(&self) -> Result<()> {
        let schema = self.schema_manager.schema();
        let mut total_fragments = 0;
        let total_size = 0;

        // Iterate Edge Delta Datasets
        for name in schema.edge_types.keys() {
            for dir in ["fwd", "bwd"] {
                if let Ok(ds) = self.delta_dataset(name, dir) {
                    // Try to open to check fragments
                    // This might be expensive if many edge types.
                    // For now, we rely on cached metadata or lightweight check if possible.
                    // Lance doesn't expose lightweight meta without opening.
                    // We can check the directory maybe?
                    // Let's rely on opening for now.
                    if let Ok(dataset) = ds.open_latest().await {
                        total_fragments += dataset.count_fragments();
                        // Approx size?
                        // total_size += ...
                    }
                }
            }
        }

        // TODO: Vertex Datasets

        let mut status = acquire_mutex(&self.compaction_status, "compaction_status")?;
        status.l1_runs = total_fragments;
        status.l1_size_bytes = total_size;
        Ok(())
    }

    fn pick_compaction_task(&self) -> Option<CompactionTask> {
        let status = acquire_mutex(&self.compaction_status, "compaction_status").ok()?;

        if status.l1_runs >= self.config.compaction.max_l1_runs {
            return Some(CompactionTask::ByRunCount);
        }
        if status.l1_size_bytes >= self.config.compaction.max_l1_size_bytes {
            return Some(CompactionTask::BySize);
        }
        // TODO: Age check

        None
    }

    async fn execute_compaction(&self, _task: CompactionTask) -> Result<CompactionStats> {
        let start = std::time::Instant::now();
        // Use guard for automatic flag management
        let _guard = CompactionGuard::new(self.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let schema = self.schema_manager.schema();
        let mut files_compacted = 0;
        let mut bytes_before = 0;
        let bytes_after = 0;

        // Naive implementation: Compact ALL edge delta datasets
        // Real implementation should target specific datasets based on fragmentation

        for name in schema.edge_types.keys() {
            for dir in ["fwd", "bwd"] {
                let ds = self.delta_dataset(name, dir)?;
                if let Ok(mut dataset) = ds.open_latest_raw().await
                    && dataset.count_fragments() > 1
                {
                    let before = dataset.count_rows(None).await? as u64; // Approx check
                    bytes_before += before; // Use rows as proxy for now

                    // Compact!
                    compact_files(&mut dataset, CompactionOptions::default(), None).await?;

                    files_compacted += 1;
                }
            }
        }

        // Compact Vertices too?
        for label in schema.labels.keys() {
            let ds = self.vertex_dataset(label)?;
            if let Ok(mut dataset) = ds.open_raw().await
                && dataset.count_fragments() > 1
            {
                let before = dataset.count_rows(None).await? as u64;
                bytes_before += before;

                compact_files(&mut dataset, CompactionOptions::default(), None).await?;
                files_compacted += 1;
                self.invalidate_dataset_cache(label);
            }
        }

        {
            let mut status = acquire_mutex(&self.compaction_status, "compaction_status")?;
            status.total_compactions += 1;
        }

        Ok(CompactionStats {
            files_compacted,
            bytes_before,
            bytes_after,
            duration: start.elapsed(),
            crdt_merges: 0,
        })
    }

    /// Get or open a cached dataset for a label
    pub async fn get_cached_dataset(&self, label: &str) -> Result<Arc<Dataset>> {
        // Check cache first
        if let Some(ds) = self.dataset_cache.get(label) {
            return Ok(ds.clone());
        }

        // Open and cache
        let dataset = self.vertex_dataset(label)?;
        let ds = if let Some(snap) = &self.pinned_snapshot {
            let lance_ver = snap
                .vertices
                .get(label)
                .map(|s| s.lance_version)
                .unwrap_or(0);
            dataset.open_at(Some(lance_ver)).await?
        } else {
            dataset.open().await?
        };

        self.dataset_cache.insert(label.to_string(), ds.clone());
        Ok(ds)
    }

    /// Invalidate cached dataset (call after writes)
    pub fn invalidate_dataset_cache(&self, label: &str) {
        self.dataset_cache.remove(label);
    }

    /// Clear all cached datasets
    pub fn clear_dataset_cache(&self) {
        self.dataset_cache.clear();
    }

    pub fn base_path(&self) -> &str {
        &self.base_uri
    }

    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    pub fn schema_manager_arc(&self) -> Arc<SchemaManager> {
        self.schema_manager.clone()
    }

    pub fn adjacency_cache(&self) -> Arc<AdjacencyCache> {
        Arc::clone(&self.adjacency_cache)
    }

    pub async fn load_subgraph_cached(
        &self,
        start_vids: &[Vid],
        edge_types: &[u16],
        max_hops: usize,
        direction: GraphDirection,
        l0: Option<Arc<RwLock<L0Buffer>>>,
    ) -> Result<WorkingGraph> {
        let mut graph = WorkingGraph::new();
        let schema = self.schema_manager.schema();

        // Build maps for ID lookups
        let edge_type_map: HashMap<u16, String> = schema
            .edge_types
            .values()
            .map(|meta| {
                (
                    meta.id,
                    schema.edge_type_name_by_id(meta.id).unwrap().to_owned(),
                )
            })
            .collect();

        // Initialize frontier
        let mut frontier: Vec<Vid> = start_vids.to_vec();
        let mut visited: HashSet<Vid> = HashSet::new();

        // Initialize start vids
        for &vid in start_vids {
            graph.add_vertex(vid);
        }

        for _hop in 0..max_hops {
            let mut next_frontier = HashSet::new();

            for &vid in &frontier {
                if visited.contains(&vid) {
                    continue;
                }
                visited.insert(vid);
                graph.add_vertex(vid);

                for &etype_id in edge_types {
                    // Mapping direction
                    let cache_dir = match direction {
                        GraphDirection::Outgoing => {
                            crate::storage::adjacency_cache::Direction::Outgoing
                        }
                        GraphDirection::Incoming => {
                            crate::storage::adjacency_cache::Direction::Incoming
                        }
                    };

                    let neighbor_is_dst = match direction {
                        GraphDirection::Outgoing => true,
                        GraphDirection::Incoming => false,
                    };

                    // Check/Warm Cache
                    let etype_name = edge_type_map
                        .get(&etype_id)
                        .ok_or_else(|| anyhow!("Unknown edge type ID: {}", etype_id))?;

                    let edge_ver = self
                        .pinned_snapshot
                        .as_ref()
                        .and_then(|s| s.edges.get(etype_name).map(|es| es.lance_version));

                    if self
                        .adjacency_cache
                        .get_csr(etype_id, cache_dir, vid.label_id())
                        .is_none()
                    {
                        // Warm cache (this loads full adj for this edge type/direction/label)
                        self.adjacency_cache
                            .warm(self, etype_id, cache_dir, vid.label_id(), edge_ver)
                            .await?;
                    }

                    // Get neighbors from cache + L0
                    // We lock L0 here if available
                    let edges = if let Some(l0_arc) = &l0 {
                        let l0_guard = l0_arc.read();
                        self.adjacency_cache.get_neighbors_with_l0(
                            vid,
                            etype_id,
                            cache_dir,
                            Some(&l0_guard),
                        )
                    } else {
                        self.adjacency_cache
                            .get_neighbors_with_l0(vid, etype_id, cache_dir, None)
                    };

                    for (neighbor_vid, eid) in edges {
                        graph.add_vertex(neighbor_vid);
                        if !visited.contains(&neighbor_vid) {
                            next_frontier.insert(neighbor_vid);
                        }

                        if neighbor_is_dst {
                            graph.add_edge(vid, neighbor_vid, eid, etype_id);
                        } else {
                            graph.add_edge(neighbor_vid, vid, eid, etype_id);
                        }
                    }
                }
            }
            frontier = next_frontier.into_iter().collect();
        }

        Ok(graph)
    }

    pub fn snapshot_manager(&self) -> &SnapshotManager {
        &self.snapshot_manager
    }

    pub fn index_manager(&self) -> IndexManager {
        IndexManager::new(&self.base_uri, self.schema_manager.clone())
    }

    pub fn vertex_dataset(&self, label: &str) -> Result<VertexDataset> {
        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;
        Ok(VertexDataset::new(&self.base_uri, label, label_meta.id))
    }

    pub fn edge_dataset(
        &self,
        edge_type: &str,
        src_label: &str,
        dst_label: &str,
    ) -> Result<EdgeDataset> {
        Ok(EdgeDataset::new(
            &self.base_uri,
            edge_type,
            src_label,
            dst_label,
        ))
    }

    pub fn delta_dataset(&self, edge_type: &str, direction: &str) -> Result<DeltaDataset> {
        Ok(DeltaDataset::new(&self.base_uri, edge_type, direction))
    }

    pub fn adjacency_dataset(
        &self,
        edge_type: &str,
        label: &str,
        direction: &str,
    ) -> Result<AdjacencyDataset> {
        Ok(AdjacencyDataset::new(
            &self.base_uri,
            edge_type,
            label,
            direction,
        ))
    }

    pub fn uid_index(&self, label: &str) -> Result<UidIndex> {
        Ok(UidIndex::new(&self.base_uri, label))
    }

    pub fn json_index(&self, label: &str, path: &str) -> Result<JsonPathIndex> {
        Ok(JsonPathIndex::new(&self.base_uri, label, path))
    }

    pub async fn inverted_index(&self, label: &str, property: &str) -> Result<InvertedIndex> {
        let schema = self.schema_manager.schema();
        let config = schema
            .indexes
            .iter()
            .find_map(|idx| match idx {
                IndexDefinition::Inverted(cfg)
                    if cfg.label == label && cfg.property == property =>
                {
                    Some(cfg.clone())
                }
                _ => None,
            })
            .ok_or_else(|| anyhow!("Inverted index not found for {}.{}", label, property))?;

        InvertedIndex::new(&self.base_uri, config).await
    }

    pub async fn vector_search(
        &self,
        label: &str,
        property: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(Vid, f32)>> {
        // Use cached dataset for better performance (avoids reopening + index reload)
        let ds = self.get_cached_dataset(label).await?;

        // Look up vector index config to get the correct distance metric
        let schema = self.schema_manager.schema();
        let metric_type = schema
            .vector_index_for_property(label, property)
            .map(|config| match config.metric {
                DistanceMetric::L2 => MetricType::L2,
                DistanceMetric::Cosine => MetricType::Cosine,
                DistanceMetric::Dot => MetricType::Dot,
                _ => MetricType::L2, // Default for future variants
            })
            .unwrap_or(MetricType::L2); // Default to L2 if no index config (matches Lance default)

        // Use Lance's nearest neighbor search with the correct metric
        let mut scanner = ds.scan();
        scanner.nearest(property, &Float32Array::from(query.to_vec()), k)?;
        scanner.distance_metric(metric_type);

        let mut stream = scanner.try_into_stream().await?;
        let mut results = Vec::new();

        while let Some(batch) = stream.try_next().await? {
            let vid_col = batch
                .column_by_name("_vid")
                .ok_or(anyhow!("Missing _vid column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(anyhow!("Invalid _vid column type"))?;

            let dist_col = batch
                .column_by_name("_distance")
                .ok_or(anyhow!("Missing _distance column"))?
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or(anyhow!("Invalid _distance column type"))?;

            for i in 0..batch.num_rows() {
                let vid = Vid::from(vid_col.value(i));
                let dist = dist_col.value(i);
                results.push((vid, dist));
            }
        }

        Ok(results)
    }

    pub async fn insert_document(&self, label: &str, vid: Vid, doc: Value) -> Result<()> {
        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label {} not found", label))?;

        if !label_meta.is_document {
            return Err(anyhow!("Label {} is not a document", label));
        }

        for idx in &label_meta.json_indexes {
            // Simple path extraction: $.field
            let key = idx.path.trim_start_matches("$.");

            if let Some(val) = doc.get(key) {
                let val_str = match val {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => continue,
                };

                let index = self.json_index(label, &idx.path)?;
                index.write_entries(vec![(val_str, vec![vid])]).await?;
            }
        }

        Ok(())
    }

    pub async fn get_vertex_by_uid(&self, uid: &UniId, label: &str) -> Result<Option<Vid>> {
        let index = self.uid_index(label)?;
        index.get_vid(uid).await
    }

    pub async fn insert_vertex_with_uid(&self, label: &str, vid: Vid, uid: UniId) -> Result<()> {
        let index = self.uid_index(label)?;
        index.write_mapping(&[(uid, vid)]).await
    }

    pub async fn load_subgraph(
        &self,
        start_vids: &[Vid],
        edge_types: &[u16],
        max_hops: usize,
        direction: GraphDirection,
        l0: Option<&L0Buffer>,
    ) -> Result<WorkingGraph> {
        let mut graph = WorkingGraph::new();
        let schema = self.schema_manager.schema();

        // Build maps for ID lookups
        let label_map: HashMap<u16, String> = schema
            .labels
            .values()
            .map(|meta| {
                (
                    meta.id,
                    schema.label_name_by_id(meta.id).unwrap().to_owned(),
                )
            })
            .collect();

        let edge_type_map: HashMap<u16, String> = schema
            .edge_types
            .values()
            .map(|meta| {
                (
                    meta.id,
                    schema.edge_type_name_by_id(meta.id).unwrap().to_owned(),
                )
            })
            .collect();

        let target_edge_types: HashSet<u16> = edge_types.iter().cloned().collect();

        // Initialize frontier
        let mut frontier: Vec<Vid> = start_vids.to_vec();
        let mut visited: HashSet<Vid> = HashSet::new();

        // Add start vertices to graph
        for &vid in start_vids {
            graph.add_vertex(vid);
        }

        for _hop in 0..max_hops {
            let mut next_frontier = HashSet::new();

            for &vid in &frontier {
                if visited.contains(&vid) {
                    continue;
                }
                visited.insert(vid);
                graph.add_vertex(vid);

                let _src_label_name = label_map
                    .get(&vid.label_id())
                    .ok_or_else(|| anyhow!("Unknown label ID: {}", vid.label_id()))?;

                // For each edge type we want to traverse
                for &etype_id in &target_edge_types {
                    let etype_name = edge_type_map
                        .get(&etype_id)
                        .ok_or_else(|| anyhow!("Unknown edge type ID: {}", etype_id))?;

                    // Determine directions
                    // Storage direction: "fwd" or "bwd".
                    // Query direction: Outgoing -> "fwd", Incoming -> "bwd".
                    let (dir_str, neighbor_is_dst) = match direction {
                        GraphDirection::Outgoing => ("fwd", true),
                        GraphDirection::Incoming => ("bwd", false),
                    };

                    let mut edges: HashMap<Eid, EdgeState> = HashMap::new();

                    // 1. L2: Adjacency (Base)
                    // Needs (edge_type, src_label, direction)
                    // Query uses `src_label` of the `vid`.
                    // We rely on vid.label_id() which is embedded in the VID.
                    // NOTE: load_subgraph iterates frontier which are VIDs.
                    let current_src_label = label_map
                        .get(&vid.label_id())
                        .ok_or_else(|| anyhow!("Unknown label ID: {}", vid.label_id()))?;

                    let edge_ver = self
                        .pinned_snapshot
                        .as_ref()
                        .and_then(|s| s.edges.get(etype_name).map(|es| es.lance_version));

                    let adj_ds = self.adjacency_dataset(etype_name, current_src_label, dir_str)?;
                    if let Some((neighbors, eids)) = adj_ds.read_adjacency_at(vid, edge_ver).await?
                    {
                        for (n, eid) in neighbors.into_iter().zip(eids) {
                            edges.insert(
                                eid,
                                EdgeState {
                                    neighbor: n,
                                    version: 0,
                                    deleted: false,
                                },
                            );
                        }
                    }

                    // 2. L1: Delta
                    let delta_ds = self.delta_dataset(etype_name, dir_str)?;
                    let delta_entries = delta_ds.read_deltas(vid, &schema, edge_ver).await?;
                    Self::apply_delta_to_edges(&mut edges, delta_entries, neighbor_is_dst);

                    // 3. L0: Buffer
                    if let Some(l0) = l0 {
                        Self::apply_l0_to_edges(&mut edges, l0, vid, etype_id, direction);
                    }

                    // Add resulting edges to graph
                    Self::add_edges_to_graph(
                        &mut graph,
                        edges,
                        vid,
                        etype_id,
                        neighbor_is_dst,
                        &visited,
                        &mut next_frontier,
                    );
                }
            }
            frontier = next_frontier.into_iter().collect();
        }

        Ok(graph)
    }

    /// Apply delta entries to edge state map, handling version conflicts.
    fn apply_delta_to_edges(
        edges: &mut HashMap<Eid, EdgeState>,
        delta_entries: Vec<crate::storage::delta::L1Entry>,
        neighbor_is_dst: bool,
    ) {
        for entry in delta_entries {
            let neighbor = if neighbor_is_dst {
                entry.dst_vid
            } else {
                entry.src_vid
            };
            let current_ver = edges.get(&entry.eid).map(|s| s.version).unwrap_or(0);

            if entry.version > current_ver {
                edges.insert(
                    entry.eid,
                    EdgeState {
                        neighbor,
                        version: entry.version,
                        deleted: matches!(entry.op, Op::Delete),
                    },
                );
            }
        }
    }

    /// Apply L0 buffer edges and tombstones to edge state map.
    fn apply_l0_to_edges(
        edges: &mut HashMap<Eid, EdgeState>,
        l0: &L0Buffer,
        vid: Vid,
        etype_id: u16,
        direction: GraphDirection,
    ) {
        let l0_neighbors = l0.get_neighbors(vid, etype_id, direction);
        for (neighbor, eid, ver) in l0_neighbors {
            let current_ver = edges.get(&eid).map(|s| s.version).unwrap_or(0);
            if ver > current_ver {
                edges.insert(
                    eid,
                    EdgeState {
                        neighbor,
                        version: ver,
                        deleted: false,
                    },
                );
            }
        }

        // Check tombstones in L0
        for (eid, state) in edges.iter_mut() {
            if l0.is_tombstoned(*eid) {
                state.deleted = true;
            }
        }
    }

    /// Add non-deleted edges to graph and collect next frontier.
    fn add_edges_to_graph(
        graph: &mut WorkingGraph,
        edges: HashMap<Eid, EdgeState>,
        vid: Vid,
        etype_id: u16,
        neighbor_is_dst: bool,
        visited: &HashSet<Vid>,
        next_frontier: &mut HashSet<Vid>,
    ) {
        for (eid, state) in edges {
            if state.deleted {
                continue;
            }
            graph.add_vertex(state.neighbor);

            if !visited.contains(&state.neighbor) {
                next_frontier.insert(state.neighbor);
            }

            if neighbor_is_dst {
                graph.add_edge(vid, state.neighbor, eid, etype_id);
            } else {
                graph.add_edge(state.neighbor, vid, eid, etype_id);
            }
        }
    }
}
