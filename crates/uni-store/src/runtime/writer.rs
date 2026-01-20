// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::embedding::{EmbeddingService, FastEmbedService};
use crate::runtime::id_allocator::IdAllocator;
use crate::runtime::l0::L0Buffer;
use crate::runtime::l0_manager::L0Manager;
use crate::runtime::property_manager::PropertyManager;
use crate::runtime::wal::WriteAheadLog;
use crate::storage::delta::{L1Entry, Op};
use crate::storage::manager::StorageManager;
use anyhow::{Result, anyhow};
use chrono::Utc;
use lance::dataset::WriteMode;
use metrics;
use parking_lot::RwLock;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, instrument};
use uni_common::Properties;
use uni_common::config::UniConfig;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{ConstraintTarget, ConstraintType, EmbeddingModel, IndexDefinition};
use uni_common::core::snapshot::{EdgeSnapshot, LabelSnapshot, SnapshotManifest};
use uni_common::sync::acquire_mutex;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct WriterConfig {
    pub max_mutations: usize,
    // Add throttling config here if we want to decouple from UniConfig,
    // but the prompt says to use UniConfig.
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            max_mutations: 10_000,
        }
    }
}

use crate::runtime::context::QueryContext;
use crate::storage::adjacency_cache::AdjacencyCache;

pub struct Writer {
    pub l0_manager: Arc<L0Manager>,
    pub storage: Arc<StorageManager>,
    pub schema_manager: Arc<uni_common::core::schema::SchemaManager>,
    pub allocator: Arc<IdAllocator>,
    pub config: UniConfig,
    pub cache: Option<Arc<AdjacencyCache>>,
    pub embedding_services: Arc<Mutex<HashMap<String, Arc<dyn EmbeddingService>>>>,
    pub transaction_l0: Option<Arc<RwLock<L0Buffer>>>,
    /// Property manager for cache invalidation after flush
    pub property_manager: Option<Arc<PropertyManager>>,
    /// Timestamp of last flush or creation
    last_flush_time: std::time::Instant,
}

impl Writer {
    pub async fn new(
        storage: Arc<StorageManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        start_version: u64,
    ) -> Result<Self> {
        Self::new_with_config(
            storage,
            schema_manager,
            start_version,
            UniConfig::default(),
            None,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_config(
        storage: Arc<StorageManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        start_version: u64,
        config: UniConfig,
        cache: Option<Arc<AdjacencyCache>>,
        wal: Option<Arc<WriteAheadLog>>,
        allocator: Option<Arc<IdAllocator>>,
    ) -> Result<Self> {
        let allocator = if let Some(a) = allocator {
            a
        } else {
            let store = storage.store();
            let path = object_store::path::Path::from("id_allocator.json");
            Arc::new(IdAllocator::new(store, path, 1000).await?)
        };

        let l0_manager = Arc::new(L0Manager::new(start_version, wal));

        // Create PropertyManager for merging if not provided?
        let property_manager = Some(Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            1000,
        )));

        Ok(Self {
            l0_manager,
            storage,
            schema_manager,
            allocator,
            config,
            cache,
            embedding_services: Arc::new(Mutex::new(HashMap::new())),
            transaction_l0: None,
            property_manager,
            last_flush_time: std::time::Instant::now(),
        })
    }

    /// Replay WAL mutations into the current L0 buffer.
    pub async fn replay_wal(&self, wal_high_water_mark: u64) -> Result<usize> {
        let l0 = self.l0_manager.get_current();
        let wal = l0.read().wal.clone();

        if let Some(wal) = wal {
            wal.initialize().await?;
            let mutations = wal.replay_since(wal_high_water_mark).await?;
            let count = mutations.len();

            if count > 0 {
                log::info!(
                    "Replaying {} mutations from WAL (LSN > {})",
                    count,
                    wal_high_water_mark
                );
                let mut l0_guard = l0.write();
                l0_guard.replay_mutations(mutations)?;
            }

            Ok(count)
        } else {
            Ok(0)
        }
    }
    // ...

    pub async fn next_vid(&self, label_id: u16) -> Result<Vid> {
        self.allocator.allocate_vid(label_id).await
    }

    pub async fn next_eid(&self, type_id: u16) -> Result<Eid> {
        self.allocator.allocate_eid(type_id).await
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.transaction_l0.is_some() {
            return Err(anyhow!("Transaction already active"));
        }
        let current_version = self.l0_manager.get_current().read().current_version;
        // Transaction L0 doesn't have its own WAL for now?
        // Actually, it should probably log to the same WAL?
        // Or we log everything at COMMIT.
        // For now, let's pass None for WAL to transaction L0.
        self.transaction_l0 = Some(Arc::new(RwLock::new(L0Buffer::new(current_version, None))));
        Ok(())
    }

    fn update_metrics(&self) {
        let l0 = self.l0_manager.get_current();
        let size = l0.read().size_bytes();
        metrics::gauge!("l0_buffer_size_bytes").set(size as f64);

        if let Some(tx_l0) = &self.transaction_l0 {
            metrics::gauge!("active_transactions").set(1.0);
            let tx_size = tx_l0.read().size_bytes();
            metrics::gauge!("transaction_l0_size_bytes").set(tx_size as f64);
        } else {
            metrics::gauge!("active_transactions").set(0.0);
            metrics::gauge!("transaction_l0_size_bytes").set(0.0);
        }
    }

    pub async fn commit_transaction(&mut self) -> Result<()> {
        let tx_l0_arc = self
            .transaction_l0
            .take()
            .ok_or_else(|| anyhow!("No active transaction"))?;

        {
            let tx_l0 = tx_l0_arc.read();
            let main_l0_arc = self.l0_manager.get_current();
            let mut main_l0 = main_l0_arc.write();
            main_l0.merge(&tx_l0)?;
        }

        self.update_metrics();

        // Flush WAL to ensure transaction is durable before returning
        self.flush_wal().await?;

        self.check_flush().await
    }

    /// Flush the WAL buffer to durable storage.
    pub async fn flush_wal(&self) -> Result<()> {
        let l0 = self.l0_manager.get_current();
        let wal = l0.read().wal.clone();

        if let Some(wal) = wal {
            wal.flush().await?;
        }
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<()> {
        if self.transaction_l0.is_none() {
            return Err(anyhow!("No active transaction"));
        }
        self.transaction_l0 = None;
        Ok(())
    }

    async fn validate_vertex_constraints(&self, vid: Vid, properties: &Properties) -> Result<()> {
        let schema = self.schema_manager.schema();
        let label_id = vid.label_id();
        let label_name = schema.label_name_by_id(label_id);

        if let Some(label) = label_name {
            // 1. Check NOT NULL constraints (from Property definitions)
            if let Some(props_meta) = schema.properties.get(label) {
                for (prop_name, meta) in props_meta {
                    if !meta.nullable {
                        let val = properties.get(prop_name);
                        if val.is_none() || val.unwrap().is_null() {
                            log::warn!(
                                "Constraint violation: Property '{}' cannot be null for label '{}'",
                                prop_name,
                                label
                            );
                            return Err(anyhow!(
                                "Constraint violation: Property '{}' cannot be null",
                                prop_name
                            ));
                        }
                    }
                }
            }

            // 2. Check Explicit Constraints (Unique, Check, etc.)
            for constraint in &schema.constraints {
                if !constraint.enabled {
                    continue;
                }
                match &constraint.target {
                    ConstraintTarget::Label(l) if l == label => {}
                    _ => continue,
                }

                // println!("DEBUG: Checking constraint {}", constraint.name);

                match &constraint.constraint_type {
                    ConstraintType::Unique {
                        properties: unique_props,
                    } => {
                        // Support single and multi-property unique constraints
                        if !unique_props.is_empty() {
                            let mut key_values = Vec::new();
                            let mut missing = false;
                            for prop in unique_props {
                                if let Some(val) = properties.get(prop) {
                                    key_values.push((prop.clone(), val.clone()));
                                } else {
                                    missing = true; // Can't enforce if property missing (partial update?)
                                    // For INSERT, missing means null?
                                    // If property is nullable, unique constraint typically allows multiple nulls or ignores?
                                    // For now, only check if ALL keys are present
                                }
                            }

                            if !missing {
                                self.check_unique_constraint_multi(label, &key_values, vid)
                                    .await?;
                            }
                        }
                    }
                    ConstraintType::Exists { property } => {
                        let val = properties.get(property);
                        if val.is_none() || val.unwrap().is_null() {
                            log::warn!(
                                "Constraint violation: Property '{}' must exist for label '{}'",
                                property,
                                label
                            );
                            return Err(anyhow!(
                                "Constraint violation: Property '{}' must exist",
                                property
                            ));
                        }
                    }
                    ConstraintType::Check { .. } => {
                        log::warn!(
                            "Check constraints are defined but validation is not yet implemented."
                        );
                    }
                    _ => {
                        return Err(anyhow!("Unsupported constraint type"));
                    }
                }
            }
        }
        Ok(())
    }

    async fn check_unique_constraint_multi(
        &self,
        label: &str,
        key_values: &[(String, Value)],
        current_vid: Vid,
    ) -> Result<()> {
        // 1. Check L0 (in-memory)
        {
            let l0 = self.l0_manager.get_current();
            let l0_guard = l0.read();

            for (vid, props) in &l0_guard.vertex_properties {
                if *vid != current_vid {
                    let mut match_all = true;
                    for (prop, val) in key_values {
                        if let Some(v) = props.get(prop) {
                            if v != val {
                                match_all = false;
                                break;
                            }
                        } else {
                            match_all = false; // Property missing in other node
                            break;
                        }
                    }
                    if match_all {
                        return Err(anyhow!(
                            "Constraint violation: Duplicate composite key for label '{}'",
                            label
                        ));
                    }
                }
            }
        }

        // Check Transaction L0
        if let Some(tx_l0) = &self.transaction_l0 {
            let tx_l0_guard = tx_l0.read();
            for (vid, props) in &tx_l0_guard.vertex_properties {
                if *vid != current_vid {
                    let mut match_all = true;
                    for (prop, val) in key_values {
                        if let Some(v) = props.get(prop) {
                            if v != val {
                                match_all = false;
                                break;
                            }
                        } else {
                            match_all = false;
                            break;
                        }
                    }
                    if match_all {
                        return Err(anyhow!(
                            "Constraint violation: Duplicate composite key for label '{}' (in tx)",
                            label
                        ));
                    }
                }
            }
        }

        // 2. Check Storage (L1/L2)
        let filters: Vec<String> = key_values
            .iter()
            .map(|(prop, val)| {
                let val_str = match val {
                    Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => "NULL".to_string(),
                };
                format!("{} = {}", prop, val_str)
            })
            .collect();

        let mut filter = filters.join(" AND ");
        filter.push_str(&format!(
            " AND _deleted = false AND _vid != {}",
            current_vid.as_u64()
        ));

        if let Ok(ds) = self.storage.vertex_dataset(label)
            && let Ok(lance_ds) = ds.open_raw().await
        {
            let count = lance_ds.count_rows(Some(filter.clone())).await?;
            if count > 0 {
                return Err(anyhow!(
                    "Constraint violation: Duplicate composite key for label '{}' (in storage). Filter: {}",
                    label,
                    filter
                ));
            }
        }

        Ok(())
    }

    async fn check_write_pressure(&self) -> Result<()> {
        let status = self.storage.compaction_status();
        let l1_runs = status.l1_runs;
        let throttle = &self.config.throttle;

        if l1_runs >= throttle.hard_limit {
            log::warn!("Write stalled: L1 runs ({}) at hard limit", l1_runs);
            // Simple polling for now
            while self.storage.compaction_status().l1_runs >= throttle.hard_limit {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        } else if l1_runs >= throttle.soft_limit {
            let excess = l1_runs - throttle.soft_limit;
            // Cap multiplier to avoid overflow
            let excess = std::cmp::min(excess, 31);
            let multiplier = 2_u32.pow(excess as u32);
            let delay = throttle.base_delay * multiplier;
            // log::debug!("Write throttled: {}ms delay", delay.as_millis());
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    async fn get_query_context(&self) -> Option<QueryContext> {
        Some(QueryContext::new_with_pending(
            self.l0_manager.get_current(),
            self.transaction_l0.clone(),
            self.l0_manager.get_pending_flush(),
        ))
    }

    async fn prepare_vertex_upsert(&self, vid: Vid, properties: &mut Properties) -> Result<()> {
        if let Some(pm) = &self.property_manager {
            let schema = self.schema_manager.schema();
            let label_name = schema.label_name_by_id(vid.label_id());

            if let Some(label) = label_name
                && let Some(props_meta) = schema.properties.get(label)
            {
                // Identify CRDT properties
                let mut crdt_keys = Vec::new();
                for (key, _) in properties.iter() {
                    if let Some(meta) = props_meta.get(key)
                        && matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                    {
                        crdt_keys.push(key.clone());
                    }
                }

                if !crdt_keys.is_empty() {
                    let ctx = self.get_query_context().await;
                    for key in crdt_keys {
                        // Read existing value
                        let existing = pm.get_vertex_prop_with_ctx(vid, &key, ctx.as_ref()).await?;

                        if !existing.is_null()
                            && let Some(val) = properties.get_mut(&key)
                        {
                            *val = pm.merge_crdt_values(&existing, val)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn prepare_edge_upsert(&self, eid: Eid, properties: &mut Properties) -> Result<()> {
        if let Some(pm) = &self.property_manager {
            let schema = self.schema_manager.schema();
            let type_name = schema.edge_type_name_by_id(eid.type_id());

            if let Some(t_name) = type_name
                && let Some(props_meta) = schema.properties.get(t_name)
            {
                let mut crdt_keys = Vec::new();
                for (key, _) in properties.iter() {
                    if let Some(meta) = props_meta.get(key)
                        && matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                    {
                        crdt_keys.push(key.clone());
                    }
                }

                if !crdt_keys.is_empty() {
                    let ctx = self.get_query_context().await;
                    for key in crdt_keys {
                        let existing = pm.get_edge_prop(eid, &key, ctx.as_ref()).await?;

                        if !existing.is_null()
                            && let Some(val) = properties.get_mut(&key)
                        {
                            *val = pm.merge_crdt_values(&existing, val)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self, properties), level = "trace")]
    pub async fn insert_vertex(&mut self, vid: Vid, mut properties: Properties) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        self.process_embeddings(vid, &mut properties).await?;
        self.validate_vertex_constraints(vid, &properties).await?;
        self.prepare_vertex_upsert(vid, &mut properties).await?;

        let l0 = if let Some(tx_l0) = &self.transaction_l0 {
            tx_l0.clone()
        } else {
            self.l0_manager.get_current()
        };

        l0.write().insert_vertex(vid, properties);
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if self.transaction_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow insert_vertex: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    pub async fn delete_vertex(&mut self, vid: Vid) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        let l0 = if let Some(tx_l0) = &self.transaction_l0 {
            tx_l0.clone()
        } else {
            self.l0_manager.get_current()
        };

        l0.write().delete_vertex(vid)?;
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if self.transaction_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow delete_vertex: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    #[instrument(skip(self, properties), level = "trace")]
    pub async fn insert_edge(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u16,
        eid: Eid,
        mut properties: Properties,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        self.prepare_edge_upsert(eid, &mut properties).await?;

        let l0 = if let Some(tx_l0) = &self.transaction_l0 {
            tx_l0.clone()
        } else {
            self.l0_manager.get_current()
        };

        l0.write()
            .insert_edge(src_vid, dst_vid, edge_type, eid, properties)?;
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if self.transaction_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow insert_edge: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    pub async fn delete_edge(
        &mut self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u16,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure().await?;
        let l0 = if let Some(tx_l0) = &self.transaction_l0 {
            tx_l0.clone()
        } else {
            self.l0_manager.get_current()
        };

        l0.write().delete_edge(eid, src_vid, dst_vid, edge_type)?;
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if self.transaction_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow delete_edge: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    /// Check if flush should be triggered based on mutation count or time elapsed.
    /// This method is called after each write operation and can also be called
    /// by a background task for time-based flushing.
    pub async fn check_flush(&mut self) -> Result<()> {
        let count = self.l0_manager.get_current().read().mutation_count;

        // Skip if no mutations
        if count == 0 {
            return Ok(());
        }

        // Flush on mutation count threshold (10,000 default)
        if count >= self.config.auto_flush_threshold {
            self.flush_to_l1(None).await?;
            return Ok(());
        }

        // Flush on time interval IF minimum mutations met
        if let Some(interval) = self.config.auto_flush_interval
            && self.last_flush_time.elapsed() >= interval
            && count >= self.config.auto_flush_min_mutations
        {
            self.flush_to_l1(None).await?;
        }

        Ok(())
    }

    async fn process_embeddings(&self, vid: Vid, properties: &mut Properties) -> Result<()> {
        let schema = self.schema_manager.schema();
        let label_id = vid.label_id();
        let label_name = schema.label_name_by_id(label_id);

        if let Some(label) = label_name {
            // Find vector indexes with embedding config for this label
            let mut configs = Vec::new();
            for idx in &schema.indexes {
                if let IndexDefinition::Vector(v_config) = idx
                    && v_config.label == label
                    && let Some(emb_config) = &v_config.embedding_config
                {
                    configs.push((v_config.property.clone(), emb_config.clone()));
                }
            }

            if configs.is_empty() {
                log::info!("No embedding config found for label {}", label);
            }

            for (target_prop, emb_config) in configs {
                // If target property already exists, skip (assume user provided it)
                if properties.contains_key(&target_prop) {
                    continue;
                }

                // Check if source properties exist
                let mut inputs = Vec::new();
                for src_prop in &emb_config.source_properties {
                    if let Some(val) = properties.get(src_prop)
                        && let Some(s) = val.as_str()
                    {
                        inputs.push(s.to_string());
                    }
                }

                if inputs.is_empty() {
                    continue;
                }

                let input_text = inputs.join(" "); // Simple concatenation

                // Get or create service
                let service = self.get_embedding_service(&emb_config.model).await?;

                // Generate
                let embeddings = service.embed(&[&input_text]).await?;
                if let Some(vec) = embeddings.first() {
                    // Store as JSON array of floats
                    let json_vec: Vec<Value> = vec.iter().map(|f| json!(f)).collect();
                    properties.insert(target_prop.clone(), Value::Array(json_vec));
                }
            }
        }
        Ok(())
    }

    async fn get_embedding_service(
        &self,
        model: &EmbeddingModel,
    ) -> Result<Arc<dyn EmbeddingService>> {
        let key = match model {
            EmbeddingModel::FastEmbed { model_name, .. } => format!("fastembed:{}", model_name),
            EmbeddingModel::OpenAI { model, .. } => format!("openai:{}", model),
            EmbeddingModel::Ollama { model, .. } => format!("ollama:{}", model),
            _ => return Err(anyhow!("Unsupported embedding model")),
        };

        {
            let services = acquire_mutex(&self.embedding_services, "embedding_services")?;
            if let Some(service) = services.get(&key) {
                return Ok(service.clone());
            }
        }

        let service: Arc<dyn EmbeddingService> = match model {
            EmbeddingModel::FastEmbed {
                model_name,
                cache_dir,
                ..
            } => Arc::new(FastEmbedService::new(model_name, cache_dir.as_deref())?),
            EmbeddingModel::OpenAI { .. } => {
                return Err(anyhow!("OpenAI embedding provider not yet implemented"));
            }
            EmbeddingModel::Ollama { .. } => {
                return Err(anyhow!("Ollama embedding provider not yet implemented"));
            }
            _ => return Err(anyhow!("Unsupported embedding model")),
        };

        let mut services = acquire_mutex(&self.embedding_services, "embedding_services")?;
        services.insert(key, service.clone());
        Ok(service)
    }

    /// Flushes the current in-memory L0 buffer to L1 storage.
    ///
    /// # Lock Ordering
    ///
    /// To prevent deadlocks, locks must be acquired in the following order:
    /// 1. `Writer` lock (held by caller)
    /// 2. `L0Manager` lock (via `begin_flush` / `get_current`)
    /// 3. `L0Buffer` lock (individual buffer RWLocks)
    /// 4. `Index` / `Storage` locks (during actual flush)
    #[instrument(
        skip(self),
        fields(snapshot_id, mutations_count, size_bytes),
        level = "info"
    )]
    pub async fn flush_to_l1(&mut self, name: Option<String>) -> Result<String> {
        let start = std::time::Instant::now();
        let schema = self.schema_manager.schema();

        let (initial_size, initial_count) = {
            let l0_arc = self.l0_manager.get_current();
            let l0 = l0_arc.read();
            (l0.size_bytes(), l0.mutation_count)
        };
        tracing::Span::current().record("size_bytes", initial_size);
        tracing::Span::current().record("mutations_count", initial_count);

        debug!("Starting L0 flush to L1");

        // 1. Flush WAL BEFORE rotating L0
        // This ensures that if WAL flush fails, the current L0 is still active
        // and mutations are retained in memory until restart/retry.
        // Capture the LSN of the flushed segment for the snapshot's wal_high_water_mark.
        let wal_for_truncate = {
            let current_l0 = self.l0_manager.get_current();
            let l0_guard = current_l0.read();
            l0_guard.wal.clone()
        };

        let wal_lsn = if let Some(ref w) = wal_for_truncate {
            w.flush().await?
        } else {
            0
        };

        // 2. Begin flush: rotate L0 and keep old L0 visible to reads
        // The old L0 stays in pending_flush list until complete_flush is called,
        // ensuring data remains visible even if L1 writes fail.
        let old_l0_arc = self.l0_manager.begin_flush(0, None);
        metrics::counter!("uni_l0_buffer_rotations_total").increment(1);

        let current_version;
        {
            // Acquire Write lock to take WAL and version
            let mut old_l0_guard = old_l0_arc.write();
            current_version = old_l0_guard.current_version;

            // Record the WAL LSN for this L0 so we don't truncate past it
            // if this flush fails and a subsequent flush succeeds.
            old_l0_guard.wal_lsn_at_flush = wal_lsn;

            let wal = old_l0_guard.wal.take();

            // Give WAL to new L0
            let new_l0_arc = self.l0_manager.get_current();
            let mut new_l0_guard = new_l0_arc.write();
            new_l0_guard.wal = wal;
            new_l0_guard.current_version = current_version;
        } // Drop locks

        // 2. Acquire Read lock on Old L0 for flushing
        let mut entries_by_type: HashMap<u16, Vec<L1Entry>> = HashMap::new();
        let mut vertices_by_label: HashMap<u16, Vec<(Vid, Properties, bool, u64)>> = HashMap::new();

        {
            let old_l0 = old_l0_arc.read();

            // 1. Collect all edges and tombstones from L0
            for edge in old_l0.graph.edges() {
                let properties = old_l0
                    .edge_properties
                    .get(&edge.eid)
                    .cloned()
                    .unwrap_or_default();
                let version = old_l0.edge_versions.get(&edge.eid).copied().unwrap_or(0);

                entries_by_type
                    .entry(edge.edge_type)
                    .or_default()
                    .push(L1Entry {
                        src_vid: edge.src_vid,
                        dst_vid: edge.dst_vid,
                        eid: edge.eid,
                        op: Op::Insert,
                        version,
                        properties,
                    });
            }

            // From tombstones
            for tombstone in old_l0.tombstones.values() {
                let version = old_l0
                    .edge_versions
                    .get(&tombstone.eid)
                    .copied()
                    .unwrap_or(0);
                entries_by_type
                    .entry(tombstone.edge_type)
                    .or_default()
                    .push(L1Entry {
                        src_vid: tombstone.src_vid,
                        dst_vid: tombstone.dst_vid,
                        eid: tombstone.eid,
                        op: Op::Delete,
                        version,
                        properties: HashMap::new(),
                    });
            }

            // 2.5 Flush Vertices - Collect
            for (vid, props) in &old_l0.vertex_properties {
                let version = old_l0.vertex_versions.get(vid).copied().unwrap_or(0);
                vertices_by_label.entry(vid.label_id()).or_default().push((
                    *vid,
                    props.clone(),
                    false,
                    version,
                ));
            }
            for &vid in &old_l0.vertex_tombstones {
                let version = old_l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                vertices_by_label.entry(vid.label_id()).or_default().push((
                    vid,
                    HashMap::new(),
                    true,
                    version,
                ));
            }
        } // Drop read lock

        // 0. Load previous snapshot or create new
        let mut manifest = self
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?
            .unwrap_or_else(|| {
                SnapshotManifest::new(Uuid::new_v4().to_string(), schema.schema_version)
            });

        // Update snapshot metadata
        // Save parent snapshot ID before generating new one (for lineage tracking)
        let parent_id = manifest.snapshot_id.clone();
        manifest.parent_snapshot = Some(parent_id);
        manifest.snapshot_id = Uuid::new_v4().to_string();
        manifest.name = name;
        manifest.created_at = Utc::now();
        manifest.version_high_water_mark = current_version;
        manifest.wal_high_water_mark = wal_lsn;
        let snapshot_id = manifest.snapshot_id.clone();

        tracing::Span::current().record("snapshot_id", &snapshot_id);

        // 2. For each edge type, write FWD and BWD runs
        for (&edge_type_id, entries) in entries_by_type.iter() {
            let edge_type_name = schema
                .edge_type_name_by_id(edge_type_id)
                .ok_or_else(|| anyhow!("Edge type ID {} not found in schema", edge_type_id))?;

            // FWD Run (sorted by src_vid)
            let mut fwd_entries = entries.clone();
            fwd_entries.sort_by_key(|e| e.src_vid);
            let fwd_ds = self.storage.delta_dataset(edge_type_name, "fwd")?;
            let fwd_batch = fwd_ds.build_record_batch(&fwd_entries, &schema)?;
            let fwd_info = fwd_ds.write_run(fwd_batch).await?;
            fwd_ds.ensure_eid_index().await?;

            // BWD Run (sorted by dst_vid)
            let mut bwd_entries = entries.clone();
            bwd_entries.sort_by_key(|e| e.dst_vid);
            let bwd_ds = self.storage.delta_dataset(edge_type_name, "bwd")?;
            let bwd_batch = bwd_ds.build_record_batch(&bwd_entries, &schema)?;
            bwd_ds.write_run(bwd_batch).await?;
            bwd_ds.ensure_eid_index().await?;

            // Update Manifest
            let current_snap =
                manifest
                    .edges
                    .entry(edge_type_name.to_string())
                    .or_insert(EdgeSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.version += 1;
            current_snap.count += entries.len() as u64;
            current_snap.lance_version = fwd_info.version().version;

            if let Some(cache) = &self.cache {
                cache.invalidate(edge_type_id);
            }
        }

        // 2.5 Flush Vertices
        for (label_id, vertices) in vertices_by_label {
            let label_name = schema
                .label_name_by_id(label_id)
                .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

            let ds = self.storage.vertex_dataset(label_name)?;

            // Collect inverted index updates before consuming vertices
            // Maps: cfg.property -> (added, removed)
            type InvertedUpdateMap = HashMap<String, (HashMap<Vid, Vec<String>>, HashSet<Vid>)>;
            let mut inverted_updates: InvertedUpdateMap = HashMap::new();

            for idx in &schema.indexes {
                if let IndexDefinition::Inverted(cfg) = idx
                    && cfg.label == label_name
                {
                    let mut added: HashMap<Vid, Vec<String>> = HashMap::new();
                    let mut removed: HashSet<Vid> = HashSet::new();

                    for (vid, props, deleted, _version) in &vertices {
                        if *deleted {
                            removed.insert(*vid);
                        } else if let Some(prop_value) = props.get(&cfg.property) {
                            // Extract terms from the property value (List<String>)
                            if let Some(arr) = prop_value.as_array() {
                                let terms: Vec<String> = arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(ToString::to_string))
                                    .collect();
                                if !terms.is_empty() {
                                    added.insert(*vid, terms);
                                }
                            }
                        }
                    }

                    if !added.is_empty() || !removed.is_empty() {
                        inverted_updates.insert(cfg.property.clone(), (added, removed));
                    }
                }
            }

            let mut v_data = Vec::new();
            let mut d_data = Vec::new();
            let mut ver_data = Vec::new();
            for (vid, props, deleted, version) in vertices {
                v_data.push((vid, props));
                d_data.push(deleted);
                ver_data.push(version);
            }

            let batch = ds.build_record_batch(&v_data, &d_data, &ver_data, &schema)?;
            let ds_info = ds.write_batch(batch, WriteMode::Append).await?;

            // Ensure default scalar indexes exist (_vid, _uid) for efficient lookups
            ds.ensure_default_indexes().await?;

            // Update Manifest
            let current_snap =
                manifest
                    .vertices
                    .entry(label_name.to_string())
                    .or_insert(LabelSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.version += 1;
            current_snap.count += v_data.len() as u64;
            current_snap.lance_version = ds_info.version().version;

            // Invalidate dataset cache to ensure next read picks up new version
            self.storage.invalidate_dataset_cache(label_name);

            // Apply inverted index updates incrementally
            for idx in &schema.indexes {
                if let IndexDefinition::Inverted(cfg) = idx
                    && cfg.label == label_name
                    && let Some((added, removed)) = inverted_updates.get(&cfg.property)
                {
                    self.storage
                        .index_manager()
                        .update_inverted_index_incremental(cfg, added, removed)
                        .await?;
                }
            }
        }

        // Save Snapshot
        self.storage
            .snapshot_manager()
            .save_snapshot(&manifest)
            .await?;
        self.storage
            .snapshot_manager()
            .set_latest_snapshot(&manifest.snapshot_id)
            .await?;

        // Complete flush: remove old L0 from pending list now that L1 writes succeeded.
        // This must happen BEFORE WAL truncation so min_pending_wal_lsn is accurate.
        self.l0_manager.complete_flush(&old_l0_arc);

        // Truncate WAL segments, but only up to the minimum LSN of any remaining pending L0s.
        // This prevents data loss if earlier flushes failed and left L0s in pending_flush.
        if let Some(w) = wal_for_truncate {
            // Determine safe truncation point: the minimum of our LSN and any pending L0s
            let safe_lsn = self
                .l0_manager
                .min_pending_wal_lsn()
                .map(|min_pending| min_pending.min(wal_lsn))
                .unwrap_or(wal_lsn);
            w.truncate_before(safe_lsn).await?;
        }

        // Invalidate property cache after flush to prevent stale reads.
        // Once L0 data moves to storage, cached values from storage may be outdated.
        if let Some(ref pm) = self.property_manager {
            pm.clear_cache().await;
        }

        // Reset last flush time for time-based auto-flush
        self.last_flush_time = std::time::Instant::now();

        info!(
            snapshot_id,
            mutations_count = initial_count,
            size_bytes = initial_size,
            "L0 flush to L1 completed successfully"
        );
        metrics::histogram!("uni_flush_duration_seconds").record(start.elapsed().as_secs_f64());
        metrics::counter!("uni_flush_bytes_total").increment(initial_size as u64);
        metrics::counter!("uni_flush_rows_total").increment(initial_count as u64);

        Ok(snapshot_id)
    }

    /// Set the property manager for cache invalidation.
    pub fn set_property_manager(&mut self, pm: Arc<PropertyManager>) {
        self.property_manager = Some(pm);
    }
}
