// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::store_utils::{DEFAULT_TIMEOUT, get_with_timeout, put_with_timeout};
use anyhow::Result;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uni_common::core::id::{Eid, Vid};

#[derive(Serialize, Deserialize, Default, Clone)]
struct CounterManifest {
    labels: HashMap<u16, u64>,
    edge_types: HashMap<u16, u64>,
}

struct AllocatorState {
    manifest: CounterManifest,
    manifest_version: Option<String>, // ETag
    current_vid: HashMap<u16, u64>,
    current_eid: HashMap<u16, u64>,
}

pub struct IdAllocator {
    store: Arc<dyn ObjectStore>,
    path: Path,
    state: Mutex<AllocatorState>,
    batch_size: u64,
}

impl IdAllocator {
    pub async fn new(store: Arc<dyn ObjectStore>, path: Path, batch_size: u64) -> Result<Self> {
        let (manifest, version) = match get_with_timeout(&store, &path, DEFAULT_TIMEOUT).await {
            Ok(get_result) => {
                let version = get_result.meta.e_tag.clone();
                let bytes = get_result.bytes().await?;
                let manifest: CounterManifest = serde_json::from_slice(&bytes)?;
                (manifest, version)
            }
            Err(e) if e.to_string().contains("not found") => (CounterManifest::default(), None),
            Err(e) => return Err(e),
        };

        let current_vid = manifest.labels.clone();
        let current_eid = manifest.edge_types.clone();

        Ok(Self {
            store,
            path,
            state: Mutex::new(AllocatorState {
                manifest,
                manifest_version: version,
                current_vid,
                current_eid,
            }),
            batch_size,
        })
    }

    pub async fn allocate_vid(&self, label_id: u16) -> Result<Vid> {
        let mut state = self.state.lock().await;

        let current_val = *state.current_vid.get(&label_id).unwrap_or(&0);
        let limit_val = *state.manifest.labels.get(&label_id).unwrap_or(&0);

        if current_val >= limit_val {
            state
                .manifest
                .labels
                .insert(label_id, limit_val + self.batch_size);
            self.persist_manifest(&mut state).await?;
        }

        let current = state.current_vid.entry(label_id).or_insert(0);
        let vid = Vid::new(label_id, *current);
        *current += 1;
        Ok(vid)
    }

    pub async fn allocate_eid(&self, type_id: u16) -> Result<Eid> {
        let mut state = self.state.lock().await;

        let current_val = *state.current_eid.get(&type_id).unwrap_or(&0);
        let limit_val = *state.manifest.edge_types.get(&type_id).unwrap_or(&0);

        if current_val >= limit_val {
            state
                .manifest
                .edge_types
                .insert(type_id, limit_val + self.batch_size);
            self.persist_manifest(&mut state).await?;
        }

        let current = state.current_eid.entry(type_id).or_insert(0);
        let eid = Eid::new(type_id, *current);
        *current += 1;
        Ok(eid)
    }

    async fn persist_manifest(&self, state: &mut AllocatorState) -> Result<()> {
        let json = serde_json::to_vec_pretty(&state.manifest)?;
        let bytes = Bytes::from(json);

        // Try conditional put first, fall back to unconditional if not supported
        // (LocalFileSystem doesn't support ETag-based conditional puts)
        let put_result = if let Some(version) = &state.manifest_version {
            let opts: PutOptions = PutMode::Update(UpdateVersion {
                e_tag: Some(version.clone()),
                version: None,
            })
            .into();
            match tokio::time::timeout(
                DEFAULT_TIMEOUT,
                self.store.put_opts(&self.path, bytes.clone().into(), opts),
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(e))
                    if e.to_string().contains("not yet implemented")
                        || e.to_string().contains("not supported") =>
                {
                    // LocalFileSystem doesn't support conditional puts, use regular put
                    put_with_timeout(&self.store, &self.path, bytes, DEFAULT_TIMEOUT).await?
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "Object store put_opts timed out after {:?}",
                        DEFAULT_TIMEOUT
                    ));
                }
            }
        } else {
            // No version yet, try create mode, fall back to regular put
            let opts: PutOptions = PutMode::Create.into();
            match tokio::time::timeout(
                DEFAULT_TIMEOUT,
                self.store.put_opts(&self.path, bytes.clone().into(), opts),
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(object_store::Error::AlreadyExists { .. })) => {
                    // Another process created it, just overwrite
                    put_with_timeout(&self.store, &self.path, bytes, DEFAULT_TIMEOUT).await?
                }
                Ok(Err(e)) if e.to_string().contains("not yet implemented") => {
                    put_with_timeout(&self.store, &self.path, bytes, DEFAULT_TIMEOUT).await?
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "Object store put_opts timed out after {:?}",
                        DEFAULT_TIMEOUT
                    ));
                }
            }
        };

        state.manifest_version = put_result.e_tag;
        Ok(())
    }
}
