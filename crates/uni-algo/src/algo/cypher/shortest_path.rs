// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! algo.shortestPath procedure implementation.

use crate::algo::DirectTraversal;
use crate::algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
use anyhow::{Result, anyhow};
use futures::stream::{self, BoxStream, StreamExt};
use serde_json::{Value, json};
use uni_common::core::id::Vid;
use uni_store::runtime::l0::L0Buffer;

pub struct ShortestPathProcedure;

impl AlgoProcedure for ShortestPathProcedure {
    fn name(&self) -> &str {
        "algo.shortestPath"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                ("sourceNode", ValueType::Node),
                ("targetNode", ValueType::Node),
                ("relationshipTypes", ValueType::List),
            ],
            optional_args: Vec::new(),
            yields: vec![
                ("nodeIds", ValueType::List),
                ("edgeIds", ValueType::List),
                ("length", ValueType::Int),
            ],
        }
    }

    fn execute(
        &self,
        ctx: AlgoContext,
        args: Vec<Value>,
    ) -> BoxStream<'static, Result<AlgoResultRow>> {
        let signature = self.signature();
        let args = match signature.validate_args(args) {
            Ok(a) => a,
            Err(e) => return stream::once(async { Err(e) }).boxed(),
        };

        let source_vid = match vid_from_value(&args[0]) {
            Ok(v) => v,
            Err(e) => return stream::once(async move { Err(e) }).boxed(),
        };
        let target_vid = match vid_from_value(&args[1]) {
            Ok(v) => v,
            Err(e) => return stream::once(async move { Err(e) }).boxed(),
        };
        let edge_types_str: Vec<String> = args[2]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        // Use stream::once with an async block for the single result
        let result_stream = async move {
            // 1. Resolve edge types and warm cache
            let schema = ctx.storage.schema_manager().schema();
            let mut edge_type_ids = Vec::new();

            use uni_store::storage::adjacency_cache::Direction;

            for type_name in &edge_types_str {
                let meta = schema
                    .edge_types
                    .get(type_name)
                    .ok_or_else(|| anyhow!("Edge type {} not found", type_name))?;
                edge_type_ids.push(meta.id);

                let edge_ver = ctx.storage.get_edge_version_by_id(meta.id);

                // Warm Outgoing for src_labels
                for label_name in &meta.src_labels {
                    if let Some(label_meta) = schema.labels.get(label_name) {
                        ctx.cache
                            .warm(
                                &ctx.storage,
                                meta.id,
                                Direction::Outgoing,
                                label_meta.id,
                                edge_ver,
                            )
                            .await?;
                    }
                }

                // Warm Incoming for dst_labels
                for label_name in &meta.dst_labels {
                    if let Some(label_meta) = schema.labels.get(label_name) {
                        ctx.cache
                            .warm(
                                &ctx.storage,
                                meta.id,
                                Direction::Incoming,
                                label_meta.id,
                                edge_ver,
                            )
                            .await?;
                    }
                }
            }

            // Lock L0 buffers and run DirectTraversal synchronously
            let l0_guard = ctx.l0.as_ref().map(|l| l.read());
            let l0_ref = l0_guard.as_deref();

            // Lock pending flush L0s and collect references
            let pending_guards: Vec<_> = ctx.pending_flush_l0s.iter().map(|l| l.read()).collect();
            let pending_refs: Vec<&L0Buffer> = pending_guards.iter().map(|g| &**g).collect();

            let traversal =
                DirectTraversal::new_with_pending(&ctx.cache, l0_ref, pending_refs, edge_type_ids);

            if let Some(path) = traversal.shortest_path(source_vid, target_vid, Direction::Outgoing)
            {
                Ok(Some(AlgoResultRow {
                    values: vec![
                        json!(path.vertices.iter().map(|v| v.as_u64()).collect::<Vec<_>>()),
                        json!(path.edges.iter().map(|e| e.as_u64()).collect::<Vec<_>>()),
                        json!(path.len()),
                    ],
                }))
            } else {
                Ok(None)
            }
        };

        // Convert the async block to a stream, filtering out None results
        stream::once(result_stream)
            .filter_map(|res: Result<Option<AlgoResultRow>>| async move {
                match res {
                    Ok(Some(row)) => Some(Ok(row)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .boxed()
    }
}

fn vid_from_value(val: &Value) -> Result<Vid> {
    if let Some(s) = val.as_str() {
        let parts: Vec<_> = s.split(':').collect();
        if parts.len() == 2
            && let (Ok(l), Ok(o)) = (parts[0].parse::<u16>(), parts[1].parse::<u64>())
        {
            return Ok(Vid::new(l, o));
        }
    }
    if let Some(v) = val.as_u64() {
        return Ok(Vid::from(v));
    }
    Err(anyhow!("Invalid Vid format: {:?}", val))
}
