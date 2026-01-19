// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! algo.astar procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{AStar, AStarConfig, Algorithm};
use crate::algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
use anyhow::Result;
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::RwLock;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::id::Vid;
use uni_store::runtime::l0::L0Buffer;

pub struct AStarProcedure;

impl AlgoProcedure for AStarProcedure {
    fn name(&self) -> &str {
        "algo.astar"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                ("startNode", ValueType::Node),
                ("endNode", ValueType::Node),
                ("edgeType", ValueType::String),
                ("heuristicProperty", ValueType::String),
            ],
            optional_args: vec![],
            yields: vec![
                ("path", ValueType::List), // List of VIDs
                ("cost", ValueType::Float),
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

        let start_vid_u64 = match &args[0] {
            Value::String(s) => s.parse::<u64>().unwrap_or_default(),
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => return stream::once(async { Err(anyhow::anyhow!("Invalid start node")) }).boxed(),
        };

        let end_vid_u64 = match &args[1] {
            Value::String(s) => s.parse::<u64>().unwrap_or(0),
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => return stream::once(async { Err(anyhow::anyhow!("Invalid end node")) }).boxed(),
        };

        let edge_type = args[2].as_str().unwrap().to_string();
        let heuristic_prop = args[3].as_str().unwrap().to_string();

        let start_vid = Vid::from(start_vid_u64);
        let end_vid = Vid::from(end_vid_u64);

        let stream = async_stream::try_stream! {
            let schema = ctx.storage.schema_manager().schema();

            if !schema.edge_types.contains_key(&edge_type) {
                Err(anyhow::anyhow!("Edge type '{}' not found", edge_type))?;
            }

            let edge_meta = schema.edge_types.get(&edge_type).unwrap();
            let mut labels = edge_meta.src_labels.clone();
            labels.extend(edge_meta.dst_labels.clone());
            labels.sort();
            labels.dedup();

            // 1. Build Projection
            let projection = ProjectionBuilder::new(ctx.storage.clone(), &ctx.cache, ctx.l0.clone())
                .pending_flush_l0s(ctx.pending_flush_l0s.clone())
                .node_labels(&labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .edge_types(&[&edge_type])
                .build()
                .await?;

            // 2. Load Heuristic Property
            let prop_manager = uni_store::runtime::property_manager::PropertyManager::new(
                ctx.storage.clone(),
                ctx.storage.schema_manager_arc(),
                1000,
            );

            let mut heuristic = HashMap::new();
            let vids: Vec<Vid> = projection.vertices().map(|(_, vid)| vid).collect();

            use uni_store::runtime::context::QueryContext;

            let l0 = ctx.l0.clone().unwrap_or_else(|| Arc::new(RwLock::new(L0Buffer::new(0, None))));

            let q_ctx = QueryContext::new_with_pending(
                l0,
                None,
                ctx.pending_flush_l0s.clone(),
            );

            for chunk in vids.chunks(1000) {
                let props_map = prop_manager.get_batch_vertex_props(chunk, &[&heuristic_prop], Some(&q_ctx)).await?;
                for (vid, props) in props_map {
                    if let Some(val) = props.get(&heuristic_prop)
                        && let Some(f) = val.as_f64() {
                            heuristic.insert(vid, f);
                        }
                }
            }

            let config = AStarConfig {
                source: start_vid,
                target: end_vid,
                heuristic,
            };

            let result = tokio::task::spawn_blocking(move || {
                AStar::run(&projection, config)
            }).await?;

            if let (Some(path), Some(cost)) = (result.path, result.distance) {
                let path_json: Vec<Value> = path.into_iter().map(|v| json!(v.as_u64())).collect();
                yield AlgoResultRow {
                    values: vec![Value::Array(path_json), json!(cost)],
                };
            }
        };

        Box::pin(stream)
    }
}
