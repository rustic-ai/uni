// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::core::*;
use crate::query::expr::Expr;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use uni_algo::algo::procedures::AlgoContext;
use uni_store::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;

impl Executor {
    pub(crate) async fn execute_procedure<'a>(
        &'a self,
        name: &str,
        args: &[Expr],
        yield_items: &[String],
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        if name.starts_with("algo.") {
            // Dispatch to AlgorithmRegistry
            if let Some(procedure) = self.algo_registry.get(name) {
                let empty_row = HashMap::new();
                let mut evaluated_args = Vec::with_capacity(args.len());
                for arg in args {
                    evaluated_args.push(
                        self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
                            .await?,
                    );
                }

                let algo_ctx = AlgoContext::new_with_pending(
                    self.storage.clone(),
                    self.storage.adjacency_cache(),
                    ctx.map(|c| c.l0.clone()),
                    ctx.map(|c| c.pending_flush_l0s.clone()).unwrap_or_default(),
                );

                let signature = procedure.signature();
                let mut stream = procedure.execute(algo_ctx, evaluated_args);
                let mut results = Vec::new();
                let mut row_count = 0usize;

                while let Some(row_res) = stream.next().await {
                    // CWE-400: Check timeout periodically during algorithm execution
                    row_count += 1;
                    if row_count.is_multiple_of(Self::AGGREGATE_TIMEOUT_CHECK_INTERVAL)
                        && let Some(ctx) = ctx
                    {
                        ctx.check_timeout()?;
                    }

                    let row = row_res?;
                    let mut result_map = HashMap::new();

                    for yield_name in yield_items {
                        if let Some(idx) =
                            signature.yields.iter().position(|(n, _)| *n == yield_name)
                            && idx < row.values.len()
                        {
                            result_map.insert(yield_name.clone(), row.values[idx].clone());
                        }
                    }
                    results.push(result_map);
                }

                return Ok(results);
            }
        }

        match name {
            "db.idx.vector.query" => {
                let empty_row = HashMap::new();
                let label = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label must be string"))?
                    .to_string();
                let property = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Property must be string"))?
                    .to_string();
                let query_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;
                let k = self
                    .evaluate_expr(&args[3], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_u64()
                    .ok_or(anyhow!("k must be integer"))? as usize;
                let query_vector: Vec<f32> = serde_json::from_value(query_val)?;
                let results = self
                    .storage
                    .vector_search(&label, &property, &query_vector, k)
                    .await?;
                let mut matches = Vec::new();
                for (vid, dist) in results {
                    let mut m = HashMap::new();
                    if !yield_items.is_empty() {
                        m.insert(yield_items[0].clone(), Value::String(vid.to_string()));
                    }
                    if yield_items.len() > 1 {
                        m.insert(yield_items[1].clone(), json!(dist));
                    }
                    matches.push(m);
                }
                Ok(matches)
            }
            "db.compact" => {
                let stats = self.storage.compact().await?;
                let mut full_result = HashMap::new();
                full_result.insert("files_compacted".to_string(), json!(stats.files_compacted));
                full_result.insert("bytes_before".to_string(), json!(stats.bytes_before));
                full_result.insert("bytes_after".to_string(), json!(stats.bytes_after));
                full_result.insert(
                    "duration_ms".to_string(),
                    json!(stats.duration.as_millis() as u64),
                );

                let mut result = HashMap::new();
                for yield_name in yield_items {
                    if let Some(val) = full_result.get(yield_name) {
                        result.insert(yield_name.clone(), val.clone());
                    }
                }
                // If no yield items, return everything? Or follows Cypher rules?
                // Cypher 'CALL ...' without YIELD returns nothing/everything depending on implementation.
                // But typically if YIELD is provided, we filter.
                if yield_items.is_empty() {
                    result = full_result;
                }

                Ok(vec![result])
            }
            "db.compactionStatus" => {
                let status = self.storage.compaction_status();
                let mut full_result = HashMap::new();
                full_result.insert("l1_runs".to_string(), json!(status.l1_runs));
                full_result.insert("l1_size_bytes".to_string(), json!(status.l1_size_bytes));
                full_result.insert(
                    "in_progress".to_string(),
                    json!(status.compaction_in_progress),
                );
                full_result.insert("pending".to_string(), json!(status.compaction_pending));
                full_result.insert(
                    "total_compactions".to_string(),
                    json!(status.total_compactions),
                );
                full_result.insert(
                    "total_bytes_compacted".to_string(),
                    json!(status.total_bytes_compacted),
                );

                let mut result = HashMap::new();
                for yield_name in yield_items {
                    if let Some(val) = full_result.get(yield_name) {
                        result.insert(yield_name.clone(), val.clone());
                    }
                }
                if yield_items.is_empty() {
                    result = full_result;
                }

                Ok(vec![result])
            }
            "db.snapshot.create" => {
                let empty_row = HashMap::new();
                let name = if !args.is_empty() {
                    Some(
                        self.evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                            .await?
                            .as_str()
                            .ok_or(anyhow!("Snapshot name must be string"))?
                            .to_string(),
                    )
                } else {
                    None
                };

                let writer_arc = self
                    .writer
                    .as_ref()
                    .ok_or_else(|| anyhow!("Database is in read-only mode"))?;
                let mut writer = writer_arc.write().await;
                let snapshot_id = writer.flush_to_l1(name).await?;

                let mut result = HashMap::new();
                result.insert("snapshot_id".to_string(), Value::String(snapshot_id));
                Ok(vec![result])
            }
            "db.snapshot.list" => {
                let sm = self.storage.snapshot_manager();
                let ids = sm.list_snapshots().await?;
                let mut results = Vec::new();
                for id in ids {
                    if let Ok(m) = sm.load_snapshot(&id).await {
                        let mut row = HashMap::new();
                        row.insert("snapshot_id".to_string(), Value::String(m.snapshot_id));
                        row.insert(
                            "name".to_string(),
                            m.name.map(Value::String).unwrap_or(Value::Null),
                        );
                        row.insert("created_at".to_string(), json!(m.created_at));
                        row.insert("version_hwm".to_string(), json!(m.version_high_water_mark));
                        results.push(row);
                    }
                }
                Ok(results)
            }
            "db.snapshot.restore" => {
                let empty_row = HashMap::new();
                let id = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Snapshot ID must be string"))?
                    .to_string();

                self.storage
                    .snapshot_manager()
                    .set_latest_snapshot(&id)
                    .await?;
                let mut result = HashMap::new();
                result.insert("status".to_string(), Value::String("Restored".to_string()));
                Ok(vec![result])
            }
            "db.labels" => {
                let schema = self.storage.schema_manager().schema();
                let mut results = Vec::new();
                for label_name in schema.labels.keys() {
                    let mut row = HashMap::new();
                    row.insert("label".to_string(), Value::String(label_name.clone()));

                    let prop_count = schema
                        .properties
                        .get(label_name)
                        .map(|p| p.len())
                        .unwrap_or(0);
                    row.insert("propertyCount".to_string(), json!(prop_count));

                    let node_count = if let Ok(ds) = self.storage.vertex_dataset(label_name) {
                        if let Ok(raw) = ds.open_raw().await {
                            raw.count_rows(None).await.unwrap_or(0)
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                    row.insert("nodeCount".to_string(), json!(node_count));

                    let idx_count = schema
                        .indexes
                        .iter()
                        .filter(|i| match i {
                            uni_common::core::schema::IndexDefinition::Vector(v) => {
                                &v.label == label_name
                            }
                            uni_common::core::schema::IndexDefinition::Scalar(s) => {
                                &s.label == label_name
                            }
                            uni_common::core::schema::IndexDefinition::FullText(f) => {
                                &f.label == label_name
                            }
                            uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                                &j.label == label_name
                            }
                            _ => false,
                        })
                        .count();
                    row.insert("indexCount".to_string(), json!(idx_count));

                    results.push(row);
                }
                Ok(results)
            }
            "db.edgeTypes" | "db.relationshipTypes" => {
                let schema = self.storage.schema_manager().schema();
                let mut results = Vec::new();
                for (type_name, meta) in &schema.edge_types {
                    let mut row = HashMap::new();
                    row.insert("type".to_string(), Value::String(type_name.clone()));
                    row.insert(
                        "relationshipType".to_string(),
                        Value::String(type_name.clone()),
                    ); // Alias
                    row.insert("sourceLabels".to_string(), json!(meta.src_labels));
                    row.insert("targetLabels".to_string(), json!(meta.dst_labels));

                    let prop_count = schema
                        .properties
                        .get(type_name)
                        .map(|p| p.len())
                        .unwrap_or(0);
                    row.insert("propertyCount".to_string(), json!(prop_count));

                    results.push(row);
                }
                Ok(results)
            }
            "db.indexes" => {
                let schema = self.storage.schema_manager().schema();
                let mut results = Vec::new();
                for idx in schema.indexes {
                    let mut row = HashMap::new();
                    // Defaults
                    row.insert("state".to_string(), Value::String("ONLINE".to_string()));

                    match idx {
                        uni_common::core::schema::IndexDefinition::Vector(v) => {
                            row.insert("name".to_string(), Value::String(v.name));
                            row.insert("type".to_string(), Value::String("VECTOR".to_string()));
                            row.insert("label".to_string(), Value::String(v.label));
                            row.insert("property".to_string(), Value::String(v.property.clone())); // For backward compat
                            row.insert("properties".to_string(), json!(vec![v.property]));
                        }
                        uni_common::core::schema::IndexDefinition::FullText(f) => {
                            row.insert("name".to_string(), Value::String(f.name));
                            row.insert("type".to_string(), Value::String("FULLTEXT".to_string()));
                            row.insert("label".to_string(), Value::String(f.label));
                            row.insert("properties".to_string(), json!(f.properties));
                        }
                        uni_common::core::schema::IndexDefinition::Scalar(s) => {
                            row.insert("name".to_string(), Value::String(s.name));
                            row.insert("type".to_string(), Value::String("SCALAR".to_string()));
                            row.insert("label".to_string(), Value::String(s.label));
                            row.insert("properties".to_string(), json!(s.properties));
                        }
                        uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                            row.insert("name".to_string(), Value::String(j.name));
                            row.insert("type".to_string(), Value::String("JSON_FTS".to_string()));
                            row.insert("label".to_string(), Value::String(j.label));
                            row.insert("properties".to_string(), json!(vec![j.column]));
                        }
                        _ => {
                            row.insert("name".to_string(), Value::String("UNKNOWN".to_string()));
                            row.insert("type".to_string(), Value::String("UNKNOWN".to_string()));
                        }
                    }
                    results.push(row);
                }
                Ok(results)
            }
            "db.constraints" => {
                let schema = self.storage.schema_manager().schema();
                let mut results = Vec::new();
                for c in schema.constraints {
                    let mut row = HashMap::new();
                    row.insert("name".to_string(), Value::String(c.name));
                    row.insert("enabled".to_string(), Value::Bool(c.enabled));

                    match c.constraint_type {
                        uni_common::core::schema::ConstraintType::Unique { properties } => {
                            row.insert("type".to_string(), Value::String("UNIQUE".to_string()));
                            row.insert("properties".to_string(), json!(properties));
                        }
                        uni_common::core::schema::ConstraintType::Exists { property } => {
                            row.insert("type".to_string(), Value::String("EXISTS".to_string()));
                            row.insert("properties".to_string(), json!(vec![property]));
                        }
                        uni_common::core::schema::ConstraintType::Check { expression } => {
                            row.insert("type".to_string(), Value::String("CHECK".to_string()));
                            row.insert("expression".to_string(), Value::String(expression));
                        }
                        _ => {
                            row.insert("type".to_string(), Value::String("UNKNOWN".to_string()));
                        }
                    }

                    match c.target {
                        uni_common::core::schema::ConstraintTarget::Label(l) => {
                            row.insert("label".to_string(), Value::String(l));
                        }
                        uni_common::core::schema::ConstraintTarget::EdgeType(t) => {
                            row.insert("relationshipType".to_string(), Value::String(t));
                        }
                        _ => {
                            row.insert("target".to_string(), Value::String("UNKNOWN".to_string()));
                        }
                    }

                    results.push(row);
                }
                Ok(results)
            }
            "db.schema.labelInfo" => {
                let schema = self.storage.schema_manager().schema();
                let empty_row = HashMap::new();
                let label_name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label must be string"))?
                    .to_string();

                let mut results = Vec::new();
                if let Some(props) = schema.properties.get(&label_name) {
                    for (prop_name, prop_meta) in props {
                        let mut row = HashMap::new();
                        row.insert("property".to_string(), Value::String(prop_name.clone()));
                        row.insert(
                            "dataType".to_string(),
                            Value::String(format!("{:?}", prop_meta.r#type)),
                        );
                        row.insert("nullable".to_string(), Value::Bool(prop_meta.nullable));

                        let is_indexed = schema.indexes.iter().any(|idx| match idx {
                            uni_common::core::schema::IndexDefinition::Vector(v) => {
                                v.label == label_name && v.property == *prop_name
                            }
                            uni_common::core::schema::IndexDefinition::Scalar(s) => {
                                s.label == label_name && s.properties.contains(prop_name)
                            }
                            uni_common::core::schema::IndexDefinition::FullText(f) => {
                                f.label == label_name && f.properties.contains(prop_name)
                            }
                            uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                                j.label == label_name && j.column == *prop_name
                            }
                            _ => false,
                        });
                        row.insert("indexed".to_string(), Value::Bool(is_indexed));

                        // Check unique constraints
                        let unique = schema.constraints.iter().any(|c| {
                            if let uni_common::core::schema::ConstraintTarget::Label(l) = &c.target
                                && l == &label_name
                                && c.enabled
                                && let uni_common::core::schema::ConstraintType::Unique {
                                    properties,
                                } = &c.constraint_type
                            {
                                return properties.contains(prop_name);
                            }
                            false
                        });
                        row.insert("unique".to_string(), Value::Bool(unique));

                        results.push(row);
                    }
                }
                Ok(results)
            }
            // DDL Procedures
            "db.createLabel" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label name must be string"))?
                    .to_string();
                let config = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;

                let success =
                    super::ddl_procedures::create_label(&self.storage, &name, &config).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "db.createEdgeType" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Edge type name must be string"))?
                    .to_string();
                let src_val = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;
                let dst_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[3], &empty_row, prop_manager, params, ctx)
                    .await?;

                // Convert src/dst to Vec<String>
                let src_labels = src_val
                    .as_array()
                    .ok_or(anyhow!("Source labels must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Label must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let dst_labels = dst_val
                    .as_array()
                    .ok_or(anyhow!("Target labels must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Label must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let success = super::ddl_procedures::create_edge_type(
                    &self.storage,
                    &name,
                    src_labels,
                    dst_labels,
                    &config,
                )
                .await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "db.createIndex" => {
                let empty_row = HashMap::new();
                let label = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label must be string"))?
                    .to_string();
                let property = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Property must be string"))?
                    .to_string();
                let config = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;

                let success =
                    super::ddl_procedures::create_index(&self.storage, &label, &property, &config)
                        .await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "db.createConstraint" => {
                let empty_row = HashMap::new();
                let label = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label must be string"))?
                    .to_string();
                let c_type = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Constraint type must be string"))?
                    .to_string();
                let props_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;

                let properties = props_val
                    .as_array()
                    .ok_or(anyhow!("Properties must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Property must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let success = super::ddl_procedures::create_constraint(
                    &self.storage,
                    &label,
                    &c_type,
                    properties,
                )
                .await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "db.dropLabel" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label name must be string"))?
                    .to_string();

                let success = super::ddl_procedures::drop_label(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "db.dropEdgeType" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Edge type name must be string"))?
                    .to_string();

                let success = super::ddl_procedures::drop_edge_type(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "db.dropIndex" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Index name must be string"))?
                    .to_string();

                let success = super::ddl_procedures::drop_index(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "db.dropConstraint" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Constraint name must be string"))?
                    .to_string();

                let success = super::ddl_procedures::drop_constraint(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            _ => Err(anyhow!("Unknown procedure {}", name)),
        }
    }
}
