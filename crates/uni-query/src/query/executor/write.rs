// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::core::*;
use crate::query::ast::{
    AlterAction, AlterEdgeTypeClause, AlterLabelClause, ConstraintTarget as AstConstraintTarget,
    ConstraintType as AstConstraintType, CreateConstraintClause, CreateEdgeTypeClause,
    CreateLabelClause, Direction, DropConstraintClause, DropEdgeTypeClause, DropLabelClause,
    Pattern, PatternPart, RemoveItem, SetClause, SetItem,
};
use crate::query::expr::{Expr, Operator};
use crate::query::planner::LogicalPlan;
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::id::Vid;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType, SchemaManager};
use uni_store::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;

impl Executor {
    pub(crate) async fn execute_vacuum(&self) -> Result<()> {
        if let Some(writer_arc) = &self.writer {
            // Flush first while holding the lock
            {
                let mut writer = writer_arc.write().await;
                writer.flush_to_l1(None).await?;
            } // Drop lock before compacting to avoid blocking reads/writes

            // Compaction can run without holding the writer lock
            let compactor = uni_store::storage::compaction::Compactor::new(self.storage.clone());
            compactor.compact_all().await?;
        }
        Ok(())
    }

    pub(crate) async fn execute_checkpoint(&self) -> Result<()> {
        if let Some(writer_arc) = &self.writer {
            let mut writer = writer_arc.write().await;
            writer.flush_to_l1(Some("checkpoint".to_string())).await?;
        }
        Ok(())
    }

    pub(crate) async fn execute_create_label(&self, clause: CreateLabelClause) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        if clause.if_not_exists && sm.schema().labels.contains_key(&clause.name) {
            return Ok(());
        }
        sm.add_label(&clause.name, false)?;
        for prop in clause.properties {
            sm.add_property(&clause.name, &prop.name, prop.data_type, prop.nullable)?;
            if prop.unique {
                let constraint = Constraint {
                    name: format!("{}_{}_unique", clause.name, prop.name),
                    constraint_type: ConstraintType::Unique {
                        properties: vec![prop.name],
                    },
                    target: ConstraintTarget::Label(clause.name.clone()),
                    enabled: true,
                };
                sm.add_constraint(constraint)?;
            }
        }
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn enrich_properties_with_generated_columns(
        &self,
        label_name: &str,
        properties: &mut HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        let schema = self.storage.schema_manager().schema();

        if let Some(props_meta) = schema.properties.get(label_name) {
            let mut generators = Vec::new();
            for (prop_name, meta) in props_meta {
                if let Some(expr_str) = &meta.generation_expression {
                    generators.push((prop_name.clone(), expr_str.clone()));
                }
            }

            for (prop_name, expr_str) in generators {
                // Check cache first to avoid re-parsing the same expression
                let cache_key = (label_name.to_string(), prop_name.clone());
                let expr = {
                    let cache = self.gen_expr_cache.read().await;
                    cache.get(&cache_key).cloned()
                };

                let expr = match expr {
                    Some(e) => e,
                    None => {
                        // Parse and cache
                        let mut parser = crate::query::parser::CypherParser::new(&expr_str)?;
                        let parsed = parser.parse_expression()?;
                        let mut cache = self.gen_expr_cache.write().await;
                        cache.insert(cache_key, parsed.clone());
                        parsed
                    }
                };

                if let Some(var) = expr.extract_variable() {
                    let mut scope = HashMap::new();
                    let props_json: serde_json::Map<String, Value> = properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    scope.insert(var, Value::Object(props_json));

                    // Use scope as row
                    let val = self
                        .evaluate_expr(&expr, &scope, prop_manager, params, ctx)
                        .await?;
                    properties.insert(prop_name, val);
                } else {
                    log::warn!(
                        "Could not extract variable from generation expression: {}",
                        expr_str
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_create_edge_type(
        &self,
        clause: CreateEdgeTypeClause,
    ) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        if clause.if_not_exists && sm.schema().edge_types.contains_key(&clause.name) {
            return Ok(());
        }
        sm.add_edge_type(&clause.name, clause.src_labels, clause.dst_labels)?;
        for prop in clause.properties {
            sm.add_property(&clause.name, &prop.name, prop.data_type, prop.nullable)?;
        }
        sm.save().await?;
        Ok(())
    }

    /// Executes an ALTER action on a schema entity.
    ///
    /// This is a shared helper for both `execute_alter_label` and
    /// `execute_alter_edge_type` since they have identical logic.
    pub(crate) async fn execute_alter_entity(
        sm: &Arc<SchemaManager>,
        entity_name: &str,
        action: AlterAction,
    ) -> Result<()> {
        match action {
            AlterAction::AddProperty(prop) => {
                sm.add_property(entity_name, &prop.name, prop.data_type, prop.nullable)?;
            }
            AlterAction::DropProperty(prop_name) => {
                sm.drop_property(entity_name, &prop_name)?;
            }
            AlterAction::RenameProperty { old_name, new_name } => {
                sm.rename_property(entity_name, &old_name, &new_name)?;
            }
        }
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_alter_label(&self, clause: AlterLabelClause) -> Result<()> {
        Self::execute_alter_entity(
            &self.storage.schema_manager_arc(),
            &clause.name,
            clause.action,
        )
        .await
    }

    pub(crate) async fn execute_alter_edge_type(&self, clause: AlterEdgeTypeClause) -> Result<()> {
        Self::execute_alter_entity(
            &self.storage.schema_manager_arc(),
            &clause.name,
            clause.action,
        )
        .await
    }

    pub(crate) async fn execute_drop_label(&self, clause: DropLabelClause) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_label(&clause.name, clause.if_exists)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_drop_edge_type(&self, clause: DropEdgeTypeClause) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_edge_type(&clause.name, clause.if_exists)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_create_constraint(
        &self,
        clause: CreateConstraintClause,
    ) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        let target = match clause.target {
            AstConstraintTarget::Label(l) => ConstraintTarget::Label(l),
            AstConstraintTarget::EdgeType(e) => ConstraintTarget::EdgeType(e),
        };
        let c_type = match clause.constraint_type {
            AstConstraintType::Unique { properties } => ConstraintType::Unique { properties },
            AstConstraintType::Exists { property } => ConstraintType::Exists { property },
            AstConstraintType::Check { expression } => ConstraintType::Check {
                expression: expression.to_string_repr(),
            },
        };

        let constraint = Constraint {
            name: clause.name,
            constraint_type: c_type,
            target,
            enabled: true,
        };

        sm.add_constraint(constraint)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_drop_constraint(&self, clause: DropConstraintClause) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_constraint(&clause.name, clause.if_exists)?;
        sm.save().await?;
        Ok(())
    }

    fn get_composite_constraint(&self, label: &str) -> Option<Constraint> {
        let schema = self.storage.schema_manager().schema();
        schema
            .constraints
            .iter()
            .find(|c| {
                if !c.enabled {
                    return false;
                }
                match &c.target {
                    ConstraintTarget::Label(l) if l == label => {
                        matches!(c.constraint_type, ConstraintType::Unique { .. })
                    }
                    _ => false,
                }
            })
            .cloned()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_merge(
        &self,
        rows: Vec<HashMap<String, Value>>,
        pattern: &Pattern,
        on_match: Option<&SetClause>,
        on_create: Option<&SetClause>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let writer_lock = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("Write operation requires a Writer"))?;

        let mut results = Vec::new();
        for mut row in rows {
            // Optimization: Check for single node pattern with unique constraint
            let mut optimized_vid = None;
            if pattern.parts.len() == 1
                && let PatternPart::Node(n) = &pattern.parts[0]
                && n.labels.len() == 1
                && let Some(constraint) = self.get_composite_constraint(&n.labels[0])
                && let ConstraintType::Unique { properties } = constraint.constraint_type
            {
                let label = &n.labels[0];
                // Evaluate pattern properties
                let mut pattern_props = HashMap::new();
                for (name, expr) in &n.properties {
                    let val = self
                        .evaluate_expr(expr, &row, prop_manager, params, ctx)
                        .await?;
                    pattern_props.insert(name.clone(), val);
                }

                // Check if all constraint properties are present
                let has_all_keys = properties.iter().all(|p| pattern_props.contains_key(p));
                if has_all_keys {
                    // Extract key properties
                    let key_props: HashMap<String, Value> = properties
                        .iter()
                        .filter_map(|p| pattern_props.get(p).map(|v| (p.clone(), v.clone())))
                        .collect();

                    // Use optimized lookup
                    if let Ok(Some(vid)) = self
                        .storage
                        .index_manager()
                        .composite_lookup(label, &key_props)
                        .await
                    {
                        optimized_vid = Some((vid, pattern_props));
                    }
                }
            }

            if let Some((vid, _pattern_props)) = optimized_vid {
                // Optimized Path: Node found via index
                let mut writer = writer_lock.write().await;
                let mut match_row = row.clone();
                if let PatternPart::Node(n) = &pattern.parts[0]
                    && let Some(var) = &n.variable
                {
                    match_row.insert(var.clone(), json!(vid.as_u64()));
                    // Also inject properties into row?
                    // If we returned 'n', it should be the node object, but Executor usually handles variable binding.
                    // For simple MERGE, we just need to handle ON MATCH.
                }

                if let Some(set) = on_match {
                    self.execute_set_items_locked(
                        &set.items,
                        &mut match_row,
                        &mut writer,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await?;
                }
                results.push(match_row);
            } else {
                // Fallback to standard execution
                let matches = self
                    .execute_merge_match(pattern, &row, prop_manager, params, ctx)
                    .await?;
                let mut writer = writer_lock.write().await;
                if !matches.is_empty() {
                    for mut m in matches {
                        if let Some(set) = on_match {
                            self.execute_set_items_locked(
                                &set.items,
                                &mut m,
                                &mut writer,
                                prop_manager,
                                params,
                                ctx,
                            )
                            .await?;
                        }
                        results.push(m);
                    }
                } else {
                    self.execute_create_pattern(
                        pattern,
                        &mut row,
                        &mut writer,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await?;
                    if let Some(set) = on_create {
                        self.execute_set_items_locked(
                            &set.items,
                            &mut row,
                            &mut writer,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await?;
                    }
                    results.push(row);
                }
            }
        }
        Ok(results)
    }

    /// Execute a CROSS JOIN operation.
    pub(crate) async fn execute_create_pattern(
        &self,
        pattern: &Pattern,
        row: &mut HashMap<String, Value>,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        let mut prev_vid: Option<Vid> = None;
        type PendingRel = (String, u16, Vec<(String, Expr)>);
        let mut rel_pending: Option<PendingRel> = None;

        for part in &pattern.parts {
            match part {
                PatternPart::Node(n) => {
                    let mut vid = None;

                    // Check if node variable already bound in row
                    if let Some(var) = &n.variable
                        && let Some(val) = row.get(var)
                        && let Ok(existing_vid) = Self::vid_from_value(val)
                    {
                        vid = Some(existing_vid);
                    }

                    // If not bound, create it
                    if vid.is_none() {
                        let mut props = HashMap::new();
                        for (name, expr) in &n.properties {
                            let val = self
                                .evaluate_expr(expr, row, prop_manager, params, ctx)
                                .await?;
                            props.insert(name.clone(), val);
                        }

                        let label_name = n
                            .labels
                            .first()
                            .ok_or(anyhow!("CREATE node must have label"))?;
                        let schema = self.storage.schema_manager().schema();
                        let label_meta = schema
                            .labels
                            .get(label_name)
                            .ok_or_else(|| anyhow!("Label {} not found", label_name))?;

                        let new_vid = writer.next_vid(label_meta.id).await?;

                        // Enrich with generated columns
                        self.enrich_properties_with_generated_columns(
                            label_name,
                            &mut props,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await?;

                        writer.insert_vertex(new_vid, props).await?;

                        if let Some(var) = &n.variable {
                            row.insert(var.clone(), Value::String(new_vid.to_string()));
                        }
                        vid = Some(new_vid);
                    }

                    let current_vid = vid.unwrap();

                    if let Some((rel_var, type_id, rel_props_vec)) = rel_pending.take()
                        && let Some(src) = prev_vid
                    {
                        // Check if relationship already bound (e.g. in MERGE case where rel was matched)
                        // But create_pattern is called when NO match found for the whole pattern?
                        // In MERGE (a)-[r]->(b), if 'a' and 'b' bound but 'r' not, we create 'r'.
                        // If 'r' bound, we do nothing?
                        // execute_create_pattern is called for the WHOLE pattern if match fails.
                        // But 'a' and 'b' might have been bound by previous MATCH or MERGE.

                        // If rel_var is bound, skip creation?
                        let is_rel_bound = !rel_var.is_empty() && row.contains_key(&rel_var);

                        if !is_rel_bound {
                            let mut rel_props = HashMap::new();
                            for (name, expr) in rel_props_vec {
                                let val = self
                                    .evaluate_expr(&expr, row, prop_manager, params, ctx)
                                    .await?;
                                rel_props.insert(name, val);
                            }
                            let eid = writer.next_eid(type_id).await?;
                            writer
                                .insert_edge(src, current_vid, type_id, eid, rel_props)
                                .await?;

                            if !rel_var.is_empty() {
                                // We should probably add the edge to the row, but we need an Edge object structure?
                                // For now, EID string or object?
                                // Using object to be consistent with Traverse
                                let edge_obj = json!({
                                    "_eid": eid.as_u64(),
                                    "_src": src.as_u64(),
                                    "_dst": current_vid.as_u64(),
                                    "_type": type_id
                                });
                                row.insert(rel_var, edge_obj);
                            }
                        }
                    }
                    prev_vid = Some(current_vid);
                }
                PatternPart::Relationship(r) => {
                    if r.rel_types.len() != 1 {
                        return Err(anyhow!("CREATE relationship must specify exactly one type"));
                    }
                    let type_name = &r.rel_types[0];
                    let schema = self.storage.schema_manager().schema();
                    let type_meta = schema
                        .edge_types
                        .get(type_name)
                        .ok_or_else(|| anyhow!("Type {} not found", type_name))?;

                    if r.direction == Direction::Incoming {
                        return Err(anyhow!("CREATE only supports outgoing relationships"));
                    }
                    rel_pending = Some((
                        r.variable.clone().unwrap_or_default(),
                        type_meta.id,
                        r.properties.clone(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_set_items_locked(
        &self,
        items: &[SetItem],
        row: &mut HashMap<String, Value>,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        for item in items {
            match item {
                SetItem::Property { expr, value } => {
                    if let Expr::Property(var_expr, prop_name) = expr
                        && let Expr::Identifier(var_name) = &**var_expr
                        && let Some(node_val) = row.get(var_name)
                    {
                        if let Ok(vid) = Self::vid_from_value(node_val) {
                            let mut props = prop_manager
                                .get_all_vertex_props_with_ctx(vid, ctx)
                                .await?
                                .unwrap_or_default();
                            let val = self
                                .evaluate_expr(value, row, prop_manager, params, ctx)
                                .await?;
                            props.insert(prop_name.clone(), val);

                            // Enrich with generated columns
                            let schema = self.storage.schema_manager().schema();
                            if let Some(label_name) = schema.label_name_by_id(vid.label_id()) {
                                self.enrich_properties_with_generated_columns(
                                    label_name,
                                    &mut props,
                                    prop_manager,
                                    params,
                                    ctx,
                                )
                                .await?;
                            }

                            writer.insert_vertex(vid, props).await?;
                        } else if let Value::Object(map) = node_val
                            && let (Some(eid_v), Some(src_v), Some(dst_v), Some(type_v)) = (
                                map.get("_eid"),
                                map.get("_src"),
                                map.get("_dst"),
                                map.get("_type"),
                            )
                        {
                            let eid = uni_common::core::id::Eid::from(
                                eid_v.as_u64().ok_or(anyhow!("Invalid _eid"))?,
                            );
                            let src = Vid::from(src_v.as_u64().ok_or(anyhow!("Invalid _src"))?);
                            let dst = Vid::from(dst_v.as_u64().ok_or(anyhow!("Invalid _dst"))?);
                            let etype = type_v.as_u64().ok_or(anyhow!("Invalid _type"))? as u16;

                            let mut props = prop_manager
                                .get_all_edge_props_with_ctx(eid, ctx)
                                .await?
                                .unwrap_or_default();
                            let val = self
                                .evaluate_expr(value, row, prop_manager, params, ctx)
                                .await?;
                            props.insert(prop_name.clone(), val);
                            writer.insert_edge(src, dst, etype, eid, props).await?;
                        }
                    }
                }
                SetItem::Labels { variable, labels } => {
                    if let Some(node_val) = row.get(variable)
                        && let Ok(vid) = Self::vid_from_value(node_val)
                    {
                        let schema = self.storage.schema_manager().schema();
                        let current_label_id = vid.label_id();
                        let current_label_name = schema
                            .label_name_by_id(current_label_id)
                            .unwrap_or("Unknown");

                        for label in labels {
                            if label != current_label_name {
                                return Err(anyhow!(
                                    "Changing or adding multiple labels is not supported yet. Node {} has label {}, attempted to add {}.",
                                    vid,
                                    current_label_name,
                                    label
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Execute REMOVE clause items (property removal or label removal).
    pub(crate) async fn execute_remove_items_locked(
        &self,
        items: &[RemoveItem],
        row: &mut HashMap<String, Value>,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        for item in items {
            match item {
                RemoveItem::Property(expr) => {
                    self.execute_remove_property(expr, row, writer, prop_manager, ctx)
                        .await?;
                }
                RemoveItem::Labels { variable, labels } => {
                    self.execute_remove_labels(variable, labels, row)?;
                }
            }
        }
        Ok(())
    }

    /// Execute property removal for a vertex or edge.
    pub(crate) async fn execute_remove_property(
        &self,
        expr: &Expr,
        row: &HashMap<String, Value>,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        if let Expr::Property(var_expr, prop_name) = expr
            && let Expr::Identifier(var_name) = &**var_expr
            && let Some(node_val) = row.get(var_name)
        {
            if let Ok(vid) = Self::vid_from_value(node_val) {
                // Remove property from vertex
                let mut props = prop_manager
                    .get_all_vertex_props_with_ctx(vid, ctx)
                    .await?
                    .unwrap_or_default();
                props.insert(prop_name.clone(), Value::Null);
                writer.insert_vertex(vid, props).await?;
            } else if let Value::Object(map) = node_val {
                // Remove property from edge
                self.execute_remove_edge_property(map, prop_name, writer, prop_manager, ctx)
                    .await?;
            }
        }
        Ok(())
    }

    /// Execute property removal from an edge.
    pub(crate) async fn execute_remove_edge_property(
        &self,
        map: &serde_json::Map<String, Value>,
        prop_name: &str,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        if let (Some(eid_v), Some(src_v), Some(dst_v), Some(type_v)) = (
            map.get("_eid"),
            map.get("_src"),
            map.get("_dst"),
            map.get("_type"),
        ) {
            let eid =
                uni_common::core::id::Eid::from(eid_v.as_u64().ok_or(anyhow!("Invalid _eid"))?);
            let src = Vid::from(src_v.as_u64().ok_or(anyhow!("Invalid _src"))?);
            let dst = Vid::from(dst_v.as_u64().ok_or(anyhow!("Invalid _dst"))?);
            let etype = type_v.as_u64().ok_or(anyhow!("Invalid _type"))? as u16;

            let mut props = prop_manager
                .get_all_edge_props_with_ctx(eid, ctx)
                .await?
                .unwrap_or_default();
            props.insert(prop_name.to_string(), Value::Null);
            writer.insert_edge(src, dst, etype, eid, props).await?;
        }
        Ok(())
    }

    /// Execute label removal (currently not supported).
    pub(crate) fn execute_remove_labels(
        &self,
        variable: &str,
        labels: &[String],
        row: &HashMap<String, Value>,
    ) -> Result<()> {
        if let Some(node_val) = row.get(variable)
            && let Ok(vid) = Self::vid_from_value(node_val)
        {
            let schema = self.storage.schema_manager().schema();
            let current_label_id = vid.label_id();
            let current_label_name = schema
                .label_name_by_id(current_label_id)
                .unwrap_or("Unknown");

            for label in labels {
                if label == current_label_name {
                    return Err(anyhow!(
                        "Removing the primary label is not supported yet. Node {} has label {}.",
                        vid,
                        current_label_name
                    ));
                }
            }
        }
        Ok(())
    }

    /// Execute DELETE clause for a single item (vertex or edge).
    pub(crate) async fn execute_delete_item_locked(
        &self,
        val: &Value,
        detach: bool,
        writer: &mut Writer,
    ) -> Result<()> {
        if let Ok(vid) = Self::vid_from_value(val) {
            self.execute_delete_vertex(vid, detach, writer).await?;
        } else if let Value::Object(map) = val {
            self.execute_delete_edge_from_map(map, writer).await?;
        }
        Ok(())
    }

    /// Execute vertex deletion with optional detach.
    pub(crate) async fn execute_delete_vertex(
        &self,
        vid: Vid,
        detach: bool,
        writer: &mut Writer,
    ) -> Result<()> {
        if detach {
            self.detach_delete_vertex(vid, writer).await?;
        } else {
            self.check_vertex_has_no_edges(vid, writer).await?;
        }
        writer.delete_vertex(vid).await?;
        Ok(())
    }

    /// Check that a vertex has no edges (required for non-DETACH DELETE).
    pub(crate) async fn check_vertex_has_no_edges(&self, vid: Vid, writer: &Writer) -> Result<()> {
        let schema = self.storage.schema_manager().schema();
        let edge_type_ids: Vec<u16> = schema.edge_types.values().map(|m| m.id).collect();

        let has_out = self
            .storage
            .load_subgraph_cached(
                &[vid],
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Outgoing,
                Some(writer.l0_manager.get_current()),
            )
            .await?
            .edges()
            .next()
            .is_some();

        let has_in = self
            .storage
            .load_subgraph_cached(
                &[vid],
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Incoming,
                Some(writer.l0_manager.get_current()),
            )
            .await?
            .edges()
            .next()
            .is_some();

        if has_out || has_in {
            return Err(anyhow!(
                "Cannot delete node {}, because it still has relationships. To delete the node and its relationships, use DETACH DELETE.",
                vid
            ));
        }
        Ok(())
    }

    /// Execute edge deletion from a map representation.
    pub(crate) async fn execute_delete_edge_from_map(
        &self,
        map: &serde_json::Map<String, Value>,
        writer: &mut Writer,
    ) -> Result<()> {
        if let (Some(eid_v), Some(src_v), Some(dst_v), Some(type_v)) = (
            map.get("_eid"),
            map.get("_src"),
            map.get("_dst"),
            map.get("_type"),
        ) {
            let eid =
                uni_common::core::id::Eid::from(eid_v.as_u64().ok_or(anyhow!("Invalid _eid"))?);
            let src = Vid::from(src_v.as_u64().ok_or(anyhow!("Invalid _src"))?);
            let dst = Vid::from(dst_v.as_u64().ok_or(anyhow!("Invalid _dst"))?);
            let etype = type_v.as_u64().ok_or(anyhow!("Invalid _type"))? as u16;
            writer.delete_edge(eid, src, dst, etype).await?;
        }
        Ok(())
    }

    pub(crate) async fn execute_merge_match(
        &self,
        pattern: &Pattern,
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Construct a LogicalPlan for the MATCH part of MERGE
        let planner = crate::query::planner::QueryPlanner::new(Arc::new(
            self.storage.schema_manager().schema().clone(),
        ));

        // We need to construct a CypherQuery to use the planner's plan() method,
        // or we can manually construct the LogicalPlan.
        // Manual construction is safer as we don't have to round-trip through AST.

        let mut plan = LogicalPlan::Empty;
        let mut vars_in_scope = Vec::new();

        // Add existing bound variables from row to scope
        for key in row.keys() {
            vars_in_scope.push(key.clone());
        }

        // Reconstruct Match logic from Planner (simplified for MERGE pattern)
        let parts = &pattern.parts;
        let mut i = 0;
        while i < parts.len() {
            let part = &parts[i];
            match part {
                PatternPart::Node(n) => {
                    let variable = n.variable.clone().unwrap_or_default();

                    // If variable is already bound in the input row, we filter
                    let is_bound = !variable.is_empty() && row.contains_key(&variable);

                    if is_bound {
                        // If bound, we must Scan this specific VID to start the chain
                        // Extract VID from row
                        let val = row.get(&variable).unwrap();
                        let vid = Self::vid_from_value(val)?;

                        // We need the label for Scan
                        let label_id = vid.label_id();
                        // Find label name? Not strictly needed for Scan LogicalPlan if we had ID,
                        // but LogicalPlan::Scan takes ID.

                        let prop_filter = planner.properties_to_expr(&variable, &n.properties);

                        // Create a filter expression for VID: variable._vid = vid
                        // But our expression engine handles `Expr::Identifier` as column.
                        // We can inject a filter `id(variable) = vid` if we had `id()` function.
                        // Or we use internal property `_vid`.

                        // Note: Scan supports `filter`.
                        // We can manually construct an Expr::BinaryOp(Eq, Prop(var, _vid), Literal(vid))

                        let vid_filter = Expr::BinaryOp {
                            left: Box::new(Expr::Property(
                                Box::new(Expr::Identifier(variable.clone())),
                                "_vid".to_string(),
                            )),
                            op: Operator::Eq,
                            right: Box::new(Expr::Literal(json!(vid.as_u64()))),
                        };

                        let combined_filter = if let Some(pf) = prop_filter {
                            Some(Expr::BinaryOp {
                                left: Box::new(vid_filter),
                                op: Operator::And,
                                right: Box::new(pf),
                            })
                        } else {
                            Some(vid_filter)
                        };

                        let scan = LogicalPlan::Scan {
                            label_id,
                            variable: variable.clone(),
                            filter: combined_filter,
                            optional: false,
                        };

                        if matches!(plan, LogicalPlan::Empty) {
                            plan = scan;
                        } else {
                            plan = LogicalPlan::CrossJoin {
                                left: Box::new(plan),
                                right: Box::new(scan),
                            };
                        }
                    } else {
                        if n.labels.is_empty() {
                            return Err(anyhow!("MERGE node must have a label"));
                        }
                        let label_name = &n.labels[0];
                        let schema = self.storage.schema_manager().schema();
                        let label_meta = schema
                            .labels
                            .get(label_name)
                            .ok_or_else(|| anyhow!("Label {} not found", label_name))?;

                        let prop_filter = planner.properties_to_expr(&variable, &n.properties);
                        let scan = LogicalPlan::Scan {
                            label_id: label_meta.id,
                            variable: variable.clone(),
                            filter: prop_filter,
                            optional: false, // MERGE MATCH is strict
                        };

                        if matches!(plan, LogicalPlan::Empty) {
                            plan = scan;
                        } else {
                            plan = LogicalPlan::CrossJoin {
                                left: Box::new(plan),
                                right: Box::new(scan),
                            };
                        }

                        if !variable.is_empty() {
                            vars_in_scope.push(variable.clone());
                        }
                    }

                    // Now look ahead for relationship
                    i += 1;
                    while i < parts.len() {
                        if let PatternPart::Relationship(r) = &parts[i] {
                            let target_node_part = &parts[i + 1];
                            if let PatternPart::Node(n_target) = target_node_part {
                                let schema = self.storage.schema_manager().schema();
                                let mut edge_type_ids = Vec::new();

                                if r.rel_types.is_empty() {
                                    return Err(anyhow!("MERGE edge must have a type"));
                                } else if r.rel_types.len() > 1 {
                                    return Err(anyhow!(
                                        "MERGE does not support multiple edge types"
                                    ));
                                } else {
                                    let type_name = &r.rel_types[0];
                                    let edge_meta =
                                        schema.edge_types.get(type_name).ok_or_else(|| {
                                            anyhow!("Edge type {} not found", type_name)
                                        })?;
                                    edge_type_ids.push(edge_meta.id);
                                }

                                let target_label_meta = if let Some(lbl) = n_target.labels.first() {
                                    schema
                                        .labels
                                        .get(lbl)
                                        .ok_or_else(|| anyhow!("Label {} not found", lbl))?
                                } else if let Some(var) = &n_target.variable {
                                    if let Some(val) = row.get(var) {
                                        if let Ok(vid) = Self::vid_from_value(val) {
                                            schema
                                                .labels
                                                .values()
                                                .find(|m| m.id == vid.label_id())
                                                .ok_or(anyhow!(
                                                    "Label ID {} not found",
                                                    vid.label_id()
                                                ))?
                                        } else {
                                            return Err(anyhow!("Variable {} is not a node", var));
                                        }
                                    } else {
                                        return Err(anyhow!(
                                            "MERGE pattern node must have a label or be a bound variable"
                                        ));
                                    }
                                } else {
                                    return Err(anyhow!("MERGE pattern node must have a label"));
                                };

                                let target_variable = n_target.variable.clone().unwrap_or_default();
                                let source_variable = parts[i - 1]
                                    .as_node()
                                    .unwrap()
                                    .variable
                                    .clone()
                                    .unwrap_or_default();

                                plan = LogicalPlan::Traverse {
                                    input: Box::new(plan),
                                    edge_type_ids,
                                    direction: r.direction.clone(),
                                    source_variable,
                                    target_variable: target_variable.clone(),
                                    target_label_id: target_label_meta.id,
                                    step_variable: r.variable.clone(),
                                    min_hops: r.min_hops.unwrap_or(1) as usize,
                                    max_hops: r.max_hops.unwrap_or(1) as usize,
                                    optional: false,
                                    target_filter: None,
                                    path_variable: None,
                                };

                                // Apply property filters for relationship
                                if !r.properties.is_empty()
                                    && let Some(r_var) = &r.variable
                                    && let Some(prop_filter) =
                                        planner.properties_to_expr(r_var, &r.properties)
                                {
                                    plan = LogicalPlan::Filter {
                                        input: Box::new(plan),
                                        predicate: prop_filter,
                                    };
                                }

                                // Apply property filters for target node if it was new
                                if !target_variable.is_empty() {
                                    if let Some(prop_filter) = planner
                                        .properties_to_expr(&target_variable, &n_target.properties)
                                    {
                                        plan = LogicalPlan::Filter {
                                            input: Box::new(plan),
                                            predicate: prop_filter,
                                        };
                                    }
                                    vars_in_scope.push(target_variable.clone());
                                }

                                if let Some(sv) = &r.variable {
                                    vars_in_scope.push(sv.clone());
                                }
                                i += 2;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
                _ => return Err(anyhow!("Pattern must start with a node")),
            }
        }

        // Execute the plan
        // We need to inject the current row into the execution if the plan starts with Empty?
        // Actually, if we use CrossJoin with existing row, we can simulate the context.
        // But Scan operators ignore input row usually?
        // Our LogicalPlan execution model passes the result of previous step as input to next.
        // Scan ignores input usually.
        // But if we bind variables from `row`, we need to ensure they are available.

        // The simple Scan operator returns all nodes.
        // If we want to filter by existing `row` values, we need to inject `row` into the stream?
        // Or we treat `row` as initial state.

        // Since `execute` takes a plan, and `Scan` generates new rows...
        // If `plan` is `CrossJoin(Empty, Scan)`, and `Empty` produces 1 row (empty map), then result is Scan rows.
        // But we want to filter `Scan` rows based on `row` values if variable matches.

        // Actually, `evaluate_expr` uses `row`.
        // If we have `LogicalPlan::Filter`, it uses the row passed to it.
        // But `Scan` produces NEW rows.
        // We need to carry over the `row` context.

        // Solution: Use `LogicalPlan::Project` or similar to inject initial context?
        // Or simply execute the plan, which returns all matches in DB, and then filter/join with `row`.

        // BUT: execute_subplan handles the flow.
        // If we pass `row` as initial context? No, `execute` starts from scratch.

        // Strategy:
        // 1. Execute the plan to find ALL matches in the DB that satisfy the pattern.
        // 2. Filter the results to keep only those that match the BOUND variables in `row`.

        let db_matches = self
            .execute_subplan(plan, prop_manager, params, ctx)
            .await?;

        let mut final_matches = Vec::new();
        for db_match in db_matches {
            // Check consistency with input row
            let mut consistent = true;
            for (key, val) in row {
                if let Some(db_val) = db_match.get(key)
                    && db_val != val
                {
                    consistent = false;
                    break;
                }
            }

            if consistent {
                // Merge db_match into row
                let mut merged = row.clone();
                merged.extend(db_match);
                final_matches.push(merged);
            }
        }

        Ok(final_matches)
    }
}
