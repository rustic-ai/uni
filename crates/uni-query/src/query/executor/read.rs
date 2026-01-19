// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::ast::{
    ConstraintTarget as AstConstraintTarget, Direction, ShowConstraintsClause, UnaryOperator,
};
use crate::query::datetime::parse_datetime_utc;
use crate::query::expr::Expr;
use crate::query::expr_eval::{eval_binary_op, eval_scalar_function, eval_vector_similarity};
use crate::query::planner::{LogicalPlan, QueryPlanner};
use crate::query::pushdown::LanceFilterGenerator;
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use metrics;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;
use tracing::instrument;
use uni_common::core::id::Vid;
use uni_common::core::schema::{ConstraintTarget, ConstraintType, DataType, SchemaManager};
use uni_store::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::arrow_convert;
use uni_store::storage::index_manager::IndexManager;

use crate::query::vectorized::batch::VectorizedBatch;
use crate::query::vectorized::operators::ExecutionContext;
use crate::query::vectorized::planner::PhysicalPlanner;
use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, ListArray, RecordBatch, StringArray, StructArray,
    UInt64Array,
};
use csv;
use parquet;

/// Helper struct for extracting L0-related components from a `QueryContext`.
///
/// This reduces duplication when building `ExecutionContext` instances.
use super::core::*;

impl Executor {
    pub(crate) async fn scan_storage_candidates(
        &self,
        label_id: u16,
        variable: &str,
        filter: Option<&Expr>,
    ) -> Result<Vec<Vid>> {
        let schema = self.storage.schema_manager().schema();
        let label_name = schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

        let ds = self.storage.vertex_dataset(label_name)?;
        match ds.open().await {
            Ok(lance_ds) => {
                let mut scanner = lance_ds.scan();
                if let Some(expr) = filter
                    && let Some(sql) =
                        LanceFilterGenerator::generate(std::slice::from_ref(expr), variable)
                {
                    scanner.filter(&sql)?;
                }
                scanner.project(&["_vid"])?;
                let mut stream = scanner.try_into_stream().await?;
                let mut vids = Vec::new();
                use arrow_array::UInt64Array;
                use futures::TryStreamExt;
                while let Some(batch) = stream.try_next().await? {
                    let vid_col = batch
                        .column_by_name("_vid")
                        .ok_or(anyhow!("Missing _vid"))?
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or(anyhow!("Invalid _vid"))?;
                    for i in 0..batch.num_rows() {
                        vids.push(Vid::from(vid_col.value(i)));
                    }
                }
                Ok(vids)
            }
            Err(e) => {
                // Only treat "not found" / "does not exist" errors as empty results.
                // Propagate all other errors (network, auth, corruption, etc.)
                let err_msg = e.to_string().to_lowercase();
                if err_msg.contains("not found")
                    || err_msg.contains("does not exist")
                    || err_msg.contains("no such file")
                    || err_msg.contains("object not found")
                {
                    Ok(Vec::new())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub(crate) async fn scan_label_with_filter(
        &self,
        label_id: u16,
        variable: &str,
        filter: Option<&Expr>,
        ctx: Option<&QueryContext>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<Vid>> {
        let mut candidates = self
            .scan_storage_candidates(label_id, variable, filter)
            .await?;

        if let Some(ctx) = ctx {
            // Main L0
            candidates.extend(ctx.l0.read().vids_for_label(label_id));

            // Transaction L0
            if let Some(tx_l0_arc) = &ctx.transaction_l0 {
                candidates.extend(tx_l0_arc.read().vids_for_label(label_id));
            }

            // Pending flush L0s (data being flushed that's still visible)
            for pending_l0_arc in &ctx.pending_flush_l0s {
                candidates.extend(pending_l0_arc.read().vids_for_label(label_id));
            }
        }

        candidates.sort_unstable();
        candidates.dedup();

        let mut verified_vids = Vec::new();
        for vid in candidates {
            let props_opt = prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await?;

            if props_opt.is_none() {
                continue; // Deleted
            }
            let props = props_opt.unwrap();

            if let Some(expr) = filter {
                let mut props_json: serde_json::Map<String, Value> = props.into_iter().collect();
                // Inject _vid for filtering
                props_json.insert("_vid".to_string(), json!(vid.as_u64()));

                let mut row = HashMap::new();
                row.insert(variable.to_string(), Value::Object(props_json));

                let res = self
                    .evaluate_expr(expr, &row, prop_manager, params, ctx)
                    .await?;
                if res.as_bool().unwrap_or(false) {
                    verified_vids.push(vid);
                }
            } else {
                verified_vids.push(vid);
            }
        }

        Ok(verified_vids)
    }

    pub(crate) fn vid_from_value(val: &Value) -> Result<Vid> {
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

    pub async fn execute_vectorized(
        &self,
        plan: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        config: uni_common::config::UniConfig,
    ) -> Result<VectorizedBatch> {
        let ctx = self.get_context().await;

        let planner =
            PhysicalPlanner::new(Arc::new(self.storage.schema_manager().schema().clone()));
        let phys_plan = planner
            .plan(&plan)
            .map_err(|e| anyhow!("Vectorized planning failed: {}", e))?;

        let l0_comp = L0Components::from_query_context(&ctx);

        let exec_ctx = ExecutionContext {
            storage: self.storage.clone(),
            property_manager: Arc::new(PropertyManager::new(
                self.storage.clone(),
                self.storage.schema_manager_arc(),
                prop_manager.cache_size(),
            )),
            l0: l0_comp.l0,
            transaction_l0: l0_comp.transaction_l0,
            pending_flush_l0s: l0_comp.pending_flush_l0s,
            writer: self.writer.clone(),
            config: config.clone(),
            deadline: Some(Instant::now() + config.query_timeout),
            current_memory: Arc::new(AtomicUsize::new(0)),
            ctes: params.clone(),
        };

        phys_plan.execute(&exec_ctx).await
    }

    #[instrument(
        skip(self, prop_manager, params),
        fields(rows_returned, duration_ms),
        level = "info"
    )]
    pub fn execute<'a>(
        &'a self,
        plan: LogicalPlan,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
    ) -> BoxFuture<'a, Result<Vec<HashMap<String, Value>>>> {
        let config = self.config.clone();
        Box::pin(async move {
            println!("DEBUG: Executor::execute plan: {:?}", plan);
            let query_type = Self::get_plan_type(&plan);
            let ctx = self.get_context().await;
            let start = Instant::now();

            // Try vectorized execution
            let res = match self
                .execute_vectorized(plan.clone(), prop_manager, params, config)
                .await
            {
                Ok(batch) => self.batch_to_rows(batch),
                Err(e) => {
                    log::debug!(
                        "Vectorized execution failed (might be expected for some plans): {}",
                        e
                    );
                    if e.to_string().contains("Query timed out")
                        || e.to_string().contains("Query exceeded memory limit")
                    {
                        return Err(e);
                    }
                    self.execute_subplan(plan, prop_manager, params, ctx.as_ref())
                        .await
                }
            };

            let duration = start.elapsed();
            metrics::histogram!("uni_query_duration_seconds", "query_type" => query_type)
                .record(duration.as_secs_f64());

            tracing::Span::current().record("duration_ms", duration.as_millis());
            match &res {
                Ok(rows) => {
                    tracing::Span::current().record("rows_returned", rows.len());
                    metrics::counter!("uni_query_rows_returned_total", "query_type" => query_type)
                        .increment(rows.len() as u64);
                }
                Err(e) => {
                    let error_type = if e.to_string().contains("timed out") {
                        "timeout"
                    } else if e.to_string().contains("syntax") {
                        "syntax"
                    } else {
                        "execution"
                    };
                    metrics::counter!("uni_query_errors_total", "query_type" => query_type, "error_type" => error_type).increment(1);
                }
            }

            res
        })
    }

    fn get_plan_type(plan: &LogicalPlan) -> &'static str {
        match plan {
            LogicalPlan::Scan { .. } => "read_scan",
            LogicalPlan::Traverse { .. } => "read_traverse",
            LogicalPlan::VectorKnn { .. } => "read_vector",
            LogicalPlan::Create { .. } => "write_create",
            LogicalPlan::Merge { .. } => "write_merge",
            LogicalPlan::Delete { .. } => "write_delete",
            LogicalPlan::Set { .. } => "write_set",
            LogicalPlan::Remove { .. } => "write_remove",
            LogicalPlan::ProcedureCall { .. } => "call",
            LogicalPlan::Copy { .. } => "copy",
            LogicalPlan::Backup { .. } => "backup",
            _ => "other",
        }
    }

    pub fn execute_stream(
        self,
        plan: LogicalPlan,
        prop_manager: Arc<PropertyManager>,
        params: HashMap<String, Value>,
    ) -> BoxStream<'static, Result<Vec<HashMap<String, Value>>>> {
        let this = self;
        let this_for_ctx = this.clone();

        let ctx_stream = stream::once(async move { this_for_ctx.get_context().await });

        ctx_stream
            .flat_map(move |ctx| {
                let plan = plan.clone();
                let this = this.clone();
                let prop_manager = prop_manager.clone();
                let params = params.clone();

                // Try vectorized execution
                let planner =
                    PhysicalPlanner::new(Arc::new(this.storage.schema_manager().schema().clone()));
                match planner.plan(&plan) {
                    Ok(phys_plan) => {
                        let l0_comp = L0Components::from_query_context(&ctx);
                        let exec_ctx = ExecutionContext {
                            storage: this.storage.clone(),
                            property_manager: prop_manager.clone(),
                            l0: l0_comp.l0,
                            transaction_l0: l0_comp.transaction_l0,
                            pending_flush_l0s: l0_comp.pending_flush_l0s,
                            writer: this.writer.clone(),
                            config: this.config.clone(),
                            deadline: Some(Instant::now() + this.config.query_timeout),
                            current_memory: Arc::new(AtomicUsize::new(0)),
                            ctes: params.clone(),
                        };
                        phys_plan
                            .execute_stream(None, exec_ctx)
                            .map(move |batch_res| {
                                batch_res.and_then(|batch| this.batch_to_rows(batch))
                            })
                            .boxed()
                    }
                    Err(e) => {
                        log::debug!("Vectorized planning failed, falling back to legacy: {}", e);
                        // Legacy fallback
                        let fut = async move {
                            this.execute_subplan(plan, &prop_manager, &params, ctx.as_ref())
                                .await
                        };
                        stream::once(fut).boxed()
                    }
                }
            })
            .boxed()
    }

    pub(crate) fn batch_to_rows(
        &self,
        batch: VectorizedBatch,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut rows = Vec::new();
        let num_rows = batch.num_rows();

        for i in 0..num_rows {
            if let Some(selection) = &batch.selection
                && !selection.value(i)
            {
                continue;
            }

            let mut row = HashMap::new();
            for (var, &col_idx) in &batch.variables {
                let col = batch.data.column(col_idx);
                let val = if col.is_null(i) {
                    Value::Null
                } else if let Some(u) = col.as_any().downcast_ref::<UInt64Array>() {
                    let vid_u64 = u.value(i);
                    // Check if this is a property column (contains ".") or a node variable
                    // Property columns like "b.id" should stay as numbers
                    // Node variables like "b" should be converted to VID strings
                    // Function results like "LENGTH(p)" should also stay as numbers
                    if var.contains('.') || var.contains('(') {
                        // Property value or Function result - keep as number
                        json!(vid_u64)
                    } else {
                        // Node variable - convert to VID string
                        let vid = Vid::from(vid_u64);
                        Value::String(vid.to_string())
                    }
                } else if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
                    Value::String(s.value(i).to_string())
                } else if let Some(b) = col.as_any().downcast_ref::<BooleanArray>() {
                    Value::Bool(b.value(i))
                } else if let Some(i64_arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>()
                {
                    json!(i64_arr.value(i))
                } else if let Some(i32_arr) = col.as_any().downcast_ref::<arrow_array::Int32Array>()
                {
                    json!(i32_arr.value(i))
                } else if let Some(f64_arr) =
                    col.as_any().downcast_ref::<arrow_array::Float64Array>()
                {
                    json!(f64_arr.value(i))
                } else if let Some(f32_arr) =
                    col.as_any().downcast_ref::<arrow_array::Float32Array>()
                {
                    json!(f32_arr.value(i))
                } else if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
                    // Handle List<UInt64> (e.g., path of EIDs)
                    let val_array = list.value(i);
                    if let Some(u64_arr) = val_array.as_any().downcast_ref::<UInt64Array>() {
                        let values: Vec<Value> = u64_arr
                            .iter()
                            .map(|opt| match opt {
                                Some(v) => json!(v),
                                None => Value::Null,
                            })
                            .collect();
                        Value::Array(values)
                    } else {
                        // Use generic fallback
                        Self::arrow_to_value(col.as_ref(), i)
                    }
                } else if col.as_any().downcast_ref::<FixedSizeListArray>().is_some()
                    || col.as_any().downcast_ref::<StructArray>().is_some()
                {
                    Self::arrow_to_value(col.as_ref(), i)
                } else if col.data_type() == &arrow_schema::DataType::Null {
                    Value::Null
                } else {
                    Value::String(format!("Unimplemented type {:?}", col.data_type()))
                };
                row.insert(var.clone(), val);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    /// Convert an Arrow array element at a given row index to a JSON Value.
    /// Delegates to the shared implementation in arrow_convert module.
    pub(crate) fn arrow_to_value(col: &dyn Array, row: usize) -> Value {
        arrow_convert::arrow_to_value(col, row)
    }

    pub(crate) fn evaluate_expr<'a>(
        &'a self,
        expr: &'a Expr,
        row: &'a HashMap<String, Value>,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> BoxFuture<'a, Result<Value>> {
        let this = self;
        Box::pin(async move {
            // First check if the expression itself is already pre-computed in the row
            let repr = expr.to_string_repr();
            if let Some(val) = row.get(&repr) {
                return Ok(val.clone());
            }

            match expr {
                Expr::Identifier(name) => {
                    if let Some(val) = row.get(name) {
                        Ok(val.clone())
                    } else {
                        Ok(params.get(name).cloned().unwrap_or(Value::Null))
                    }
                }
                Expr::Parameter(name) => Ok(params.get(name).cloned().unwrap_or(Value::Null)),
                Expr::Property(var_expr, prop_name) => {
                    let base_val = this
                        .evaluate_expr(var_expr, row, prop_manager, params, ctx)
                        .await?;

                    // Handle system properties _vid and _id directly
                    if (prop_name == "_vid" || prop_name == "_id")
                        && let Ok(vid) = Self::vid_from_value(&base_val)
                    {
                        return Ok(json!(vid.as_u64()));
                    }

                    if let Ok(vid) = Self::vid_from_value(&base_val) {
                        return prop_manager
                            .get_vertex_prop_with_ctx(vid, prop_name, ctx)
                            .await;
                    }
                    if let Value::Object(map) = &base_val {
                        if let Some(val) = map.get(prop_name.as_str()) {
                            return Ok(val.clone());
                        }
                        // Fallback to storage lookup
                        if let Some(id) = map.get("_vid").and_then(|v| v.as_u64()) {
                            let vid = Vid::from(id);
                            if let Ok(val) = prop_manager
                                .get_vertex_prop_with_ctx(vid, prop_name, ctx)
                                .await
                            {
                                return Ok(val);
                            }
                        } else if let Some(id) = map.get("_eid").and_then(|v| v.as_u64()) {
                            let eid = uni_common::core::id::Eid::from(id);
                            if let Ok(val) = prop_manager.get_edge_prop(eid, prop_name, ctx).await {
                                return Ok(val);
                            }
                        }
                        return Ok(Value::Null);
                    }
                    if base_val.is_null() {
                        return Ok(Value::Null);
                    }
                    Err(anyhow!(
                        "Cannot access property '{}' on {:?}",
                        prop_name,
                        base_val
                    ))
                }
                Expr::ArrayIndex(arr_expr, idx_expr) => {
                    let arr_val = this
                        .evaluate_expr(arr_expr, row, prop_manager, params, ctx)
                        .await?;
                    let idx_val = this
                        .evaluate_expr(idx_expr, row, prop_manager, params, ctx)
                        .await?;

                    if let Value::Array(arr) = &arr_val
                        && let Some(i) = idx_val.as_u64()
                    {
                        let idx = i as usize;
                        if idx < arr.len() {
                            return Ok(arr[idx].clone());
                        }
                        return Ok(Value::Null);
                    }
                    if arr_val.is_null() {
                        return Ok(Value::Null);
                    }
                    Err(anyhow!("Cannot index into {:?}", arr_val))
                }
                Expr::Literal(val) => Ok(val.clone()),
                Expr::List(items) => {
                    let mut vals = Vec::new();
                    for item in items {
                        vals.push(
                            this.evaluate_expr(item, row, prop_manager, params, ctx)
                                .await?,
                        );
                    }
                    Ok(Value::Array(vals))
                }
                Expr::Map(items) => {
                    let mut map = serde_json::Map::new();
                    for (key, value_expr) in items {
                        let val = this
                            .evaluate_expr(value_expr, row, prop_manager, params, ctx)
                            .await?;
                        map.insert(key.clone(), val);
                    }
                    Ok(Value::Object(map))
                }
                Expr::Exists(query) => {
                    // Create planner and plan the subquery with current scope
                    let planner =
                        QueryPlanner::new(Arc::new(this.storage.schema_manager().schema().clone()));

                    let vars_in_scope: Vec<String> = row.keys().cloned().collect();

                    // We need to handle any error from planning/execution and turn into Result<Value>
                    match planner.plan_with_scope(*query.clone(), vars_in_scope) {
                        Ok(plan) => {
                            // Merge row into params
                            let mut sub_params = params.clone();
                            sub_params.extend(row.clone());

                            match this.execute(plan, prop_manager, &sub_params).await {
                                Ok(results) => Ok(Value::Bool(!results.is_empty())),
                                Err(e) => Err(anyhow!("Subquery execution failed: {}", e)),
                            }
                        }
                        Err(e) => Err(anyhow!("Subquery planning failed: {}", e)),
                    }
                }
                Expr::ScalarSubquery(_) => {
                    // ScalarSubquery is an optimization for vectorized execution.
                    // Legacy executor evaluates Expr::Exists directly as above.
                    // If we encounter ScalarSubquery here, it means AST was mutated or planned
                    // by something expecting vectorization but fell back to legacy.
                    // However, legacy executor doesn't have the compiled subquery plan cache.
                    // So we treat it as an error or TODO.
                    // Given that LogicalPlan doesn't store compiled plans for expressions,
                    // but the PhysicalPlanner (vectorized) does, this variant should
                    // ideally not reach the legacy executor if we are careful.
                    // But to satisfy exhaustiveness:
                    Err(anyhow!("ScalarSubquery not supported in legacy executor"))
                }
                Expr::BinaryOp { left, op, right } => {
                    let l_val = this
                        .evaluate_expr(left, row, prop_manager, params, ctx)
                        .await?;
                    let r_val = this
                        .evaluate_expr(right, row, prop_manager, params, ctx)
                        .await?;
                    eval_binary_op(&l_val, op, &r_val)
                }
                Expr::UnaryOp { op, expr } => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    match op {
                        UnaryOperator::Not => {
                            let b = val.as_bool().ok_or(anyhow!("Expected bool for NOT"))?;
                            Ok(Value::Bool(!b))
                        }
                    }
                }
                Expr::IsNull(expr) => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    Ok(Value::Bool(val.is_null()))
                }
                Expr::IsNotNull(expr) => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    Ok(Value::Bool(!val.is_null()))
                }
                Expr::Case {
                    expr,
                    when_then,
                    else_expr,
                } => {
                    if let Some(base_expr) = expr {
                        let base_val = this
                            .evaluate_expr(base_expr, row, prop_manager, params, ctx)
                            .await?;
                        for (w, t) in when_then {
                            let w_val = this
                                .evaluate_expr(w, row, prop_manager, params, ctx)
                                .await?;
                            if base_val == w_val {
                                return this.evaluate_expr(t, row, prop_manager, params, ctx).await;
                            }
                        }
                    } else {
                        for (w, t) in when_then {
                            let w_val = this
                                .evaluate_expr(w, row, prop_manager, params, ctx)
                                .await?;
                            if w_val.as_bool() == Some(true) {
                                return this.evaluate_expr(t, row, prop_manager, params, ctx).await;
                            }
                        }
                    }
                    if let Some(e) = else_expr {
                        return this.evaluate_expr(e, row, prop_manager, params, ctx).await;
                    }
                    Ok(Value::Null)
                }
                Expr::Wildcard => Ok(Value::Null),
                Expr::FunctionCall { name, args, .. } => {
                    // Special case: COALESCE needs short-circuit evaluation
                    if name.eq_ignore_ascii_case("COALESCE") {
                        for arg in args {
                            let val = this
                                .evaluate_expr(arg, row, prop_manager, params, ctx)
                                .await?;
                            if !val.is_null() {
                                return Ok(val);
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: vector_similarity has dedicated implementation
                    if name.eq_ignore_ascii_case("vector_similarity") {
                        if args.len() != 2 {
                            return Err(anyhow!("vector_similarity takes 2 arguments"));
                        }
                        let v1 = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        let v2 = this
                            .evaluate_expr(&args[1], row, prop_manager, params, ctx)
                            .await?;
                        return eval_vector_similarity(&v1, &v2);
                    }

                    // Special case: uni.validAt handles node fetching
                    if name.eq_ignore_ascii_case("uni.validAt")
                        || name.eq_ignore_ascii_case("validAt")
                    {
                        if args.len() != 4 {
                            return Err(anyhow!("validAt requires 4 arguments"));
                        }
                        let node_val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        let start_prop = this
                            .evaluate_expr(&args[1], row, prop_manager, params, ctx)
                            .await?
                            .as_str()
                            .ok_or(anyhow!("start_prop must be string"))?
                            .to_string();
                        let end_prop = this
                            .evaluate_expr(&args[2], row, prop_manager, params, ctx)
                            .await?
                            .as_str()
                            .ok_or(anyhow!("end_prop must be string"))?
                            .to_string();
                        let time_val = this
                            .evaluate_expr(&args[3], row, prop_manager, params, ctx)
                            .await?;

                        let time_str = time_val
                            .as_str()
                            .ok_or(anyhow!("time argument must be string"))?;
                        let query_time = parse_datetime_utc(time_str)
                            .map_err(|_| anyhow!("Invalid query time format: {}", time_str))?;

                        // Fetch temporal property values - supports both vertices and edges
                        let valid_from_val = if let Ok(vid) = Self::vid_from_value(&node_val) {
                            // Vertex case - VID string format
                            prop_manager
                                .get_vertex_prop_with_ctx(vid, &start_prop, ctx)
                                .await
                                .ok()
                        } else if let Value::Object(map) = &node_val {
                            // Check for embedded _vid or _eid in object
                            if let Some(vid_val) = map.get("_vid").and_then(|v| v.as_u64()) {
                                let vid = Vid::from(vid_val);
                                prop_manager
                                    .get_vertex_prop_with_ctx(vid, &start_prop, ctx)
                                    .await
                                    .ok()
                            } else if let Some(eid_val) = map.get("_eid").and_then(|v| v.as_u64()) {
                                // Edge case
                                let eid = uni_common::core::id::Eid::from(eid_val);
                                prop_manager.get_edge_prop(eid, &start_prop, ctx).await.ok()
                            } else {
                                // Inline object - property embedded directly
                                map.get(&start_prop).cloned()
                            }
                        } else {
                            return Ok(Value::Bool(false));
                        };

                        let valid_from = match valid_from_val {
                            Some(Value::String(s)) => parse_datetime_utc(&s).map_err(|_| {
                                anyhow!("Invalid datetime in {}: {}", start_prop, s)
                            })?,
                            Some(Value::Null) | None => return Ok(Value::Bool(false)),
                            _ => {
                                return Err(anyhow!(
                                    "Property {} must be a datetime string",
                                    start_prop
                                ));
                            }
                        };

                        let valid_to_val = if let Ok(vid) = Self::vid_from_value(&node_val) {
                            // Vertex case - VID string format
                            prop_manager
                                .get_vertex_prop_with_ctx(vid, &end_prop, ctx)
                                .await
                                .ok()
                        } else if let Value::Object(map) = &node_val {
                            // Check for embedded _vid or _eid in object
                            if let Some(vid_val) = map.get("_vid").and_then(|v| v.as_u64()) {
                                let vid = Vid::from(vid_val);
                                prop_manager
                                    .get_vertex_prop_with_ctx(vid, &end_prop, ctx)
                                    .await
                                    .ok()
                            } else if let Some(eid_val) = map.get("_eid").and_then(|v| v.as_u64()) {
                                // Edge case
                                let eid = uni_common::core::id::Eid::from(eid_val);
                                prop_manager.get_edge_prop(eid, &end_prop, ctx).await.ok()
                            } else {
                                // Inline object - property embedded directly
                                map.get(&end_prop).cloned()
                            }
                        } else {
                            return Ok(Value::Bool(false));
                        };

                        let valid_to = match valid_to_val {
                            Some(Value::String(s)) => {
                                Some(parse_datetime_utc(&s).map_err(|_| {
                                    anyhow!("Invalid datetime in {}: {}", end_prop, s)
                                })?)
                            }
                            Some(Value::Null) | None => None,
                            _ => {
                                return Err(anyhow!(
                                    "Property {} must be a datetime string or null",
                                    end_prop
                                ));
                            }
                        };

                        let is_valid = valid_from <= query_time
                            && valid_to.map(|vt| query_time < vt).unwrap_or(true);
                        return Ok(Value::Bool(is_valid));
                    }

                    // For all other functions, evaluate arguments then call helper
                    let mut evaluated_args = Vec::with_capacity(args.len());
                    for arg in args {
                        evaluated_args.push(
                            this.evaluate_expr(arg, row, prop_manager, params, ctx)
                                .await?,
                        );
                    }
                    eval_scalar_function(name, &evaluated_args)
                }
                Expr::ListComprehension {
                    variable,
                    list,
                    where_clause,
                    mapping,
                } => {
                    // Evaluate the source list expression
                    let list_val = self
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;

                    let items = match list_val {
                        Value::Array(arr) => arr,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(anyhow!(
                                "List comprehension requires a list, got {:?}",
                                list_val
                            ));
                        }
                    };

                    let mut result = Vec::with_capacity(items.len());

                    for item in items {
                        // Create scope with the iteration variable
                        let mut scope = row.clone();
                        scope.insert(variable.clone(), item.clone());

                        // Apply WHERE filter if present
                        if let Some(where_expr) = where_clause {
                            let filter_result = self
                                .evaluate_expr(where_expr, &scope, prop_manager, params, ctx)
                                .await?;

                            match filter_result {
                                Value::Bool(true) => {}
                                Value::Bool(false) | Value::Null => continue,
                                _ => {
                                    return Err(anyhow!(
                                        "List comprehension WHERE clause must evaluate to boolean"
                                    ));
                                }
                            }
                        }

                        // Apply mapping expression if present, otherwise use the item
                        let output = if let Some(map_expr) = mapping {
                            self.evaluate_expr(map_expr, &scope, prop_manager, params, ctx)
                                .await?
                        } else {
                            item
                        };

                        result.push(output);
                    }

                    Ok(Value::Array(result))
                }
                Expr::Reduce {
                    accumulator,
                    init,
                    variable,
                    list,
                    expr,
                } => {
                    let mut acc = self
                        .evaluate_expr(init, row, prop_manager, params, ctx)
                        .await?;
                    let list_val = self
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;

                    if let Value::Array(items) = list_val {
                        for item in items {
                            // Create a temporary scope/row with accumulator and variable
                            // For simplicity in legacy executor, we can construct a new row map
                            // merging current row + new variables.
                            let mut scope = row.clone();
                            scope.insert(accumulator.clone(), acc.clone());
                            scope.insert(variable.clone(), item);

                            acc = self
                                .evaluate_expr(expr, &scope, prop_manager, params, ctx)
                                .await?;
                        }
                    } else {
                        return Err(anyhow!("REDUCE list argument must evaluate to a list"));
                    }
                    Ok(acc)
                }
                Expr::WindowFunction { .. } => {
                    // Try to look up pre-computed window value from row (added by Window operator)
                    let key = expr.to_string_repr();
                    if let Some(val) = row.get(&key) {
                        Ok(val.clone())
                    } else {
                        Err(anyhow!(
                            "Window function not computed (missing Window operator?)"
                        ))
                    }
                }
            }
        })
    }

    pub(crate) fn execute_subplan<'a>(
        &'a self,
        plan: LogicalPlan,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> BoxFuture<'a, Result<Vec<HashMap<String, Value>>>> {
        Box::pin(async move {
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }
            match plan {
                LogicalPlan::Union { left, right, all } => {
                    self.execute_union(left, right, all, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::CreateVectorIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                    );
                    idx_mgr.create_vector_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateFullTextIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                    );
                    idx_mgr.create_fts_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateScalarIndex {
                    mut config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }

                    // Check for expression index (single property that looks like expr)
                    if config.properties.len() == 1 {
                        let prop = &config.properties[0];
                        // Heuristic: if contains '(' and ')', it's an expression
                        if prop.contains('(') && prop.contains(')') {
                            let gen_col = SchemaManager::generated_column_name(prop);

                            // Add generated property to schema
                            let sm = self.storage.schema_manager_arc();
                            // Use String type for now as default for expressions
                            if let Err(e) = sm.add_generated_property(
                                &config.label,
                                &gen_col,
                                DataType::String,
                                prop.clone(),
                            ) {
                                // Ignore if already exists (might be re-indexing)
                                log::warn!("Failed to add generated property (might exist): {}", e);
                            }

                            // Use generated column for index
                            config.properties = vec![gen_col];
                        }
                    }

                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                    );
                    idx_mgr.create_scalar_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateJsonFtsIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                    );
                    idx_mgr.create_json_fts_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::ShowDatabase => Ok(self.execute_show_database()),
                LogicalPlan::ShowConfig => Ok(self.execute_show_config()),
                LogicalPlan::ShowStatistics => self.execute_show_statistics().await,
                LogicalPlan::Vacuum => {
                    self.execute_vacuum().await?;
                    Ok(vec![])
                }
                LogicalPlan::Checkpoint => {
                    self.execute_checkpoint().await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateLabel(clause) => {
                    self.execute_create_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateEdgeType(clause) => {
                    self.execute_create_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::AlterLabel(clause) => {
                    self.execute_alter_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::AlterEdgeType(clause) => {
                    self.execute_alter_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropLabel(clause) => {
                    self.execute_drop_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropEdgeType(clause) => {
                    self.execute_drop_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateConstraint(clause) => {
                    self.execute_create_constraint(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropConstraint(clause) => {
                    self.execute_drop_constraint(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::ShowConstraints(clause) => Ok(self.execute_show_constraints(clause)),
                LogicalPlan::DropIndex { name, if_exists } => {
                    let idx_mgr = IndexManager::new(
                        self.storage.base_path(),
                        self.storage.schema_manager_arc(),
                    );
                    match idx_mgr.drop_index(&name).await {
                        Ok(_) => Ok(vec![]),
                        Err(e) => {
                            if if_exists && e.to_string().contains("not found") {
                                Ok(vec![])
                            } else {
                                Err(e)
                            }
                        }
                    }
                }
                LogicalPlan::ShowIndexes { filter } => {
                    Ok(self.execute_show_indexes(filter.as_deref()))
                }
                LogicalPlan::Scan {
                    label_id,
                    variable,
                    filter,
                    optional,
                } => {
                    let vids = self
                        .scan_label_with_filter(
                            label_id,
                            &variable,
                            filter.as_ref(),
                            ctx,
                            prop_manager,
                            params,
                        )
                        .await?;
                    if vids.is_empty() && optional {
                        let mut map = HashMap::new();
                        map.insert(variable.clone(), Value::Null);
                        return Ok(vec![map]);
                    }
                    let mut matches = Vec::new();
                    for vid in vids {
                        let mut map = HashMap::new();
                        map.insert(variable.clone(), Value::String(vid.to_string()));
                        matches.push(map);
                    }
                    Ok(matches)
                }
                LogicalPlan::Traverse {
                    input,
                    edge_type_ids,
                    direction,
                    source_variable,
                    target_variable,
                    target_label_id,
                    step_variable,
                    min_hops,
                    max_hops,
                    optional,
                    target_filter,
                    path_variable,
                } => {
                    let input_matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let traverse_results = self
                        .execute_traverse(
                            input_matches,
                            edge_type_ids,
                            &direction,
                            &source_variable,
                            &target_variable,
                            target_label_id,
                            &step_variable,
                            min_hops,
                            max_hops,
                            optional,
                            &path_variable,
                            ctx,
                        )
                        .await?;

                    // Apply target_filter if present
                    if let Some(filter) = target_filter {
                        let mut filtered = Vec::new();
                        for row in traverse_results {
                            let res = self
                                .evaluate_expr(&filter, &row, prop_manager, params, ctx)
                                .await?;
                            if res.as_bool().unwrap_or(false) {
                                filtered.push(row);
                            }
                        }
                        Ok(filtered)
                    } else {
                        Ok(traverse_results)
                    }
                }
                LogicalPlan::Filter { input, predicate } => {
                    let input_matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let mut filtered = Vec::new();
                    for row in input_matches {
                        let res = self
                            .evaluate_expr(&predicate, &row, prop_manager, params, ctx)
                            .await?;
                        if res.as_bool().unwrap_or(false) {
                            filtered.push(row);
                        }
                    }
                    Ok(filtered)
                }
                LogicalPlan::ProcedureCall {
                    procedure_name,
                    arguments,
                    yield_items,
                } => {
                    let yield_names: Vec<String> =
                        yield_items.iter().map(|(n, _)| n.clone()).collect();
                    let results = self
                        .execute_procedure(
                            &procedure_name,
                            &arguments,
                            &yield_names,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await?;

                    // Handle aliasing
                    let mut aliased_results = Vec::with_capacity(results.len());
                    for mut row in results {
                        let mut new_row = row.clone();
                        for (name, alias) in &yield_items {
                            if let Some(a) = alias
                                && let Some(val) = row.remove(name)
                            {
                                new_row.remove(name);
                                new_row.insert(a.clone(), val);
                            }
                        }
                        aliased_results.push(new_row);
                    }
                    Ok(aliased_results)
                }
                LogicalPlan::VectorKnn {
                    label_id,
                    variable,
                    property,
                    query,
                    k,
                    threshold,
                } => {
                    self.execute_vector_knn(
                        label_id,
                        &variable,
                        &property,
                        &query,
                        k,
                        threshold,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::InvertedIndexLookup {
                    label_id,
                    variable,
                    property,
                    terms,
                } => {
                    self.execute_inverted_index_lookup(
                        label_id,
                        &variable,
                        &property,
                        &terms,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::Sort { input, order_by } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_sort(rows, &order_by, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Limit { input, skip, fetch } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let skip = skip.unwrap_or(0);
                    let take = fetch.unwrap_or(usize::MAX);
                    Ok(rows.into_iter().skip(skip).take(take).collect())
                }
                LogicalPlan::Aggregate {
                    input,
                    group_by,
                    aggregates,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_aggregate(rows, &group_by, &aggregates, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Window {
                    input,
                    window_exprs,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_window(rows, &window_exprs, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Project { input, projections } => {
                    let matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_project(matches, &projections, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Unwind {
                    input,
                    expr,
                    variable,
                } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_unwind(input_rows, &expr, &variable, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Apply {
                    input,
                    subquery,
                    input_filter,
                } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_apply(
                        input_rows,
                        &subquery,
                        input_filter.as_ref(),
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::RecursiveCTE {
                    cte_name,
                    initial,
                    recursive,
                } => {
                    self.execute_recursive_cte(
                        &cte_name,
                        *initial,
                        *recursive,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::CrossJoin { left, right } => {
                    self.execute_cross_join(left, right, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Set { input, items } => {
                    let mut rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        for row in &mut rows {
                            self.execute_set_items_locked(
                                &items,
                                row,
                                &mut writer,
                                prop_manager,
                                params,
                                ctx,
                            )
                            .await?;
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    Ok(rows)
                }
                LogicalPlan::Remove { input, items } => {
                    let mut rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        for row in &mut rows {
                            self.execute_remove_items_locked(
                                &items,
                                row,
                                &mut writer,
                                prop_manager,
                                ctx,
                            )
                            .await?;
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    Ok(rows)
                }
                LogicalPlan::Merge {
                    input,
                    pattern,
                    on_match,
                    on_create,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_merge(
                        rows,
                        &pattern,
                        on_match.as_ref(),
                        on_create.as_ref(),
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::Create { input, pattern } => {
                    let mut rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        for row in &mut rows {
                            self.execute_create_pattern(
                                &pattern,
                                row,
                                &mut writer,
                                prop_manager,
                                params,
                                ctx,
                            )
                            .await?;
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    Ok(rows)
                }
                LogicalPlan::Delete {
                    input,
                    items,
                    detach,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        for row in &rows {
                            for expr in &items {
                                let val = self
                                    .evaluate_expr(expr, row, prop_manager, params, ctx)
                                    .await?;
                                self.execute_delete_item_locked(&val, detach, &mut writer)
                                    .await?;
                            }
                        }
                    } else {
                        return Err(anyhow!("Write operation requires a Writer"));
                    }
                    Ok(rows)
                }
                LogicalPlan::Begin => {
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        writer.begin_transaction()?;
                    } else {
                        return Err(anyhow!("Transaction requires a Writer"));
                    }
                    Ok(vec![HashMap::new()])
                }
                LogicalPlan::Commit => {
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        writer.commit_transaction().await?;
                    } else {
                        return Err(anyhow!("Transaction requires a Writer"));
                    }
                    Ok(vec![HashMap::new()])
                }
                LogicalPlan::Rollback => {
                    if let Some(writer_lock) = &self.writer {
                        let mut writer = writer_lock.write().await;
                        writer.rollback_transaction()?;
                    } else {
                        return Err(anyhow!("Transaction requires a Writer"));
                    }
                    Ok(vec![HashMap::new()])
                }
                LogicalPlan::Copy {
                    target,
                    source,
                    is_export,
                    options,
                } => {
                    if is_export {
                        self.execute_export(&target, &source, &options, prop_manager, ctx)
                            .await
                    } else {
                        self.execute_copy(&target, &source, &options, prop_manager)
                            .await
                    }
                }
                LogicalPlan::Backup {
                    destination,
                    options,
                } => self.execute_backup(&destination, &options).await,
                LogicalPlan::Explain { plan } => {
                    let plan_str = format!("{:#?}", plan);
                    let mut row = HashMap::new();
                    row.insert("plan".to_string(), Value::String(plan_str));
                    Ok(vec![row])
                }
                LogicalPlan::ShortestPath { .. } => {
                    // ShortestPath is handled at the vectorized execution layer
                    // If we reach here, return empty result
                    Ok(vec![])
                }
                LogicalPlan::Empty => Ok(vec![HashMap::new()]),
            }
        })
    }

    /// Executes a graph traversal operation using BFS.
    ///
    /// # Errors
    ///
    /// Returns an error if the traversal times out or encounters a storage error.
    #[expect(
        clippy::too_many_arguments,
        reason = "Graph traversal requires many parameters"
    )]
    pub(crate) async fn execute_traverse(
        &self,
        input_matches: Vec<HashMap<String, Value>>,
        edge_type_ids: Vec<u16>,
        direction: &Direction,
        source_variable: &str,
        target_variable: &str,
        target_label_id: u16,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        path_variable: &Option<String>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut new_matches = Vec::new();
        for m in input_matches {
            // Check timeout between rows to prevent long-running traversals
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }

            let found = self
                .traverse_from_row(
                    &m,
                    &edge_type_ids,
                    direction,
                    source_variable,
                    target_variable,
                    target_label_id,
                    step_variable,
                    min_hops,
                    max_hops,
                    path_variable,
                    &mut new_matches,
                    ctx,
                )
                .await?;

            if !found && optional {
                let mut new_m = m.clone();
                new_m.insert(target_variable.to_string(), Value::Null);
                if let Some(sv) = step_variable {
                    new_m.insert(sv.clone(), Value::Null);
                }
                if let Some(pv) = path_variable {
                    new_m.insert(pv.clone(), Value::Null);
                }
                new_matches.push(new_m);
            }
        }
        Ok(new_matches)
    }

    /// Performs BFS traversal from a single row, collecting matching results.
    ///
    /// # Errors
    ///
    /// Returns an error if the traversal times out or encounters a storage error.
    #[expect(
        clippy::too_many_arguments,
        reason = "Graph traversal requires many parameters"
    )]
    pub(crate) async fn traverse_from_row(
        &self,
        row: &HashMap<String, Value>,
        edge_type_ids: &[u16],
        direction: &Direction,
        source_variable: &str,
        target_variable: &str,
        target_label_id: u16,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        path_variable: &Option<String>,
        new_matches: &mut Vec<HashMap<String, Value>>,
        ctx: Option<&QueryContext>,
    ) -> Result<bool> {
        let source_vid = match row
            .get(source_variable)
            .and_then(|v| Self::vid_from_value(v).ok())
        {
            Some(v) => v,
            None => return Ok(false),
        };

        let graph_dir = Self::map_to_store_direction(direction);
        let l0_arc_opt = self.get_l0_arc().await;

        let graph = self
            .storage
            .load_subgraph_cached(
                &[source_vid],
                edge_type_ids,
                max_hops,
                graph_dir,
                l0_arc_opt,
            )
            .await?;

        if !graph.contains_vertex(source_vid) {
            return Ok(false);
        }

        self.bfs_traverse(
            row,
            &graph,
            source_vid,
            edge_type_ids,
            direction,
            target_variable,
            target_label_id,
            step_variable,
            min_hops,
            max_hops,
            path_variable,
            new_matches,
            ctx,
        )
    }

    /// Interval for timeout checks in BFS loop to avoid excessive overhead.
    const BFS_TIMEOUT_CHECK_INTERVAL: usize = 100;

    /// BFS traversal core logic with timeout enforcement.
    ///
    /// # Errors
    ///
    /// Returns an error if the query times out during traversal.
    ///
    /// # Security
    ///
    /// **CWE-400 (Resource Consumption)**: Periodic timeout checks prevent
    /// unbounded traversal on large graphs with high fan-out.
    #[expect(
        clippy::too_many_arguments,
        reason = "Graph traversal requires many parameters"
    )]
    pub(crate) fn bfs_traverse(
        &self,
        row: &HashMap<String, Value>,
        graph: &uni_store::runtime::WorkingGraph,
        source_vid: Vid,
        edge_type_ids: &[u16],
        direction: &Direction,
        target_variable: &str,
        target_label_id: u16,
        step_variable: &Option<String>,
        min_hops: usize,
        max_hops: usize,
        path_variable: &Option<String>,
        new_matches: &mut Vec<HashMap<String, Value>>,
        ctx: Option<&QueryContext>,
    ) -> Result<bool> {
        println!(
            "DEBUG: bfs_traverse from {:?} to {} (target_label={})",
            source_vid, target_variable, target_label_id
        );
        let mut found_neighbor = false;
        let mut visited = HashMap::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((source_vid, 0, Vec::new()));
        visited.insert(source_vid, 0);

        let mut iteration_count = 0usize;

        while let Some((curr, depth, path)) = queue.pop_front() {
            // Periodic timeout check to prevent unbounded traversal
            iteration_count += 1;
            if iteration_count.is_multiple_of(Self::BFS_TIMEOUT_CHECK_INTERVAL)
                && let Some(ctx) = ctx
            {
                ctx.check_timeout()?;
            }

            if depth >= max_hops {
                continue;
            }

            for (next, edge_entry) in
                self.collect_incident_edges(graph, curr, edge_type_ids, direction)
            {
                println!(
                    "DEBUG:   neighbor {:?} (label={}) at depth {}",
                    next,
                    next.label_id(),
                    depth + 1
                );
                if path.contains(&edge_entry.eid) {
                    continue;
                }

                let mut new_path = path.clone();
                new_path.push(edge_entry.eid);

                if Self::should_visit_vertex(&visited, next, depth, step_variable) {
                    visited.insert(next, depth + 1);
                    queue.push_back((next, depth + 1, new_path.clone()));

                    if Self::is_valid_target(next, depth + 1, min_hops, target_label_id) {
                        println!("DEBUG:     MATCH!");
                        found_neighbor = true;
                        let new_m = Self::build_traverse_match(
                            row,
                            target_variable,
                            next,
                            step_variable,
                            &new_path,
                            &edge_entry,
                            curr,
                            max_hops,
                            path_variable,
                            source_vid,
                            graph,
                        );
                        new_matches.push(new_m);
                    }
                }
            }
        }

        Ok(found_neighbor)
    }

    /// Check if a vertex should be visited during BFS.
    pub(crate) fn should_visit_vertex(
        visited: &HashMap<Vid, usize>,
        next: Vid,
        depth: usize,
        step_variable: &Option<String>,
    ) -> bool {
        step_variable.is_some() || !visited.contains_key(&next) || visited[&next] > depth + 1
    }

    /// Check if a vertex is a valid target for the traversal.
    pub(crate) fn is_valid_target(
        vid: Vid,
        current_depth: usize,
        min_hops: usize,
        target_label_id: u16,
    ) -> bool {
        current_depth >= min_hops && (target_label_id == 0 || vid.label_id() == target_label_id)
    }

    /// Collect incident edges from a graph node based on direction.
    pub(crate) fn collect_incident_edges(
        &self,
        graph: &uni_store::runtime::WorkingGraph,
        curr: Vid,
        edge_type_ids: &[u16],
        direction: &Direction,
    ) -> Vec<(Vid, uni_common::graph::simple_graph::EdgeEntry)> {
        let directions = match direction {
            Direction::Outgoing => vec![uni_store::runtime::Direction::Outgoing],
            Direction::Incoming => vec![uni_store::runtime::Direction::Incoming],
            Direction::Both => vec![
                uni_store::runtime::Direction::Outgoing,
                uni_store::runtime::Direction::Incoming,
            ],
        };

        let mut incident_edges = Vec::new();
        for dir in directions {
            for edge in graph.neighbors(curr, dir) {
                if !edge_type_ids.contains(&edge.edge_type) {
                    continue;
                }
                let next = match dir {
                    uni_store::runtime::Direction::Outgoing => edge.dst_vid,
                    uni_store::runtime::Direction::Incoming => edge.src_vid,
                };
                incident_edges.push((next, *edge));
            }
        }
        incident_edges
    }

    /// Map query direction to storage direction.
    pub(crate) fn map_to_store_direction(direction: &Direction) -> uni_store::runtime::Direction {
        match direction {
            Direction::Outgoing => uni_store::runtime::Direction::Outgoing,
            Direction::Incoming => uni_store::runtime::Direction::Incoming,
            Direction::Both => uni_store::runtime::Direction::Outgoing,
        }
    }

    /// Get L0 arc from writer or l0_manager.
    pub(crate) async fn get_l0_arc(
        &self,
    ) -> Option<std::sync::Arc<parking_lot::RwLock<uni_store::runtime::L0Buffer>>> {
        if let Some(writer_lock) = &self.writer {
            let writer = writer_lock.read().await;
            Some(writer.l0_manager.get_current())
        } else {
            self.l0_manager.as_ref().map(|l0_mgr| l0_mgr.get_current())
        }
    }

    /// Build a match result for BFS traversal with optional step variable.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_traverse_match(
        row: &HashMap<String, Value>,
        target_variable: &str,
        target_vid: Vid,
        step_variable: &Option<String>,
        path: &[uni_common::core::id::Eid],
        edge_entry: &uni_common::graph::simple_graph::EdgeEntry,
        curr_vid: Vid,
        max_hops: usize,
        path_variable: &Option<String>,
        source_vid: Vid,
        graph: &uni_store::runtime::WorkingGraph,
    ) -> HashMap<String, Value> {
        let mut new_m = row.clone();
        new_m.insert(
            target_variable.to_string(),
            Value::String(target_vid.to_string()),
        );

        if let Some(sv) = step_variable {
            if max_hops > 1 {
                let eids: Vec<Value> = path.iter().map(|e| json!(e.as_u64())).collect();
                new_m.insert(sv.clone(), Value::Array(eids));
            } else {
                let edge_obj = json!({
                    "_eid": edge_entry.eid.as_u64(),
                    "_src": curr_vid.as_u64(),
                    "_dst": target_vid.as_u64(),
                    "_type": edge_entry.edge_type
                });
                new_m.insert(sv.clone(), edge_obj);
            }
        }

        if let Some(pv) = path_variable {
            // Reconstruct path object (Nodes and Edges)
            let mut path_nodes = Vec::new();
            let mut path_edges = Vec::new();

            // Start node
            // Note: We don't have properties here, just VIDs.
            // Executor typically returns IDs and properties are fetched on demand or projected.
            // But Path object structure in types.rs expects Node { vid, label, properties }.
            // We can return minimal Node/Edge objects.

            // To get full nodes/edges, we'd need to fetch them.
            // For now, let's construct minimal objects with IDs.
            // Properties will be empty map.

            path_nodes.push(crate::types::Node {
                vid: source_vid,
                label: String::new(), // We don't know label easily without lookup
                properties: HashMap::new(),
            });

            let mut current = source_vid;
            for eid in path {
                // Find edge in graph to get dst
                // WorkingGraph is optimized for out/in edges.
                // We assume path is followed in valid direction.
                // We need to find the edge with 'eid' starting from 'current'.
                // This is slow if we iterate. But 'path' came from traversal.

                // Optimized: We know the sequence of edges.
                // But we don't know the exact target node for each edge without lookup if we only have Eids.
                // However, bfs_traverse built the path.

                // Actually, bfs_traverse `queue` stores `(Vid, depth, Vec<Eid>)`.
                // It doesn't store the intermediate vertices!
                // To reconstruct the path nodes, we need to traverse the edges again.
                // Or change queue to store `Vec<(Eid, Vid)>`?
                // Or since we have the graph, we can lookup edge endpoints.

                // WorkingGraph doesn't index by Eid.
                // So we can't look up edge by Eid efficiently to get next node.
                // We MUST traverse from current.

                // Let's assume we can find the edge in outgoing edges of current.
                // (Or incoming if direction is incoming).

                // For simplicity/correctness, bfs_traverse should probably track the path of vertices too?
                // Or we iterate neighbors of current and find the one with eid.

                let mut next_vid = None;
                // Try both directions? The graph is loaded with direction.
                // We assume consistent direction for the whole path traversal.
                // But we don't have direction passed here easily (it's in caller).
                // Actually, we do pass `edge_entry` which has `edge_type` but not direction used.

                // We can iterate all edges of `current` in graph.

                // Iterate both directions manually as SimpleGraph neighbors doesn't support Both
                let out_edges = graph.neighbors(
                    current,
                    uni_common::graph::simple_graph::Direction::Outgoing,
                );
                let inc_edges = graph.neighbors(
                    current,
                    uni_common::graph::simple_graph::Direction::Incoming,
                );

                for edge in out_edges.iter().chain(inc_edges.iter()) {
                    if edge.eid == *eid {
                        let neighbor = if edge.src_vid == current {
                            edge.dst_vid
                        } else {
                            edge.src_vid
                        };
                        next_vid = Some(neighbor);

                        path_edges.push(crate::types::Edge {
                            eid: *eid,
                            edge_type: String::new(), // Unknown name
                            src: edge.src_vid,
                            dst: edge.dst_vid,
                            properties: HashMap::new(),
                        });

                        path_nodes.push(crate::types::Node {
                            vid: neighbor,
                            label: String::new(),
                            properties: HashMap::new(),
                        });

                        break;
                    }
                }

                // Remove unused get_edges block if present (it was not in the replace range, but I should ensure it's clean)
                // The previous code had `if let Some(edges) = graph.get_edges(current)` block.
                // I am replacing the block that contained `for (neighbor, edge) in graph.neighbors(...)`.

                if let Some(next) = next_vid {
                    current = next;
                } else {
                    // Should not happen if graph is consistent
                    log::warn!("Could not find next node for edge {} from {}", eid, current);
                }
            }

            let path_obj = crate::types::Path {
                nodes: path_nodes,
                edges: path_edges,
            };

            new_m.insert(pv.clone(), crate::types::Value::Path(path_obj).into());
        }

        new_m
    }

    /// Execute aggregate operation: GROUP BY + aggregate functions.
    /// Interval for timeout checks in aggregate loops.
    pub(crate) const AGGREGATE_TIMEOUT_CHECK_INTERVAL: usize = 1000;

    pub(crate) async fn execute_aggregate(
        &self,
        rows: Vec<HashMap<String, Value>>,
        group_by: &[Expr],
        aggregates: &[Expr],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before aggregation
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        let mut groups: HashMap<String, (Vec<Value>, Vec<Accumulator>)> = HashMap::new();

        for (idx, row) in rows.into_iter().enumerate() {
            // Periodic timeout check during aggregation
            if idx.is_multiple_of(Self::AGGREGATE_TIMEOUT_CHECK_INTERVAL)
                && let Some(ctx) = ctx
            {
                ctx.check_timeout()?;
            }

            let key_vals = self
                .evaluate_group_keys(group_by, &row, prop_manager, params, ctx)
                .await?;
            // Note: JSON serialization for grouping keys is a known performance concern.
            // serde_json::Value doesn't implement Hash/Ord, requiring string-based grouping.
            // For high-performance paths, use the vectorized executor with Arrow-native grouping.
            let key_str = serde_json::to_string(&key_vals)?;

            let entry = groups
                .entry(key_str)
                .or_insert_with(|| (key_vals, Self::create_accumulators(aggregates)));

            self.update_accumulators(&mut entry.1, aggregates, &row, prop_manager, params, ctx)
                .await?;
        }

        let results = groups
            .values()
            .map(|(k_vals, accs)| Self::build_aggregate_result(group_by, aggregates, k_vals, accs))
            .collect();

        Ok(results)
    }

    /// Interval for timeout checks in window function loops.
    const WINDOW_TIMEOUT_CHECK_INTERVAL: usize = 1000;

    pub(crate) async fn execute_window(
        &self,
        rows: Vec<HashMap<String, Value>>,
        window_exprs: &[Expr],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before window computation
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        let mut rows = rows;
        for expr in window_exprs {
            if let Expr::WindowFunction {
                function,
                partition_by,
                order_by,
            } = expr
            {
                // 1. Evaluate partitioning and ordering keys
                let mut row_data = Vec::with_capacity(rows.len());
                for (i, row) in rows.iter().enumerate() {
                    // Periodic timeout check during key evaluation
                    if i.is_multiple_of(Self::WINDOW_TIMEOUT_CHECK_INTERVAL)
                        && let Some(ctx) = ctx
                    {
                        ctx.check_timeout()?;
                    }

                    let mut p_keys = Vec::new();
                    for p in partition_by {
                        p_keys.push(
                            self.evaluate_expr(p, row, prop_manager, params, ctx)
                                .await?,
                        );
                    }
                    let mut o_keys = Vec::new();
                    for (o, _) in order_by {
                        o_keys.push(
                            self.evaluate_expr(o, row, prop_manager, params, ctx)
                                .await?,
                        );
                    }
                    row_data.push((i, p_keys, o_keys));
                }

                // Check timeout before synchronous sort
                if let Some(ctx) = ctx {
                    ctx.check_timeout()?;
                }

                // 2. Sort indices
                row_data.sort_by(|a, b| {
                    // Compare partition keys
                    // PERF: JSON serialization for comparison is O(key_size) per comparison.
                    // For performance-critical paths, use the vectorized executor.
                    let p_a = serde_json::to_string(&a.1).unwrap();
                    let p_b = serde_json::to_string(&b.1).unwrap();
                    let cmp = p_a.cmp(&p_b);
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }

                    // Compare sort keys
                    for (j, (_, asc)) in order_by.iter().enumerate() {
                        let val_a = &a.2[j];
                        let val_b = &b.2[j];
                        let ord = Self::compare_values(val_a, val_b);
                        if ord != std::cmp::Ordering::Equal {
                            return if *asc { ord } else { ord.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                // 3. Compute window function
                let mut current_partition = String::new();
                let mut row_number = 0;
                let mut rank = 0;
                let mut dense_rank = 0;
                let mut prev_o_keys: Option<&Vec<Value>> = None;

                let mut results = Vec::with_capacity(rows.len());

                for (idx, p_keys, o_keys) in &row_data {
                    let p_str = serde_json::to_string(p_keys).unwrap();
                    if p_str != current_partition {
                        current_partition = p_str;
                        row_number = 1;
                        rank = 1;
                        dense_rank = 1;
                    } else {
                        row_number += 1;
                        if let Some(prev) = prev_o_keys {
                            let mut eq = true;
                            for (j, val) in o_keys.iter().enumerate() {
                                if Self::compare_values(val, &prev[j]) != std::cmp::Ordering::Equal
                                {
                                    eq = false;
                                    break;
                                }
                            }
                            if !eq {
                                rank = row_number;
                                dense_rank += 1;
                            }
                        }
                    }
                    prev_o_keys = Some(o_keys);

                    let result = match function.as_ref() {
                        Expr::FunctionCall { name, .. } => match name.as_str() {
                            "ROW_NUMBER" => json!(row_number),
                            "RANK" => json!(rank),
                            "DENSE_RANK" => json!(dense_rank),
                            _ => return Err(anyhow!("Unsupported window function: {}", name)),
                        },
                        _ => return Err(anyhow!("Window function must be a function call")),
                    };
                    results.push((*idx, result));
                }

                // 4. Update rows
                let key = expr.to_string_repr();
                for (idx, val) in results {
                    rows[idx].insert(key.clone(), val);
                }
            }
        }
        Ok(rows)
    }

    /// Evaluate group-by key expressions for a row.
    pub(crate) async fn evaluate_group_keys(
        &self,
        group_by: &[Expr],
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<Value>> {
        let mut key_vals = Vec::new();
        for expr in group_by {
            key_vals.push(
                self.evaluate_expr(expr, row, prop_manager, params, ctx)
                    .await?,
            );
        }
        Ok(key_vals)
    }

    /// Update accumulators with values from the current row.
    pub(crate) async fn update_accumulators(
        &self,
        accs: &mut [Accumulator],
        aggregates: &[Expr],
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        for (i, agg_expr) in aggregates.iter().enumerate() {
            if let Expr::FunctionCall { args, .. } = agg_expr {
                let is_wildcard = args.is_empty() || matches!(args[0], Expr::Wildcard);
                let val = if is_wildcard {
                    Value::Null
                } else {
                    self.evaluate_expr(&args[0], row, prop_manager, params, ctx)
                        .await?
                };
                accs[i].update(&val, is_wildcard);
            }
        }
        Ok(())
    }

    /// Execute sort operation with ORDER BY clauses.
    pub(crate) async fn execute_recursive_cte(
        &self,
        cte_name: &str,
        initial: LogicalPlan,
        recursive: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        use std::collections::HashSet;

        // Helper to create a stable key for cycle detection.
        // Uses sorted keys to ensure consistent ordering.
        pub(crate) fn row_key(row: &HashMap<String, Value>) -> String {
            let mut pairs: Vec<_> = row.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            serde_json::to_string(&pairs).unwrap_or_default()
        }

        // 1. Execute Anchor
        let mut working_table = self
            .execute_subplan(initial, prop_manager, params, ctx)
            .await?;
        let mut result_table = working_table.clone();

        // Track seen rows for cycle detection
        let mut seen: HashSet<String> = working_table.iter().map(row_key).collect();

        // 2. Loop
        // Safety: Max iterations to prevent infinite loop
        // TODO: expose this via UniConfig for user control
        let max_iterations = 1000;
        for _iteration in 0..max_iterations {
            // CWE-400: Check timeout at each iteration to prevent resource exhaustion
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }

            if working_table.is_empty() {
                break;
            }

            // Bind working table to CTE name in params
            let working_val = Value::Array(
                working_table
                    .iter()
                    .map(|row| {
                        if row.len() == 1 {
                            row.values().next().unwrap().clone()
                        } else {
                            Value::Object(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        }
                    })
                    .collect(),
            );

            let mut next_params = params.clone();
            next_params.insert(cte_name.to_string(), working_val);

            // Execute recursive part
            let next_result = self
                .execute_subplan(recursive.clone(), prop_manager, &next_params, ctx)
                .await?;

            if next_result.is_empty() {
                break;
            }

            // Filter out already-seen rows (cycle detection)
            let new_rows: Vec<_> = next_result
                .into_iter()
                .filter(|row| {
                    let key = row_key(row);
                    seen.insert(key) // Returns false if already present
                })
                .collect();

            if new_rows.is_empty() {
                // All results were cycles - terminate
                break;
            }

            result_table.extend(new_rows.clone());
            working_table = new_rows;
        }

        // Output accumulated results as a variable
        let final_list = Value::Array(
            result_table
                .into_iter()
                .map(|row| {
                    // If the CTE returns a single column and we want to treat it as a list of values?
                    // E.g. WITH RECURSIVE r AS (RETURN 1 UNION RETURN 2) -> [1, 2] or [{expr:1}, {expr:2}]?
                    // Cypher LISTs usually contain values.
                    // If the row has 1 column, maybe unwrap?
                    // But SQL CTEs are tables.
                    // Let's stick to List<Map> for consistency with how we pass it in.
                    // UNLESS the user extracts it.
                    // My parser test `MATCH (n) WHERE n IN hierarchy` implies `hierarchy` contains Nodes.
                    // If `row` contains `root` (Node), then `hierarchy` should be `[Node, Node]`.
                    // If row has multiple cols, `[ {a:1, b:2}, ... ]`.
                    // If row has 1 col, users expect `[val, val]`.
                    if row.len() == 1 {
                        row.values().next().unwrap().clone()
                    } else {
                        Value::Object(row.into_iter().collect())
                    }
                })
                .collect(),
        );

        let mut final_row = HashMap::new();
        final_row.insert(cte_name.to_string(), final_list);
        Ok(vec![final_row])
    }

    /// Interval for timeout checks in sort loops.
    const SORT_TIMEOUT_CHECK_INTERVAL: usize = 1000;

    pub(crate) async fn execute_sort(
        &self,
        rows: Vec<HashMap<String, Value>>,
        order_by: &[crate::query::ast::SortItem],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before potentially expensive sort
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        let mut rows_with_keys = Vec::with_capacity(rows.len());
        for (idx, row) in rows.into_iter().enumerate() {
            // Periodic timeout check during key extraction
            if idx.is_multiple_of(Self::SORT_TIMEOUT_CHECK_INTERVAL)
                && let Some(ctx) = ctx
            {
                ctx.check_timeout()?;
            }

            let mut keys = Vec::new();
            for item in order_by {
                let val = row
                    .get(&item.expr.to_string_repr())
                    .cloned()
                    .unwrap_or(Value::Null);
                let val = if val.is_null() {
                    self.evaluate_expr(&item.expr, &row, prop_manager, params, ctx)
                        .await
                        .unwrap_or(Value::Null)
                } else {
                    val
                };
                keys.push(val);
            }
            rows_with_keys.push((row, keys));
        }

        // Check timeout again before synchronous sort (can't be interrupted)
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        rows_with_keys.sort_by(|a, b| Self::compare_sort_keys(&a.1, &b.1, order_by));

        Ok(rows_with_keys.into_iter().map(|(r, _)| r).collect())
    }

    /// Create accumulators for aggregate expressions.
    pub(crate) fn create_accumulators(aggregates: &[Expr]) -> Vec<Accumulator> {
        aggregates
            .iter()
            .map(|expr| {
                if let Expr::FunctionCall { name, distinct, .. } = expr {
                    Accumulator::new(name, *distinct)
                } else {
                    Accumulator::new("COUNT", false)
                }
            })
            .collect()
    }

    /// Build result row from group-by keys and accumulators.
    pub(crate) fn build_aggregate_result(
        group_by: &[Expr],
        aggregates: &[Expr],
        key_vals: &[Value],
        accs: &[Accumulator],
    ) -> HashMap<String, Value> {
        let mut res_row = HashMap::new();
        for (i, expr) in group_by.iter().enumerate() {
            res_row.insert(expr.to_string_repr(), key_vals[i].clone());
        }
        for (i, expr) in aggregates.iter().enumerate() {
            res_row.insert(expr.to_string_repr(), accs[i].finish());
        }
        res_row
    }

    /// Compare and return ordering for sort operation.
    pub(crate) fn compare_sort_keys(
        a_keys: &[Value],
        b_keys: &[Value],
        order_by: &[crate::query::ast::SortItem],
    ) -> std::cmp::Ordering {
        for (i, item) in order_by.iter().enumerate() {
            let order = Self::compare_values(&a_keys[i], &b_keys[i]);
            if order != std::cmp::Ordering::Equal {
                return if item.ascending {
                    order
                } else {
                    order.reverse()
                };
            }
        }
        std::cmp::Ordering::Equal
    }

    pub(crate) async fn execute_backup(
        &self,
        destination: &str,
        _options: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Validate destination path against sandbox
        let validated_dest = self.validate_path(destination)?;

        // 1. Flush L0
        if let Some(writer_arc) = &self.writer {
            let mut writer = writer_arc.write().await;
            writer.flush_to_l1(None).await?;
        }

        // 2. Snapshot
        let snapshot_manager = self.storage.snapshot_manager();
        let snapshot = snapshot_manager
            .load_latest_snapshot()
            .await?
            .ok_or_else(|| anyhow!("No snapshot found"))?;

        // 3. Copy files
        // TODO: This assumes local storage. Update for ObjectStore.
        let source_path = std::path::Path::new(self.storage.base_path());
        let dest_path = &validated_dest;

        if !dest_path.exists() {
            std::fs::create_dir_all(dest_path)?;
        }

        // Recursive copy (Only works if source is local)
        if source_path.exists() {
            Self::copy_dir_all(source_path, dest_path)?;
        }

        // 4. Copy schema
        // SchemaManager now uses ObjectStore
        // We need to read from store and write to local destination
        // TODO: Access store from SchemaManager (need to expose it or add copy_to method)
        // For now, let's skip schema backup if not local or implement using save() to new location?

        let schema_manager = self.storage.schema_manager();
        // Since we can't easily access the store here without exposing it,
        // and we want to write to a specific destination...
        // Let's manually save current schema to destination/catalog/schema.json

        let dest_catalog = dest_path.join("catalog");
        if !dest_catalog.exists() {
            std::fs::create_dir_all(&dest_catalog)?;
        }

        let schema_content = serde_json::to_string_pretty(&schema_manager.schema())?;
        std::fs::write(dest_catalog.join("schema.json"), schema_content)?;

        let mut res = HashMap::new();
        res.insert(
            "status".to_string(),
            Value::String("Backup completed".to_string()),
        );
        res.insert(
            "snapshot_id".to_string(),
            Value::String(snapshot.snapshot_id),
        );
        Ok(vec![res])
    }

    /// Maximum directory depth for backup operations.
    ///
    /// **CWE-674 (Uncontrolled Recursion)**: Prevents stack overflow from
    /// excessively deep directory structures.
    const MAX_BACKUP_DEPTH: usize = 100;

    /// Maximum file count for backup operations.
    ///
    /// **CWE-400 (Resource Consumption)**: Prevents disk exhaustion and
    /// long-running operations from malicious or unexpectedly large directories.
    const MAX_BACKUP_FILES: usize = 100_000;

    /// Recursively copies a directory with security limits.
    ///
    /// # Security
    ///
    /// - **CWE-674**: Depth limit prevents stack overflow
    /// - **CWE-400**: File count limit prevents resource exhaustion
    /// - **Symlink handling**: Symlinks are skipped to prevent loop attacks
    pub(crate) fn copy_dir_all(
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> std::io::Result<()> {
        let mut file_count = 0usize;
        Self::copy_dir_all_impl(src, dst, 0, &mut file_count)
    }

    /// Internal implementation with depth and file count tracking.
    pub(crate) fn copy_dir_all_impl(
        src: &std::path::Path,
        dst: &std::path::Path,
        depth: usize,
        file_count: &mut usize,
    ) -> std::io::Result<()> {
        if depth >= Self::MAX_BACKUP_DEPTH {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Maximum backup depth {} exceeded at {:?}",
                    Self::MAX_BACKUP_DEPTH,
                    src
                ),
            ));
        }

        std::fs::create_dir_all(dst)?;

        for entry in std::fs::read_dir(src)? {
            if *file_count >= Self::MAX_BACKUP_FILES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Maximum backup file count {} exceeded",
                        Self::MAX_BACKUP_FILES
                    ),
                ));
            }
            *file_count += 1;

            let entry = entry?;
            let metadata = entry.metadata()?;

            // Skip symlinks to prevent loops and traversal attacks
            if metadata.file_type().is_symlink() {
                // Silently skip - logging would require tracing dependency
                continue;
            }

            let dst_path = dst.join(entry.file_name());
            if metadata.is_dir() {
                Self::copy_dir_all_impl(&entry.path(), &dst_path, depth + 1, file_count)?;
            } else {
                std::fs::copy(entry.path(), dst_path)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_copy(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let format = options
            .get("format")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                if source.ends_with(".parquet") {
                    "parquet"
                } else {
                    "csv"
                }
            });

        match format.to_lowercase().as_str() {
            "csv" => self.execute_csv_import(target, source, options).await,
            "parquet" => {
                self.execute_parquet_import(target, source, options, prop_manager)
                    .await
            }
            _ => Err(anyhow!("Unsupported format: {}", format)),
        }
    }

    pub(crate) async fn execute_csv_import(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Validate source path against sandbox
        let validated_source = self.validate_path(source)?;

        let writer_lock = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("COPY requires a Writer"))?;

        let schema = self.storage.schema_manager().schema();

        // 1. Determine if target is Label or EdgeType
        let label_meta = schema.labels.get(target);
        let edge_meta = schema.edge_types.get(target);

        if label_meta.is_none() && edge_meta.is_none() {
            return Err(anyhow!("Target '{}' not found in schema", target));
        }

        // 2. Open CSV
        let delimiter_str = options
            .get("delimiter")
            .and_then(|v| v.as_str())
            .unwrap_or(",");
        let delimiter = if delimiter_str.is_empty() {
            b','
        } else {
            delimiter_str.as_bytes()[0]
        };
        let has_header = options
            .get("header")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(has_header)
            .from_path(&validated_source)?;

        let headers = rdr.headers()?.clone();
        let mut count = 0;

        let mut writer = writer_lock.write().await;

        if let Some(meta) = label_meta {
            let label_id = meta.id;
            let target_props = schema
                .properties
                .get(target)
                .ok_or_else(|| anyhow!("Properties for label '{}' not found", target))?;

            for result in rdr.records() {
                let record = result?;
                let mut props = HashMap::new();

                for (i, header) in headers.iter().enumerate() {
                    if let Some(val_str) = record.get(i)
                        && let Some(prop_meta) = target_props.get(header)
                    {
                        let val = self.parse_csv_value(val_str, &prop_meta.r#type, header)?;
                        props.insert(header.to_string(), val);
                    }
                }

                let vid = writer.next_vid(label_id).await?;
                writer.insert_vertex(vid, props).await?;
                count += 1;
            }
        } else if let Some(meta) = edge_meta {
            let type_id = meta.id;
            let target_props = schema
                .properties
                .get(target)
                .ok_or_else(|| anyhow!("Properties for edge type '{}' not found", target))?;

            // For edges, we need src and dst VIDs.
            // Expecting columns '_src' and '_dst' or as specified in options.
            let src_col = options
                .get("src_col")
                .and_then(|v| v.as_str())
                .unwrap_or("_src");
            let dst_col = options
                .get("dst_col")
                .and_then(|v| v.as_str())
                .unwrap_or("_dst");

            for result in rdr.records() {
                let record = result?;
                let mut props = HashMap::new();
                let mut src_vid = None;
                let mut dst_vid = None;

                for (i, header) in headers.iter().enumerate() {
                    if let Some(val_str) = record.get(i) {
                        if header == src_col {
                            src_vid =
                                Some(Self::vid_from_value(&Value::String(val_str.to_string()))?);
                        } else if header == dst_col {
                            dst_vid =
                                Some(Self::vid_from_value(&Value::String(val_str.to_string()))?);
                        } else if let Some(prop_meta) = target_props.get(header) {
                            let val = self.parse_csv_value(val_str, &prop_meta.r#type, header)?;
                            props.insert(header.to_string(), val);
                        }
                    }
                }

                let src =
                    src_vid.ok_or_else(|| anyhow!("Missing source VID in column '{}'", src_col))?;
                let dst = dst_vid
                    .ok_or_else(|| anyhow!("Missing destination VID in column '{}'", dst_col))?;

                let eid = writer.next_eid(type_id).await?;
                writer.insert_edge(src, dst, type_id, eid, props).await?;
                count += 1;
            }
        }

        let mut res = HashMap::new();
        res.insert("count".to_string(), json!(count));
        Ok(vec![res])
    }

    pub(crate) async fn execute_parquet_import(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
        _prop_manager: &PropertyManager,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Validate source path against sandbox
        let validated_source = self.validate_path(source)?;

        let writer_lock = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("COPY requires a Writer"))?;

        let schema = self.storage.schema_manager().schema();

        // 1. Determine if target is Label or EdgeType
        let label_meta = schema.labels.get(target);
        let edge_meta = schema.edge_types.get(target);

        if label_meta.is_none() && edge_meta.is_none() {
            return Err(anyhow!("Target '{}' not found in schema", target));
        }

        // 2. Open Parquet
        let file = std::fs::File::open(&validated_source)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;

        let mut count = 0;
        let mut writer = writer_lock.write().await;

        if let Some(meta) = label_meta {
            let label_id = meta.id;
            let target_props = schema
                .properties
                .get(target)
                .ok_or_else(|| anyhow!("Properties for label '{}' not found", target))?;

            for batch in reader.by_ref() {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    let mut props = HashMap::new();
                    for field in batch.schema().fields() {
                        let name = field.name();
                        if target_props.contains_key(name) {
                            let col = batch.column_by_name(name).unwrap();
                            if !col.is_null(row) {
                                let val = Self::arrow_to_value(col.as_ref(), row);
                                // arrow_to_value might return generic JSON, we should probably ensure type match.
                                props.insert(name.clone(), val);
                            }
                        }
                    }
                    let vid = writer.next_vid(label_id).await?;
                    writer.insert_vertex(vid, props).await?;
                    count += 1;
                }
            }
        } else if let Some(meta) = edge_meta {
            let type_id = meta.id;
            let target_props = schema
                .properties
                .get(target)
                .ok_or_else(|| anyhow!("Properties for edge type '{}' not found", target))?;

            let src_col = options
                .get("src_col")
                .and_then(|v| v.as_str())
                .unwrap_or("_src");
            let dst_col = options
                .get("dst_col")
                .and_then(|v| v.as_str())
                .unwrap_or("_dst");

            for batch in reader {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    let mut props = HashMap::new();
                    let mut src_vid = None;
                    let mut dst_vid = None;

                    for field in batch.schema().fields() {
                        let name = field.name();
                        let col = batch.column_by_name(name).unwrap();
                        if col.is_null(row) {
                            continue;
                        }

                        if name == src_col {
                            let val = Self::arrow_to_value(col.as_ref(), row);
                            src_vid = Some(Self::vid_from_value(&val)?);
                        } else if name == dst_col {
                            let val = Self::arrow_to_value(col.as_ref(), row);
                            dst_vid = Some(Self::vid_from_value(&val)?);
                        } else if target_props.get(name).is_some() {
                            let val = Self::arrow_to_value(col.as_ref(), row);
                            props.insert(name.clone(), val);
                        }
                    }

                    let src = src_vid
                        .ok_or_else(|| anyhow!("Missing source VID in column '{}'", src_col))?;
                    let dst = dst_vid.ok_or_else(|| {
                        anyhow!("Missing destination VID in column '{}'", dst_col)
                    })?;

                    let eid = writer.next_eid(type_id).await?;
                    writer.insert_edge(src, dst, type_id, eid, props).await?;
                    count += 1;
                }
            }
        }

        let mut res = HashMap::new();
        res.insert("count".to_string(), json!(count));
        Ok(vec![res])
    }

    pub(crate) async fn scan_edge_type(
        &self,
        edge_type: &str,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<(uni_common::core::id::Eid, Vid, Vid)>> {
        let mut edges: HashMap<uni_common::core::id::Eid, (Vid, Vid)> = HashMap::new();

        // 1. Scan L2 (Base)
        self.scan_edge_type_l2(edge_type, &mut edges).await?;

        // 2. Scan L1 (Delta)
        self.scan_edge_type_l1(edge_type, &mut edges).await?;

        // 3. Scan L0 (Memory) and filter tombstoned vertices
        if let Some(ctx) = ctx {
            self.scan_edge_type_l0(edge_type, ctx, &mut edges);
            self.filter_tombstoned_vertex_edges(ctx, &mut edges);
        }

        Ok(edges
            .into_iter()
            .map(|(eid, (src, dst))| (eid, src, dst))
            .collect())
    }

    /// Scan L2 (base) storage for edges of a given type.
    pub(crate) async fn scan_edge_type_l2(
        &self,
        edge_type: &str,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) -> Result<()> {
        if let Ok(ds) = self.storage.edge_dataset(edge_type, "", "")
            && let Ok(dataset) = ds.open().await
        {
            let mut scanner = dataset.scan();
            scanner.project(&["eid", "src_vid", "dst_vid"])?;
            let mut stream = scanner.try_into_stream().await?;
            use futures::TryStreamExt;
            while let Some(batch) = stream.try_next().await? {
                self.process_edge_batch(&batch, edges)?;
            }
        }
        Ok(())
    }

    /// Process a record batch of edges, adding them to the edges map.
    pub(crate) fn process_edge_batch(
        &self,
        batch: &arrow_array::RecordBatch,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) -> Result<()> {
        use arrow_array::UInt64Array;
        let eid_col = batch
            .column_by_name("eid")
            .ok_or(anyhow!("Missing eid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid eid"))?;
        let src_col = batch
            .column_by_name("src_vid")
            .ok_or(anyhow!("Missing src_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid src_vid"))?;
        let dst_col = batch
            .column_by_name("dst_vid")
            .ok_or(anyhow!("Missing dst_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid dst_vid"))?;

        for i in 0..batch.num_rows() {
            let eid = uni_common::core::id::Eid::from(eid_col.value(i));
            let src = Vid::from(src_col.value(i));
            let dst = Vid::from(dst_col.value(i));
            edges.insert(eid, (src, dst));
        }
        Ok(())
    }

    /// Scan L1 (delta) storage for edges of a given type.
    pub(crate) async fn scan_edge_type_l1(
        &self,
        edge_type: &str,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) -> Result<()> {
        if let Ok(_ds) = self.storage.delta_dataset(edge_type, "fwd") {
            let path = format!("{}/deltas/{}_fwd", self.storage.base_path(), edge_type);
            if let Ok(dataset) = lance::Dataset::open(&path).await {
                let mut scanner = dataset.scan();
                scanner.project(&["eid", "src_vid", "dst_vid", "op", "_version"])?;
                let mut stream = scanner.try_into_stream().await?;
                use futures::TryStreamExt;

                // Collect ops with versions: eid -> (version, op, src, dst)
                let mut versioned_ops: HashMap<uni_common::core::id::Eid, (u64, u8, Vid, Vid)> =
                    HashMap::new();

                while let Some(batch) = stream.try_next().await? {
                    self.process_delta_batch(&batch, &mut versioned_ops)?;
                }

                // Apply the winning ops
                for (eid, (_, op, src, dst)) in versioned_ops {
                    if op == 0 {
                        edges.insert(eid, (src, dst));
                    } else if op == 1 {
                        edges.remove(&eid);
                    }
                }
            }
        }
        Ok(())
    }

    /// Process a delta batch, tracking versioned operations.
    pub(crate) fn process_delta_batch(
        &self,
        batch: &arrow_array::RecordBatch,
        versioned_ops: &mut HashMap<uni_common::core::id::Eid, (u64, u8, Vid, Vid)>,
    ) -> Result<()> {
        use arrow_array::UInt64Array;
        let eid_col = batch
            .column_by_name("eid")
            .ok_or(anyhow!("Missing eid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid eid"))?;
        let src_col = batch
            .column_by_name("src_vid")
            .ok_or(anyhow!("Missing src_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid src_vid"))?;
        let dst_col = batch
            .column_by_name("dst_vid")
            .ok_or(anyhow!("Missing dst_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid dst_vid"))?;
        let op_col = batch
            .column_by_name("op")
            .ok_or(anyhow!("Missing op"))?
            .as_any()
            .downcast_ref::<arrow_array::UInt8Array>()
            .ok_or(anyhow!("Invalid op"))?;
        let version_col = batch
            .column_by_name("_version")
            .ok_or(anyhow!("Missing _version"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid _version"))?;

        for i in 0..batch.num_rows() {
            let eid = uni_common::core::id::Eid::from(eid_col.value(i));
            let version = version_col.value(i);
            let op = op_col.value(i);
            let src = Vid::from(src_col.value(i));
            let dst = Vid::from(dst_col.value(i));

            match versioned_ops.entry(eid) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert((version, op, src, dst));
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if version > e.get().0 {
                        e.insert((version, op, src, dst));
                    }
                }
            }
        }
        Ok(())
    }

    /// Scan L0 (memory) buffers for edges of a given type.
    pub(crate) fn scan_edge_type_l0(
        &self,
        edge_type: &str,
        ctx: &QueryContext,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) {
        let schema = self.storage.schema_manager().schema();
        let type_id = schema.edge_types.get(edge_type).map(|m| m.id);

        if let Some(type_id) = type_id {
            // Main L0
            self.scan_single_l0(&ctx.l0.read(), type_id, edges);

            // Transaction L0
            if let Some(tx_l0_arc) = &ctx.transaction_l0 {
                self.scan_single_l0(&tx_l0_arc.read(), type_id, edges);
            }

            // Pending flush L0s
            for pending_l0_arc in &ctx.pending_flush_l0s {
                self.scan_single_l0(&pending_l0_arc.read(), type_id, edges);
            }
        }
    }

    /// Scan a single L0 buffer for edges and apply tombstones.
    pub(crate) fn scan_single_l0(
        &self,
        l0: &uni_store::runtime::L0Buffer,
        type_id: u16,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) {
        for edge_entry in l0.graph.edges() {
            if edge_entry.edge_type == type_id {
                edges.insert(edge_entry.eid, (edge_entry.src_vid, edge_entry.dst_vid));
            }
        }
        // Process Tombstones
        let eids_to_check: Vec<_> = edges.keys().cloned().collect();
        for eid in eids_to_check {
            if l0.is_tombstoned(eid) {
                edges.remove(&eid);
            }
        }
    }

    /// Filter out edges connected to tombstoned vertices.
    pub(crate) fn filter_tombstoned_vertex_edges(
        &self,
        ctx: &QueryContext,
        edges: &mut HashMap<uni_common::core::id::Eid, (Vid, Vid)>,
    ) {
        let l0 = ctx.l0.read();
        let mut all_vertex_tombstones = l0.vertex_tombstones.clone();

        // Include tx_l0 vertex tombstones if present
        if let Some(tx_l0_arc) = &ctx.transaction_l0 {
            let tx_l0 = tx_l0_arc.read();
            all_vertex_tombstones.extend(tx_l0.vertex_tombstones.iter().cloned());
        }

        // Include pending flush L0 vertex tombstones
        for pending_l0_arc in &ctx.pending_flush_l0s {
            let pending_l0 = pending_l0_arc.read();
            all_vertex_tombstones.extend(pending_l0.vertex_tombstones.iter().cloned());
        }

        edges.retain(|_, (src, dst)| {
            !all_vertex_tombstones.contains(src) && !all_vertex_tombstones.contains(dst)
        });
    }

    /// Execute a vector KNN search.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_vector_knn(
        &self,
        label_id: u16,
        variable: &str,
        property: &str,
        query: &Expr,
        k: usize,
        threshold: Option<f32>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let empty_row = HashMap::new();
        let query_val = self
            .evaluate_expr(query, &empty_row, prop_manager, params, ctx)
            .await?;

        let query_vector: Vec<f32> = match query_val {
            Value::Array(arr) => {
                let mut vec = Vec::with_capacity(arr.len());
                for v in arr {
                    if let Some(f) = v.as_f64() {
                        vec.push(f as f32);
                    } else {
                        return Err(anyhow!("Query vector must contain numbers"));
                    }
                }
                vec
            }
            _ => return Err(anyhow!("Query vector must be an array")),
        };

        let schema = self.storage.schema_manager().schema();
        let label_name = schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

        let results = self
            .storage
            .vector_search(label_name, property, &query_vector, k)
            .await?;

        let mut matches = Vec::new();
        for (vid, dist) in results {
            if let Some(thresh) = threshold {
                // Convert distance to similarity (assuming Cosine/Dot)
                // TODO: Check index metric from schema for precise conversion
                let sim = 1.0 - dist;
                if sim < thresh {
                    continue;
                }
            }
            let mut m = HashMap::new();
            m.insert(variable.to_string(), Value::String(vid.to_string()));
            matches.push(m);
        }
        Ok(matches)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_inverted_index_lookup(
        &self,
        label_id: u16,
        variable: &str,
        property: &str,
        terms_expr: &Expr,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let empty_row = HashMap::new();
        let terms_val = self
            .evaluate_expr(terms_expr, &empty_row, prop_manager, params, ctx)
            .await?;

        let terms: Vec<String> = match terms_val {
            Value::Array(arr) => arr
                .iter()
                .map(|v| v.as_str().map(|s| s.to_string()).unwrap_or_default())
                .collect(),
            _ => return Err(anyhow!("Terms must be a list")),
        };

        let schema = self.storage.schema_manager().schema();
        let label_name = schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

        let index = self.storage.inverted_index(label_name, property).await?;
        let vids = index.query_any(&terms).await?;

        let mut matches = Vec::with_capacity(vids.len());
        for vid in vids {
            // Check visibility/deletion and fetch properties
            // We use get_all_vertex_props_with_ctx to respect transaction isolation
            let props_opt = prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await?;
            if let Some(props) = props_opt {
                let mut props_json: serde_json::Map<String, Value> = props.into_iter().collect();
                props_json.insert("_vid".to_string(), json!(vid.as_u64()));

                let mut row = HashMap::new();
                row.insert(variable.to_string(), Value::Object(props_json));
                matches.push(row);
            }
        }
        Ok(matches)
    }

    /// Execute a projection operation.
    pub(crate) async fn execute_project(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        projections: &[(Expr, Option<String>)],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut results = Vec::new();
        for m in input_rows {
            let mut row = HashMap::new();
            for (expr, alias) in projections {
                let val = self
                    .evaluate_expr(expr, &m, prop_manager, params, ctx)
                    .await?;
                let name = alias.clone().unwrap_or_else(|| expr.to_string_repr());
                row.insert(name, val);
            }
            results.push(row);
        }
        Ok(results)
    }

    /// Execute an UNWIND operation.
    pub(crate) async fn execute_unwind(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        expr: &Expr,
        variable: &str,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut results = Vec::new();
        for row in input_rows {
            let val = self
                .evaluate_expr(expr, &row, prop_manager, params, ctx)
                .await?;
            if let Value::Array(items) = val {
                for item in items {
                    let mut new_row = row.clone();
                    new_row.insert(variable.to_string(), item);
                    results.push(new_row);
                }
            }
        }
        Ok(results)
    }

    /// Execute an APPLY (correlated subquery) operation.
    pub(crate) async fn execute_apply(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        subquery: &LogicalPlan,
        input_filter: Option<&Expr>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut filtered_rows = input_rows;

        if let Some(filter) = input_filter {
            let mut filtered = Vec::new();
            for row in filtered_rows {
                let res = self
                    .evaluate_expr(filter, &row, prop_manager, params, ctx)
                    .await?;
                if res.as_bool().unwrap_or(false) {
                    filtered.push(row);
                }
            }
            filtered_rows = filtered;
        }

        let mut results = Vec::new();
        for row in filtered_rows {
            let mut sub_params = params.clone();
            sub_params.extend(row.clone());

            let sub_rows = self
                .execute_subplan(subquery.clone(), prop_manager, &sub_params, ctx)
                .await?;

            for sub_row in sub_rows {
                let mut new_row = row.clone();
                new_row.extend(sub_row);
                results.push(new_row);
            }
        }
        Ok(results)
    }

    /// Execute SHOW INDEXES command.
    pub(crate) fn execute_show_indexes(&self, filter: Option<&str>) -> Vec<HashMap<String, Value>> {
        let schema = self.storage.schema_manager().schema();
        let mut rows = Vec::new();
        for idx in schema.indexes {
            let (name, type_str, details) = match idx {
                uni_common::core::schema::IndexDefinition::Vector(c) => (
                    c.name,
                    "VECTOR",
                    format!("{:?} on {}.{}", c.index_type, c.label, c.property),
                ),
                uni_common::core::schema::IndexDefinition::FullText(c) => (
                    c.name,
                    "FULLTEXT",
                    format!("on {}:{:?}", c.label, c.properties),
                ),
                uni_common::core::schema::IndexDefinition::Scalar(cfg) => (
                    cfg.name.clone(),
                    "SCALAR",
                    format!(":{}({:?})", cfg.label, cfg.properties),
                ),
                _ => ("UNKNOWN".to_string(), "UNKNOWN", "".to_string()),
            };

            if let Some(f) = filter
                && f != type_str
            {
                continue;
            }

            let mut row = HashMap::new();
            row.insert("name".to_string(), Value::String(name));
            row.insert("type".to_string(), Value::String(type_str.to_string()));
            row.insert("details".to_string(), Value::String(details));
            rows.push(row);
        }
        rows
    }

    pub(crate) fn execute_show_database(&self) -> Vec<HashMap<String, Value>> {
        let mut row = HashMap::new();
        row.insert("name".to_string(), Value::String("uni".to_string()));
        // Could add storage path, etc.
        vec![row]
    }

    pub(crate) fn execute_show_config(&self) -> Vec<HashMap<String, Value>> {
        // Placeholder as we don't easy access to config struct from here
        vec![]
    }

    pub(crate) async fn execute_show_statistics(&self) -> Result<Vec<HashMap<String, Value>>> {
        let snapshot = self
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?;
        let mut results = Vec::new();

        if let Some(snap) = snapshot {
            for (label, s) in &snap.vertices {
                let mut row = HashMap::new();
                row.insert("type".to_string(), Value::String("Label".to_string()));
                row.insert("name".to_string(), Value::String(label.clone()));
                row.insert("count".to_string(), json!(s.count));
                results.push(row);
            }
            for (edge, s) in &snap.edges {
                let mut row = HashMap::new();
                row.insert("type".to_string(), Value::String("Edge".to_string()));
                row.insert("name".to_string(), Value::String(edge.clone()));
                row.insert("count".to_string(), json!(s.count));
                results.push(row);
            }
        }

        Ok(results)
    }

    pub(crate) fn execute_show_constraints(
        &self,
        clause: ShowConstraintsClause,
    ) -> Vec<HashMap<String, Value>> {
        let schema = self.storage.schema_manager().schema();
        let mut rows = Vec::new();
        for c in schema.constraints {
            if let Some(target) = &clause.target {
                match (target, &c.target) {
                    (AstConstraintTarget::Label(l1), ConstraintTarget::Label(l2)) if l1 == l2 => {}
                    (AstConstraintTarget::EdgeType(e1), ConstraintTarget::EdgeType(e2))
                        if e1 == e2 => {}
                    _ => continue,
                }
            }

            let mut row = HashMap::new();
            row.insert("name".to_string(), Value::String(c.name));
            let type_str = match c.constraint_type {
                ConstraintType::Unique { .. } => "UNIQUE",
                ConstraintType::Exists { .. } => "EXISTS",
                ConstraintType::Check { .. } => "CHECK",
                _ => "UNKNOWN",
            };
            row.insert("type".to_string(), Value::String(type_str.to_string()));

            let target_str = match c.target {
                ConstraintTarget::Label(l) => format!("(:{})", l),
                ConstraintTarget::EdgeType(e) => format!("[:{}]", e),
                _ => "UNKNOWN".to_string(),
            };
            row.insert("target".to_string(), Value::String(target_str));

            rows.push(row);
        }
        rows
    }

    /// Execute a MERGE operation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_cross_join(
        &self,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let left_rows = self
            .execute_subplan(*left, prop_manager, params, ctx)
            .await?;
        let right_rows = self
            .execute_subplan(*right, prop_manager, params, ctx)
            .await?;

        let mut results = Vec::new();
        for l in &left_rows {
            for r in &right_rows {
                let mut combined = l.clone();
                combined.extend(r.clone());
                results.push(combined);
            }
        }
        Ok(results)
    }

    /// Execute a UNION operation with optional deduplication.
    pub(crate) async fn execute_union(
        &self,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut left_rows = self
            .execute_subplan(*left, prop_manager, params, ctx)
            .await?;
        let mut right_rows = self
            .execute_subplan(*right, prop_manager, params, ctx)
            .await?;

        left_rows.append(&mut right_rows);

        if !all {
            let mut seen = HashSet::new();
            left_rows.retain(|row| {
                let sorted_row: std::collections::BTreeMap<_, _> = row.iter().collect();
                let json = serde_json::to_string(&sorted_row).unwrap();
                seen.insert(json)
            });
        }
        Ok(left_rows)
    }

    /// Check if an index with the given name exists.
    pub(crate) fn index_exists_by_name(&self, name: &str) -> bool {
        let schema = self.storage.schema_manager().schema();
        schema.indexes.iter().any(|idx| match idx {
            uni_common::core::schema::IndexDefinition::Vector(c) => c.name == name,
            uni_common::core::schema::IndexDefinition::FullText(c) => c.name == name,
            uni_common::core::schema::IndexDefinition::Scalar(c) => c.name == name,
            _ => false,
        })
    }

    pub(crate) async fn execute_export(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let format = options
            .get("format")
            .and_then(|v| v.as_str())
            .unwrap_or("csv")
            .to_lowercase();

        match format.as_str() {
            "csv" => {
                self.execute_csv_export(target, source, options, prop_manager, ctx)
                    .await
            }
            "parquet" => {
                self.execute_parquet_export(target, source, options, prop_manager, ctx)
                    .await
            }
            _ => Err(anyhow!("Unsupported export format: {}", format)),
        }
    }

    pub(crate) async fn execute_csv_export(
        &self,
        target: &str,
        source: &str,
        options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Validate destination path against sandbox
        let validated_dest = self.validate_path(source)?;

        let schema = self.storage.schema_manager().schema();
        let label_meta = schema.labels.get(target);
        let edge_meta = schema.edge_types.get(target);

        if label_meta.is_none() && edge_meta.is_none() {
            return Err(anyhow!("Target '{}' not found in schema", target));
        }

        let delimiter_str = options
            .get("delimiter")
            .and_then(|v| v.as_str())
            .unwrap_or(",");
        let delimiter = if delimiter_str.is_empty() {
            b','
        } else {
            delimiter_str.as_bytes()[0]
        };
        let has_header = options
            .get("header")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let mut wtr = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .from_path(&validated_dest)?;

        let mut count = 0;
        // Empty properties map for labels/edge types without registered properties
        let empty_props = HashMap::new();

        if let Some(meta) = label_meta {
            let label_id = meta.id;
            let props_meta = schema.properties.get(target).unwrap_or(&empty_props);
            let mut prop_names: Vec<_> = props_meta.keys().cloned().collect();
            prop_names.sort();

            let mut headers = vec!["_vid".to_string()];
            headers.extend(prop_names.clone());

            if has_header {
                wtr.write_record(&headers)?;
            }

            let vids = self
                .scan_label_with_filter(label_id, "n", None, ctx, prop_manager, &HashMap::new())
                .await?;

            for vid in vids {
                let props = prop_manager
                    .get_all_vertex_props_with_ctx(vid, ctx)
                    .await?
                    .unwrap_or_default();

                let mut row = Vec::with_capacity(headers.len());
                row.push(vid.to_string());
                for p_name in &prop_names {
                    let val = props.get(p_name).cloned().unwrap_or(Value::Null);
                    row.push(self.format_csv_value(val));
                }
                wtr.write_record(&row)?;
                count += 1;
            }
        } else if let Some(meta) = edge_meta {
            let props_meta = schema.properties.get(target).unwrap_or(&empty_props);
            let mut prop_names: Vec<_> = props_meta.keys().cloned().collect();
            prop_names.sort();

            // Headers for Edge: _eid, _src, _dst, _type, ...props
            let mut headers = vec![
                "_eid".to_string(),
                "_src".to_string(),
                "_dst".to_string(),
                "_type".to_string(),
            ];
            headers.extend(prop_names.clone());

            if has_header {
                wtr.write_record(&headers)?;
            }

            let edges = self.scan_edge_type(target, ctx).await?;

            for (eid, src, dst) in edges {
                let props = prop_manager
                    .get_all_edge_props_with_ctx(eid, ctx)
                    .await?
                    .unwrap_or_default();

                let mut row = Vec::with_capacity(headers.len());
                row.push(eid.to_string());
                row.push(src.to_string());
                row.push(dst.to_string());
                row.push(meta.id.to_string());

                for p_name in &prop_names {
                    let val = props.get(p_name).cloned().unwrap_or(Value::Null);
                    row.push(self.format_csv_value(val));
                }
                wtr.write_record(&row)?;
                count += 1;
            }
        }

        wtr.flush()?;
        let mut res = HashMap::new();
        res.insert("count".to_string(), json!(count));
        Ok(vec![res])
    }

    pub(crate) async fn execute_parquet_export(
        &self,
        target: &str,
        source: &str,
        _options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Validate destination path against sandbox
        let validated_dest = self.validate_path(source)?;

        let schema_manager = self.storage.schema_manager();
        let schema = schema_manager.schema();
        let label_meta = schema.labels.get(target);
        let edge_meta = schema.edge_types.get(target);

        if label_meta.is_none() && edge_meta.is_none() {
            return Err(anyhow!("Target '{}' not found in schema", target));
        }

        let arrow_schema = if label_meta.is_some() {
            let dataset = self.storage.vertex_dataset(target)?;
            dataset.get_arrow_schema(&schema)?
        } else {
            // Edge Schema
            let dataset = self.storage.edge_dataset(target, "", "")?;
            dataset.get_arrow_schema(&schema)?
        };

        let mut rows = Vec::new();

        if let Some(meta) = label_meta {
            let label_id = meta.id;
            let vids = self
                .scan_label_with_filter(label_id, "n", None, ctx, prop_manager, &HashMap::new())
                .await?;

            for vid in vids {
                let mut props = prop_manager
                    .get_all_vertex_props_with_ctx(vid, ctx)
                    .await?
                    .unwrap_or_default();

                props.insert("_vid".to_string(), json!(vid.as_u64()));
                if !props.contains_key("_uid") {
                    props.insert("_uid".to_string(), Value::Array(vec![json!(0); 32]));
                }
                props.insert("_deleted".to_string(), Value::Bool(false));
                props.insert("_version".to_string(), json!(1));
                rows.push(props);
            }
        } else if edge_meta.is_some() {
            let edges = self.scan_edge_type(target, ctx).await?;
            for (eid, src, dst) in edges {
                let mut props = prop_manager
                    .get_all_edge_props_with_ctx(eid, ctx)
                    .await?
                    .unwrap_or_default();

                props.insert("eid".to_string(), json!(eid.as_u64())); // schema uses 'eid', not '_eid'? Check EdgeDataset
                props.insert("src_vid".to_string(), json!(src.as_u64()));
                props.insert("dst_vid".to_string(), json!(dst.as_u64()));
                props.insert("_deleted".to_string(), Value::Bool(false));
                props.insert("_version".to_string(), json!(1));
                rows.push(props);
            }
        }

        let file = std::fs::File::create(&validated_dest)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, arrow_schema.clone(), None)?;

        // Write all in one batch for now (simplification)
        if !rows.is_empty() {
            let batch = self.rows_to_batch(&rows, &arrow_schema)?;
            writer.write(&batch)?;
        }

        writer.close()?;

        let mut res = HashMap::new();
        res.insert("count".to_string(), json!(rows.len()));
        Ok(vec![res])
    }

    pub(crate) fn rows_to_batch(
        &self,
        rows: &[HashMap<String, Value>],
        schema: &arrow_schema::Schema,
    ) -> Result<RecordBatch> {
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();

        for field in schema.fields() {
            let name = field.name();
            let dt = field.data_type();

            let values: Vec<Value> = rows
                .iter()
                .map(|row| row.get(name).cloned().unwrap_or(Value::Null))
                .collect();
            let array = self.values_to_array(&values, dt)?;
            columns.push(array);
        }

        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }

    /// Convert a slice of JSON Values to an Arrow array.
    /// Delegates to the shared implementation in arrow_convert module.
    pub(crate) fn values_to_array(
        &self,
        values: &[Value],
        dt: &arrow_schema::DataType,
    ) -> Result<Arc<dyn Array>> {
        arrow_convert::values_to_array(values, dt)
    }

    pub(crate) fn format_csv_value(&self, val: Value) -> String {
        match val {
            Value::Null => "".to_string(),
            Value::String(s) => s,
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Array(a) => serde_json::to_string(&a).unwrap(),
            Value::Object(o) => serde_json::to_string(&o).unwrap(),
        }
    }

    pub(crate) fn parse_csv_value(
        &self,
        s: &str,
        data_type: &uni_common::core::schema::DataType,
        prop_name: &str,
    ) -> Result<Value> {
        if s.is_empty() || s.to_lowercase() == "null" {
            return Ok(Value::Null);
        }

        use uni_common::core::schema::DataType;
        match data_type {
            DataType::String => Ok(Value::String(s.to_string())),
            DataType::Int32 | DataType::Int64 => {
                let i = s.parse::<i64>().map_err(|_| {
                    anyhow!(
                        "Failed to parse integer for property '{}': {}",
                        prop_name,
                        s
                    )
                })?;
                Ok(json!(i))
            }
            DataType::Float32 | DataType::Float64 => {
                let f = s.parse::<f64>().map_err(|_| {
                    anyhow!("Failed to parse float for property '{}': {}", prop_name, s)
                })?;
                Ok(json!(f))
            }
            DataType::Bool => {
                let b = s.to_lowercase().parse::<bool>().map_err(|_| {
                    anyhow!(
                        "Failed to parse boolean for property '{}': {}",
                        prop_name,
                        s
                    )
                })?;
                Ok(Value::Bool(b))
            }
            DataType::Json => {
                let v: Value = serde_json::from_str(s).map_err(|_| {
                    anyhow!("Failed to parse JSON for property '{}': {}", prop_name, s)
                })?;
                Ok(v)
            }
            DataType::Vector { .. } => {
                let v: Vec<f32> = serde_json::from_str(s).map_err(|_| {
                    anyhow!("Failed to parse Vector for property '{}': {}", prop_name, s)
                })?;
                let json_vec: Vec<Value> = v.iter().map(|f| json!(f)).collect();
                Ok(Value::Array(json_vec))
            }
            _ => Ok(Value::String(s.to_string())),
        }
    }

    pub(crate) async fn detach_delete_vertex(&self, vid: Vid, writer: &mut Writer) -> Result<()> {
        let schema = self.storage.schema_manager().schema();
        let edge_type_ids: Vec<u16> = schema.edge_types.values().map(|m| m.id).collect();

        // 1. Find and delete all outgoing edges
        let out_graph = self
            .storage
            .load_subgraph_cached(
                &[vid],
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Outgoing,
                Some(writer.l0_manager.get_current()),
            )
            .await?;

        for edge in out_graph.edges() {
            writer
                .delete_edge(edge.eid, edge.src_vid, edge.dst_vid, edge.edge_type)
                .await?;
        }

        // 2. Find and delete all incoming edges
        let in_graph = self
            .storage
            .load_subgraph_cached(
                &[vid],
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Incoming,
                Some(writer.l0_manager.get_current()),
            )
            .await?;

        for edge in in_graph.edges() {
            writer
                .delete_edge(edge.eid, edge.src_vid, edge.dst_vid, edge.edge_type)
                .await?;
        }

        Ok(())
    }
}
