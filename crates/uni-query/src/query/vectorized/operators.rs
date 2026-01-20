// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use arrow_array::builder::{BooleanBuilder, ListBuilder, UInt64Builder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, ListArray,
    MapArray, RecordBatch, StringArray, StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::query::expr::{Expr, Operator, UnaryOperator};
use crate::query::expr_eval::eval_scalar_function;
use crate::query::vectorized::aggregates::compare_values;
use crate::query::vectorized::batch::VectorizedBatch;
use crate::query::vectorized::crdt_functions::{eval_crdt_function, is_crdt_function};
use lance_index::scalar::FullTextSearchQuery;
use parking_lot::RwLock;
use serde_json::{Value, json};
use tokio::sync::RwLock as AsyncRwLock;
use uni_common::config::UniConfig;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::Schema;
use uni_store::QueryContext;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::adjacency_cache::{AdjacencyCache, Direction};
use uni_store::storage::manager::StorageManager;

/// Merge a single-row batch into a multi-row batch by broadcasting columns.
fn broadcast_merge(batch: &mut VectorizedBatch, input: &VectorizedBatch) -> Result<()> {
    if input.num_rows() != 1 {
        return Err(anyhow!("Broadcast merge requires single-row input"));
    }

    let count = batch.num_rows();
    // Input must be compacted to ensure row 0 is valid
    let input = input.compact()?;

    for (name, &idx) in &input.variables {
        if batch.variables.contains_key(name) {
            continue;
        }

        let col = input.data.column(idx);
        let scalar = arrow_val_to_json(col.as_ref(), 0);

        let vals = vec![scalar; count];
        let array = json_vec_to_array(&vals);

        batch.add_column(name.clone(), array)?;
    }
    Ok(())
}

/// Execution Context for Vectorized Operators
#[derive(Clone)]
pub struct ExecutionContext {
    pub storage: Arc<StorageManager>,
    pub property_manager: Arc<PropertyManager>,
    pub l0: Option<Arc<RwLock<L0Buffer>>>,
    pub transaction_l0: Option<Arc<RwLock<L0Buffer>>>,
    /// L0 buffers currently being flushed (still visible to reads).
    pub pending_flush_l0s: Vec<Arc<RwLock<L0Buffer>>>,
    pub writer: Option<Arc<AsyncRwLock<Writer>>>,
    pub config: UniConfig,
    pub deadline: Option<Instant>,
    pub current_memory: Arc<AtomicUsize>,
    pub ctes: HashMap<String, Value>,
}

impl ExecutionContext {
    pub fn with_cte(&self, name: &str, value: Value) -> Self {
        let mut new_ctx = self.clone();
        new_ctx.ctes.insert(name.to_string(), value);
        new_ctx
    }

    /// Check if the query has timed out or exceeded memory limits.
    pub fn check_timeout(&self) -> Result<()> {
        if let Some(deadline) = self.deadline
            && Instant::now() > deadline
        {
            return Err(anyhow!("Query timed out"));
        }

        if self.current_memory.load(Ordering::Relaxed) > self.config.max_query_memory {
            return Err(anyhow!(
                "Query exceeded memory limit of {} bytes",
                self.config.max_query_memory
            ));
        }

        Ok(())
    }

    /// Observe memory usage of a batch and update total.
    pub fn observe_memory(&self, batch: &VectorizedBatch) -> Result<()> {
        let usage = batch.get_memory_usage();
        self.current_memory.fetch_add(usage, Ordering::Relaxed);
        self.check_timeout()
    }
}

#[derive(Clone)]
pub struct PhysicalPlan {
    pub operators: Vec<Arc<dyn VectorizedOperator>>,
}

impl PhysicalPlan {
    pub async fn execute(&self, ctx: &ExecutionContext) -> Result<VectorizedBatch> {
        self.execute_with_input(None, ctx).await
    }

    pub async fn execute_with_input(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        if self.operators.is_empty() {
            return Err(anyhow!("Empty plan"));
        }

        // Split operators into Parallel Stage and Sequential Stage.
        // The Parallel Stage ends before the first Pipeline Breaker.
        let mut parallel_ops = Vec::new();
        let mut sequential_ops = Vec::new();
        let mut found_breaker = false;

        for op in &self.operators {
            if found_breaker {
                sequential_ops.push(op.clone());
            } else if op.is_pipeline_breaker() {
                found_breaker = true;
                sequential_ops.push(op.clone());
            } else {
                parallel_ops.push(op.clone());
            }
        }

        // --- Execute Parallel Stage ---
        let mut batch = if parallel_ops.is_empty() {
            // Should not happen if source is always first and not a breaker?
            // Scan is not a breaker.
            // If the plan is just "Empty" (VectorizedSingle), it's not a breaker.
            // If the plan starts with a breaker? (unlikely for now)
            return Err(anyhow!("Plan must start with a source operator"));
        } else {
            self.execute_parallel(&parallel_ops, input, ctx).await?
        };

        // --- Execute Sequential Stage ---
        for op in sequential_ops {
            batch = op.execute(Some(batch), ctx).await?;
        }

        Ok(batch)
    }

    pub fn execute_stream(
        self,
        input: Option<VectorizedBatch>, // Changed to accept input
        ctx: ExecutionContext,
    ) -> BoxStream<'static, Result<VectorizedBatch>> {
        if let Err(e) = ctx.check_timeout() {
            return Box::pin(stream::once(async { Err(e) }));
        }
        if self.operators.is_empty() {
            return Box::pin(stream::once(async { Err(anyhow!("Empty plan")) }));
        }

        // Split operators into Parallel Stage and Sequential Stage.
        let mut parallel_ops = Vec::new();
        let mut sequential_ops = Vec::new();
        let mut found_breaker = false;

        for op in &self.operators {
            if found_breaker {
                sequential_ops.push(op.clone());
            } else if op.is_pipeline_breaker() {
                found_breaker = true;
                sequential_ops.push(op.clone());
            } else {
                parallel_ops.push(op.clone());
            }
        }

        if found_breaker {
            // Materialize everything if there's a pipeline breaker
            let fut = async move { self.execute_with_input(input, &ctx).await };
            return Box::pin(stream::once(fut));
        }

        // Streaming execution for pure pipeline
        self.execute_parallel_stream(parallel_ops, input, ctx)
    }

    fn execute_parallel_stream(
        self,
        ops: Vec<Arc<dyn VectorizedOperator>>,
        input: Option<VectorizedBatch>,
        ctx: ExecutionContext,
    ) -> BoxStream<'static, Result<VectorizedBatch>> {
        let source = ops[0].clone();
        let pipeline = Arc::new(ops[1..].to_vec());
        let parallelism = ctx.config.parallelism;
        let ctx_clone = ctx.clone();

        // Source executes with input
        source
            .execute_stream(input, ctx.clone())
            .map(move |initial_batch_res| {
                let p_inner = pipeline.clone();
                let c_inner = ctx_clone.clone();

                async move {
                    let initial_batch = initial_batch_res?;
                    let mut batch = initial_batch;
                    for op in p_inner.iter() {
                        batch = op.execute(Some(batch), &c_inner).await?;
                        if batch.num_rows() == 0 {
                            break;
                        }
                    }
                    Ok(batch)
                }
            })
            .buffer_unordered(parallelism)
            .filter(|res| {
                futures::future::ready(match res {
                    Ok(b) => b.num_rows() > 0,
                    Err(_) => true,
                })
            })
            .boxed()
    }

    async fn execute_parallel(
        &self,
        ops: &[Arc<dyn VectorizedOperator>],
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        // 1. Execute Source (Scan) - Single Threaded for now (but Scan uses stream internally)
        let source = &ops[0];
        let initial_batch = source.execute(input, ctx).await?;

        if ops.len() == 1 {
            return Ok(initial_batch);
        }

        // 2. Partition into Morsels
        let total_rows = initial_batch.num_rows();
        if total_rows == 0 {
            // Run empty batch through pipeline for correctness schema
            let empty_in = initial_batch.slice(0, 0);
            let pipeline = Arc::new(ops[1..].to_vec());
            let mut empty_out = empty_in;
            for op in pipeline.iter() {
                empty_out = op.execute(Some(empty_out), ctx).await?;
            }
            return Ok(empty_out);
        }

        let batch_size = ctx.config.batch_size;
        let mut morsels = Vec::new();
        let mut offset = 0;

        while offset < total_rows {
            let length = std::cmp::min(batch_size, total_rows - offset);
            morsels.push(initial_batch.slice(offset, length));
            offset += length;
        }

        // 3. Parallel Pipeline Execution
        let pipeline = Arc::new(ops[1..].to_vec());
        let parallelism = ctx.config.parallelism;

        let results: Vec<Result<VectorizedBatch>> = stream::iter(morsels)
            .map(|morsel| {
                let p = pipeline.clone();
                let task_ctx = ctx.clone();

                async move {
                    let mut batch = morsel;
                    for op in p.iter() {
                        batch = op.execute(Some(batch), &task_ctx).await?;
                        if batch.num_rows() == 0 {
                            break;
                        }
                    }
                    Ok(batch)
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // 4. Merge Results
        let mut valid_batches = Vec::new();
        for r in results {
            let b = r?;
            if b.num_rows() > 0 {
                valid_batches.push(b);
            }
        }

        if valid_batches.is_empty() {
            // Return empty batch with correct schema by running empty through pipeline
            let empty_in = initial_batch.slice(0, 0);
            let mut empty_out = empty_in;
            for op in pipeline.iter() {
                empty_out = op.execute(Some(empty_out), ctx).await?;
            }
            return Ok(empty_out);
        }

        VectorizedBatch::concat(&valid_batches)
    }
}

#[derive(Clone)]
pub struct VectorizedUnion {
    pub left: PhysicalPlan,
    pub right: PhysicalPlan,
    pub all: bool,
}

#[async_trait]
impl VectorizedOperator for VectorizedUnion {
    async fn execute(
        &self,
        _input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        // Union ignores input (it acts as a source combining two sub-plans)
        // Execute both plans
        let left_batch = self.left.execute(ctx).await?;
        let right_batch = self.right.execute(ctx).await?;

        // Concatenate
        let result = VectorizedBatch::concat(&[left_batch, right_batch])?;

        if !self.all {
            // Deduplicate logic
            // Compact first to simplify
            let compacted = result.compact()?;

            let mut seen = std::collections::HashSet::new();
            let mut keep_indices = Vec::new();

            for i in 0..compacted.num_rows() {
                let mut key = Vec::with_capacity(compacted.data.num_columns());
                for col in compacted.data.columns() {
                    let val = arrow_val_to_json(col.as_ref(), i);
                    match val {
                        Value::String(s) => key.push(s),
                        v => key.push(v.to_string()),
                    }
                }

                if seen.insert(key) {
                    keep_indices.push(i as u64);
                }
            }

            let indices = UInt64Array::from(keep_indices);
            let mut new_columns = Vec::new();
            for col in compacted.data.columns() {
                new_columns.push(arrow::compute::take(col.as_ref(), &indices, None)?);
            }

            let new_data = RecordBatch::try_new(compacted.data.schema(), new_columns)?;
            return Ok(VectorizedBatch::new(new_data, compacted.variables));
        }

        Ok(result)
    }
}

#[derive(Clone)]
pub struct VectorizedCrossJoin {
    pub left: PhysicalPlan,
    pub right: PhysicalPlan,
}

#[async_trait]
impl VectorizedOperator for VectorizedCrossJoin {
    async fn execute(
        &self,
        _input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        // CrossJoin ignores input (it combines two sub-plans)
        let left_batch = self.left.execute(ctx).await?.compact()?;
        let right_batch = self.right.execute(ctx).await?.compact()?;

        if left_batch.num_rows() == 0 || right_batch.num_rows() == 0 {
            // Return empty batch with combined schema
            let mut variables = left_batch.variables.clone();
            let offset = left_batch.data.num_columns();
            for (var, &idx) in &right_batch.variables {
                variables.insert(var.clone(), offset + idx);
            }

            let mut fields = left_batch.data.schema().fields().to_vec();
            fields.extend(right_batch.data.schema().fields().to_vec());
            let schema = Arc::new(ArrowSchema::new(fields));
            let empty_batch = RecordBatch::new_empty(schema);
            return Ok(VectorizedBatch::new(empty_batch, variables));
        }

        // Cartesian product
        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();
        let mut new_vars = left_batch.variables.clone();

        let l_rows = left_batch.num_rows();
        let r_rows = right_batch.num_rows();

        // Indices for 'take'
        // Left: [0, 0, ..., 1, 1, ...] (each row repeated r_rows times)
        let mut l_indices = UInt64Builder::with_capacity(l_rows * r_rows);
        for i in 0..l_rows {
            for _ in 0..r_rows {
                l_indices.append_value(i as u64);
            }
        }
        let l_indices_arr = l_indices.finish();

        // Right: [0, 1, ..., r_rows-1, 0, 1, ...] (repeated l_rows times)
        let mut r_indices = UInt64Builder::with_capacity(l_rows * r_rows);
        for _ in 0..l_rows {
            for j in 0..r_rows {
                r_indices.append_value(j as u64);
            }
        }
        let r_indices_arr = r_indices.finish();

        use arrow::compute::take;

        for i in 0..left_batch.data.num_columns() {
            let col = left_batch.data.column(i);
            let new_col = take(col.as_ref(), &l_indices_arr, None)?;
            new_columns.push(new_col);
            new_fields.push(left_batch.data.schema().field(i).clone());
        }

        let r_offset = new_columns.len();
        for i in 0..right_batch.data.num_columns() {
            let col = right_batch.data.column(i);
            let new_col = take(col.as_ref(), &r_indices_arr, None)?;
            new_columns.push(new_col);
            new_fields.push(right_batch.data.schema().field(i).clone());
        }

        for (var, &idx) in &right_batch.variables {
            new_vars.insert(var.clone(), r_offset + idx);
        }

        let schema = Arc::new(ArrowSchema::new(new_fields));
        let data = RecordBatch::try_new(schema, new_columns)?;
        Ok(VectorizedBatch::new(data, new_vars))
    }
}

// ============================================================================
// Property materialization helpers
// ============================================================================

/// Build a string array from property values.
fn build_string_prop_array(
    vids: &[Vid],
    props_map: &HashMap<Vid, HashMap<String, Value>>,
    prop_name: &str,
) -> Arc<dyn Array> {
    let mut builder = arrow_array::builder::StringBuilder::new();
    for vid in vids {
        if let Some(val) = props_map.get(vid).and_then(|p| p.get(prop_name)) {
            if let Some(s) = val.as_str() {
                builder.append_value(s);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

/// Build a uint64 array from property values.
fn build_uint64_prop_array(
    vids: &[Vid],
    props_map: &HashMap<Vid, HashMap<String, Value>>,
    prop_name: &str,
) -> Arc<dyn Array> {
    let mut builder = arrow_array::builder::UInt64Builder::new();
    for vid in vids {
        if let Some(val) = props_map.get(vid).and_then(|p| p.get(prop_name)) {
            if let Some(u) = val.as_u64() {
                builder.append_value(u);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

/// Build a float64 array from property values.
fn build_float64_prop_array(
    vids: &[Vid],
    props_map: &HashMap<Vid, HashMap<String, Value>>,
    prop_name: &str,
) -> Arc<dyn Array> {
    let mut builder = arrow_array::builder::Float64Builder::new();
    for vid in vids {
        if let Some(val) = props_map.get(vid).and_then(|p| p.get(prop_name)) {
            if let Some(f) = val.as_f64() {
                builder.append_value(f);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

/// Build a boolean array from property values.
fn build_bool_prop_array(
    vids: &[Vid],
    props_map: &HashMap<Vid, HashMap<String, Value>>,
    prop_name: &str,
) -> Arc<dyn Array> {
    let mut builder = arrow_array::builder::BooleanBuilder::new();
    for vid in vids {
        if let Some(val) = props_map.get(vid).and_then(|p| p.get(prop_name)) {
            if let Some(b) = val.as_bool() {
                builder.append_value(b);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

/// Build an Arrow array from property map based on the inferred type.
fn build_prop_array_from_type(
    vids: &[Vid],
    props_map: &HashMap<Vid, HashMap<String, Value>>,
    prop_name: &str,
    first_val: &Value,
) -> Result<Arc<dyn Array>> {
    match first_val {
        Value::String(_) => Ok(build_string_prop_array(vids, props_map, prop_name)),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                Ok(build_uint64_prop_array(vids, props_map, prop_name))
            } else {
                Ok(build_float64_prop_array(vids, props_map, prop_name))
            }
        }
        Value::Bool(_) => Ok(build_bool_prop_array(vids, props_map, prop_name)),
        _ => Err(anyhow!(
            "Unsupported property type for vectorized materialization: {:?}",
            first_val
        )),
    }
}

impl ExecutionContext {
    /// Create a query context for property lookups if L0 buffers are present.
    fn create_query_context(&self) -> Option<QueryContext> {
        if self.l0.is_some() || self.transaction_l0.is_some() || !self.pending_flush_l0s.is_empty()
        {
            Some(QueryContext::new_with_pending(
                self.l0.clone().unwrap_or_else(|| {
                    Arc::new(RwLock::new(uni_store::runtime::l0::L0Buffer::new(0, None)))
                }),
                self.transaction_l0.clone(),
                self.pending_flush_l0s.clone(),
            ))
        } else {
            None
        }
    }

    /// Find the first non-null property value to infer the type.
    fn find_first_non_null_prop<'a>(
        vids: &[Vid],
        props_map: &'a HashMap<Vid, HashMap<String, Value>>,
        prop_name: &str,
    ) -> &'a Value {
        for vid in vids {
            if let Some(props) = props_map.get(vid)
                && let Some(val) = props.get(prop_name)
                && !val.is_null()
            {
                return val;
            }
        }
        &Value::Null
    }

    /// Lazily materialize a property for all vertices in a column of a batch.
    pub async fn materialize_property(
        &self,
        batch: &mut VectorizedBatch,
        variable: &str,
        prop_name: &str,
    ) -> Result<()> {
        let prop_col_name = format!("{}.{}", variable, prop_name);
        if batch.variables.contains_key(&prop_col_name) {
            return Ok(());
        }

        let vid_col = batch.column(variable)?;
        let vids_uint64 = vid_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Column '{}' is not a VID column", variable))?;

        // 1. Prepare VIDs for batch load
        let vids: Vec<Vid> = (0..vids_uint64.len())
            .map(|i| Vid::from(vids_uint64.value(i)))
            .collect();

        // 2. Batch fetch from PropertyManager
        let ctx = self.create_query_context();

        let mut props_map = self
            .property_manager
            .get_batch_vertex_props(&vids, &[prop_name], ctx.as_ref())
            .await?;

        if props_map.is_empty() {
            let eids: Vec<Eid> = (0..vids_uint64.len())
                .map(|i| Eid::from(vids_uint64.value(i)))
                .collect();
            props_map = self
                .property_manager
                .get_batch_edge_props(&eids, &[prop_name], ctx.as_ref())
                .await?;
        }

        // 3. Build Arrow Array from results
        let first_val = Self::find_first_non_null_prop(&vids, &props_map, prop_name);
        let array = build_prop_array_from_type(&vids, &props_map, prop_name, first_val)?;

        // 4. Add to batch
        batch.add_column(prop_col_name, array)?;

        Ok(())
    }
}

#[async_trait]
pub trait VectorizedOperator: Send + Sync {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch>;

    fn execute_stream(
        self: Arc<Self>,
        input: Option<VectorizedBatch>,
        ctx: ExecutionContext,
    ) -> BoxStream<'static, Result<VectorizedBatch>>
    where
        Self: 'static,
    {
        let fut = async move { self.execute(input, &ctx).await };
        Box::pin(stream::once(fut))
    }

    /// Returns true if this operator requires the full dataset (pipeline breaker).
    /// Pipeline breakers (Sort, Limit, Aggregate) cannot be executed in parallel morsels.
    fn is_pipeline_breaker(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        "VectorizedOperator".to_string()
    }
}

#[derive(Clone)]
pub struct VectorizedApply {
    pub subquery: PhysicalPlan,
}

#[async_trait]
impl VectorizedOperator for VectorizedApply {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let input = input.ok_or(anyhow!("VectorizedApply requires input"))?;

        if input.num_rows() == 0 {
            // Run subquery with empty input to get schema
            let dummy = self
                .subquery
                .execute_with_input(Some(input.slice(0, 0)), ctx)
                .await?;
            let mut result_batch = dummy;
            broadcast_merge(&mut result_batch, &input.slice(0, 0))?;
            return Ok(result_batch);
        }

        let mut output_batches = Vec::new();

        for i in 0..input.num_rows() {
            if !input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                continue;
            }

            let row_batch = input.slice(i, 1);
            let sub_res = self
                .subquery
                .execute_with_input(Some(row_batch.clone()), ctx)
                .await?;

            if sub_res.num_rows() > 0 {
                let mut result_batch = sub_res;
                broadcast_merge(&mut result_batch, &row_batch)?;
                output_batches.push(result_batch);
            }
        }

        if output_batches.is_empty() {
            // If all subqueries filtered out rows, return empty with correct schema
            let dummy = self
                .subquery
                .execute_with_input(Some(input.slice(0, 0)), ctx)
                .await?;
            let mut result_batch = dummy;
            broadcast_merge(&mut result_batch, &input.slice(0, 0))?;
            return Ok(result_batch);
        }

        VectorizedBatch::concat(&output_batches)
    }

    fn name(&self) -> String {
        "Apply".to_string()
    }
}

#[derive(Clone)]
pub struct VectorizedSingle;

#[async_trait]
impl VectorizedOperator for VectorizedSingle {
    async fn execute(
        &self,
        _input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        // To have 1 row in Arrow, we need at least one column with 1 element.
        let dummy_field = Field::new("_dummy", DataType::Int8, false);
        let schema = Arc::new(ArrowSchema::new(vec![dummy_field]));
        let dummy_col = Arc::new(arrow_array::Int8Array::from(vec![0]));
        let data = RecordBatch::try_new(schema, vec![dummy_col])?;
        Ok(VectorizedBatch::new(data, HashMap::new()))
    }
}

#[derive(Clone)]
pub struct VectorizedRecursiveCTE {
    pub cte_name: String,
    pub initial: PhysicalPlan,
    pub recursive: PhysicalPlan,
}

#[async_trait]
impl VectorizedOperator for VectorizedRecursiveCTE {
    async fn execute(
        &self,
        _input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;

        // 1. Execute Anchor
        let mut working_table = self.initial.execute(ctx).await?;
        let mut result_table = working_table.clone();

        // Deduplication set (using JSON string representation of rows)
        let mut seen = std::collections::HashSet::new();
        for i in 0..working_table.num_rows() {
            let row_json = batch_row_to_json(&working_table, i)?;
            seen.insert(row_json);
        }

        let max_iterations = 1000; // TODO: Config

        for _ in 0..max_iterations {
            if working_table.num_rows() == 0 {
                break;
            }

            // Bind working_table to CTE name
            let working_val = batch_to_value_list(&working_table)?;
            let next_ctx = ctx.with_cte(&self.cte_name, working_val);

            // Execute recursive step
            let next_result = self.recursive.execute(&next_ctx).await?;

            if next_result.num_rows() == 0 {
                break;
            }

            // Filter new rows (dedup against seen)
            let compacted_next = next_result.compact()?;
            let mut keep_indices = Vec::new();

            for i in 0..compacted_next.num_rows() {
                let row_json = batch_row_to_json(&compacted_next, i)?;
                if seen.insert(row_json) {
                    keep_indices.push(i as u64);
                }
            }

            if keep_indices.is_empty() {
                break;
            }

            let indices = UInt64Array::from(keep_indices);
            let mut new_columns = Vec::new();
            for col in compacted_next.data.columns() {
                new_columns.push(arrow::compute::take(col.as_ref(), &indices, None)?);
            }
            let new_data = RecordBatch::try_new(compacted_next.data.schema(), new_columns)?;
            let new_rows_batch = VectorizedBatch::new(new_data, compacted_next.variables.clone());

            // Append to result
            result_table = VectorizedBatch::concat(&[result_table, new_rows_batch.clone()])?;
            working_table = new_rows_batch;
        }

        Ok(result_table)
    }
}

fn batch_row_to_json(batch: &VectorizedBatch, row_idx: usize) -> Result<String> {
    let mut pairs = Vec::new();
    for (name, &col_idx) in &batch.variables {
        let col = batch.data.column(col_idx);
        let val = arrow_val_to_json(col.as_ref(), row_idx);
        pairs.push((name.clone(), val));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(serde_json::to_string(&pairs)?)
}

fn batch_to_value_list(batch: &VectorizedBatch) -> Result<Value> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let mut map = serde_json::Map::new();
        for (name, &col_idx) in &batch.variables {
            let col = batch.data.column(col_idx);
            let val = arrow_val_to_json(col.as_ref(), i);
            map.insert(name.clone(), val);
        }

        if map.len() == 1 {
            rows.push(map.into_iter().next().unwrap().1);
        } else {
            rows.push(Value::Object(map));
        }
    }
    Ok(Value::Array(rows))
}

#[derive(Clone)]
pub struct VectorizedDelete {
    pub items: Vec<Expr>,
    pub detach: bool,
}

impl VectorizedDelete {
    /// Check if a vertex has any incident edges in the given direction.
    async fn has_incident_edges(
        storage: &StorageManager,
        vid: Vid,
        edge_type_ids: &[u16],
        direction: uni_store::runtime::Direction,
        l0: Arc<RwLock<L0Buffer>>,
    ) -> Result<bool> {
        Ok(storage
            .load_subgraph_cached(&[vid], edge_type_ids, 1, direction, Some(l0))
            .await?
            .edges()
            .next()
            .is_some())
    }

    /// Delete all incident edges for a vertex in the given direction.
    async fn delete_incident_edges(
        storage: &StorageManager,
        vid: Vid,
        edge_type_ids: &[u16],
        direction: uni_store::runtime::Direction,
        l0: Arc<RwLock<L0Buffer>>,
        writer: &mut Writer,
    ) -> Result<()> {
        let subgraph = storage
            .load_subgraph_cached(&[vid], edge_type_ids, 1, direction, Some(l0))
            .await?;

        for edge in subgraph.edges() {
            writer
                .delete_edge(edge.eid, edge.src_vid, edge.dst_vid, edge.edge_type)
                .await?;
        }
        Ok(())
    }

    /// Handle vertex deletion (with or without DETACH).
    async fn delete_vertex_item(
        &self,
        vid: Vid,
        edge_type_ids: &[u16],
        storage: &StorageManager,
        writer: &mut Writer,
    ) -> Result<()> {
        let l0 = writer.l0_manager.get_current();

        if !self.detach {
            // Check for existing edges
            let has_out = Self::has_incident_edges(
                storage,
                vid,
                edge_type_ids,
                uni_store::runtime::Direction::Outgoing,
                l0.clone(),
            )
            .await?;

            let has_in = Self::has_incident_edges(
                storage,
                vid,
                edge_type_ids,
                uni_store::runtime::Direction::Incoming,
                l0,
            )
            .await?;

            if has_out || has_in {
                return Err(anyhow!(
                    "Cannot delete node {}, because it still has relationships. To delete the node and its relationships, use DETACH DELETE.",
                    vid
                ));
            }
        } else {
            // Delete all incident edges
            Self::delete_incident_edges(
                storage,
                vid,
                edge_type_ids,
                uni_store::runtime::Direction::Outgoing,
                l0.clone(),
                writer,
            )
            .await?;

            Self::delete_incident_edges(
                storage,
                vid,
                edge_type_ids,
                uni_store::runtime::Direction::Incoming,
                l0,
                writer,
            )
            .await?;
        }

        writer.delete_vertex(vid).await?;
        Ok(())
    }

    async fn delete_edge_item(
        &self,
        eid_val: u64,
        schema: &Schema,
        storage: &StorageManager,
        writer: &mut Writer,
    ) -> Result<()> {
        let eid = Eid::from(eid_val);
        let type_id = eid.type_id();
        let offset = eid.local_offset();

        let (edge_name, _edge_type) = schema
            .edge_types
            .iter()
            .find(|(_, et)| et.id == type_id)
            .ok_or_else(|| anyhow!("Edge type ID {} not found", type_id))?;

        if let Ok(dataset) = storage.get_cached_dataset(edge_name).await {
            // Fetch endpoints from Lance to support deletion
            // Note: This assumes offset maps to row index (which is the design)
            // Lance take() expects u64 indices
            let projection = dataset.schema().project(&["_src", "_dst"])?;
            let batch = dataset.take(&[offset], projection).await?;

            if batch.num_rows() > 0 {
                let src_col = batch
                    .column_by_name("_src")
                    .ok_or_else(|| anyhow!("Missing _src column"))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("_src is not UInt64Array"))?;
                let dst_col = batch
                    .column_by_name("_dst")
                    .ok_or_else(|| anyhow!("Missing _dst column"))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("_dst is not UInt64Array"))?;

                let src = Vid::from(src_col.value(0));
                let dst = Vid::from(dst_col.value(0));

                writer.delete_edge(eid, src, dst, type_id).await?;
            }
        }
        // TODO: Handle L0-only edges (not in Lance yet).
        // If fetch fails from Lance, we should check L0?
        // But L0 delete_edge handles L0/L1 logic.
        // If edge is ONLY in L0, we can't find src/dst from Lance.
        // We need to look up in L0.
        // L0 stores `vertex_edges` (adj list) but not `edges` (eid -> src,dst) map?
        // Actually L0 does not have eid index.
        // But if we have Eid, we can't easily find src/dst in L0 without scanning all L0 edges.
        // This is a known limitation of the current design.
        // For now, we assume edge is in Lance or we can't delete it efficiently if we don't know src/dst.
        // (Unless VectorizedDelete input batch provided src/dst, which it currently doesn't).

        Ok(())
    }
}

#[async_trait]
impl VectorizedOperator for VectorizedDelete {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let mut input = input.ok_or(anyhow!("VectorizedDelete requires input"))?;
        let writer_lock = ctx
            .writer
            .as_ref()
            .ok_or(anyhow!("Write operation requires a Writer"))?;
        let mut writer = writer_lock.write().await;

        let schema = ctx.storage.schema_manager().schema();
        let edge_type_ids: Vec<u16> = schema.edge_types.values().map(|m| m.id).collect();

        for i in 0..input.num_rows() {
            if !input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                continue;
            }

            for expr in &self.items {
                let val = evaluate_scalar(expr, &mut input, i, ctx, &[]).await?;

                if let Some(u) = val.as_u64() {
                    let id_part = (u >> 48) as u16;

                    if schema.labels.values().any(|m| m.id == id_part) {
                        let vid = Vid::from(u);
                        self.delete_vertex_item(vid, &edge_type_ids, &ctx.storage, &mut writer)
                            .await?;
                    } else if schema.edge_types.values().any(|m| m.id == id_part) {
                        self.delete_edge_item(u, &schema, &ctx.storage, &mut writer)
                            .await?;
                    }
                }
            }
        }

        Ok(input)
    }
}

/// Limits the number of rows returned.
#[derive(Clone)]
pub struct VectorizedLimit {
    pub skip: usize,
    pub limit: Option<usize>,
}

#[async_trait]
impl VectorizedOperator for VectorizedLimit {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let input = input.ok_or(anyhow!("VectorizedLimit requires input"))?;

        // We need to act on the *compacted* batch because selection mask
        // makes offset/limit calculation tricky on physical rows vs logical rows.
        // For simplicity and correctness, compact first.
        let compacted = input.compact()?;

        let total_rows = compacted.num_rows();
        let start = std::cmp::min(self.skip, total_rows);
        let len = if let Some(limit) = self.limit {
            std::cmp::min(limit, total_rows - start)
        } else {
            total_rows - start
        };

        if len == 0 {
            return Ok(compacted.slice(0, 0));
        }

        Ok(compacted.slice(start, len))
    }

    fn is_pipeline_breaker(&self) -> bool {
        true
    }
}

#[derive(Clone)]
pub struct VectorizedSort {
    pub sort_entries: Vec<(Expr, bool)>, // Expr, ascending
    pub subqueries: Vec<PhysicalPlan>,
}

#[async_trait]
impl VectorizedOperator for VectorizedSort {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let input = input.ok_or(anyhow!("VectorizedSort requires input"))?;

        // Compact input to ensure we only sort valid rows and avoid issues with selection masks
        let mut input = input.compact()?;

        // 1. Evaluate sort expressions
        let mut sort_columns = Vec::new();
        for (expr, asc) in &self.sort_entries {
            // Evaluate expression for all rows
            let mut values = Vec::with_capacity(input.num_rows());
            for i in 0..input.num_rows() {
                let val = evaluate_scalar(expr, &mut input, i, ctx, &self.subqueries).await?;
                values.push(val);
            }
            let array = json_vec_to_array(&values);

            sort_columns.push(arrow::compute::SortColumn {
                values: array,
                options: Some(arrow::compute::SortOptions {
                    descending: !asc,
                    nulls_first: true,
                }),
            });
        }

        // 2. Sort
        let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)?;

        // 3. Reorder batch
        let mut new_columns = Vec::new();
        let schema = input.data.schema();

        for i in 0..input.data.num_columns() {
            let col = input.data.column(i);
            let new_col = arrow::compute::take(col.as_ref(), &indices, None)?;
            new_columns.push(new_col);
        }

        let new_data = RecordBatch::try_new(schema, new_columns)?;

        Ok(VectorizedBatch::new(new_data, input.variables))
    }

    fn is_pipeline_breaker(&self) -> bool {
        true
    }
}

#[derive(Clone)]
pub struct VectorizedAggregate {
    pub group_by: Vec<Expr>,
    pub aggregates: Vec<Expr>,
    pub subqueries: Vec<PhysicalPlan>,
}

#[async_trait]
impl VectorizedOperator for VectorizedAggregate {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let mut input = input.ok_or(anyhow!("VectorizedAggregate requires input"))?;

        // 1. Grouping
        // Key: Vec<String> (serialized group values), Value: Vec<usize> (row indices)
        // PERF: String-based grouping is O(n * key_size) per row. For true vectorized
        // performance, this should use Arrow's native hash grouping (DictionaryArray
        // or DataFusion's GroupsAccumulator) to operate on columnar data directly.
        let mut groups: HashMap<Vec<String>, Vec<usize>> = HashMap::new();

        // Evaluate group columns once
        let mut group_cols = Vec::new();
        for expr in &self.group_by {
            let mut col_values = Vec::with_capacity(input.num_rows());
            for i in 0..input.num_rows() {
                if input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                    let val = evaluate_scalar(expr, &mut input, i, ctx, &self.subqueries).await?;
                    col_values.push(val);
                } else {
                    col_values.push(Value::Null); // Ignored later
                }
            }
            group_cols.push(col_values);
        }

        for i in 0..input.num_rows() {
            if !input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                continue;
            }

            let mut key = Vec::with_capacity(self.group_by.len());
            for col in &group_cols {
                // Simple string serialization for grouping key
                // Note: This conflates 1 and "1", but acceptable for MVP
                match &col[i] {
                    Value::String(s) => key.push(s.clone()),
                    v => key.push(v.to_string()),
                }
            }
            groups.entry(key).or_default().push(i);
        }

        // 2. Aggregation
        let mut results: HashMap<String, Vec<Value>> = HashMap::new();

        // Initialize output vectors
        for expr in &self.group_by {
            results.insert(expr.to_string_repr(), Vec::new());
        }
        for expr in &self.aggregates {
            results.insert(expr.to_string_repr(), Vec::new());
        }

        // Deterministic iteration order
        let mut sorted_keys: Vec<_> = groups.keys().cloned().collect();
        sorted_keys.sort();

        for key in sorted_keys {
            let indices = &groups[&key];
            let first_idx = indices[0];

            // Fill group keys
            for (i, expr) in self.group_by.iter().enumerate() {
                results
                    .get_mut(&expr.to_string_repr())
                    .unwrap()
                    .push(group_cols[i][first_idx].clone());
            }

            // Compute aggregates
            for expr in &self.aggregates {
                if let Expr::FunctionCall {
                    name,
                    args,
                    distinct,
                } = expr
                {
                    let val = compute_aggregate(
                        name,
                        args,
                        *distinct,
                        indices,
                        &mut input,
                        ctx,
                        &self.subqueries,
                    )
                    .await?;
                    results.get_mut(&expr.to_string_repr()).unwrap().push(val);
                } else {
                    return Err(anyhow!("Aggregate expression must be a function call"));
                }
            }
        }

        // 3. Build Batch
        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();
        let mut new_vars = HashMap::new();

        // Add group columns first
        for expr in &self.group_by {
            let name = expr.to_string_repr();
            let values = results.get(&name).unwrap();
            let array = json_vec_to_array(values);
            new_columns.push(array.clone());
            new_fields.push(Field::new(&name, array.data_type().clone(), true));
            new_vars.insert(name, new_columns.len() - 1);
        }

        // Add aggregate columns
        for expr in &self.aggregates {
            let name = expr.to_string_repr();
            let values = results.get(&name).unwrap();
            let array = json_vec_to_array(values);
            new_columns.push(array.clone());
            new_fields.push(Field::new(&name, array.data_type().clone(), true));
            new_vars.insert(name, new_columns.len() - 1);
        }

        let schema = Arc::new(ArrowSchema::new(new_fields));
        let data = RecordBatch::try_new(schema, new_columns)?;

        Ok(VectorizedBatch::new(data, new_vars))
    }

    fn is_pipeline_breaker(&self) -> bool {
        true
    }
}

/// Helper to compute aggregate value for a group.
/// Delegates to aggregate functions in the `aggregates` module after collecting values.
async fn compute_aggregate(
    name: &str,
    args: &[Expr],
    distinct: bool,
    indices: &[usize],
    batch: &mut VectorizedBatch,
    ctx: &ExecutionContext,
    subqueries: &[PhysicalPlan],
) -> Result<Value> {
    use crate::query::vectorized::aggregates::{
        avg_aggregate, collect_aggregate, count_aggregate, max_aggregate, min_aggregate,
        sum_aggregate,
    };

    let upper_name = name.to_uppercase();
    let is_count_star =
        upper_name == "COUNT" && (args.is_empty() || matches!(args[0], Expr::Wildcard));

    // For COUNT(*), we just count the rows without evaluating expressions
    if is_count_star {
        return Ok(json!(indices.len()));
    }

    // Collect evaluated values for the group
    let mut values = Vec::with_capacity(indices.len());
    for &idx in indices {
        let val = evaluate_scalar(&args[0], batch, idx, ctx, subqueries).await?;
        values.push(val);
    }

    // Handle DISTINCT
    if distinct && !matches!(upper_name.as_str(), "MIN" | "MAX") {
        let mut seen = std::collections::HashSet::new();
        let mut unique_values = Vec::new();
        for val in values {
            // Simple string serialization for deduplication key
            let key = match &val {
                Value::String(s) => s.clone(),
                v => v.to_string(),
            };
            if seen.insert(key) {
                unique_values.push(val);
            }
        }
        values = unique_values;
    }

    // Delegate to aggregate functions
    match upper_name.as_str() {
        "COUNT" => Ok(count_aggregate(&values, false)),
        "SUM" => Ok(sum_aggregate(&values)),
        "AVG" => Ok(avg_aggregate(&values)),
        "MIN" => Ok(min_aggregate(&values)),
        "MAX" => Ok(max_aggregate(&values)),
        "COLLECT" => Ok(collect_aggregate(&values)),
        _ => Err(anyhow!("Unsupported aggregate function: {}", name)),
    }
}

/// Scans vertices of a specific label.
#[derive(Clone)]
pub struct VectorizedScan {
    pub label_id: u16,
    pub variable: String,
    pub pushed_filter: Option<String>,
    pub pushed_predicates: Vec<Expr>,
    pub residual_filter: Option<Expr>,
    pub subqueries: Vec<PhysicalPlan>,
    pub project_columns: Vec<String>,
    pub optional: bool,
    /// UID lookup: direct index lookup for `_uid = 'xxx'` predicates.
    /// When set, bypasses Lance scan and uses UidIndex for O(1) lookup.
    pub uid_lookup: Option<uni_common::core::id::UniId>,
    /// JsonPath index lookups: (path, value) pairs for indexed properties.
    /// When set, uses JsonPathIndex to get candidate VIDs before Lance scan.
    pub jsonpath_lookups: Vec<(String, String)>,
    /// JSON FTS predicates: (column, query, optional_path) for full-text search.
    /// When set, uses Lance inverted index for BM25-based text search.
    pub json_fts_predicates: Vec<(String, String, Option<String>)>,
}

#[async_trait]
impl VectorizedOperator for VectorizedScan {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let mut stream = Arc::new(self.clone()).execute_stream(input, ctx.clone());
        let mut batches = Vec::new();
        while let Some(batch_res) = stream.next().await {
            batches.push(batch_res?);
        }
        if batches.is_empty() {
            // If empty and NOT optional, return empty batch with correct schema.
            // If optional, execute_stream should have handled it?
            // Actually execute_stream handles optional logic now.
            // But if execute_stream returns empty (e.g. error filtered out?), we need fallback schema.
            // For now, construct empty batch with minimal schema.
            return Ok(VectorizedBatch::new(
                RecordBatch::new_empty(Arc::new(ArrowSchema::new(Vec::<Field>::new()))),
                HashMap::new(),
            ));
        }
        VectorizedBatch::concat(&batches)
    }

    fn execute_stream(
        self: Arc<Self>,
        input: Option<VectorizedBatch>,
        ctx: ExecutionContext,
    ) -> BoxStream<'static, Result<VectorizedBatch>> {
        let this = self;

        Box::pin(
            stream::once(async move {
                let schema = ctx.storage.schema_manager().schema();
                let label_name = schema
                    .label_name_by_id(this.label_id)
                    .ok_or_else(|| anyhow!("Label ID {} not found", this.label_id))?;

                let mut latest_states: HashMap<u64, (u64, bool, Vec<Value>)> = HashMap::new();

                // === Index-based VID collection ===
                // When index lookups are specified, collect candidate VIDs before Lance scan.
                // This can dramatically reduce scan scope for selective predicates.
                let mut index_candidate_vids: Option<std::collections::HashSet<u64>> = None;

                // 1. UID Index Lookup (most selective - single VID result)
                if let Some(uid) = &this.uid_lookup {
                    let uid_index = ctx.storage.uid_index(label_name)?;
                    match uid_index.get_vid(uid).await? {
                        Some(vid) => {
                            index_candidate_vids =
                                Some(std::collections::HashSet::from([vid.as_u64()]));
                        }
                        None => {
                            // UID not found - return empty result immediately
                            index_candidate_vids = Some(std::collections::HashSet::new());
                        }
                    }
                }

                // 2. JsonPath Index Lookups (intersect results for multiple)
                for (path, value) in &this.jsonpath_lookups {
                    let json_index = ctx.storage.json_index(label_name, &format!("$.{}", path))?;
                    let vids = json_index.get_vids(value).await?;
                    let vid_set: std::collections::HashSet<u64> =
                        vids.iter().map(|v| v.as_u64()).collect();

                    index_candidate_vids = match index_candidate_vids {
                        Some(existing) => Some(existing.intersection(&vid_set).cloned().collect()),
                        None => Some(vid_set),
                    };

                    // Early exit if intersection is empty
                    if index_candidate_vids
                        .as_ref()
                        .map(|s| s.is_empty())
                        .unwrap_or(false)
                    {
                        break;
                    }
                }

                // 3. JSON FTS Lookups (intersect results for multiple)
                for (column, query, path_filter) in &this.json_fts_predicates {
                    // Execute FTS query using Lance full-text search
                    let vid_set = if let Ok(lance_ds) =
                        ctx.storage.get_cached_dataset(label_name).await
                    {
                        // Build the FTS query string
                        // If path_filter is set, use path:term syntax
                        let fts_query = match path_filter {
                            Some(path) => {
                                // Convert $.title to just "title"
                                let path_key = path.trim_start_matches("$.");
                                format!("{}:{}", path_key, query)
                            }
                            None => query.clone(),
                        };

                        // Create scanner with full-text search
                        let mut scanner = lance_ds.scan();
                        if let Err(e) = scanner.full_text_search(
                            FullTextSearchQuery::new(fts_query).columns(Some(vec![column.clone()])),
                        ) {
                            log::warn!("FTS query failed: {}", e);
                            std::collections::HashSet::new()
                        } else {
                            // Only project _vid
                            if let Err(e) = scanner.project(&["_vid"]) {
                                log::warn!("FTS project failed: {}", e);
                                std::collections::HashSet::new()
                            } else {
                                match scanner.try_into_stream().await {
                                    Ok(mut stream) => {
                                        let mut result_vids = std::collections::HashSet::new();
                                        while let Some(batch_res) = stream.next().await {
                                            if let Ok(batch) = batch_res
                                                && let Some(vid_col) = batch.column_by_name("_vid")
                                                && let Some(u64_arr) =
                                                    vid_col.as_any().downcast_ref::<UInt64Array>()
                                            {
                                                for i in 0..u64_arr.len() {
                                                    result_vids.insert(u64_arr.value(i));
                                                }
                                            }
                                        }
                                        result_vids
                                    }
                                    Err(e) => {
                                        log::warn!("FTS stream creation failed: {}", e);
                                        std::collections::HashSet::new()
                                    }
                                }
                            }
                        }
                    } else {
                        std::collections::HashSet::new()
                    };

                    index_candidate_vids = match index_candidate_vids {
                        Some(existing) => Some(existing.intersection(&vid_set).cloned().collect()),
                        None => Some(vid_set),
                    };

                    // Early exit if intersection is empty
                    if index_candidate_vids
                        .as_ref()
                        .map(|s| s.is_empty())
                        .unwrap_or(false)
                    {
                        break;
                    }
                }

                // Determine if we should skip Lance scan (index returned empty set)
                let skip_lance_scan = index_candidate_vids
                    .as_ref()
                    .map(|vids| vids.is_empty())
                    .unwrap_or(false);

                if !skip_lance_scan
                    && let Ok(lance_ds) = ctx.storage.get_cached_dataset(label_name).await
                {
                    // Build the effective filter combining index VIDs and pushed_filter
                    let effective_filter: Option<String> =
                        match (&index_candidate_vids, &this.pushed_filter) {
                            (Some(vids), Some(pf)) if !vids.is_empty() => {
                                // Combine index VID filter with pushed filter
                                let vid_list: Vec<String> =
                                    vids.iter().map(|v| v.to_string()).collect();
                                let vid_filter = format!("_vid IN ({})", vid_list.join(", "));
                                Some(format!("({}) AND ({})", vid_filter, pf))
                            }
                            (Some(vids), None) if !vids.is_empty() => {
                                // Only index VID filter
                                let vid_list: Vec<String> =
                                    vids.iter().map(|v| v.to_string()).collect();
                                Some(format!("_vid IN ({})", vid_list.join(", ")))
                            }
                            (None, Some(pf)) => Some(pf.clone()),
                            _ => None,
                        };

                    if let Some(filter) = &effective_filter {
                        // Two-phase scan: first get VIDs, then full rows
                        let mut phase1_scanner = lance_ds.scan();
                        phase1_scanner.filter(filter)?;
                        phase1_scanner.project(&["_vid"])?;
                        let mut phase1_stream = phase1_scanner.try_into_stream().await?;
                        let mut candidate_vids = std::collections::HashSet::new();
                        while let Some(batch) = phase1_stream.next().await {
                            let batch = batch?;
                            let vid_col = batch
                                .column_by_name("_vid")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap();
                            for i in 0..batch.num_rows() {
                                candidate_vids.insert(vid_col.value(i));
                            }
                        }
                        if !candidate_vids.is_empty() {
                            let vid_list: Vec<String> =
                                candidate_vids.iter().map(|v| v.to_string()).collect();
                            let vid_in_filter = format!("_vid IN ({})", vid_list.join(", "));
                            let mut phase2_scanner = lance_ds.scan();
                            phase2_scanner.filter(&vid_in_filter)?;
                            let mut columns = vec!["_vid", "_version", "_deleted"];
                            for col in &this.project_columns {
                                columns.push(col.as_str());
                            }
                            phase2_scanner.project(&columns)?;
                            let mut phase2_stream = phase2_scanner.try_into_stream().await?;
                            while let Some(batch) = phase2_stream.next().await {
                                let batch = batch?;
                                this.process_batch(&batch, &mut latest_states)?;
                                let estimated_memory = latest_states.len() * 200;
                                if estimated_memory > ctx.config.max_query_memory {
                                    return Err(anyhow!("Query exceeded memory limit during scan"));
                                }
                            }
                        }
                    } else {
                        // No filter - full table scan
                        let mut scanner = lance_ds.scan();
                        let mut columns = vec!["_vid", "_version", "_deleted"];
                        for col in &this.project_columns {
                            columns.push(col.as_str());
                        }
                        scanner.project(&columns)?;
                        let mut stream = scanner.try_into_stream().await?;
                        while let Some(batch) = stream.next().await {
                            let batch = batch?;
                            this.process_batch(&batch, &mut latest_states)?;
                            let estimated_memory = latest_states.len() * 200;
                            if estimated_memory > ctx.config.max_query_memory {
                                return Err(anyhow!("Query exceeded memory limit during scan"));
                            }
                        }
                    }
                }

                if let Some(l0_lock) = &ctx.l0 {
                    this.overlay_l0(l0_lock, &mut latest_states, 50);
                }
                for (i, l0_lock) in ctx.pending_flush_l0s.iter().enumerate() {
                    this.overlay_l0(l0_lock, &mut latest_states, 1 + i as u64);
                }
                if let Some(tx_l0_lock) = &ctx.transaction_l0 {
                    this.overlay_l0(tx_l0_lock, &mut latest_states, 100);
                }

                let mut final_vids = Vec::new();
                let mut final_projected: HashMap<String, Vec<Value>> = HashMap::new();
                for col in &this.project_columns {
                    final_projected.insert(col.clone(), Vec::new());
                }

                for (vid, (_ver, deleted, vals)) in &latest_states {
                    if !*deleted {
                        final_vids.push(*vid);
                        for (i, col_name) in this.project_columns.iter().enumerate() {
                            final_projected
                                .get_mut(col_name)
                                .unwrap()
                                .push(vals[i].clone());
                        }
                    }
                }

                let mut batches = Vec::new();

                let batch_size = ctx.config.batch_size;
                let mut offset = 0;
                while offset < final_vids.len() {
                    let length = std::cmp::min(batch_size, final_vids.len() - offset);
                    let slice_vids = &final_vids[offset..offset + length];

                    let vid_array = UInt64Array::from(slice_vids.to_vec());
                    let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(vid_array)];
                    let mut fields =
                        vec![Field::new(this.variable.clone(), DataType::UInt64, false)];
                    let mut variables = HashMap::new();
                    variables.insert(this.variable.clone(), 0);

                    for col_name in &this.project_columns {
                        let values =
                            &final_projected.get(col_name).unwrap()[offset..offset + length];
                        let array = json_vec_to_array(values);
                        columns.push(array.clone());
                        let field_name = format!("{}.{}", this.variable, col_name);
                        fields.push(Field::new(
                            field_name.clone(),
                            array.data_type().clone(),
                            true,
                        ));
                        variables.insert(field_name, columns.len() - 1);
                    }

                    let arrow_schema = Arc::new(ArrowSchema::new(fields));
                    let mut batch = VectorizedBatch::new(
                        RecordBatch::try_new(arrow_schema, columns)?,
                        variables,
                    );

                    if let Some(in_batch) = &input {
                        broadcast_merge(&mut batch, in_batch)?;
                    }

                    ctx.observe_memory(&batch)?;
                    batches.push(batch);
                    offset += length;
                }

                // Apply Residual Filter
                if let Some(pred) = &this.residual_filter {
                    let mut filtered_batches = Vec::new();
                    for mut batch in batches {
                        let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());
                        for i in 0..batch.num_rows() {
                            let pass = if !batch
                                .selection
                                .as_ref()
                                .map(|s| s.value(i))
                                .unwrap_or(true)
                            {
                                false
                            } else {
                                let val =
                                    evaluate_scalar(pred, &mut batch, i, &ctx, &this.subqueries)
                                        .await?;
                                val.as_bool().unwrap_or(false)
                            };
                            mask_builder.append_value(pass);
                        }
                        batch.selection = Some(mask_builder.finish());
                        if batch.active_rows() > 0 {
                            filtered_batches.push(batch);
                        }
                    }
                    batches = filtered_batches;
                }

                // Handle Optional Match if no results (either initially empty or filtered out)
                if batches.is_empty() && this.optional {
                    let vid_array = UInt64Array::from(vec![None::<u64>]);
                    let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(vid_array)];
                    let mut fields =
                        vec![Field::new(this.variable.clone(), DataType::UInt64, true)];
                    let mut variables = HashMap::new();
                    variables.insert(this.variable.clone(), 0);

                    for col_name in &this.project_columns {
                        let arrow_type = if let Some(props) = schema.properties.get(label_name) {
                            if let Some(meta) = props.get(col_name) {
                                meta.r#type.to_arrow()
                            } else {
                                DataType::Null
                            }
                        } else {
                            DataType::Null
                        };

                        let array = arrow_array::new_null_array(&arrow_type, 1);
                        columns.push(Arc::new(array));
                        let field_name = format!("{}.{}", this.variable, col_name);
                        fields.push(Field::new(field_name.clone(), arrow_type, true));
                        variables.insert(field_name, columns.len() - 1);
                    }

                    let arrow_schema = Arc::new(ArrowSchema::new(fields));
                    let mut batch = VectorizedBatch::new(
                        RecordBatch::try_new(arrow_schema, columns)?,
                        variables,
                    );

                    if let Some(in_batch) = &input {
                        broadcast_merge(&mut batch, in_batch)?;
                    }
                    batches.push(batch);
                }

                Ok(batches)
            })
            .flat_map(|res: Result<Vec<VectorizedBatch>>| match res {
                Ok(batches) => stream::iter(batches).map(Ok).boxed(),
                Err(e) => stream::once(async { Err(e) }).boxed(),
            }),
        )
    }

    fn name(&self) -> String {
        format!("Scan({})", self.variable)
    }
}

impl VectorizedScan {
    /// Process a batch from Lance and update latest_states with MVCC resolution
    fn process_batch(
        &self,
        batch: &RecordBatch,
        latest_states: &mut HashMap<u64, (u64, bool, Vec<Value>)>,
    ) -> Result<()> {
        let vid_col = batch
            .column_by_name("_vid")
            .ok_or_else(|| anyhow!("Missing _vid column"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("_vid is not UInt64Array"))?;
        let ver_col = batch
            .column_by_name("_version")
            .ok_or_else(|| anyhow!("Missing _version column"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("_version is not UInt64Array"))?;
        let del_col = batch
            .column_by_name("_deleted")
            .ok_or_else(|| anyhow!("Missing _deleted column"))?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow!("_deleted is not BooleanArray"))?;

        for i in 0..batch.num_rows() {
            let vid = vid_col.value(i);
            let version = ver_col.value(i);
            let deleted = del_col.value(i);

            let entry = latest_states.entry(vid).or_insert((0, true, vec![]));
            if version >= entry.0 {
                let mut vals = Vec::with_capacity(self.project_columns.len());
                for col_name in &self.project_columns {
                    let val = if let Some(col) = batch.column_by_name(col_name) {
                        arrow_val_to_json(col.as_ref(), i)
                    } else {
                        Value::Null
                    };
                    vals.push(val);
                }
                *entry = (version, deleted, vals.clone());
            }
        }

        Ok(())
    }

    fn overlay_l0(
        &self,
        l0_lock: &RwLock<L0Buffer>,
        latest_states: &mut HashMap<u64, (u64, bool, Vec<Value>)>,
        priority: u64,
    ) {
        let l0 = l0_lock.read();

        // Use a high base version for L0, but combine with actual mutation version for consistency

        let base_version = 1u64 << 60;

        // 1. Handle tombstones

        for &vid in &l0.vertex_tombstones {
            if vid.label_id() == self.label_id {
                let vid_u64 = vid.as_u64();

                let version = l0.vertex_versions.get(&vid).copied().unwrap_or(0);

                let overlay_version = base_version + priority + version;

                let entry = latest_states.entry(vid_u64).or_insert((0, true, vec![]));

                if overlay_version >= entry.0 {
                    *entry = (
                        overlay_version,
                        true,
                        vec![Value::Null; self.project_columns.len()],
                    );
                }
            }
        }

        // 2. Handle properties (updates/inserts)

        for (vid, props) in &l0.vertex_properties {
            if vid.label_id() == self.label_id {
                let vid_u64 = vid.as_u64();

                let version = l0.vertex_versions.get(vid).copied().unwrap_or(0);

                let overlay_version = base_version + priority + version;

                // Check predicates for L0 data

                let mut pass = true;

                for pred in &self.pushed_predicates {
                    if !evaluate_l0(pred, props, &self.variable)
                        .as_bool()
                        .unwrap_or(false)
                    {
                        pass = false;

                        break;
                    }
                }

                if pass {
                    let mut vals = Vec::with_capacity(self.project_columns.len());

                    for col_name in &self.project_columns {
                        let val = props.get(col_name).cloned().unwrap_or(Value::Null);

                        vals.push(val);
                    }

                    let entry = latest_states.entry(vid_u64).or_insert((0, true, vec![]));

                    if overlay_version >= entry.0 {
                        *entry = (overlay_version, false, vals);
                    }
                } else {
                    // It matched Lance but doesn't match L0 anymore - must be removed/hidden
                    let entry = latest_states.entry(vid_u64).or_insert((0, true, vec![]));

                    if overlay_version >= entry.0 {
                        // Mark as deleted in latest_states so it's filtered out later
                        *entry = (
                            overlay_version,
                            true,
                            vec![Value::Null; self.project_columns.len()],
                        );
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct VectorizedShortestPath {
    pub edge_type_ids: Vec<u16>,
    pub direction: Direction,
    pub source_variable: String,
    pub target_variable: String,
    pub path_variable: String,
    pub target_label_id: u16,
    /// Minimum number of hops (edges) in the path. Default is 1.
    pub min_hops: u32,
    /// Maximum number of hops (edges) in the path. Default is u32::MAX (unlimited).
    pub max_hops: u32,
}

#[async_trait]
impl VectorizedOperator for VectorizedShortestPath {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let input = input.ok_or(anyhow!("VectorizedShortestPath requires input"))?;
        let src_col = input.column(&self.source_variable)?;
        let src_vids = src_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Source variable must be UInt64 (Vid)"))?;

        let dst_col = input.column(&self.target_variable)?;
        let dst_vids = dst_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Target variable must be UInt64 (Vid)"))?;

        let l0_arc = ctx.l0.clone();
        let tx_l0_arc = ctx.transaction_l0.clone();

        let cache = ctx.storage.adjacency_cache();

        // 1. Warm Cache
        let mut source_labels = std::collections::HashSet::new();
        let mut target_labels = std::collections::HashSet::new();
        for i in 0..src_vids.len() {
            if input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                if !src_vids.is_null(i) {
                    source_labels.insert(Vid::from(src_vids.value(i)).label_id());
                }
                if !dst_vids.is_null(i) {
                    target_labels.insert(Vid::from(dst_vids.value(i)).label_id());
                }
            }
        }

        for &edge_type_id in &self.edge_type_ids {
            let edge_ver = ctx.storage.get_edge_version_by_id(edge_type_id);
            for label_id in &source_labels {
                cache
                    .warm(
                        &ctx.storage,
                        edge_type_id,
                        Direction::Outgoing,
                        *label_id,
                        edge_ver,
                    )
                    .await?;
            }
            for label_id in &target_labels {
                cache
                    .warm(
                        &ctx.storage,
                        edge_type_id,
                        Direction::Incoming,
                        *label_id,
                        edge_ver,
                    )
                    .await?;
            }
        }

        let mut paths = Vec::new();
        let mut row_indices = Vec::new();

        for i in 0..src_vids.len() {
            if !input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                continue;
            }
            if src_vids.is_null(i) || dst_vids.is_null(i) {
                continue;
            }

            let src_vid = Vid::from(src_vids.value(i));
            let dst_vid = Vid::from(dst_vids.value(i));

            let l0_guard = l0_arc.as_ref().map(|l| l.read());
            let tx_l0_guard = tx_l0_arc.as_ref().map(|l| l.read());

            // Use DirectTraversal for bidirectional BFS
            let adj_cache = ctx.storage.adjacency_cache();
            let traversal = uni_algo::algo::DirectTraversal::new(
                &adj_cache,
                tx_l0_guard.as_deref().or(l0_guard.as_deref()), // Simplification: use tx if exists, else main
                self.edge_type_ids.clone(),
            );

            if let Some(path) = traversal.shortest_path_with_hops(
                src_vid,
                dst_vid,
                self.direction,
                self.min_hops,
                self.max_hops,
            ) {
                paths.push(path);
                row_indices.push(i as u64);
            }
        }

        // Build output batch
        use arrow::compute::take;
        let indices_array = UInt64Array::from(row_indices);

        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();
        let mut new_vars = HashMap::new();

        for (name, &col_idx) in &input.variables {
            let col = input.data.column(col_idx);
            let new_col = take(col.as_ref(), &indices_array, None)?;
            new_columns.push(new_col);
            new_fields.push(Field::new(name, col.data_type().clone(), true));
            new_vars.insert(name.clone(), new_columns.len() - 1);
        }

        // Add path variable column
        let mut rels_builder = ListBuilder::new(UInt64Builder::new());
        let mut nodes_builder = ListBuilder::new(UInt64Builder::new());

        for path in &paths {
            for &eid in &path.edges {
                rels_builder.values().append_value(eid.as_u64());
            }
            rels_builder.append(true);

            for &vid in &path.vertices {
                nodes_builder.values().append_value(vid.as_u64());
            }
            nodes_builder.append(true);
        }

        let rels_array = Arc::new(rels_builder.finish());
        let nodes_array = Arc::new(nodes_builder.finish());

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "relationships",
                    rels_array.data_type().clone(),
                    false,
                )),
                rels_array as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("nodes", nodes_array.data_type().clone(), false)),
                nodes_array as Arc<dyn Array>,
            ),
        ]);
        let struct_array = Arc::new(struct_array);

        new_columns.push(struct_array.clone());
        new_fields.push(Field::new(
            self.path_variable.clone(),
            struct_array.data_type().clone(),
            false,
        ));

        new_vars.insert(self.path_variable.clone(), new_columns.len() - 1);

        let schema = Arc::new(ArrowSchema::new(new_fields));
        let batch = RecordBatch::try_new(schema, new_columns)?;

        Ok(VectorizedBatch::new(batch, new_vars))
    }
}

#[derive(Clone)]
pub struct VectorizedKnn {
    pub label_id: u16,
    pub variable: String,
    pub property: String,
    pub query: Expr,
    pub k: usize,
    pub threshold: Option<f32>,
}

#[async_trait]
impl VectorizedOperator for VectorizedKnn {
    async fn execute(
        &self,
        _input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;

        // Evaluate query vector
        // We use dummy batch/row since query must be constant (literal or param)
        // If it depends on row variables, Knn logic needs to be different (per-row Knn? Not supported by Lance efficiently yet)
        let mut dummy_batch = VectorizedBatch::new(
            RecordBatch::new_empty(Arc::new(ArrowSchema::new(Vec::<Field>::new()))),
            HashMap::new(),
        );
        let query_val = evaluate_scalar(&self.query, &mut dummy_batch, 0, ctx, &[]).await?;

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

        let schema = ctx.storage.schema_manager().schema();
        let label_name = schema
            .label_name_by_id(self.label_id)
            .ok_or_else(|| anyhow!("Label ID {} not found", self.label_id))?;

        let results = ctx
            .storage
            .vector_search(label_name, &self.property, &query_vector, self.k)
            .await?;

        let mut vids = Vec::new();
        for (vid, dist) in results {
            if let Some(thresh) = self.threshold {
                // Assuming Cosine Similarity where sim = 1.0 - dist
                // query: similarity > thresh
                // (1 - dist) > thresh => dist < 1 - thresh
                // NOTE: This assumes default Lance metric matches what's expected.
                // Ideally we check index metric.
                let sim = 1.0 - dist;
                if sim < thresh {
                    continue;
                }
            }
            vids.push(vid.as_u64());
        }

        let vid_array = UInt64Array::from(vids);
        let columns: Vec<Arc<dyn Array>> = vec![Arc::new(vid_array)];
        let fields = vec![Field::new(self.variable.clone(), DataType::UInt64, false)];
        let mut variables = HashMap::new();
        variables.insert(self.variable.clone(), 0);

        let arrow_schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(arrow_schema, columns)?;
        let batch = VectorizedBatch::new(batch, variables);
        ctx.observe_memory(&batch)?;

        Ok(batch)
    }
}

// ============================================================================
// Arrow to JSON conversion helpers
// ============================================================================

/// Convert a JSON value to a string key for use in JSON objects.
fn value_to_json_key(val: Value) -> String {
    match val {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => val.to_string(),
    }
}

/// Convert a struct array (from MapArray or List<Struct>) to a JSON object.
fn struct_entries_to_json(keys: &dyn Array, values: &dyn Array, len: usize) -> Value {
    let mut map = serde_json::Map::new();
    for i in 0..len {
        let key_val = arrow_val_to_json(keys, i);
        let val_val = arrow_val_to_json(values, i);
        map.insert(value_to_json_key(key_val), val_val);
    }
    Value::Object(map)
}

/// Try to convert a primitive Arrow array value to JSON.
fn try_arrow_primitive_to_json(col: &dyn Array, row: usize) -> Option<Value> {
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return Some(Value::String(s.value(row).to_string()));
    }
    if let Some(u) = col.as_any().downcast_ref::<UInt64Array>() {
        return Some(json!(u.value(row)));
    }
    if let Some(i) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return Some(json!(i.value(row)));
    }
    if let Some(i) = col.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return Some(json!(i.value(row)));
    }
    if let Some(i) = col.as_any().downcast_ref::<arrow_array::Int16Array>() {
        return Some(json!(i.value(row)));
    }
    if let Some(i) = col.as_any().downcast_ref::<arrow_array::Int8Array>() {
        return Some(json!(i.value(row)));
    }
    if let Some(u) = col.as_any().downcast_ref::<arrow_array::UInt32Array>() {
        return Some(json!(u.value(row)));
    }
    if let Some(u) = col.as_any().downcast_ref::<arrow_array::UInt16Array>() {
        return Some(json!(u.value(row)));
    }
    if let Some(u) = col.as_any().downcast_ref::<arrow_array::UInt8Array>() {
        return Some(json!(u.value(row)));
    }
    if let Some(f) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(json!(f.value(row)));
    }
    if let Some(f) = col.as_any().downcast_ref::<arrow_array::Float32Array>() {
        return Some(json!(f.value(row)));
    }
    if let Some(b) = col.as_any().downcast_ref::<BooleanArray>() {
        return Some(Value::Bool(b.value(row)));
    }
    None
}

/// Convert an Arrow array to a JSON array.
fn arrow_array_to_json_array(arr: &dyn Array) -> Value {
    let mut vals = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        vals.push(arrow_val_to_json(arr, i));
    }
    Value::Array(vals)
}

/// Check if a StructArray represents a map (has key/value fields).
fn is_map_struct(struct_arr: &StructArray) -> bool {
    let fields = struct_arr.fields();
    fields.len() == 2
        && fields.iter().any(|f| f.name() == "key")
        && fields.iter().any(|f| f.name() == "value")
}

/// Convert a ListArray value to JSON (handles both regular lists and map-as-list).
fn list_array_to_json(list: &ListArray, row: usize) -> Value {
    let arr = list.value(row);
    if let Some(struct_arr) = arr.as_any().downcast_ref::<StructArray>()
        && is_map_struct(struct_arr)
    {
        let keys = struct_arr.column_by_name("key").unwrap();
        let values = struct_arr.column_by_name("value").unwrap();
        return struct_entries_to_json(keys.as_ref(), values.as_ref(), struct_arr.len());
    }
    arrow_array_to_json_array(arr.as_ref())
}

/// Convert a MapArray value to JSON.
fn map_array_to_json(map_arr: &MapArray, row: usize) -> Value {
    if map_arr.is_null(row) {
        return Value::Null;
    }
    let struct_arr = map_arr.value(row);
    let keys = struct_arr.column(0);
    let values = struct_arr.column(1);
    struct_entries_to_json(keys.as_ref(), values.as_ref(), struct_arr.len())
}

/// Convert a StructArray value to JSON.
fn struct_array_to_json(s: &StructArray, row: usize) -> Value {
    let mut map = serde_json::Map::new();
    for (field, child) in s.fields().iter().zip(s.columns()) {
        map.insert(field.name().clone(), arrow_val_to_json(child.as_ref(), row));
    }
    Value::Object(map)
}

fn arrow_val_to_json(col: &dyn Array, row: usize) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }

    // Try primitive types first (most common)
    if let Some(val) = try_arrow_primitive_to_json(col, row) {
        return val;
    }

    // Map types
    if let Some(map_arr) = col.as_any().downcast_ref::<MapArray>() {
        return map_array_to_json(map_arr, row);
    }

    // List types
    if let Some(list) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        return arrow_array_to_json_array(list.value(row).as_ref());
    }
    if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
        return list_array_to_json(list, row);
    }

    // Struct type
    if let Some(s) = col.as_any().downcast_ref::<StructArray>() {
        return struct_array_to_json(s, row);
    }

    // Log unknown type for debugging
    log::warn!(
        "arrow_val_to_json: unsupported array type {:?}",
        col.data_type()
    );
    Value::Null
}

// ============================================================================
// JSON to Arrow conversion helpers
// ============================================================================

/// Convert mixed-type values to a string array (type promotion).
fn json_vec_to_string_promoted(values: &[Value]) -> Arc<dyn Array> {
    let data: Vec<Option<String>> = values
        .iter()
        .map(|v| match v {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => Some(v.to_string()),
        })
        .collect();
    Arc::new(StringArray::from(data))
}

/// Convert JSON number values to the appropriate Arrow array based on first value type.
fn json_numbers_to_array(values: &[Value], first_num: &serde_json::Number) -> Arc<dyn Array> {
    if first_num.is_i64() {
        let data: Vec<Option<i64>> = values.iter().map(|v| v.as_i64()).collect();
        Arc::new(Int64Array::from(data))
    } else if first_num.is_u64() {
        let data: Vec<Option<u64>> = values.iter().map(|v| v.as_u64()).collect();
        Arc::new(UInt64Array::from(data))
    } else {
        let data: Vec<Option<f64>> = values.iter().map(|v| v.as_f64()).collect();
        Arc::new(Float64Array::from(data))
    }
}

/// Build a list array of u64 values from JSON arrays.
fn json_arrays_to_uint64_list(values: &[Value]) -> Arc<dyn Array> {
    let mut builder = ListBuilder::new(UInt64Builder::new());
    for v in values {
        if let Value::Array(list) = v {
            for item in list {
                if let Some(n) = item.as_u64() {
                    builder.values().append_value(n);
                } else {
                    builder.values().append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append(false);
        }
    }
    Arc::new(builder.finish())
}

/// Build a list array of f64 values from JSON arrays.
fn json_arrays_to_float64_list(values: &[Value]) -> Arc<dyn Array> {
    let mut builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::Float64Builder::new());
    for v in values {
        if let Value::Array(list) = v {
            for item in list {
                if let Some(f) = item.as_f64() {
                    builder.values().append_value(f);
                } else {
                    builder.values().append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append(false);
        }
    }
    Arc::new(builder.finish())
}

/// Build a list array of string values from JSON arrays.
fn json_arrays_to_string_list(values: &[Value]) -> Arc<dyn Array> {
    let mut builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    for v in values {
        if let Value::Array(list) = v {
            for item in list {
                if let Some(s) = item.as_str() {
                    builder.values().append_value(s);
                } else {
                    builder.values().append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append(false);
        }
    }
    Arc::new(builder.finish())
}

/// Convert JSON arrays to an Arrow list array.
fn json_arrays_to_list_array(values: &[Value], sample_arr: &[Value]) -> Arc<dyn Array> {
    // First try the provided sample array
    let inner_first = sample_arr.iter().find(|v| !v.is_null());

    // If sample is empty, search all arrays for a non-empty one to infer type
    let inner_first = inner_first.or_else(|| {
        values.iter().find_map(|v| {
            if let Value::Array(arr) = v {
                arr.iter().find(|item| !item.is_null())
            } else {
                None
            }
        })
    });

    match inner_first {
        Some(v) if v.is_u64() || v.is_i64() => json_arrays_to_uint64_list(values),
        Some(v) if v.is_f64() || v.is_number() => json_arrays_to_float64_list(values),
        Some(v) if v.is_string() => json_arrays_to_string_list(values),
        Some(Value::Array(inner_arr)) => json_arrays_to_nested_list(values, inner_arr),
        _ => {
            // All arrays are empty or contain only nulls - use Int64 as default
            // This preserves empty arrays as valid empty lists rather than null
            json_arrays_to_uint64_list(values)
        }
    }
}

/// Build a list array of nested lists from JSON arrays (for nested list comprehensions).
fn json_arrays_to_nested_list(values: &[Value], sample_inner: &[Value]) -> Arc<dyn Array> {
    // Flatten all inner arrays to determine the inner element type
    let all_inner_items: Vec<Value> = values
        .iter()
        .filter_map(|v| {
            if let Value::Array(outer) = v {
                Some(outer.iter().filter_map(|inner| {
                    if let Value::Array(arr) = inner {
                        Some(arr.clone())
                    } else {
                        None
                    }
                }))
            } else {
                None
            }
        })
        .flatten()
        .flatten()
        .collect();

    // Determine inner type from sample or flattened items
    let inner_type_sample = sample_inner.iter().find(|v| !v.is_null());
    let inner_type = inner_type_sample.or_else(|| all_inner_items.iter().find(|v| !v.is_null()));

    // Build the inner list array type based on what we find
    match inner_type {
        Some(v) if v.is_u64() || v.is_i64() => {
            // List<List<UInt64>>
            let inner_builder = ListBuilder::new(UInt64Builder::new());
            let mut builder = ListBuilder::new(inner_builder);

            for v in values {
                if let Value::Array(outer_list) = v {
                    for inner_item in outer_list {
                        if let Value::Array(inner_list) = inner_item {
                            for item in inner_list {
                                if let Some(n) =
                                    item.as_u64().or_else(|| item.as_i64().map(|i| i as u64))
                                {
                                    builder.values().values().append_value(n);
                                } else {
                                    builder.values().values().append_null();
                                }
                            }
                            builder.values().append(true);
                        } else {
                            builder.values().append(false);
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
        Some(v) if v.is_f64() || v.is_number() => {
            // List<List<Float64>>
            let inner_builder = ListBuilder::new(arrow_array::builder::Float64Builder::new());
            let mut builder = ListBuilder::new(inner_builder);

            for v in values {
                if let Value::Array(outer_list) = v {
                    for inner_item in outer_list {
                        if let Value::Array(inner_list) = inner_item {
                            for item in inner_list {
                                if let Some(f) = item.as_f64() {
                                    builder.values().values().append_value(f);
                                } else {
                                    builder.values().values().append_null();
                                }
                            }
                            builder.values().append(true);
                        } else {
                            builder.values().append(false);
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
        Some(v) if v.is_string() => {
            // List<List<Utf8>>
            let inner_builder = ListBuilder::new(arrow_array::builder::StringBuilder::new());
            let mut builder = ListBuilder::new(inner_builder);

            for v in values {
                if let Value::Array(outer_list) = v {
                    for inner_item in outer_list {
                        if let Value::Array(inner_list) = inner_item {
                            for item in inner_list {
                                if let Some(s) = item.as_str() {
                                    builder.values().values().append_value(s);
                                } else {
                                    builder.values().values().append_null();
                                }
                            }
                            builder.values().append(true);
                        } else {
                            builder.values().append(false);
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Default to List<List<UInt64>> for empty nested arrays
            let inner_builder = ListBuilder::new(UInt64Builder::new());
            let mut builder = ListBuilder::new(inner_builder);

            for v in values {
                if let Value::Array(outer_list) = v {
                    for inner_item in outer_list {
                        if let Value::Array(inner_list) = inner_item {
                            for item in inner_list {
                                if let Some(n) = item.as_u64() {
                                    builder.values().values().append_value(n);
                                } else {
                                    builder.values().values().append_null();
                                }
                            }
                            builder.values().append(true);
                        } else {
                            builder.values().append(false);
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
    }
}

/// Convert JSON objects to an Arrow struct array.
fn json_objects_to_struct_array(values: &[Value]) -> Arc<dyn Array> {
    let mut keys = std::collections::BTreeSet::new();
    for v in values {
        if let Value::Object(map) = v {
            for k in map.keys() {
                keys.insert(k.clone());
            }
        }
    }

    let mut fields = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    for key in keys {
        let mut child_values = Vec::with_capacity(values.len());
        for v in values {
            if let Value::Object(map) = v {
                child_values.push(map.get(&key).cloned().unwrap_or(Value::Null));
            } else {
                child_values.push(Value::Null);
            }
        }
        let child_array = json_vec_to_array(&child_values);
        fields.push(Arc::new(Field::new(
            &key,
            child_array.data_type().clone(),
            true,
        )));
        columns.push(child_array);
    }

    let struct_data: Vec<(Arc<Field>, Arc<dyn Array>)> = fields.into_iter().zip(columns).collect();
    Arc::new(StructArray::from(struct_data))
}

fn json_vec_to_array(values: &[Value]) -> Arc<dyn Array> {
    if values.is_empty() {
        return Arc::new(arrow_array::new_null_array(&DataType::Null, 0));
    }

    let first = values.iter().find(|v| !v.is_null()).unwrap_or(&Value::Null);

    // Check for mixed types requiring string promotion
    let has_string = values.iter().any(|v| v.is_string());
    let has_number = values.iter().any(|v| v.is_number());
    let has_boolean = values.iter().any(|v| v.is_boolean());

    if has_string && (has_number || has_boolean) {
        return json_vec_to_string_promoted(values);
    }

    match first {
        Value::String(_) => {
            let data: Vec<Option<&str>> = values.iter().map(|v| v.as_str()).collect();
            Arc::new(StringArray::from(data))
        }
        Value::Number(n) => json_numbers_to_array(values, n),
        Value::Bool(_) => {
            let data: Vec<Option<bool>> = values.iter().map(|v| v.as_bool()).collect();
            Arc::new(BooleanArray::from(data))
        }
        Value::Array(arr) => json_arrays_to_list_array(values, arr),
        Value::Object(_) => json_objects_to_struct_array(values),
        _ => Arc::new(arrow_array::new_null_array(&DataType::Null, values.len())),
    }
}

fn evaluate_l0(
    expr: &Expr,
    props: &std::collections::HashMap<String, Value>,
    variable: &str,
) -> Value {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let l = evaluate_l0(left, props, variable);
            let r = evaluate_l0(right, props, variable);
            match op {
                Operator::Eq => Value::Bool(l == r),
                Operator::NotEq => Value::Bool(l != r),
                Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                    compare_values(&l, &r, op)
                }
                Operator::In => {
                    if let Value::Array(arr) = r {
                        Value::Bool(arr.contains(&l))
                    } else {
                        Value::Null
                    }
                }
                Operator::And => {
                    Value::Bool(l.as_bool().unwrap_or(false) && r.as_bool().unwrap_or(false))
                }
                Operator::Or => {
                    Value::Bool(l.as_bool().unwrap_or(false) || r.as_bool().unwrap_or(false))
                }
                Operator::Contains => {
                    if let (Value::String(ls), Value::String(rs)) = (&l, &r) {
                        Value::Bool(ls.contains(rs))
                    } else {
                        Value::Null
                    }
                }
                Operator::StartsWith => {
                    if let (Value::String(ls), Value::String(rs)) = (&l, &r) {
                        Value::Bool(ls.starts_with(rs))
                    } else {
                        Value::Null
                    }
                }
                Operator::EndsWith => {
                    if let (Value::String(ls), Value::String(rs)) = (&l, &r) {
                        Value::Bool(ls.ends_with(rs))
                    } else {
                        Value::Null
                    }
                }
                Operator::Regex => {
                    // Handle NULL operands per Cypher semantics
                    if l.is_null() || r.is_null() {
                        Value::Null
                    } else if let (Value::String(ls), Value::String(pattern)) = (&l, &r) {
                        match regex::Regex::new(pattern) {
                            Ok(re) => Value::Bool(re.is_match(ls)),
                            Err(_) => Value::Null, // Invalid regex -> Null
                        }
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null, // Arithmetic not yet supported in L0 overlay
            }
        }
        Expr::Property(box_expr, prop) => {
            if let Expr::Identifier(v) = box_expr.as_ref()
                && v == variable
            {
                return props.get(prop).cloned().unwrap_or(Value::Null);
            }
            Value::Null
        }
        Expr::Literal(v) => v.clone(),
        Expr::List(items) => {
            let vals = items
                .iter()
                .map(|e| evaluate_l0(e, props, variable))
                .collect();
            Value::Array(vals)
        }
        _ => Value::Null, // Unsupported
    }
}

/// Traverses edges using AdjacencyCache (CSR).
#[derive(Clone)]
pub struct VectorizedTraverse {
    pub edge_type_ids: Vec<u16>,
    pub direction: Direction,
    pub source_variable: String,
    pub target_variable: String,
    pub step_variable: Option<String>,
    pub optional: bool,
    pub target_filter: Option<Expr>,
    pub path_variable: Option<String>,
    pub target_label_id: Option<u16>,
}

use crate::query::pushdown::{LanceFilterGenerator, PredicateAnalyzer};

#[async_trait]
impl VectorizedOperator for VectorizedTraverse {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let input = input.ok_or(anyhow!("VectorizedTraverse requires input"))?;
        let src_col = input.column(&self.source_variable)?;
        let src_vids = src_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Source variable must be UInt64 (Vid)"))?;

        // 1. Warm adjacency cache for all source labels
        self.warm_cache(src_vids, input.selection.as_ref(), ctx)
            .await?;

        // 2. Expand source VIDs to neighbors using CSR + L0 overlay
        let (mut new_dsts, mut new_eids, mut indices) =
            self.expand_neighbors(src_vids, &input, ctx);

        // 3. Apply target filter if present
        if let Some(filter) = &self.target_filter {
            (new_dsts, new_eids, indices) = self
                .apply_target_filter(filter, &input, src_vids, new_dsts, new_eids, indices, ctx)
                .await?;
        }

        // 4. Build output batch
        let batch = self.build_output(&input, indices, new_dsts, new_eids)?;
        ctx.observe_memory(&batch)?;
        Ok(batch)
    }

    fn name(&self) -> String {
        format!(
            "Traverse({}->{})",
            self.source_variable, self.target_variable
        )
    }
}

impl VectorizedTraverse {
    // ... (keep warm_cache and expand_neighbors and get_neighbors_with_overlay_accumulate and build_output as is) ...
    /// Warm the adjacency cache for all distinct source labels.
    async fn warm_cache(
        &self,
        src_vids: &UInt64Array,
        selection: Option<&BooleanArray>,
        ctx: &ExecutionContext,
    ) -> Result<()> {
        let cache = ctx.storage.adjacency_cache();

        let mut distinct_labels = std::collections::HashSet::new();
        for i in 0..src_vids.len() {
            if selection.map(|s| s.value(i)).unwrap_or(true) && !src_vids.is_null(i) {
                let vid = Vid::from(src_vids.value(i));
                distinct_labels.insert(vid.label_id());
            }
        }

        for &edge_type_id in &self.edge_type_ids {
            let edge_ver = ctx.storage.get_edge_version_by_id(edge_type_id);
            for &label_id in &distinct_labels {
                if cache
                    .get_csr(edge_type_id, self.direction, label_id)
                    .is_none()
                {
                    cache
                        .warm(
                            &ctx.storage,
                            edge_type_id,
                            self.direction,
                            label_id,
                            edge_ver,
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Expand source VIDs to their neighbors using CSR + L0 overlay.
    fn expand_neighbors(
        &self,
        src_vids: &UInt64Array,
        input: &VectorizedBatch,
        ctx: &ExecutionContext,
    ) -> (Vec<Option<u64>>, Vec<Option<u64>>, Vec<u64>) {
        let cache = ctx.storage.adjacency_cache();
        let mut new_dsts: Vec<Option<u64>> = Vec::new();
        let mut new_eids: Vec<Option<u64>> = Vec::new();
        let mut indices = Vec::new();

        let l0_guard = ctx.l0.as_ref().map(|l| l.read());
        let tx_l0_guard = ctx.transaction_l0.as_ref().map(|l| l.read());

        // Check if target is already bound
        let target_bound_col = input
            .variables
            .get(&self.target_variable)
            .map(|&idx| input.data.column(idx))
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>());

        for i in 0..src_vids.len() {
            if !input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                continue;
            }

            let bound_vid = target_bound_col.and_then(|col| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            });

            if src_vids.is_null(i) {
                if self.optional {
                    new_dsts.push(None);
                    if self.step_variable.is_some() {
                        new_eids.push(None);
                    }
                    indices.push(i as u64);
                }
                continue;
            }

            let src_vid = Vid::from(src_vids.value(i));

            let mut neighbors_map = HashMap::new();
            for &edge_type_id in &self.edge_type_ids {
                self.get_neighbors_with_overlay_accumulate(
                    &cache,
                    src_vid,
                    edge_type_id,
                    &l0_guard,
                    &tx_l0_guard,
                    &mut neighbors_map,
                );
            }

            let mut matched = false;
            for (eid, dst_vid) in neighbors_map {
                // Filter by target label if specified
                if let Some(target_label) = self.target_label_id
                    && dst_vid.label_id() != target_label
                {
                    continue;
                }

                // If target is bound, it must match
                if let Some(bv) = bound_vid
                    && dst_vid.as_u64() != bv
                {
                    continue;
                }

                matched = true;
                new_dsts.push(Some(dst_vid.as_u64()));
                if self.step_variable.is_some() {
                    new_eids.push(Some(eid.as_u64()));
                }
                indices.push(i as u64);
            }

            if !matched && self.optional {
                new_dsts.push(None);
                if self.step_variable.is_some() {
                    new_eids.push(None);
                }
                indices.push(i as u64);
            }
        }

        (new_dsts, new_eids, indices)
    }

    /// Get neighbors from CSR with L0 overlay and accumulate.
    fn get_neighbors_with_overlay_accumulate(
        &self,
        cache: &AdjacencyCache,
        src_vid: Vid,
        edge_type_id: u16,
        l0_guard: &Option<parking_lot::RwLockReadGuard<'_, L0Buffer>>,
        tx_l0_guard: &Option<parking_lot::RwLockReadGuard<'_, L0Buffer>>,
        neighbors_map: &mut HashMap<Eid, Vid>,
    ) {
        if let Some(csr) = cache.get_csr(edge_type_id, self.direction, src_vid.label_id()) {
            let (n, e) = csr.get_neighbors(src_vid);

            for (&neighbor, &eid) in n.iter().zip(e.iter()) {
                neighbors_map.insert(eid, neighbor);
            }
        }

        if let Some(l0) = l0_guard.as_deref() {
            cache.overlay_l0_neighbors(src_vid, edge_type_id, self.direction, l0, neighbors_map);
        }

        if let Some(tx_l0) = tx_l0_guard.as_deref() {
            cache.overlay_l0_neighbors(src_vid, edge_type_id, self.direction, tx_l0, neighbors_map);
        }
    }

    /// Build the output batch from expanded data.
    fn build_output(
        &self,
        input: &VectorizedBatch,
        indices: Vec<u64>,
        new_dsts: Vec<Option<u64>>,
        new_eids: Vec<Option<u64>>,
    ) -> Result<VectorizedBatch> {
        use arrow::compute::take;
        let indices_array = UInt64Array::from(indices);

        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();
        let mut new_vars = HashMap::new();

        for (name, &col_idx) in &input.variables {
            let col = input.data.column(col_idx);
            let new_col = take(col.as_ref(), &indices_array, None)?;
            new_columns.push(new_col);
            new_fields.push(Field::new(name, col.data_type().clone(), true));
            new_vars.insert(name.clone(), new_columns.len() - 1);
        }

        let target_col = UInt64Array::from(new_dsts);
        new_columns.push(Arc::new(target_col));
        new_fields.push(Field::new(
            self.target_variable.clone(),
            DataType::UInt64,
            true,
        ));
        new_vars.insert(self.target_variable.clone(), new_columns.len() - 1);

        if let Some(step_var) = &self.step_variable {
            let step_col = UInt64Array::from(new_eids);
            new_columns.push(Arc::new(step_col));
            new_fields.push(Field::new(step_var.clone(), DataType::UInt64, true));
            new_vars.insert(step_var.clone(), new_columns.len() - 1);
        }

        let schema = Arc::new(ArrowSchema::new(new_fields));
        let batch = RecordBatch::try_new(schema, new_columns)?;

        Ok(VectorizedBatch::new(batch, new_vars))
    }

    /// Helper to combine multiple predicates into one AND expression.
    fn combine_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        if predicates.is_empty() {
            return None;
        }
        let mut result = predicates[0].clone();
        for pred in predicates.iter().skip(1) {
            result = Expr::BinaryOp {
                left: Box::new(result),
                op: Operator::And,
                right: Box::new(pred.clone()),
            };
        }
        Some(result)
    }

    /// Check if a predicate references the _vid pseudo-property.
    /// Used to filter out _vid predicates from L0 evaluation since _vid
    /// is stored as the key in L0, not as a property.
    fn predicate_references_vid(expr: &Expr) -> bool {
        match expr {
            Expr::Property(_, prop) => prop == "_vid",
            Expr::BinaryOp { left, right, .. } => {
                Self::predicate_references_vid(left) || Self::predicate_references_vid(right)
            }
            Expr::UnaryOp { expr, .. } => Self::predicate_references_vid(expr),
            Expr::List(items) => items.iter().any(Self::predicate_references_vid),
            Expr::FunctionCall { args, .. } => args.iter().any(Self::predicate_references_vid),
            _ => false,
        }
    }

    /// Apply target filter to expanded neighbors and handle optional match semantics.
    /// Utilizes VectorizedScan to perform efficient pushdown and L0 overlay.
    #[allow(clippy::too_many_arguments)]
    async fn apply_target_filter(
        &self,
        filter: &Expr,
        input: &VectorizedBatch,
        src_vids: &UInt64Array,
        new_dsts: Vec<Option<u64>>,
        new_eids: Vec<Option<u64>>,
        indices: Vec<u64>,
        ctx: &ExecutionContext,
    ) -> Result<(Vec<Option<u64>>, Vec<Option<u64>>, Vec<u64>)> {
        let count = new_dsts.len();
        if count == 0 {
            return Ok((new_dsts, new_eids, indices));
        }

        // 1. Collect candidate VIDs by label
        let mut vids_by_label: HashMap<u16, Vec<u64>> = HashMap::new();
        for vid_u64 in new_dsts.iter().flatten() {
            let vid = Vid::from(*vid_u64);
            vids_by_label
                .entry(vid.label_id())
                .or_default()
                .push(*vid_u64);
        }

        let mut allowed_vids = std::collections::HashSet::new();

        // 2. Scan each label to verify VIDs against filter using VectorizedScan
        for (label_id, vids) in vids_by_label {
            // Chunk VIDs to avoid massive IN clauses (e.g. max 1000 per scan)
            for chunk in vids.chunks(1000) {
                let vid_list: Vec<Expr> = chunk
                    .iter()
                    .map(|&v| Expr::Literal(Value::Number(serde_json::Number::from(v))))
                    .collect();
                let in_expr = Expr::BinaryOp {
                    left: Box::new(Expr::Property(
                        Box::new(Expr::Identifier(self.target_variable.clone())),
                        "_vid".to_string(),
                    )),
                    op: Operator::In,
                    right: Box::new(Expr::List(vid_list)),
                };

                let combined_filter = Expr::BinaryOp {
                    left: Box::new(filter.clone()),
                    op: Operator::And,
                    right: Box::new(in_expr),
                };

                // Analyze for pushdown
                let analyzer = PredicateAnalyzer::new(None);
                let analysis = analyzer.analyze(&combined_filter, &self.target_variable);

                let pushed_filter =
                    LanceFilterGenerator::generate(&analysis.pushable, &self.target_variable);

                let residual_filter = if !analysis.residual.is_empty() {
                    Self::combine_predicates(analysis.residual)
                } else {
                    None
                };

                // Collect properties needed for residual evaluation + implicitly _vid
                let project_columns = analysis.required_properties;

                // Filter out _vid predicates from pushed_predicates since L0 can't
                // evaluate them (vid is the key, not a property in vertex_properties).
                // The _vid IN filter is already applied via pushed_filter for Lance,
                // and L0 vertices will be naturally limited to the candidate VIDs
                // by the outer loop's vid_by_label grouping.
                let l0_pushable: Vec<Expr> = analysis
                    .pushable
                    .into_iter()
                    .filter(|pred| !Self::predicate_references_vid(pred))
                    .collect();

                let scan = VectorizedScan {
                    label_id,
                    variable: self.target_variable.clone(),
                    pushed_filter,
                    pushed_predicates: l0_pushable,
                    residual_filter,
                    subqueries: vec![], // Assuming no subqueries in target_filter
                    project_columns,
                    optional: false,
                    uid_lookup: None, // Index lookups not used for target_filter
                    jsonpath_lookups: vec![], // Index lookups not used for target_filter
                    json_fts_predicates: vec![], // FTS not used for target_filter
                };

                // Execute scan
                let batch = scan.execute(None, ctx).await?;

                // Collect surviving VIDs
                if let Ok(col) = batch.column(&self.target_variable)
                    && let Some(u64_col) = col.as_any().downcast_ref::<UInt64Array>()
                {
                    for i in 0..u64_col.len() {
                        if batch.selection.as_ref().map(|s| s.value(i)).unwrap_or(true)
                            && !u64_col.is_null(i)
                        {
                            allowed_vids.insert(u64_col.value(i));
                        }
                    }
                }
            }
        }

        // 3. Filter the original lists
        let mut filtered_indices = Vec::new();
        let mut filtered_dsts = Vec::new();
        let mut filtered_eids = Vec::new();

        for i in 0..count {
            let keep = match new_dsts[i] {
                Some(vid) => allowed_vids.contains(&vid),
                None => true, // Keep existing nulls (from optional expansion)
            };

            if keep {
                filtered_indices.push(indices[i]);
                filtered_dsts.push(new_dsts[i]);
                if self.step_variable.is_some() {
                    filtered_eids.push(new_eids[i]);
                }
            }
        }

        // 4. Re-handle Optional: If a source index is not in filtered_indices, add a NULL row
        if self.optional {
            let mut present_indices = std::collections::HashSet::new();
            for &idx in &filtered_indices {
                present_indices.insert(idx);
            }

            for i in 0..input.num_rows() {
                if !input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                    continue;
                }
                if src_vids.is_null(i) {
                    if !present_indices.contains(&(i as u64)) {
                        filtered_indices.push(i as u64);
                        filtered_dsts.push(None);
                        if self.step_variable.is_some() {
                            filtered_eids.push(None);
                        }
                    }
                    continue;
                }

                if !present_indices.contains(&(i as u64)) {
                    filtered_indices.push(i as u64);
                    filtered_dsts.push(None);
                    if self.step_variable.is_some() {
                        filtered_eids.push(None);
                    }
                }
            }
        }

        Ok((filtered_dsts, filtered_eids, filtered_indices))
    }
}

#[derive(Clone)]
pub struct VectorizedVariableLengthTraverse {
    pub edge_type_ids: Vec<u16>,
    pub direction: Direction,
    pub source_variable: String,
    pub target_variable: String,
    pub step_variable: Option<String>,
    pub min_hops: usize,
    pub max_hops: usize,
    pub path_variable: Option<String>,
    pub target_label_id: Option<u16>,
    pub optional: bool,
}

/// Type alias for BFS frontier entry: (origin_index, current_vid, path_eids, path_vids)
type BfsFrontierEntry = (u32, u64, Vec<u64>, Vec<u64>);

/// Type alias for BFS result entry: (origin_idx, dst_vid, path_eids, path_vids)
type BfsResultEntry = (u32, u64, Vec<u64>, Vec<u64>);

/// Mutable state for path expansion in BFS traversal.
struct PathExpansionState<'a> {
    new_frontier: &'a mut Vec<BfsFrontierEntry>,
    results: &'a mut Vec<BfsResultEntry>,
    target_bound_col: Option<&'a UInt64Array>,
}

/// Edge expansion context: destination, edge ID, and current path state.
struct EdgeExpansionContext {
    dst_vid: u64,
    eid: u64,
    parent_path: Vec<u64>,
    parent_path_vids: Vec<u64>,
}

/// Parameters for list comprehension evaluation.
struct ListComprehensionParams<'a> {
    variable: &'a str,
    list: &'a Expr,
    where_clause: &'a Option<Box<Expr>>,
    mapping: &'a Option<Box<Expr>>,
}

impl VectorizedVariableLengthTraverse {
    /// Warm the adjacency cache for all distinct labels in the source VIDs.
    async fn warm_adjacency_cache(
        &self,
        src_vids: &UInt64Array,
        selection: &Option<BooleanArray>,
        cache: &Arc<AdjacencyCache>,
        storage: &StorageManager,
    ) -> Result<()> {
        let mut distinct_labels = std::collections::HashSet::new();
        for i in 0..src_vids.len() {
            if selection.as_ref().map(|s| s.value(i)).unwrap_or(true) && !src_vids.is_null(i) {
                let vid = Vid::from(src_vids.value(i));
                distinct_labels.insert(vid.label_id());
            }
        }

        for &edge_type_id in &self.edge_type_ids {
            let edge_ver = storage.get_edge_version_by_id(edge_type_id);
            for &label_id in &distinct_labels {
                if cache
                    .get_csr(edge_type_id, self.direction, label_id)
                    .is_none()
                {
                    cache
                        .warm(storage, edge_type_id, self.direction, label_id, edge_ver)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Initialize the BFS frontier from source VIDs.
    fn init_frontier(
        &self,
        src_vids: &UInt64Array,
        selection: &Option<BooleanArray>,
        track_paths: bool,
    ) -> (
        Vec<BfsFrontierEntry>,
        HashMap<u32, std::collections::HashSet<u64>>,
    ) {
        let mut frontier = Vec::new();
        let mut global_visited: HashMap<u32, std::collections::HashSet<u64>> = HashMap::new();

        for i in 0..src_vids.len() {
            if !selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                continue;
            }
            if src_vids.is_null(i) {
                continue;
            }
            let vid = src_vids.value(i);
            let initial_vids = if track_paths { vec![vid] } else { Vec::new() };
            frontier.push((i as u32, vid, Vec::new(), initial_vids));

            if !track_paths {
                global_visited.entry(i as u32).or_default().insert(vid);
            }
        }
        (frontier, global_visited)
    }

    /// Process expansion results for path-tracking mode.
    fn process_path_mode_result(
        &self,
        origin_idx: u32,
        edge_ctx: EdgeExpansionContext,
        depth: usize,
        state: &mut PathExpansionState<'_>,
    ) {
        // Cypher enforces Relationship Uniqueness
        if edge_ctx.parent_path.contains(&edge_ctx.eid) {
            return; // Cycle detected (Rel uniqueness)
        }

        let mut new_path = edge_ctx.parent_path;
        new_path.push(edge_ctx.eid);

        let mut new_path_vids = edge_ctx.parent_path_vids;
        new_path_vids.push(edge_ctx.dst_vid);

        state.new_frontier.push((
            origin_idx,
            edge_ctx.dst_vid,
            new_path.clone(),
            new_path_vids.clone(),
        ));

        if depth >= self.min_hops {
            if let Some(target_label) = self.target_label_id
                && Vid::from(edge_ctx.dst_vid).label_id() != target_label
            {
                return;
            }

            // If target is bound, it must match
            if let Some(col) = state.target_bound_col
                && !col.is_null(origin_idx as usize)
                && col.value(origin_idx as usize) != edge_ctx.dst_vid
            {
                return;
            }

            state
                .results
                .push((origin_idx, edge_ctx.dst_vid, new_path, new_path_vids));
        }
    }

    /// Process expansion results for reachability mode.
    #[allow(clippy::too_many_arguments)]
    fn process_reachability_result(
        &self,
        origin_idx: u32,
        dst_u64: u64,
        depth: usize,
        global_visited: &mut HashMap<u32, std::collections::HashSet<u64>>,
        new_frontier: &mut Vec<BfsFrontierEntry>,
        results: &mut Vec<BfsResultEntry>,
        target_bound_col: Option<&UInt64Array>,
    ) {
        let origin_visited = global_visited.entry(origin_idx).or_default();
        if !origin_visited.contains(&dst_u64) {
            origin_visited.insert(dst_u64);
            new_frontier.push((origin_idx, dst_u64, Vec::new(), Vec::new()));

            if depth >= self.min_hops {
                if let Some(target_label) = self.target_label_id
                    && Vid::from(dst_u64).label_id() != target_label
                {
                    return;
                }

                // If target is bound, it must match
                if let Some(col) = target_bound_col
                    && !col.is_null(origin_idx as usize)
                    && col.value(origin_idx as usize) != dst_u64
                {
                    return;
                }

                results.push((origin_idx, dst_u64, Vec::new(), Vec::new()));
            }
        }
    }

    /// Build the output batch from traversal results.
    fn build_output_batch(
        &self,
        input: &VectorizedBatch,
        results: &[BfsResultEntry],
    ) -> Result<VectorizedBatch> {
        use arrow::compute::take;

        let mut indices: Vec<u64> = results.iter().map(|(idx, _, _, _)| *idx as u64).collect();
        let mut target_vids: Vec<Option<u64>> =
            results.iter().map(|(_, vid, _, _)| Some(*vid)).collect();
        let mut path_eids: Vec<Option<Vec<u64>>> =
            results.iter().map(|(_, _, e, _)| Some(e.clone())).collect();
        let mut path_vids: Vec<Option<Vec<u64>>> =
            results.iter().map(|(_, _, _, v)| Some(v.clone())).collect();

        if self.optional {
            let found_indices: std::collections::HashSet<u64> = indices.iter().cloned().collect();
            for i in 0..input.num_rows() {
                if !input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                    continue;
                }
                let i_u64 = i as u64;
                if !found_indices.contains(&i_u64) {
                    indices.push(i_u64);
                    target_vids.push(None);
                    path_eids.push(None);
                    path_vids.push(None);
                }
            }
        }

        let indices_array = UInt64Array::from(indices);

        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();
        let mut new_vars = HashMap::new();

        for (name, &col_idx) in &input.variables {
            let col = input.data.column(col_idx);
            let new_col = take(col.as_ref(), &indices_array, None)?;
            new_columns.push(new_col);
            new_fields.push(Field::new(name, col.data_type().clone(), true));
            new_vars.insert(name.clone(), new_columns.len() - 1);
        }

        let target_col = UInt64Array::from(target_vids);
        new_columns.push(Arc::new(target_col));
        new_fields.push(Field::new(
            self.target_variable.clone(),
            DataType::UInt64,
            true,
        ));
        new_vars.insert(self.target_variable.clone(), new_columns.len() - 1);

        if let Some(step_var) = &self.step_variable {
            let mut rels_builder = ListBuilder::new(UInt64Builder::new());
            let mut nodes_builder = ListBuilder::new(UInt64Builder::new());
            let mut valid_builder = BooleanBuilder::new();

            for (eids_opt, vids_opt) in path_eids.iter().zip(path_vids.iter()) {
                if let (Some(eids), Some(vids)) = (eids_opt, vids_opt) {
                    for &eid in eids {
                        rels_builder.values().append_value(eid);
                    }
                    rels_builder.append(true);

                    for &vid in vids {
                        nodes_builder.values().append_value(vid);
                    }
                    nodes_builder.append(true);
                    valid_builder.append_value(true);
                } else {
                    rels_builder.append(false);
                    nodes_builder.append(false);
                    valid_builder.append_value(false);
                }
            }

            let rels_array = Arc::new(rels_builder.finish());
            let nodes_array = Arc::new(nodes_builder.finish());
            let nulls = Some(arrow::buffer::NullBuffer::new(
                valid_builder.finish().values().clone(),
            ));

            let fields = vec![
                Arc::new(Field::new(
                    "relationships",
                    rels_array.data_type().clone(),
                    false,
                )),
                Arc::new(Field::new("nodes", nodes_array.data_type().clone(), false)),
            ];

            let struct_array =
                StructArray::try_new(fields.into(), vec![rels_array, nodes_array], nulls)?;
            let struct_array = Arc::new(struct_array);

            new_columns.push(struct_array.clone());
            new_fields.push(Field::new(
                step_var.clone(),
                struct_array.data_type().clone(),
                true,
            ));
            new_vars.insert(step_var.clone(), new_columns.len() - 1);
        }

        let schema = Arc::new(ArrowSchema::new(new_fields));
        let batch = RecordBatch::try_new(schema, new_columns)?;
        Ok(VectorizedBatch::new(batch, new_vars))
    }
}

#[async_trait]
impl VectorizedOperator for VectorizedVariableLengthTraverse {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let input = input.ok_or(anyhow!("VectorizedVariableLengthTraverse requires input"))?;
        let src_col = input.column(&self.source_variable)?;
        let src_vids = src_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Source variable must be UInt64 (Vid)"))?;

        let cache = ctx.storage.adjacency_cache();

        // 1. Warm Cache
        self.warm_adjacency_cache(src_vids, &input.selection, &cache, &ctx.storage)
            .await?;

        // 2. Initialize BFS
        let track_paths = self.step_variable.is_some();
        let (mut frontier, mut global_visited) =
            self.init_frontier(src_vids, &input.selection, track_paths);

        let mut results: Vec<BfsResultEntry> = Vec::new();
        let l0_arc = ctx.l0.clone();
        let tx_l0_arc = ctx.transaction_l0.clone();

        let target_bound_col = input
            .variables
            .get(&self.target_variable)
            .map(|&idx| input.data.column(idx))
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>());

        // 3. BFS Loop
        for depth in 1..=self.max_hops {
            if frontier.is_empty() {
                break;
            }

            if frontier.len() > ctx.config.max_frontier_size {
                log::warn!(
                    "Frontier size {} exceeds limit {}, pruning",
                    frontier.len(),
                    ctx.config.max_frontier_size
                );
                frontier.truncate(ctx.config.max_frontier_size);
            }

            let chunks: Vec<Vec<BfsFrontierEntry>> = frontier
                .chunks(ctx.config.batch_size)
                .map(|c| c.to_vec())
                .collect();

            let edge_type_ids = self.edge_type_ids.clone();
            let direction = self.direction;
            let storage_arc = ctx.storage.clone();
            let l0_arc_inner = l0_arc.clone();
            let tx_l0_arc_inner = tx_l0_arc.clone();

            let mut stream = futures::stream::iter(chunks)
                .map(|chunk| {
                    let storage = storage_arc.clone();
                    let l0 = l0_arc_inner.clone();
                    let tx_l0 = tx_l0_arc_inner.clone();
                    let e_ids = edge_type_ids.clone(); // Clone for task
                    tokio::task::spawn_blocking(move || {
                        let mut local_results = Vec::with_capacity(chunk.len() * 2);
                        let cache = storage.adjacency_cache();

                        let l0_guard = l0.as_ref().map(|l| l.read());
                        let tx_l0_guard = tx_l0.as_ref().map(|l| l.read());

                        for (origin_idx, current_u64, path, path_vids) in chunk {
                            let current_vid = Vid::from(current_u64);

                            let mut neighbors_map = HashMap::new();

                            for &edge_type_id in &e_ids {
                                if let Some(csr) =
                                    cache.get_csr(edge_type_id, direction, current_vid.label_id())
                                {
                                    let (n, e) = csr.get_neighbors(current_vid);
                                    for (&neighbor, &eid) in n.iter().zip(e.iter()) {
                                        neighbors_map.insert(eid, neighbor);
                                    }
                                }

                                if let Some(l0_ref) = l0_guard.as_deref() {
                                    cache.overlay_l0_neighbors(
                                        current_vid,
                                        edge_type_id,
                                        direction,
                                        l0_ref,
                                        &mut neighbors_map,
                                    );
                                }

                                if let Some(tx_l0_ref) = tx_l0_guard.as_deref() {
                                    cache.overlay_l0_neighbors(
                                        current_vid,
                                        edge_type_id,
                                        direction,
                                        tx_l0_ref,
                                        &mut neighbors_map,
                                    );
                                }
                            }

                            for (eid, dst_vid) in neighbors_map {
                                local_results.push((
                                    origin_idx,
                                    dst_vid.as_u64(),
                                    eid.as_u64(),
                                    path.clone(),
                                    path_vids.clone(),
                                ));
                            }
                        }
                        local_results
                    })
                })
                .buffer_unordered(ctx.config.parallelism);

            let mut new_frontier = Vec::new();

            while let Some(result) = stream.next().await {
                let morsel_results = result?;
                let mut state = PathExpansionState {
                    new_frontier: &mut new_frontier,
                    results: &mut results,
                    target_bound_col,
                };
                for (origin_idx, dst_u64, eid_u64, parent_path, parent_path_vids) in morsel_results
                {
                    if track_paths {
                        let edge_ctx = EdgeExpansionContext {
                            dst_vid: dst_u64,
                            eid: eid_u64,
                            parent_path,
                            parent_path_vids,
                        };
                        self.process_path_mode_result(origin_idx, edge_ctx, depth, &mut state);
                    } else {
                        self.process_reachability_result(
                            origin_idx,
                            dst_u64,
                            depth,
                            &mut global_visited,
                            state.new_frontier,
                            state.results,
                            state.target_bound_col,
                        );
                    }
                }
            }

            frontier = new_frontier;
        }

        // 4. Build Output
        let batch = self.build_output_batch(&input, &results)?;
        ctx.observe_memory(&batch)?;
        Ok(batch)
    }
}

#[derive(Clone)]
pub struct VectorizedFilter {
    pub predicate: Expr,
    pub subqueries: Vec<PhysicalPlan>,
}

#[async_trait]
impl VectorizedOperator for VectorizedFilter {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let mut batch = input.ok_or(anyhow!("VectorizedFilter requires input"))?;

        let mut mask_builder = BooleanBuilder::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            if !batch.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                mask_builder.append_value(false);
                continue;
            }

            let val =
                evaluate_scalar(&self.predicate, &mut batch, i, ctx, &self.subqueries).await?;
            let pass = val
                .as_bool()
                .ok_or(anyhow!("Filter predicate must evaluate to boolean"))?;
            mask_builder.append_value(pass);
        }

        batch.selection = Some(mask_builder.finish());
        Ok(batch)
    }

    fn name(&self) -> String {
        "Filter".to_string()
    }
}

#[derive(Clone)]
pub struct VectorizedProject {
    pub projections: Vec<(String, Expr)>,
    pub subqueries: Vec<PhysicalPlan>,
}

#[async_trait]
impl VectorizedOperator for VectorizedProject {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let mut input = input.ok_or(anyhow!("VectorizedProject requires input"))?;

        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();
        let mut new_vars = HashMap::new();

        for (out_name, expr) in &self.projections {
            // Optimization: Check if the expression matches an existing column (e.g. from Aggregate)
            let expr_name = expr.to_string_repr();
            if let Ok(col) = input.column(&expr_name) {
                new_columns.push(col.clone());
                new_fields.push(Field::new(out_name, col.data_type().clone(), true));
                new_vars.insert(out_name.clone(), new_columns.len() - 1);
                continue;
            }

            if let Expr::Identifier(var) = expr
                && let Ok(col) = input.column(var)
            {
                new_columns.push(col.clone());
                new_fields.push(Field::new(out_name, col.data_type().clone(), true));
                new_vars.insert(out_name.clone(), new_columns.len() - 1);
                continue;
            }

            let mut values = Vec::with_capacity(input.num_rows());
            for i in 0..input.num_rows() {
                if input.selection.as_ref().map(|s| s.value(i)).unwrap_or(true) {
                    let val = evaluate_scalar(expr, &mut input, i, ctx, &self.subqueries).await?;
                    values.push(val);
                } else {
                    values.push(Value::Null);
                }
            }

            let array = json_vec_to_array(&values);

            new_columns.push(array.clone());
            new_fields.push(Field::new(out_name, array.data_type().clone(), true));
            new_vars.insert(out_name.clone(), new_columns.len() - 1);
        }

        let schema = Arc::new(ArrowSchema::new(new_fields));
        let data = RecordBatch::try_new(schema, new_columns)?;
        let mut new_batch = VectorizedBatch::new(data, new_vars);
        new_batch.selection = input.selection;
        ctx.observe_memory(&new_batch)?;
        Ok(new_batch)
    }

    fn name(&self) -> String {
        "Project".to_string()
    }
}

// ============================================================================
// Scalar Evaluation Helpers
// ============================================================================

/// Extract a value from an Arrow column at the given row index.
/// Handles common Arrow types: UInt64, String, Boolean, List<UInt64>, Struct.
fn extract_arrow_column_value(col: &ArrayRef, row_idx: usize) -> Result<Value> {
    Ok(arrow_val_to_json(col.as_ref(), row_idx))
}

// ============================================================================
// Path function helpers
// ============================================================================

fn eval_path_length(val: Value) -> Result<Value> {
    match val {
        Value::Object(map) => {
            if let Some(Value::Array(rels)) = map.get("relationships") {
                Ok(json!(rels.len()))
            } else {
                Err(anyhow!("LENGTH argument must be a Path with relationships"))
            }
        }
        Value::Array(arr) => Ok(json!(arr.len())),
        Value::String(s) => Ok(json!(s.len())),
        _ => Err(anyhow!("LENGTH argument must be a Path, List or String")),
    }
}

fn eval_path_nodes(val: Value) -> Result<Value> {
    match val {
        Value::Object(map) => {
            if let Some(nodes) = map.get("nodes") {
                Ok(nodes.clone())
            } else {
                Err(anyhow!("Argument is not a Path (missing 'nodes')"))
            }
        }
        _ => Err(anyhow!("NODES argument must be a Path")),
    }
}

fn eval_path_relationships(val: Value) -> Result<Value> {
    match val {
        Value::Object(map) => {
            if let Some(rels) = map.get("relationships") {
                Ok(rels.clone())
            } else {
                Err(anyhow!("Argument is not a Path (missing 'relationships')"))
            }
        }
        Value::Array(_) => Ok(val),
        _ => Err(anyhow!("RELATIONSHIPS argument must be a Path")),
    }
}

/// Evaluate path-related functions: LENGTH, NODES, RELATIONSHIPS.
fn eval_path_function(name: &str, val: Value) -> Result<Value> {
    match name {
        "length" => eval_path_length(val),
        "nodes" => eval_path_nodes(val),
        "relationships" => eval_path_relationships(val),
        _ => Err(anyhow!("Unknown path function: {}", name)),
    }
}

// ============================================================================
// Collection function helpers
// ============================================================================

fn eval_coll_size(val: Value) -> Result<Value> {
    match val {
        Value::Array(arr) => Ok(json!(arr.len())),
        Value::Object(map) => Ok(json!(map.len())),
        Value::String(s) => Ok(json!(s.len())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("size() expects a List, Map, or String")),
    }
}

fn eval_coll_keys(val: Value) -> Result<Value> {
    match val {
        Value::Object(map) => Ok(json!(map.keys().collect::<Vec<_>>())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("keys() expects a Map")),
    }
}

fn eval_coll_head(val: Value) -> Result<Value> {
    match val {
        Value::Array(arr) => Ok(arr.first().cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("head() expects a List")),
    }
}

fn eval_coll_tail(val: Value) -> Result<Value> {
    match val {
        Value::Array(arr) => {
            if arr.is_empty() {
                Ok(json!([]))
            } else {
                Ok(json!(arr[1..]))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("tail() expects a List")),
    }
}

/// Evaluate collection functions: SIZE, KEYS, HEAD, TAIL.
fn eval_collection_function(name: &str, val: Value) -> Result<Value> {
    match name {
        "size" => eval_coll_size(val),
        "keys" => eval_coll_keys(val),
        "head" => eval_coll_head(val),
        "tail" => eval_coll_tail(val),
        _ => Err(anyhow!("Unknown collection function: {}", name)),
    }
}

// ============================================================================
// Comparison operator helpers
// ============================================================================

/// Evaluate binary operators.
fn eval_comparison_op(op: &Operator, l: &Value, r: &Value) -> Result<Value> {
    crate::query::expr_eval::eval_binary_op(l, op, r)
}

// ============================================================================
// Scalar expression evaluation helpers
// ============================================================================

/// Look up a property from a numeric ID (VID or EID) using PropertyManager.
async fn lookup_property_from_id(
    id: u64,
    prop_name: &str,
    ctx: &ExecutionContext,
) -> Result<Value> {
    // Construct QueryContext for L0
    let q_ctx = ctx.l0.as_ref().map(|l| {
        QueryContext::new_with_pending(
            l.clone(),
            ctx.transaction_l0.clone(),
            ctx.pending_flush_l0s.clone(),
        )
    });

    // 1. Try vertex property lookup
    let vid = Vid::from(id);
    if let Ok(val) = ctx
        .property_manager
        .get_vertex_prop_with_ctx(vid, prop_name, q_ctx.as_ref())
        .await
        && !val.is_null()
    {
        return Ok(val);
    }

    // 2. Try edge property lookup
    let eid = Eid::from(id);
    if let Ok(val) = ctx
        .property_manager
        .get_edge_prop(eid, prop_name, q_ctx.as_ref())
        .await
    {
        return Ok(val);
    }

    Ok(Value::Null)
}

/// Evaluate short-circuit AND operation.
async fn eval_short_circuit_and(
    left: &Expr,
    right: &Expr,
    batch: &mut VectorizedBatch,
    row_idx: usize,
    ctx: &ExecutionContext,
    scope: &HashMap<String, Value>,
    subqueries: &[PhysicalPlan],
) -> Result<Value> {
    let l = evaluate_scalar_with_scope(left, batch, row_idx, ctx, scope, subqueries).await?;
    if l.as_bool() == Some(false) {
        return Ok(Value::Bool(false));
    }
    let r = evaluate_scalar_with_scope(right, batch, row_idx, ctx, scope, subqueries).await?;
    Ok(Value::Bool(
        l.as_bool().unwrap_or(false) && r.as_bool().unwrap_or(false),
    ))
}

/// Evaluate short-circuit OR operation.
async fn eval_short_circuit_or(
    left: &Expr,
    right: &Expr,
    batch: &mut VectorizedBatch,
    row_idx: usize,
    ctx: &ExecutionContext,
    scope: &HashMap<String, Value>,
    subqueries: &[PhysicalPlan],
) -> Result<Value> {
    let l = evaluate_scalar_with_scope(left, batch, row_idx, ctx, scope, subqueries).await?;
    if l.as_bool() == Some(true) {
        return Ok(Value::Bool(true));
    }
    let r = evaluate_scalar_with_scope(right, batch, row_idx, ctx, scope, subqueries).await?;
    Ok(Value::Bool(
        l.as_bool().unwrap_or(false) || r.as_bool().unwrap_or(false),
    ))
}

/// Evaluate a CASE expression.
#[allow(clippy::too_many_arguments)]
async fn eval_case_expr(
    base_expr: &Option<Box<Expr>>,
    when_then: &[(Expr, Expr)],
    else_expr: &Option<Box<Expr>>,
    batch: &mut VectorizedBatch,
    row_idx: usize,
    ctx: &ExecutionContext,
    scope: &HashMap<String, Value>,
    subqueries: &[PhysicalPlan],
) -> Result<Value> {
    if let Some(base) = base_expr {
        let base_val =
            evaluate_scalar_with_scope(base, batch, row_idx, ctx, scope, subqueries).await?;
        for (w, t) in when_then {
            let w_val =
                evaluate_scalar_with_scope(w, batch, row_idx, ctx, scope, subqueries).await?;
            if base_val == w_val {
                return evaluate_scalar_with_scope(t, batch, row_idx, ctx, scope, subqueries).await;
            }
        }
    } else {
        for (w, t) in when_then {
            let w_val =
                evaluate_scalar_with_scope(w, batch, row_idx, ctx, scope, subqueries).await?;
            if w_val.as_bool() == Some(true) {
                return evaluate_scalar_with_scope(t, batch, row_idx, ctx, scope, subqueries).await;
            }
        }
    }

    if let Some(e) = else_expr {
        return evaluate_scalar_with_scope(e, batch, row_idx, ctx, scope, subqueries).await;
    }
    Ok(Value::Null)
}

/// Evaluate a list comprehension expression.
async fn eval_list_comprehension(
    params: &ListComprehensionParams<'_>,
    batch: &mut VectorizedBatch,
    row_idx: usize,
    ctx: &ExecutionContext,
    scope: &HashMap<String, Value>,
    subqueries: &[PhysicalPlan],
) -> Result<Value> {
    let list_val =
        evaluate_scalar_with_scope(params.list, batch, row_idx, ctx, scope, subqueries).await?;

    let items = match list_val {
        Value::Array(items) => items,
        Value::Null => return Ok(Value::Array(vec![])),
        _ => return Err(anyhow!("List comprehension source must be a list")),
    };

    let mut results = Vec::new();
    for item in items {
        // Bind variable to local scope
        let mut inner_scope = scope.clone();
        inner_scope.insert(params.variable.to_string(), item.clone());

        // Check where clause
        let keep = if let Some(w) = params.where_clause {
            let w_val =
                evaluate_scalar_with_scope(w, batch, row_idx, ctx, &inner_scope, subqueries)
                    .await?;
            w_val.as_bool().unwrap_or(false)
        } else {
            true
        };

        if keep {
            // Map
            let map_val = if let Some(m) = params.mapping {
                evaluate_scalar_with_scope(m, batch, row_idx, ctx, &inner_scope, subqueries).await?
            } else {
                item // Identity mapping
            };
            results.push(map_val);
        }
    }
    Ok(Value::Array(results))
}

/// Evaluate REDUCE expression.
#[allow(clippy::too_many_arguments)]
async fn eval_reduce(
    accumulator: &str,
    init: &Expr,
    variable: &str,
    list: &Expr,
    expr: &Expr,
    batch: &mut VectorizedBatch,
    row_idx: usize,
    ctx: &ExecutionContext,
    scope: &HashMap<String, Value>,
    subqueries: &[PhysicalPlan],
) -> Result<Value> {
    // 1. Evaluate initial accumulator value
    let mut acc_val =
        evaluate_scalar_with_scope(init, batch, row_idx, ctx, scope, subqueries).await?;

    // 2. Evaluate list expression
    let list_val = evaluate_scalar_with_scope(list, batch, row_idx, ctx, scope, subqueries).await?;

    let items = match list_val {
        Value::Array(items) => items,
        Value::Null => return Ok(Value::Null), // Cypher: if list is null, result is null
        _ => return Err(anyhow!("REDUCE list argument must evaluate to a list")),
    };

    // 3. Iterate
    for item in items {
        let mut inner_scope = scope.clone();
        inner_scope.insert(accumulator.to_string(), acc_val.clone());
        inner_scope.insert(variable.to_string(), item.clone());

        acc_val =
            evaluate_scalar_with_scope(expr, batch, row_idx, ctx, &inner_scope, subqueries).await?;
    }

    Ok(acc_val)
}

// Helpers
#[async_recursion]
pub async fn evaluate_scalar(
    expr: &Expr,
    batch: &mut VectorizedBatch,
    row_idx: usize,
    ctx: &ExecutionContext,
    subqueries: &[PhysicalPlan],
) -> Result<Value> {
    evaluate_scalar_with_scope(expr, batch, row_idx, ctx, &HashMap::new(), subqueries).await
}

#[async_recursion]
async fn evaluate_scalar_with_scope(
    expr: &Expr,
    batch: &mut VectorizedBatch,
    row_idx: usize,
    ctx: &ExecutionContext,
    scope: &HashMap<String, Value>,
    subqueries: &[PhysicalPlan],
) -> Result<Value> {
    match expr {
        Expr::Identifier(var) => {
            // Check scope first for list comprehension variables
            if let Some(val) = scope.get(var) {
                return Ok(val.clone());
            }
            // Try extracting from Arrow column
            if let Ok(col) = batch.column(var) {
                extract_arrow_column_value(col, row_idx)
            } else {
                // Fallback to CTEs/Params
                if let Some(val) = ctx.ctes.get(var) {
                    Ok(val.clone())
                } else {
                    Err(anyhow!("Variable '{}' not found in batch or scope", var))
                }
            }
        }
        Expr::Parameter(name) => {
            if let Some(val) = ctx.ctes.get(name) {
                Ok(val.clone())
            } else {
                // If not in CTEs, maybe return Null or error?
                // Legacy executor returns Null if not found.
                Ok(Value::Null)
            }
        }
        Expr::FunctionCall { name, args, .. } => {
            let name_lower = name.to_lowercase();

            // 1. Path functions (need single-arg evaluation)
            if matches!(name_lower.as_str(), "length" | "nodes" | "relationships") {
                if args.len() != 1 {
                    return Err(anyhow!("{} requires 1 argument", name.to_uppercase()));
                }
                let val =
                    evaluate_scalar_with_scope(&args[0], batch, row_idx, ctx, scope, subqueries)
                        .await?;
                return eval_path_function(&name_lower, val);
            }

            // 2. Collection functions (need single-arg evaluation)
            if matches!(name_lower.as_str(), "size" | "keys" | "head" | "tail") {
                if args.len() != 1 {
                    return Err(anyhow!("{}() requires 1 argument", name_lower));
                }
                let val =
                    evaluate_scalar_with_scope(&args[0], batch, row_idx, ctx, scope, subqueries)
                        .await?;
                return eval_collection_function(&name_lower, val);
            }

            // 3. Short-circuit null handling functions
            if name_lower == "coalesce" {
                for arg in args {
                    let val =
                        evaluate_scalar_with_scope(arg, batch, row_idx, ctx, scope, subqueries)
                            .await?;
                    if !val.is_null() {
                        return Ok(val);
                    }
                }
                return Ok(Value::Null);
            }
            if name_lower == "nullif" {
                if args.len() != 2 {
                    return Err(anyhow!("NULLIF requires 2 arguments"));
                }
                let v1 =
                    evaluate_scalar_with_scope(&args[0], batch, row_idx, ctx, scope, subqueries)
                        .await?;
                let v2 =
                    evaluate_scalar_with_scope(&args[1], batch, row_idx, ctx, scope, subqueries)
                        .await?;
                return Ok(if v1 == v2 { Value::Null } else { v1 });
            }

            // 4. Evaluate all args for remaining functions
            let mut evaluated_args = Vec::with_capacity(args.len());
            for arg in args {
                evaluated_args.push(
                    evaluate_scalar_with_scope(arg, batch, row_idx, ctx, scope, subqueries).await?,
                );
            }

            // 5. Try CRDT functions module (handles all crdt.* functions)
            if is_crdt_function(&name_lower)
                && let Some(result) = eval_crdt_function(&name_lower, &evaluated_args)?
            {
                return Ok(result);
            }

            // 6. Fallback to expr_eval scalar functions
            eval_scalar_function(name, &evaluated_args)
                .map_err(|_| anyhow!("Function '{}' not supported", name))
        }
        Expr::Property(var_expr, prop_name) => {
            // OPTIMIZATION: Check if composed column "var.prop" exists in batch first.
            // VectorizedScan projects properties as "{variable}.{property}" columns,
            // so we can avoid expensive PropertyManager lookups by using the batch data.
            if let Expr::Identifier(var_name) = var_expr.as_ref() {
                let composed_name = format!("{}.{}", var_name, prop_name);
                if let Ok(col) = batch.column(&composed_name) {
                    // Column exists in batch - extract value directly (no I/O!)
                    return Ok(arrow_val_to_json(col.as_ref(), row_idx));
                }
            }

            // Fallback: evaluate the base expression recursively
            let base_val =
                evaluate_scalar_with_scope(var_expr, batch, row_idx, ctx, scope, subqueries)
                    .await?;

            match base_val {
                Value::Object(map) => Ok(map.get(prop_name).cloned().unwrap_or(Value::Null)),
                Value::Number(n) => {
                    // Assume it is a VID or EID - use helper for property lookup
                    if let Some(id) = n.as_u64() {
                        if prop_name == "_vid" || prop_name == "_id" {
                            return Ok(serde_json::json!(id));
                        }
                        lookup_property_from_id(id, prop_name, ctx).await
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Ok(Value::Null), // prop accessed on NULL returns NULL
            }
        }
        Expr::ArrayIndex(arr_expr, idx_expr) => {
            let arr_val =
                evaluate_scalar_with_scope(arr_expr, batch, row_idx, ctx, scope, subqueries)
                    .await?;
            let idx_val =
                evaluate_scalar_with_scope(idx_expr, batch, row_idx, ctx, scope, subqueries)
                    .await?;

            if let (Value::Array(list), Some(i)) = (arr_val, idx_val.as_u64()) {
                if (i as usize) < list.len() {
                    Ok(list[i as usize].clone())
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        // ... (Other match arms unchanged) ...
        Expr::BinaryOp { left, op, right } => {
            // Short-circuit evaluation for And/Or
            match op {
                Operator::And => {
                    return eval_short_circuit_and(
                        left, right, batch, row_idx, ctx, scope, subqueries,
                    )
                    .await;
                }
                Operator::Or => {
                    return eval_short_circuit_or(
                        left, right, batch, row_idx, ctx, scope, subqueries,
                    )
                    .await;
                }
                _ => {}
            }
            // All other operators: evaluate both sides and delegate to helper
            let l =
                evaluate_scalar_with_scope(left, batch, row_idx, ctx, scope, subqueries).await?;
            let r =
                evaluate_scalar_with_scope(right, batch, row_idx, ctx, scope, subqueries).await?;
            eval_comparison_op(op, &l, &r)
        }
        Expr::UnaryOp { op, expr } => {
            let val =
                evaluate_scalar_with_scope(expr, batch, row_idx, ctx, scope, subqueries).await?;
            match op {
                UnaryOperator::Not => {
                    let b = val.as_bool().ok_or(anyhow!("Expected bool for NOT"))?;
                    Ok(Value::Bool(!b))
                }
            }
        }
        Expr::IsNull(expr) => {
            let val =
                evaluate_scalar_with_scope(expr, batch, row_idx, ctx, scope, subqueries).await?;
            Ok(Value::Bool(val.is_null()))
        }
        Expr::IsNotNull(expr) => {
            let val =
                evaluate_scalar_with_scope(expr, batch, row_idx, ctx, scope, subqueries).await?;
            Ok(Value::Bool(!val.is_null()))
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            eval_case_expr(
                expr, when_then, else_expr, batch, row_idx, ctx, scope, subqueries,
            )
            .await
        }
        Expr::List(items) => {
            let mut vals = Vec::with_capacity(items.len());
            for item in items {
                vals.push(
                    evaluate_scalar_with_scope(item, batch, row_idx, ctx, scope, subqueries)
                        .await?,
                );
            }
            Ok(Value::Array(vals))
        }
        Expr::Map(items) => {
            let mut map = serde_json::Map::new();
            for (key, value) in items {
                map.insert(
                    key.clone(),
                    evaluate_scalar_with_scope(value, batch, row_idx, ctx, scope, subqueries)
                        .await?,
                );
            }
            Ok(Value::Object(map))
        }
        Expr::ListComprehension {
            variable,
            list,
            where_clause,
            mapping,
        } => {
            let params = ListComprehensionParams {
                variable,
                list,
                where_clause,
                mapping,
            };
            eval_list_comprehension(&params, batch, row_idx, ctx, scope, subqueries).await
        }
        Expr::Reduce {
            accumulator,
            init,
            variable,
            list,
            expr,
        } => {
            eval_reduce(
                accumulator,
                init,
                variable,
                list,
                expr,
                batch,
                row_idx,
                ctx,
                scope,
                subqueries,
            )
            .await
        }
        Expr::Literal(v) => Ok(v.clone()),
        Expr::ScalarSubquery(idx) => {
            if *idx >= subqueries.len() {
                return Err(anyhow!("Subquery index {} out of bounds", idx));
            }
            let plan = &subqueries[*idx];

            // Execute subquery for the current row
            // We need to pass the current row context.
            // VectorizedApply logic: broadcast_merge outer variables.
            let input_row = batch.slice(row_idx, 1);
            let result_batch = plan
                .execute_with_input(Some(input_row.clone()), ctx)
                .await?;

            // EXISTS semantics: return true if any rows returned
            // For general scalar subquery (future): return value
            // Since we use this for EXISTS, check if rows > 0

            // Wait, VectorizedApply handles correlated vars via broadcast_merge *inside* the operator.
            // But here we are executing the plan directly.
            // Does the plan expect input? Yes, VectorizedApply logic assumes input.
            // If the subquery starts with Scan, it ignores input unless it's correlated.
            // If it's correlated, it should have been planned with correlation.
            // The PhysicalPlanner should have handled correlation?
            // Actually, VectorizedScan doesn't currently support "Outer Reference".
            // Correlation in VectorizedApply works because subquery is fully re-executed.
            // If we pass `input_row` to `execute_with_input`, it flows down.
            // Operators inside subquery need to access it.
            // Currently, `VectorizedFilter` uses `evaluate_scalar` which uses `batch`.
            // If `batch` has the outer variables (broadcasted), it works.
            // `execute_with_input` does NOT automatically broadcast input columns to output unless specific operators do it.
            // But `VectorizedFilter` inside subquery will see the input batch.

            Ok(Value::Bool(result_batch.num_rows() > 0))
        }
        Expr::Exists(_) => Err(anyhow!(
            "EXISTS subquery not compiled (should be ScalarSubquery)"
        )),
        _ => Err(anyhow!(
            "Expr {:?} not supported in vectorized scalar eval",
            expr
        )),
    }
}

#[derive(Clone)]
pub struct VectorizedWindow {
    pub window_exprs: Vec<Expr>,
    pub subqueries: Vec<PhysicalPlan>,
}

#[async_trait]
impl VectorizedOperator for VectorizedWindow {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        ctx.check_timeout()?;
        let mut batch = input.ok_or(anyhow!("VectorizedWindow requires input"))?;

        // Compact to simplify indexing
        batch = batch.compact()?;
        let num_rows = batch.num_rows();

        for expr in &self.window_exprs {
            if let Expr::WindowFunction {
                function,
                partition_by,
                order_by,
            } = expr
            {
                // 1. Prepare Sort Columns (Partition + Order)
                let mut sort_columns = Vec::new();

                // Partition keys
                let mut partition_arrays = Vec::new();
                for p_expr in partition_by {
                    let val_array =
                        eval_expr_to_array(p_expr, &mut batch, ctx, &self.subqueries).await?;
                    partition_arrays.push(val_array.clone());
                    sort_columns.push(arrow::compute::SortColumn {
                        values: val_array,
                        options: Some(arrow::compute::SortOptions {
                            descending: false,
                            nulls_first: true,
                        }),
                    });
                }

                // Order keys
                let mut order_arrays = Vec::new();
                for (o_expr, asc) in order_by {
                    let val_array =
                        eval_expr_to_array(o_expr, &mut batch, ctx, &self.subqueries).await?;
                    order_arrays.push(val_array.clone());
                    sort_columns.push(arrow::compute::SortColumn {
                        values: val_array,
                        options: Some(arrow::compute::SortOptions {
                            descending: !(*asc), // asc=true means descending=false
                            nulls_first: true,
                        }),
                    });
                }

                // 2. Get Sorted Indices
                let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)?;

                // 3. Compute Window Values
                // We need to produce an array of size num_rows, where result[original_idx] = value
                // We iterate sorted indices. indices[i] = original_idx.

                let mut result_values = vec![Value::Null; num_rows];

                let (func_name, args) =
                    if let Expr::FunctionCall { name, args, .. } = function.as_ref() {
                        (name.to_lowercase(), args)
                    } else {
                        return Err(anyhow!("Window function must be a function call"));
                    };

                match func_name.as_str() {
                    "row_number" => {
                        let mut current_row_num = 0;
                        for i in 0..num_rows {
                            let curr_idx = indices.value(i) as usize;

                            // Check partition change
                            let mut partition_changed = false;
                            if i > 0 {
                                let prev_idx = indices.value(i - 1) as usize;
                                for arr in &partition_arrays {
                                    let v1 = arrow_val_to_json(arr.as_ref(), prev_idx);
                                    let v2 = arrow_val_to_json(arr.as_ref(), curr_idx);
                                    if v1 != v2 {
                                        partition_changed = true;
                                        break;
                                    }
                                }
                            } else {
                                partition_changed = true; // First row always starts partition
                            }

                            if partition_changed {
                                current_row_num = 1;
                            } else {
                                current_row_num += 1;
                            }

                            result_values[curr_idx] = Value::from(current_row_num);
                        }
                    }
                    "rank" | "dense_rank" => {
                        let is_dense = func_name == "dense_rank";
                        let mut current_rank = 0;
                        let mut peers = 0; // For rank

                        for i in 0..num_rows {
                            let curr_idx = indices.value(i) as usize;

                            // Check partition change
                            let mut partition_changed = false;
                            if i > 0 {
                                let prev_idx = indices.value(i - 1) as usize;
                                for arr in &partition_arrays {
                                    let v1 = arrow_val_to_json(arr.as_ref(), prev_idx);
                                    let v2 = arrow_val_to_json(arr.as_ref(), curr_idx);
                                    if v1 != v2 {
                                        partition_changed = true;
                                        break;
                                    }
                                }
                            } else {
                                partition_changed = true;
                            }

                            if partition_changed {
                                current_rank = 1;
                                peers = 0;
                            } else {
                                // Check if values changed (for rank)
                                let mut order_changed = false;
                                let prev_idx = indices.value(i - 1) as usize;
                                for arr in &order_arrays {
                                    let v1 = arrow_val_to_json(arr.as_ref(), prev_idx);
                                    let v2 = arrow_val_to_json(arr.as_ref(), curr_idx);
                                    if v1 != v2 {
                                        order_changed = true;
                                        break;
                                    }
                                }

                                if order_changed {
                                    if is_dense {
                                        current_rank += 1;
                                    } else {
                                        current_rank += 1 + peers;
                                        peers = 0;
                                    }
                                } else {
                                    // Tie
                                    if !is_dense {
                                        peers += 1;
                                    }
                                }
                            }

                            result_values[curr_idx] = Value::from(current_rank);
                        }
                    }
                    "lag" | "lead" => {
                        if args.is_empty() {
                            return Err(anyhow!("{} requires at least 1 argument", func_name));
                        }
                        // 1. Evaluate target expression for all rows (in original order)
                        let target_values =
                            eval_expr_to_array(&args[0], &mut batch, ctx, &self.subqueries).await?;

                        // 2. Parse offset (default 1)
                        let offset = if args.len() > 1 {
                            match &args[1] {
                                Expr::Literal(Value::Number(n)) => {
                                    n.as_i64().ok_or(anyhow!("Offset must be an integer"))?
                                }
                                _ => return Err(anyhow!("Offset must be an integer literal")),
                            }
                        } else {
                            1
                        };

                        // 3. Parse default value (default Null)
                        let default_val = if args.len() > 2 {
                            match &args[2] {
                                Expr::Literal(v) => v.clone(),
                                // If it's not a literal, we'd need to evaluate it.
                                // For now assume literal or simple scalar?
                                // Let's support evaluating it once if it's constant, otherwise... complex.
                                // Simplification: Expect literal or assume null.
                                _ => return Err(anyhow!("Default value must be a literal")),
                            }
                        } else {
                            Value::Null
                        };

                        let is_lag = func_name == "lag";
                        let mut partition_start = 0;

                        for i in 0..num_rows {
                            let curr_idx = indices.value(i) as usize;

                            // Check partition change
                            if i > 0 {
                                let prev_idx = indices.value(i - 1) as usize;
                                for arr in &partition_arrays {
                                    let v1 = arrow_val_to_json(arr.as_ref(), prev_idx);
                                    let v2 = arrow_val_to_json(arr.as_ref(), curr_idx);
                                    if v1 != v2 {
                                        partition_start = i;
                                        break;
                                    }
                                }
                            }

                            // Calculate source index
                            let source_pos = if is_lag {
                                i as i64 - offset
                            } else {
                                i as i64 + offset
                            };

                            if source_pos >= partition_start as i64 && source_pos < num_rows as i64
                            {
                                // Check if source_pos is still in the same partition (for LEAD especially)
                                // Actually, we only need to check if source_pos < next_partition_start?
                                // But we haven't found next partition start yet.
                                // So we need to check partition equality between i and source_pos.
                                // Optimization: For LAG, we know source_pos < i, so it is >= partition_start is enough?
                                // Yes, if source_pos >= partition_start, it is in same partition because partition is contiguous range [partition_start, i].
                                // For LEAD, source_pos > i. We need to check if it is in the same partition.

                                if is_lag {
                                    // Valid LAG
                                    let source_idx = indices.value(source_pos as usize) as usize;
                                    result_values[curr_idx] =
                                        arrow_val_to_json(target_values.as_ref(), source_idx);
                                } else {
                                    // LEAD: Check partition match
                                    let source_sorted_idx = source_pos as usize;
                                    let mut same_partition = true;

                                    // We need to compare partition keys of `curr_idx` vs `source_idx`.
                                    // Or compare `i` vs `source_pos` using sorted partition arrays?
                                    // Yes, partition_arrays are in original order, so we use indices.
                                    let source_original_idx =
                                        indices.value(source_sorted_idx) as usize;

                                    for arr in &partition_arrays {
                                        let v1 = arrow_val_to_json(arr.as_ref(), curr_idx);
                                        let v2 =
                                            arrow_val_to_json(arr.as_ref(), source_original_idx);
                                        if v1 != v2 {
                                            same_partition = false;
                                            break;
                                        }
                                    }

                                    if same_partition {
                                        result_values[curr_idx] = arrow_val_to_json(
                                            target_values.as_ref(),
                                            source_original_idx,
                                        );
                                    } else {
                                        result_values[curr_idx] = default_val.clone();
                                    }
                                }
                            } else {
                                result_values[curr_idx] = default_val.clone();
                            }
                        }
                    }
                    "first_value" | "last_value" => {
                        if args.is_empty() {
                            return Err(anyhow!("{} requires 1 argument", func_name));
                        }
                        let target_values =
                            eval_expr_to_array(&args[0], &mut batch, ctx, &self.subqueries).await?;

                        let is_first = func_name == "first_value";
                        let mut partition_start = 0;

                        for i in 0..=num_rows {
                            let mut partition_ended = false;

                            if i == num_rows {
                                partition_ended = true;
                            } else if i > 0 {
                                let curr_idx = indices.value(i) as usize;
                                let prev_idx = indices.value(i - 1) as usize;
                                for arr in &partition_arrays {
                                    let v1 = arrow_val_to_json(arr.as_ref(), prev_idx);
                                    let v2 = arrow_val_to_json(arr.as_ref(), curr_idx);
                                    if v1 != v2 {
                                        partition_ended = true;
                                        break;
                                    }
                                }
                            }

                            if partition_ended {
                                // Process partition [partition_start, i)
                                let p_end = i;
                                let val_idx = if is_first {
                                    indices.value(partition_start) as usize
                                } else {
                                    indices.value(p_end - 1) as usize
                                };
                                let val = arrow_val_to_json(target_values.as_ref(), val_idx);

                                for k in partition_start..p_end {
                                    let idx = indices.value(k) as usize;
                                    result_values[idx] = val.clone();
                                }
                                partition_start = i;
                            }
                        }
                    }
                    "percent_rank" | "cume_dist" => {
                        let is_percent_rank = func_name == "percent_rank";
                        let mut partition_start = 0;

                        for i in 0..=num_rows {
                            let mut partition_ended = false;

                            if i == num_rows {
                                partition_ended = true;
                            } else if i > 0 {
                                let curr_idx = indices.value(i) as usize;
                                let prev_idx = indices.value(i - 1) as usize;
                                for arr in &partition_arrays {
                                    let v1 = arrow_val_to_json(arr.as_ref(), prev_idx);
                                    let v2 = arrow_val_to_json(arr.as_ref(), curr_idx);
                                    if v1 != v2 {
                                        partition_ended = true;
                                        break;
                                    }
                                }
                            }

                            if partition_ended {
                                // Process partition [partition_start, i)
                                let p_end = i;
                                let partition_size = p_end - partition_start;

                                let mut group_start = partition_start;
                                for j in partition_start..=p_end {
                                    let mut group_ended = false;
                                    if j == p_end {
                                        group_ended = true;
                                    } else if j > partition_start {
                                        let curr_idx = indices.value(j) as usize;
                                        let prev_idx = indices.value(j - 1) as usize;
                                        for arr in &order_arrays {
                                            let v1 = arrow_val_to_json(arr.as_ref(), prev_idx);
                                            let v2 = arrow_val_to_json(arr.as_ref(), curr_idx);
                                            if v1 != v2 {
                                                group_ended = true;
                                                break;
                                            }
                                        }
                                    }

                                    if group_ended {
                                        // Peer group [group_start, j)
                                        let rank = group_start - partition_start + 1;
                                        let count_le = j - partition_start;

                                        let val = if is_percent_rank {
                                            if partition_size <= 1 {
                                                0.0
                                            } else {
                                                (rank as f64 - 1.0) / (partition_size as f64 - 1.0)
                                            }
                                        } else {
                                            count_le as f64 / partition_size as f64
                                        };

                                        for k in group_start..j {
                                            let idx = indices.value(k) as usize;
                                            result_values[idx] = Value::from(val);
                                        }
                                        group_start = j;
                                    }
                                }
                                partition_start = i;
                            }
                        }
                    }
                    _ => return Err(anyhow!("Unsupported window function: {}", func_name)),
                }

                // 4. Append Result Column
                let array = json_vec_to_array(&result_values);
                let col_name = expr.to_string_repr(); // Or however we want to name it
                batch.add_column(col_name, array)?;
            } else {
                return Err(anyhow!("Expected WindowFunction expression"));
            }
        }

        Ok(batch)
    }

    fn is_pipeline_breaker(&self) -> bool {
        true
    }
}

/// Helper to evaluate an expression to an Arrow Array for all rows in the batch
async fn eval_expr_to_array(
    expr: &Expr,
    batch: &mut VectorizedBatch,
    ctx: &ExecutionContext,
    subqueries: &[PhysicalPlan],
) -> Result<ArrayRef> {
    let mut values = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let val = evaluate_scalar(expr, batch, i, ctx, subqueries).await?;
        values.push(val);
    }
    Ok(json_vec_to_array(&values))
}
