// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::expr::Expr;
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;
use tokio::sync::RwLock;
use uni_algo::algo::AlgorithmRegistry;
use uni_store::QueryContext;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::l0_manager::L0Manager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

use crate::query::vectorized::operators::{ExecutionContext, VectorizedOperator};
use crate::query::vectorized::planner::PhysicalPlanner;
use crate::query::vectorized::profiling::ProfilingOperator;
use parking_lot::Mutex;

/// Helper struct for extracting L0-related components from a `QueryContext`.
///
/// This reduces duplication when building `ExecutionContext` instances.
pub(crate) struct L0Components {
    pub(crate) l0: Option<Arc<parking_lot::RwLock<L0Buffer>>>,
    pub(crate) transaction_l0: Option<Arc<parking_lot::RwLock<L0Buffer>>>,
    pub(crate) pending_flush_l0s: Vec<Arc<parking_lot::RwLock<L0Buffer>>>,
}

impl L0Components {
    /// Extracts L0 components from an optional `QueryContext`.
    pub(crate) fn from_query_context(ctx: &Option<QueryContext>) -> Self {
        Self {
            l0: ctx.as_ref().map(|c| c.l0.clone()),
            transaction_l0: ctx.as_ref().and_then(|c| c.transaction_l0.clone()),
            pending_flush_l0s: ctx
                .as_ref()
                .map(|c| c.pending_flush_l0s.clone())
                .unwrap_or_default(),
        }
    }
}

// ... Accumulator struct and impl ...
pub(crate) enum Accumulator {
    Count(i64),
    Sum(f64),
    Collect(Vec<Value>),
    CountDistinct(HashSet<String>),
}

impl Accumulator {
    pub(crate) fn new(op: &str, distinct: bool) -> Self {
        match op {
            "COUNT" => {
                if distinct {
                    Accumulator::CountDistinct(HashSet::new())
                } else {
                    Accumulator::Count(0)
                }
            }
            "SUM" => Accumulator::Sum(0.0),
            "COLLECT" => Accumulator::Collect(Vec::new()),
            _ => Accumulator::Count(0),
        }
    }

    pub(crate) fn update(&mut self, val: &Value, is_wildcard: bool) {
        match self {
            Accumulator::Count(c) => {
                if is_wildcard || !val.is_null() {
                    *c += 1;
                }
            }
            Accumulator::Sum(s) => {
                if let Some(f) = val.as_f64() {
                    *s += f;
                } else if let Some(i) = val.as_i64() {
                    *s += i as f64;
                }
            }
            Accumulator::Collect(v) => {
                if !val.is_null() {
                    v.push(val.clone());
                }
            }
            Accumulator::CountDistinct(s) => {
                if !val.is_null() {
                    s.insert(val.to_string());
                }
            }
        }
    }

    pub(crate) fn finish(&self) -> Value {
        match self {
            Accumulator::Count(c) => json!(*c),
            Accumulator::Sum(s) => json!(*s),
            Accumulator::Collect(v) => Value::Array(v.clone()),
            Accumulator::CountDistinct(s) => json!(s.len()),
        }
    }
}

/// Cache key for parsed generation expressions: (label_name, property_name)
pub(crate) type GenExprCacheKey = (String, String);

#[derive(Clone)]
pub struct Executor {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) writer: Option<Arc<RwLock<Writer>>>,
    pub(crate) l0_manager: Option<Arc<L0Manager>>,
    pub(crate) algo_registry: Arc<AlgorithmRegistry>,
    pub(crate) use_transaction: bool,
    /// File sandbox configuration for BACKUP/COPY/EXPORT commands
    pub(crate) file_sandbox: uni_common::config::FileSandboxConfig,
    pub(crate) config: uni_common::config::UniConfig,
    /// Cache for parsed generation expressions to avoid re-parsing on every row
    pub(crate) gen_expr_cache: Arc<RwLock<HashMap<GenExprCacheKey, Expr>>>,
}

impl Executor {
    pub fn new(storage: Arc<StorageManager>) -> Self {
        Self {
            storage,
            writer: None,
            l0_manager: None,
            algo_registry: Arc::new(AlgorithmRegistry::new()),
            use_transaction: false,
            file_sandbox: uni_common::config::FileSandboxConfig::default(),
            config: uni_common::config::UniConfig::default(),
            gen_expr_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn new_with_writer(storage: Arc<StorageManager>, writer: Arc<RwLock<Writer>>) -> Self {
        Self {
            storage,
            writer: Some(writer),
            l0_manager: None,
            algo_registry: Arc::new(AlgorithmRegistry::new()),
            use_transaction: false,
            file_sandbox: uni_common::config::FileSandboxConfig::default(),
            config: uni_common::config::UniConfig::default(),
            gen_expr_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set the file sandbox configuration for BACKUP/COPY/EXPORT commands.
    /// MUST be called with sandboxed config in server mode.
    pub fn set_file_sandbox(&mut self, sandbox: uni_common::config::FileSandboxConfig) {
        self.file_sandbox = sandbox;
    }

    pub fn set_config(&mut self, config: uni_common::config::UniConfig) {
        self.config = config;
    }

    /// Validate a file path against the sandbox configuration.
    pub(crate) fn validate_path(&self, path: &str) -> Result<std::path::PathBuf> {
        self.file_sandbox
            .validate_path(path)
            .map_err(|e| anyhow!("Path validation failed: {}", e))
    }

    pub fn set_writer(&mut self, writer: Arc<RwLock<Writer>>) {
        self.writer = Some(writer);
    }

    pub fn set_use_transaction(&mut self, use_transaction: bool) {
        self.use_transaction = use_transaction;
    }

    pub(crate) async fn get_context(&self) -> Option<QueryContext> {
        if let Some(writer_lock) = &self.writer {
            let writer = writer_lock.read().await;
            // Include pending_flush L0s so data being flushed remains visible
            let mut ctx = QueryContext::new_with_pending(
                writer.l0_manager.get_current(),
                writer.transaction_l0.clone(),
                writer.l0_manager.get_pending_flush(),
            );
            ctx.set_deadline(Instant::now() + self.config.query_timeout);
            Some(ctx)
        } else {
            self.l0_manager.as_ref().map(|m| {
                let mut ctx = QueryContext::new(m.get_current());
                ctx.set_deadline(Instant::now() + self.config.query_timeout);
                ctx
            })
        }
    }

    pub(crate) fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Number(n1), Value::Number(n2)) => {
                if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                    f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    std::cmp::Ordering::Equal
                }
            }
            (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
            (Value::Bool(b1), Value::Bool(b2)) => b1.cmp(b2),
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileOutput {
    pub explain: crate::query::planner::ExplainOutput,
    pub runtime_stats: Vec<OperatorStats>,
    pub total_time_ms: u64,
    pub peak_memory_bytes: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OperatorStats {
    pub operator: String,
    pub actual_rows: usize,
    pub time_ms: f64,
    pub memory_bytes: usize,
    pub index_hits: Option<usize>,
    pub index_misses: Option<usize>,
}

impl Executor {
    pub async fn profile(
        &self,
        plan: crate::query::planner::LogicalPlan,
        params: &HashMap<String, Value>,
    ) -> Result<(Vec<HashMap<String, Value>>, ProfileOutput)> {
        // Generate ExplainOutput first
        let planner =
            crate::query::planner::QueryPlanner::new(self.storage.schema_manager().schema().into());
        let explain_output = planner.explain_logical_plan(&plan)?;

        let start = Instant::now();

        // Try to profile using vectorized engine
        let phys_planner =
            PhysicalPlanner::new(Arc::new(self.storage.schema_manager().schema().clone()));

        // If planning fails, fallback to legacy execute but we won't get granular stats
        let (results, stats) = match phys_planner.plan(&plan) {
            Ok(mut phys_plan) => {
                let mut op_stats_handles = Vec::new();
                let mut wrapped_ops: Vec<Arc<dyn VectorizedOperator>> = Vec::new();

                for op in phys_plan.operators {
                    let name = op.name();
                    let stats = Arc::new(Mutex::new(OperatorStats {
                        operator: name.clone(),
                        actual_rows: 0,
                        time_ms: 0.0,
                        memory_bytes: 0,
                        index_hits: None,
                        index_misses: None,
                    }));

                    op_stats_handles.push(stats.clone());
                    wrapped_ops.push(Arc::new(ProfilingOperator {
                        inner: op,
                        name,
                        stats,
                    }));
                }
                phys_plan.operators = wrapped_ops;

                // Execute
                let ctx = self.get_context().await;
                let l0_comp = L0Components::from_query_context(&ctx);
                let prop_manager = Arc::new(self.create_prop_manager());

                let exec_ctx = ExecutionContext {
                    storage: self.storage.clone(),
                    property_manager: prop_manager,
                    l0: l0_comp.l0,
                    transaction_l0: l0_comp.transaction_l0,
                    pending_flush_l0s: l0_comp.pending_flush_l0s,
                    writer: self.writer.clone(),
                    config: self.config.clone(),
                    deadline: Some(Instant::now() + self.config.query_timeout),
                    current_memory: Arc::new(AtomicUsize::new(0)),
                    ctes: params.clone(),
                };

                let batch_res = phys_plan.execute(&exec_ctx).await;
                let results = match batch_res {
                    Ok(batch) => self.batch_to_rows(batch)?,
                    Err(e) => return Err(e),
                };

                // Collect stats
                let mut final_stats = Vec::new();
                for handle in op_stats_handles {
                    final_stats.push(handle.lock().clone());
                }
                (results, final_stats)
            }
            Err(_) => {
                // Fallback to legacy
                let prop_manager = self.create_prop_manager();
                let results = self.execute(plan.clone(), &prop_manager, params).await?;
                (
                    results.clone(),
                    vec![OperatorStats {
                        operator: "Legacy Execution (No granular profile)".to_string(),
                        actual_rows: results.len(),
                        time_ms: start.elapsed().as_secs_f64() * 1000.0,
                        memory_bytes: 0,
                        index_hits: None,
                        index_misses: None,
                    }],
                )
            }
        };

        let total_time = start.elapsed();

        Ok((
            results,
            ProfileOutput {
                explain: explain_output,
                runtime_stats: stats,
                total_time_ms: total_time.as_millis() as u64,
                peak_memory_bytes: 0,
            },
        ))
    }

    fn create_prop_manager(&self) -> uni_store::runtime::property_manager::PropertyManager {
        uni_store::runtime::property_manager::PropertyManager::new(
            self.storage.clone(),
            self.storage.schema_manager_arc(),
            1000,
        )
    }
}
