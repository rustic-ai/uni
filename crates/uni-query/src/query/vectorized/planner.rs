// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::expr::{Expr, Operator};
use crate::query::planner::{LogicalPlan, QueryPlanner};
use crate::query::pushdown::{IndexAwareAnalyzer, LanceFilterGenerator};
use crate::query::vectorized::operators::{
    PhysicalPlan, VectorizedAggregate, VectorizedApply, VectorizedCrossJoin, VectorizedDelete,
    VectorizedFilter, VectorizedKnn, VectorizedLimit, VectorizedOperator, VectorizedProject,
    VectorizedRecursiveCTE, VectorizedScan, VectorizedShortestPath, VectorizedSingle,
    VectorizedSort, VectorizedTraverse, VectorizedUnion, VectorizedVariableLengthTraverse,
    VectorizedWindow,
};
use anyhow::{Result, anyhow};
use std::sync::Arc;
use uni_common::core::schema::Schema;

pub struct PhysicalPlanner {
    _schema: Arc<Schema>,
}

impl PhysicalPlanner {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { _schema: schema }
    }

    pub fn plan(&self, logical_plan: &LogicalPlan) -> Result<PhysicalPlan> {
        // First pass: collect all properties that will be accessed for each variable.
        // This allows us to project them in the scan and avoid per-row PropertyManager lookups.
        let mut all_properties: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        Self::collect_all_properties(logical_plan, &mut all_properties);

        let mut operators: Vec<Arc<dyn VectorizedOperator>> = Vec::new();
        self.plan_recursive(logical_plan, &mut operators, &all_properties)?;
        Ok(PhysicalPlan { operators })
    }

    /// Recursively collect all properties accessed for each variable in the logical plan.
    fn collect_all_properties(
        plan: &LogicalPlan,
        properties: &mut std::collections::HashMap<String, Vec<String>>,
    ) {
        match plan {
            LogicalPlan::Scan {
                variable, filter, ..
            } => {
                properties.entry(variable.clone()).or_default();
                if let Some(f) = filter {
                    Self::collect_properties_from_expr(f, properties);
                }
            }
            LogicalPlan::Filter { input, predicate } => {
                Self::collect_all_properties(input, properties);
                // Try to find the variable from the predicate
                Self::collect_properties_from_expr(predicate, properties);
            }
            LogicalPlan::Project { input, projections } => {
                Self::collect_all_properties(input, properties);
                for (expr, _) in projections {
                    Self::collect_properties_from_expr(expr, properties);
                }
            }
            LogicalPlan::Traverse {
                input,
                source_variable,
                target_variable,
                ..
            } => {
                Self::collect_all_properties(input, properties);
                // Initialize empty entries for traversal variables
                properties.entry(source_variable.clone()).or_default();
                properties.entry(target_variable.clone()).or_default();
            }
            LogicalPlan::Delete { input, items, .. } => {
                Self::collect_all_properties(input, properties);
                for expr in items {
                    Self::collect_properties_from_expr(expr, properties);
                }
            }
            LogicalPlan::Sort { input, order_by } => {
                Self::collect_all_properties(input, properties);
                for item in order_by {
                    Self::collect_properties_from_expr(&item.expr, properties);
                }
            }
            LogicalPlan::Limit { input, .. } => {
                Self::collect_all_properties(input, properties);
            }
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                Self::collect_all_properties(left, properties);
                Self::collect_all_properties(right, properties);
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                Self::collect_all_properties(input, properties);
                for expr in group_by {
                    Self::collect_properties_from_expr(expr, properties);
                }
                for expr in aggregates {
                    Self::collect_properties_from_expr(expr, properties);
                }
            }
            LogicalPlan::Window {
                input,
                window_exprs,
            } => {
                Self::collect_all_properties(input, properties);
                for expr in window_exprs {
                    Self::collect_properties_from_expr(expr, properties);
                }
            }
            LogicalPlan::Apply {
                input, subquery, ..
            } => {
                Self::collect_all_properties(input, properties);
                Self::collect_all_properties(subquery, properties);
            }
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => {
                Self::collect_all_properties(initial, properties);
                Self::collect_all_properties(recursive, properties);
            }
            LogicalPlan::VectorKnn {
                variable, property, ..
            } => {
                let entry = properties.entry(variable.clone()).or_default();
                if !entry.contains(property) {
                    entry.push(property.clone());
                }
            }
            LogicalPlan::ShortestPath { input, .. } => {
                Self::collect_all_properties(input, properties);
            }
            LogicalPlan::Empty => {}
            // Other variants (Create, Merge, Set, Remove, etc.) don't use vectorized execution
            _ => {}
        }
    }

    /// Extract properties from an expression and add them to the properties map.
    fn collect_properties_from_expr(
        expr: &Expr,
        properties: &mut std::collections::HashMap<String, Vec<String>>,
    ) {
        match expr {
            Expr::Property(var_expr, prop_name) => {
                if let Expr::Identifier(var_name) = var_expr.as_ref() {
                    let entry = properties.entry(var_name.clone()).or_default();
                    if !entry.contains(prop_name) {
                        entry.push(prop_name.clone());
                    }
                }
                Self::collect_properties_from_expr(var_expr, properties);
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_properties_from_expr(left, properties);
                Self::collect_properties_from_expr(right, properties);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                Self::collect_properties_from_expr(inner, properties);
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                Self::collect_properties_from_expr(inner, properties);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    Self::collect_properties_from_expr(arg, properties);
                }
            }
            Expr::ArrayIndex(arr, idx) => {
                Self::collect_properties_from_expr(arr, properties);
                Self::collect_properties_from_expr(idx, properties);
            }
            Expr::List(items) => {
                for item in items {
                    Self::collect_properties_from_expr(item, properties);
                }
            }
            Expr::Map(entries) => {
                for (_, value) in entries {
                    Self::collect_properties_from_expr(value, properties);
                }
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                if let Some(e) = expr {
                    Self::collect_properties_from_expr(e, properties);
                }
                for (when_e, then_e) in when_then {
                    Self::collect_properties_from_expr(when_e, properties);
                    Self::collect_properties_from_expr(then_e, properties);
                }
                if let Some(e) = else_expr {
                    Self::collect_properties_from_expr(e, properties);
                }
            }
            Expr::ListComprehension {
                list,
                where_clause,
                mapping,
                ..
            } => {
                Self::collect_properties_from_expr(list, properties);
                if let Some(w) = where_clause {
                    Self::collect_properties_from_expr(w, properties);
                }
                if let Some(m) = mapping {
                    Self::collect_properties_from_expr(m, properties);
                }
            }
            Expr::Reduce {
                init, list, expr, ..
            } => {
                Self::collect_properties_from_expr(init, properties);
                Self::collect_properties_from_expr(list, properties);
                Self::collect_properties_from_expr(expr, properties);
            }
            Expr::WindowFunction {
                function,
                partition_by,
                order_by,
            } => {
                Self::collect_properties_from_expr(function, properties);
                for expr in partition_by {
                    Self::collect_properties_from_expr(expr, properties);
                }
                for (expr, _) in order_by {
                    Self::collect_properties_from_expr(expr, properties);
                }
            }
            Expr::Exists(_) => {} // Subquery handled separately?
            Expr::ScalarSubquery(_) => {}
            Expr::Literal(_) | Expr::Identifier(_) | Expr::Parameter(_) | Expr::Wildcard => {}
        }
    }

    fn plan_recursive(
        &self,
        plan: &LogicalPlan,
        ops: &mut Vec<Arc<dyn VectorizedOperator>>,
        all_properties: &std::collections::HashMap<String, Vec<String>>,
    ) -> Result<()> {
        match plan {
            LogicalPlan::Union { left, right, all } => {
                let left_plan = self.plan(left)?;
                let right_plan = self.plan(right)?;
                ops.push(Arc::new(VectorizedUnion {
                    left: left_plan,
                    right: right_plan,
                    all: *all,
                }));
            }
            LogicalPlan::Scan {
                label_id,
                variable,
                filter,
                optional,
            } => {
                self.plan_scan(
                    *label_id,
                    variable,
                    filter.as_ref(),
                    *optional,
                    ops,
                    all_properties,
                )?;
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
                self.plan_recursive(input, ops, all_properties)?;
                use crate::query::ast::Direction as ParserDir;
                use uni_store::storage::adjacency_cache::Direction as CacheDir;
                let dir = match direction {
                    ParserDir::Outgoing => CacheDir::Outgoing,
                    ParserDir::Incoming => CacheDir::Incoming,
                    ParserDir::Both => CacheDir::Both,
                };

                if *min_hops == 1 && *max_hops == 1 {
                    ops.push(Arc::new(VectorizedTraverse {
                        edge_type_ids: edge_type_ids.clone(),
                        direction: dir,
                        source_variable: source_variable.clone(),
                        target_variable: target_variable.clone(),
                        step_variable: step_variable.clone(),
                        optional: *optional,
                        target_filter: target_filter.clone(),
                        path_variable: path_variable.clone(),
                        target_label_id: if *target_label_id == 0 {
                            None
                        } else {
                            Some(*target_label_id)
                        },
                    }));
                } else {
                    ops.push(Arc::new(VectorizedVariableLengthTraverse {
                        edge_type_ids: edge_type_ids.clone(),
                        direction: dir,
                        source_variable: source_variable.clone(),
                        target_variable: target_variable.clone(),
                        step_variable: step_variable.clone(),
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        path_variable: path_variable.clone(),
                        target_label_id: if *target_label_id == 0 {
                            None
                        } else {
                            Some(*target_label_id)
                        },
                        optional: *optional,
                    }));
                }
            }
            LogicalPlan::Apply {
                input,
                subquery,
                input_filter,
            } => {
                self.plan_recursive(input, ops, all_properties)?;

                // Handle input_filter (Subquery Predicate Pushdown)
                if let Some(pred) = input_filter {
                    check_expr_supported(pred)?;
                    let mut subqueries = Vec::new();
                    let compiled = self.compile_expr(pred, &mut subqueries)?;

                    ops.push(Arc::new(VectorizedFilter {
                        predicate: compiled,
                        subqueries,
                    }));
                }

                let sub_plan = self.plan(subquery)?;
                ops.push(Arc::new(VectorizedApply { subquery: sub_plan }));
            }
            LogicalPlan::Filter { input, predicate } => {
                // Optimization: Merge Filter into Scan if input is Scan
                if let LogicalPlan::Scan {
                    label_id,
                    variable,
                    filter: scan_filter,
                    optional,
                } = input.as_ref()
                {
                    // Combine predicates: (ScanFilter) AND (FilterPredicate)
                    let combined_filter = if let Some(sf) = scan_filter {
                        Expr::BinaryOp {
                            left: Box::new(sf.clone()),
                            op: Operator::And,
                            right: Box::new(predicate.clone()),
                        }
                    } else {
                        predicate.clone()
                    };
                    self.plan_scan(
                        *label_id,
                        variable,
                        Some(&combined_filter),
                        *optional,
                        ops,
                        all_properties,
                    )?;
                } else if let LogicalPlan::Traverse {
                    input: traverse_input,
                    edge_type_ids,
                    direction,
                    source_variable,
                    target_variable,
                    target_label_id,
                    step_variable,
                    min_hops,
                    max_hops,
                    optional,
                    target_filter: existing_target_filter,
                    path_variable,
                } = input.as_ref()
                {
                    // Push predicates that only reference source_variable through the Traverse
                    let (source_preds, other_preds) =
                        split_predicates_by_variable(predicate, source_variable, target_variable);

                    // Build modified input with source predicates pushed down
                    let modified_input = if !source_preds.is_empty() {
                        let source_filter = Self::combine_predicates(source_preds).unwrap();
                        LogicalPlan::Filter {
                            input: traverse_input.clone(),
                            predicate: source_filter,
                        }
                    } else {
                        (**traverse_input).clone()
                    };

                    // Plan the modified traverse
                    let modified_traverse = LogicalPlan::Traverse {
                        input: Box::new(modified_input),
                        edge_type_ids: edge_type_ids.clone(),
                        direction: direction.clone(),
                        source_variable: source_variable.clone(),
                        target_variable: target_variable.clone(),
                        target_label_id: *target_label_id,
                        step_variable: step_variable.clone(),
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        optional: *optional,
                        target_filter: existing_target_filter.clone(),
                        path_variable: path_variable.clone(),
                    };

                    self.plan_recursive(&modified_traverse, ops, all_properties)?;

                    // Add remaining filter if any
                    if !other_preds.is_empty() {
                        let remaining = Self::combine_predicates(other_preds).unwrap();
                        check_expr_supported(&remaining)?;

                        let mut subqueries = Vec::new();
                        let compiled = self.compile_expr(&remaining, &mut subqueries)?;

                        ops.push(Arc::new(VectorizedFilter {
                            predicate: compiled,
                            subqueries,
                        }));
                    }
                } else {
                    // Standard path
                    check_expr_supported(predicate)?;
                    self.plan_recursive(input, ops, all_properties)?;

                    let mut subqueries = Vec::new();
                    let compiled = self.compile_expr(predicate, &mut subqueries)?;

                    ops.push(Arc::new(VectorizedFilter {
                        predicate: compiled,
                        subqueries,
                    }));
                }
            }
            LogicalPlan::Project { input, projections } => {
                // Check for unsupported expressions before planning
                for (expr, _) in projections {
                    check_expr_supported(expr)?;
                }
                self.plan_recursive(input, ops, all_properties)?;

                let mut subqueries = Vec::new();
                let mut proj_vec = Vec::new();

                for (expr, alias) in projections {
                    let compiled = self.compile_expr(expr, &mut subqueries)?;
                    let name = alias.clone().unwrap_or_else(|| expr.to_string_repr());
                    proj_vec.push((name, compiled));
                }

                ops.push(Arc::new(VectorizedProject {
                    projections: proj_vec,
                    subqueries,
                }));
            }
            LogicalPlan::VectorKnn {
                label_id,
                variable,
                property,
                query,
                k,
                threshold,
            } => {
                ops.push(Arc::new(VectorizedKnn {
                    label_id: *label_id,
                    variable: variable.clone(),
                    property: property.clone(),
                    query: query.clone(),
                    k: *k,
                    threshold: *threshold,
                }));
            }
            LogicalPlan::ShortestPath {
                input,
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id,
                path_variable,
            } => {
                self.plan_recursive(input, ops, all_properties)?;
                use crate::query::ast::Direction as ParserDir;
                use uni_store::storage::adjacency_cache::Direction as CacheDir;
                let dir = match direction {
                    ParserDir::Outgoing => CacheDir::Outgoing,
                    ParserDir::Incoming => CacheDir::Incoming,
                    ParserDir::Both => CacheDir::Both,
                };

                ops.push(Arc::new(VectorizedShortestPath {
                    edge_type_ids: edge_type_ids.clone(),
                    direction: dir,
                    source_variable: source_variable.clone(),
                    target_variable: target_variable.clone(),
                    path_variable: path_variable.clone(),
                    target_label_id: *target_label_id,
                }));
            }
            LogicalPlan::Delete {
                input,
                items,
                detach,
            } => {
                self.plan_recursive(input, ops, all_properties)?;
                ops.push(Arc::new(VectorizedDelete {
                    items: items.clone(),
                    detach: *detach,
                }));
            }
            LogicalPlan::Sort { input, order_by } => {
                self.plan_recursive(input, ops, all_properties)?;
                // Convert Logical Sort Items to Vectorized Sort Entries
                let mut subqueries = Vec::new();
                let mut sort_entries = Vec::new();
                for item in order_by {
                    let compiled = self.compile_expr(&item.expr, &mut subqueries)?;
                    sort_entries.push((compiled, item.ascending));
                }
                ops.push(Arc::new(VectorizedSort {
                    sort_entries,
                    subqueries,
                }));
            }
            LogicalPlan::Limit { input, skip, fetch } => {
                self.plan_recursive(input, ops, all_properties)?;
                ops.push(Arc::new(VectorizedLimit {
                    skip: skip.unwrap_or(0),
                    limit: *fetch,
                }));
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                self.plan_recursive(input, ops, all_properties)?;

                let mut subqueries = Vec::new();
                let mut c_group_by = Vec::new();
                for expr in group_by {
                    c_group_by.push(self.compile_expr(expr, &mut subqueries)?);
                }
                let mut c_aggregates = Vec::new();
                for expr in aggregates {
                    c_aggregates.push(self.compile_expr(expr, &mut subqueries)?);
                }

                ops.push(Arc::new(VectorizedAggregate {
                    group_by: c_group_by,
                    aggregates: c_aggregates,
                    subqueries,
                }));
            }
            LogicalPlan::Window {
                input,
                window_exprs,
            } => {
                self.plan_recursive(input, ops, all_properties)?;
                ops.push(Arc::new(VectorizedWindow {
                    window_exprs: window_exprs.clone(),
                    subqueries: vec![],
                }));
            }
            LogicalPlan::CrossJoin { left, right } => {
                let left_plan = self.plan(left)?;
                let right_plan = self.plan(right)?;
                ops.push(Arc::new(VectorizedCrossJoin {
                    left: left_plan,
                    right: right_plan,
                }));
            }
            LogicalPlan::RecursiveCTE {
                cte_name,
                initial,
                recursive,
            } => {
                let initial_plan = self.plan(initial)?;
                let recursive_plan = self.plan(recursive)?;
                ops.push(Arc::new(VectorizedRecursiveCTE {
                    cte_name: cte_name.clone(),
                    initial: initial_plan,
                    recursive: recursive_plan,
                }));
            }
            LogicalPlan::Empty => {
                ops.push(Arc::new(VectorizedSingle {}));
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported logical plan for vectorization: {:?}",
                    plan
                ));
            }
        }
        Ok(())
    }

    fn plan_scan(
        &self,
        label_id: u16,
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        ops: &mut Vec<Arc<dyn VectorizedOperator>>,
        all_properties: &std::collections::HashMap<String, Vec<String>>,
    ) -> Result<()> {
        // Two-phase scan with MVCC-aware filtering and index-aware pushdown:
        // 1. IndexAwareAnalyzer categorizes predicates by optimal execution path
        // 2. UID lookups and JsonPath index lookups narrow candidates before Lance scan
        // 3. Lance filter is applied during scan
        // 4. Residual predicates applied after MVCC resolution

        let (
            pushed_filter,
            pushed_predicates,
            _residual,
            uid_lookup,
            jsonpath_lookups,
            json_fts_predicates,
        ) = if let Some(f) = filter {
            // Use IndexAwareAnalyzer for index-aware pushdown
            let index_analyzer = IndexAwareAnalyzer::new(&self._schema);
            let strategy = index_analyzer.analyze(f, variable, label_id);

            // Generate Lance filter from lance_predicates
            let lance_filter = LanceFilterGenerator::generate(&strategy.lance_predicates, variable);
            let pushed_predicates = strategy.lance_predicates.clone();

            // Residual predicates need to be applied after MVCC resolution
            let residual = if !strategy.residual.is_empty() {
                Some(Self::combine_predicates(strategy.residual).unwrap())
            } else {
                None
            };

            (
                lance_filter,
                pushed_predicates,
                residual,
                strategy.uid_lookup,
                strategy.jsonpath_lookups,
                strategy.json_fts_predicates,
            )
        } else {
            (None, vec![], None, None, vec![], vec![])
        };

        // Use pre-collected properties for this variable (from filter + projections).
        // This allows VectorizedFilter and VectorizedProject to use batch columns
        // instead of expensive per-row PropertyManager lookups.
        let project_columns = all_properties.get(variable).cloned().unwrap_or_default();

        let (residual_filter, subqueries) = if let Some(f) = filter {
            let mut subqueries = Vec::new();
            let compiled = self.compile_expr(f, &mut subqueries)?;
            (Some(compiled), subqueries)
        } else {
            (None, Vec::new())
        };

        ops.push(Arc::new(VectorizedScan {
            label_id,
            variable: variable.to_string(),
            pushed_filter,
            pushed_predicates,
            residual_filter,
            subqueries,
            project_columns,
            optional,
            uid_lookup,
            jsonpath_lookups,
            json_fts_predicates,
        }));

        Ok(())
    }

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

    /// Recursively compile subqueries within an expression.
    fn compile_expr(&self, expr: &Expr, subqueries: &mut Vec<PhysicalPlan>) -> Result<Expr> {
        match expr {
            Expr::Exists(query_box) => {
                let planner = QueryPlanner::new(self._schema.clone());
                let logical_plan = planner.plan(*query_box.clone())?;
                let physical_plan = self.plan(&logical_plan)?;
                let idx = subqueries.len();
                subqueries.push(physical_plan);
                Ok(Expr::ScalarSubquery(idx))
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.compile_expr(left, subqueries)?),
                op: op.clone(),
                right: Box::new(self.compile_expr(right, subqueries)?),
            }),
            Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.compile_expr(expr, subqueries)?),
            }),
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let mut compiled_args = Vec::new();
                for arg in args {
                    compiled_args.push(self.compile_expr(arg, subqueries)?);
                }
                Ok(Expr::FunctionCall {
                    name: name.clone(),
                    args: compiled_args,
                    distinct: *distinct,
                })
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                let c_expr = if let Some(e) = expr {
                    Some(Box::new(self.compile_expr(e, subqueries)?))
                } else {
                    None
                };
                let mut c_when_then = Vec::new();
                for (w, t) in when_then {
                    c_when_then.push((
                        self.compile_expr(w, subqueries)?,
                        self.compile_expr(t, subqueries)?,
                    ));
                }
                let c_else = if let Some(e) = else_expr {
                    Some(Box::new(self.compile_expr(e, subqueries)?))
                } else {
                    None
                };
                Ok(Expr::Case {
                    expr: c_expr,
                    when_then: c_when_then,
                    else_expr: c_else,
                })
            }
            Expr::List(items) => {
                let mut c_items = Vec::new();
                for item in items {
                    c_items.push(self.compile_expr(item, subqueries)?);
                }
                Ok(Expr::List(c_items))
            }
            Expr::Map(items) => {
                let mut c_items = Vec::new();
                for (k, v) in items {
                    c_items.push((k.clone(), self.compile_expr(v, subqueries)?));
                }
                Ok(Expr::Map(c_items))
            }
            Expr::Property(expr, prop) => Ok(Expr::Property(
                Box::new(self.compile_expr(expr, subqueries)?),
                prop.clone(),
            )),
            Expr::ArrayIndex(arr, idx) => Ok(Expr::ArrayIndex(
                Box::new(self.compile_expr(arr, subqueries)?),
                Box::new(self.compile_expr(idx, subqueries)?),
            )),
            Expr::IsNull(expr) => Ok(Expr::IsNull(Box::new(self.compile_expr(expr, subqueries)?))),
            Expr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                self.compile_expr(expr, subqueries)?,
            ))),
            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                mapping,
            } => Ok(Expr::ListComprehension {
                variable: variable.clone(),
                list: Box::new(self.compile_expr(list, subqueries)?),
                where_clause: if let Some(w) = where_clause {
                    Some(Box::new(self.compile_expr(w, subqueries)?))
                } else {
                    None
                },
                mapping: if let Some(m) = mapping {
                    Some(Box::new(self.compile_expr(m, subqueries)?))
                } else {
                    None
                },
            }),
            _ => Ok(expr.clone()),
        }
    }
}

/// Split predicates by which variables they reference.
/// Returns (source_only_predicates, other_predicates).
fn split_predicates_by_variable(
    predicate: &Expr,
    source_var: &str,
    target_var: &str,
) -> (Vec<Expr>, Vec<Expr>) {
    let mut source_only = Vec::new();
    let mut other = Vec::new();

    // Split AND-connected predicates
    let conjuncts = split_conjuncts(predicate);

    for conj in conjuncts {
        let vars = collect_variables(&conj);
        let refs_source = vars.contains(source_var);
        let refs_target = vars.contains(target_var);

        if refs_source && !refs_target {
            // Only references source variable - can be pushed down
            source_only.push(conj);
        } else {
            // References target or both - must stay after traverse
            other.push(conj);
        }
    }

    (source_only, other)
}

/// Split AND-connected expressions into a list of conjuncts.
fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
    let mut result = Vec::new();
    split_conjuncts_impl(expr, &mut result);
    result
}

fn split_conjuncts_impl(expr: &Expr, result: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: Operator::And,
            right,
        } => {
            split_conjuncts_impl(left, result);
            split_conjuncts_impl(right, result);
        }
        _ => {
            result.push(expr.clone());
        }
    }
}

/// Collect all variable names referenced in an expression.
fn collect_variables(expr: &Expr) -> std::collections::HashSet<String> {
    let mut vars = std::collections::HashSet::new();
    collect_variables_impl(expr, &mut vars);
    vars
}

fn collect_variables_impl(expr: &Expr, vars: &mut std::collections::HashSet<String>) {
    match expr {
        Expr::Identifier(name) => {
            vars.insert(name.clone());
        }
        Expr::Property(inner, _) => {
            // For Property(Identifier(var), prop), extract var
            if let Expr::Identifier(name) = inner.as_ref() {
                vars.insert(name.clone());
            } else {
                collect_variables_impl(inner, vars);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_variables_impl(left, vars);
            collect_variables_impl(right, vars);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_variables_impl(expr, vars);
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            collect_variables_impl(expr, vars);
        }
        Expr::List(items) => {
            for item in items {
                collect_variables_impl(item, vars);
            }
        }
        Expr::Map(items) => {
            for (_, item) in items {
                collect_variables_impl(item, vars);
            }
        }
        Expr::Exists(_) => {
            // TODO: Collect vars from subquery?
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_variables_impl(arg, vars);
            }
        }
        Expr::ArrayIndex(arr, idx) => {
            collect_variables_impl(arr, vars);
            collect_variables_impl(idx, vars);
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = expr {
                collect_variables_impl(e, vars);
            }
            for (w, t) in when_then {
                collect_variables_impl(w, vars);
                collect_variables_impl(t, vars);
            }
            if let Some(e) = else_expr {
                collect_variables_impl(e, vars);
            }
        }
        Expr::ListComprehension {
            variable,
            list,
            where_clause,
            mapping,
        } => {
            collect_variables_impl(list, vars);

            // For where and mapping, remove 'variable' from collected set as it is bound locally
            let mut inner_vars = std::collections::HashSet::new();
            if let Some(w) = where_clause {
                collect_variables_impl(w, &mut inner_vars);
            }
            if let Some(m) = mapping {
                collect_variables_impl(m, &mut inner_vars);
            }
            inner_vars.remove(variable);
            vars.extend(inner_vars);
        }
        Expr::Reduce {
            accumulator,
            init,
            variable,
            list,
            expr,
        } => {
            collect_variables_impl(init, vars);
            collect_variables_impl(list, vars);

            let mut inner_vars = std::collections::HashSet::new();
            collect_variables_impl(expr, &mut inner_vars);
            inner_vars.remove(accumulator);
            inner_vars.remove(variable);
            vars.extend(inner_vars);
        }
        Expr::WindowFunction {
            function,
            partition_by,
            order_by,
        } => {
            collect_variables_impl(function, vars);
            for expr in partition_by {
                collect_variables_impl(expr, vars);
            }
            for (expr, _) in order_by {
                collect_variables_impl(expr, vars);
            }
        }
        Expr::Literal(_) | Expr::Parameter(_) | Expr::Wildcard | Expr::ScalarSubquery(_) => {}
    }
}

/// Check if an expression is supported for vectorized execution.
/// Returns Err if the expression contains unsupported constructs.
fn check_expr_supported(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Identifier(_) => Ok(()),
        Expr::Property(inner, _) => check_expr_supported(inner),
        Expr::Literal(_) => Ok(()),
        Expr::BinaryOp { left, right, .. } => {
            check_expr_supported(left)?;
            check_expr_supported(right)
        }
        Expr::UnaryOp { expr, .. } => check_expr_supported(expr),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => check_expr_supported(expr),
        Expr::FunctionCall { name, .. } => match name.as_str() {
            "LENGTH" | "NODES" | "RELATIONSHIPS" | "COALESCE" | "NULLIF" | "COUNT" | "SUM"
            | "AVG" | "MIN" | "MAX" | "COLLECT" | "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE"
            | "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" => Ok(()),
            _ => Err(anyhow!(
                "Function '{}' not supported in vectorized execution",
                name
            )),
        },
        Expr::List(items) => {
            for item in items {
                check_expr_supported(item)?;
            }
            Ok(())
        }
        Expr::Map(items) => {
            for (_, item) in items {
                check_expr_supported(item)?;
            }
            Ok(())
        }
        Expr::Exists(_) => Ok(()), // Will be compiled
        Expr::ScalarSubquery(_) => Ok(()),
        Expr::ArrayIndex(arr, idx) => {
            check_expr_supported(arr)?;
            check_expr_supported(idx)
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = expr {
                check_expr_supported(e)?;
            }
            for (w, t) in when_then {
                check_expr_supported(w)?;
                check_expr_supported(t)?;
            }
            if let Some(e) = else_expr {
                check_expr_supported(e)?;
            }
            Ok(())
        }
        Expr::ListComprehension {
            list,
            where_clause,
            mapping,
            ..
        } => {
            check_expr_supported(list)?;
            if let Some(w) = where_clause {
                check_expr_supported(w)?;
            }
            if let Some(m) = mapping {
                check_expr_supported(m)?;
            }
            Ok(())
        }
        Expr::Reduce {
            init, list, expr, ..
        } => {
            check_expr_supported(init)?;
            check_expr_supported(list)?;
            check_expr_supported(expr)?;
            Ok(())
        }
        Expr::WindowFunction {
            function,
            partition_by,
            order_by,
        } => {
            check_expr_supported(function)?;
            for expr in partition_by {
                check_expr_supported(expr)?;
            }
            for (expr, _) in order_by {
                check_expr_supported(expr)?;
            }
            Ok(())
        }
        Expr::Parameter(_) | Expr::Wildcard => Ok(()),
    }
}
