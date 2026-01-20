// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::ast::{
    AlterEdgeTypeClause, AlterLabelClause, Clause, CreateConstraintClause, CreateEdgeTypeClause,
    CreateLabelClause, CypherQuery, Direction, DropConstraintClause, DropEdgeTypeClause,
    DropLabelClause, Pattern, PatternPart, Query, RemoveItem, ReturnItem, SetClause, SetItem,
    ShowConstraintsClause, SortItem,
};
use crate::query::expr::{Expr, Operator};
use crate::query::pushdown::PredicateAnalyzer;
use crate::query::subsumption::check_subsumption;
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::schema::{
    DistanceMetric, EmbeddingConfig, EmbeddingModel, FullTextIndexConfig, IndexDefinition,
    JsonFtsIndexConfig, ScalarIndexConfig, ScalarIndexType, Schema, TokenizerConfig,
    VectorIndexConfig, VectorIndexType,
};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
    },
    Scan {
        label_id: u16,
        variable: String,
        filter: Option<Expr>,
        optional: bool,
    },
    Empty, // Produces 1 empty row
    Unwind {
        input: Box<LogicalPlan>,
        expr: Expr,
        variable: String,
    },
    Traverse {
        input: Box<LogicalPlan>,
        edge_type_ids: Vec<u16>,
        direction: Direction,
        source_variable: String,
        target_variable: String,
        target_label_id: u16,
        step_variable: Option<String>,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        target_filter: Option<Expr>,
        path_variable: Option<String>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Create {
        input: Box<LogicalPlan>,
        pattern: Pattern,
    },
    Merge {
        input: Box<LogicalPlan>,
        pattern: Pattern,
        on_match: Option<SetClause>,
        on_create: Option<SetClause>,
    },
    Set {
        input: Box<LogicalPlan>,
        items: Vec<SetItem>,
    },
    Remove {
        input: Box<LogicalPlan>,
        items: Vec<RemoveItem>,
    },
    Delete {
        input: Box<LogicalPlan>,
        items: Vec<Expr>,
        detach: bool,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<SortItem>,
    },
    Limit {
        input: Box<LogicalPlan>,
        skip: Option<usize>,
        fetch: Option<usize>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
    },
    Window {
        input: Box<LogicalPlan>,
        window_exprs: Vec<Expr>,
    },
    Project {
        input: Box<LogicalPlan>,
        projections: Vec<(Expr, Option<String>)>,
    },
    CrossJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    Apply {
        input: Box<LogicalPlan>,
        subquery: Box<LogicalPlan>,
        input_filter: Option<Expr>,
    },
    RecursiveCTE {
        cte_name: String,
        initial: Box<LogicalPlan>,
        recursive: Box<LogicalPlan>,
    },
    ProcedureCall {
        procedure_name: String,
        arguments: Vec<Expr>,
        yield_items: Vec<(String, Option<String>)>,
    },
    VectorKnn {
        label_id: u16,
        variable: String,
        property: String,
        query: Expr,
        k: usize,
        threshold: Option<f32>,
    },
    InvertedIndexLookup {
        label_id: u16,
        variable: String,
        property: String,
        terms: Expr,
    },
    ShortestPath {
        input: Box<LogicalPlan>,
        edge_type_ids: Vec<u16>,
        direction: Direction,
        source_variable: String,
        target_variable: String,
        target_label_id: u16,
        path_variable: String,
        /// Minimum number of hops (edges) in the path. Default is 1.
        min_hops: u32,
        /// Maximum number of hops (edges) in the path. Default is u32::MAX (unlimited).
        max_hops: u32,
    },
    // DDL Plans
    CreateVectorIndex {
        config: VectorIndexConfig,
        if_not_exists: bool,
    },
    CreateFullTextIndex {
        config: FullTextIndexConfig,
        if_not_exists: bool,
    },
    CreateScalarIndex {
        config: ScalarIndexConfig,
        if_not_exists: bool,
    },
    CreateJsonFtsIndex {
        config: JsonFtsIndexConfig,
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    ShowIndexes {
        filter: Option<String>,
    },
    Copy {
        target: String,
        source: String,
        is_export: bool,
        options: HashMap<String, Value>,
    },
    Backup {
        destination: String,
        options: HashMap<String, Value>,
    },
    Explain {
        plan: Box<LogicalPlan>,
    },
    // Admin Plans
    ShowDatabase,
    ShowConfig,
    ShowStatistics,
    Vacuum,
    Checkpoint,
    // Schema DDL
    CreateLabel(CreateLabelClause),
    CreateEdgeType(CreateEdgeTypeClause),
    AlterLabel(AlterLabelClause),
    AlterEdgeType(AlterEdgeTypeClause),
    DropLabel(DropLabelClause),
    DropEdgeType(DropEdgeTypeClause),
    // Constraints
    CreateConstraint(CreateConstraintClause),
    DropConstraint(DropConstraintClause),
    ShowConstraints(ShowConstraintsClause),
    // Transaction Plans
    Begin,
    Commit,
    Rollback,
}

/// Result of extracting ANY IN predicate
struct AnyInExtraction {
    predicate: AnyInPredicate,
    residual: Option<Expr>,
}

struct AnyInPredicate {
    variable: String,
    property: String,
    terms: Expr,
}

fn extract_any_in_predicate(expr: &Expr) -> Option<AnyInExtraction> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, Operator::And) {
                if let Some(mut extraction) = extract_any_in_predicate(left) {
                    extraction.residual = Some(combine_with_and(
                        extraction.residual,
                        right.as_ref().clone(),
                    ));
                    return Some(extraction);
                }
                if let Some(mut extraction) = extract_any_in_predicate(right) {
                    extraction.residual =
                        Some(combine_with_and(extraction.residual, left.as_ref().clone()));
                    return Some(extraction);
                }
                return None;
            }
            // Check direct match
            if let Some(pred) = extract_simple_any_in(expr) {
                return Some(AnyInExtraction {
                    predicate: pred,
                    residual: None,
                });
            }
            None
        }
        _ => {
            if let Some(pred) = extract_simple_any_in(expr) {
                return Some(AnyInExtraction {
                    predicate: pred,
                    residual: None,
                });
            }
            None
        }
    }
}

fn extract_simple_any_in(expr: &Expr) -> Option<AnyInPredicate> {
    // ANY(x IN n.tags WHERE x IN ['a', 'b'])
    // Parsed as FunctionCall("ANY", [ListComprehension])
    if let Expr::FunctionCall { name, args, .. } = expr
        && name.eq_ignore_ascii_case("ANY")
        && args.len() == 1
        && let Expr::ListComprehension {
            variable: loop_var,
            list,
            where_clause,
            mapping: _,
        } = &args[0]
    {
        // Check list is property access: n.tags
        if let Expr::Property(var_expr, prop) = list.as_ref()
            && let Expr::Identifier(node_var) = var_expr.as_ref()
        {
            // Check where clause: x IN [...]
            if let Some(where_expr) = where_clause
                && let Expr::BinaryOp {
                    left,
                    op: Operator::In,
                    right,
                } = where_expr.as_ref()
                && let Expr::Identifier(v) = left.as_ref()
                && v == loop_var
            {
                return Some(AnyInPredicate {
                    variable: node_var.clone(),
                    property: prop.clone(),
                    terms: right.as_ref().clone(),
                });
            }
        }
    }
    None
}

/// Extracted vector similarity predicate info for optimization
struct VectorSimilarityPredicate {
    variable: String,
    property: String,
    query: Expr,
    threshold: Option<f32>,
}

/// Result of extracting vector_similarity from a predicate
struct VectorSimilarityExtraction {
    /// The extracted vector similarity predicate
    predicate: VectorSimilarityPredicate,
    /// Remaining predicates that couldn't be optimized (if any)
    residual: Option<Expr>,
}

/// Try to extract a vector_similarity predicate from an expression.
/// Matches patterns like:
/// - vector_similarity(n.embedding, [1,2,3]) > 0.8
/// - n.embedding ~= $query
///
/// Also handles AND predicates.
fn extract_vector_similarity(expr: &Expr) -> Option<VectorSimilarityExtraction> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Handle AND: check both sides for vector_similarity
            if matches!(op, Operator::And) {
                // Try left side first
                if let Some(vs) = extract_simple_vector_similarity(left) {
                    return Some(VectorSimilarityExtraction {
                        predicate: vs,
                        residual: Some(right.as_ref().clone()),
                    });
                }
                // Try right side
                if let Some(vs) = extract_simple_vector_similarity(right) {
                    return Some(VectorSimilarityExtraction {
                        predicate: vs,
                        residual: Some(left.as_ref().clone()),
                    });
                }
                // Recursively check within left/right for nested ANDs
                if let Some(mut extraction) = extract_vector_similarity(left) {
                    extraction.residual = Some(combine_with_and(
                        extraction.residual,
                        right.as_ref().clone(),
                    ));
                    return Some(extraction);
                }
                if let Some(mut extraction) = extract_vector_similarity(right) {
                    extraction.residual =
                        Some(combine_with_and(extraction.residual, left.as_ref().clone()));
                    return Some(extraction);
                }
                return None;
            }

            // Simple case: direct vector_similarity comparison
            if let Some(vs) = extract_simple_vector_similarity(expr) {
                return Some(VectorSimilarityExtraction {
                    predicate: vs,
                    residual: None,
                });
            }
            None
        }
        _ => None,
    }
}

/// Helper to combine an optional expression with another using AND
fn combine_with_and(opt_expr: Option<Expr>, other: Expr) -> Expr {
    match opt_expr {
        Some(e) => Expr::BinaryOp {
            left: Box::new(e),
            op: Operator::And,
            right: Box::new(other),
        },
        None => other,
    }
}

/// Extract a simple vector_similarity comparison (no AND)
fn extract_simple_vector_similarity(expr: &Expr) -> Option<VectorSimilarityPredicate> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Pattern: n.embedding ~= query
            if matches!(op, Operator::ApproxEq)
                && let Expr::Property(var_expr, prop) = left.as_ref()
                && let Expr::Identifier(var) = var_expr.as_ref()
            {
                return Some(VectorSimilarityPredicate {
                    variable: var.clone(),
                    property: prop.clone(),
                    query: right.as_ref().clone(),
                    threshold: None,
                });
            }

            // Pattern: vector_similarity(...) > threshold or vector_similarity(...) >= threshold
            if matches!(op, Operator::Gt | Operator::GtEq)
                && let (Some(vs), Some(thresh)) = (
                    extract_vector_similarity_call(left),
                    extract_float_literal(right),
                )
            {
                return Some(VectorSimilarityPredicate {
                    variable: vs.0,
                    property: vs.1,
                    query: vs.2,
                    threshold: Some(thresh),
                });
            }
            // Pattern: threshold < vector_similarity(...) or threshold <= vector_similarity(...)
            if matches!(op, Operator::Lt | Operator::LtEq)
                && let (Some(thresh), Some(vs)) = (
                    extract_float_literal(left),
                    extract_vector_similarity_call(right),
                )
            {
                return Some(VectorSimilarityPredicate {
                    variable: vs.0,
                    property: vs.1,
                    query: vs.2,
                    threshold: Some(thresh),
                });
            }
            None
        }
        _ => None,
    }
}

/// Extract (variable, property, query_expr) from vector_similarity(n.prop, query)
fn extract_vector_similarity_call(expr: &Expr) -> Option<(String, String, Expr)> {
    if let Expr::FunctionCall { name, args, .. } = expr
        && name.eq_ignore_ascii_case("vector_similarity")
        && args.len() == 2
    {
        // First arg should be Property(Identifier(var), prop)
        if let Expr::Property(var_expr, prop) = &args[0]
            && let Expr::Identifier(var) = var_expr.as_ref()
        {
            // Second arg is query
            return Some((var.clone(), prop.clone(), args[1].clone()));
        }
    }
    None
}

/// Extract a float value from a literal expression
fn extract_float_literal(expr: &Expr) -> Option<f32> {
    if let Expr::Literal(Value::Number(n)) = expr {
        return n.as_f64().map(|f| f as f32);
    }
    None
}

/// Extract a Vec<f32> from a literal array expression
#[allow(dead_code)]
fn extract_float_array(expr: &Expr) -> Option<Vec<f32>> {
    match expr {
        Expr::Literal(Value::Array(arr)) => {
            let mut result = Vec::with_capacity(arr.len());
            for v in arr {
                if let Some(n) = v.as_f64() {
                    result.push(n as f32);
                } else {
                    return None;
                }
            }
            Some(result)
        }
        Expr::List(exprs) => {
            let mut result = Vec::with_capacity(exprs.len());
            for e in exprs {
                if let Expr::Literal(Value::Number(n)) = e {
                    if let Some(f) = n.as_f64() {
                        result.push(f as f32);
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            }
            Some(result)
        }
        _ => None,
    }
}

pub struct QueryPlanner {
    schema: Arc<Schema>,
    /// Cache of parsed generation expressions, keyed by (label_name, gen_col_name).
    gen_expr_cache: std::collections::HashMap<(String, String), Expr>,
    /// Counter for generating unique anonymous variable names.
    anon_counter: std::cell::Cell<usize>,
}

struct TraverseParams<'a> {
    rel: &'a crate::query::ast::RelationshipPattern,
    target_node: &'a crate::query::ast::NodePattern,
    _source_part: &'a PatternPart,
    optional: bool,
    path_variable: Option<String>,
}

/// Information about a partial index that matches the query predicates.
#[derive(Debug)]
struct PartialIndexMatch {
    /// Name of the partial index.
    #[allow(dead_code)]
    index_name: String,
    /// Properties covered by this index.
    #[allow(dead_code)]
    indexed_properties: Vec<String>,
    /// Residual predicate after removing index-matched predicates.
    residual_predicate: Option<Expr>,
}

impl QueryPlanner {
    pub fn new(schema: Arc<Schema>) -> Self {
        // Pre-parse all generation expressions for caching
        let mut gen_expr_cache = std::collections::HashMap::new();
        for (label, props) in &schema.properties {
            for (gen_col, meta) in props {
                if let Some(expr_str) = &meta.generation_expression
                    && let Ok(mut parser) = crate::query::parser::CypherParser::new(expr_str)
                    && let Ok(parsed_expr) = parser.parse_expression()
                {
                    gen_expr_cache.insert((label.clone(), gen_col.clone()), parsed_expr);
                }
            }
        }
        Self {
            schema,
            gen_expr_cache,
            anon_counter: std::cell::Cell::new(0),
        }
    }

    pub fn plan(&self, query: Query) -> Result<LogicalPlan> {
        self.plan_with_scope(query, Vec::new())
    }

    pub fn plan_with_scope(&self, query: Query, vars: Vec<String>) -> Result<LogicalPlan> {
        match query {
            Query::Single(q) => self.plan_single(q, vars),
            Query::Union { left, right, all } => {
                let l_plan = self.plan_with_scope(*left, vars.clone())?;
                let r_plan = self.plan_with_scope(*right, vars)?;
                Ok(LogicalPlan::Union {
                    left: Box::new(l_plan),
                    right: Box::new(r_plan),
                    all,
                })
            }
            Query::Explain(q) => {
                let plan = self.plan_with_scope(*q, vars)?;
                Ok(LogicalPlan::Explain {
                    plan: Box::new(plan),
                })
            }
        }
    }

    fn next_anon_var(&self) -> String {
        let id = self.anon_counter.get();
        self.anon_counter.set(id + 1);
        format!("_anon_{}", id)
    }

    fn plan_single(&self, query: CypherQuery, initial_vars: Vec<String>) -> Result<LogicalPlan> {
        let mut plan = LogicalPlan::Empty;

        // Inject initial variables into the plan via Project
        if !initial_vars.is_empty() {
            let projections = initial_vars
                .iter()
                .map(|v| (Expr::Identifier(v.clone()), Some(v.clone())))
                .collect();
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections,
            };
        }

        let mut vars_in_scope = initial_vars;

        for clause in query.clauses {
            match clause {
                // ... (CreateVectorIndex etc unchanged)
                Clause::CreateVectorIndex(clause) => {
                    plan = self.plan_create_vector_index(&clause)?;
                }
                Clause::CreateFullTextIndex(clause) => {
                    let config = FullTextIndexConfig {
                        name: clause.name,
                        label: clause.label,
                        properties: clause.properties,
                        tokenizer: TokenizerConfig::Standard, // Default for now
                        with_positions: true,
                    };
                    plan = LogicalPlan::CreateFullTextIndex {
                        config,
                        if_not_exists: clause.if_not_exists,
                    };
                }
                Clause::CreateScalarIndex(clause) => {
                    let config = ScalarIndexConfig {
                        name: clause.name,
                        label: clause.label,
                        properties: clause.properties,
                        index_type: ScalarIndexType::BTree, // Default
                        where_clause: clause.where_clause.map(|e| e.to_string_repr()),
                    };
                    plan = LogicalPlan::CreateScalarIndex {
                        config,
                        if_not_exists: clause.if_not_exists,
                    };
                }
                Clause::CreateJsonFtsIndex(clause) => {
                    let with_positions = clause
                        .options
                        .get("with_positions")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let config = JsonFtsIndexConfig {
                        name: clause.name,
                        label: clause.label,
                        column: clause.column,
                        paths: Vec::new(), // No path filtering for now
                        with_positions,
                    };
                    plan = LogicalPlan::CreateJsonFtsIndex {
                        config,
                        if_not_exists: clause.if_not_exists,
                    };
                }
                Clause::DropIndex(clause) => {
                    plan = LogicalPlan::DropIndex {
                        name: clause.name,
                        if_exists: clause.if_exists,
                    };
                }
                Clause::ShowIndexes(clause) => {
                    plan = LogicalPlan::ShowIndexes {
                        filter: clause.filter,
                    };
                }
                Clause::Copy(clause) => {
                    plan = LogicalPlan::Copy {
                        target: clause.target,
                        source: clause.source,
                        is_export: clause.is_export,
                        options: clause.options,
                    };
                }
                Clause::Backup(clause) => {
                    plan = LogicalPlan::Backup {
                        destination: clause.destination,
                        options: clause.options,
                    };
                }
                Clause::ShowDatabase => plan = LogicalPlan::ShowDatabase,
                Clause::ShowConfig => plan = LogicalPlan::ShowConfig,
                Clause::ShowStatistics => plan = LogicalPlan::ShowStatistics,
                Clause::Vacuum => plan = LogicalPlan::Vacuum,
                Clause::Checkpoint => plan = LogicalPlan::Checkpoint,
                Clause::WithRecursive(cte) => {
                    let (anchor, recursive) = match *cte.query {
                        crate::query::ast::Query::Union { left, right, .. } => {
                            let anchor_plan = self.plan_with_scope(*left, vars_in_scope.clone())?;
                            let recursive_plan =
                                self.plan_with_scope(*right, vars_in_scope.clone())?;
                            (anchor_plan, recursive_plan)
                        }
                        _ => return Err(anyhow!("Recursive CTE must be a UNION query")),
                    };

                    plan = LogicalPlan::RecursiveCTE {
                        cte_name: cte.name.clone(),
                        initial: Box::new(anchor),
                        recursive: Box::new(recursive),
                    };
                    vars_in_scope.push(cte.name);
                }
                Clause::CreateLabel(c) => plan = LogicalPlan::CreateLabel(c),
                Clause::CreateEdgeType(c) => plan = LogicalPlan::CreateEdgeType(c),
                Clause::AlterLabel(c) => plan = LogicalPlan::AlterLabel(c),
                Clause::AlterEdgeType(c) => plan = LogicalPlan::AlterEdgeType(c),
                Clause::DropLabel(c) => plan = LogicalPlan::DropLabel(c),
                Clause::DropEdgeType(c) => plan = LogicalPlan::DropEdgeType(c),
                Clause::CreateConstraint(c) => plan = LogicalPlan::CreateConstraint(c),
                Clause::DropConstraint(c) => plan = LogicalPlan::DropConstraint(c),
                Clause::ShowConstraints(c) => plan = LogicalPlan::ShowConstraints(c),
                Clause::Unwind(unwind) => {
                    plan = LogicalPlan::Unwind {
                        input: Box::new(plan),
                        expr: unwind.expr.clone(),
                        variable: unwind.variable.clone(),
                    };
                    vars_in_scope.push(unwind.variable.clone());
                }
                Clause::Call(call_clause) => {
                    for (name, alias) in &call_clause.yield_items {
                        if let Some(a) = alias {
                            vars_in_scope.push(a.clone());
                        } else {
                            vars_in_scope.push(name.clone());
                        }
                    }
                    plan = LogicalPlan::ProcedureCall {
                        procedure_name: call_clause.procedure_name.clone(),
                        arguments: call_clause.arguments.clone(),
                        yield_items: call_clause.yield_items.clone(),
                    };
                }
                Clause::CallSubquery(query) => {
                    let sub_plan = self.plan_with_scope(*query, vars_in_scope.clone())?;
                    plan = LogicalPlan::Apply {
                        input: Box::new(plan),
                        subquery: Box::new(sub_plan),
                        input_filter: None,
                    };
                    // Note: We should ideally update vars_in_scope here based on subquery return
                }
                Clause::Match(match_clause) => {
                    plan = self.plan_match_clause(&match_clause, plan, &mut vars_in_scope)?;
                }
                Clause::Merge(merge_clause) => {
                    // For now, simple MERGE on single pattern part.
                    // If multiple parts, we should decompose or handle.
                    // The Executor::execute_merge_match currently only handles single node.
                    // We need to implement full pattern matching for MERGE.

                    // For MVP, we will pass the whole pattern to LogicalPlan::Merge
                    // and let the executor handle the "Match or Create" logic.
                    // However, we should properly bind variables if they exist in scope.

                    // Note: This naive implementation assumes the whole pattern is the MERGE target.
                    // Complex MERGE logic (partial match, binding) is advanced.

                    plan = LogicalPlan::Merge {
                        input: Box::new(plan),
                        pattern: merge_clause.pattern.clone(),
                        on_match: merge_clause.on_match.clone(),
                        on_create: merge_clause.on_create.clone(),
                    };

                    // Update vars in scope from pattern
                    for part in &merge_clause.pattern.parts {
                        if let Some(n) = part.as_node() {
                            if let Some(v) = &n.variable
                                && !vars_in_scope.contains(v)
                            {
                                vars_in_scope.push(v.clone());
                            }
                        } else if let Some(r) = part.as_relationship()
                            && let Some(v) = &r.variable
                            && !vars_in_scope.contains(v)
                        {
                            vars_in_scope.push(v.clone());
                        }
                    }
                }
                Clause::Create(create_clause) => {
                    plan = LogicalPlan::Create {
                        input: Box::new(plan),
                        pattern: create_clause.pattern.clone(),
                    };
                }
                Clause::Set(set_clause) => {
                    plan = LogicalPlan::Set {
                        input: Box::new(plan),
                        items: set_clause.items.clone(),
                    };
                }
                Clause::Remove(remove_clause) => {
                    plan = LogicalPlan::Remove {
                        input: Box::new(plan),
                        items: remove_clause.items.clone(),
                    };
                }
                Clause::Delete(delete_clause) => {
                    plan = LogicalPlan::Delete {
                        input: Box::new(plan),
                        items: delete_clause.items.clone(),
                        detach: delete_clause.detach,
                    };
                }
                Clause::With(with_clause) => {
                    let (new_plan, new_vars) =
                        self.plan_with_clause(&with_clause, plan, &vars_in_scope)?;
                    plan = new_plan;
                    vars_in_scope = new_vars;
                }
                Clause::Begin => {
                    plan = LogicalPlan::Begin;
                }
                Clause::Commit => {
                    plan = LogicalPlan::Commit;
                }
                Clause::Rollback => {
                    plan = LogicalPlan::Rollback;
                }
            }
        }

        if matches!(plan, LogicalPlan::Empty) && query.return_clause.is_none() {
            // For DDL, Return is optional. We can verify if plan is DDL or requires Return.
            // If plan is CreateVectorIndex etc., return_clause None is valid.
            if matches!(
                plan,
                LogicalPlan::CreateVectorIndex { .. }
                    | LogicalPlan::CreateFullTextIndex { .. }
                    | LogicalPlan::CreateScalarIndex { .. }
                    | LogicalPlan::CreateJsonFtsIndex { .. }
                    | LogicalPlan::DropIndex { .. }
                    | LogicalPlan::ShowIndexes { .. }
                    | LogicalPlan::Copy { .. }
                    | LogicalPlan::Backup { .. }
            ) {
                // OK
            } else {
                return Err(anyhow!("Query must have clauses or RETURN"));
            }
        }

        // 3. Analyze Return Clause
        let mut group_by = Vec::new();
        let mut aggregates = Vec::new();
        let mut has_agg = false;
        let mut projections = Vec::new();

        if let Some(return_clause) = query.return_clause {
            for item in return_clause.items {
                match item {
                    ReturnItem::Expr { expr, alias } => {
                        projections.push((expr.clone(), alias));
                        if expr.is_aggregate() {
                            has_agg = true;
                            aggregates.push(expr);
                        } else if !group_by.contains(&expr) {
                            group_by.push(expr.clone());
                        }
                    }
                    ReturnItem::All => {
                        for v in &vars_in_scope {
                            projections.push((Expr::Identifier(v.clone()), Some(v.clone())));
                        }
                    }
                }
            }
        }

        if has_agg {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
            };
        }

        // 3.5 Window Functions
        let mut window_exprs = Vec::new();
        for (expr, _) in &projections {
            Self::collect_window_functions(expr, &mut window_exprs);
        }
        if let Some(order_by) = &query.order_by {
            for item in order_by {
                Self::collect_window_functions(&item.expr, &mut window_exprs);
            }
        }

        if !window_exprs.is_empty() {
            plan = LogicalPlan::Window {
                input: Box::new(plan),
                window_exprs,
            };
        }

        // 4. Sort
        if let Some(order_by) = query.order_by {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by,
            };
        }

        // 5. Limit/Skip
        if query.limit.is_some() || query.skip.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                skip: query.skip,
                fetch: query.limit,
            };
        }

        // 6. Project (Final reshaping and aliases)
        if !projections.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections,
            };
        }

        Ok(plan)
    }

    fn collect_window_functions(expr: &Expr, collected: &mut Vec<Expr>) {
        match expr {
            Expr::WindowFunction { .. } => {
                if !collected
                    .iter()
                    .any(|e| e.to_string_repr() == expr.to_string_repr())
                {
                    collected.push(expr.clone());
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_window_functions(left, collected);
                Self::collect_window_functions(right, collected);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    Self::collect_window_functions(arg, collected);
                }
            }
            Expr::List(items) => {
                for i in items {
                    Self::collect_window_functions(i, collected);
                }
            }
            Expr::Map(items) => {
                for (_, i) in items {
                    Self::collect_window_functions(i, collected);
                }
            }
            Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::UnaryOp { expr: e, .. } => {
                Self::collect_window_functions(e, collected);
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                if let Some(e) = expr {
                    Self::collect_window_functions(e, collected);
                }
                for (w, t) in when_then {
                    Self::collect_window_functions(w, collected);
                    Self::collect_window_functions(t, collected);
                }
                if let Some(e) = else_expr {
                    Self::collect_window_functions(e, collected);
                }
            }
            Expr::ListComprehension {
                list,
                where_clause,
                mapping,
                ..
            } => {
                Self::collect_window_functions(list, collected);
                if let Some(w) = where_clause {
                    Self::collect_window_functions(w, collected);
                }
                if let Some(m) = mapping {
                    Self::collect_window_functions(m, collected);
                }
            }
            Expr::Reduce {
                init, list, expr, ..
            } => {
                Self::collect_window_functions(init, collected);
                Self::collect_window_functions(list, collected);
                Self::collect_window_functions(expr, collected);
            }
            _ => {}
        }
    }

    /// Plan a MATCH clause, handling both shortestPath and regular patterns.
    fn plan_match_clause(
        &self,
        match_clause: &crate::query::ast::MatchClause,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<String>,
    ) -> Result<LogicalPlan> {
        let mut plan = plan;
        let parts = &match_clause.pattern.parts;

        if parts.is_empty() {
            return Err(anyhow!("Empty pattern"));
        }

        if match_clause.pattern.shortest_path {
            plan = self.plan_shortest_path(match_clause, plan, vars_in_scope)?;
        } else {
            plan = self.plan_regular_match(match_clause, plan, vars_in_scope)?;
        }

        // Handle WHERE clause with vector_similarity and predicate pushdown
        if let Some(predicate) = &match_clause.where_clause {
            plan = self.plan_where_clause(predicate, plan, vars_in_scope)?;
        }

        Ok(plan)
    }

    /// Plan a shortestPath pattern.
    fn plan_shortest_path(
        &self,
        match_clause: &crate::query::ast::MatchClause,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<String>,
    ) -> Result<LogicalPlan> {
        let mut plan = plan;
        let parts = &match_clause.pattern.parts;

        if parts.len() != 3 {
            return Err(anyhow!(
                "shortestPath only supported for 1-hop patterns like (a)-[:REL*]->(b) for now"
            ));
        }

        let source_node = parts[0].as_node().unwrap();
        let rel = parts[1].as_relationship().unwrap();
        let target_node = parts[2].as_node().unwrap();

        let source_var = source_node
            .variable
            .clone()
            .ok_or_else(|| anyhow!("Source node must have variable in shortestPath"))?;
        let target_var = target_node
            .variable
            .clone()
            .ok_or_else(|| anyhow!("Target node must have variable in shortestPath"))?;
        let path_var = match_clause
            .pattern
            .variable
            .clone()
            .ok_or_else(|| anyhow!("shortestPath must be assigned to a variable"))?;

        let source_bound = vars_in_scope.contains(&source_var);
        let target_bound = vars_in_scope.contains(&target_var);

        // Plan source node if not bound
        if !source_bound {
            plan = self.plan_unbound_node(source_node, &source_var, plan, false)?;
        } else if !source_node.properties.is_empty()
            && let Some(prop_filter) = self.properties_to_expr(&source_var, &source_node.properties)
        {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: prop_filter,
            };
        }

        // Plan target node if not bound
        let target_label_id = if !target_bound {
            let target_label_name = target_node
                .labels
                .first()
                .ok_or_else(|| anyhow!("Target node must have label if not already bound"))?;
            let target_label_meta = self
                .schema
                .labels
                .get(target_label_name)
                .ok_or_else(|| anyhow!("Label {} not found", target_label_name))?;

            let target_scan = LogicalPlan::Scan {
                label_id: target_label_meta.id,
                variable: target_var.clone(),
                filter: self.properties_to_expr(&target_var, &target_node.properties),
                optional: false,
            };

            if matches!(plan, LogicalPlan::Empty) {
                plan = target_scan;
            } else {
                plan = LogicalPlan::CrossJoin {
                    left: Box::new(plan),
                    right: Box::new(target_scan),
                };
            }
            target_label_meta.id
        } else {
            if !target_node.properties.is_empty()
                && let Some(prop_filter) =
                    self.properties_to_expr(&target_var, &target_node.properties)
            {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: prop_filter,
                };
            }
            0 // Wildcard for already-bound target
        };

        // Add ShortestPath operator
        let mut edge_type_ids = Vec::new();
        if rel.rel_types.is_empty() {
            // If no type specified, fetch all edge types
            for meta in self.schema.edge_types.values() {
                edge_type_ids.push(meta.id);
            }
        } else {
            for type_name in &rel.rel_types {
                let edge_meta = self
                    .schema
                    .edge_types
                    .get(type_name)
                    .ok_or_else(|| anyhow!("Edge type {} not found", type_name))?;
                edge_type_ids.push(edge_meta.id);
            }
        }

        // Extract hop constraints from relationship pattern
        // Default: min_hops = 1, max_hops = unlimited (u32::MAX)
        let min_hops = rel.min_hops.unwrap_or(1);
        let max_hops = rel.max_hops.unwrap_or(u32::MAX);

        plan = LogicalPlan::ShortestPath {
            input: Box::new(plan),
            edge_type_ids,
            direction: rel.direction.clone(),
            source_variable: source_var.clone(),
            target_variable: target_var.clone(),
            target_label_id,
            path_variable: path_var.clone(),
            min_hops,
            max_hops,
        };

        if !source_bound {
            vars_in_scope.push(source_var);
        }
        if !target_bound {
            vars_in_scope.push(target_var);
        }
        vars_in_scope.push(path_var);

        Ok(plan)
    }

    /// Plan a regular MATCH pattern (not shortestPath).
    fn plan_regular_match(
        &self,
        match_clause: &crate::query::ast::MatchClause,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<String>,
    ) -> Result<LogicalPlan> {
        let mut plan = plan;
        let parts = &match_clause.pattern.parts;
        let mut i = 0;

        // Count relationships to validate path variable usage
        let rel_count = parts
            .iter()
            .filter(|p| matches!(p, PatternPart::Relationship(_)))
            .count();

        let mut path_variable = match_clause.pattern.variable.clone();
        if path_variable.is_some() && rel_count > 1 {
            return Err(anyhow!(
                "Named path variables not yet supported for multi-hop patterns (e.g. (a)-[]->(b)-[]->(c))"
            ));
        }

        while i < parts.len() {
            let part = &parts[i];
            match part {
                PatternPart::Node(n) => {
                    let mut variable = n.variable.clone().unwrap_or_default();
                    if variable.is_empty() {
                        variable = self.next_anon_var();
                    }
                    let is_bound = !variable.is_empty() && vars_in_scope.contains(&variable);

                    if is_bound {
                        if let Some(prop_filter) = self.properties_to_expr(&variable, &n.properties)
                        {
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate: prop_filter,
                            };
                        }
                    } else {
                        plan = self.plan_unbound_node(n, &variable, plan, match_clause.optional)?;
                        if !variable.is_empty() {
                            vars_in_scope.push(variable.clone());
                        }
                    }

                    // Look ahead for relationships
                    let mut current_source_var = variable;
                    i += 1;
                    while i < parts.len() {
                        if let PatternPart::Relationship(r) = &parts[i] {
                            let target_node_part = &parts[i + 1];
                            if let PatternPart::Node(n_target) = target_node_part {
                                // Plan the traverse from the current source node
                                let (new_plan, target_var) = self.plan_traverse_with_source(
                                    plan,
                                    vars_in_scope,
                                    TraverseParams {
                                        rel: r,
                                        target_node: n_target,
                                        _source_part: part,
                                        optional: match_clause.optional,
                                        path_variable: path_variable.take(),
                                    },
                                    &current_source_var,
                                )?;
                                plan = new_plan;
                                current_source_var = target_var;
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

        Ok(plan)
    }

    /// Plan a traverse with an explicit source variable name.
    fn plan_traverse_with_source(
        &self,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<String>,
        params: TraverseParams<'_>,
        source_variable: &str,
    ) -> Result<(LogicalPlan, String)> {
        let mut edge_type_ids = Vec::new();
        let mut dst_labels = Vec::new();

        if params.rel.rel_types.is_empty() {
            // All types
            for meta in self.schema.edge_types.values() {
                edge_type_ids.push(meta.id);
                dst_labels.extend(meta.dst_labels.iter().cloned());
            }
        } else {
            for type_name in &params.rel.rel_types {
                let edge_meta = self
                    .schema
                    .edge_types
                    .get(type_name)
                    .ok_or_else(|| anyhow!("Edge type {} not found", type_name))?;
                edge_type_ids.push(edge_meta.id);
                dst_labels.extend(edge_meta.dst_labels.iter().cloned());
            }
        }

        let mut target_variable = params.target_node.variable.clone().unwrap_or_default();
        if target_variable.is_empty() {
            target_variable = self.next_anon_var();
        }
        let _target_is_bound =
            !target_variable.is_empty() && vars_in_scope.contains(&target_variable);

        let target_label_meta = if let Some(label_name) = params.target_node.labels.first() {
            Some(
                self.schema
                    .labels
                    .get(label_name)
                    .ok_or_else(|| anyhow!("Label {} not found", label_name))?,
            )
        } else if !_target_is_bound {
            // Infer from edge type(s)
            let unique_dsts: Vec<_> = dst_labels
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            if unique_dsts.len() == 1 {
                let label_name = &unique_dsts[0];
                Some(self.schema.labels.get(label_name).ok_or_else(|| {
                    anyhow!("Label {} not found (inferred from edge)", label_name)
                })?)
            } else {
                return Err(anyhow!(
                    "Target node must have label (inference ambiguous or not supported for multiple dst labels)"
                ));
            }
        } else {
            None
        };

        let plan = LogicalPlan::Traverse {
            input: Box::new(plan),
            edge_type_ids,
            direction: params.rel.direction.clone(),
            source_variable: source_variable.to_string(),
            target_variable: target_variable.clone(),
            target_label_id: target_label_meta.map(|m| m.id).unwrap_or(0),
            step_variable: params.rel.variable.clone(),
            min_hops: params.rel.min_hops.unwrap_or(1) as usize,
            max_hops: params.rel.max_hops.unwrap_or(1) as usize,
            optional: params.optional,
            target_filter: self
                .properties_to_expr(&target_variable, &params.target_node.properties),
            path_variable: params.path_variable.clone(),
        };

        if let Some(sv) = &params.rel.variable {
            vars_in_scope.push(sv.clone());
        }
        if !vars_in_scope.contains(&target_variable) {
            vars_in_scope.push(target_variable.clone());
        }
        if let Some(pv) = params.path_variable {
            vars_in_scope.push(pv);
        }

        Ok((plan, target_variable))
    }

    /// Plan an unbound node (creates a Scan or CrossJoin).
    fn plan_unbound_node(
        &self,
        node: &crate::query::ast::NodePattern,
        variable: &str,
        plan: LogicalPlan,
        optional: bool,
    ) -> Result<LogicalPlan> {
        if node.labels.is_empty() {
            return Err(anyhow!("Node must have a label for now"));
        }
        let label_name = &node.labels[0];
        let label_meta = self
            .schema
            .labels
            .get(label_name)
            .ok_or_else(|| anyhow!("Label {} not found", label_name))?;

        let prop_filter = self.properties_to_expr(variable, &node.properties);
        let scan = LogicalPlan::Scan {
            label_id: label_meta.id,
            variable: variable.to_string(),
            filter: prop_filter,
            optional,
        };

        if matches!(plan, LogicalPlan::Empty) {
            Ok(scan)
        } else {
            Ok(LogicalPlan::CrossJoin {
                left: Box::new(plan),
                right: Box::new(scan),
            })
        }
    }

    /// Plan a traverse (edge traversal between nodes).
    fn _plan_traverse(
        &self,
        plan: LogicalPlan,
        vars_in_scope: &mut Vec<String>,
        params: TraverseParams<'_>,
    ) -> Result<LogicalPlan> {
        let source_variable = params
            ._source_part
            .as_node()
            .unwrap()
            .variable
            .clone()
            .unwrap_or_default();
        let (new_plan, _) =
            self.plan_traverse_with_source(plan, vars_in_scope, params, &source_variable)?;
        Ok(new_plan)
    }

    /// Plan a WHERE clause with vector_similarity extraction and predicate pushdown.
    fn plan_where_clause(
        &self,
        predicate: &Expr,
        plan: LogicalPlan,
        vars_in_scope: &[String],
    ) -> Result<LogicalPlan> {
        let mut plan = plan;
        let mut current_predicate =
            self.rewrite_predicates_using_indexes(predicate, &plan, vars_in_scope)?;

        // 0. Try to extract ANY IN predicate for Inverted Index optimization
        if let Some(extraction) = extract_any_in_predicate(&current_predicate) {
            let any_pred = &extraction.predicate;
            // Check if index exists
            if let Some(label_id) = Self::find_scan_label_id(&plan, &any_pred.variable) {
                let label_name = self.schema.label_name_by_id(label_id);
                if let Some(label) = label_name {
                    // Verify index exists in schema
                    let has_index = self.schema.indexes.iter().any(|idx| match idx {
                        IndexDefinition::Inverted(cfg) => {
                            cfg.label == label && cfg.property == any_pred.property
                        }
                        _ => false,
                    });

                    if has_index {
                        // Replace Scan with InvertedIndexLookup
                        plan = Self::replace_scan_with_inverted_lookup(
                            plan,
                            &any_pred.variable,
                            label_id,
                            &any_pred.property,
                            any_pred.terms.clone(),
                        );

                        if let Some(residual) = extraction.residual {
                            current_predicate = residual;
                        } else {
                            current_predicate = Expr::Literal(serde_json::Value::Bool(true));
                        }
                    }
                }
            }
        }

        // 1. Try to extract vector_similarity predicate for optimization
        if let Some(extraction) = extract_vector_similarity(&current_predicate) {
            let vs = &extraction.predicate;
            if Self::find_scan_label_id(&plan, &vs.variable).is_some() {
                plan = Self::replace_scan_with_knn(
                    plan,
                    &vs.variable,
                    &vs.property,
                    vs.query.clone(),
                    vs.threshold,
                );
                if let Some(residual) = extraction.residual {
                    current_predicate = residual;
                } else {
                    current_predicate = Expr::Literal(serde_json::Value::Bool(true));
                }
            }
        }

        // 2. Check for applicable partial indexes for each scan variable
        for var in vars_in_scope {
            if let Some(label_id) = Self::find_scan_label_id(&plan, var) {
                let partial_matches =
                    self.find_applicable_partial_indexes(label_id, var, &current_predicate);

                // If we found a partial index match, update predicate to residual only
                // (the index condition is implicitly satisfied)
                if let Some(best_match) = partial_matches.first() {
                    if let Some(residual) = &best_match.residual_predicate {
                        current_predicate = residual.clone();
                    } else {
                        // All predicates were matched by the index
                        current_predicate = Expr::Literal(serde_json::Value::Bool(true));
                    }
                }
            }
        }

        // 3. Push eligible predicates to Scan OR Traverse filters
        for var in vars_in_scope {
            // Check if var is produced by a Scan
            if Self::find_scan_label_id(&plan, var).is_some() {
                let (pushable, residual) =
                    Self::extract_variable_predicates(&current_predicate, var);

                for pred in pushable {
                    plan = Self::push_predicate_to_scan(plan, var, pred);
                }

                if let Some(r) = residual {
                    current_predicate = r;
                } else {
                    current_predicate = Expr::Literal(serde_json::Value::Bool(true));
                }
            } else if Self::is_traverse_target(&plan, var) {
                // Push to Traverse
                let (pushable, residual) =
                    Self::extract_variable_predicates(&current_predicate, var);

                for pred in pushable {
                    plan = Self::push_predicate_to_traverse(plan, var, pred);
                }

                if let Some(r) = residual {
                    current_predicate = r;
                } else {
                    current_predicate = Expr::Literal(serde_json::Value::Bool(true));
                }
            }
        }

        // 4. Push predicates to Apply.input_filter
        // This filters input rows BEFORE executing correlated subqueries.
        plan = Self::push_predicates_to_apply(plan, &mut current_predicate);

        // 5. Add Filter node for any remaining predicates
        if !matches!(
            &current_predicate,
            Expr::Literal(serde_json::Value::Bool(true))
        ) {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: current_predicate,
            };
        }

        Ok(plan)
    }

    fn rewrite_predicates_using_indexes(
        &self,
        predicate: &Expr,
        plan: &LogicalPlan,
        vars_in_scope: &[String],
    ) -> Result<Expr> {
        // ... (unchanged)
        let mut rewritten = predicate.clone();

        for var in vars_in_scope {
            if let Some(label_id) = Self::find_scan_label_id(plan, var) {
                // Find label name
                let label_name = self.schema.label_name_by_id(label_id).map(str::to_owned);

                if let Some(label) = label_name
                    && let Some(props) = self.schema.properties.get(&label)
                {
                    for (gen_col, meta) in props {
                        if meta.generation_expression.is_some() {
                            // Use cached parsed expression
                            if let Some(schema_expr) =
                                self.gen_expr_cache.get(&(label.clone(), gen_col.clone()))
                            {
                                // Rewrite 'rewritten' replacing occurrences of schema_expr with gen_col
                                rewritten =
                                    Self::replace_expression(rewritten, schema_expr, var, gen_col);
                            }
                        }
                    }
                }
            }
        }
        Ok(rewritten)
    }

    // ... (replace_expression unchanged) ...
    fn replace_expression(expr: Expr, schema_expr: &Expr, query_var: &str, gen_col: &str) -> Expr {
        // First, normalize schema_expr to use query_var
        let schema_var = schema_expr.extract_variable();

        if let Some(s_var) = schema_var {
            let target_expr = schema_expr.substitute_variable(&s_var, query_var);

            if expr == target_expr {
                return Expr::Property(
                    Box::new(Expr::Identifier(query_var.to_string())),
                    gen_col.to_string(),
                );
            }
        }

        // Recurse
        match expr {
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::replace_expression(
                    *left,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                op,
                right: Box::new(Self::replace_expression(
                    *right,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::replace_expression(
                    *expr,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
            },
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => Expr::FunctionCall {
                name,
                args: args
                    .into_iter()
                    .map(|a| Self::replace_expression(a, schema_expr, query_var, gen_col))
                    .collect(),
                distinct,
            },
            Expr::IsNull(expr) => Expr::IsNull(Box::new(Self::replace_expression(
                *expr,
                schema_expr,
                query_var,
                gen_col,
            ))),
            Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(Self::replace_expression(
                *expr,
                schema_expr,
                query_var,
                gen_col,
            ))),
            Expr::ArrayIndex(e, idx) => Expr::ArrayIndex(
                Box::new(Self::replace_expression(
                    *e,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                Box::new(Self::replace_expression(
                    *idx,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
            ),
            Expr::List(exprs) => Expr::List(
                exprs
                    .into_iter()
                    .map(|e| Self::replace_expression(e, schema_expr, query_var, gen_col))
                    .collect(),
            ),
            Expr::Map(entries) => Expr::Map(
                entries
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            Self::replace_expression(v, schema_expr, query_var, gen_col),
                        )
                    })
                    .collect(),
            ),
            Expr::Property(e, prop) => Expr::Property(
                Box::new(Self::replace_expression(
                    *e,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                prop,
            ),
            Expr::Case {
                expr: case_expr,
                when_then,
                else_expr,
            } => Expr::Case {
                expr: case_expr.map(|e| {
                    Box::new(Self::replace_expression(
                        *e,
                        schema_expr,
                        query_var,
                        gen_col,
                    ))
                }),
                when_then: when_then
                    .into_iter()
                    .map(|(w, t)| {
                        (
                            Self::replace_expression(w, schema_expr, query_var, gen_col),
                            Self::replace_expression(t, schema_expr, query_var, gen_col),
                        )
                    })
                    .collect(),
                else_expr: else_expr.map(|e| {
                    Box::new(Self::replace_expression(
                        *e,
                        schema_expr,
                        query_var,
                        gen_col,
                    ))
                }),
            },
            Expr::ListComprehension {
                variable: lc_var,
                list,
                where_clause,
                mapping,
            } => Expr::ListComprehension {
                variable: lc_var,
                list: Box::new(Self::replace_expression(
                    *list,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                where_clause: where_clause.map(|e| {
                    Box::new(Self::replace_expression(
                        *e,
                        schema_expr,
                        query_var,
                        gen_col,
                    ))
                }),
                mapping: mapping.map(|e| {
                    Box::new(Self::replace_expression(
                        *e,
                        schema_expr,
                        query_var,
                        gen_col,
                    ))
                }),
            },
            Expr::Reduce {
                accumulator,
                init,
                variable: reduce_var,
                list,
                expr: reduce_expr,
            } => Expr::Reduce {
                accumulator,
                init: Box::new(Self::replace_expression(
                    *init,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                variable: reduce_var,
                list: Box::new(Self::replace_expression(
                    *list,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                expr: Box::new(Self::replace_expression(
                    *reduce_expr,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
            },
            Expr::WindowFunction {
                function,
                partition_by,
                order_by,
            } => Expr::WindowFunction {
                function: Box::new(Self::replace_expression(
                    *function,
                    schema_expr,
                    query_var,
                    gen_col,
                )),
                partition_by: partition_by
                    .into_iter()
                    .map(|e| Self::replace_expression(e, schema_expr, query_var, gen_col))
                    .collect(),
                order_by: order_by
                    .into_iter()
                    .map(|(e, dir)| {
                        (
                            Self::replace_expression(e, schema_expr, query_var, gen_col),
                            dir,
                        )
                    })
                    .collect(),
            },
            // Leaf nodes (Identifier, Literal, Parameter, etc.) need no recursion
            _ => expr,
        }
    }

    /// Check if the variable is the target of a Traverse node
    fn is_traverse_target(plan: &LogicalPlan, variable: &str) -> bool {
        match plan {
            LogicalPlan::Traverse {
                target_variable,
                input,
                ..
            } => target_variable == variable || Self::is_traverse_target(input, variable),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Apply { input, .. } => Self::is_traverse_target(input, variable),
            LogicalPlan::CrossJoin { left, right } => {
                Self::is_traverse_target(left, variable)
                    || Self::is_traverse_target(right, variable)
            }
            _ => false,
        }
    }

    /// Push a predicate into a Traverse's target_filter for the specified variable
    fn push_predicate_to_traverse(
        plan: LogicalPlan,
        variable: &str,
        predicate: Expr,
    ) -> LogicalPlan {
        match plan {
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
                if target_variable == variable {
                    // Found the traverse producing this variable
                    let new_filter = match target_filter {
                        Some(existing) => Some(Expr::BinaryOp {
                            left: Box::new(existing),
                            op: Operator::And,
                            right: Box::new(predicate),
                        }),
                        None => Some(predicate),
                    };
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
                        target_filter: new_filter,
                        path_variable,
                    }
                } else {
                    // Recurse into input
                    LogicalPlan::Traverse {
                        input: Box::new(Self::push_predicate_to_traverse(
                            *input, variable, predicate,
                        )),
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
                    }
                }
            }
            LogicalPlan::Filter {
                input,
                predicate: p,
            } => LogicalPlan::Filter {
                input: Box::new(Self::push_predicate_to_traverse(
                    *input, variable, predicate,
                )),
                predicate: p,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::push_predicate_to_traverse(
                    *input, variable, predicate,
                )),
                projections,
            },
            LogicalPlan::CrossJoin { left, right } => {
                // Check which side has the variable
                if Self::is_traverse_target(&left, variable) {
                    LogicalPlan::CrossJoin {
                        left: Box::new(Self::push_predicate_to_traverse(
                            *left, variable, predicate,
                        )),
                        right,
                    }
                } else {
                    LogicalPlan::CrossJoin {
                        left,
                        right: Box::new(Self::push_predicate_to_traverse(
                            *right, variable, predicate,
                        )),
                    }
                }
            }
            other => other,
        }
    }

    /// Find partial indexes applicable to the given variable and predicates.
    ///
    /// A partial index is applicable if the query predicates subsume (imply) the
    /// index's WHERE clause. This allows the planner to use the partial index
    /// and only filter by the residual predicates.
    fn find_applicable_partial_indexes(
        &self,
        label_id: u16,
        variable: &str,
        predicates: &Expr,
    ) -> Vec<PartialIndexMatch> {
        let mut matches = Vec::new();

        // Find label name from label_id
        let label_name = self.schema.label_name_by_id(label_id).map(str::to_owned);

        let Some(label) = label_name else {
            return matches;
        };

        // Find partial scalar indexes for this label
        for index in &self.schema.indexes {
            if let IndexDefinition::Scalar(config) = index {
                if config.label != label {
                    continue;
                }

                // Skip non-partial indexes
                let Some(where_clause_str) = &config.where_clause else {
                    continue;
                };

                // Parse the index's WHERE clause
                let parsed_index_expr =
                    match crate::query::parser::CypherParser::new(where_clause_str) {
                        Ok(mut parser) => match parser.parse_expression() {
                            Ok(expr) => expr,
                            Err(_) => continue,
                        },
                        Err(_) => continue,
                    };

                // Extract the variable used in the index's WHERE clause
                let index_var = parsed_index_expr
                    .extract_variable()
                    .unwrap_or_else(|| "x".to_string());

                // Check subsumption
                let result =
                    check_subsumption(predicates, &parsed_index_expr, variable, &index_var);

                if result.subsumes {
                    log::debug!(
                        "Partial index '{}' matches query predicates for variable '{}'",
                        config.name,
                        variable
                    );
                    matches.push(PartialIndexMatch {
                        index_name: config.name.clone(),
                        indexed_properties: config.properties.clone(),
                        residual_predicate: result.residual,
                    });
                }
            }
        }

        matches
    }

    /// Plan a CREATE VECTOR INDEX clause, parsing all index options.
    fn plan_create_vector_index(
        &self,
        clause: &crate::query::ast::CreateVectorIndexClause,
    ) -> Result<LogicalPlan> {
        let metric_str = clause
            .options
            .get("metric")
            .and_then(|v| v.as_str())
            .unwrap_or("cosine");
        let metric = match metric_str.to_lowercase().as_str() {
            "l2" | "euclidean" => DistanceMetric::L2,
            "dot" => DistanceMetric::Dot,
            _ => DistanceMetric::Cosine,
        };

        let index_type = self.parse_vector_index_type(&clause.options)?;
        let embedding_config = self.parse_embedding_config(&clause.options)?;

        let config = VectorIndexConfig {
            name: clause.name.clone(),
            label: clause.label.clone(),
            property: clause.property.clone(),
            index_type,
            metric,
            embedding_config,
        };

        Ok(LogicalPlan::CreateVectorIndex {
            config,
            if_not_exists: clause.if_not_exists,
        })
    }

    /// Parse vector index type from options.
    fn parse_vector_index_type(&self, options: &HashMap<String, Value>) -> Result<VectorIndexType> {
        let type_str = options
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("hnsw");

        let index_type = match type_str.to_lowercase().as_str() {
            "ivf_pq" => {
                let num_partitions = options
                    .get("num_partitions")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(256) as u32;
                let num_sub_vectors = options
                    .get("num_sub_vectors")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(16) as u32;
                VectorIndexType::IvfPq {
                    num_partitions,
                    num_sub_vectors,
                    bits_per_subvector: 8,
                }
            }
            "flat" => VectorIndexType::Flat,
            _ => {
                let m = options.get("m").and_then(|v| v.as_u64()).unwrap_or(16) as u32;
                let ef_construction = options
                    .get("ef_construction")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(100) as u32;
                let ef_search = options
                    .get("ef_search")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(50) as u32;
                VectorIndexType::Hnsw {
                    m,
                    ef_construction,
                    ef_search,
                }
            }
        };

        Ok(index_type)
    }

    /// Parse embedding configuration from options.
    fn parse_embedding_config(
        &self,
        options: &HashMap<String, Value>,
    ) -> Result<Option<EmbeddingConfig>> {
        let emb_val = match options.get("embedding") {
            Some(v) => v,
            None => return Ok(None),
        };

        let emb_obj = match emb_val.as_object() {
            Some(o) => o,
            None => return Ok(None),
        };

        let provider = emb_obj
            .get("provider")
            .and_then(|v| v.as_str())
            .unwrap_or("fastembed");

        let model = match provider {
            "fastembed" => {
                let model_name = emb_obj
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("AllMiniLML6V2")
                    .to_string();
                EmbeddingModel::FastEmbed {
                    model_name,
                    cache_dir: None,
                    max_length: None,
                }
            }
            "openai" => {
                let model = emb_obj
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("text-embedding-3-small")
                    .to_string();
                let api_key_env = emb_obj
                    .get("api_key_env")
                    .and_then(|v| v.as_str())
                    .unwrap_or("OPENAI_API_KEY")
                    .to_string();
                EmbeddingModel::OpenAI {
                    model,
                    api_key_env,
                    dimensions: None,
                }
            }
            "ollama" => {
                let model = emb_obj
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("nomic-embed-text")
                    .to_string();
                let host = emb_obj
                    .get("host")
                    .and_then(|v| v.as_str())
                    .unwrap_or("http://localhost:11434")
                    .to_string();
                EmbeddingModel::Ollama { model, host }
            }
            _ => return Err(anyhow!("Unsupported embedding provider: {}", provider)),
        };

        let source_properties = if let Some(src) = emb_obj.get("source") {
            if let Some(arr) = src.as_array() {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        Ok(Some(EmbeddingConfig {
            model,
            source_properties,
            batch_size: 32,
        }))
    }

    /// Plan a WITH clause, handling aggregations and projections.
    fn plan_with_clause(
        &self,
        with_clause: &crate::query::ast::WithClause,
        plan: LogicalPlan,
        vars_in_scope: &[String],
    ) -> Result<(LogicalPlan, Vec<String>)> {
        let mut plan = plan;
        let mut group_by: Vec<Expr> = Vec::new();
        let mut aggregates: Vec<Expr> = Vec::new();
        let mut has_agg = false;
        let mut projections = Vec::new();
        let mut new_vars = Vec::new();

        for item in &with_clause.return_clause.items {
            match item {
                ReturnItem::Expr { expr, alias } => {
                    projections.push((expr.clone(), alias.clone()));
                    if expr.is_aggregate() {
                        has_agg = true;
                        aggregates.push(expr.clone());
                    } else if !group_by.contains(expr) {
                        group_by.push(expr.clone());
                    }

                    if let Some(a) = alias {
                        new_vars.push(a.clone());
                    } else if let Expr::Identifier(v) = expr {
                        new_vars.push(v.clone());
                    }
                }
                ReturnItem::All => {
                    for v in vars_in_scope {
                        projections.push((Expr::Identifier(v.clone()), Some(v.clone())));
                    }
                    new_vars.extend(vars_in_scope.iter().cloned());
                }
            }
        }

        if has_agg {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
            };
        } else if !projections.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projections,
            };
        }

        if let Some(predicate) = &with_clause.where_clause {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: predicate.clone(),
            };
        }

        Ok((plan, new_vars))
    }

    pub fn properties_to_expr(
        &self,
        variable: &str,
        properties: &[(String, Expr)],
    ) -> Option<Expr> {
        if properties.is_empty() {
            return None;
        }
        let mut final_expr = None;
        for (prop, val_expr) in properties {
            let eq_expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Identifier(variable.to_string())),
                    prop.clone(),
                )),
                op: crate::query::expr::Operator::Eq,
                right: Box::new(val_expr.clone()),
            };

            if let Some(e) = final_expr {
                final_expr = Some(Expr::BinaryOp {
                    left: Box::new(e),
                    op: crate::query::expr::Operator::And,
                    right: Box::new(eq_expr),
                });
            } else {
                final_expr = Some(eq_expr);
            }
        }
        final_expr
    }

    /// Replace a Scan node matching the variable with a VectorKnn node
    fn replace_scan_with_knn(
        plan: LogicalPlan,
        variable: &str,
        property: &str,
        query: Expr,
        threshold: Option<f32>,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan {
                label_id,
                variable: scan_var,
                filter,
                optional,
            } => {
                if scan_var == variable {
                    // Inject any existing scan filter into VectorKnn?
                    // VectorKnn doesn't support pre-filtering natively in logical plan yet (except threshold).
                    // Typically filter is applied post-Knn or during Knn if supported.
                    // For now, we assume filter is residual or handled by `extract_vector_similarity` which separates residual.
                    // If `filter` is present on Scan, it must be preserved.
                    // We can wrap VectorKnn in Filter if Scan had filter.

                    let knn = LogicalPlan::VectorKnn {
                        label_id,
                        variable: variable.to_string(),
                        property: property.to_string(),
                        query,
                        k: 100, // Default K, should push down LIMIT
                        threshold,
                    };

                    if let Some(f) = filter {
                        LogicalPlan::Filter {
                            input: Box::new(knn),
                            predicate: f,
                        }
                    } else {
                        knn
                    }
                } else {
                    LogicalPlan::Scan {
                        label_id,
                        variable: scan_var.clone(),
                        filter,
                        optional,
                    }
                }
            }
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(Self::replace_scan_with_knn(
                    *input, variable, property, query, threshold,
                )),
                predicate,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::replace_scan_with_knn(
                    *input, variable, property, query, threshold,
                )),
                projections,
            },
            LogicalPlan::Limit { input, skip, fetch } => {
                // If we encounter Limit, we should ideally push K down to VectorKnn
                // But replace_scan_with_knn is called from plan_where_clause which is inside plan_match.
                // Limit comes later.
                // To support Limit pushdown, we need a separate optimizer pass or do it in plan_single.
                LogicalPlan::Limit {
                    input: Box::new(Self::replace_scan_with_knn(
                        *input, variable, property, query, threshold,
                    )),
                    skip,
                    fetch,
                }
            }
            LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
                left: Box::new(Self::replace_scan_with_knn(
                    *left,
                    variable,
                    property,
                    query.clone(),
                    threshold,
                )),
                right: Box::new(Self::replace_scan_with_knn(
                    *right, variable, property, query, threshold,
                )),
            },
            other => other,
        }
    }

    /// Find the label_id for a Scan node matching the given variable
    fn find_scan_label_id(plan: &LogicalPlan, variable: &str) -> Option<u16> {
        match plan {
            LogicalPlan::Scan {
                label_id,
                variable: var,
                ..
            } if var == variable => Some(*label_id),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Apply { input, .. } => Self::find_scan_label_id(input, variable),
            LogicalPlan::CrossJoin { left, right } => Self::find_scan_label_id(left, variable)
                .or_else(|| Self::find_scan_label_id(right, variable)),
            LogicalPlan::Traverse { input, .. } => Self::find_scan_label_id(input, variable),
            _ => None,
        }
    }

    fn replace_scan_with_inverted_lookup(
        plan: LogicalPlan,
        variable: &str,
        label_id: u16,
        property: &str,
        terms: Expr,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan { variable: v, .. } if v == variable => {
                LogicalPlan::InvertedIndexLookup {
                    label_id,
                    variable: v,
                    property: property.to_string(),
                    terms,
                }
            }
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::replace_scan_with_inverted_lookup(
                    *input, variable, label_id, property, terms,
                )),
                projections,
            },
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(Self::replace_scan_with_inverted_lookup(
                    *input, variable, label_id, property, terms,
                )),
                predicate,
            },
            LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
                left: Box::new(Self::replace_scan_with_inverted_lookup(
                    *left,
                    variable,
                    label_id,
                    property,
                    terms.clone(),
                )),
                right: Box::new(Self::replace_scan_with_inverted_lookup(
                    *right, variable, label_id, property, terms,
                )),
            },
            _ => plan,
        }
    }

    /// Push a predicate into a Scan's filter for the specified variable
    fn push_predicate_to_scan(plan: LogicalPlan, variable: &str, predicate: Expr) -> LogicalPlan {
        match plan {
            LogicalPlan::Scan {
                label_id,
                variable: var,
                filter,
                optional,
            } if var == variable => {
                // Merge the predicate with existing filter
                let new_filter = match filter {
                    Some(existing) => Some(Expr::BinaryOp {
                        left: Box::new(existing),
                        op: Operator::And,
                        right: Box::new(predicate),
                    }),
                    None => Some(predicate),
                };
                LogicalPlan::Scan {
                    label_id,
                    variable: var,
                    filter: new_filter,
                    optional,
                }
            }
            LogicalPlan::Filter {
                input,
                predicate: p,
            } => LogicalPlan::Filter {
                input: Box::new(Self::push_predicate_to_scan(*input, variable, predicate)),
                predicate: p,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::push_predicate_to_scan(*input, variable, predicate)),
                projections,
            },
            LogicalPlan::CrossJoin { left, right } => {
                // Check which side has the variable
                if Self::find_scan_label_id(&left, variable).is_some() {
                    LogicalPlan::CrossJoin {
                        left: Box::new(Self::push_predicate_to_scan(*left, variable, predicate)),
                        right,
                    }
                } else {
                    LogicalPlan::CrossJoin {
                        left,
                        right: Box::new(Self::push_predicate_to_scan(*right, variable, predicate)),
                    }
                }
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
            } => LogicalPlan::Traverse {
                input: Box::new(Self::push_predicate_to_scan(*input, variable, predicate)),
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
            },
            other => other,
        }
    }

    /// Extract predicates that reference only the specified variable
    fn extract_variable_predicates(predicate: &Expr, variable: &str) -> (Vec<Expr>, Option<Expr>) {
        let analyzer = PredicateAnalyzer::new(None);
        let analysis = analyzer.analyze(predicate, variable);

        // Return pushable predicates and combined residual
        let residual = if analysis.residual.is_empty() {
            None
        } else {
            let mut iter = analysis.residual.into_iter();
            let first = iter.next().unwrap();
            Some(iter.fold(first, |acc, e| Expr::BinaryOp {
                left: Box::new(acc),
                op: Operator::And,
                right: Box::new(e),
            }))
        };

        (analysis.pushable, residual)
    }

    // =====================================================================
    // Apply Predicate Pushdown - Helper Functions
    // =====================================================================

    /// Split AND-connected predicates into a list.
    fn split_and_conjuncts(expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: Operator::And,
                right,
            } => {
                let mut result = Self::split_and_conjuncts(left);
                result.extend(Self::split_and_conjuncts(right));
                result
            }
            _ => vec![expr.clone()],
        }
    }

    /// Combine predicates with AND.
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

    /// Collect all variable names referenced in an expression.
    fn collect_expr_variables(expr: &Expr) -> std::collections::HashSet<String> {
        let mut vars = std::collections::HashSet::new();
        Self::collect_expr_variables_impl(expr, &mut vars);
        vars
    }

    fn collect_expr_variables_impl(expr: &Expr, vars: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::Identifier(name) => {
                vars.insert(name.clone());
            }
            Expr::Property(inner, _) => {
                if let Expr::Identifier(name) = inner.as_ref() {
                    vars.insert(name.clone());
                } else {
                    Self::collect_expr_variables_impl(inner, vars);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_expr_variables_impl(left, vars);
                Self::collect_expr_variables_impl(right, vars);
            }
            Expr::UnaryOp { expr, .. } => Self::collect_expr_variables_impl(expr, vars),
            Expr::IsNull(e) | Expr::IsNotNull(e) => Self::collect_expr_variables_impl(e, vars),
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    Self::collect_expr_variables_impl(arg, vars);
                }
            }
            Expr::List(items) => {
                for item in items {
                    Self::collect_expr_variables_impl(item, vars);
                }
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                if let Some(e) = expr {
                    Self::collect_expr_variables_impl(e, vars);
                }
                for (w, t) in when_then {
                    Self::collect_expr_variables_impl(w, vars);
                    Self::collect_expr_variables_impl(t, vars);
                }
                if let Some(e) = else_expr {
                    Self::collect_expr_variables_impl(e, vars);
                }
            }
            _ => {}
        }
    }

    /// Collect all variables produced by a logical plan.
    fn collect_plan_variables(plan: &LogicalPlan) -> std::collections::HashSet<String> {
        let mut vars = std::collections::HashSet::new();
        Self::collect_plan_variables_impl(plan, &mut vars);
        vars
    }

    fn collect_plan_variables_impl(
        plan: &LogicalPlan,
        vars: &mut std::collections::HashSet<String>,
    ) {
        match plan {
            LogicalPlan::Scan { variable, .. } => {
                vars.insert(variable.clone());
            }
            LogicalPlan::Traverse {
                target_variable,
                step_variable,
                input,
                path_variable,
                ..
            } => {
                vars.insert(target_variable.clone());
                if let Some(sv) = step_variable {
                    vars.insert(sv.clone());
                }
                if let Some(pv) = path_variable {
                    vars.insert(pv.clone());
                }
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Filter { input, .. } => Self::collect_plan_variables_impl(input, vars),
            LogicalPlan::Project { input, projections } => {
                for (expr, alias) in projections {
                    if let Some(a) = alias {
                        vars.insert(a.clone());
                    } else if let Expr::Identifier(v) = expr {
                        vars.insert(v.clone());
                    }
                }
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Apply {
                input, subquery, ..
            } => {
                Self::collect_plan_variables_impl(input, vars);
                Self::collect_plan_variables_impl(subquery, vars);
            }
            LogicalPlan::CrossJoin { left, right } => {
                Self::collect_plan_variables_impl(left, vars);
                Self::collect_plan_variables_impl(right, vars);
            }
            LogicalPlan::Unwind {
                input, variable, ..
            } => {
                vars.insert(variable.clone());
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Aggregate { input, .. } => {
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Sort { input, .. } => {
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::Limit { input, .. } => {
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::VectorKnn { variable, .. } => {
                vars.insert(variable.clone());
            }
            LogicalPlan::ProcedureCall { yield_items, .. } => {
                for (name, alias) in yield_items {
                    vars.insert(alias.clone().unwrap_or_else(|| name.clone()));
                }
            }
            LogicalPlan::ShortestPath {
                input,
                path_variable,
                ..
            } => {
                vars.insert(path_variable.clone());
                Self::collect_plan_variables_impl(input, vars);
            }
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => {
                Self::collect_plan_variables_impl(initial, vars);
                Self::collect_plan_variables_impl(recursive, vars);
            }
            _ => {}
        }
    }

    /// Extract predicates that only reference variables from Apply's input.
    /// Returns (input_only_predicates, remaining_predicates).
    fn extract_apply_input_predicates(
        predicate: &Expr,
        input_variables: &std::collections::HashSet<String>,
        subquery_new_variables: &std::collections::HashSet<String>,
    ) -> (Vec<Expr>, Vec<Expr>) {
        let conjuncts = Self::split_and_conjuncts(predicate);
        let mut input_preds = Vec::new();
        let mut remaining = Vec::new();

        for conj in conjuncts {
            let vars = Self::collect_expr_variables(&conj);

            // Predicate only references input variables (none from subquery)
            let refs_input_only = vars.iter().all(|v| input_variables.contains(v));
            let refs_any_subquery = vars.iter().any(|v| subquery_new_variables.contains(v));

            if refs_input_only && !refs_any_subquery && !vars.is_empty() {
                input_preds.push(conj);
            } else {
                remaining.push(conj);
            }
        }

        (input_preds, remaining)
    }

    /// Push eligible predicates into Apply.input_filter.
    /// This filters input rows BEFORE executing the correlated subquery.
    fn push_predicates_to_apply(plan: LogicalPlan, current_predicate: &mut Expr) -> LogicalPlan {
        match plan {
            LogicalPlan::Apply {
                input,
                subquery,
                input_filter,
            } => {
                // Collect variables from input plan
                let input_vars = Self::collect_plan_variables(&input);

                // Collect NEW variables introduced by subquery (not in input)
                let subquery_vars = Self::collect_plan_variables(&subquery);
                let new_subquery_vars: std::collections::HashSet<String> =
                    subquery_vars.difference(&input_vars).cloned().collect();

                // Extract predicates that only reference input variables
                let (input_preds, remaining) = Self::extract_apply_input_predicates(
                    current_predicate,
                    &input_vars,
                    &new_subquery_vars,
                );

                // Update current_predicate to only remaining predicates
                *current_predicate = if remaining.is_empty() {
                    Expr::Literal(Value::Bool(true))
                } else {
                    Self::combine_predicates(remaining).unwrap()
                };

                // Combine extracted predicates with existing input_filter
                let new_input_filter = if input_preds.is_empty() {
                    input_filter
                } else {
                    let extracted = Self::combine_predicates(input_preds).unwrap();
                    match input_filter {
                        Some(existing) => Some(Expr::BinaryOp {
                            left: Box::new(existing),
                            op: Operator::And,
                            right: Box::new(extracted),
                        }),
                        None => Some(extracted),
                    }
                };

                // Recurse into input plan
                let new_input = Self::push_predicates_to_apply(*input, current_predicate);

                LogicalPlan::Apply {
                    input: Box::new(new_input),
                    subquery,
                    input_filter: new_input_filter,
                }
            }
            // Recurse into other plan nodes
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                predicate,
            },
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                projections,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                order_by,
            },
            LogicalPlan::Limit { input, skip, fetch } => LogicalPlan::Limit {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                skip,
                fetch,
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
                group_by,
                aggregates,
            },
            LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
                left: Box::new(Self::push_predicates_to_apply(*left, current_predicate)),
                right: Box::new(Self::push_predicates_to_apply(*right, current_predicate)),
            },
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
            } => LogicalPlan::Traverse {
                input: Box::new(Self::push_predicates_to_apply(*input, current_predicate)),
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
            },
            other => other,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExplainOutput {
    pub plan_text: String,
    pub index_usage: Vec<IndexUsage>,
    pub cost_estimates: CostEstimates,
    pub warnings: Vec<String>,
    pub suggestions: Vec<IndexSuggestion>,
}

/// Suggestion for creating an index to improve query performance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexSuggestion {
    pub label_or_type: String,
    pub property: String,
    pub index_type: String,
    pub reason: String,
    pub create_statement: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexUsage {
    pub label_or_type: String,
    pub property: String,
    pub index_type: String,
    pub used: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CostEstimates {
    pub estimated_rows: f64,
    pub estimated_cost: f64,
}

impl QueryPlanner {
    pub fn explain_plan(&self, ast: Query) -> Result<ExplainOutput> {
        let plan = match ast {
            Query::Explain(inner) => self.plan(*inner)?,
            _ => self.plan(ast)?,
        };
        self.explain_logical_plan(&plan)
    }

    pub fn explain_logical_plan(&self, plan: &LogicalPlan) -> Result<ExplainOutput> {
        let index_usage = self.analyze_index_usage(plan)?;
        let cost_estimates = self.estimate_costs(plan)?;
        let suggestions = self.collect_index_suggestions(plan);
        let warnings = Vec::new();
        let plan_text = format!("{:#?}", plan);

        Ok(ExplainOutput {
            plan_text,
            index_usage,
            cost_estimates,
            warnings,
            suggestions,
        })
    }

    fn analyze_index_usage(&self, plan: &LogicalPlan) -> Result<Vec<IndexUsage>> {
        let mut usage = Vec::new();
        self.collect_index_usage(plan, &mut usage);
        Ok(usage)
    }

    fn collect_index_usage(&self, plan: &LogicalPlan, usage: &mut Vec<IndexUsage>) {
        match plan {
            LogicalPlan::Scan { .. } => {
                // Placeholder: Scan might use index if it was optimized
                // Ideally LogicalPlan::Scan should store if it uses index.
                // But typically Planner converts Scan to specific index scan or we infer it here.
            }
            LogicalPlan::VectorKnn {
                label_id, property, ..
            } => {
                let label_name = self.schema.label_name_by_id(*label_id).unwrap_or("?");
                usage.push(IndexUsage {
                    label_or_type: label_name.to_string(),
                    property: property.clone(),
                    index_type: "VECTOR".to_string(),
                    used: true,
                    reason: None,
                });
            }
            LogicalPlan::Explain { plan } => self.collect_index_usage(plan, usage),
            LogicalPlan::Filter { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Project { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Limit { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Sort { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Aggregate { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Traverse { input, .. } => self.collect_index_usage(input, usage),
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                self.collect_index_usage(left, usage);
                self.collect_index_usage(right, usage);
            }
            _ => {}
        }
    }

    fn estimate_costs(&self, _plan: &LogicalPlan) -> Result<CostEstimates> {
        Ok(CostEstimates {
            estimated_rows: 100.0,
            estimated_cost: 10.0,
        })
    }

    /// Collect index suggestions based on query patterns.
    ///
    /// Currently detects:
    /// - Temporal predicates from `uni.validAt()` function calls
    /// - Temporal predicates from `VALID_AT` macro expansion
    fn collect_index_suggestions(&self, plan: &LogicalPlan) -> Vec<IndexSuggestion> {
        let mut suggestions = Vec::new();
        self.collect_temporal_suggestions(plan, &mut suggestions);
        suggestions
    }

    /// Recursively collect temporal index suggestions from the plan.
    fn collect_temporal_suggestions(
        &self,
        plan: &LogicalPlan,
        suggestions: &mut Vec<IndexSuggestion>,
    ) {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                // Check for temporal patterns in the predicate
                self.detect_temporal_pattern(predicate, suggestions);
                // Recurse into input
                self.collect_temporal_suggestions(input, suggestions);
            }
            LogicalPlan::Explain { plan } => self.collect_temporal_suggestions(plan, suggestions),
            LogicalPlan::Project { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Limit { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Sort { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Aggregate { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Traverse { input, .. } => {
                self.collect_temporal_suggestions(input, suggestions)
            }
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                self.collect_temporal_suggestions(left, suggestions);
                self.collect_temporal_suggestions(right, suggestions);
            }
            _ => {}
        }
    }

    /// Detect temporal predicate patterns and suggest indexes.
    ///
    /// Detects two patterns:
    /// 1. `uni.validAt(node, 'start_prop', 'end_prop', time)` function call
    /// 2. `node.valid_from <= time AND (node.valid_to IS NULL OR node.valid_to > time)` from VALID_AT macro
    fn detect_temporal_pattern(&self, expr: &Expr, suggestions: &mut Vec<IndexSuggestion>) {
        use crate::query::ast::{Expr, Operator};

        match expr {
            // Pattern 1: uni.validAt() function call
            Expr::FunctionCall { name, args, .. }
                if name.eq_ignore_ascii_case("uni.validAt")
                    || name.eq_ignore_ascii_case("validAt") =>
            {
                // args[0] = node, args[1] = start_prop, args[2] = end_prop, args[3] = time
                if args.len() >= 2 {
                    let start_prop =
                        if let Some(Expr::Literal(serde_json::Value::String(s))) = args.get(1) {
                            s.clone()
                        } else {
                            "valid_from".to_string()
                        };

                    // Try to extract label from the node expression
                    if let Some(var) = args.first().and_then(|e| e.extract_variable()) {
                        self.suggest_temporal_index(&var, &start_prop, suggestions);
                    }
                }
            }

            // Pattern 2: VALID_AT macro expansion - look for property <= time pattern
            Expr::BinaryOp {
                left,
                op: Operator::And,
                right,
            } => {
                // Check left side for `prop <= time` pattern (temporal start condition)
                if let Expr::BinaryOp {
                    left: prop_expr,
                    op: Operator::LtEq,
                    ..
                } = left.as_ref()
                    && let Expr::Property(base, prop_name) = prop_expr.as_ref()
                    && (prop_name == "valid_from"
                        || prop_name.contains("start")
                        || prop_name.contains("from")
                        || prop_name.contains("begin"))
                    && let Some(var) = base.extract_variable()
                {
                    self.suggest_temporal_index(&var, prop_name, suggestions);
                }

                // Recurse into both sides of AND
                self.detect_temporal_pattern(left, suggestions);
                self.detect_temporal_pattern(right, suggestions);
            }

            // Recurse into other binary ops
            Expr::BinaryOp { left, right, .. } => {
                self.detect_temporal_pattern(left, suggestions);
                self.detect_temporal_pattern(right, suggestions);
            }

            _ => {}
        }
    }

    /// Suggest a scalar index for a temporal property if one doesn't already exist.
    fn suggest_temporal_index(
        &self,
        _variable: &str,
        property: &str,
        suggestions: &mut Vec<IndexSuggestion>,
    ) {
        // Check if a scalar index already exists for this property
        // We need to check all labels since we may not know the exact label from the variable
        let mut has_index = false;

        for index in &self.schema.indexes {
            if let IndexDefinition::Scalar(config) = index
                && config.properties.contains(&property.to_string())
            {
                has_index = true;
                break;
            }
        }

        if !has_index {
            // Avoid duplicate suggestions
            let already_suggested = suggestions.iter().any(|s| s.property == property);
            if !already_suggested {
                suggestions.push(IndexSuggestion {
                    label_or_type: "(detected from temporal query)".to_string(),
                    property: property.to_string(),
                    index_type: "SCALAR (BTree)".to_string(),
                    reason: format!(
                        "Temporal queries using '{}' can benefit from a scalar index for range scans",
                        property
                    ),
                    create_statement: format!(
                        "CREATE INDEX idx_{} FOR (n:YourLabel) ON (n.{})",
                        property, property
                    ),
                });
            }
        }
    }
}
