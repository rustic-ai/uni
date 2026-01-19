// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::expr::{Expr, Operator, UnaryOperator};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

use uni_common::core::id::UniId;
use uni_common::core::schema::Schema;

/// Categorized pushdown strategy for predicates with index awareness.
///
/// This struct represents the optimal execution path for predicates,
/// routing them to the most selective index when available.
#[derive(Debug, Clone, Default)]
pub struct PushdownStrategy {
    /// UID lookup predicate: WHERE n._uid = 'base32string'
    /// Contains the UniId parsed from the predicate value.
    pub uid_lookup: Option<UniId>,

    /// JsonPath index lookups: WHERE n.path = 'value' for indexed paths.
    /// Vec of: (json_path, value)
    pub jsonpath_lookups: Vec<(String, String)>,

    /// JSON FTS (Full-Text Search) predicates: WHERE n._doc CONTAINS 'term'.
    /// Vec of: (column, query_string, optional_path_filter)
    pub json_fts_predicates: Vec<(String, String, Option<String>)>,

    /// Predicates pushable to Lance scan filter.
    pub lance_predicates: Vec<Expr>,

    /// Residual predicates (not pushable to storage).
    pub residual: Vec<Expr>,
}

/// Analyzer that considers available indexes when categorizing predicates.
///
/// Unlike `PredicateAnalyzer` which only categorizes into pushable/residual,
/// this analyzer routes predicates to the most optimal execution path:
/// 1. UID index lookup (most selective, O(1) lookup)
/// 2. JsonPath index lookup (secondary index)
/// 3. Lance scan filter (columnar scan with filter)
/// 4. Residual (post-scan evaluation)
pub struct IndexAwareAnalyzer<'a> {
    schema: &'a Schema,
}

impl<'a> IndexAwareAnalyzer<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    /// Analyze predicates and determine optimal pushdown strategy.
    ///
    /// Predicates are categorized in order of selectivity:
    /// 1. `_uid = 'xxx'` -> UID index lookup
    /// 2. `column CONTAINS 'term'` -> JSON FTS lookup (BM25)
    /// 3. `prop = 'value'` where prop has json_index -> JsonPath index lookup
    /// 4. Pushable to Lance -> Lance filter
    /// 5. Everything else -> Residual
    pub fn analyze(&self, predicate: &Expr, variable: &str, label_id: u16) -> PushdownStrategy {
        let mut strategy = PushdownStrategy::default();
        let conjuncts = Self::split_conjuncts(predicate);

        for conj in conjuncts {
            // 1. Check for _uid = 'xxx' pattern (most selective)
            if let Some(uid) = self.extract_uid_predicate(&conj, variable) {
                strategy.uid_lookup = Some(uid);
                continue;
            }

            // 2. Check for JSON FTS predicates (CONTAINS operator on FTS-indexed columns)
            if let Some((column, query, path)) =
                self.extract_json_fts_predicate(&conj, variable, label_id)
            {
                strategy.json_fts_predicates.push((column, query, path));
                continue;
            }

            // 3. Check for indexed json path predicates
            if let Some((path, value)) = self.extract_jsonpath_predicate(&conj, variable, label_id)
            {
                strategy.jsonpath_lookups.push((path, value));
                continue;
            }

            // 4. Check if pushable to Lance
            let analyzer = PredicateAnalyzer::new(None);
            if analyzer.is_pushable(&conj, variable) {
                strategy.lance_predicates.push(conj);
            } else {
                strategy.residual.push(conj);
            }
        }

        strategy
    }

    /// Extract UniId from `_uid = 'xxx'` predicate.
    ///
    /// # Security
    ///
    /// **CWE-345 (Insufficient Verification)**: The UID value is validated using
    /// `UniId::from_multibase()` which enforces Base32Lower encoding and 32-byte
    /// length. Invalid UIDs are rejected and the predicate becomes residual.
    fn extract_uid_predicate(&self, expr: &Expr, variable: &str) -> Option<UniId> {
        if let Expr::BinaryOp {
            left,
            op: Operator::Eq,
            right,
        } = expr
            && let Expr::Property(var_expr, prop) = left.as_ref()
            && let Expr::Identifier(v) = var_expr.as_ref()
            && v == variable
            && prop == "_uid"
            && let Expr::Literal(Value::String(s)) = right.as_ref()
        {
            // Security: UniId::from_multibase validates Base32Lower and 32-byte length
            return UniId::from_multibase(s).ok();
        }
        None
    }

    /// Extract JSON FTS predicate if an index exists for the column.
    ///
    /// Returns `Some((column, query_string, optional_path))` if:
    /// - The predicate is `variable.column CONTAINS 'search_term'`
    /// - The label has a json_fts_index defined for that column
    ///
    /// Supports two patterns:
    /// - `n._doc CONTAINS 'term'` -> FTS on entire document
    /// - `n._doc.title CONTAINS 'term'` -> FTS on specific path (path:term query)
    fn extract_json_fts_predicate(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<(String, String, Option<String>)> {
        if let Expr::BinaryOp {
            left,
            op: Operator::Contains,
            right,
        } = expr
        {
            // Get the query string from the right side
            let query_string = match right.as_ref() {
                Expr::Literal(Value::String(s)) => s.clone(),
                _ => return None,
            };

            // Check for `variable.column CONTAINS 'term'` pattern
            if let Expr::Property(var_expr, column) = left.as_ref() {
                if let Expr::Identifier(v) = var_expr.as_ref()
                    && v == variable
                {
                    // Check if column has a JSON FTS index
                    let label_name = self.schema.label_name_by_id(label_id)?;
                    // Verify label exists
                    let _ = self.schema.labels.get(label_name)?;

                    // Look for JSON FTS index on this column
                    for idx in &self.schema.indexes {
                        if let uni_common::core::schema::IndexDefinition::JsonFullText(cfg) = idx
                            && cfg.label == *label_name
                            && cfg.column == *column
                        {
                            return Some((column.clone(), query_string, None));
                        }
                    }

                    // Also check for path-specific search: `n._doc.title CONTAINS 'term'`
                    // If column is a path like "_doc", look for nested property access
                    // This would be parsed as Property(Property(Identifier, "_doc"), "title")
                    // But our current pattern is Property(Identifier, column)
                    // So we need to handle nested properties differently
                }

                // Handle nested property access for path-specific FTS
                // Pattern: n._doc.title CONTAINS 'term' -> Property(Property(Identifier(n), _doc), title)
                if let Expr::Property(inner_var, base_column) = var_expr.as_ref()
                    && let Expr::Identifier(v) = inner_var.as_ref()
                    && v == variable
                {
                    let label_name = self.schema.label_name_by_id(label_id)?;

                    // Look for JSON FTS index on base_column
                    for idx in &self.schema.indexes {
                        if let uni_common::core::schema::IndexDefinition::JsonFullText(cfg) = idx
                            && cfg.label == *label_name
                            && cfg.column == *base_column
                        {
                            // Path is the column from the outer Property
                            let path = format!("$.{}", column);
                            return Some((base_column.clone(), query_string, Some(path)));
                        }
                    }
                }
            }
        }
        None
    }

    /// Extract indexed JsonPath predicate if an index exists for the property.
    ///
    /// Returns `Some((path, value))` if:
    /// - The predicate is `variable.property = 'literal_string'`
    /// - The label has a json_index defined for that property
    fn extract_jsonpath_predicate(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<(String, String)> {
        if let Expr::BinaryOp {
            left,
            op: Operator::Eq,
            right,
        } = expr
            && let Expr::Property(var_expr, prop) = left.as_ref()
            && let Expr::Identifier(v) = var_expr.as_ref()
            && v == variable
        {
            // Check if this property has a json_index
            let label_name = self.schema.label_name_by_id(label_id)?;
            let label_meta = self.schema.labels.get(label_name)?;

            // Check if path matches any json_index ($.prop or prop)
            let json_path = format!("$.{}", prop);
            let has_index = label_meta
                .json_indexes
                .iter()
                .any(|idx| idx.path == json_path || idx.path == *prop);

            if has_index && let Expr::Literal(Value::String(val)) = right.as_ref() {
                return Some((prop.clone(), val.clone()));
            }
        }
        None
    }

    /// Split AND-connected predicates into a list.
    fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: Operator::And,
                right,
            } => {
                let mut result = Self::split_conjuncts(left);
                result.extend(Self::split_conjuncts(right));
                result
            }
            _ => vec![expr.clone()],
        }
    }
}

pub struct PredicateAnalysis {
    /// Predicates that can be pushed to storage
    pub pushable: Vec<Expr>,
    /// Predicates that must be evaluated post-scan
    pub residual: Vec<Expr>,
    /// Properties needed for residual evaluation
    pub required_properties: Vec<String>,
}

pub struct PredicateAnalyzer {
    #[allow(dead_code)]
    schema: Option<Arc<Schema>>,
}

impl PredicateAnalyzer {
    pub fn new(schema: Option<Arc<Schema>>) -> Self {
        Self { schema }
    }

    /// Analyze a predicate and determine pushdown strategy
    pub fn analyze(&self, predicate: &Expr, scan_variable: &str) -> PredicateAnalysis {
        let mut pushable = Vec::new();
        let mut residual = Vec::new();

        self.split_conjuncts(predicate, scan_variable, &mut pushable, &mut residual);

        let required_properties = self.extract_properties(&residual, scan_variable);

        PredicateAnalysis {
            pushable,
            residual,
            required_properties,
        }
    }

    /// Split AND-connected predicates
    fn split_conjuncts(
        &self,
        expr: &Expr,
        variable: &str,
        pushable: &mut Vec<Expr>,
        residual: &mut Vec<Expr>,
    ) {
        // Try OR-to-IN conversion first
        if let Some(in_expr) = try_or_to_in(expr, variable)
            && self.is_pushable(&in_expr, variable)
        {
            pushable.push(in_expr);
            return;
        }

        match expr {
            Expr::BinaryOp {
                left,
                op: Operator::And,
                right,
            } => {
                self.split_conjuncts(left, variable, pushable, residual);
                self.split_conjuncts(right, variable, pushable, residual);
            }
            _ => {
                if self.is_pushable(expr, variable) {
                    pushable.push(expr.clone());
                } else {
                    residual.push(expr.clone());
                }
            }
        }
    }

    /// Check if a predicate can be pushed to Lance
    #[allow(clippy::only_used_in_recursion)]
    pub fn is_pushable(&self, expr: &Expr, variable: &str) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check operator is supported
                let op_supported = matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::In
                        | Operator::Contains
                        | Operator::StartsWith
                        | Operator::EndsWith
                );

                if !op_supported {
                    return false;
                }

                // Check left side is a property of the scan variable
                // Structure: Property(Identifier(var), prop_name)
                let left_is_property = matches!(
                    left.as_ref(),
                    Expr::Property(box_expr, _) if matches!(box_expr.as_ref(), Expr::Identifier(v) if v == variable)
                );

                // Check right side is a literal or parameter or list of literals
                // For string operators, strict requirement on String Literal
                let right_valid = if matches!(
                    op,
                    Operator::Contains | Operator::StartsWith | Operator::EndsWith
                ) {
                    matches!(right.as_ref(), Expr::Literal(Value::String(_)))
                } else {
                    matches!(
                        right.as_ref(),
                        Expr::Literal(_) | Expr::Parameter(_) | Expr::List(_)
                    )
                };

                left_is_property && right_valid
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => self.is_pushable(expr, variable),

            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                // Check if inner is a property of the scan variable
                matches!(
                    inner.as_ref(),
                    Expr::Property(var_expr, _)
                        if matches!(var_expr.as_ref(), Expr::Identifier(v) if v == variable)
                )
            }

            _ => false,
        }
    }

    /// Extract property names required by residual predicates
    fn extract_properties(&self, exprs: &[Expr], variable: &str) -> Vec<String> {
        let mut props = HashSet::new();
        for expr in exprs {
            collect_properties(expr, variable, &mut props);
        }
        props.into_iter().collect()
    }
}

/// Attempt to convert OR disjunctions to IN predicates
fn try_or_to_in(expr: &Expr, variable: &str) -> Option<Expr> {
    match expr {
        Expr::BinaryOp {
            op: Operator::Or, ..
        } => {
            // Collect all equality comparisons on the same property
            let mut property: Option<String> = None;
            let mut values: Vec<Expr> = Vec::new();

            if collect_or_equals(expr, variable, &mut property, &mut values)
                && let Some(prop) = property
                && values.len() >= 2
            {
                return Some(Expr::BinaryOp {
                    left: Box::new(Expr::Property(
                        Box::new(Expr::Identifier(variable.to_string())),
                        prop,
                    )),
                    op: Operator::In,
                    right: Box::new(Expr::List(values)),
                });
            }
            None
        }
        _ => None,
    }
}

fn collect_or_equals(
    expr: &Expr,
    variable: &str,
    property: &mut Option<String>,
    values: &mut Vec<Expr>,
) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: Operator::Or,
            right,
        } => {
            collect_or_equals(left, variable, property, values)
                && collect_or_equals(right, variable, property, values)
        }
        Expr::BinaryOp {
            left,
            op: Operator::Eq,
            right,
        } => {
            if let Expr::Property(var_expr, prop) = left.as_ref()
                && let Expr::Identifier(v) = var_expr.as_ref()
                && v == variable
            {
                match property {
                    None => {
                        *property = Some(prop.clone());
                        values.push(right.as_ref().clone());
                        return true;
                    }
                    Some(p) if p == prop => {
                        values.push(right.as_ref().clone());
                        return true;
                    }
                    _ => return false, // Different properties
                }
            }
            false
        }
        _ => false,
    }
}

fn collect_properties(expr: &Expr, variable: &str, props: &mut HashSet<String>) {
    match expr {
        Expr::Property(box_expr, prop) => {
            if let Expr::Identifier(v) = box_expr.as_ref()
                && v == variable
            {
                props.insert(prop.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_properties(left, variable, props);
            collect_properties(right, variable, props);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_properties(expr, variable, props);
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            collect_properties(expr, variable, props);
        }
        Expr::List(items) => {
            for item in items {
                collect_properties(item, variable, props);
            }
        }
        Expr::Map(items) => {
            for (_, item) in items {
                collect_properties(item, variable, props);
            }
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_properties(arg, variable, props);
            }
        }
        Expr::ArrayIndex(arr, idx) => {
            collect_properties(arr, variable, props);
            collect_properties(idx, variable, props);
        }
        _ => {}
    }
}

/// Flatten nested AND expressions into a vector
fn flatten_ands(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: Operator::And,
            right,
        } => {
            let mut result = flatten_ands(left);
            result.extend(flatten_ands(right));
            result
        }
        _ => vec![expr],
    }
}

pub struct LanceFilterGenerator;

impl LanceFilterGenerator {
    /// Checks if a string contains SQL LIKE wildcard characters.
    ///
    /// # Security
    ///
    /// **CWE-89 (SQL Injection)**: Predicates containing wildcards are NOT pushed
    /// to storage because Lance DataFusion doesn't support the ESCAPE clause.
    /// Instead, they're evaluated at the application layer where we have full
    /// control over string matching semantics.
    fn contains_sql_wildcards(s: &str) -> bool {
        s.contains('%') || s.contains('_')
    }

    /// Escapes special characters in LIKE patterns.
    ///
    /// **Note**: This function is kept for documentation and potential future use,
    /// but currently we do not push down LIKE patterns containing wildcards
    /// because Lance DataFusion doesn't support the ESCAPE clause.
    #[expect(
        dead_code,
        reason = "Reserved for future use when Lance supports ESCAPE"
    )]
    fn escape_like_pattern(s: &str) -> String {
        s.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
            .replace('\'', "''")
    }

    /// Converts pushable predicates to Lance SQL filter string.
    pub fn generate(predicates: &[Expr], variable: &str) -> Option<String> {
        if predicates.is_empty() {
            return None;
        }

        // Flatten nested ANDs first
        let flattened: Vec<&Expr> = predicates.iter().flat_map(|p| flatten_ands(p)).collect();

        // Optimize Ranges: Group predicates by column and combine into >= AND <= if possible
        let mut by_column: std::collections::HashMap<String, Vec<&Expr>> =
            std::collections::HashMap::new();
        let mut optimized_filters: Vec<String> = Vec::new();
        let mut used_expressions: std::collections::HashSet<*const Expr> =
            std::collections::HashSet::new();

        for expr in flattened.iter() {
            if let Some(col) = Self::extract_column_from_range(expr, variable) {
                by_column.entry(col).or_default().push(expr);
            }
        }

        for (col, exprs) in &by_column {
            if exprs.len() < 2 {
                continue;
            }

            // Try to find pairs of >/>= and </<=
            // Very naive: find ONE pair and emit range expression.
            // Complex ranges (e.g. >10 AND >20) are not merged but valid.
            // We look for: (col > L OR col >= L) AND (col < R OR col <= R)

            let mut lower: Option<(bool, &Expr, &Expr)> = None; // (inclusive, val_expr, original_expr)
            let mut upper: Option<(bool, &Expr, &Expr)> = None;

            for expr in exprs {
                if let Expr::BinaryOp { op, right, .. } = expr {
                    match op {
                        Operator::Gt => {
                            // If we have multiple lower bounds, pick the last one (arbitrary for now, intersection handles logic)
                            lower = Some((false, right, expr));
                        }
                        Operator::GtEq => {
                            lower = Some((true, right, expr));
                        }
                        Operator::Lt => {
                            upper = Some((false, right, expr));
                        }
                        Operator::LtEq => {
                            upper = Some((true, right, expr));
                        }
                        _ => {}
                    }
                }
            }

            if let (Some((true, l_val, l_expr)), Some((true, u_val, u_expr))) = (lower, upper) {
                // Both inclusive -> use >= AND <= (Lance doesn't support BETWEEN)
                if let (Some(l_str), Some(u_str)) =
                    (Self::value_to_lance(l_val), Self::value_to_lance(u_val))
                {
                    optimized_filters.push(format!(
                        "\"{}\" >= {} AND \"{}\" <= {}",
                        col, l_str, col, u_str
                    ));
                    used_expressions.insert(l_expr as *const Expr);
                    used_expressions.insert(u_expr as *const Expr);
                }
            }
        }

        let mut filters = optimized_filters;

        for expr in flattened {
            if used_expressions.contains(&(expr as *const Expr)) {
                continue;
            }
            if let Some(s) = Self::expr_to_lance(expr, variable) {
                filters.push(s);
            }
        }

        if filters.is_empty() {
            None
        } else {
            Some(filters.join(" AND "))
        }
    }

    fn extract_column_from_range(expr: &Expr, variable: &str) -> Option<String> {
        match expr {
            Expr::BinaryOp { left, op, .. } => {
                if matches!(
                    op,
                    Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
                ) {
                    return Self::extract_column(left, variable);
                }
                None
            }
            _ => None,
        }
    }

    fn expr_to_lance(expr: &Expr, variable: &str) -> Option<String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let column = Self::extract_column(left, variable)?;

                // Special handling for string operators
                // Security: CWE-89 - Prevent SQL wildcard injection
                //
                // Lance DataFusion doesn't support the ESCAPE clause, so we cannot
                // safely push down LIKE predicates containing SQL wildcards (% or _).
                // If the input contains these characters, we return None to keep
                // the predicate as a residual for application-level evaluation.
                match op {
                    Operator::Contains | Operator::StartsWith | Operator::EndsWith => {
                        let raw_value = Self::get_string_value(right)?;

                        // If the value contains SQL wildcards, don't push down
                        // to prevent wildcard injection attacks
                        if Self::contains_sql_wildcards(&raw_value) {
                            return None;
                        }

                        // Escape single quotes for the SQL string
                        let escaped = raw_value.replace('\'', "''");

                        match op {
                            Operator::Contains => Some(format!("{} LIKE '%{}%'", column, escaped)),
                            Operator::StartsWith => Some(format!("{} LIKE '{}%'", column, escaped)),
                            Operator::EndsWith => Some(format!("{} LIKE '%{}'", column, escaped)),
                            _ => unreachable!(),
                        }
                    }
                    _ => {
                        let op_str = Self::op_to_lance(op)?;
                        let value = Self::value_to_lance(right)?;
                        // Use unquoted column name for DataFusion compatibility
                        // DataFusion treats unquoted identifiers case-insensitively
                        Some(format!("{} {} {}", column, op_str, value))
                    }
                }
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => {
                let inner = Self::expr_to_lance(expr, variable)?;
                Some(format!("NOT ({})", inner))
            }
            Expr::IsNull(inner) => {
                let column = Self::extract_column(inner, variable)?;
                Some(format!("{} IS NULL", column))
            }
            Expr::IsNotNull(inner) => {
                let column = Self::extract_column(inner, variable)?;
                Some(format!("{} IS NOT NULL", column))
            }
            _ => None,
        }
    }

    fn extract_column(expr: &Expr, variable: &str) -> Option<String> {
        match expr {
            Expr::Property(box_expr, prop) => {
                if let Expr::Identifier(var) = box_expr.as_ref()
                    && var == variable
                {
                    return Some(prop.clone());
                }
                None
            }
            _ => None,
        }
    }

    fn op_to_lance(op: &Operator) -> Option<&'static str> {
        match op {
            Operator::Eq => Some("="),
            Operator::NotEq => Some("!="),
            Operator::Lt => Some("<"),
            Operator::LtEq => Some("<="),
            Operator::Gt => Some(">"),
            Operator::GtEq => Some(">="),
            Operator::In => Some("IN"),
            _ => None,
        }
    }

    fn value_to_lance(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal(Value::String(s)) => Some(format!("'{}'", s.replace("'", "''"))),
            Expr::Literal(Value::Number(n)) => Some(n.to_string()),
            Expr::Literal(Value::Bool(b)) => Some(b.to_string()),
            Expr::Literal(Value::Null) => Some("NULL".to_string()),
            Expr::List(items) => {
                let values: Option<Vec<String>> = items.iter().map(Self::value_to_lance).collect();
                values.map(|v| format!("({})", v.join(", ")))
            }
            // Security: CWE-89 - Parameters are NOT pushed to storage layer.
            // Parameterized predicates stay in the application layer where the
            // query executor can safely substitute values with proper type handling.
            // This prevents potential SQL injection if Lance doesn't support the $name syntax.
            Expr::Parameter(_) => None,
            _ => None,
        }
    }

    /// Extracts raw string value from expression for LIKE pattern use.
    ///
    /// Returns the raw string without escaping - escaping is handled by
    /// `escape_like_pattern` for LIKE clauses.
    fn get_string_value(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal(Value::String(s)) => Some(s.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod security_tests {
    use super::*;

    /// Tests for CWE-89 (SQL Injection) prevention in LIKE patterns.
    mod wildcard_protection {
        use super::*;

        #[test]
        fn test_contains_sql_wildcards_detects_percent() {
            assert!(LanceFilterGenerator::contains_sql_wildcards("admin%"));
            assert!(LanceFilterGenerator::contains_sql_wildcards("%admin"));
            assert!(LanceFilterGenerator::contains_sql_wildcards("ad%min"));
        }

        #[test]
        fn test_contains_sql_wildcards_detects_underscore() {
            assert!(LanceFilterGenerator::contains_sql_wildcards("a_min"));
            assert!(LanceFilterGenerator::contains_sql_wildcards("_admin"));
            assert!(LanceFilterGenerator::contains_sql_wildcards("admin_"));
        }

        #[test]
        fn test_contains_sql_wildcards_safe_strings() {
            assert!(!LanceFilterGenerator::contains_sql_wildcards("admin"));
            assert!(!LanceFilterGenerator::contains_sql_wildcards("John Smith"));
            assert!(!LanceFilterGenerator::contains_sql_wildcards(
                "test@example.com"
            ));
        }

        #[test]
        fn test_wildcard_in_contains_not_pushed_down() {
            // Input with % should NOT be pushed to storage
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Identifier("n".to_string())),
                    "name".to_string(),
                )),
                op: Operator::Contains,
                right: Box::new(Expr::Literal(Value::String("admin%".to_string()))),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n");
            assert!(
                filter.is_none(),
                "CONTAINS with wildcard should not be pushed to storage"
            );
        }

        #[test]
        fn test_underscore_in_startswith_not_pushed_down() {
            // Input with _ should NOT be pushed to storage
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Identifier("n".to_string())),
                    "name".to_string(),
                )),
                op: Operator::StartsWith,
                right: Box::new(Expr::Literal(Value::String("user_".to_string()))),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n");
            assert!(
                filter.is_none(),
                "STARTSWITH with underscore should not be pushed to storage"
            );
        }

        #[test]
        fn test_safe_contains_is_pushed_down() {
            // Input without wildcards SHOULD be pushed to storage
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Identifier("n".to_string())),
                    "name".to_string(),
                )),
                op: Operator::Contains,
                right: Box::new(Expr::Literal(Value::String("admin".to_string()))),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n");
            assert!(filter.is_some(), "Safe CONTAINS should be pushed down");
            assert!(
                filter.as_ref().unwrap().contains("LIKE '%admin%'"),
                "Generated filter: {:?}",
                filter
            );
        }

        #[test]
        fn test_single_quotes_escaped_in_safe_string() {
            // Single quotes should be doubled in safe strings
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Identifier("n".to_string())),
                    "name".to_string(),
                )),
                op: Operator::Contains,
                right: Box::new(Expr::Literal(Value::String("O'Brien".to_string()))),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n").unwrap();
            assert!(
                filter.contains("O''Brien"),
                "Single quotes should be doubled: {}",
                filter
            );
        }
    }

    /// Tests for parameter handling (not pushed to storage).
    mod parameter_safety {
        use super::*;

        #[test]
        fn test_parameters_not_pushed_down() {
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Identifier("n".to_string())),
                    "name".to_string(),
                )),
                op: Operator::Eq,
                right: Box::new(Expr::Parameter("userInput".to_string())),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n");
            assert!(
                filter.is_none(),
                "Parameterized predicates should not be pushed to storage"
            );
        }
    }
}
