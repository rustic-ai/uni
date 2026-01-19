// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Predicate subsumption checking for partial index matching.
//!
//! This module provides functionality to check if a query's predicates
//! subsume (imply) a partial index's WHERE clause, enabling automatic
//! partial index selection during query planning.

use crate::query::expr::{Expr, Operator};
use std::collections::HashSet;

/// Result of predicate subsumption analysis.
#[derive(Debug, Clone)]
pub struct SubsumptionResult {
    /// True if query predicates subsume the index predicate.
    pub subsumes: bool,
    /// Remaining query predicates after removing index-matched ones.
    pub residual: Option<Expr>,
}

/// Checks if query_predicates subsume (imply) index_predicate.
///
/// For MVP, we use a simple conjunct-based approach:
/// - Flatten both predicates into sets of conjuncts (AND-connected terms)
/// - Check if all index conjuncts appear in query conjuncts
/// - Return the residual (unmatched query conjuncts)
///
/// # Arguments
/// * `query_predicates` - The WHERE clause from the query
/// * `index_predicate` - The WHERE clause from the partial index
/// * `query_variable` - The variable name used in the query (e.g., "u")
/// * `index_variable` - The variable name used in the index definition (e.g., "p")
///
/// # Example
/// ```text
/// Index: CREATE INDEX idx FOR (u:User) ON (u.email) WHERE u.active = true
/// Query: MATCH (p:User) WHERE p.active = true AND p.email = 'alice@example.com'
///
/// check_subsumption(query_pred, index_pred, "p", "u")
/// -> SubsumptionResult { subsumes: true, residual: Some(p.email = 'alice@example.com') }
/// ```
pub fn check_subsumption(
    query_predicates: &Expr,
    index_predicate: &Expr,
    query_variable: &str,
    index_variable: &str,
) -> SubsumptionResult {
    // 1. Normalize index predicate to use query's variable name
    let normalized_index = index_predicate.substitute_variable(index_variable, query_variable);

    // 2. Flatten both into conjuncts (AND-connected predicates)
    let query_conjuncts = flatten_conjuncts(query_predicates);
    let index_conjuncts = flatten_conjuncts(&normalized_index);

    // 3. Check if all index conjuncts are present in query conjuncts
    let mut matched_indices = HashSet::new();
    for idx_conj in &index_conjuncts {
        let mut found = false;
        for (i, q_conj) in query_conjuncts.iter().enumerate() {
            if predicates_equivalent(q_conj, idx_conj) {
                matched_indices.insert(i);
                found = true;
                break;
            }
        }
        if !found {
            return SubsumptionResult {
                subsumes: false,
                residual: None,
            };
        }
    }

    // 4. Build residual from unmatched query conjuncts
    let residual_conjuncts: Vec<_> = query_conjuncts
        .iter()
        .enumerate()
        .filter(|(i, _)| !matched_indices.contains(i))
        .map(|(_, e)| e.clone())
        .collect();

    let residual = combine_conjuncts(residual_conjuncts);

    SubsumptionResult {
        subsumes: true,
        residual,
    }
}

/// Flatten AND-connected predicates into a vector of conjuncts.
///
/// For example: `a AND b AND c` becomes `[a, b, c]`
fn flatten_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: Operator::And,
            right,
        } => {
            let mut result = flatten_conjuncts(left);
            result.extend(flatten_conjuncts(right));
            result
        }
        _ => vec![expr.clone()],
    }
}

/// Check if two predicates are semantically equivalent.
///
/// For MVP: Uses structural equality since Expr derives PartialEq.
/// This works for simple cases like `u.active = true`.
fn predicates_equivalent(a: &Expr, b: &Expr) -> bool {
    a == b
}

/// Combine conjuncts back into an AND expression.
///
/// For example: `[a, b, c]` becomes `a AND b AND c`
fn combine_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    if conjuncts.is_empty() {
        return None;
    }
    let mut iter = conjuncts.into_iter();
    let first = iter.next().unwrap();
    Some(iter.fold(first, |acc, e| Expr::BinaryOp {
        left: Box::new(acc),
        op: Operator::And,
        right: Box::new(e),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::CypherParser;

    fn parse_expr(s: &str) -> Expr {
        CypherParser::new(s).unwrap().parse_expression().unwrap()
    }

    #[test]
    fn test_simple_subsumption() {
        let query = parse_expr("u.active = true AND u.email = 'test@example.com'");
        let index = parse_expr("u.active = true");

        let result = check_subsumption(&query, &index, "u", "u");

        assert!(result.subsumes);
        assert!(result.residual.is_some());
    }

    #[test]
    fn test_no_subsumption_when_missing() {
        let query = parse_expr("u.email = 'test@example.com'");
        let index = parse_expr("u.active = true");

        let result = check_subsumption(&query, &index, "u", "u");

        assert!(!result.subsumes);
    }

    #[test]
    fn test_variable_substitution() {
        let query = parse_expr("p.active = true AND p.email = 'test@example.com'");
        let index = parse_expr("u.active = true");

        let result = check_subsumption(&query, &index, "p", "u");

        assert!(result.subsumes);
    }

    #[test]
    fn test_exact_match_no_residual() {
        let query = parse_expr("u.active = true");
        let index = parse_expr("u.active = true");

        let result = check_subsumption(&query, &index, "u", "u");

        assert!(result.subsumes);
        assert!(result.residual.is_none());
    }

    #[test]
    fn test_multiple_index_conjuncts() {
        let query =
            parse_expr("u.active = true AND u.verified = true AND u.email = 'test@example.com'");
        let index = parse_expr("u.active = true AND u.verified = true");

        let result = check_subsumption(&query, &index, "u", "u");

        assert!(result.subsumes);
        assert!(result.residual.is_some());
    }

    #[test]
    fn test_flatten_conjuncts() {
        let expr = parse_expr("a = 1 AND b = 2 AND c = 3");
        let conjuncts = flatten_conjuncts(&expr);
        assert_eq!(conjuncts.len(), 3);
    }

    #[test]
    fn test_combine_conjuncts() {
        let a = parse_expr("x = 1");
        let b = parse_expr("y = 2");
        let combined = combine_conjuncts(vec![a.clone(), b.clone()]);

        assert!(combined.is_some());
        let result = combined.unwrap();
        // Should be (x = 1) AND (y = 2)
        if let Expr::BinaryOp { op, .. } = result {
            assert_eq!(op, Operator::And);
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn test_combine_empty_conjuncts() {
        let result = combine_conjuncts(vec![]);
        assert!(result.is_none());
    }
}
