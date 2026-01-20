// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_query::CypherParser;
use uni_query::query::ast::{Expr, Operator, Query};

#[test]
fn test_parse_string_operators() {
    let sql = "MATCH (n) WHERE n.name CONTAINS 'foo' RETURN n";
    let mut parser = CypherParser::new(sql).unwrap();
    let query = parser.parse().unwrap();

    if let Query::Single(q) = query {
        if let Some(uni_query::query::ast::Clause::Match(m)) = q.clauses.first() {
            if let Some(Expr::BinaryOp {
                left: _,
                op,
                right: _,
            }) = &m.where_clause
            {
                assert_eq!(*op, Operator::Contains);
                // Verify structure
            } else {
                panic!("Expected binary op");
            }
        } else {
            panic!("Expected match clause");
        }
    } else {
        panic!("Expected single query");
    }

    let sql = "MATCH (n) WHERE n.name STARTS WITH 'foo' RETURN n";
    let mut parser = CypherParser::new(sql).unwrap();
    let _query = parser.parse().unwrap();
    // Validate StartsWith...

    let sql = "MATCH (n) WHERE n.name ENDS WITH 'foo' RETURN n";
    let mut parser = CypherParser::new(sql).unwrap();
    let _query = parser.parse().unwrap();
    // Validate EndsWith...
}

#[test]
fn test_parse_regex_operator() {
    // Basic regex operator
    let sql = "MATCH (n) WHERE n.email =~ '.*@gmail\\.com$' RETURN n";
    let mut parser = CypherParser::new(sql).unwrap();
    let query = parser.parse().unwrap();

    if let Query::Single(q) = query {
        if let Some(uni_query::query::ast::Clause::Match(m)) = q.clauses.first() {
            if let Some(Expr::BinaryOp { left: _, op, right }) = &m.where_clause {
                assert_eq!(*op, Operator::Regex, "Expected Regex operator");
                // Verify the pattern is correct
                if let Expr::Literal(serde_json::Value::String(pattern)) = right.as_ref() {
                    assert_eq!(pattern, ".*@gmail\\.com$");
                } else {
                    panic!("Expected string literal pattern");
                }
            } else {
                panic!("Expected binary op in WHERE clause");
            }
        } else {
            panic!("Expected match clause");
        }
    } else {
        panic!("Expected single query");
    }

    // Case insensitive regex
    let sql = "MATCH (n:Person) WHERE n.name =~ '(?i)john' RETURN n";
    let mut parser = CypherParser::new(sql).unwrap();
    let query = parser.parse().unwrap();
    if let Query::Single(q) = query
        && let Some(uni_query::query::ast::Clause::Match(m)) = q.clauses.first()
        && let Some(Expr::BinaryOp { op, .. }) = &m.where_clause
    {
        assert_eq!(*op, Operator::Regex);
    } else {
        panic!("Expected regex binary op in match clause");
    }
}
