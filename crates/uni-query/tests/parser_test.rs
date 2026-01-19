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
