// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_query::query::ast::{Clause, Expr, Operator, Query};
use uni_query::query::parser::CypherParser;

#[test]
fn test_parse_complex_where_with_or_and() {
    let sql = "MATCH (n) WHERE n.age > 20 AND (n.name = 'Alice' OR n.name = 'Bob')";
    let query = CypherParser::new(sql).unwrap().parse().unwrap();

    if let Query::Single(cypher) = query {
        if let Clause::Match(m) = &cypher.clauses[0] {
            if let Some(Expr::BinaryOp { left: _, op, right }) = &m.where_clause {
                assert_eq!(*op, Operator::And);
                // Check precedence: AND binds tighter than OR?
                // Cypher: AND has higher precedence than OR?
                // "AND has higher precedence than OR" -> (A AND B) OR C vs A AND (B OR C).
                // My query: A AND (B OR C).
                // Parser recursive descent: parse_expr (OR) calls parse_and (AND).
                // So OR is top level if unparenthesized?
                // parse_expr: left = parse_and(); while OR ...
                // So A OR B AND C -> A OR (B AND C).
                // But my query has parens.

                // My parser structure:
                // parse_expr calls parse_and.
                // parse_and calls parse_unary.
                // So AND binds tighter.
                // "A AND (B OR C)"
                // parse_and sees "A". loop AND ...
                // right side is "(B OR C)".
                // parse_unary handles parens via parse_atom -> parse_expr recursion.

                // So top level should be AND.
                // left: n.age > 20
                // right: (n.name = 'Alice' OR n.name = 'Bob')

                // Let's verify right side is OR.
                if let Expr::BinaryOp { op: op2, .. } = &**right {
                    assert_eq!(*op2, Operator::Or);
                } else {
                    panic!("Right side should be OR");
                }
            } else {
                panic!("Expected BinaryOp in WHERE");
            }
        } else {
            panic!("Expected MATCH clause");
        }
    } else {
        panic!("Expected Single query");
    }
}

#[test]
fn test_parse_nested_function_calls() {
    let sql = "RETURN count(distinct toInteger(head(keys(n))))";
    let query = CypherParser::new(sql).unwrap().parse().unwrap();

    // Just verifying it parses without error and structure is roughly correct
    if let Query::Single(cypher) = query {
        assert!(cypher.return_clause.is_some());
    }
}

#[test]
fn test_parse_list_comprehension_edge_cases() {
    // Empty list
    let sql = "RETURN [x IN [] | x]";
    let _query = CypherParser::new(sql).unwrap().parse().unwrap();

    // Nested
    let sql2 = "RETURN [x IN [1,2] | [y IN [3,4] | x+y]]";
    let _query2 = CypherParser::new(sql2).unwrap().parse().unwrap();

    // Filter only
    let sql3 = "RETURN [x IN [1] WHERE x > 0]";
    let _query3 = CypherParser::new(sql3).unwrap().parse().unwrap();
}

#[test]
fn test_parse_map_literal_edge_cases() {
    // Empty map
    let sql = "RETURN {}";
    let _query = CypherParser::new(sql).unwrap().parse().unwrap();

    // Nested map
    let sql2 = "RETURN {a: {b: 1}}";
    let _query2 = CypherParser::new(sql2).unwrap().parse().unwrap();

    // Keywords as keys
    let sql3 = "RETURN {match: 1, return: 2}";
    let _query3 = CypherParser::new(sql3).unwrap().parse().unwrap();
}
