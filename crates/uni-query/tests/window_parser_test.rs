// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_query::query::ast::*;
use uni_query::query::parser::CypherParser;

fn parse_expr(input: &str) -> Expr {
    let mut parser = CypherParser::new(input).unwrap();
    let query = parser.parse().unwrap();
    if let Query::Single(q) = query
        && let Some(return_clause) = q.return_clause
        && let ReturnItem::Expr { expr, .. } = &return_clause.items[0]
    {
        return expr.clone();
    }
    panic!("Failed to parse expression");
}

#[test]
fn test_parse_window_function() {
    let input = "RETURN rank() OVER (ORDER BY p.age DESC)";
    let expr = parse_expr(input);

    if let Expr::WindowFunction {
        function,
        partition_by,
        order_by,
    } = expr
    {
        if let Expr::FunctionCall { name, .. } = *function {
            assert_eq!(name, "RANK");
        } else {
            panic!("Expected FunctionCall");
        }
        assert!(partition_by.is_empty());
        assert_eq!(order_by.len(), 1);
        if let (Expr::Property(_, prop), asc) = &order_by[0] {
            assert_eq!(prop, "age");
            assert!(!asc);
        } else {
            panic!("Expected property in ORDER BY");
        }
    } else {
        panic!("Expected Expr::WindowFunction");
    }
}

#[test]
fn test_parse_window_function_partition() {
    let input = "RETURN row_number() OVER (PARTITION BY p.dept ORDER BY p.age)";
    let expr = parse_expr(input);

    if let Expr::WindowFunction {
        function,
        partition_by,
        order_by,
    } = expr
    {
        if let Expr::FunctionCall { name, .. } = *function {
            assert_eq!(name, "ROW_NUMBER");
        } else {
            panic!("Expected FunctionCall");
        }
        assert_eq!(partition_by.len(), 1);
        if let Expr::Property(_, prop) = &partition_by[0] {
            assert_eq!(prop, "dept");
        } else {
            panic!("Expected property in PARTITION BY");
        }

        assert_eq!(order_by.len(), 1);
    } else {
        panic!("Expected Expr::WindowFunction");
    }
}
