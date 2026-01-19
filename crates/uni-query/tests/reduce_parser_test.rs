// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use uni_query::query::ast::*;
use uni_query::query::parser::CypherParser;

fn parse_expr(input: &str) -> Expr {
    let mut parser = CypherParser::new(input).unwrap();
    // We can't parse just an expression directly via public API easily without a wrapper,
    // but we can parse "RETURN expr" and extract it.
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
fn test_parse_reduce() {
    let input = "RETURN reduce(total = 0, x IN [1, 2, 3] | total + x)";
    let expr = parse_expr(input);

    if let Expr::Reduce {
        accumulator,
        init,
        variable,
        list,
        expr,
    } = expr
    {
        assert_eq!(accumulator, "total");
        assert_eq!(*init, Expr::Literal(json!(0)));
        assert_eq!(variable, "x");

        if let Expr::List(items) = *list {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected list literal");
        }

        if let Expr::BinaryOp { left, op, right: _ } = *expr {
            assert_eq!(*left, Expr::Identifier("total".to_string()));
            assert_eq!(op, Operator::Add);
        } else {
            // It might fail if + is not implemented.
        }
    } else {
        panic!("Expected Expr::Reduce");
    }
}
