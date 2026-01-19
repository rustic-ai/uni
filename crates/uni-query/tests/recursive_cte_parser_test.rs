// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_query::query::ast::*;
use uni_query::query::parser::CypherParser;

#[test]
fn test_parse_recursive_cte() {
    let input = "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Node) WHERE root.id = 0 RETURN root
            UNION
            MATCH (parent:Node)-[:CHILD]->(child:Node)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n) WHERE n IN hierarchy RETURN n
    ";

    let mut parser = CypherParser::new(input).unwrap();
    let query = parser.parse().unwrap();

    if let Query::Single(q) = query {
        // First clause should be WithRecursive
        if let Clause::WithRecursive(cte) = &q.clauses[0] {
            assert_eq!(cte.name, "hierarchy");

            // Check anchor + recursive union
            if let Query::Union { left, right, .. } = &*cte.query {
                if let Query::Single(anchor) = &**left {
                    // Check anchor MATCH
                    if let Clause::Match(m) = &anchor.clauses[0] {
                        if let PatternPart::Node(n) = &m.pattern.parts[0] {
                            assert_eq!(n.labels[0], "Node");
                        }
                    } else {
                        panic!("Expected Anchor Match");
                    }
                } else {
                    panic!("Expected Single Query Anchor");
                }

                // Check recursive part
                if let Query::Single(recursive) = &**right {
                    // Check recursive MATCH
                    if let Clause::Match(_) = &recursive.clauses[0] {
                        // OK
                    } else {
                        panic!("Expected Recursive Match");
                    }
                }
            } else {
                panic!("Expected Union in CTE");
            }
        } else {
            panic!("Expected WithRecursive clause");
        }

        // Second clause should be Match
        if let Clause::Match(_) = &q.clauses[1] {
            // OK
        } else {
            panic!("Expected main query Match");
        }
    } else {
        panic!("Expected Single Query");
    }
}
