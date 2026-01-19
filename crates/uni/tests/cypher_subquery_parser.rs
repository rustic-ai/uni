// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni::query::parser::CypherParser;

#[test]
fn test_cypher_subquery_parser() -> anyhow::Result<()> {
    // Just test parsing for now
    let query = "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN n";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse();
    assert!(ast.is_ok());

    let query = "CALL { MATCH (n:Person) RETURN n.name } RETURN n.name";
    let mut parser = CypherParser::new(query)?;
    let ast = parser.parse();
    assert!(ast.is_ok());

    Ok(())
}
