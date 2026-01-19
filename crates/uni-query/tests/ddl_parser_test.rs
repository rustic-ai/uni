// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_common::DataType;
use uni_query::query::ast::*;
use uni_query::query::parser::CypherParser;

fn parse(input: &str) -> Clause {
    let mut parser = CypherParser::new(input).unwrap();
    let query = parser.parse().unwrap();
    if let Query::Single(q) = query {
        q.clauses[0].clone()
    } else {
        panic!("Expected single query");
    }
}

#[test]
fn test_create_label() {
    let input = "CREATE LABEL Person (name STRING NOT NULL, age INT)";
    let clause = parse(input);

    if let Clause::CreateLabel(c) = clause {
        assert_eq!(c.name, "Person");
        assert_eq!(c.properties.len(), 2);

        assert_eq!(c.properties[0].name, "name");
        assert!(matches!(c.properties[0].data_type, DataType::String));
        assert!(!c.properties[0].nullable);

        assert_eq!(c.properties[1].name, "age");
        assert!(matches!(c.properties[1].data_type, DataType::Int64));
        assert!(c.properties[1].nullable);
    } else {
        panic!("Expected CreateLabel");
    }
}

#[test]
fn test_create_edge_type() {
    let input = "CREATE EDGE TYPE FOLLOWS (weight FLOAT) FROM Person TO Person";
    let clause = parse(input);

    if let Clause::CreateEdgeType(c) = clause {
        assert_eq!(c.name, "FOLLOWS");
        assert_eq!(c.src_labels, vec!["Person"]);
        assert_eq!(c.dst_labels, vec!["Person"]);
        assert_eq!(c.properties.len(), 1);
        assert!(matches!(c.properties[0].data_type, DataType::Float64));
    } else {
        panic!("Expected CreateEdgeType");
    }
}

#[test]
fn test_create_constraint_unique() {
    let input = "CREATE CONSTRAINT unique_email ON (p:Person) ASSERT p.email IS UNIQUE";
    let clause = parse(input);

    if let Clause::CreateConstraint(c) = clause {
        assert_eq!(c.name, "unique_email");
        if let ConstraintTarget::Label(l) = c.target {
            assert_eq!(l, "Person");
        } else {
            panic!("Expected Label target");
        }

        if let ConstraintType::Unique { properties } = c.constraint_type {
            assert_eq!(properties, vec!["email"]);
        } else {
            panic!("Expected Unique constraint");
        }
    } else {
        panic!("Expected CreateConstraint");
    }
}

#[test]
fn test_create_constraint_exists() {
    let input = "CREATE CONSTRAINT exists_name ON (p:Person) ASSERT EXISTS(p.name)";
    let clause = parse(input);

    if let Clause::CreateConstraint(c) = clause {
        assert_eq!(c.name, "exists_name");

        if let ConstraintType::Exists { property } = c.constraint_type {
            assert_eq!(property, "name");
        } else {
            panic!("Expected Exists constraint");
        }
    } else {
        panic!("Expected CreateConstraint");
    }
}

#[test]
fn test_alter_label() {
    let input = "ALTER LABEL Person ADD PROPERTY bio STRING";
    let clause = parse(input);

    if let Clause::AlterLabel(c) = clause {
        assert_eq!(c.name, "Person");
        if let AlterAction::AddProperty(p) = c.action {
            assert_eq!(p.name, "bio");
            assert!(matches!(p.data_type, DataType::String));
        } else {
            panic!("Expected AddProperty");
        }
    } else {
        panic!("Expected AlterLabel");
    }
}

#[test]
fn test_drop_label() {
    let input = "DROP LABEL IF EXISTS Person";
    let clause = parse(input);

    if let Clause::DropLabel(c) = clause {
        assert_eq!(c.name, "Person");
        assert!(c.if_exists);
    } else {
        panic!("Expected DropLabel");
    }
}

#[test]
fn test_show_constraints() {
    let input = "SHOW CONSTRAINTS FOR (n:Person)";
    let clause = parse(input);

    if let Clause::ShowConstraints(c) = clause {
        if let Some(ConstraintTarget::Label(l)) = c.target {
            assert_eq!(l, "Person");
        } else {
            panic!("Expected Label target");
        }
    } else {
        panic!("Expected ShowConstraints");
    }
}
