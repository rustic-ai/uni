// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::Value;
use uni_common::core::schema::{DataType, JsonIndex, LabelMeta, Schema, SchemaElementState};
use uni_query::query::expr::{Expr, Operator};
use uni_query::query::pushdown::{IndexAwareAnalyzer, LanceFilterGenerator, PredicateAnalyzer};

#[test]
fn test_lance_filter_generation() {
    // n.name CONTAINS 'foo'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Identifier("n".to_string())),
            "name".to_string(),
        )),
        op: Operator::Contains,
        right: Box::new(Expr::Literal(Value::String("foo".to_string()))),
    };

    let filter = LanceFilterGenerator::generate(&[expr], "n").unwrap();
    assert_eq!(filter, "name LIKE '%foo%'");

    // n.title STARTS WITH 'Intro'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Identifier("n".to_string())),
            "title".to_string(),
        )),
        op: Operator::StartsWith,
        right: Box::new(Expr::Literal(Value::String("Intro".to_string()))),
    };

    let filter = LanceFilterGenerator::generate(&[expr], "n").unwrap();
    assert_eq!(filter, "title LIKE 'Intro%'");
}

#[test]
fn test_or_to_in_conversion() {
    // Manually construct: n.status = 'a' OR n.status = 'b'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Identifier("n".to_string())),
                "status".to_string(),
            )),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(Value::String("a".to_string()))),
        }),
        op: Operator::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Identifier("n".to_string())),
                "status".to_string(),
            )),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(Value::String("b".to_string()))),
        }),
    };

    let analyzer = PredicateAnalyzer::new(None);
    let analysis = analyzer.analyze(&expr, "n");

    // Should be pushed as a single IN expression
    assert_eq!(analysis.pushable.len(), 1);
    assert!(analysis.residual.is_empty());

    let pushed = &analysis.pushable[0];
    if let Expr::BinaryOp { op, right, .. } = pushed {
        assert_eq!(*op, Operator::In);
        if let Expr::List(items) = right.as_ref() {
            assert_eq!(items.len(), 2);
        } else {
            panic!("Expected list on RHS of IN");
        }
    } else {
        panic!("Expected BinaryOp IN");
    }

    // Verify SQL generation
    let sql = LanceFilterGenerator::generate(&analysis.pushable, "n").unwrap();
    // order might vary? No, vec insertion order.
    assert!(sql == "status IN ('a', 'b')" || sql == "status IN ('b', 'a')");
}

#[test]
fn test_is_null_pushdown() {
    let expr = Expr::IsNull(Box::new(Expr::Property(
        Box::new(Expr::Identifier("n".to_string())),
        "email".to_string(),
    )));

    let analyzer = PredicateAnalyzer::new(None);
    let analysis = analyzer.analyze(&expr, "n");

    assert_eq!(analysis.pushable.len(), 1);
    assert!(analysis.residual.is_empty());

    let sql = LanceFilterGenerator::generate(&analysis.pushable, "n").unwrap();
    assert_eq!(sql, "email IS NULL");
}

#[test]
fn test_is_not_null_pushdown() {
    let expr = Expr::IsNotNull(Box::new(Expr::Property(
        Box::new(Expr::Identifier("n".to_string())),
        "email".to_string(),
    )));

    let analyzer = PredicateAnalyzer::new(None);
    let analysis = analyzer.analyze(&expr, "n");

    assert_eq!(analysis.pushable.len(), 1);

    let sql = LanceFilterGenerator::generate(&analysis.pushable, "n").unwrap();
    assert_eq!(sql, "email IS NOT NULL");
}

#[test]
fn test_predicate_flattening() {
    // (a=1 AND b=2)
    // Analyzer splits conjuncts into vector
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Identifier("n".to_string())),
                "a".to_string(),
            )),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(Value::Number(1.into()))),
        }),
        op: Operator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Identifier("n".to_string())),
                "b".to_string(),
            )),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(Value::Number(2.into()))),
        }),
    };

    let analyzer = PredicateAnalyzer::new(None);
    let analysis = analyzer.analyze(&expr, "n");

    assert_eq!(analysis.pushable.len(), 2);

    let sql = LanceFilterGenerator::generate(&analysis.pushable, "n").unwrap();
    assert_eq!(sql, "a = 1 AND b = 2");
}

// =====================================================================
// IndexAwareAnalyzer Tests
// =====================================================================

fn create_test_schema_with_label(
    label: &str,
    label_id: u16,
    json_indexes: Vec<JsonIndex>,
) -> Schema {
    let mut schema = Schema::default();
    schema.labels.insert(
        label.to_string(),
        LabelMeta {
            id: label_id,
            created_at: chrono::Utc::now(),
            state: SchemaElementState::Active,
            is_document: true,
            json_indexes,
        },
    );
    schema
}

#[test]
fn test_index_aware_uid_extraction() {
    // Test that _uid = 'valid_base32' is recognized
    // Note: We can't test actual UID lookup without a valid Base32Lower multibase string
    // but we can verify the pattern detection

    let schema = create_test_schema_with_label("Person", 1, vec![]);

    // Create a predicate: n._uid = 'invalid_format'
    // This should NOT be extracted (invalid UID format)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Identifier("n".to_string())),
            "_uid".to_string(),
        )),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(Value::String("not-a-valid-uid".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Invalid UID should not be extracted
    assert!(strategy.uid_lookup.is_none());
    // Should become residual since _uid column doesn't exist in Lance
    assert!(!strategy.residual.is_empty() || !strategy.lance_predicates.is_empty());
}

#[test]
fn test_index_aware_jsonpath_extraction() {
    let schema = create_test_schema_with_label(
        "Doc",
        2,
        vec![JsonIndex {
            path: "$.title".to_string(),
            r#type: DataType::String,
        }],
    );

    // Create predicate: n.title = 'Hello'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Identifier("n".to_string())),
            "title".to_string(),
        )),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(Value::String("Hello".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 2);

    // Should be extracted as jsonpath lookup
    assert_eq!(strategy.jsonpath_lookups.len(), 1);
    assert_eq!(strategy.jsonpath_lookups[0].0, "title");
    assert_eq!(strategy.jsonpath_lookups[0].1, "Hello");

    // Should NOT be in lance_predicates (routed to index instead)
    assert!(strategy.lance_predicates.is_empty());
}

#[test]
fn test_index_aware_non_indexed_property() {
    let schema = create_test_schema_with_label(
        "Doc",
        2,
        vec![JsonIndex {
            path: "$.title".to_string(),
            r#type: DataType::String,
        }],
    );

    // Create predicate: n.author = 'John' (not indexed)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Identifier("n".to_string())),
            "author".to_string(),
        )),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(Value::String("John".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 2);

    // Should NOT be extracted as jsonpath lookup (no index)
    assert!(strategy.jsonpath_lookups.is_empty());

    // Should go to lance_predicates
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_index_aware_combined_predicates() {
    let schema = create_test_schema_with_label(
        "Doc",
        2,
        vec![JsonIndex {
            path: "$.title".to_string(),
            r#type: DataType::String,
        }],
    );

    // Create predicate: n.title = 'Hello' AND n.author = 'John'
    // title is indexed, author is not
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Identifier("n".to_string())),
                "title".to_string(),
            )),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(Value::String("Hello".to_string()))),
        }),
        op: Operator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Identifier("n".to_string())),
                "author".to_string(),
            )),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(Value::String("John".to_string()))),
        }),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 2);

    // title should go to jsonpath lookup
    assert_eq!(strategy.jsonpath_lookups.len(), 1);
    assert_eq!(strategy.jsonpath_lookups[0].0, "title");

    // author should go to lance_predicates
    assert_eq!(strategy.lance_predicates.len(), 1);
}
