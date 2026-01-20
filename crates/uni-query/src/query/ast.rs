// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::Value;
use std::collections::HashMap;
use uni_common::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Eq,         // =
    NotEq,      // <>
    Lt,         // <
    LtEq,       // <=
    Gt,         // >
    GtEq,       // >=
    And,        // AND
    Or,         // OR
    In,         // IN
    Contains,   // CONTAINS
    StartsWith, // STARTS WITH
    EndsWith,   // ENDS WITH
    Add,        // +
    Sub,        // -
    Mul,        // *
    Div,        // /
    Mod,        // %
    Pow,        // ^
    ApproxEq,   // ~=
    Regex,      // =~
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Wildcard,
    Identifier(String),               // n
    Property(Box<Expr>, String),      // n.age
    ArrayIndex(Box<Expr>, Box<Expr>), // n.authors[0]
    Parameter(String),                // $name
    Literal(Value),                   // 30, 'Alice'
    List(Vec<Expr>),                  // [1, 2, 3]
    Map(Vec<(String, Expr)>),         // {a: 1, b: 2}
    IsNull(Box<Expr>),                // x IS NULL
    IsNotNull(Box<Expr>),             // x IS NOT NULL
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    BinaryOp {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Case {
        expr: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    ListComprehension {
        variable: String,
        list: Box<Expr>,
        where_clause: Option<Box<Expr>>,
        mapping: Option<Box<Expr>>,
    },
    Reduce {
        accumulator: String,
        init: Box<Expr>,
        variable: String,
        list: Box<Expr>,
        expr: Box<Expr>,
    },
    WindowFunction {
        function: Box<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<(Expr, bool)>,
    },
    Exists(Box<Query>),    // EXISTS { ... }
    ScalarSubquery(usize), // Compiled subquery index
}

impl Expr {
    pub fn extract_variable(&self) -> Option<String> {
        match self {
            Expr::Identifier(n) => Some(n.clone()),
            Expr::Property(e, _) => e.extract_variable(),
            Expr::FunctionCall { args, .. } => args.iter().find_map(|a| a.extract_variable()),
            Expr::UnaryOp { expr, .. } => expr.extract_variable(),
            Expr::BinaryOp { left, right, .. } => {
                left.extract_variable().or_else(|| right.extract_variable())
            }
            _ => None,
        }
    }

    pub fn substitute_variable(&self, old_var: &str, new_var: &str) -> Expr {
        match self {
            Expr::Identifier(n) if n == old_var => Expr::Identifier(new_var.to_string()),
            Expr::Property(e, p) => {
                Expr::Property(Box::new(e.substitute_variable(old_var, new_var)), p.clone())
            }
            Expr::ArrayIndex(e, idx) => Expr::ArrayIndex(
                Box::new(e.substitute_variable(old_var, new_var)),
                Box::new(idx.substitute_variable(old_var, new_var)),
            ),
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => Expr::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| a.substitute_variable(old_var, new_var))
                    .collect(),
                distinct: *distinct,
            },
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(left.substitute_variable(old_var, new_var)),
                op: op.clone(),
                right: Box::new(right.substitute_variable(old_var, new_var)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(expr.substitute_variable(old_var, new_var)),
            },
            Expr::IsNull(e) => Expr::IsNull(Box::new(e.substitute_variable(old_var, new_var))),
            Expr::IsNotNull(e) => {
                Expr::IsNotNull(Box::new(e.substitute_variable(old_var, new_var)))
            }
            Expr::List(exprs) => Expr::List(
                exprs
                    .iter()
                    .map(|e| e.substitute_variable(old_var, new_var))
                    .collect(),
            ),
            Expr::Map(entries) => Expr::Map(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.substitute_variable(old_var, new_var)))
                    .collect(),
            ),
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => Expr::Case {
                expr: expr
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| {
                        (
                            w.substitute_variable(old_var, new_var),
                            t.substitute_variable(old_var, new_var),
                        )
                    })
                    .collect(),
                else_expr: else_expr
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
            },
            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                mapping,
            } => Expr::ListComprehension {
                variable: variable.clone(),
                list: Box::new(list.substitute_variable(old_var, new_var)),
                where_clause: where_clause
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
                mapping: mapping
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
            },
            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr,
            } => Expr::Reduce {
                accumulator: accumulator.clone(),
                init: Box::new(init.substitute_variable(old_var, new_var)),
                variable: variable.clone(),
                list: Box::new(list.substitute_variable(old_var, new_var)),
                expr: Box::new(expr.substitute_variable(old_var, new_var)),
            },
            Expr::WindowFunction {
                function,
                partition_by,
                order_by,
            } => Expr::WindowFunction {
                function: Box::new(function.substitute_variable(old_var, new_var)),
                partition_by: partition_by
                    .iter()
                    .map(|e| e.substitute_variable(old_var, new_var))
                    .collect(),
                order_by: order_by
                    .iter()
                    .map(|(e, asc)| (e.substitute_variable(old_var, new_var), *asc))
                    .collect(),
            },
            // Wildcard, Identifier (non-match), Parameter, Literal, Exists don't need substitution
            _ => self.clone(),
        }
    }

    pub fn is_aggregate(&self) -> bool {
        match self {
            Expr::FunctionCall { name, .. } => {
                matches!(
                    name.as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "COLLECT"
                )
            }
            Expr::BinaryOp { left, right, .. } => left.is_aggregate() || right.is_aggregate(),
            Expr::Property(expr, _) => expr.is_aggregate(),
            Expr::ArrayIndex(expr, idx) => expr.is_aggregate() || idx.is_aggregate(),
            Expr::List(exprs) => exprs.iter().any(|e| e.is_aggregate()),
            Expr::Map(entries) => entries.iter().any(|(_, e)| e.is_aggregate()),
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => expr.is_aggregate(),
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                expr.as_ref().is_some_and(|e| e.is_aggregate())
                    || when_then
                        .iter()
                        .any(|(w, t)| w.is_aggregate() || t.is_aggregate())
                    || else_expr.as_ref().is_some_and(|e| e.is_aggregate())
            }
            Expr::ListComprehension {
                list,
                where_clause,
                mapping,
                ..
            } => {
                list.is_aggregate()
                    || where_clause.as_ref().is_some_and(|e| e.is_aggregate())
                    || mapping.as_ref().is_some_and(|e| e.is_aggregate())
            }
            Expr::Reduce {
                init, list, expr, ..
            } => init.is_aggregate() || list.is_aggregate() || expr.is_aggregate(),
            Expr::WindowFunction { .. } => false, // Window functions do not collapse rows
            Expr::Exists(_) => false,             // Subquery aggregation is internal to subquery
            _ => false,
        }
    }

    pub fn to_string_repr(&self) -> String {
        match self {
            Expr::Wildcard => "*".to_string(),
            Expr::Identifier(s) => s.clone(),
            Expr::Property(e, p) => format!("{}.{}", e.to_string_repr(), p),
            Expr::ArrayIndex(arr, idx) => {
                format!("{}[{}]", arr.to_string_repr(), idx.to_string_repr())
            }
            Expr::Parameter(name) => format!("${}", name),
            Expr::Literal(v) => format!("{:?}", v),
            Expr::List(items) => {
                let items_str: Vec<_> = items.iter().map(|e| e.to_string_repr()).collect();
                format!("[{}]", items_str.join(", "))
            }
            Expr::Map(items) => {
                let items_str: Vec<_> = items
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v.to_string_repr()))
                    .collect();
                format!("{{ {} }}", items_str.join(", "))
            }
            Expr::IsNull(expr) => format!("{} IS NULL", expr.to_string_repr()),
            Expr::IsNotNull(expr) => format!("{} IS NOT NULL", expr.to_string_repr()),
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let args_str: Vec<_> = args.iter().map(|e| e.to_string_repr()).collect();
                let dist_str = if *distinct { "DISTINCT " } else { "" };
                format!("{}({}{})", name, dist_str, args_str.join(", "))
            }
            Expr::BinaryOp { left, op, right } => {
                let op_str = match op {
                    Operator::Eq => "=",
                    Operator::NotEq => "<>",
                    Operator::Lt => "<",
                    Operator::LtEq => "<=",
                    Operator::Gt => ">",
                    Operator::GtEq => ">=",
                    Operator::And => "AND",
                    Operator::Or => "OR",
                    Operator::In => "IN",
                    Operator::Contains => "CONTAINS",
                    Operator::StartsWith => "STARTS WITH",
                    Operator::EndsWith => "ENDS WITH",
                    Operator::Add => "+",
                    Operator::Sub => "-",
                    Operator::Mul => "*",
                    Operator::Div => "/",
                    Operator::Mod => "%",
                    Operator::Pow => "^",
                    Operator::ApproxEq => "~=",
                    Operator::Regex => "=~",
                };
                format!(
                    "({} {} {})",
                    left.to_string_repr(),
                    op_str,
                    right.to_string_repr()
                )
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => format!("NOT {}", expr.to_string_repr()),
            },
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                let mut s = "CASE".to_string();
                if let Some(e) = expr {
                    s.push(' ');
                    s.push_str(&e.to_string_repr());
                }
                for (w, t) in when_then {
                    s.push_str(" WHEN ");
                    s.push_str(&w.to_string_repr());
                    s.push_str(" THEN ");
                    s.push_str(&t.to_string_repr());
                }
                if let Some(e) = else_expr {
                    s.push_str(" ELSE ");
                    s.push_str(&e.to_string_repr());
                }
                s.push_str(" END");
                s
            }
            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                mapping,
            } => {
                let mut s = format!("[{} IN {}", variable, list.to_string_repr());
                if let Some(w) = where_clause {
                    s.push_str(" WHERE ");
                    s.push_str(&w.to_string_repr());
                }
                if let Some(m) = mapping {
                    s.push_str(" | ");
                    s.push_str(&m.to_string_repr());
                }
                s.push(']');
                s
            }
            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr,
            } => {
                format!(
                    "reduce({} = {}, {} IN {} | {})",
                    accumulator,
                    init.to_string_repr(),
                    variable,
                    list.to_string_repr(),
                    expr.to_string_repr()
                )
            }
            Expr::WindowFunction {
                function,
                partition_by,
                order_by,
            } => {
                let mut s = format!("{} OVER (", function.to_string_repr());
                if !partition_by.is_empty() {
                    let parts: Vec<_> = partition_by.iter().map(|e| e.to_string_repr()).collect();
                    s.push_str("PARTITION BY ");
                    s.push_str(&parts.join(", "));
                }
                if !order_by.is_empty() {
                    if !partition_by.is_empty() {
                        s.push(' ');
                    }
                    let parts: Vec<_> = order_by
                        .iter()
                        .map(|(e, asc)| {
                            format!(
                                "{} {}",
                                e.to_string_repr(),
                                if *asc { "ASC" } else { "DESC" }
                            )
                        })
                        .collect();
                    s.push_str("ORDER BY ");
                    s.push_str(&parts.join(", "));
                }
                s.push(')');
                s
            }
            Expr::Exists(_) => "EXISTS {...}".to_string(),
            Expr::ScalarSubquery(idx) => format!("SUBQUERY({})", idx),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    Single(CypherQuery),
    Union {
        left: Box<Query>,
        right: Box<Query>,
        all: bool,
    },
    Explain(Box<Query>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortItem {
    pub expr: Expr,
    pub ascending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CypherQuery {
    pub clauses: Vec<Clause>,
    pub return_clause: Option<ReturnClause>, // Final RETURN
    pub order_by: Option<Vec<SortItem>>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Unwind(UnwindClause),
    Match(MatchClause),
    Merge(MergeClause),
    Create(CreateClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
    Call(CallClause),
    CallSubquery(Box<Query>), // CALL { ... }
    With(WithClause),
    WithRecursive(WithRecursiveClause),
    // Transactions
    Begin,
    Commit,
    Rollback,
    // DDL
    CreateVectorIndex(CreateVectorIndexClause),
    CreateFullTextIndex(CreateFullTextIndexClause),
    CreateScalarIndex(CreateScalarIndexClause),
    CreateJsonFtsIndex(CreateJsonFtsIndexClause),
    DropIndex(DropIndexClause),
    ShowIndexes(ShowIndexesClause),
    // Schema DDL
    CreateLabel(CreateLabelClause),
    CreateEdgeType(CreateEdgeTypeClause),
    AlterLabel(AlterLabelClause),
    AlterEdgeType(AlterEdgeTypeClause),
    DropLabel(DropLabelClause),
    DropEdgeType(DropEdgeTypeClause),
    // Constraints
    CreateConstraint(CreateConstraintClause),
    DropConstraint(DropConstraintClause),
    ShowConstraints(ShowConstraintsClause),
    Copy(CopyClause),
    Backup(BackupClause),
    // Admin
    ShowDatabase,
    ShowConfig,
    ShowStatistics,
    Vacuum,
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackupClause {
    pub destination: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CopyClause {
    pub target: String,
    pub source: String,
    pub is_export: bool,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateVectorIndexClause {
    pub name: String,
    pub label: String,
    pub property: String,
    pub options: HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateFullTextIndexClause {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub options: HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateScalarIndexClause {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub where_clause: Option<Expr>,
    pub options: HashMap<String, Value>,
    pub if_not_exists: bool,
}

/// AST node for CREATE JSON FULLTEXT INDEX.
///
/// Represents a DDL statement to create a Lance inverted index on a JSON column.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateJsonFtsIndexClause {
    /// Index name.
    pub name: String,
    /// Label (node type) this index applies to.
    pub label: String,
    /// Column containing JSON data (typically "_doc").
    pub column: String,
    /// Options like `with_positions: true`.
    pub options: HashMap<String, Value>,
    /// Skip creation if index already exists.
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowIndexesClause {
    pub filter: Option<String>, // e.g. "VECTOR", "FULLTEXT"
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub return_clause: ReturnClause,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithRecursiveClause {
    pub name: String,
    pub query: Box<Query>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_match: Option<SetClause>,
    pub on_create: Option<SetClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnwindClause {
    pub expr: Expr,
    pub variable: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetItem {
    Property {
        expr: Expr, // Should be Expr::Property
        value: Expr,
    },
    Labels {
        variable: String,
        labels: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoveClause {
    pub items: Vec<RemoveItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoveItem {
    Property(Expr), // Should be Expr::Property
    Labels {
        variable: String,
        labels: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteClause {
    pub items: Vec<Expr>,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateClause {
    pub pattern: Pattern,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallClause {
    pub procedure_name: String,
    pub arguments: Vec<Expr>,
    pub yield_items: Vec<(String, Option<String>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    pub pattern: Pattern,
    pub where_clause: Option<Expr>,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub variable: Option<String>,
    pub parts: Vec<PatternPart>,
    pub shortest_path: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatternPart {
    Node(NodePattern),
    Relationship(RelationshipPattern),
}

impl PatternPart {
    pub fn as_node(&self) -> Option<&NodePattern> {
        match self {
            PatternPart::Node(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_relationship(&self) -> Option<&RelationshipPattern> {
        match self {
            PatternPart::Relationship(r) => Some(r),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationshipPattern {
    pub variable: Option<String>,
    pub rel_types: Vec<String>,
    pub direction: Direction,
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Direction {
    Outgoing, // -[]->
    Incoming, // <-[]-
    Both,     // -[]-
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnItem {
    Expr { expr: Expr, alias: Option<String> },
    All, // *
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateLabelClause {
    pub name: String,
    pub properties: Vec<PropertyDefinition>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PropertyDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub unique: bool,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateEdgeTypeClause {
    pub name: String,
    pub src_labels: Vec<String>,
    pub dst_labels: Vec<String>,
    pub properties: Vec<PropertyDefinition>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterLabelClause {
    pub name: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterEdgeTypeClause {
    pub name: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    AddProperty(PropertyDefinition),
    DropProperty(String),
    RenameProperty { old_name: String, new_name: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropLabelClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropEdgeTypeClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateConstraintClause {
    pub name: String,
    pub target: ConstraintTarget,
    pub constraint_type: ConstraintType,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintTarget {
    Label(String),
    EdgeType(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintType {
    Unique { properties: Vec<String> },
    Exists { property: String },
    Check { expression: Expr },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropConstraintClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowConstraintsClause {
    pub target: Option<ConstraintTarget>,
}
