// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::ast::{
    AlterAction, AlterEdgeTypeClause, AlterLabelClause, BackupClause, CallClause, Clause,
    ConstraintTarget, ConstraintType, CopyClause, CreateClause, CreateConstraintClause,
    CreateEdgeTypeClause, CreateFullTextIndexClause, CreateJsonFtsIndexClause, CreateLabelClause,
    CreateScalarIndexClause, CreateVectorIndexClause, CypherQuery, DeleteClause, Direction,
    DropConstraintClause, DropEdgeTypeClause, DropIndexClause, DropLabelClause, Expr, MatchClause,
    MergeClause, NodePattern, Operator, Pattern, PatternPart, PropertyDefinition, Query,
    RelationshipPattern, RemoveClause, RemoveItem, ReturnClause, ReturnItem, SetClause, SetItem,
    ShowConstraintsClause, ShowIndexesClause, SortItem, UnaryOperator, UnwindClause, WithClause,
    WithRecursiveClause,
};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::collections::HashMap;
use uni_common::DataType;

pub struct CypherParser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    _input: &'a str,
}

impl<'a> CypherParser<'a> {
    pub fn new(input: &'a str) -> Result<Self> {
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, input).tokenize()?;
        let tokens: Vec<Token> = tokens
            .into_iter()
            .filter(|t| !matches!(t, Token::Whitespace(_)))
            .collect();
        Ok(Self {
            tokens,
            pos: 0,
            _input: input,
        })
    }

    pub fn parse_expression(&mut self) -> Result<Expr> {
        self.parse_expr()
    }

    pub fn parse(&mut self) -> Result<Query> {
        if self.peek_keyword("EXPLAIN") {
            self.advance();
            let query = self.parse()?;
            return Ok(Query::Explain(Box::new(query)));
        }

        let left = self.parse_single_query()?;

        if self.peek_keyword("UNION") {
            self.advance();
            let mut all = false;
            if self.peek_keyword("ALL") {
                self.advance();
                all = true;
            }
            let right = self.parse()?;
            return Ok(Query::Union {
                left: Box::new(Query::Single(left)),
                right: Box::new(right),
                all,
            });
        }

        Ok(Query::Single(left))
    }

    pub fn parse_single_query(&mut self) -> Result<CypherQuery> {
        let mut clauses = Vec::new();

        loop {
            if self.peek_keyword("UNWIND") {
                self.advance();
                clauses.push(Clause::Unwind(self.parse_unwind()?));
                continue;
            }

            if self.peek_keyword("OPTIONAL") {
                self.advance();
                self.expect_keyword("MATCH")?;
                clauses.push(Clause::Match(self.parse_match(true)?));
                continue;
            }

            if self.peek_keyword("MATCH") {
                self.advance();
                clauses.push(Clause::Match(self.parse_match(false)?));
                continue;
            }

            if self.peek_keyword("MERGE") {
                self.advance();
                clauses.push(Clause::Merge(self.parse_merge()?));
                continue;
            }

            if self.peek_keyword("CALL") {
                self.advance();
                // Check for CALL { ... } (Subquery) vs CALL proc()
                if let Some(Token::LBrace) = self.peek_token() {
                    clauses.push(Clause::CallSubquery(self.parse_call_subquery()?));
                } else {
                    clauses.push(Clause::Call(self.parse_call()?));
                }
                continue;
            }

            if self.peek_keyword("CREATE") {
                self.advance();

                // DDL check: CREATE [VECTOR|FULLTEXT] INDEX ...
                // or CREATE INDEX ...
                if self.peek_keyword("VECTOR") {
                    self.advance();
                    self.expect_keyword("INDEX")?;
                    clauses.push(Clause::CreateVectorIndex(self.parse_create_vector_index()?));
                    continue;
                }

                if self.peek_keyword("FULLTEXT") {
                    self.advance();
                    self.expect_keyword("INDEX")?;
                    clauses.push(Clause::CreateFullTextIndex(
                        self.parse_create_fulltext_index()?,
                    ));
                    continue;
                }

                // CREATE JSON FULLTEXT INDEX
                if self.peek_keyword("JSON") {
                    self.advance();
                    self.expect_keyword("FULLTEXT")?;
                    self.expect_keyword("INDEX")?;
                    clauses.push(Clause::CreateJsonFtsIndex(
                        self.parse_create_json_fts_index()?,
                    ));
                    continue;
                }

                if self.peek_keyword("INDEX") {
                    self.advance();
                    clauses.push(Clause::CreateScalarIndex(self.parse_create_scalar_index()?));
                    continue;
                }

                // DDL: CREATE LABEL
                if self.peek_keyword("LABEL") {
                    self.advance();
                    clauses.push(Clause::CreateLabel(self.parse_create_label()?));
                    continue;
                }

                // DDL: CREATE EDGE TYPE
                if self.peek_keyword("EDGE") {
                    self.advance();
                    self.expect_keyword("TYPE")?;
                    clauses.push(Clause::CreateEdgeType(self.parse_create_edge_type()?));
                    continue;
                }

                // DDL: CREATE CONSTRAINT
                if self.peek_keyword("CONSTRAINT") {
                    self.advance();
                    clauses.push(Clause::CreateConstraint(self.parse_create_constraint()?));
                    continue;
                }

                // Regular CREATE
                clauses.push(Clause::Create(self.parse_create()?));
                continue;
            }

            if self.peek_keyword("ALTER") {
                self.advance();
                if self.peek_keyword("LABEL") {
                    self.advance();
                    clauses.push(Clause::AlterLabel(self.parse_alter_label()?));
                    continue;
                }
                if self.peek_keyword("EDGE") {
                    self.advance();
                    self.expect_keyword("TYPE")?;
                    clauses.push(Clause::AlterEdgeType(self.parse_alter_edge_type()?));
                    continue;
                }
                return Err(anyhow!("Expected LABEL or EDGE TYPE after ALTER"));
            }

            if self.peek_keyword("DROP") {
                self.advance();
                if self.peek_keyword("INDEX") {
                    self.advance();
                    clauses.push(Clause::DropIndex(self.parse_drop_index()?));
                    continue;
                }
                if self.peek_keyword("LABEL") {
                    self.advance();
                    clauses.push(Clause::DropLabel(self.parse_drop_label()?));
                    continue;
                }
                if self.peek_keyword("EDGE") {
                    self.advance();
                    self.expect_keyword("TYPE")?;
                    clauses.push(Clause::DropEdgeType(self.parse_drop_edge_type()?));
                    continue;
                }
                if self.peek_keyword("CONSTRAINT") {
                    self.advance();
                    clauses.push(Clause::DropConstraint(self.parse_drop_constraint()?));
                    continue;
                }
                return Err(anyhow!(
                    "Expected INDEX, LABEL, EDGE TYPE, or CONSTRAINT after DROP"
                ));
            }

            if self.peek_keyword("SHOW") {
                self.advance();

                if self.peek_keyword("CONSTRAINTS") {
                    self.advance();
                    clauses.push(Clause::ShowConstraints(self.parse_show_constraints()?));
                    continue;
                }

                if self.peek_keyword("DATABASE") {
                    self.advance();
                    clauses.push(Clause::ShowDatabase);
                    continue;
                }

                if self.peek_keyword("CONFIG") {
                    self.advance();
                    clauses.push(Clause::ShowConfig);
                    continue;
                }

                if self.peek_keyword("STATISTICS") {
                    self.advance();
                    clauses.push(Clause::ShowStatistics);
                    continue;
                }

                // SHOW INDEXES or SHOW VECTOR INDEXES
                let mut filter = None;
                if self.peek_keyword("VECTOR") {
                    self.advance();
                    filter = Some("VECTOR".to_string());
                } else if self.peek_keyword("FULLTEXT") {
                    self.advance();
                    filter = Some("FULLTEXT".to_string());
                }

                self.expect_keyword("INDEXES")?;
                clauses.push(Clause::ShowIndexes(ShowIndexesClause { filter }));
                continue;
            }

            if self.peek_keyword("COPY") {
                self.advance();
                clauses.push(Clause::Copy(self.parse_copy()?));
                continue;
            }

            if self.peek_keyword("BACKUP") {
                self.advance();
                clauses.push(Clause::Backup(self.parse_backup()?));
                continue;
            }

            if self.peek_keyword("VACUUM") {
                self.advance();
                clauses.push(Clause::Vacuum);
                continue;
            }

            if self.peek_keyword("CHECKPOINT") {
                self.advance();
                clauses.push(Clause::Checkpoint);
                continue;
            }

            if self.peek_keyword("SET") {
                self.advance();
                clauses.push(Clause::Set(self.parse_set()?));
                continue;
            }

            if self.peek_keyword("REMOVE") {
                self.advance();
                clauses.push(Clause::Remove(self.parse_remove()?));
                continue;
            }

            if self.peek_keyword("DELETE") {
                self.advance();
                clauses.push(Clause::Delete(self.parse_delete(false)?));
                continue;
            }

            if self.peek_keyword("DETACH") {
                self.advance();
                self.expect_keyword("DELETE")?;
                clauses.push(Clause::Delete(self.parse_delete(true)?));
                continue;
            }

            if self.peek_keyword("WITH") {
                self.advance();
                if self.peek_keyword("RECURSIVE") {
                    self.advance();
                    clauses.push(Clause::WithRecursive(self.parse_with_recursive()?));
                } else {
                    clauses.push(Clause::With(self.parse_with()?));
                }
                continue;
            }

            if self.peek_keyword("BEGIN") {
                self.advance();
                clauses.push(Clause::Begin);
                continue;
            }

            if self.peek_keyword("COMMIT") {
                self.advance();
                clauses.push(Clause::Commit);
                continue;
            }

            if self.peek_keyword("ROLLBACK") {
                self.advance();
                clauses.push(Clause::Rollback);
                continue;
            }

            break;
        }

        let mut return_clause = None;
        println!(
            "DEBUG: parse_single_query produced {} clauses",
            clauses.len()
        );
        for (i, c) in clauses.iter().enumerate() {
            println!("DEBUG: Clause {}: {:?}", i, c);
        }
        if self.peek_keyword("RETURN") {
            self.advance();
            return_clause = Some(self.parse_return()?);
        }

        let mut order_by = None;
        if self.peek_keyword("ORDER") {
            self.expect_keyword("ORDER")?;
            self.expect_keyword("BY")?;
            order_by = Some(self.parse_order_by()?);
        }

        let mut skip = None;
        if self.peek_keyword("SKIP") {
            self.advance();
            if let Some(Token::Number(n, _)) = self.peek_token() {
                self.advance();
                skip = Some(n.parse::<usize>()?);
            } else {
                return Err(anyhow!("Expected integer after SKIP"));
            }
        }

        let mut limit = None;
        if self.peek_keyword("LIMIT") {
            self.advance();
            if let Some(Token::Number(n, _)) = self.peek_token() {
                self.advance();
                limit = Some(n.parse::<usize>()?);
            } else {
                return Err(anyhow!("Expected integer after LIMIT"));
            }
        }

        Ok(CypherQuery {
            clauses,
            return_clause,
            order_by,
            skip,
            limit,
        })
    }

    // DDL Parsing Methods

    fn parse_create_vector_index(&mut self) -> Result<CreateVectorIndexClause> {
        let name = self.parse_identifier()?;
        let mut if_not_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            if_not_exists = true;
        }

        self.expect_keyword("FOR")?;
        self.expect_token(Token::LParen)?;
        let _var = self.parse_identifier()?; // e.g. (p:Person) -> p
        self.expect_token(Token::Colon)?;
        let label = self.parse_identifier()?;
        self.expect_token(Token::RParen)?;

        self.expect_keyword("ON")?;
        let prop_expr = self.parse_expr()?;
        // prop_expr should be var.property
        let property = if let Expr::Property(_, p) = prop_expr {
            p
        } else {
            return Err(anyhow!(
                "Expected property access in ON clause (e.g. n.embedding)"
            ));
        };

        let mut options = HashMap::new();
        if self.peek_keyword("OPTIONS") {
            self.advance();
            options = self.parse_map_literal()?;
        }

        Ok(CreateVectorIndexClause {
            name,
            label,
            property,
            options,
            if_not_exists,
        })
    }

    fn parse_create_fulltext_index(&mut self) -> Result<CreateFullTextIndexClause> {
        let name = self.parse_identifier()?;
        let mut if_not_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            if_not_exists = true;
        }

        self.expect_keyword("FOR")?;
        self.expect_token(Token::LParen)?;
        let _var = self.parse_identifier()?;
        self.expect_token(Token::Colon)?;
        let label = self.parse_identifier()?;
        self.expect_token(Token::RParen)?;

        self.expect_keyword("ON")?;
        self.expect_keyword("EACH")?;
        self.expect_token(Token::LBracket)?;

        let mut properties = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            if let Expr::Property(_, p) = expr {
                properties.push(p);
            } else {
                return Err(anyhow!("Expected property in ON EACH list"));
            }
            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        self.expect_token(Token::RBracket)?;

        let mut options = HashMap::new();
        if self.peek_keyword("OPTIONS") {
            self.advance();
            options = self.parse_map_literal()?;
        }

        Ok(CreateFullTextIndexClause {
            name,
            label,
            properties,
            options,
            if_not_exists,
        })
    }

    /// Parses a CREATE JSON FULLTEXT INDEX statement.
    ///
    /// Syntax: CREATE JSON FULLTEXT INDEX name FOR (v:Label) ON column OPTIONS {...}
    fn parse_create_json_fts_index(&mut self) -> Result<CreateJsonFtsIndexClause> {
        let name = self.parse_identifier()?;
        let mut if_not_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            if_not_exists = true;
        }

        self.expect_keyword("FOR")?;
        self.expect_token(Token::LParen)?;
        let _var = self.parse_identifier()?;
        self.expect_token(Token::Colon)?;
        let label = self.parse_identifier()?;
        self.expect_token(Token::RParen)?;

        self.expect_keyword("ON")?;
        // The column can be a simple identifier like "_doc"
        let column = self.parse_identifier()?;

        let mut options = HashMap::new();
        if self.peek_keyword("OPTIONS") {
            self.advance();
            options = self.parse_map_literal()?;
        }

        Ok(CreateJsonFtsIndexClause {
            name,
            label,
            column,
            options,
            if_not_exists,
        })
    }

    fn parse_create_scalar_index(&mut self) -> Result<CreateScalarIndexClause> {
        let name = self.parse_identifier()?;
        let mut if_not_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            if_not_exists = true;
        }

        self.expect_keyword("FOR")?;
        self.expect_token(Token::LParen)?;
        let _var = self.parse_identifier()?;
        self.expect_token(Token::Colon)?;
        let label = self.parse_identifier()?;
        self.expect_token(Token::RParen)?;

        self.expect_keyword("ON")?;
        self.expect_token(Token::LParen)?;
        let mut properties = Vec::new();
        loop {
            let prop_expr = self.parse_expr()?;
            // For simple property access, store just the property name (for Lance column)
            // For complex expressions, store the string representation
            match &prop_expr {
                Expr::Property(_, p) => properties.push(p.clone()),
                _ => properties.push(prop_expr.to_string_repr()),
            }

            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        self.expect_token(Token::RParen)?;

        let mut where_clause = None;
        if self.peek_keyword("WHERE") {
            self.advance();
            where_clause = Some(self.parse_expr()?);
        }

        // Optional options? Not strictly in syntax but good for consistency
        let mut options = HashMap::new();
        if self.peek_keyword("OPTIONS") {
            self.advance();
            options = self.parse_map_literal()?;
        }

        Ok(CreateScalarIndexClause {
            name,
            label,
            properties,
            where_clause,
            options,
            if_not_exists,
        })
    }

    fn parse_drop_index(&mut self) -> Result<DropIndexClause> {
        let name = self.parse_identifier()?;
        let mut if_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("EXISTS")?;
            if_exists = true;
        }
        Ok(DropIndexClause { name, if_exists })
    }

    fn parse_identifier(&mut self) -> Result<String> {
        if let Some(Token::Word(w)) = self.peek_token() {
            self.advance();
            Ok(w.value)
        } else {
            Err(anyhow!("Expected identifier"))
        }
    }

    fn parse_map_literal(&mut self) -> Result<HashMap<String, Value>> {
        self.expect_token(Token::LBrace)?;
        let mut map = HashMap::new();
        if !self.consume_token(Token::RBrace) {
            loop {
                let key = if let Some(Token::Word(w)) = self.peek_token() {
                    self.advance();
                    w.value
                } else if let Some(Token::SingleQuotedString(s)) = self.peek_token() {
                    self.advance();
                    s
                } else {
                    return Err(anyhow!("Expected map key"));
                };

                self.expect_token(Token::Colon)?;
                let val = self.parse_json_value()?;
                map.insert(key, val);

                if self.consume_token(Token::RBrace) {
                    break;
                }
                self.expect_token(Token::Comma)?;
            }
        }
        Ok(map)
    }

    fn parse_json_value(&mut self) -> Result<Value> {
        // Simple JSON parser using tokens
        if let Some(Token::Number(n, _)) = self.peek_token() {
            self.advance();
            if let Ok(i) = n.parse::<i64>() {
                return Ok(json!(i));
            }
            if let Ok(f) = n.parse::<f64>() {
                return Ok(json!(f));
            }
            return Ok(Value::String(n)); // Fallback
        }
        if let Some(Token::SingleQuotedString(s)) = self.peek_token() {
            self.advance();
            return Ok(Value::String(s));
        }
        if let Some(Token::DoubleQuotedString(s)) = self.peek_token() {
            self.advance();
            return Ok(Value::String(s));
        }
        if let Some(Token::Word(w)) = self.peek_token() {
            self.advance();
            if w.quote_style == Some('"') {
                return Ok(Value::String(w.value));
            }
            if w.value == "true" {
                return Ok(Value::Bool(true));
            }
            if w.value == "false" {
                return Ok(Value::Bool(false));
            }
            if w.value == "null" {
                return Ok(Value::Null);
            }
            return Ok(Value::String(w.value));
        }

        if let Some(token) = self.peek_token() {
            if token == Token::LBrace {
                return Ok(Value::Object(
                    self.parse_map_literal()?.into_iter().collect(),
                ));
            }
            if token == Token::LBracket {
                self.advance(); // consume [
                let mut list = Vec::new();
                if !self.consume_token(Token::RBracket) {
                    loop {
                        list.push(self.parse_json_value()?);
                        if self.consume_token(Token::RBracket) {
                            break;
                        }
                        self.expect_token(Token::Comma)?;
                    }
                }
                return Ok(Value::Array(list));
            }
        }

        Err(anyhow!(
            "Unexpected token in map value: {:?}",
            self.peek_token()
        ))
    }

    fn parse_call(&mut self) -> Result<CallClause> {
        let mut name_parts = Vec::new();
        while let Some(Token::Word(w)) = self.peek_token() {
            self.advance();
            name_parts.push(w.value);
            if !self.consume_token(Token::Period) {
                break;
            }
        }
        let procedure_name = name_parts.join(".");

        self.expect_token(Token::LParen)?;
        let mut arguments = Vec::new();
        if !self.consume_token(Token::RParen) {
            loop {
                arguments.push(self.parse_expr()?);
                if self.consume_token(Token::RParen) {
                    break;
                }
                self.expect_token(Token::Comma)?;
            }
        }

        let mut yield_items = Vec::new();
        if self.peek_keyword("YIELD") {
            self.advance();
            loop {
                if let Some(Token::Word(w)) = self.peek_token() {
                    self.advance();
                    let name = w.value;
                    let mut alias = None;
                    if self.peek_keyword("AS") {
                        self.advance();
                        if let Some(Token::Word(aw)) = self.peek_token() {
                            self.advance();
                            alias = Some(aw.value);
                        } else {
                            return Err(anyhow!("Expected alias after AS"));
                        }
                    }
                    yield_items.push((name, alias));
                } else {
                    return Err(anyhow!("Expected yield item"));
                }
                if !self.consume_token(Token::Comma) {
                    break;
                }
            }
        }

        Ok(CallClause {
            procedure_name,
            arguments,
            yield_items,
        })
    }

    fn parse_call_subquery(&mut self) -> Result<Box<Query>> {
        self.expect_token(Token::LBrace)?;
        let query = self.parse()?;
        self.expect_token(Token::RBrace)?;
        Ok(Box::new(query))
    }

    fn parse_order_by(&mut self) -> Result<Vec<SortItem>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let mut ascending = true;

            if self.peek_keyword("DESC") || self.peek_keyword("DESCENDING") {
                self.advance();
                ascending = false;
            } else if self.peek_keyword("ASC") || self.peek_keyword("ASCENDING") {
                self.advance();
            }

            items.push(SortItem { expr, ascending });

            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_pattern(&mut self) -> Result<Pattern> {
        let mut variable = None;
        let mut shortest_path = false;

        // Check for named path: p = ...
        if let Some(Token::Word(w)) = self.peek_token() {
            let next = self.tokens.get(self.pos + 1);
            if matches!(next, Some(Token::Eq)) {
                variable = Some(w.value);
                self.advance(); // consume variable
                self.advance(); // consume =
            }
        }

        // Check for shortestPath: shortestPath(...)
        if self.peek_keyword("shortestPath") {
            self.advance();
            self.expect_token(Token::LParen)?;
            shortest_path = true;
        }

        let mut parts = Vec::new();

        loop {
            parts.push(PatternPart::Node(self.parse_node()?));

            loop {
                // Check for relationship
                if self.consume_token(Token::Lt) {
                    self.expect_token(Token::Minus)?;
                    self.expect_token(Token::LBracket)?;
                    parts.push(PatternPart::Relationship(
                        self.parse_relationship_inner(Direction::Incoming)?,
                    ));
                    parts.push(PatternPart::Node(self.parse_node()?));
                } else if self.consume_token(Token::Minus) {
                    if self.consume_token(Token::LBracket) {
                        parts.push(PatternPart::Relationship(
                            self.parse_relationship_inner(Direction::Both)?,
                        ));
                        parts.push(PatternPart::Node(self.parse_node()?));
                    } else if self.consume_token(Token::Minus) {
                        // Support -- or -->
                        if self.consume_token(Token::Gt) {
                            parts.push(PatternPart::Relationship(RelationshipPattern {
                                variable: None,
                                rel_types: vec![],
                                direction: Direction::Outgoing,
                                min_hops: None,
                                max_hops: None,
                                properties: vec![],
                            }));
                            parts.push(PatternPart::Node(self.parse_node()?));
                        } else {
                            parts.push(PatternPart::Relationship(RelationshipPattern {
                                variable: None,
                                rel_types: vec![],
                                direction: Direction::Both,
                                min_hops: None,
                                max_hops: None,
                                properties: vec![],
                            }));
                            parts.push(PatternPart::Node(self.parse_node()?));
                        }
                    } else {
                        // Maybe it's just - followed by node? No, Cypher relationships are []
                        return Err(anyhow!("Expected [ after - in relationship"));
                    }
                } else {
                    break;
                }
            }

            if !self.consume_token(Token::Comma) {
                break;
            }
        }

        if shortest_path {
            self.expect_token(Token::RParen)?;
        }

        Ok(Pattern {
            variable,
            parts,
            shortest_path,
        })
    }

    fn parse_relationship_inner(
        &mut self,
        mut direction: Direction,
    ) -> Result<RelationshipPattern> {
        let variable = if let Some(Token::Word(w)) = self.peek_token() {
            let val_up = w.value.to_uppercase();
            if val_up != "WHERE" && val_up != "RETURN" && !val_up.starts_with(':') {
                self.advance();
                Some(w.value)
            } else {
                None
            }
        } else {
            None
        };

        let mut rel_types = Vec::new();
        if self.consume_token(Token::Colon) {
            loop {
                if let Some(Token::Word(w)) = self.peek_token() {
                    self.advance();
                    rel_types.push(w.value);
                }
                if !self.consume_token(Token::Pipe) {
                    break;
                }
            }
        }

        let mut min_hops = None;
        let mut max_hops = None;

        if self.consume_token(Token::Mul) {
            let mut min = 1;
            let mut max = u32::MAX;

            if let Some(Token::Number(n, _)) = self.peek_token() {
                self.advance();
                let n_trimmed = n.trim_end_matches('.');
                min = n_trimmed.parse::<u32>()?;

                let has_range_dots = n.ends_with('.') || self.consume_token(Token::Period);
                if has_range_dots {
                    self.consume_token(Token::Period); // optional second dot
                    if let Some(Token::Number(n2, _)) = self.peek_token() {
                        self.advance();
                        max = n2.trim_start_matches('.').parse::<u32>()?;
                    } else {
                        max = u32::MAX;
                    }
                } else {
                    max = min;
                }
            } else if self.consume_token(Token::Period)
                && self.consume_token(Token::Period)
                && let Some(Token::Number(n2, _)) = self.peek_token()
            {
                self.advance();
                max = n2.trim_start_matches('.').parse::<u32>()?;
            }

            min_hops = Some(min);
            max_hops = Some(max);
        }

        let properties = self.parse_properties()?;

        self.expect_token(Token::RBracket)?;

        if self.consume_token(Token::Arrow) {
            if direction == Direction::Incoming {
                return Err(anyhow!("Invalid relationship direction <-[]->"));
            }
            direction = Direction::Outgoing;
        } else {
            self.expect_token(Token::Minus)?;
            if direction == Direction::Both && self.consume_token(Token::Gt) {
                direction = Direction::Outgoing;
            }
        }

        Ok(RelationshipPattern {
            variable,
            rel_types,
            direction,
            min_hops,
            max_hops,
            properties,
        })
    }

    fn parse_match(&mut self, optional: bool) -> Result<MatchClause> {
        let pattern = self.parse_pattern()?;

        let where_clause = if self.peek_keyword("WHERE") {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(MatchClause {
            pattern,
            where_clause,
            optional,
        })
    }

    fn parse_create(&mut self) -> Result<CreateClause> {
        let pattern = self.parse_pattern()?;
        Ok(CreateClause { pattern })
    }

    fn parse_node(&mut self) -> Result<NodePattern> {
        self.expect_token(Token::LParen)?;

        let variable = if let Some(Token::Word(w)) = self.peek_token() {
            let next = self.tokens.get(self.pos + 1);
            if matches!(
                next,
                Some(Token::Colon) | Some(Token::LBrace) | Some(Token::RParen)
            ) {
                self.advance();
                Some(w.value)
            } else {
                None
            }
        } else {
            None
        };

        let mut labels = Vec::new();
        if self.consume_token(Token::Colon)
            && let Some(Token::Word(w)) = self.peek_token()
        {
            self.advance();
            labels.push(w.value);
        }

        let properties = self.parse_properties()?;

        self.expect_token(Token::RParen)?;

        Ok(NodePattern {
            variable,
            labels,
            properties,
        })
    }

    fn parse_return(&mut self) -> Result<ReturnClause> {
        let mut items = Vec::new();
        loop {
            if self.consume_token(Token::Mul) {
                items.push(ReturnItem::All);
            } else {
                let expr = self.parse_expr()?;
                let mut alias = None;
                if self.peek_keyword("AS") {
                    self.advance();
                    alias = Some(self.parse_identifier()?);
                }
                items.push(ReturnItem::Expr { expr, alias });
            }

            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        Ok(ReturnClause { items })
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.peek_keyword("OR") {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: Operator::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        while self.peek_keyword("AND") {
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: Operator::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.peek_keyword("NOT") {
            self.advance();
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_arithmetic()?;

        if self.peek_keyword("IS") {
            // Peek ahead to see if it is NULL or NOT NULL
            let next_pos = self.pos + 1;
            let is_null = if let Some(Token::Word(w)) = self.tokens.get(next_pos) {
                w.value.eq_ignore_ascii_case("NULL")
            } else {
                false
            };

            let is_not_null = if let Some(Token::Word(w)) = self.tokens.get(next_pos) {
                if w.value.eq_ignore_ascii_case("NOT") {
                    if let Some(Token::Word(w2)) = self.tokens.get(next_pos + 1) {
                        w2.value.eq_ignore_ascii_case("NULL")
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if is_null || is_not_null {
                self.advance();
                let negated = if self.peek_keyword("NOT") {
                    self.advance();
                    true
                } else {
                    false
                };
                self.expect_keyword("NULL")?;

                if negated {
                    return Ok(Expr::IsNotNull(Box::new(left)));
                } else {
                    return Ok(Expr::IsNull(Box::new(left)));
                }
            }
        }

        if self.peek_keyword("VALID_AT") {
            self.advance();
            let time_expr;
            let mut start_prop = None;
            let mut end_prop = None;

            if self.consume_token(Token::LParen) {
                time_expr = self.parse_expr()?;
                if self.consume_token(Token::Comma) {
                    let s = self.parse_expr()?;
                    if let Expr::Literal(Value::String(str_val)) = s {
                        start_prop = Some(str_val);
                    } else {
                        return Err(anyhow!("VALID_AT start property must be a string literal"));
                    }

                    if self.consume_token(Token::Comma) {
                        let e = self.parse_expr()?;
                        if let Expr::Literal(Value::String(str_val)) = e {
                            end_prop = Some(str_val);
                        } else {
                            return Err(anyhow!("VALID_AT end property must be a string literal"));
                        }
                    }
                }
                self.expect_token(Token::RParen)?;
            } else {
                time_expr = self.parse_arithmetic()?;
            }

            return self.expand_valid_at_macro(left, time_expr, start_prop, end_prop);
        }

        let op = if self.consume_token(Token::Eq) {
            Some(Operator::Eq)
        } else if self.consume_token(Token::Tilde) {
            self.expect_token(Token::Eq)?;
            Some(Operator::ApproxEq)
        } else if self.consume_token(Token::Neq) {
            Some(Operator::NotEq)
        } else if self.consume_token(Token::Lt) {
            Some(Operator::Lt)
        } else if self.consume_token(Token::LtEq) {
            Some(Operator::LtEq)
        } else if self.consume_token(Token::Gt) {
            Some(Operator::Gt)
        } else if self.consume_token(Token::GtEq) {
            Some(Operator::GtEq)
        } else if self.peek_keyword("IN") {
            self.advance();
            Some(Operator::In)
        } else if self.peek_keyword("CONTAINS") {
            self.advance();
            Some(Operator::Contains)
        } else if self.peek_keyword("STARTS") {
            self.advance();
            self.expect_keyword("WITH")?;
            Some(Operator::StartsWith)
        } else if self.peek_keyword("ENDS") {
            self.advance();
            self.expect_keyword("WITH")?;
            Some(Operator::EndsWith)
        } else {
            None
        };

        if let Some(op) = op {
            let right = self.parse_arithmetic()?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_arithmetic(&mut self) -> Result<Expr> {
        let mut left = self.parse_term()?;

        loop {
            if self.consume_token(Token::Plus) {
                let right = self.parse_term()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: Operator::Add,
                    right: Box::new(right),
                };
            } else if self.consume_token(Token::Minus) {
                let right = self.parse_term()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: Operator::Sub,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_term(&mut self) -> Result<Expr> {
        let mut left = self.parse_power()?;

        loop {
            if self.consume_token(Token::Mul) {
                let right = self.parse_power()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: Operator::Mul,
                    right: Box::new(right),
                };
            } else if self.consume_token(Token::Div) {
                let right = self.parse_power()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: Operator::Div,
                    right: Box::new(right),
                };
            } else if self.consume_token(Token::Mod) {
                let right = self.parse_power()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: Operator::Mod,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_power(&mut self) -> Result<Expr> {
        let left = self.parse_atom()?;

        if self.consume_token(Token::Caret) {
            let right = self.parse_power()?; // Right-associative
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: Operator::Pow,
                right: Box::new(right),
            });
        }
        Ok(left)
    }

    fn parse_atom(&mut self) -> Result<Expr> {
        if self.consume_token(Token::Mul) {
            return Ok(Expr::Wildcard);
        }

        if self.peek_keyword("REDUCE") {
            self.advance();
            self.expect_token(Token::LParen)?;

            // accumulator = init
            let accumulator = self.parse_identifier()?;
            self.expect_token(Token::Eq)?;
            let init = self.parse_expr()?;
            self.expect_token(Token::Comma)?;

            // variable IN list
            let variable = self.parse_identifier()?;
            self.expect_keyword("IN")?;
            let list = self.parse_expr()?;

            // | expression
            self.expect_token(Token::Pipe)?;
            let expr = self.parse_expr()?;

            self.expect_token(Token::RParen)?;

            return Ok(Expr::Reduce {
                accumulator,
                init: Box::new(init),
                variable,
                list: Box::new(list),
                expr: Box::new(expr),
            });
        }

        if self.peek_keyword("EXISTS") {
            self.advance();
            self.expect_token(Token::LBrace)?;
            let query = self.parse()?;
            self.expect_token(Token::RBrace)?;
            return Ok(Expr::Exists(Box::new(query)));
        }

        if self.peek_keyword("CASE") {
            self.advance();
            let mut expr = None;
            if !self.peek_keyword("WHEN") {
                expr = Some(Box::new(self.parse_expr()?));
            }

            let mut when_then = Vec::new();
            while self.peek_keyword("WHEN") {
                self.advance();
                let w = self.parse_expr()?;
                self.expect_keyword("THEN")?;
                let t = self.parse_expr()?;
                when_then.push((w, t));
            }

            let mut else_expr = None;
            if self.peek_keyword("ELSE") {
                self.advance();
                else_expr = Some(Box::new(self.parse_expr()?));
            }

            self.expect_keyword("END")?;

            return Ok(Expr::Case {
                expr,
                when_then,
                else_expr,
            });
        }

        if self.consume_token(Token::LBracket) {
            // Check for List Comprehension: [ var IN list ... ]
            let mut is_comprehension = false;
            if let Some(Token::Word(_)) = self.peek_token() {
                let next_pos = self.pos + 1;
                if next_pos < self.tokens.len()
                    && let Token::Word(w2) = &self.tokens[next_pos]
                    && w2.value.eq_ignore_ascii_case("IN")
                {
                    is_comprehension = true;
                }
            }

            if is_comprehension {
                let variable = self.parse_identifier()?;
                self.expect_keyword("IN")?;
                let list = self.parse_expr()?;

                let mut where_clause = None;
                if self.peek_keyword("WHERE") {
                    self.advance();
                    where_clause = Some(Box::new(self.parse_expr()?));
                }

                let mut mapping = None;
                if self.consume_token(Token::Pipe) {
                    mapping = Some(Box::new(self.parse_expr()?));
                }

                self.expect_token(Token::RBracket)?;

                return Ok(Expr::ListComprehension {
                    variable,
                    list: Box::new(list),
                    where_clause,
                    mapping,
                });
            }

            let mut items = Vec::new();
            if !self.consume_token(Token::RBracket) {
                loop {
                    items.push(self.parse_expr()?);
                    if self.consume_token(Token::RBracket) {
                        break;
                    }
                    self.expect_token(Token::Comma)?;
                }
            }
            return Ok(Expr::List(items));
        }

        if self.consume_token(Token::LBrace) {
            let mut items = Vec::new();
            if !self.consume_token(Token::RBrace) {
                loop {
                    let key = if let Some(Token::Word(w)) = self.peek_token() {
                        self.advance();
                        w.value
                    } else if let Some(Token::SingleQuotedString(s)) = self.peek_token() {
                        self.advance();
                        s
                    } else if let Some(Token::DoubleQuotedString(s)) = self.peek_token() {
                        self.advance();
                        s
                    } else {
                        return Err(anyhow!("Expected property key"));
                    };

                    self.expect_token(Token::Colon)?;
                    let value = self.parse_expr()?;
                    items.push((key, value));

                    if self.consume_token(Token::RBrace) {
                        break;
                    }
                    self.expect_token(Token::Comma)?;
                }
            }
            return Ok(Expr::Map(items));
        }

        if self.consume_token(Token::LParen) {
            let expr = self.parse_expr()?;
            self.expect_token(Token::RParen)?;
            return Ok(expr);
        }

        if self.consume_token(Token::Char('$')) {
            let mut expr = if let Some(Token::Word(w)) = self.peek_token() {
                self.advance();
                Expr::Parameter(w.value)
            } else {
                return Err(anyhow!("Expected parameter name after $"));
            };

            loop {
                if self.consume_token(Token::Period) {
                    if let Some(Token::Word(p)) = self.peek_token() {
                        self.advance();
                        expr = Expr::Property(Box::new(expr), p.value);
                    } else {
                        return Err(anyhow!("Expected property name"));
                    }
                } else if self.consume_token(Token::LBracket) {
                    let index_expr = self.parse_expr()?;
                    self.expect_token(Token::RBracket)?;
                    expr = Expr::ArrayIndex(Box::new(expr), Box::new(index_expr));
                } else {
                    break;
                }
            }
            return Ok(expr);
        }

        if let Some(Token::Placeholder(s)) = self.peek_token() {
            self.advance();
            let name = s.trim_start_matches('$').to_string();
            let mut expr = Expr::Parameter(name);

            loop {
                if self.consume_token(Token::Period) {
                    if let Some(Token::Word(p)) = self.peek_token() {
                        self.advance();
                        expr = Expr::Property(Box::new(expr), p.value);
                    } else {
                        return Err(anyhow!("Expected property name"));
                    }
                } else if self.consume_token(Token::LBracket) {
                    let index_expr = self.parse_expr()?;
                    self.expect_token(Token::RBracket)?;
                    expr = Expr::ArrayIndex(Box::new(expr), Box::new(index_expr));
                } else {
                    break;
                }
            }
            return Ok(expr);
        }

        if self.peek_keyword("ANY")
            || self.peek_keyword("ALL")
            || self.peek_keyword("NONE")
            || self.peek_keyword("SINGLE")
        {
            let func_name = if let Some(Token::Word(w)) = self.peek_token() {
                w.value.to_uppercase()
            } else {
                unreachable!()
            };
            self.advance();
            self.expect_token(Token::LParen)?;

            // Parse list comprehension syntax without brackets
            // variable IN list WHERE predicate

            let variable = self.parse_identifier()?;
            self.expect_keyword("IN")?;
            let list = self.parse_expr()?;

            let mut where_clause = None;
            if self.peek_keyword("WHERE") {
                self.advance();
                where_clause = Some(Box::new(self.parse_expr()?));
            }

            self.expect_token(Token::RParen)?;

            // Construct Expr::FunctionCall with ListComprehension as argument
            let lc = Expr::ListComprehension {
                variable,
                list: Box::new(list),
                where_clause,
                mapping: None,
            };

            return Ok(Expr::FunctionCall {
                name: func_name,
                args: vec![lc],
                distinct: false,
            });
        }

        if let Some(Token::Word(w)) = self.peek_token() {
            self.advance();

            if w.quote_style == Some('"') {
                return Ok(Expr::Literal(Value::String(w.value)));
            }

            if w.value == "true" {
                return Ok(Expr::Literal(Value::Bool(true)));
            }
            if w.value == "false" {
                return Ok(Expr::Literal(Value::Bool(false)));
            }
            if w.value == "null" {
                return Ok(Expr::Literal(Value::Null));
            }

            let mut expr = if self.consume_token(Token::LParen) {
                let mut distinct = false;
                if self.peek_keyword("DISTINCT") {
                    self.advance();
                    distinct = true;
                }
                let mut args = Vec::new();
                if !self.consume_token(Token::RParen) {
                    loop {
                        args.push(self.parse_expr()?);
                        if self.consume_token(Token::RParen) {
                            break;
                        }
                        self.expect_token(Token::Comma)?;
                    }
                }
                Expr::FunctionCall {
                    name: w.value.to_uppercase(),
                    args,
                    distinct,
                }
            } else {
                Expr::Identifier(w.value)
            };

            loop {
                if self.consume_token(Token::Period) {
                    if let Some(Token::Word(p)) = self.peek_token() {
                        self.advance();

                        // Check for namespaced function call: identifier.method(...)
                        if self.consume_token(Token::LParen) {
                            let mut args = Vec::new();
                            if !self.consume_token(Token::RParen) {
                                loop {
                                    args.push(self.parse_expr()?);
                                    if self.consume_token(Token::RParen) {
                                        break;
                                    }
                                    self.expect_token(Token::Comma)?;
                                }
                            }

                            // Construct function name
                            let func_name = if let Expr::Identifier(id) = &expr {
                                format!("{}.{}", id, p.value)
                            } else {
                                // Potentially chained property: a.b.c() -> property(a,b).c()
                                // For now, let's support Expr::Property as well?
                                // If expr is Property(a, b), name = a.b.c
                                // We'd need to reconstruct the full dot-separated name.
                                // But typically namespaced functions are 2 levels: namespace.func.
                                return Err(anyhow!(
                                    "Namespaced function call base must be an identifier"
                                ));
                            };

                            expr = Expr::FunctionCall {
                                name: func_name,
                                args,
                                distinct: false,
                            };
                        } else {
                            expr = Expr::Property(Box::new(expr), p.value);
                        }
                    } else {
                        return Err(anyhow!("Expected property name"));
                    }
                } else if self.consume_token(Token::LBracket) {
                    let index_expr = self.parse_expr()?;
                    self.expect_token(Token::RBracket)?;
                    expr = Expr::ArrayIndex(Box::new(expr), Box::new(index_expr));
                } else {
                    break;
                }
            }

            if self.peek_keyword("OVER") {
                self.advance();
                self.expect_token(Token::LParen)?;

                let mut partition_by = Vec::new();
                if self.peek_keyword("PARTITION") {
                    self.advance();
                    self.expect_keyword("BY")?;
                    loop {
                        partition_by.push(self.parse_expr()?);
                        if !self.consume_token(Token::Comma) {
                            break;
                        }
                    }
                }

                let mut order_by = Vec::new();
                if self.peek_keyword("ORDER") {
                    self.advance();
                    self.expect_keyword("BY")?;
                    loop {
                        let e = self.parse_expr()?;
                        let mut asc = true;
                        if self.peek_keyword("DESC") || self.peek_keyword("DESCENDING") {
                            self.advance();
                            asc = false;
                        } else if self.peek_keyword("ASC") || self.peek_keyword("ASCENDING") {
                            self.advance();
                        }
                        order_by.push((e, asc));
                        if !self.consume_token(Token::Comma) {
                            break;
                        }
                    }
                }

                self.expect_token(Token::RParen)?;

                expr = Expr::WindowFunction {
                    function: Box::new(expr),
                    partition_by,
                    order_by,
                };
            }

            return Ok(expr);
        }

        if let Some(Token::Number(n, _)) = self.peek_token() {
            self.advance();
            if let Ok(i) = n.parse::<i64>() {
                return Ok(Expr::Literal(json!(i)));
            }
            if let Ok(f) = n.parse::<f64>() {
                return Ok(Expr::Literal(json!(f)));
            }
            return Err(anyhow!("Invalid number {}", n));
        }

        if let Some(Token::SingleQuotedString(s)) = self.peek_token() {
            self.advance();
            return Ok(Expr::Literal(Value::String(s)));
        }
        if let Some(Token::DoubleQuotedString(s)) = self.peek_token() {
            self.advance();
            return Ok(Expr::Literal(Value::String(s)));
        }

        Err(anyhow!(
            "Unexpected token in expression: {:?}",
            self.peek_token()
        ))
    }

    fn peek_token(&self) -> Option<Token> {
        self.tokens.get(self.pos).cloned()
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    fn consume_token(&mut self, expected: Token) -> bool {
        if let Some(token) = self.peek_token()
            && token == expected
        {
            self.advance();
            return true;
        }
        false
    }

    fn expect_token(&mut self, expected: Token) -> Result<()> {
        if !self.consume_token(expected.clone()) {
            return Err(anyhow!(
                "Expected {:?}, found {:?}",
                expected,
                self.peek_token()
            ));
        }
        Ok(())
    }

    fn peek_keyword(&self, kw: &str) -> bool {
        if let Some(Token::Word(w)) = self.peek_token() {
            w.value.eq_ignore_ascii_case(kw)
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<()> {
        if self.peek_keyword(kw) {
            self.advance();
            Ok(())
        } else {
            Err(anyhow!(
                "Expected keyword {}, found {:?}",
                kw,
                self.peek_token()
            ))
        }
    }

    fn parse_unwind(&mut self) -> Result<UnwindClause> {
        let expr = self.parse_expr()?;
        self.expect_keyword("AS")?;
        if let Some(Token::Word(w)) = self.peek_token() {
            self.advance();
            Ok(UnwindClause {
                expr,
                variable: w.value,
            })
        } else {
            Err(anyhow!("Expected variable after AS in UNWIND"))
        }
    }

    fn parse_set(&mut self) -> Result<SetClause> {
        let mut items = Vec::new();
        loop {
            let left = self.parse_atom()?;
            if self.consume_token(Token::Eq) {
                let right = self.parse_expr()?;
                items.push(SetItem::Property {
                    expr: left,
                    value: right,
                });
            } else if self.consume_token(Token::Colon) {
                if let Expr::Identifier(var) = left {
                    let mut labels = Vec::new();
                    if let Some(Token::Word(w)) = self.peek_token() {
                        self.advance();
                        labels.push(w.value);
                    }
                    items.push(SetItem::Labels {
                        variable: var,
                        labels,
                    });
                } else {
                    return Err(anyhow!("Expected variable before : in SET"));
                }
            } else {
                return Err(anyhow!("Expected = or : in SET"));
            }

            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        Ok(SetClause { items })
    }

    fn parse_remove(&mut self) -> Result<RemoveClause> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_atom()?;
            if self.consume_token(Token::Colon) {
                if let Expr::Identifier(var) = expr {
                    let mut labels = Vec::new();
                    if let Some(Token::Word(w)) = self.peek_token() {
                        self.advance();
                        labels.push(w.value);
                    }
                    items.push(RemoveItem::Labels {
                        variable: var,
                        labels,
                    });
                } else {
                    return Err(anyhow!("Expected variable before : in REMOVE"));
                }
            } else {
                items.push(RemoveItem::Property(expr));
            }

            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        Ok(RemoveClause { items })
    }

    fn parse_delete(&mut self, detach: bool) -> Result<DeleteClause> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_expr()?);
            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        Ok(DeleteClause { items, detach })
    }

    fn parse_properties(&mut self) -> Result<Vec<(String, Expr)>> {
        let mut props = Vec::new();
        if self.consume_token(Token::LBrace) && !self.consume_token(Token::RBrace) {
            loop {
                if let Some(Token::Word(w)) = self.peek_token() {
                    self.advance();
                    self.expect_token(Token::Colon)?;
                    let value = self.parse_expr()?;
                    props.push((w.value, value));
                } else {
                    return Err(anyhow!("Expected property name"));
                }
                if self.consume_token(Token::RBrace) {
                    break;
                }
                self.expect_token(Token::Comma)?;
            }
        }
        Ok(props)
    }

    fn parse_merge(&mut self) -> Result<MergeClause> {
        let pattern = self.parse_pattern()?;
        let mut on_match = None;
        let mut on_create = None;

        while self.peek_keyword("ON") {
            self.advance();
            if self.peek_keyword("MATCH") {
                self.advance();
                self.expect_keyword("SET")?;
                on_match = Some(self.parse_set()?);
            } else if self.peek_keyword("CREATE") {
                self.advance();
                self.expect_keyword("SET")?;
                on_create = Some(self.parse_set()?);
            } else {
                return Err(anyhow!("Expected MATCH or CREATE after ON in MERGE"));
            }
        }

        Ok(MergeClause {
            pattern,
            on_match,
            on_create,
        })
    }

    fn parse_with(&mut self) -> Result<WithClause> {
        let mut return_items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let alias = if self.peek_keyword("AS") {
                self.advance();
                Some(self.parse_identifier()?)
            } else {
                None
            };
            return_items.push(ReturnItem::Expr { expr, alias });

            if !self.consume_token(Token::Comma) {
                break;
            }
        }

        let where_clause = if self.peek_keyword("WHERE") {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(WithClause {
            return_clause: ReturnClause {
                items: return_items,
            },
            where_clause,
        })
    }

    fn parse_with_recursive(&mut self) -> Result<WithRecursiveClause> {
        let name = self.parse_identifier()?;
        self.expect_keyword("AS")?;
        self.expect_token(Token::LParen)?;

        let query = self.parse()?;

        self.expect_token(Token::RParen)?;

        Ok(WithRecursiveClause {
            name,
            query: Box::new(query),
        })
    }

    fn parse_copy(&mut self) -> Result<CopyClause> {
        let target = self.parse_identifier()?;

        let is_export = if self.peek_keyword("FROM") {
            self.advance();
            false
        } else if self.peek_keyword("TO") {
            self.advance();
            true
        } else {
            return Err(anyhow!("Expected FROM or TO after target in COPY"));
        };

        let source = if let Some(Token::SingleQuotedString(s)) = self.peek_token() {
            self.advance();
            s
        } else if let Some(Token::DoubleQuotedString(s)) = self.peek_token() {
            self.advance();
            s
        } else {
            return Err(anyhow!(
                "Expected string literal for COPY source/destination"
            ));
        };

        let mut options = HashMap::new();
        if self.peek_keyword("WITH") {
            self.advance();
            options = self.parse_map_literal()?;
        }

        Ok(CopyClause {
            target,
            source,
            is_export,
            options,
        })
    }

    fn parse_backup(&mut self) -> Result<BackupClause> {
        self.expect_keyword("TO")?;

        let destination = if let Some(Token::SingleQuotedString(s)) = self.peek_token() {
            self.advance();
            s
        } else if let Some(Token::DoubleQuotedString(s)) = self.peek_token() {
            self.advance();
            s
        } else {
            return Err(anyhow!("Expected string literal for BACKUP destination"));
        };

        let mut options = HashMap::new();
        if self.peek_keyword("WITH") {
            self.advance();
            options = self.parse_map_literal()?;
        }

        Ok(BackupClause {
            destination,
            options,
        })
    }

    // Schema DDL Parsing Methods

    fn parse_create_label(&mut self) -> Result<CreateLabelClause> {
        let name = self.parse_identifier()?;
        let mut if_not_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            if_not_exists = true;
        }

        self.expect_token(Token::LParen)?;
        let mut properties = Vec::new();
        if !self.consume_token(Token::RParen) {
            loop {
                properties.push(self.parse_property_definition()?);
                if self.consume_token(Token::RParen) {
                    break;
                }
                self.expect_token(Token::Comma)?;
            }
        }
        Ok(CreateLabelClause {
            name,
            properties,
            if_not_exists,
        })
    }

    fn parse_create_edge_type(&mut self) -> Result<CreateEdgeTypeClause> {
        let name = self.parse_identifier()?;
        let mut if_not_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            if_not_exists = true;
        }

        self.expect_token(Token::LParen)?;
        let mut properties = Vec::new();
        if !self.consume_token(Token::RParen) {
            loop {
                properties.push(self.parse_property_definition()?);
                if self.consume_token(Token::RParen) {
                    break;
                }
                self.expect_token(Token::Comma)?;
            }
        }

        self.expect_keyword("FROM")?;
        let mut src_labels = Vec::new();
        loop {
            src_labels.push(self.parse_identifier()?);
            if !self.consume_token(Token::Comma) {
                break;
            }
        }

        self.expect_keyword("TO")?;
        let mut dst_labels = Vec::new();
        loop {
            dst_labels.push(self.parse_identifier()?);
            if !self.consume_token(Token::Comma) {
                break;
            }
        }

        Ok(CreateEdgeTypeClause {
            name,
            src_labels,
            dst_labels,
            properties,
            if_not_exists,
        })
    }

    fn parse_property_definition(&mut self) -> Result<PropertyDefinition> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;

        let mut nullable = true;
        let mut unique = false;
        let mut default = None;

        loop {
            if self.peek_keyword("NOT") {
                self.advance();
                self.expect_keyword("NULL")?;
                nullable = false;
            } else if self.peek_keyword("NULL") || self.peek_keyword("NULLABLE") {
                self.advance();
                nullable = true;
            } else if self.peek_keyword("UNIQUE") {
                self.advance();
                unique = true;
            } else if self.peek_keyword("DEFAULT") {
                self.advance();
                default = Some(self.parse_expr()?);
            } else {
                break;
            }
        }

        Ok(PropertyDefinition {
            name,
            data_type,
            nullable,
            unique,
            default,
        })
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        if self.peek_keyword("STRING") || self.peek_keyword("TEXT") {
            self.advance();
            return Ok(DataType::String);
        }
        if self.peek_keyword("INT") || self.peek_keyword("INTEGER") || self.peek_keyword("INT64") {
            self.advance();
            return Ok(DataType::Int64);
        }
        if self.peek_keyword("INT32") {
            self.advance();
            return Ok(DataType::Int32);
        }
        if self.peek_keyword("FLOAT") || self.peek_keyword("FLOAT64") || self.peek_keyword("DOUBLE")
        {
            self.advance();
            return Ok(DataType::Float64);
        }
        if self.peek_keyword("FLOAT32") {
            self.advance();
            return Ok(DataType::Float32);
        }
        if self.peek_keyword("BOOL") || self.peek_keyword("BOOLEAN") {
            self.advance();
            return Ok(DataType::Bool);
        }
        if self.peek_keyword("TIMESTAMP") {
            self.advance();
            return Ok(DataType::Timestamp);
        }
        if self.peek_keyword("DATE") {
            self.advance();
            return Ok(DataType::Date);
        }
        if self.peek_keyword("TIME") {
            self.advance();
            return Ok(DataType::Time);
        }
        if self.peek_keyword("DATETIME") {
            self.advance();
            return Ok(DataType::DateTime);
        }
        if self.peek_keyword("DURATION") {
            self.advance();
            return Ok(DataType::Duration);
        }
        if self.peek_keyword("JSON") {
            self.advance();
            return Ok(DataType::Json);
        }
        if self.peek_keyword("VECTOR") {
            self.advance();
            self.expect_token(Token::LParen)?;
            let dim_token = self.peek_token().ok_or(anyhow!("Expected dimensions"))?;
            let dimensions = match dim_token {
                Token::Number(n, _) => n.parse::<usize>()?,
                _ => return Err(anyhow!("Expected integer for vector dimensions")),
            };
            self.advance();
            self.expect_token(Token::RParen)?;
            return Ok(DataType::Vector { dimensions });
        }

        Err(anyhow!("Unknown data type: {:?}", self.peek_token()))
    }

    fn parse_create_constraint(&mut self) -> Result<CreateConstraintClause> {
        let name = self.parse_identifier()?;

        let mut if_not_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            if_not_exists = true;
        }

        self.expect_keyword("ON")?;
        let target = self.parse_constraint_target()?;

        self.expect_keyword("ASSERT")?;

        if self.peek_keyword("EXISTS") {
            self.advance();
            self.expect_token(Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(Token::RParen)?;

            let property = if let Expr::Property(_, p) = expr {
                p
            } else {
                return Err(anyhow!("Expected property in EXISTS constraint"));
            };

            return Ok(CreateConstraintClause {
                name,
                target,
                constraint_type: ConstraintType::Exists { property },
                if_not_exists,
            });
        }

        let expr = self.parse_expr()?;

        if self.peek_keyword("IS") {
            self.advance();
            if self.peek_keyword("UNIQUE") {
                self.advance();
                let property = if let Expr::Property(_, p) = expr {
                    p
                } else {
                    return Err(anyhow!("Expected property for UNIQUE constraint"));
                };
                return Ok(CreateConstraintClause {
                    name,
                    target,
                    constraint_type: ConstraintType::Unique {
                        properties: vec![property],
                    },
                    if_not_exists,
                });
            }
            return Err(anyhow!("Expected UNIQUE after IS"));
        }

        Ok(CreateConstraintClause {
            name,
            target,
            constraint_type: ConstraintType::Check { expression: expr },
            if_not_exists,
        })
    }

    fn parse_constraint_target(&mut self) -> Result<ConstraintTarget> {
        self.expect_token(Token::LParen)?;

        let _var = if let Some(Token::Word(w)) = self.peek_token() {
            self.advance();
            Some(w.value)
        } else {
            None
        };

        if self.consume_token(Token::Colon) {
            let label = self.parse_identifier()?;
            self.expect_token(Token::RParen)?;
            return Ok(ConstraintTarget::Label(label));
        }

        if self.consume_token(Token::RParen) {
            self.expect_token(Token::Minus)?;
            self.expect_token(Token::LBracket)?;
            let _var = self.parse_identifier()?;
            self.expect_token(Token::Colon)?;
            let rel_type = self.parse_identifier()?;
            self.expect_token(Token::RBracket)?;
            self.expect_token(Token::Minus)?;
            self.expect_token(Token::LParen)?;
            self.expect_token(Token::RParen)?;
            return Ok(ConstraintTarget::EdgeType(rel_type));
        }

        Err(anyhow!("Invalid constraint target syntax"))
    }

    fn parse_drop_constraint(&mut self) -> Result<DropConstraintClause> {
        let name = self.parse_identifier()?;
        let mut if_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("EXISTS")?;
            if_exists = true;
        }
        Ok(DropConstraintClause { name, if_exists })
    }

    fn parse_show_constraints(&mut self) -> Result<ShowConstraintsClause> {
        let mut target = None;
        if self.peek_keyword("FOR") {
            self.advance();
            target = Some(self.parse_constraint_target()?);
        }
        Ok(ShowConstraintsClause { target })
    }

    fn parse_drop_label(&mut self) -> Result<DropLabelClause> {
        let mut if_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("EXISTS")?;
            if_exists = true;
        }
        let name = self.parse_identifier()?;
        Ok(DropLabelClause { name, if_exists })
    }

    fn parse_drop_edge_type(&mut self) -> Result<DropEdgeTypeClause> {
        let mut if_exists = false;
        if self.peek_keyword("IF") {
            self.advance();
            self.expect_keyword("EXISTS")?;
            if_exists = true;
        }
        let name = self.parse_identifier()?;
        Ok(DropEdgeTypeClause { name, if_exists })
    }

    fn parse_alter_label(&mut self) -> Result<AlterLabelClause> {
        let name = self.parse_identifier()?;
        let action = self.parse_alter_action()?;
        Ok(AlterLabelClause { name, action })
    }

    fn parse_alter_edge_type(&mut self) -> Result<AlterEdgeTypeClause> {
        let name = self.parse_identifier()?;
        let action = self.parse_alter_action()?;
        Ok(AlterEdgeTypeClause { name, action })
    }

    fn parse_alter_action(&mut self) -> Result<AlterAction> {
        if self.peek_keyword("ADD") {
            self.advance();
            self.expect_keyword("PROPERTY")?;
            let prop = self.parse_property_definition()?;
            return Ok(AlterAction::AddProperty(prop));
        }
        if self.peek_keyword("DROP") {
            self.advance();
            self.expect_keyword("PROPERTY")?;
            let prop_name = self.parse_identifier()?;
            return Ok(AlterAction::DropProperty(prop_name));
        }
        if self.peek_keyword("RENAME") {
            self.advance();
            self.expect_keyword("PROPERTY")?;
            let old_name = self.parse_identifier()?;
            self.expect_keyword("TO")?;
            let new_name = self.parse_identifier()?;
            return Ok(AlterAction::RenameProperty { old_name, new_name });
        }
        Err(anyhow!(
            "Expected ADD PROPERTY, DROP PROPERTY, or RENAME PROPERTY"
        ))
    }

    fn expand_valid_at_macro(
        &self,
        node: Expr,
        time: Expr,
        start_prop: Option<String>,
        end_prop: Option<String>,
    ) -> Result<Expr> {
        let start_prop_str = start_prop.unwrap_or_else(|| "valid_from".to_string());
        let end_prop_str = end_prop.unwrap_or_else(|| "valid_to".to_string());

        // node.valid_from <= time
        let start_cond = Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(node.clone()),
                start_prop_str.clone(),
            )),
            op: Operator::LtEq,
            right: Box::new(time.clone()),
        };

        // node.valid_to IS NULL
        let end_is_null = Expr::IsNull(Box::new(Expr::Property(
            Box::new(node.clone()),
            end_prop_str.clone(),
        )));

        // node.valid_to > time
        let end_gt_time = Expr::BinaryOp {
            left: Box::new(Expr::Property(Box::new(node.clone()), end_prop_str.clone())),
            op: Operator::Gt,
            right: Box::new(time.clone()),
        };

        // (node.valid_to IS NULL OR node.valid_to > time)
        let end_cond = Expr::BinaryOp {
            left: Box::new(end_is_null),
            op: Operator::Or,
            right: Box::new(end_gt_time),
        };

        // start_cond AND end_cond
        Ok(Expr::BinaryOp {
            left: Box::new(start_cond),
            op: Operator::And,
            right: Box::new(end_cond),
        })
    }
}
