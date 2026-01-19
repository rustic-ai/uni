// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use crate::api::query_builder::QueryBuilder;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::{Result, UniConfig, UniError};
use uni_query::{
    ExecuteResult, ExplainOutput, LogicalPlan, ProfileOutput, QueryCursor, QueryResult, Row,
    Value as ApiValue,
};

/// Extract projection column names from a LogicalPlan, preserving query order.
/// Returns None if the plan doesn't have projections at the top level.
fn extract_projection_order(plan: &LogicalPlan) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::Project { projections, .. } => Some(
            projections
                .iter()
                .map(|(expr, alias)| alias.clone().unwrap_or_else(|| expr.to_string_repr()))
                .collect(),
        ),
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            // For aggregate plans, combine group_by and aggregates in order
            let mut names: Vec<String> = group_by.iter().map(|e| e.to_string_repr()).collect();
            names.extend(aggregates.iter().map(|e| e.to_string_repr()));
            Some(names)
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Filter { input, .. } => extract_projection_order(input),
        _ => None,
    }
}

impl Uni {
    /// Explain a Cypher query plan without executing it.
    pub async fn explain(&self, cypher: &str) -> Result<ExplainOutput> {
        let mut parser = uni_query::CypherParser::new(cypher).map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;
        let ast = parser.parse().map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone().into());
        planner.explain_plan(ast).map_err(|e| UniError::Query {
            message: e.to_string(),
            query: Some(cypher.to_string()),
        })
    }

    /// Profile a Cypher query execution.
    pub async fn profile(&self, cypher: &str) -> Result<(QueryResult, ProfileOutput)> {
        let mut parser = uni_query::CypherParser::new(cypher).map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;
        let ast = parser.parse().map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone().into());
        let logical_plan = planner.plan(ast).map_err(|e| UniError::Query {
            message: e.to_string(),
            query: Some(cypher.to_string()),
        })?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(self.config.clone());
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let json_params: HashMap<String, serde_json::Value> = HashMap::new(); // TODO: Support params in profile

        // Extract projection order
        let projection_order = extract_projection_order(&logical_plan);

        let (results, profile_output) = executor
            .profile(logical_plan, &json_params)
            .await
            .map_err(|e| UniError::Query {
                message: e.to_string(),
                query: Some(cypher.to_string()),
            })?;

        // Convert results to QueryResult
        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let json_val = map.get(col).cloned().unwrap_or(serde_json::Value::Null);
                    values.push(ApiValue::from(json_val));
                }
                Row {
                    columns: columns.clone(),
                    values,
                }
            })
            .collect();

        Ok((
            QueryResult {
                columns,
                rows,
                warnings: Vec::new(),
            },
            profile_output,
        ))
    }

    /// Execute a Cypher query
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.execute_internal(cypher, HashMap::new()).await
    }

    /// Execute query returning a cursor for streaming results
    pub async fn query_cursor(&self, cypher: &str) -> Result<QueryCursor> {
        self.execute_cursor_internal(cypher, HashMap::new()).await
    }

    /// Execute a query with parameters using a builder
    pub fn query_with(&self, cypher: &str) -> QueryBuilder<'_> {
        QueryBuilder::new(self, cypher)
    }

    /// Execute a modification query (CREATE, SET, DELETE, etc.)
    /// Returns the number of affected rows/elements
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        let result = self.execute_internal(cypher, HashMap::new()).await?;
        Ok(ExecuteResult {
            affected_rows: result.len(),
        })
    }

    pub(crate) async fn execute_cursor_internal(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<QueryCursor> {
        self.execute_cursor_internal_with_config(cypher, params, self.config.clone())
            .await
    }

    pub(crate) async fn execute_cursor_internal_with_config(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
    ) -> Result<QueryCursor> {
        let mut parser = uni_query::CypherParser::new(cypher).map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;
        let ast = parser.parse().map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone().into());
        let logical_plan = planner.plan(ast).map_err(|e| UniError::Query {
            message: e.to_string(),
            query: Some(cypher.to_string()),
        })?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let json_params: HashMap<String, serde_json::Value> =
            params.into_iter().map(|(k, v)| (k, v.into())).collect();

        // Extract projection order from the plan
        let projection_order = extract_projection_order(&logical_plan);
        let projection_order_for_rows = projection_order.clone();
        let cypher_for_error = cypher.to_string();

        let stream = executor.execute_stream(logical_plan, self.properties.clone(), json_params);

        let row_stream = stream.map(move |batch_res| {
            let results = batch_res.map_err(|e| UniError::Query {
                message: e.to_string(),
                query: Some(cypher_for_error.clone()),
            })?;

            if results.is_empty() {
                return Ok(vec![]);
            }

            // Determine columns for this batch (should be stable for the whole query)
            let columns = if let Some(order) = &projection_order_for_rows {
                Arc::new(order.clone())
            } else {
                let mut cols: Vec<String> = results[0].keys().cloned().collect();
                cols.sort();
                Arc::new(cols)
            };

            let rows = results
                .into_iter()
                .map(|map| {
                    let mut values = Vec::with_capacity(columns.len());
                    for col in columns.iter() {
                        let json_val = map.get(col).cloned().unwrap_or(serde_json::Value::Null);
                        values.push(ApiValue::from(json_val));
                    }
                    Row {
                        columns: columns.clone(),
                        values,
                    }
                })
                .collect();

            Ok(rows)
        });

        // We need columns ahead of time for QueryCursor if possible.
        let columns = if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            Arc::new(vec![])
        };

        Ok(QueryCursor {
            columns,
            stream: Box::pin(row_stream),
        })
    }

    pub(crate) async fn execute_internal(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<QueryResult> {
        self.execute_internal_with_config(cypher, params, self.config.clone())
            .await
    }

    pub(crate) async fn execute_internal_with_config(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
    ) -> Result<QueryResult> {
        let mut parser = uni_query::CypherParser::new(cypher).map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;
        let ast = parser.parse().map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone().into());
        let logical_plan = planner.plan(ast).map_err(|e| UniError::Query {
            message: e.to_string(),
            query: Some(cypher.to_string()),
        })?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let json_params: HashMap<String, serde_json::Value> =
            params.into_iter().map(|(k, v)| (k, v.into())).collect();

        // Extract projection order from the plan before execution
        let projection_order = extract_projection_order(&logical_plan);

        let results = executor
            .execute(logical_plan, &self.properties, &json_params)
            .await
            .map_err(|e| UniError::Query {
                message: e.to_string(),
                query: Some(cypher.to_string()),
            })?;

        // Convert Vec<HashMap<String, serde_json::Value>> to QueryResult (uni_query::Value)
        // Use projection order from the plan if available, otherwise fall back to sorted order
        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            // Use the original query projection order
            Arc::new(order)
        } else {
            // Fallback: sorted order for determinism
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let json_val = map.get(col).cloned().unwrap_or(serde_json::Value::Null);
                    values.push(ApiValue::from(json_val));
                }
                Row {
                    columns: columns.clone(),
                    values,
                }
            })
            .collect();

        Ok(QueryResult {
            columns,
            rows,
            warnings: Vec::new(),
        })
    }
}
