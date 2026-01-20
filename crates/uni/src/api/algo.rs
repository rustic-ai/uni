// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use uni_common::Result;
use uni_common::core::id::Vid;

/// Builder for graph algorithms.
///
/// Provides a fluent API to configure and execute graph algorithms like PageRank,
/// Weakly Connected Components (WCC), etc.
///
/// # Example
///
/// ```no_run
/// # async fn example(db: &uni_db::Uni) -> uni_db::Result<()> {
/// // Run PageRank
/// let results = db.algo()
///     .pagerank()
///     .damping(0.85)
///     .max_iterations(50)
///     .run()
///     .await?;
///
/// for (vid, score) in results {
///     println!("Node: {}, Score: {}", vid, score);
/// }
/// # Ok(())
/// # }
/// ```
#[must_use = "builders do nothing until a specific algorithm is selected"]
pub struct AlgoBuilder<'a> {
    pub(crate) db: &'a Uni,
}

impl<'a> AlgoBuilder<'a> {
    pub fn new(db: &'a Uni) -> Self {
        Self { db }
    }

    pub fn pagerank(&self) -> PageRankBuilder<'a> {
        PageRankBuilder::new(self.db)
    }

    pub fn wcc(&self) -> WccBuilder<'a> {
        WccBuilder::new(self.db)
    }
}

#[must_use = "builders do nothing until .run() is called"]
pub struct PageRankBuilder<'a> {
    db: &'a Uni,
    labels: Option<Vec<String>>,
    edge_types: Option<Vec<String>>,
    damping: f64,
    max_iterations: usize,
    tolerance: f64,
}

impl<'a> PageRankBuilder<'a> {
    fn new(db: &'a Uni) -> Self {
        Self {
            db,
            labels: None,
            edge_types: None,
            damping: 0.85,
            max_iterations: 20,
            tolerance: 1e-6,
        }
    }

    pub fn labels(mut self, labels: &[&str]) -> Self {
        self.labels = Some(labels.iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn edge_types(mut self, types: &[&str]) -> Self {
        self.edge_types = Some(types.iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn damping(mut self, d: f64) -> Self {
        self.damping = d;
        self
    }

    pub fn max_iterations(mut self, n: usize) -> Self {
        self.max_iterations = n;
        self
    }

    pub fn tolerance(mut self, t: f64) -> Self {
        self.tolerance = t;
        self
    }

    pub fn run(self) -> futures::future::BoxFuture<'a, Result<Vec<(Vid, f64)>>> {
        let db = self.db;
        Box::pin(async move {
            let labels = self.labels.unwrap_or_default();
            let edge_types = self.edge_types.unwrap_or_default();

            // Positional arguments for algo.pageRank(labels, edge_types, damping, max_iter, tolerance)
            let query = format!(
                "CALL algo.pageRank({:?}, {:?}, {}, {}, {}) YIELD nodeId, score RETURN nodeId, score",
                labels, edge_types, self.damping, self.max_iterations, self.tolerance
            );

            let result = db.query(&query).await?;
            let mut output = Vec::with_capacity(result.len());
            for row in result {
                let vid = row.get::<Vid>("nodeId")?;
                let score = row.get::<f64>("score")?;
                output.push((vid, score));
            }
            Ok(output)
        })
    }
}

#[must_use = "builders do nothing until .run() is called"]
pub struct WccBuilder<'a> {
    db: &'a Uni,
    labels: Option<Vec<String>>,
    edge_types: Option<Vec<String>>,
}

impl<'a> WccBuilder<'a> {
    fn new(db: &'a Uni) -> Self {
        Self {
            db,
            labels: None,
            edge_types: None,
        }
    }

    pub fn run(self) -> futures::future::BoxFuture<'a, Result<Vec<(Vid, i64)>>> {
        let db = self.db;
        Box::pin(async move {
            let labels = self.labels.unwrap_or_default();
            let edge_types = self.edge_types.unwrap_or_default();

            let query = format!(
                "CALL algo.wcc({:?}, {:?}) YIELD nodeId, componentId RETURN nodeId, componentId",
                labels, edge_types
            );

            let result = db.query(&query).await?;
            let mut output = Vec::with_capacity(result.len());
            for row in result {
                let vid = row.get::<Vid>("nodeId")?;
                let comp = row.get::<i64>("componentId")?;
                output.push((vid, comp));
            }
            Ok(output)
        })
    }
}

impl Uni {
    pub fn algo(&self) -> AlgoBuilder<'_> {
        AlgoBuilder::new(self)
    }
}
