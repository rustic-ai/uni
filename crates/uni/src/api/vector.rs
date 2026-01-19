// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use uni_common::Result;
use uni_common::core::id::Vid;
use uni_query::Node;

pub struct VectorMatch {
    pub vid: Vid,
    pub distance: f32,
}

#[must_use = "search builders do nothing until .search() is called"]
pub struct VectorSearchBuilder<'a> {
    db: &'a Uni,
    label: String,
    property: String,
    query: Vec<f32>,
    k: usize,
    threshold: Option<f32>,
    filter: Option<String>,
}

impl<'a> VectorSearchBuilder<'a> {
    pub fn new(db: &'a Uni, label: &str, property: &str, query: &[f32]) -> Self {
        Self {
            db,
            label: label.to_string(),
            property: property.to_string(),
            query: query.to_vec(),
            k: 10,
            threshold: None,
            filter: None,
        }
    }

    pub fn k(mut self, k: usize) -> Self {
        self.k = k;
        self
    }

    pub fn threshold(mut self, threshold: f32) -> Self {
        self.threshold = Some(threshold);
        self
    }

    pub fn filter(mut self, filter: &str) -> Self {
        self.filter = Some(filter.to_string());
        self
    }

    /// Execute the vector search
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni::Uni;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), uni::UniError> {
    /// # let db = Uni::open("db").build().await?;
    /// let query_vector = vec![0.1, 0.2, 0.3];
    /// let matches = db.vector_search_with("Document", "embedding", &query_vector)
    ///     .k(5)
    ///     .threshold(0.8)
    ///     .search()
    ///     .await?;
    ///
    /// for m in matches {
    ///     println!("Found node {} with score {}", m.vid, m.distance);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search(self) -> Result<Vec<VectorMatch>> {
        let results = self
            .db
            .storage
            .vector_search(&self.label, &self.property, &self.query, self.k)
            .await
            .map_err(|e| uni_common::UniError::Query {
                message: e.to_string(),
                query: None,
            })?;
        let mut matches = Vec::new();
        for (vid, dist) in results {
            if let Some(t) = self.threshold
                && dist > t
            {
                continue;
            }
            matches.push(VectorMatch {
                vid,
                distance: dist,
            });
        }

        Ok(matches)
    }

    pub async fn fetch_nodes(self) -> Result<Vec<(Node, f32)>> {
        let label = self.label.clone();
        let db = self.db;
        let matches = self.search().await?;
        let mut results = Vec::with_capacity(matches.len());

        for m in matches {
            let props = db
                .properties
                .get_all_vertex_props(m.vid)
                .await
                .map_err(|e| uni_common::UniError::Query {
                    message: e.to_string(),
                    query: None,
                })?;
            results.push((
                Node {
                    vid: m.vid,
                    label: label.clone(),
                    properties: props.into_iter().map(|(k, v)| (k, v.into())).collect(),
                },
                m.distance,
            ));
        }

        Ok(results)
    }
}

impl Uni {
    pub async fn vector_search(
        &self,
        label: &str,
        property: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<VectorMatch>> {
        VectorSearchBuilder::new(self, label, property, query)
            .k(k)
            .search()
            .await
    }

    pub fn vector_search_with(
        &self,
        label: &str,
        property: &str,
        query: &[f32],
    ) -> VectorSearchBuilder<'_> {
        VectorSearchBuilder::new(self, label, property, query)
    }
}
