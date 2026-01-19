// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use std::path::Path;
use uni_common::core::schema::{
    DataType, DistanceMetric, FullTextIndexConfig, IndexDefinition, ScalarIndexConfig,
    ScalarIndexType, TokenizerConfig, VectorIndexConfig, VectorIndexType,
};
use uni_common::{Result, UniError};

/// Builder for defining and modifying the graph schema.
///
/// Use this builder to define labels, edge types, properties, and indexes.
/// Changes are batched and applied atomically when `.apply()` is called.
///
/// # Example
///
/// ```no_run
/// # async fn example(db: &uni::Uni) -> uni::Result<()> {
/// db.schema()
///     .label("Person")
///         .property("name", uni::DataType::String)
///         .property("age", uni::DataType::Int64)
///         .vector("embedding", 1536) // Adds property AND vector index
///         .index("name", uni::IndexType::Scalar(uni::ScalarType::BTree))
///     .edge_type("KNOWS", &["Person"], &["Person"])
///         .property("since", uni::DataType::Date)
///     .apply()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "schema builders do nothing until .apply() is called"]
pub struct SchemaBuilder<'a> {
    db: &'a Uni,
    pending: Vec<SchemaChange>,
}

pub enum SchemaChange {
    AddLabel {
        name: String,
        is_document: bool,
    },
    AddProperty {
        label_or_type: String,
        name: String,
        data_type: DataType,
        nullable: bool,
    },
    AddIndex(IndexDefinition),
    AddEdgeType {
        name: String,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
    },
}

impl<'a> SchemaBuilder<'a> {
    pub fn new(db: &'a Uni) -> Self {
        Self {
            db,
            pending: Vec::new(),
        }
    }

    pub fn label(self, name: &str) -> LabelBuilder<'a> {
        LabelBuilder::new(self, name.to_string())
    }

    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilder<'a> {
        EdgeTypeBuilder::new(
            self,
            name.to_string(),
            from.iter().map(|s| s.to_string()).collect(),
            to.iter().map(|s| s.to_string()).collect(),
        )
    }

    pub async fn apply(self) -> Result<()> {
        let manager = &self.db.schema;

        for change in self.pending {
            match change {
                SchemaChange::AddLabel { name, is_document } => {
                    manager
                        .add_label(&name, is_document)
                        .map_err(|e| UniError::Schema {
                            message: e.to_string(),
                        })?;
                }
                SchemaChange::AddProperty {
                    label_or_type,
                    name,
                    data_type,
                    nullable,
                } => {
                    manager
                        .add_property(&label_or_type, &name, data_type, nullable)
                        .map_err(|e| UniError::Schema {
                            message: e.to_string(),
                        })?;
                }
                SchemaChange::AddIndex(idx) => {
                    manager.add_index(idx).map_err(|e| UniError::Schema {
                        message: e.to_string(),
                    })?;
                }
                SchemaChange::AddEdgeType {
                    name,
                    from_labels,
                    to_labels,
                } => {
                    manager
                        .add_edge_type(&name, from_labels, to_labels)
                        .map_err(|e| UniError::Schema {
                            message: e.to_string(),
                        })?;
                }
            }
        }

        manager.save().await.map_err(UniError::Internal)?;
        Ok(())
    }
}

#[must_use = "builders do nothing until .done() or .apply() is called"]
pub struct LabelBuilder<'a> {
    builder: SchemaBuilder<'a>,
    name: String,
    is_document: bool,
}

impl<'a> LabelBuilder<'a> {
    fn new(builder: SchemaBuilder<'a>, name: String) -> Self {
        Self {
            builder,
            name,
            is_document: false,
        }
    }

    pub fn document(mut self) -> Self {
        self.is_document = true;
        self
    }

    pub fn property(mut self, name: &str, data_type: DataType) -> Self {
        self.builder.pending.push(SchemaChange::AddProperty {
            label_or_type: self.name.clone(),
            name: name.to_string(),
            data_type,
            nullable: false,
        });
        self
    }

    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self {
        self.builder.pending.push(SchemaChange::AddProperty {
            label_or_type: self.name.clone(),
            name: name.to_string(),
            data_type,
            nullable: true,
        });
        self
    }

    pub fn vector(self, name: &str, dimensions: usize) -> Self {
        self.property(name, DataType::Vector { dimensions })
    }

    pub fn index(mut self, property: &str, index_type: IndexType) -> Self {
        let idx = match index_type {
            IndexType::Vector(cfg) => IndexDefinition::Vector(VectorIndexConfig {
                name: format!("idx_{}_{}", self.name, property),
                label: self.name.clone(),
                property: property.to_string(),
                index_type: cfg.algorithm.into_internal(),
                metric: cfg.metric.into_internal(),
                embedding_config: None, // Explicitly set via SchemaBuilder or future API
            }),
            IndexType::FullText => IndexDefinition::FullText(FullTextIndexConfig {
                name: format!("fts_{}_{}", self.name, property),
                label: self.name.clone(),
                properties: vec![property.to_string()],
                tokenizer: TokenizerConfig::Standard,
                with_positions: true,
            }),
            IndexType::Scalar(stype) => IndexDefinition::Scalar(ScalarIndexConfig {
                name: format!("idx_{}_{}", self.name, property),
                label: self.name.clone(),
                properties: vec![property.to_string()],
                index_type: stype.into_internal(),
                where_clause: None,
            }),
            IndexType::Inverted(config) => IndexDefinition::Inverted(config),
        };
        self.builder.pending.push(SchemaChange::AddIndex(idx));
        self
    }

    pub fn done(mut self) -> SchemaBuilder<'a> {
        self.builder.pending.insert(
            0,
            SchemaChange::AddLabel {
                name: self.name,
                is_document: self.is_document,
            },
        );
        self.builder
    }

    // Chaining
    pub fn label(self, name: &str) -> LabelBuilder<'a> {
        self.done().label(name)
    }

    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilder<'a> {
        self.done().edge_type(name, from, to)
    }

    pub async fn apply(self) -> Result<()> {
        self.done().apply().await
    }
}

#[must_use = "builders do nothing until .done() or .apply() is called"]
pub struct EdgeTypeBuilder<'a> {
    builder: SchemaBuilder<'a>,
    name: String,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
}

impl<'a> EdgeTypeBuilder<'a> {
    fn new(
        builder: SchemaBuilder<'a>,
        name: String,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
    ) -> Self {
        Self {
            builder,
            name,
            from_labels,
            to_labels,
        }
    }

    pub fn property(mut self, name: &str, data_type: DataType) -> Self {
        self.builder.pending.push(SchemaChange::AddProperty {
            label_or_type: self.name.clone(),
            name: name.to_string(),
            data_type,
            nullable: false,
        });
        self
    }

    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self {
        self.builder.pending.push(SchemaChange::AddProperty {
            label_or_type: self.name.clone(),
            name: name.to_string(),
            data_type,
            nullable: true,
        });
        self
    }

    pub fn done(mut self) -> SchemaBuilder<'a> {
        self.builder.pending.insert(
            0,
            SchemaChange::AddEdgeType {
                name: self.name,
                from_labels: self.from_labels,
                to_labels: self.to_labels,
            },
        );
        self.builder
    }

    pub fn label(self, name: &str) -> LabelBuilder<'a> {
        self.done().label(name)
    }

    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilder<'a> {
        self.done().edge_type(name, from, to)
    }

    pub async fn apply(self) -> Result<()> {
        self.done().apply().await
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LabelInfo {
    pub name: String,
    pub is_document: bool,
    pub count: usize,
    pub properties: Vec<PropertyInfo>,
    pub indexes: Vec<IndexInfo>,
    pub constraints: Vec<ConstraintInfo>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PropertyInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_indexed: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub index_type: String,
    pub properties: Vec<String>,
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConstraintInfo {
    pub name: String,
    pub constraint_type: String,
    pub properties: Vec<String>,
    pub enabled: bool,
}

#[non_exhaustive]
pub enum IndexType {
    Vector(VectorIndexCfg),
    FullText,
    Scalar(ScalarType),
    Inverted(uni_common::core::schema::InvertedIndexConfig),
}

pub struct VectorIndexCfg {
    pub algorithm: VectorAlgo,
    pub metric: VectorMetric,
}

#[non_exhaustive]
pub enum VectorAlgo {
    Hnsw { m: u32, ef_construction: u32 },
    IvfPq { partitions: u32, sub_vectors: u32 },
    Flat,
}

impl VectorAlgo {
    fn into_internal(self) -> VectorIndexType {
        match self {
            VectorAlgo::Hnsw { m, ef_construction } => VectorIndexType::Hnsw {
                m,
                ef_construction,
                ef_search: 50,
            },
            VectorAlgo::IvfPq {
                partitions,
                sub_vectors,
            } => VectorIndexType::IvfPq {
                num_partitions: partitions,
                num_sub_vectors: sub_vectors,
                bits_per_subvector: 8,
            },
            VectorAlgo::Flat => VectorIndexType::Flat,
        }
    }
}

#[non_exhaustive]
pub enum VectorMetric {
    Cosine,
    L2,
    Dot,
}

impl VectorMetric {
    fn into_internal(self) -> DistanceMetric {
        match self {
            VectorMetric::Cosine => DistanceMetric::Cosine,
            VectorMetric::L2 => DistanceMetric::L2,
            VectorMetric::Dot => DistanceMetric::Dot,
        }
    }
}

#[non_exhaustive]
pub enum ScalarType {
    BTree,
    Hash,
    Bitmap,
}

impl ScalarType {
    fn into_internal(self) -> ScalarIndexType {
        match self {
            ScalarType::BTree => ScalarIndexType::BTree,
            ScalarType::Hash => ScalarIndexType::Hash,
            ScalarType::Bitmap => ScalarIndexType::Bitmap,
        }
    }
}

impl Uni {
    pub fn schema(&self) -> SchemaBuilder<'_> {
        SchemaBuilder::new(self)
    }

    pub async fn load_schema(&self, path: impl AsRef<Path>) -> Result<()> {
        // We can't easily "replace" the SchemaManager's schema in-place if it's already Arc-ed around.
        // But SchemaManager has internal RwLock<Schema>.
        // Let's check if we can add a method to SchemaManager to reload.
        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(UniError::Io)?;
        let schema: uni_common::core::schema::Schema =
            serde_json::from_str(&content).map_err(|e| UniError::Schema {
                message: e.to_string(),
            })?;

        // We need a way to update the schema in SchemaManager.
        // I'll add a `replace_schema` or similar to SchemaManager.
        self.schema.replace_schema(schema);
        Ok(())
    }

    pub async fn save_schema(&self, path: impl AsRef<Path>) -> Result<()> {
        let content =
            serde_json::to_string_pretty(&self.schema.schema()).map_err(|e| UniError::Schema {
                message: e.to_string(),
            })?;
        tokio::fs::write(path, content)
            .await
            .map_err(UniError::Io)?;
        Ok(())
    }
}
