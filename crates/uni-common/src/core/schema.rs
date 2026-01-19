// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::sync::{acquire_read, acquire_write};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum SchemaElementState {
    Active,
    Deprecated {
        since: DateTime<Utc>,
        reason: String,
        migration_hint: Option<String>,
    },
    Hidden {
        since: DateTime<Utc>,
        last_active_snapshot: String, // SnapshotId
    },
    Tombstone {
        since: DateTime<Utc>,
    },
}

use arrow_schema::{DataType as ArrowDataType, Field, TimeUnit};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum CrdtType {
    GCounter,
    GSet,
    ORSet,
    LWWRegister,
    LWWMap,
    Rga,
    VectorClock,
    VCRegister,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum DataType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Timestamp,
    Date,
    Time,
    DateTime,
    Duration,
    Json,
    Vector { dimensions: usize },
    Crdt(CrdtType),
    List(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
}

use arrow_schema::Fields;

impl DataType {
    // Alias for compatibility/convenience if needed, but preferable to use exact types.
    #[allow(non_upper_case_globals)]
    pub const Float: DataType = DataType::Float64;
    #[allow(non_upper_case_globals)]
    pub const Int: DataType = DataType::Int64;

    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            DataType::String => ArrowDataType::Utf8,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Timestamp => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            DataType::Date => ArrowDataType::Date32,
            DataType::Time => ArrowDataType::Time64(TimeUnit::Microsecond),
            DataType::DateTime => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            DataType::Duration => ArrowDataType::Duration(TimeUnit::Microsecond),
            DataType::Json => ArrowDataType::Utf8, // Store JSON as string for now
            DataType::Vector { dimensions } => ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, true)),
                *dimensions as i32,
            ),
            DataType::Crdt(_) => ArrowDataType::Binary, // Store CRDT as binary MessagePack
            DataType::List(inner) => {
                ArrowDataType::List(Arc::new(Field::new("item", inner.to_arrow(), true)))
            }
            DataType::Map(key, value) => ArrowDataType::List(Arc::new(Field::new(
                "item",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("key", key.to_arrow(), false),
                    Field::new("value", value.to_arrow(), true),
                ])),
                true,
            ))),
        }
    }
}

fn default_created_at() -> DateTime<Utc> {
    Utc::now()
}

fn default_state() -> SchemaElementState {
    SchemaElementState::Active
}

fn default_version_1() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropertyMeta {
    pub r#type: DataType,
    pub nullable: bool,
    #[serde(default = "default_version_1")]
    pub added_in: u32, // SchemaVersion
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
    #[serde(default)]
    pub generation_expression: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonIndex {
    pub path: String,
    pub r#type: DataType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelMeta {
    pub id: u16, // LabelId
    #[serde(default = "default_created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
    #[serde(default)]
    pub is_document: bool,
    #[serde(default)]
    pub json_indexes: Vec<JsonIndex>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeTypeMeta {
    pub id: u16, // TypeId
    pub src_labels: Vec<String>,
    pub dst_labels: Vec<String>,
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ConstraintType {
    Unique { properties: Vec<String> },
    Exists { property: String },
    Check { expression: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ConstraintTarget {
    Label(String),
    EdgeType(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Constraint {
    pub name: String,
    pub constraint_type: ConstraintType,
    pub target: ConstraintTarget,
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub schema_version: u32,
    pub labels: HashMap<String, LabelMeta>,
    pub edge_types: HashMap<String, EdgeTypeMeta>,
    pub properties: HashMap<String, HashMap<String, PropertyMeta>>,
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
    #[serde(default)]
    pub constraints: Vec<Constraint>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            schema_version: 1,
            labels: HashMap::new(),
            edge_types: HashMap::new(),
            properties: HashMap::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
        }
    }
}

impl Schema {
    /// Returns the label name for a given label ID.
    ///
    /// Performs a linear scan over all labels. This is efficient because
    /// the number of labels in a schema is typically small.
    pub fn label_name_by_id(&self, label_id: u16) -> Option<&str> {
        self.labels
            .iter()
            .find(|(_, meta)| meta.id == label_id)
            .map(|(name, _)| name.as_str())
    }

    /// Returns the edge type name for a given type ID.
    ///
    /// Performs a linear scan over all edge types. This is efficient because
    /// the number of edge types in a schema is typically small.
    pub fn edge_type_name_by_id(&self, type_id: u16) -> Option<&str> {
        self.edge_types
            .iter()
            .find(|(_, meta)| meta.id == type_id)
            .map(|(name, _)| name.as_str())
    }

    /// Returns the vector index configuration for a given label and property.
    ///
    /// Performs a linear scan over all indexes. This is efficient because
    /// the number of indexes in a schema is typically small.
    pub fn vector_index_for_property(
        &self,
        label: &str,
        property: &str,
    ) -> Option<&VectorIndexConfig> {
        self.indexes.iter().find_map(|idx| {
            if let IndexDefinition::Vector(config) = idx
                && config.label == label
                && config.property == property
            {
                return Some(config);
            }
            None
        })
    }
}

/// Configuration for JSON Full-Text Search (FTS) indexes.
///
/// Creates a Lance inverted index on JSON document columns, enabling
/// BM25-based full-text search with optional path-specific queries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonFtsIndexConfig {
    /// Unique name for this index.
    pub name: String,
    /// Label (node type) this index applies to.
    pub label: String,
    /// JSON column to index (e.g., "_doc").
    pub column: String,
    /// Optional specific JSON paths to index. Empty means all paths.
    #[serde(default)]
    pub paths: Vec<String>,
    /// Enable position information for phrase search support.
    #[serde(default)]
    pub with_positions: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum IndexDefinition {
    Vector(VectorIndexConfig),
    FullText(FullTextIndexConfig),
    Scalar(ScalarIndexConfig),
    /// JSON Full-Text Search index using Lance inverted index.
    JsonFullText(JsonFtsIndexConfig),
    Inverted(InvertedIndexConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InvertedIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    #[serde(default = "default_normalize")]
    pub normalize: bool,
    #[serde(default = "default_max_terms_per_doc")]
    pub max_terms_per_doc: usize,
}

fn default_normalize() -> bool {
    true
}

fn default_max_terms_per_doc() -> usize {
    10_000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    pub index_type: VectorIndexType,
    pub metric: DistanceMetric,
    pub embedding_config: Option<EmbeddingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingConfig {
    pub model: EmbeddingModel,
    pub source_properties: Vec<String>,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "provider")]
#[non_exhaustive]
pub enum EmbeddingModel {
    FastEmbed {
        model_name: String,
        cache_dir: Option<String>,
        max_length: Option<usize>,
    },
    OpenAI {
        model: String,
        api_key_env: String,
        dimensions: Option<u32>,
    },
    Ollama {
        model: String,
        host: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum VectorIndexType {
    IvfPq {
        num_partitions: u32,
        num_sub_vectors: u32,
        bits_per_subvector: u8,
    },
    Hnsw {
        m: u32,
        ef_construction: u32,
        ef_search: u32,
    },
    Flat,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum DistanceMetric {
    Cosine,
    L2,
    Dot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FullTextIndexConfig {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub tokenizer: TokenizerConfig,
    pub with_positions: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum TokenizerConfig {
    Standard,
    Whitespace,
    Ngram { min: u8, max: u8 },
    Custom { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScalarIndexConfig {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub index_type: ScalarIndexType,
    pub where_clause: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ScalarIndexType {
    BTree,
    Hash,
    Bitmap,
}

pub struct SchemaManager {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
    schema: RwLock<Schema>,
}

impl SchemaManager {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("Invalid schema path"))?;
        let filename = path
            .file_name()
            .ok_or_else(|| anyhow!("Invalid schema filename"))?
            .to_str()
            .ok_or_else(|| anyhow!("Invalid utf8 filename"))?;

        let store = Arc::new(LocalFileSystem::new_with_prefix(parent)?);
        let obj_path = ObjectStorePath::from(filename);

        Self::load_from_store(store, &obj_path).await
    }

    pub async fn load_from_store(
        store: Arc<dyn ObjectStore>,
        path: &ObjectStorePath,
    ) -> Result<Self> {
        match store.get(path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8(bytes.to_vec())?;
                let schema: Schema = serde_json::from_str(&content)?;
                Ok(Self {
                    store,
                    path: path.clone(),
                    schema: RwLock::new(schema),
                })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self {
                store,
                path: path.clone(),
                schema: RwLock::new(Schema::default()),
            }),
            Err(e) => Err(anyhow::Error::from(e)),
        }
    }

    pub async fn save(&self) -> Result<()> {
        let content = {
            let schema_guard = acquire_read(&self.schema, "schema")?;
            serde_json::to_string_pretty(&*schema_guard)?
        };
        self.store
            .put(&self.path, content.into())
            .await
            .map_err(anyhow::Error::from)?;
        Ok(())
    }

    pub fn path(&self) -> &ObjectStorePath {
        &self.path
    }

    pub fn schema(&self) -> Schema {
        self.schema
            .read()
            .expect("Schema lock poisoned - a thread panicked while holding it")
            .clone()
    }

    /// Generate a consistent internal column name for an expression index.
    /// Uses a hash suffix to ensure uniqueness for different expressions that
    /// might sanitize to the same string (e.g., "a+b" and "a-b" both become "a_b").
    ///
    /// IMPORTANT: Uses FNV-1a hash which is stable across Rust versions and platforms.
    /// DefaultHasher is not guaranteed to be stable and could break persistent data
    /// if the hash changes after a compiler upgrade.
    pub fn generated_column_name(expr: &str) -> String {
        let sanitized = expr
            .replace(|c: char| !c.is_alphanumeric(), "_")
            .trim_matches('_')
            .to_string();

        // FNV-1a 64-bit hash - stable across Rust versions and platforms
        const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;

        let mut hash = FNV_OFFSET_BASIS;
        for byte in expr.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }

        format!("_gen_{}_{:x}", sanitized, hash)
    }

    pub fn replace_schema(&self, new_schema: Schema) {
        let mut schema = self
            .schema
            .write()
            .expect("Schema lock poisoned - a thread panicked while holding it");
        *schema = new_schema;
    }

    pub fn next_label_id(&self) -> u16 {
        self.schema()
            .labels
            .values()
            .map(|l| l.id)
            .max()
            .unwrap_or(0)
            + 1
    }

    pub fn next_type_id(&self) -> u16 {
        self.schema()
            .edge_types
            .values()
            .map(|t| t.id)
            .max()
            .unwrap_or(0)
            + 1
    }

    pub fn add_label(&self, name: &str, is_document: bool) -> Result<u16> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if schema.labels.contains_key(name) {
            return Err(anyhow!("Label '{}' already exists", name));
        }

        let id = schema.labels.values().map(|l| l.id).max().unwrap_or(0) + 1;
        schema.labels.insert(
            name.to_string(),
            LabelMeta {
                id,
                created_at: Utc::now(),
                state: SchemaElementState::Active,
                is_document,
                json_indexes: Vec::new(),
            },
        );
        Ok(id)
    }

    pub fn add_json_index(&self, label: &str, path: &str, data_type: DataType) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        let label_meta = schema
            .labels
            .get_mut(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        if !label_meta.is_document {
            return Err(anyhow!("Label '{}' is not a document collection", label));
        }

        if label_meta.json_indexes.iter().any(|idx| idx.path == path) {
            return Err(anyhow!("Index for path '{}' already exists", path));
        }

        label_meta.json_indexes.push(JsonIndex {
            path: path.to_string(),
            r#type: data_type,
        });

        Ok(())
    }

    pub fn add_edge_type(
        &self,
        name: &str,
        src_labels: Vec<String>,
        dst_labels: Vec<String>,
    ) -> Result<u16> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if schema.edge_types.contains_key(name) {
            return Err(anyhow!("Edge type '{}' already exists", name));
        }

        let id = schema.edge_types.values().map(|t| t.id).max().unwrap_or(0) + 1;

        schema.edge_types.insert(
            name.to_string(),
            EdgeTypeMeta {
                id,
                src_labels,
                dst_labels,
                state: SchemaElementState::Active,
            },
        );
        Ok(id)
    }

    pub fn add_property(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        let version = schema.schema_version;
        let props = schema
            .properties
            .entry(label_or_type.to_string())
            .or_default();

        if props.contains_key(prop_name) {
            return Err(anyhow!(
                "Property '{}' already exists for '{}'",
                prop_name,
                label_or_type
            ));
        }

        props.insert(
            prop_name.to_string(),
            PropertyMeta {
                r#type: data_type,
                nullable,
                added_in: version,
                state: SchemaElementState::Active,
                generation_expression: None,
            },
        );
        Ok(())
    }

    pub fn add_generated_property(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        expr: String,
    ) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        let version = schema.schema_version;
        let props = schema
            .properties
            .entry(label_or_type.to_string())
            .or_default();

        if props.contains_key(prop_name) {
            return Err(anyhow!("Property '{}' already exists", prop_name));
        }

        props.insert(
            prop_name.to_string(),
            PropertyMeta {
                r#type: data_type,
                nullable: true,
                added_in: version,
                state: SchemaElementState::Active,
                generation_expression: Some(expr),
            },
        );
        Ok(())
    }

    pub fn add_index(&self, index_def: IndexDefinition) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        schema.indexes.push(index_def);
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<IndexDefinition> {
        let schema = self.schema.read().expect("Schema lock poisoned");
        schema
            .indexes
            .iter()
            .find(|i| match i {
                IndexDefinition::Vector(c) => c.name == name,
                IndexDefinition::Scalar(c) => c.name == name,
                IndexDefinition::FullText(c) => c.name == name,
                IndexDefinition::JsonFullText(c) => c.name == name,
                IndexDefinition::Inverted(c) => c.name == name,
            })
            .cloned()
    }

    pub fn remove_index(&self, name: &str) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(pos) = schema.indexes.iter().position(|i| match i {
            IndexDefinition::Vector(c) => c.name == name,
            IndexDefinition::Scalar(c) => c.name == name,
            IndexDefinition::FullText(c) => c.name == name,
            IndexDefinition::JsonFullText(c) => c.name == name,
            IndexDefinition::Inverted(c) => c.name == name,
        }) {
            schema.indexes.remove(pos);
            Ok(())
        } else {
            Err(anyhow!("Index '{}' not found", name))
        }
    }

    pub fn add_constraint(&self, constraint: Constraint) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if schema.constraints.iter().any(|c| c.name == constraint.name) {
            return Err(anyhow!("Constraint '{}' already exists", constraint.name));
        }
        schema.constraints.push(constraint);
        Ok(())
    }

    pub fn drop_constraint(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(pos) = schema.constraints.iter().position(|c| c.name == name) {
            schema.constraints.remove(pos);
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Constraint '{}' not found", name))
        }
    }

    pub fn drop_property(&self, label_or_type: &str, prop_name: &str) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(props) = schema.properties.get_mut(label_or_type) {
            if props.remove(prop_name).is_some() {
                Ok(())
            } else {
                Err(anyhow!(
                    "Property '{}' not found for '{}'",
                    prop_name,
                    label_or_type
                ))
            }
        } else {
            Err(anyhow!("Label or Edge Type '{}' not found", label_or_type))
        }
    }

    pub fn rename_property(
        &self,
        label_or_type: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(props) = schema.properties.get_mut(label_or_type) {
            if let Some(meta) = props.remove(old_name) {
                if props.contains_key(new_name) {
                    // Rollback removal? Or just error.
                    props.insert(old_name.to_string(), meta); // Restore
                    return Err(anyhow!("Property '{}' already exists", new_name));
                }
                props.insert(new_name.to_string(), meta);
                Ok(())
            } else {
                Err(anyhow!(
                    "Property '{}' not found for '{}'",
                    old_name,
                    label_or_type
                ))
            }
        } else {
            Err(anyhow!("Label or Edge Type '{}' not found", label_or_type))
        }
    }

    pub fn drop_label(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if schema.labels.remove(name).is_some() {
            // Also remove properties
            schema.properties.remove(name);
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Label '{}' not found", name))
        }
    }

    pub fn drop_edge_type(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if schema.edge_types.remove(name).is_some() {
            // Also remove properties
            schema.properties.remove(name);
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Edge Type '{}' not found", name))
        }
    }
}

/// Validate identifier names to prevent injection and ensure compatibility.
pub fn validate_identifier(name: &str) -> Result<()> {
    // Length check
    if name.is_empty() || name.len() > 64 {
        return Err(anyhow!("Identifier '{}' must be 1-64 characters", name));
    }

    // First character must be letter or underscore
    let first = name.chars().next().unwrap();
    if !first.is_alphabetic() && first != '_' {
        return Err(anyhow!(
            "Identifier '{}' must start with letter or underscore",
            name
        ));
    }

    // Remaining characters: alphanumeric or underscore
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(anyhow!(
            "Identifier '{}' must contain only alphanumeric and underscore",
            name
        ));
    }

    // Reserved words
    const RESERVED: &[&str] = &[
        "MATCH", "CREATE", "DELETE", "SET", "RETURN", "WHERE", "MERGE", "CALL", "YIELD", "WITH",
        "UNION", "ORDER", "LIMIT",
    ];
    if RESERVED.contains(&name.to_uppercase().as_str()) {
        return Err(anyhow!("Identifier '{}' cannot be a reserved word", name));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_schema_management() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = SchemaManager::load_from_store(store.clone(), &path).await?;

        // Labels
        let lid = manager.add_label("Person", false)?;
        assert_eq!(lid, 1);
        assert!(manager.add_label("Person", false).is_err());

        // Properties
        manager.add_property("Person", "name", DataType::String, false)?;
        assert!(
            manager
                .add_property("Person", "name", DataType::String, false)
                .is_err()
        );

        // Edge types
        let tid = manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
        assert_eq!(tid, 1);

        // JSON Indexes
        manager.add_label("Article", true)?;
        manager.add_json_index("Article", "$.title", DataType::String)?;
        assert!(
            manager
                .add_json_index("Person", "$.name", DataType::String)
                .is_err()
        ); // Not a doc

        manager.save().await?;
        // Check file exists
        assert!(store.get(&path).await.is_ok());

        let manager2 = SchemaManager::load_from_store(store, &path).await?;
        assert!(manager2.schema().labels.contains_key("Person"));
        assert!(
            manager2
                .schema()
                .properties
                .get("Person")
                .unwrap()
                .contains_key("name")
        );

        Ok(())
    }
}
