// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Python bindings for the Uni embedded graph database.
//!
//! This module provides PyO3-based Python bindings that expose the Uni graph
//! database API to Python applications. It includes database management,
//! query execution, schema management, bulk loading, and vector search.

use ::uni_db::{Uni, Value};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uni_common::core::schema::{
    DataType, DistanceMetric, IndexDefinition, ScalarIndexConfig, ScalarIndexType,
    VectorIndexConfig, VectorIndexType,
};

// ============================================================================
// Data Classes (PyO3 structs with get_all for automatic getters)
// ============================================================================

/// Result from a vector similarity search.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct VectorMatch {
    /// Vertex ID of the matched node.
    pub vid: u64,
    /// Distance/similarity score (lower is more similar for L2).
    pub distance: f32,
}

#[pymethods]
impl VectorMatch {
    fn __repr__(&self) -> String {
        format!("VectorMatch(vid={}, distance={})", self.vid, self.distance)
    }
}

/// Metadata about a database snapshot.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Unique identifier for the snapshot.
    pub snapshot_id: String,
    /// Human-readable name (if provided).
    pub name: Option<String>,
    /// ISO 8601 timestamp when created.
    pub created_at: String,
    /// Number of vertices at snapshot time.
    pub vertex_count: u64,
    /// Number of edges at snapshot time.
    pub edge_count: u64,
}

#[pymethods]
impl SnapshotInfo {
    fn __repr__(&self) -> String {
        format!(
            "SnapshotInfo(id='{}', name={:?}, vertices={}, edges={})",
            self.snapshot_id, self.name, self.vertex_count, self.edge_count
        )
    }
}

/// Information about a vertex label in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct LabelInfo {
    /// Label name.
    pub name: String,
    /// Whether this is a document label.
    pub is_document: bool,
    /// Approximate count of vertices with this label.
    pub count: usize,
    /// Properties defined on this label.
    pub properties: Vec<PropertyInfo>,
    /// Indexes defined on this label.
    pub indexes: Vec<IndexInfo>,
    /// Constraints defined on this label.
    pub constraints: Vec<ConstraintInfo>,
}

#[pymethods]
impl LabelInfo {
    fn __repr__(&self) -> String {
        format!(
            "LabelInfo(name='{}', count={}, properties={}, indexes={})",
            self.name,
            self.count,
            self.properties.len(),
            self.indexes.len()
        )
    }
}

/// Information about a property in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PropertyInfo {
    /// Property name.
    pub name: String,
    /// Data type (e.g., "String", "Int64", "Vector{128}").
    pub data_type: String,
    /// Whether null values are allowed.
    pub nullable: bool,
    /// Whether an index exists on this property.
    pub is_indexed: bool,
}

#[pymethods]
impl PropertyInfo {
    fn __repr__(&self) -> String {
        format!(
            "PropertyInfo(name='{}', type='{}', nullable={})",
            self.name, self.data_type, self.nullable
        )
    }
}

/// Information about an index in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Type of index (SCALAR, VECTOR, FULLTEXT).
    pub index_type: String,
    /// Properties covered by the index.
    pub properties: Vec<String>,
    /// Current status (ONLINE, BUILDING, FAILED).
    pub status: String,
}

#[pymethods]
impl IndexInfo {
    fn __repr__(&self) -> String {
        format!(
            "IndexInfo(name='{}', type='{}', properties={:?})",
            self.name, self.index_type, self.properties
        )
    }
}

/// Information about a constraint in the schema.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct ConstraintInfo {
    /// Constraint name.
    pub name: String,
    /// Type of constraint (UNIQUE, EXISTS, CHECK).
    pub constraint_type: String,
    /// Properties covered by the constraint.
    pub properties: Vec<String>,
    /// Whether the constraint is currently enforced.
    pub enabled: bool,
}

#[pymethods]
impl ConstraintInfo {
    fn __repr__(&self) -> String {
        format!(
            "ConstraintInfo(name='{}', type='{}', enabled={})",
            self.name, self.constraint_type, self.enabled
        )
    }
}

/// Statistics from a bulk loading operation.
#[pyclass(get_all)]
#[derive(Debug, Clone, Default)]
pub struct BulkStats {
    /// Number of vertices inserted.
    pub vertices_inserted: usize,
    /// Number of edges inserted.
    pub edges_inserted: usize,
    /// Number of indexes rebuilt.
    pub indexes_rebuilt: usize,
    /// Total duration in seconds.
    pub duration_secs: f64,
    /// Duration spent building indexes in seconds.
    pub index_build_duration_secs: f64,
    /// Whether indexes are still building in background.
    pub indexes_pending: bool,
}

#[pymethods]
impl BulkStats {
    fn __repr__(&self) -> String {
        format!(
            "BulkStats(vertices={}, edges={}, duration={:.2}s)",
            self.vertices_inserted, self.edges_inserted, self.duration_secs
        )
    }
}

/// Progress callback data during bulk loading.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct BulkProgress {
    /// Current phase of bulk loading.
    pub phase: String,
    /// Number of rows processed so far.
    pub rows_processed: usize,
    /// Total rows if known.
    pub total_rows: Option<usize>,
    /// Current label being processed.
    pub current_label: Option<String>,
}

#[pymethods]
impl BulkProgress {
    fn __repr__(&self) -> String {
        format!(
            "BulkProgress(phase='{}', processed={})",
            self.phase, self.rows_processed
        )
    }
}

/// Status of an index rebuild task.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct IndexRebuildTask {
    /// Unique task identifier.
    pub task_id: String,
    /// Label being reindexed.
    pub label: String,
    /// Current status (PENDING, IN_PROGRESS, COMPLETED, FAILED).
    pub status: String,
    /// Error message if failed.
    pub error: Option<String>,
}

#[pymethods]
impl IndexRebuildTask {
    fn __repr__(&self) -> String {
        format!(
            "IndexRebuildTask(label='{}', status='{}')",
            self.label, self.status
        )
    }
}

/// Statistics from a compaction operation.
#[pyclass(get_all)]
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of files compacted.
    pub files_compacted: usize,
    /// Bytes before compaction.
    pub bytes_before: u64,
    /// Bytes after compaction.
    pub bytes_after: u64,
    /// Duration in seconds.
    pub duration_secs: f64,
    /// Number of CRDT merges performed.
    pub crdt_merges: usize,
}

#[pymethods]
impl CompactionStats {
    fn __repr__(&self) -> String {
        format!(
            "CompactionStats(files={}, bytes_before={}, bytes_after={}, duration={:.2}s)",
            self.files_compacted, self.bytes_before, self.bytes_after, self.duration_secs
        )
    }
}

// ============================================================================
// DatabaseBuilder
// ============================================================================

/// Mode for opening database.
#[derive(Debug, Clone, Copy)]
enum OpenMode {
    /// Open or create
    Open,
    /// Open existing only
    OpenExisting,
    /// Create new only
    Create,
    /// Temporary database
    Temporary,
}

/// Builder for creating and configuring a Database instance.
#[pyclass]
#[derive(Debug, Clone)]
struct DatabaseBuilder {
    uri: String,
    mode: OpenMode,
    hybrid_local: Option<String>,
    hybrid_remote: Option<String>,
    cache_size: Option<usize>,
    parallelism: Option<usize>,
    at_snapshot: Option<String>,
}

#[pymethods]
impl DatabaseBuilder {
    /// Open or create a database at the given path.
    #[staticmethod]
    fn open(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::Open,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            at_snapshot: None,
        }
    }

    /// Open an existing database. Fails if it does not exist.
    #[staticmethod]
    fn open_existing(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::OpenExisting,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            at_snapshot: None,
        }
    }

    /// Create a new database. Fails if it already exists.
    #[staticmethod]
    fn create(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::Create,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            at_snapshot: None,
        }
    }

    /// Create a temporary database that is deleted when dropped.
    #[staticmethod]
    fn temporary() -> Self {
        Self {
            uri: String::new(),
            mode: OpenMode::Temporary,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            at_snapshot: None,
        }
    }

    /// Create an in-memory database (alias for temporary).
    #[staticmethod]
    fn in_memory() -> Self {
        Self::temporary()
    }

    /// Configure hybrid storage with local metadata and remote data.
    fn hybrid(
        mut slf: PyRefMut<'_, Self>,
        local_path: String,
        remote_url: String,
    ) -> PyRefMut<'_, Self> {
        slf.hybrid_local = Some(local_path);
        slf.hybrid_remote = Some(remote_url);
        slf
    }

    /// Set maximum cache size in bytes.
    fn cache_size(mut slf: PyRefMut<'_, Self>, bytes: usize) -> PyRefMut<'_, Self> {
        slf.cache_size = Some(bytes);
        slf
    }

    /// Set query parallelism (number of worker threads).
    fn parallelism(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.parallelism = Some(n);
        slf
    }

    /// Open database at a specific snapshot (time-travel, read-only).
    fn at_snapshot(mut slf: PyRefMut<'_, Self>, snapshot_id: String) -> PyRefMut<'_, Self> {
        slf.at_snapshot = Some(snapshot_id);
        slf
    }

    /// Build and return the Database instance.
    fn build(&self) -> PyResult<Database> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create runtime: {}",
                    e
                ))
            })?;

        let uni = rt
            .block_on(async {
                let mut builder = match self.mode {
                    OpenMode::Open => Uni::open(&self.uri),
                    OpenMode::OpenExisting => Uni::open_existing(&self.uri),
                    OpenMode::Create => Uni::create(&self.uri),
                    OpenMode::Temporary => Uni::temporary(),
                };

                if let (Some(local), Some(remote)) = (&self.hybrid_local, &self.hybrid_remote) {
                    builder = builder.hybrid(local, remote);
                }

                if let Some(size) = self.cache_size {
                    builder = builder.cache_size(size);
                }

                if let Some(n) = self.parallelism {
                    builder = builder.parallelism(n);
                }

                if let Some(snap) = &self.at_snapshot {
                    builder = builder.at_snapshot(snap);
                }

                builder.build().await
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

        Ok(Database {
            inner: Arc::new(uni),
            rt,
        })
    }
}

// ============================================================================
// QueryBuilder
// ============================================================================

/// Builder for constructing and executing parameterized queries.
#[pyclass]
struct QueryBuilder {
    inner: Arc<Uni>,
    rt: tokio::runtime::Handle,
    cypher: String,
    params: HashMap<String, PyObject>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
}

#[pymethods]
impl QueryBuilder {
    /// Bind a parameter to the query.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: PyObject) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters from a dictionary.
    fn params(&mut self, params: Bound<'_, PyDict>) {
        for (k, v) in params {
            if let Ok(key) = k.extract::<String>() {
                self.params.insert(key, v.into());
            }
        }
    }

    /// Set maximum execution time in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum memory for this query in bytes.
    fn max_memory(mut slf: PyRefMut<'_, Self>, bytes: usize) -> PyRefMut<'_, Self> {
        slf.max_memory = Some(bytes);
        slf
    }

    /// Execute the query and fetch all results.
    fn fetch_all(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        let cypher = self.cypher.clone();
        let inner = self.inner.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;

        let result = self
            .rt
            .block_on(async move {
                let mut builder = inner.query_with(&cypher);
                for (k, v) in rust_params {
                    builder = builder.param(&k, v);
                }
                if let Some(t) = timeout_secs {
                    builder = builder.timeout(Duration::from_secs_f64(t));
                }
                if let Some(m) = max_memory {
                    builder = builder.max_memory(m);
                }
                builder.fetch_all().await
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let mut rows = Vec::new();
        for row in result {
            let dict = PyDict::new(py);
            for (col_name, val) in row.as_map() {
                dict.set_item(col_name, value_to_py(py, val)?)?;
            }
            rows.push(dict.into());
        }
        Ok(rows)
    }
}

// ============================================================================
// SchemaBuilder, LabelBuilder, EdgeTypeBuilder
// ============================================================================

/// Builder for defining and modifying the graph schema.
#[pyclass]
#[derive(Clone)]
struct SchemaBuilder {
    inner: Arc<Uni>,
    rt: tokio::runtime::Handle,
    pending_labels: Vec<(String, bool)>,
    pending_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    pending_properties: Vec<(String, String, DataType, bool)>,
    pending_indexes: Vec<IndexDefinition>,
}

#[pymethods]
impl SchemaBuilder {
    /// Start defining a new label.
    fn label(&self, name: &str) -> PyResult<LabelBuilder> {
        Ok(LabelBuilder {
            parent_inner: self.inner.clone(),
            parent_rt: self.rt.clone(),
            parent_labels: self.pending_labels.clone(),
            parent_edge_types: self.pending_edge_types.clone(),
            parent_properties: self.pending_properties.clone(),
            parent_indexes: self.pending_indexes.clone(),
            name: name.to_string(),
            is_document: false,
            properties: Vec::new(),
            indexes: Vec::new(),
        })
    }

    /// Start defining a new edge type.
    fn edge_type(
        &self,
        name: &str,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
    ) -> PyResult<EdgeTypeBuilder> {
        Ok(EdgeTypeBuilder {
            parent_inner: self.inner.clone(),
            parent_rt: self.rt.clone(),
            parent_labels: self.pending_labels.clone(),
            parent_edge_types: self.pending_edge_types.clone(),
            parent_properties: self.pending_properties.clone(),
            parent_indexes: self.pending_indexes.clone(),
            name: name.to_string(),
            from_labels,
            to_labels,
            properties: Vec::new(),
        })
    }

    /// Apply all pending schema changes.
    fn apply(&self) -> PyResult<()> {
        self.rt
            .block_on(async {
                let sm = self.inner.schema_manager();

                // Add labels
                for (name, is_document) in &self.pending_labels {
                    sm.add_label(name, *is_document)
                        .map_err(|e| e.to_string())?;
                }

                // Add edge types
                for (name, from, to) in &self.pending_edge_types {
                    sm.add_edge_type(name, from.clone(), to.clone())
                        .map_err(|e| e.to_string())?;
                }

                // Add properties
                for (label_or_type, prop_name, data_type, nullable) in &self.pending_properties {
                    sm.add_property(label_or_type, prop_name, data_type.clone(), *nullable)
                        .map_err(|e| e.to_string())?;
                }

                // Add indexes
                for idx in &self.pending_indexes {
                    sm.add_index(idx.clone()).map_err(|e| e.to_string())?;
                }

                sm.save().await.map_err(|e| e.to_string())?;
                Ok::<(), String>(())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }
}

/// Builder for defining a label with its properties and indexes.
#[pyclass]
#[derive(Clone)]
struct LabelBuilder {
    parent_inner: Arc<Uni>,
    parent_rt: tokio::runtime::Handle,
    parent_labels: Vec<(String, bool)>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, DataType, bool)>,
    parent_indexes: Vec<IndexDefinition>,
    name: String,
    is_document: bool,
    properties: Vec<(String, DataType, bool)>,
    indexes: Vec<IndexDefinition>,
}

#[pymethods]
impl LabelBuilder {
    /// Mark this label as a document label.
    fn document(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.is_document = true;
        slf
    }

    /// Add a required property to this label.
    fn property(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = parse_data_type(&data_type)?;
        slf.properties.push((name, dt, false));
        Ok(slf)
    }

    /// Add a nullable property to this label.
    fn property_nullable(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = parse_data_type(&data_type)?;
        slf.properties.push((name, dt, true));
        Ok(slf)
    }

    /// Add a vector property (shorthand for vector type + index).
    fn vector(mut slf: PyRefMut<'_, Self>, name: String, dimensions: usize) -> PyRefMut<'_, Self> {
        slf.properties
            .push((name, DataType::Vector { dimensions }, false));
        slf
    }

    /// Add an index on a property.
    fn index(
        mut slf: PyRefMut<'_, Self>,
        property: String,
        index_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let label = slf.name.clone();
        let idx = create_index_definition(&label, &property, &index_type)?;
        slf.indexes.push(idx);
        Ok(slf)
    }

    /// Finish this label and return to SchemaBuilder.
    fn done(&self) -> PyResult<SchemaBuilder> {
        let mut labels = self.parent_labels.clone();
        labels.push((self.name.clone(), self.is_document));

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable) in &self.properties {
            properties.push((self.name.clone(), prop_name.clone(), dt.clone(), *nullable));
        }

        let mut indexes = self.parent_indexes.clone();
        indexes.extend(self.indexes.clone());

        Ok(SchemaBuilder {
            inner: self.parent_inner.clone(),
            rt: self.parent_rt.clone(),
            pending_labels: labels,
            pending_edge_types: self.parent_edge_types.clone(),
            pending_properties: properties,
            pending_indexes: indexes,
        })
    }

    /// Apply schema changes immediately.
    fn apply(&self) -> PyResult<()> {
        self.done()?.apply()
    }
}

/// Builder for defining an edge type with its properties.
#[pyclass]
#[derive(Clone)]
struct EdgeTypeBuilder {
    parent_inner: Arc<Uni>,
    parent_rt: tokio::runtime::Handle,
    parent_labels: Vec<(String, bool)>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, DataType, bool)>,
    parent_indexes: Vec<IndexDefinition>,
    name: String,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    properties: Vec<(String, DataType, bool)>,
}

#[pymethods]
impl EdgeTypeBuilder {
    /// Add a required property to this edge type.
    fn property(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = parse_data_type(&data_type)?;
        slf.properties.push((name, dt, false));
        Ok(slf)
    }

    /// Add a nullable property to this edge type.
    fn property_nullable(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = parse_data_type(&data_type)?;
        slf.properties.push((name, dt, true));
        Ok(slf)
    }

    /// Finish this edge type and return to SchemaBuilder.
    fn done(&self) -> PyResult<SchemaBuilder> {
        let mut edge_types = self.parent_edge_types.clone();
        edge_types.push((
            self.name.clone(),
            self.from_labels.clone(),
            self.to_labels.clone(),
        ));

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable) in &self.properties {
            properties.push((self.name.clone(), prop_name.clone(), dt.clone(), *nullable));
        }

        Ok(SchemaBuilder {
            inner: self.parent_inner.clone(),
            rt: self.parent_rt.clone(),
            pending_labels: self.parent_labels.clone(),
            pending_edge_types: edge_types,
            pending_properties: properties,
            pending_indexes: self.parent_indexes.clone(),
        })
    }

    /// Apply schema changes immediately.
    fn apply(&self) -> PyResult<()> {
        self.done()?.apply()
    }
}

// ============================================================================
// SessionBuilder and Session
// ============================================================================

/// Builder for creating query sessions with scoped variables.
#[pyclass]
#[derive(Clone)]
struct SessionBuilder {
    inner: Arc<Uni>,
    rt: tokio::runtime::Handle,
    variables: HashMap<String, serde_json::Value>,
}

#[pymethods]
impl SessionBuilder {
    /// Set a session variable.
    fn set(&mut self, py: Python, key: String, value: PyObject) -> PyResult<()> {
        let json_val = py_object_to_json(py, &value)?;
        self.variables.insert(key, json_val);
        Ok(())
    }

    /// Build the session.
    fn build(&self) -> PyResult<Session> {
        Ok(Session {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            variables: self.variables.clone(),
        })
    }
}

/// A query session with scoped variables.
#[pyclass]
#[derive(Clone)]
struct Session {
    inner: Arc<Uni>,
    rt: tokio::runtime::Handle,
    variables: HashMap<String, serde_json::Value>,
}

#[pymethods]
impl Session {
    /// Execute a query with session variables available.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, PyObject>>,
    ) -> PyResult<Vec<PyObject>> {
        let mut rust_params = HashMap::new();

        // Add session variables
        for (k, v) in &self.variables {
            let val = Value::from(v.clone());
            rust_params.insert(format!("session.{}", k), val);
        }

        // Add query params (override session vars if same name)
        if let Some(p) = params {
            for (k, v) in p {
                let val = py_object_to_value(py, &v)?;
                rust_params.insert(k, val);
            }
        }

        let result = self
            .rt
            .block_on(async {
                let mut builder = self.inner.query_with(cypher);
                for (k, v) in rust_params {
                    builder = builder.param(&k, v);
                }
                builder.fetch_all().await
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let mut rows = Vec::new();
        for row in result {
            let dict = PyDict::new(py);
            for (col_name, val) in row.as_map() {
                dict.set_item(col_name, value_to_py(py, val)?)?;
            }
            rows.push(dict.into());
        }
        Ok(rows)
    }

    /// Execute a mutation query, returning affected row count.
    fn execute(&self, cypher: &str) -> PyResult<usize> {
        let result = self
            .rt
            .block_on(async { self.inner.execute(cypher).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(result.affected_rows)
    }

    /// Get a session variable value.
    fn get(&self, py: Python, key: &str) -> PyResult<Option<PyObject>> {
        match self.variables.get(key) {
            Some(v) => Ok(Some(json_value_to_py(py, v)?)),
            None => Ok(None),
        }
    }
}

// ============================================================================
// BulkWriterBuilder and BulkWriter
// ============================================================================

/// Builder for configuring bulk data loading.
#[pyclass]
#[derive(Clone)]
struct BulkWriterBuilder {
    inner: Arc<Uni>,
    rt: tokio::runtime::Handle,
    defer_vector_indexes: bool,
    defer_scalar_indexes: bool,
    batch_size: usize,
    async_indexes: bool,
}

#[pymethods]
impl BulkWriterBuilder {
    /// Defer vector index building until commit.
    fn defer_vector_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_vector_indexes = defer;
        slf
    }

    /// Defer scalar index building until commit.
    fn defer_scalar_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_scalar_indexes = defer;
        slf
    }

    /// Set batch size for flushing to storage.
    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.batch_size = size;
        slf
    }

    /// Build indexes asynchronously after commit.
    fn async_indexes(mut slf: PyRefMut<'_, Self>, async_: bool) -> PyRefMut<'_, Self> {
        slf.async_indexes = async_;
        slf
    }

    /// Build the BulkWriter.
    fn build(&self) -> PyResult<BulkWriter> {
        Ok(BulkWriter {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            stats: BulkStats::default(),
            aborted: false,
            committed: false,
        })
    }
}

/// Bulk writer for high-throughput data ingestion.
#[pyclass]
struct BulkWriter {
    inner: Arc<Uni>,
    rt: tokio::runtime::Handle,
    stats: BulkStats,
    aborted: bool,
    committed: bool,
}

#[pymethods]
impl BulkWriter {
    /// Insert vertices in bulk, returning allocated VIDs.
    fn insert_vertices(
        &mut self,
        py: Python,
        label: &str,
        vertices: Vec<HashMap<String, PyObject>>,
    ) -> PyResult<Vec<u64>> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        let mut rust_props = Vec::new();
        for v in vertices {
            let mut map = HashMap::new();
            for (k, val) in v {
                let value = py_object_to_value(py, &val)?;
                map.insert(k, serde_json::Value::from(value));
            }
            rust_props.push(map);
        }

        let vids = self
            .rt
            .block_on(async {
                self.inner
                    .bulk_insert_vertices(label, rust_props)
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        self.stats.vertices_inserted += vids.len();
        Ok(vids.into_iter().map(|v| v.as_u64()).collect())
    }

    /// Insert edges in bulk.
    fn insert_edges(
        &mut self,
        py: Python,
        edge_type: &str,
        edges: Vec<(u64, u64, HashMap<String, PyObject>)>,
    ) -> PyResult<()> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        let edge_count = edges.len();
        let mut rust_edges = Vec::new();
        for (src, dst, props) in edges {
            let mut map = HashMap::new();
            for (k, v) in props {
                let val = py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_edges.push((::uni_db::Vid::from(src), ::uni_db::Vid::from(dst), map));
        }

        self.rt
            .block_on(async {
                self.inner
                    .bulk_insert_edges(edge_type, rust_edges)
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        self.stats.edges_inserted += edge_count;
        Ok(())
    }

    /// Commit all pending data and rebuild indexes.
    fn commit(&mut self) -> PyResult<BulkStats> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        // Flush remaining data
        self.rt
            .block_on(async { self.inner.flush().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        self.committed = true;
        Ok(self.stats.clone())
    }

    /// Abort bulk loading and discard uncommitted changes.
    fn abort(&mut self) -> PyResult<()> {
        if self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Cannot abort: already committed",
            ));
        }
        self.aborted = true;
        Ok(())
    }
}

// ============================================================================
// VectorSearchBuilder
// ============================================================================

/// Builder for vector similarity search queries.
#[pyclass]
#[derive(Clone)]
struct VectorSearchBuilder {
    inner: Arc<Uni>,
    rt: tokio::runtime::Handle,
    label: String,
    property: String,
    query_vec: Vec<f32>,
    k: usize,
    threshold: Option<f32>,
    filter_expr: Option<String>,
}

#[pymethods]
impl VectorSearchBuilder {
    /// Set the number of results to return.
    fn k(mut slf: PyRefMut<'_, Self>, k: usize) -> PyRefMut<'_, Self> {
        slf.k = k;
        slf
    }

    /// Set distance threshold (results with distance > threshold are filtered).
    fn threshold(mut slf: PyRefMut<'_, Self>, threshold: f32) -> PyRefMut<'_, Self> {
        slf.threshold = Some(threshold);
        slf
    }

    /// Set filter expression (Cypher WHERE clause).
    fn filter(mut slf: PyRefMut<'_, Self>, filter_expr: String) -> PyRefMut<'_, Self> {
        slf.filter_expr = Some(filter_expr);
        slf
    }

    /// Execute the search and return VectorMatch results.
    fn search(&self) -> PyResult<Vec<VectorMatch>> {
        let matches = self
            .rt
            .block_on(async {
                self.inner
                    .vector_search(&self.label, &self.property, &self.query_vec, self.k)
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        let mut results = Vec::new();
        for m in matches {
            if let Some(t) = self.threshold {
                if m.distance > t {
                    continue;
                }
            }
            results.push(VectorMatch {
                vid: m.vid.as_u64(),
                distance: m.distance,
            });
        }
        Ok(results)
    }

    /// Execute search and fetch full node data.
    fn fetch_nodes(&self, py: Python) -> PyResult<Vec<(PyObject, f32)>> {
        let nodes = self
            .rt
            .block_on(async {
                self.inner
                    .vector_search_with(&self.label, &self.property, &self.query_vec)
                    .k(self.k)
                    .fetch_nodes()
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        let mut results = Vec::new();
        for (node, dist) in nodes {
            if let Some(t) = self.threshold {
                if dist > t {
                    continue;
                }
            }
            let dict = PyDict::new(py);
            dict.set_item("_id", node.vid.to_string())?;
            dict.set_item("_label", &node.label)?;
            for (k, v) in &node.properties {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            results.push((dict.into(), dist));
        }
        Ok(results)
    }
}

// ============================================================================
// Transaction
// ============================================================================

/// A database transaction for atomic operations.
#[pyclass]
struct Transaction {
    inner: Arc<Uni>,
    rt: tokio::runtime::Handle,
    completed: bool,
}

#[pymethods]
impl Transaction {
    /// Execute a query within this transaction.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, PyObject>>,
    ) -> PyResult<Vec<PyObject>> {
        let mut rust_params = HashMap::new();
        if let Some(p) = params {
            for (k, v) in p {
                let val = py_object_to_value(py, &v)?;
                rust_params.insert(k, val);
            }
        }

        let result = self
            .rt
            .block_on(async {
                let mut builder = self.inner.query_with(cypher);
                for (k, v) in rust_params {
                    builder = builder.param(&k, v);
                }
                builder.fetch_all().await
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let mut rows = Vec::new();
        for row in result {
            let dict = PyDict::new(py);
            for (col_name, val) in row.as_map() {
                dict.set_item(col_name, value_to_py(py, val)?)?;
            }
            rows.push(dict.into());
        }
        Ok(rows)
    }

    /// Commit the transaction.
    fn commit(&mut self) -> PyResult<()> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        self.rt
            .block_on(async {
                let writer_lock = self.inner.writer().ok_or_else(|| "Read only".to_string())?;
                let mut writer = writer_lock.write().await;
                writer
                    .commit_transaction()
                    .await
                    .map_err(|e| e.to_string())?;
                Ok::<(), String>(())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        self.completed = true;
        Ok(())
    }

    /// Rollback the transaction.
    fn rollback(&mut self) -> PyResult<()> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        self.rt
            .block_on(async {
                let writer_lock = self.inner.writer().ok_or_else(|| "Read only".to_string())?;
                let mut writer = writer_lock.write().await;
                writer.rollback_transaction().map_err(|e| e.to_string())?;
                Ok::<(), String>(())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        self.completed = true;
        Ok(())
    }
}

// ============================================================================
// Database (main entry point)
// ============================================================================

/// Main entry point for the Uni embedded graph database.
#[pyclass]
struct Database {
    inner: Arc<Uni>,
    rt: tokio::runtime::Runtime,
}

#[pymethods]
impl Database {
    /// Create or open a database at the given path.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let uni = rt
            .block_on(async { Uni::open(path).build().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

        Ok(Database {
            inner: Arc::new(uni),
            rt,
        })
    }

    // ========================================================================
    // Query Methods
    // ========================================================================

    /// Execute a Cypher query and return results.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, PyObject>>,
    ) -> PyResult<Vec<PyObject>> {
        let mut rust_params = HashMap::new();
        if let Some(p) = params {
            for (k, v) in p {
                let val = py_object_to_value(py, &v)?;
                rust_params.insert(k, val);
            }
        }

        let result = self
            .rt
            .block_on(async {
                let mut builder = self.inner.query_with(cypher);
                for (k, v) in rust_params {
                    builder = builder.param(&k, v);
                }
                builder.fetch_all().await
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let mut rows = Vec::new();
        for row in result {
            let dict = PyDict::new(py);
            for (col_name, val) in row.as_map() {
                dict.set_item(col_name, value_to_py(py, val)?)?;
            }
            rows.push(dict.into());
        }
        Ok(rows)
    }

    /// Create a query builder for parameterized queries.
    fn query_with(&self, cypher: &str) -> QueryBuilder {
        QueryBuilder {
            inner: self.inner.clone(),
            rt: self.rt.handle().clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
        }
    }

    /// Execute a mutation query, returning affected row count.
    fn execute(&self, cypher: &str) -> PyResult<usize> {
        let result = self
            .rt
            .block_on(async { self.inner.execute(cypher).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(result.affected_rows)
    }

    /// Explain the query plan without executing.
    fn explain(&self, py: Python, cypher: &str) -> PyResult<PyObject> {
        let output = self
            .rt
            .block_on(async { self.inner.explain(cypher).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let dict = PyDict::new(py);
        dict.set_item("plan_text", &output.plan_text)?;
        dict.set_item("warnings", &output.warnings)?;

        // Convert cost estimates
        let cost_dict = PyDict::new(py);
        cost_dict.set_item("estimated_rows", output.cost_estimates.estimated_rows)?;
        cost_dict.set_item("estimated_cost", output.cost_estimates.estimated_cost)?;
        dict.set_item("cost_estimates", cost_dict)?;

        // Convert index usage
        let index_usage = PyList::empty(py);
        for usage in &output.index_usage {
            let usage_dict = PyDict::new(py);
            usage_dict.set_item("label_or_type", &usage.label_or_type)?;
            usage_dict.set_item("property", &usage.property)?;
            usage_dict.set_item("index_type", &usage.index_type)?;
            usage_dict.set_item("used", usage.used)?;
            if let Some(reason) = &usage.reason {
                usage_dict.set_item("reason", reason)?;
            }
            index_usage.append(usage_dict)?;
        }
        dict.set_item("index_usage", index_usage)?;

        // Convert suggestions
        let suggestions = PyList::empty(py);
        for suggestion in &output.suggestions {
            let sug_dict = PyDict::new(py);
            sug_dict.set_item("label_or_type", &suggestion.label_or_type)?;
            sug_dict.set_item("property", &suggestion.property)?;
            sug_dict.set_item("index_type", &suggestion.index_type)?;
            sug_dict.set_item("reason", &suggestion.reason)?;
            sug_dict.set_item("create_statement", &suggestion.create_statement)?;
            suggestions.append(sug_dict)?;
        }
        dict.set_item("suggestions", suggestions)?;

        Ok(dict.into())
    }

    /// Profile query execution with operator-level statistics.
    fn profile(&self, py: Python, cypher: &str) -> PyResult<(Vec<PyObject>, PyObject)> {
        let (results, profile) = self
            .rt
            .block_on(async { self.inner.profile(cypher).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        // Convert results
        let mut rows = Vec::new();
        for row in results {
            let dict = PyDict::new(py);
            for (col_name, val) in row.as_map() {
                dict.set_item(col_name, value_to_py(py, val)?)?;
            }
            rows.push(dict.into());
        }

        // Convert profile output
        let profile_dict = PyDict::new(py);
        profile_dict.set_item("total_time_ms", profile.total_time_ms)?;
        profile_dict.set_item("peak_memory_bytes", profile.peak_memory_bytes)?;

        // Add explain info from profile
        profile_dict.set_item("plan_text", &profile.explain.plan_text)?;

        let ops = PyList::empty(py);
        for op in &profile.runtime_stats {
            let op_dict = PyDict::new(py);
            op_dict.set_item("operator", &op.operator)?;
            op_dict.set_item("actual_rows", op.actual_rows)?;
            op_dict.set_item("time_ms", op.time_ms)?;
            op_dict.set_item("memory_bytes", op.memory_bytes)?;
            if let Some(hits) = op.index_hits {
                op_dict.set_item("index_hits", hits)?;
            }
            if let Some(misses) = op.index_misses {
                op_dict.set_item("index_misses", misses)?;
            }
            ops.append(op_dict)?;
        }
        profile_dict.set_item("operators", ops)?;

        Ok((rows, profile_dict.into()))
    }

    // ========================================================================
    // Transaction Methods
    // ========================================================================

    /// Begin a new transaction.
    fn begin(&self) -> PyResult<Transaction> {
        let handle = self.rt.handle().clone();
        handle
            .block_on(async {
                let writer_lock = self.inner.writer().ok_or_else(|| "Read only".to_string())?;
                let mut writer = writer_lock.write().await;
                writer.begin_transaction().map_err(|e| e.to_string())?;
                Ok::<(), String>(())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(Transaction {
            inner: self.inner.clone(),
            rt: handle,
            completed: false,
        })
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush(&self) -> PyResult<()> {
        self.rt
            .block_on(async {
                self.inner.flush().await.map_err(|e| e.to_string())?;
                Ok::<(), String>(())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    // ========================================================================
    // Schema Methods
    // ========================================================================

    /// Create a schema builder for defining labels, edge types, and indexes.
    fn schema(&self) -> SchemaBuilder {
        SchemaBuilder {
            inner: self.inner.clone(),
            rt: self.rt.handle().clone(),
            pending_labels: Vec::new(),
            pending_edge_types: Vec::new(),
            pending_properties: Vec::new(),
            pending_indexes: Vec::new(),
        }
    }

    /// Create a label (shorthand for schema().label(name).apply()).
    fn create_label(&self, name: &str) -> PyResult<u16> {
        self.rt
            .block_on(async {
                let sm = self.inner.schema_manager();
                let id = sm.add_label(name, false).map_err(|e| e.to_string())?;
                sm.save().await.map_err(|e| e.to_string())?;
                Ok::<u16, String>(id)
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Create an edge type.
    #[pyo3(signature = (name, from_labels=None, to_labels=None))]
    fn create_edge_type(
        &self,
        name: &str,
        from_labels: Option<Vec<String>>,
        to_labels: Option<Vec<String>>,
    ) -> PyResult<u16> {
        let from = from_labels.unwrap_or_default();
        let to = to_labels.unwrap_or_default();
        self.rt
            .block_on(async {
                let sm = self.inner.schema_manager();
                let id = sm
                    .add_edge_type(name, from, to)
                    .map_err(|e| e.to_string())?;
                sm.save().await.map_err(|e| e.to_string())?;
                Ok::<u16, String>(id)
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Add a property to a label or edge type.
    fn add_property(
        &self,
        label_or_type: &str,
        name: &str,
        data_type: &str,
        nullable: bool,
    ) -> PyResult<()> {
        let dt = parse_data_type(data_type)?;
        self.rt
            .block_on(async {
                let sm = self.inner.schema_manager();
                sm.add_property(label_or_type, name, dt, nullable)
                    .map_err(|e| e.to_string())?;
                sm.save().await.map_err(|e| e.to_string())?;
                Ok::<(), String>(())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    /// Check if a label exists in the schema.
    fn label_exists(&self, name: &str) -> PyResult<bool> {
        self.rt
            .block_on(async { self.inner.label_exists(name).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Check if an edge type exists in the schema.
    fn edge_type_exists(&self, name: &str) -> PyResult<bool> {
        self.rt
            .block_on(async { self.inner.edge_type_exists(name).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get all label names.
    fn list_labels(&self) -> PyResult<Vec<String>> {
        self.rt
            .block_on(async { self.inner.list_labels().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get all edge type names.
    fn list_edge_types(&self) -> PyResult<Vec<String>> {
        self.rt
            .block_on(async { self.inner.list_edge_types().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get detailed information about a label.
    fn get_label_info(&self, name: &str) -> PyResult<Option<LabelInfo>> {
        let info = self
            .rt
            .block_on(async { self.inner.get_label_info(name).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(info.map(|i| LabelInfo {
            name: i.name,
            is_document: i.is_document,
            count: i.count,
            properties: i
                .properties
                .into_iter()
                .map(|p| PropertyInfo {
                    name: p.name,
                    data_type: p.data_type,
                    nullable: p.nullable,
                    is_indexed: p.is_indexed,
                })
                .collect(),
            indexes: i
                .indexes
                .into_iter()
                .map(|idx| IndexInfo {
                    name: idx.name,
                    index_type: idx.index_type,
                    properties: idx.properties,
                    status: idx.status,
                })
                .collect(),
            constraints: i
                .constraints
                .into_iter()
                .map(|c| ConstraintInfo {
                    name: c.name,
                    constraint_type: c.constraint_type,
                    properties: c.properties,
                    enabled: c.enabled,
                })
                .collect(),
        }))
    }

    /// Get the full schema as a dictionary.
    fn get_schema(&self, py: Python) -> PyResult<PyObject> {
        let schema = self.inner.get_schema();
        let dict = PyDict::new(py);

        // Labels
        let labels = PyDict::new(py);
        for (name, meta) in &schema.labels {
            let label_dict = PyDict::new(py);
            label_dict.set_item("id", meta.id)?;
            label_dict.set_item("is_document", meta.is_document)?;
            labels.set_item(name, label_dict)?;
        }
        dict.set_item("labels", labels)?;

        // Edge types
        let edge_types = PyDict::new(py);
        for (name, meta) in &schema.edge_types {
            let et_dict = PyDict::new(py);
            et_dict.set_item("id", meta.id)?;
            edge_types.set_item(name, et_dict)?;
        }
        dict.set_item("edge_types", edge_types)?;

        Ok(dict.into())
    }

    /// Load schema from a JSON file.
    fn load_schema(&self, path: &str) -> PyResult<()> {
        self.rt
            .block_on(async { self.inner.load_schema(path).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Save schema to a JSON file.
    fn save_schema(&self, path: &str) -> PyResult<()> {
        self.rt
            .block_on(async { self.inner.save_schema(path).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    // ========================================================================
    // Index Methods
    // ========================================================================

    /// Create a scalar index on a property.
    fn create_scalar_index(&self, label: &str, property: &str, index_type: &str) -> PyResult<()> {
        let it = match index_type.to_lowercase().as_str() {
            "btree" => ScalarIndexType::BTree,
            "hash" => ScalarIndexType::Hash,
            "bitmap" => ScalarIndexType::Bitmap,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unknown index type: {}",
                    index_type
                )));
            }
        };

        self.rt
            .block_on(async {
                let sm = self.inner.schema_manager();
                let idx_config = ScalarIndexConfig {
                    name: format!("idx_{}_{}", label, property),
                    label: label.to_string(),
                    properties: vec![property.to_string()],
                    index_type: it,
                    where_clause: None,
                };
                let def = IndexDefinition::Scalar(idx_config);
                sm.add_index(def).map_err(|e| e.to_string())?;
                sm.save().await.map_err(|e| e.to_string())?;
                Ok::<(), String>(())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    /// Create a vector index on a property.
    fn create_vector_index(&self, label: &str, property: &str, metric: &str) -> PyResult<()> {
        self.rt
            .block_on(async {
                let sm = self.inner.schema_manager();

                let metric_type = match metric.to_lowercase().as_str() {
                    "l2" => DistanceMetric::L2,
                    "cosine" => DistanceMetric::Cosine,
                    "dot" => DistanceMetric::Dot,
                    _ => return Err(format!("Unknown metric: {}", metric)),
                };

                let idx_config = VectorIndexConfig {
                    name: format!("idx_{}_{}_vec", label, property),
                    label: label.to_string(),
                    property: property.to_string(),
                    index_type: VectorIndexType::Hnsw {
                        m: 16,
                        ef_construction: 200,
                        ef_search: 50,
                    },
                    metric: metric_type,
                    embedding_config: None,
                };

                let def = IndexDefinition::Vector(idx_config);
                sm.add_index(def).map_err(|e| e.to_string())?;
                sm.save().await.map_err(|e| e.to_string())?;

                Ok::<(), String>(())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    /// Rebuild indexes for a label.
    #[pyo3(signature = (label, async_=false))]
    fn rebuild_indexes(&self, label: &str, async_: bool) -> PyResult<Option<String>> {
        self.rt
            .block_on(async { self.inner.rebuild_indexes(label, async_).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get status of background index rebuild tasks.
    fn index_rebuild_status(&self) -> PyResult<Vec<IndexRebuildTask>> {
        let tasks = self
            .rt
            .block_on(async { self.inner.index_rebuild_status().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(tasks
            .into_iter()
            .map(|t| IndexRebuildTask {
                task_id: t.id,
                label: t.label,
                status: format!("{:?}", t.status),
                error: t.error,
            })
            .collect())
    }

    /// Retry failed index rebuild tasks.
    fn retry_index_rebuilds(&self) -> PyResult<Vec<String>> {
        self.rt
            .block_on(async { self.inner.retry_index_rebuilds().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Check if an index is currently being rebuilt.
    fn is_index_building(&self, label: &str) -> PyResult<bool> {
        self.rt
            .block_on(async { self.inner.is_index_building(label).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    // ========================================================================
    // Snapshot Methods
    // ========================================================================

    /// Create a point-in-time snapshot.
    #[pyo3(signature = (name=None))]
    fn create_snapshot(&self, name: Option<&str>) -> PyResult<String> {
        self.rt
            .block_on(async { self.inner.create_snapshot(name).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Create a named snapshot that can be retrieved later.
    fn create_named_snapshot(&self, name: &str) -> PyResult<String> {
        self.rt
            .block_on(async { self.inner.create_named_snapshot(name).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// List all available snapshots.
    fn list_snapshots(&self) -> PyResult<Vec<SnapshotInfo>> {
        let manifests = self
            .rt
            .block_on(async { self.inner.list_snapshots().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(manifests
            .into_iter()
            .map(|m| {
                let vertex_count: u64 = m.vertices.values().map(|v| v.count).sum();
                let edge_count: u64 = m.edges.values().map(|e| e.count).sum();
                SnapshotInfo {
                    snapshot_id: m.snapshot_id,
                    name: m.name,
                    created_at: m.created_at.to_rfc3339(),
                    vertex_count,
                    edge_count,
                }
            })
            .collect())
    }

    /// Restore the database to a specific snapshot.
    fn restore_snapshot(&self, snapshot_id: &str) -> PyResult<()> {
        self.rt
            .block_on(async { self.inner.restore_snapshot(snapshot_id).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Open a read-only view at a specific snapshot.
    fn at_snapshot(&self, snapshot_id: &str) -> PyResult<Database> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create runtime: {}",
                    e
                ))
            })?;

        let uni = rt
            .block_on(async { self.inner.at_snapshot(snapshot_id).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Database {
            inner: Arc::new(uni),
            rt,
        })
    }

    /// Open a read-only view at a named snapshot.
    fn open_named_snapshot(&self, name: &str) -> PyResult<Database> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create runtime: {}",
                    e
                ))
            })?;

        let uni = rt
            .block_on(async { self.inner.open_named_snapshot(name).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Database {
            inner: Arc::new(uni),
            rt,
        })
    }

    // ========================================================================
    // Session Methods
    // ========================================================================

    /// Create a session builder for scoped query context.
    fn session(&self) -> SessionBuilder {
        SessionBuilder {
            inner: self.inner.clone(),
            rt: self.rt.handle().clone(),
            variables: HashMap::new(),
        }
    }

    // ========================================================================
    // Bulk Loading Methods
    // ========================================================================

    /// Create a bulk writer builder for high-throughput loading.
    fn bulk_writer(&self) -> BulkWriterBuilder {
        BulkWriterBuilder {
            inner: self.inner.clone(),
            rt: self.rt.handle().clone(),
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: 10_000,
            async_indexes: false,
        }
    }

    /// Bulk insert vertices (legacy API, prefer bulk_writer()).
    fn bulk_insert_vertices(
        &self,
        py: Python,
        label: &str,
        properties_list: Vec<HashMap<String, PyObject>>,
    ) -> PyResult<Vec<u64>> {
        let mut rust_props = Vec::new();
        for p in properties_list {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_props.push(map);
        }

        let vids = self
            .rt
            .block_on(async {
                self.inner
                    .bulk_insert_vertices(label, rust_props)
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(vids.into_iter().map(|v| v.as_u64()).collect())
    }

    /// Bulk insert edges (legacy API, prefer bulk_writer()).
    fn bulk_insert_edges(
        &self,
        py: Python,
        edge_type: &str,
        edges: Vec<(u64, u64, HashMap<String, PyObject>)>,
    ) -> PyResult<()> {
        let mut rust_edges = Vec::new();
        for (src, dst, p) in edges {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_edges.push((::uni_db::Vid::from(src), ::uni_db::Vid::from(dst), map));
        }

        self.rt
            .block_on(async {
                self.inner
                    .bulk_insert_edges(edge_type, rust_edges)
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(())
    }

    // ========================================================================
    // Vector Search Methods
    // ========================================================================

    /// Perform vector similarity search.
    fn vector_search(
        &self,
        label: &str,
        property: &str,
        query_vec: Vec<f32>,
        k: usize,
    ) -> PyResult<Vec<VectorMatch>> {
        let matches = self
            .rt
            .block_on(async {
                self.inner
                    .vector_search(label, property, &query_vec, k)
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(matches
            .into_iter()
            .map(|m| VectorMatch {
                vid: m.vid.as_u64(),
                distance: m.distance,
            })
            .collect())
    }

    /// Create a vector search builder for advanced options.
    fn vector_search_with(
        &self,
        label: &str,
        property: &str,
        query_vec: Vec<f32>,
    ) -> VectorSearchBuilder {
        VectorSearchBuilder {
            inner: self.inner.clone(),
            rt: self.rt.handle().clone(),
            label: label.to_string(),
            property: property.to_string(),
            query_vec,
            k: 10,
            threshold: None,
            filter_expr: None,
        }
    }

    // ========================================================================
    // Compaction Methods
    // ========================================================================

    /// Trigger compaction for a label.
    fn compact_label(&self, label: &str) -> PyResult<CompactionStats> {
        let stats = self
            .rt
            .block_on(async { self.inner.compact_label(label).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(CompactionStats {
            files_compacted: stats.files_compacted,
            bytes_before: stats.bytes_before,
            bytes_after: stats.bytes_after,
            duration_secs: stats.duration.as_secs_f64(),
            crdt_merges: stats.crdt_merges,
        })
    }

    /// Trigger compaction for an edge type.
    fn compact_edge_type(&self, edge_type: &str) -> PyResult<CompactionStats> {
        let stats = self
            .rt
            .block_on(async { self.inner.compact_edge_type(edge_type).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(CompactionStats {
            files_compacted: stats.files_compacted,
            bytes_before: stats.bytes_before,
            bytes_after: stats.bytes_after,
            duration_secs: stats.duration.as_secs_f64(),
            crdt_merges: stats.crdt_merges,
        })
    }

    /// Wait for any ongoing compaction to complete.
    fn wait_for_compaction(&self) -> PyResult<()> {
        self.rt
            .block_on(async { self.inner.wait_for_compaction().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    // ========================================================================
    // UID Methods
    // ========================================================================

    /// Create a VID from label_id and offset.
    fn make_vid(&self, label_id: u16, offset: u64) -> u64 {
        ::uni_db::Vid::new(label_id, offset).as_u64()
    }

    /// Insert a vertex with a specific UID.
    fn insert_vertex_with_uid(&self, label: &str, vid: u64, uid: Vec<u8>) -> PyResult<()> {
        if uid.len() != 32 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "UID must be 32 bytes",
            ));
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&uid);
        let uni_id = ::uni_db::UniId::from_bytes(bytes);

        self.rt
            .block_on(async {
                self.inner
                    .storage()
                    .insert_vertex_with_uid(label, ::uni_db::Vid::from(vid), uni_id)
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    /// Get a vertex by its UID.
    fn get_vertex_by_uid(&self, uid: Vec<u8>, label: &str) -> PyResult<Option<u64>> {
        if uid.len() != 32 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "UID must be 32 bytes",
            ));
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&uid);
        let uni_id = ::uni_db::UniId::from_bytes(bytes);

        let vid = self
            .rt
            .block_on(async {
                self.inner
                    .storage()
                    .get_vertex_by_uid(&uni_id, label)
                    .await
                    .map_err(|e| e.to_string())
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(vid.map(|v| v.as_u64()))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Parse a data type string into a DataType enum.
fn parse_data_type(data_type: &str) -> PyResult<DataType> {
    if data_type.starts_with("vector:") {
        let dims = data_type
            .split(':')
            .nth(1)
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Vector type must specify dimensions, e.g., 'vector:128'",
                )
            })?
            .parse::<usize>()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid dimensions for vector type",
                )
            })?;
        Ok(DataType::Vector { dimensions: dims })
    } else {
        match data_type.to_lowercase().as_str() {
            "string" => Ok(DataType::String),
            "int64" | "int" => Ok(DataType::Int64),
            "int32" => Ok(DataType::Int32),
            "float64" | "float" => Ok(DataType::Float64),
            "float32" => Ok(DataType::Float32),
            "bool" => Ok(DataType::Bool),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unknown data type: {}",
                data_type
            ))),
        }
    }
}

/// Create an index definition from parameters.
fn create_index_definition(
    label: &str,
    property: &str,
    index_type: &str,
) -> PyResult<IndexDefinition> {
    match index_type.to_lowercase().as_str() {
        "btree" | "scalar" => Ok(IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
        })),
        "hash" => Ok(IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            index_type: ScalarIndexType::Hash,
            where_clause: None,
        })),
        "vector" => Ok(IndexDefinition::Vector(VectorIndexConfig {
            name: format!("idx_{}_{}_vec", label, property),
            label: label.to_string(),
            property: property.to_string(),
            index_type: VectorIndexType::Hnsw {
                m: 16,
                ef_construction: 200,
                ef_search: 50,
            },
            metric: DistanceMetric::Cosine,
            embedding_config: None,
        })),
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unknown index type: {}",
            index_type
        ))),
    }
}

/// Convert a Uni Value to a Python object.
fn value_to_py(py: Python, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_py_any(py)?),
        Value::Int(i) => Ok(i.into_py_any(py)?),
        Value::Float(f) => Ok(f.into_py_any(py)?),
        Value::String(s) => Ok(s.into_py_any(py)?),
        Value::Bytes(b) => Ok(PyBytes::new(py, b).into()),
        Value::List(l) => {
            let list = PyList::empty(py);
            for item in l {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        Value::Map(m) => {
            let dict = PyDict::new(py);
            for (k, v) in m {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Vector(v) => Ok(v.clone().into_py_any(py)?),
        Value::Node(n) => {
            let dict = PyDict::new(py);
            dict.set_item("_id", n.vid.to_string())?;
            dict.set_item("_label", &n.label)?;
            for (k, v) in &n.properties {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Edge(e) => {
            let dict = PyDict::new(py);
            dict.set_item("_id", e.eid.as_u64())?;
            dict.set_item("_type", &e.edge_type)?;
            dict.set_item("_src", e.src.to_string())?;
            dict.set_item("_dst", e.dst.to_string())?;
            for (k, v) in &e.properties {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Path(p) => {
            let dict = PyDict::new(py);
            let nodes = PyList::empty(py);
            for n in p.nodes() {
                nodes.append(value_to_py(py, &Value::Node(n.clone()))?)?;
            }
            dict.set_item("nodes", nodes)?;

            let edges = PyList::empty(py);
            for e in p.edges() {
                edges.append(value_to_py(py, &Value::Edge(e.clone()))?)?;
            }
            dict.set_item("edges", edges)?;
            Ok(dict.into())
        }
        _ => Ok(py.None()),
    }
}

/// Convert a Python object to a serde_json::Value.
fn py_object_to_json(py: Python, obj: &PyObject) -> PyResult<serde_json::Value> {
    if obj.is_none(py) {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(b) = obj.extract::<bool>(py) {
        return Ok(serde_json::Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>(py) {
        return Ok(serde_json::json!(i));
    }
    if let Ok(f) = obj.extract::<f64>(py) {
        return Ok(serde_json::json!(f));
    }
    if let Ok(s) = obj.extract::<String>(py) {
        return Ok(serde_json::Value::String(s));
    }

    let bound = obj.bind(py);
    if let Ok(l) = bound.downcast::<PyList>() {
        let mut vec = Vec::new();
        for item in l {
            vec.push(py_object_to_json(py, &item.into())?);
        }
        return Ok(serde_json::Value::Array(vec));
    }

    if let Ok(d) = bound.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in d {
            let key = k.extract::<String>()?;
            let val = py_object_to_json(py, &v.into())?;
            map.insert(key, val);
        }
        return Ok(serde_json::Value::Object(map));
    }

    Ok(serde_json::Value::Null)
}

/// Convert a serde_json::Value to a Python object.
fn json_value_to_py(py: Python, val: &serde_json::Value) -> PyResult<PyObject> {
    match val {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py).unwrap().into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py).unwrap().into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py).unwrap().into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_value_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
    }
}

/// Convert a Python object to a Uni Value.
fn py_object_to_value(py: Python, obj: &PyObject) -> PyResult<Value> {
    if obj.is_none(py) {
        return Ok(Value::Null);
    }

    // Check types in order of specificity
    if let Ok(b) = obj.extract::<bool>(py) {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>(py) {
        return Ok(Value::Int(i));
    }
    if let Ok(f) = obj.extract::<f64>(py) {
        return Ok(Value::Float(f));
    }
    if let Ok(s) = obj.extract::<String>(py) {
        return Ok(Value::String(s));
    }

    let bound = obj.bind(py);
    if let Ok(l) = bound.downcast::<PyList>() {
        let mut vec = Vec::new();
        for item in l {
            vec.push(py_object_to_value(py, &item.into())?);
        }
        return Ok(Value::List(vec));
    }

    if let Ok(d) = bound.downcast::<PyDict>() {
        let mut map = HashMap::new();
        for (k, v) in d {
            let key = k.extract::<String>()?;
            let val = py_object_to_value(py, &v.into())?;
            map.insert(key, val);
        }
        return Ok(Value::Map(map));
    }

    Ok(Value::Null)
}

// ============================================================================
// Module Definition
// ============================================================================

/// Python module for the Uni embedded graph database.
#[pymodule]
fn uni_db(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Main classes
    m.add_class::<Database>()?;
    m.add_class::<DatabaseBuilder>()?;
    m.add_class::<Transaction>()?;

    // Query
    m.add_class::<QueryBuilder>()?;

    // Schema
    m.add_class::<SchemaBuilder>()?;
    m.add_class::<LabelBuilder>()?;
    m.add_class::<EdgeTypeBuilder>()?;

    // Session
    m.add_class::<SessionBuilder>()?;
    m.add_class::<Session>()?;

    // Bulk loading
    m.add_class::<BulkWriterBuilder>()?;
    m.add_class::<BulkWriter>()?;

    // Vector search
    m.add_class::<VectorSearchBuilder>()?;

    // Data classes
    m.add_class::<VectorMatch>()?;
    m.add_class::<SnapshotInfo>()?;
    m.add_class::<LabelInfo>()?;
    m.add_class::<PropertyInfo>()?;
    m.add_class::<IndexInfo>()?;
    m.add_class::<ConstraintInfo>()?;
    m.add_class::<BulkStats>()?;
    m.add_class::<BulkProgress>()?;
    m.add_class::<IndexRebuildTask>()?;
    m.add_class::<CompactionStats>()?;

    Ok(())
}
