// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::storage::property_builder::PropertyColumnBuilder;
use anyhow::{Result, anyhow};
use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{Field, Schema as ArrowSchema};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_index::scalar::ScalarIndexParams;
use lance_index::{DatasetIndexExt, IndexType};
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::Vid;
use uni_common::core::schema::Schema;

pub struct VertexDataset {
    uri: String,
    label: String,
    #[allow(dead_code)]
    label_id: u16,
}

impl VertexDataset {
    pub fn new(base_uri: &str, label: &str, label_id: u16) -> Self {
        let uri = format!("{}/vertices/{}", base_uri, label);
        Self {
            uri,
            label: label.to_string(),
            label_id,
        }
    }

    pub async fn open(&self) -> Result<Arc<Dataset>> {
        self.open_at(None).await
    }

    pub async fn open_at(&self, version: Option<u64>) -> Result<Arc<Dataset>> {
        let mut ds = Dataset::open(&self.uri).await?;
        if let Some(v) = version {
            ds = ds.checkout_version(v).await?;
        }
        Ok(Arc::new(ds))
    }

    pub async fn open_raw(&self) -> Result<Dataset> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(ds)
    }

    pub async fn write_batch(&self, batch: RecordBatch, mode: WriteMode) -> Result<Arc<Dataset>> {
        let arrow_schema = batch.schema();
        let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), arrow_schema);

        let params = WriteParams {
            mode,
            ..Default::default()
        };

        let ds = Dataset::write(Box::new(reader), &self.uri, Some(params)).await?;
        Ok(Arc::new(ds))
    }

    /// Ensure default scalar indexes exist on system columns (_vid, _uid).
    /// Called automatically after every flush to maintain index consistency.
    pub async fn ensure_default_indexes(&self) -> Result<()> {
        let mut ds = match self.open_raw().await {
            Ok(ds) => ds,
            Err(_) => return Ok(()), // Dataset doesn't exist yet
        };

        let indexes = ds.load_indices().await?;

        // Ensure _vid index for efficient two-phase scans (O(k) lookups)
        if !indexes.iter().any(|idx| idx.name.contains("_vid")) {
            log::info!("Creating _vid scalar index for label '{}'", self.label);
            if let Err(e) = ds
                .create_index(
                    &["_vid"],
                    IndexType::Scalar,
                    Some(format!("{}_vid_idx", self.label)),
                    &ScalarIndexParams::default(),
                    true,
                )
                .await
            {
                log::warn!("Failed to create _vid index for '{}': {}", self.label, e);
            }
        }

        // Ensure _uid index for efficient UniId lookups
        if !indexes.iter().any(|idx| idx.name.contains("_uid")) {
            log::info!("Creating _uid scalar index for label '{}'", self.label);
            if let Err(e) = ds
                .create_index(
                    &["_uid"],
                    IndexType::Scalar,
                    Some(format!("{}_uid_idx", self.label)),
                    &ScalarIndexParams::default(),
                    true,
                )
                .await
            {
                log::warn!("Failed to create _uid index for '{}': {}", self.label, e);
            }
        }

        Ok(())
    }

    /// Ensure a scalar index exists on _vid for efficient two-phase scans.
    /// This enables O(k) lookups when fetching all versions of k candidate VIDs.
    #[deprecated(since = "0.2.0", note = "Use ensure_default_indexes() instead")]
    pub async fn ensure_vid_index(&self) -> Result<()> {
        self.ensure_default_indexes().await
    }

    pub fn build_record_batch(
        &self,
        vertices: &[(Vid, Properties)],
        deleted: &[bool],
        versions: &[u64],
        schema: &Schema,
    ) -> Result<RecordBatch> {
        let arrow_schema = self.get_arrow_schema(schema)?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        let vids: Vec<u64> = vertices.iter().map(|(v, _)| v.as_u64()).collect();
        columns.push(Arc::new(UInt64Array::from(vids)));

        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        for _ in 0..vertices.len() {
            uid_builder.append_null();
        }
        columns.push(Arc::new(uid_builder.finish()));

        columns.push(Arc::new(BooleanArray::from(deleted.to_vec())));
        columns.push(Arc::new(UInt64Array::from(versions.to_vec())));

        // Build property columns using shared builder
        let prop_columns = PropertyColumnBuilder::new(schema, &self.label, vertices.len())
            .with_deleted(deleted)
            .build(|i| &vertices[i].1)?;

        columns.extend(prop_columns);

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    pub fn get_arrow_schema(&self, schema: &Schema) -> Result<Arc<ArrowSchema>> {
        let mut fields = vec![
            Field::new("_vid", arrow_schema::DataType::UInt64, false),
            Field::new("_uid", arrow_schema::DataType::FixedSizeBinary(32), true),
            Field::new("_deleted", arrow_schema::DataType::Boolean, false),
            Field::new("_version", arrow_schema::DataType::UInt64, false),
        ];

        if let Some(label_props) = schema.properties.get(&self.label) {
            let mut sorted_props: Vec<_> = label_props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            for (name, meta) in sorted_props {
                fields.push(Field::new(name, meta.r#type.to_arrow(), meta.nullable));
            }
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }
}
