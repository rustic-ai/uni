// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::storage::property_builder::PropertyColumnBuilder;
use crate::storage::value_codec::CrdtDecodeMode;
use anyhow::{Result, anyhow};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchIterator, UInt8Array, UInt64Array};
use arrow_schema::{Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use std::sync::Arc;
use uni_common::DataType;
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::Schema;

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Insert = 0,
    Delete = 1,
}

#[derive(Clone, Debug)]
pub struct L1Entry {
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub eid: Eid,
    pub op: Op,
    pub version: u64,
    pub properties: Properties,
}

pub struct DeltaDataset {
    uri: String,
    edge_type: String,
    direction: String, // "fwd" or "bwd"
}

use lance_index::scalar::ScalarIndexParams;
use lance_index::{DatasetIndexExt, IndexType};

impl DeltaDataset {
    pub fn new(base_uri: &str, edge_type: &str, direction: &str) -> Self {
        let uri = format!("{}/deltas/{}_{}", base_uri, edge_type, direction);
        Self {
            uri,
            edge_type: edge_type.to_string(),
            direction: direction.to_string(),
        }
    }

    /// Ensure a scalar index exists on the 'eid' column for efficient point lookups.
    pub async fn ensure_eid_index(&self) -> Result<()> {
        let mut ds = match self.open_latest_raw().await {
            Ok(ds) => ds,
            Err(_) => return Ok(()), // Dataset doesn't exist yet
        };

        let indexes = ds.load_indices().await?;

        if !indexes.iter().any(|idx| idx.name.contains("eid_idx")) {
            log::info!(
                "Creating eid scalar index for edge type '{}'",
                self.edge_type
            );
            if let Err(e) = ds
                .create_index(
                    &["eid"],
                    IndexType::Scalar,
                    Some(format!("{}_eid_idx", self.edge_type)),
                    &ScalarIndexParams::default(),
                    true,
                )
                .await
            {
                log::warn!("Failed to create eid index for '{}': {}", self.edge_type, e);
            }
        }

        Ok(())
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

    pub async fn open_latest(&self) -> Result<Arc<Dataset>> {
        self.open().await
    }

    pub async fn open_latest_raw(&self) -> Result<Dataset> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(ds)
    }

    pub async fn write_run(&self, batch: RecordBatch) -> Result<Arc<Dataset>> {
        let arrow_schema = batch.schema();
        let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), arrow_schema);

        let params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };

        let ds = Dataset::write(Box::new(reader), &self.uri, Some(params)).await?;
        Ok(Arc::new(ds))
    }

    pub fn get_arrow_schema(&self, schema: &Schema) -> Result<Arc<ArrowSchema>> {
        let mut fields = vec![
            Field::new("src_vid", arrow_schema::DataType::UInt64, false),
            Field::new("dst_vid", arrow_schema::DataType::UInt64, false),
            Field::new("eid", arrow_schema::DataType::UInt64, false),
            Field::new("op", arrow_schema::DataType::UInt8, false), // 0=INSERT, 1=DELETE
            Field::new("_version", arrow_schema::DataType::UInt64, false),
        ];

        if let Some(type_props) = schema.properties.get(&self.edge_type) {
            let mut sorted_props: Vec<_> = type_props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            for (name, meta) in sorted_props {
                fields.push(Field::new(name, meta.r#type.to_arrow(), meta.nullable));
            }
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    pub fn build_record_batch(&self, entries: &[L1Entry], schema: &Schema) -> Result<RecordBatch> {
        let arrow_schema = self.get_arrow_schema(schema)?;

        let mut src_vids = Vec::with_capacity(entries.len());
        let mut dst_vids = Vec::with_capacity(entries.len());
        let mut eids = Vec::with_capacity(entries.len());
        let mut ops = Vec::with_capacity(entries.len());
        let mut versions = Vec::with_capacity(entries.len());

        for entry in entries {
            src_vids.push(entry.src_vid.as_u64());
            dst_vids.push(entry.dst_vid.as_u64());
            eids.push(entry.eid.as_u64());
            ops.push(entry.op.clone() as u8);
            versions.push(entry.version);
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(src_vids)),
            Arc::new(UInt64Array::from(dst_vids)),
            Arc::new(UInt64Array::from(eids)),
            Arc::new(UInt8Array::from(ops)),
            Arc::new(UInt64Array::from(versions)),
        ];

        // Build property columns using shared builder
        let prop_columns = PropertyColumnBuilder::new(schema, &self.edge_type, entries.len())
            .build(|i| &entries[i].properties)?;

        columns.extend(prop_columns);

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    pub async fn scan_all(&self, schema: &Schema) -> Result<Vec<L1Entry>> {
        let ds = match self.open_latest().await {
            Ok(ds) => ds,
            Err(_) => return Ok(vec![]),
        };

        // Note: Lance doesn't guarantee global sort on scan unless we explicitly sort.
        // For compaction, we pull everything and sort in memory for now.
        // In a production system, we'd use an external merge sort or Lance's indexing.

        let mut stream = ds.scan().try_into_stream().await?;

        let mut entries = Vec::new();

        while let Some(batch) = stream.try_next().await? {
            // Reusing the parsing logic would be nice, but for now duplicate it to keep it self-contained
            // or refactor parsing into a helper. Let's refactor into a helper.
            let mut batch_entries = self.parse_batch(&batch, schema)?;
            entries.append(&mut batch_entries);
        }

        // Sort by key and then version to ensure correct order of operations
        entries.sort_by(|a, b| {
            let key_a = if self.direction == "fwd" {
                a.src_vid
            } else {
                a.dst_vid
            };
            let key_b = if self.direction == "fwd" {
                b.src_vid
            } else {
                b.dst_vid
            };

            match key_a.cmp(&key_b) {
                std::cmp::Ordering::Equal => a.version.cmp(&b.version),
                other => other,
            }
        });

        Ok(entries)
    }

    fn parse_batch(&self, batch: &RecordBatch, schema: &Schema) -> Result<Vec<L1Entry>> {
        let src_vids = batch
            .column_by_name("src_vid")
            .ok_or(anyhow!("Missing src_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid src_vid type"))?;
        let dst_vids = batch
            .column_by_name("dst_vid")
            .ok_or(anyhow!("Missing dst_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid dst_vid type"))?;
        let eids = batch
            .column_by_name("eid")
            .ok_or(anyhow!("Missing eid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid eid type"))?;
        let ops = batch
            .column_by_name("op")
            .ok_or(anyhow!("Missing op"))?
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or(anyhow!("Invalid op type"))?;
        let versions = batch
            .column_by_name("_version")
            .ok_or(anyhow!("Missing _version"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid _version type"))?;

        // Prepare property columns
        let mut prop_cols = Vec::new();
        if let Some(type_props) = schema.properties.get(&self.edge_type) {
            for (name, meta) in type_props {
                if let Some(col) = batch.column_by_name(name) {
                    prop_cols.push((name, meta.r#type.clone(), col));
                }
            }
        }

        let mut entries = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let op = match ops.value(i) {
                0 => Op::Insert,
                1 => Op::Delete,
                _ => continue, // Unknown op
            };

            let properties = self.extract_properties(&prop_cols, i)?;

            entries.push(L1Entry {
                src_vid: Vid::from(src_vids.value(i)),
                dst_vid: Vid::from(dst_vids.value(i)),
                eid: Eid::from(eids.value(i)),
                op,
                version: versions.value(i),
                properties,
            });
        }
        Ok(entries)
    }

    /// Extract properties from columns for a single row.
    fn extract_properties(
        &self,
        prop_cols: &[(&String, DataType, &ArrayRef)],
        row: usize,
    ) -> Result<Properties> {
        let mut properties = Properties::new();
        for (name, dtype, col) in prop_cols {
            if col.is_null(row) {
                continue;
            }
            let val = Self::value_from_column(col.as_ref(), dtype, row)?;
            properties.insert(name.to_string(), val);
        }
        Ok(properties)
    }

    pub async fn read_deltas(
        &self,
        vid: Vid,
        schema: &Schema,
        version: Option<u64>,
    ) -> Result<Vec<L1Entry>> {
        let ds = match self.open_at(version).await {
            Ok(ds) => ds,
            Err(_) => return Ok(vec![]), // Dataset might not exist yet
        };

        let filter_col = if self.direction == "fwd" {
            "src_vid"
        } else {
            "dst_vid"
        };

        let mut stream = ds
            .scan()
            .filter(&format!("{} = {}", filter_col, vid.as_u64()))?
            .try_into_stream()
            .await?;

        let mut entries = Vec::new();

        while let Some(batch) = stream.try_next().await? {
            let mut batch_entries = self.parse_batch(&batch, schema)?;
            entries.append(&mut batch_entries);
        }

        Ok(entries)
    }

    /// Decode an Arrow column value to JSON with lenient CRDT error handling.
    fn value_from_column(
        col: &dyn arrow_array::Array,
        dtype: &uni_common::DataType,
        row: usize,
    ) -> Result<serde_json::Value> {
        crate::storage::value_codec::value_from_column(col, dtype, row, CrdtDecodeMode::Lenient)
    }
}
