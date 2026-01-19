// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use arrow_array::{ListArray, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use std::sync::Arc;
use uni_common::core::id::{Eid, Vid};

pub struct AdjacencyDataset {
    uri: String,
}

impl AdjacencyDataset {
    pub fn new(base_uri: &str, edge_type: &str, label: &str, direction: &str) -> Self {
        let uri = format!(
            "{}/adjacency/{}_{}_{}",
            base_uri, direction, edge_type, label
        );
        Self { uri }
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

    pub async fn write_chunk(&self, batch: RecordBatch, mode: WriteMode) -> Result<Arc<Dataset>> {
        let arrow_schema = batch.schema();
        let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), arrow_schema);

        let params = WriteParams {
            mode,
            ..Default::default()
        };

        let ds = Dataset::write(Box::new(reader), &self.uri, Some(params)).await?;
        Ok(Arc::new(ds))
    }

    pub fn get_arrow_schema(&self) -> Arc<ArrowSchema> {
        let fields = vec![
            Field::new("src_vid", ArrowDataType::UInt64, false),
            // neighbors: list<uint64>
            Field::new(
                "neighbors",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::UInt64, true))),
                false,
            ),
            // edge_ids: list<uint64>
            Field::new(
                "edge_ids",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::UInt64, true))),
                false,
            ),
        ];

        Arc::new(ArrowSchema::new(fields))
    }

    pub async fn read_adjacency(&self, vid: Vid) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
        self.read_adjacency_at(vid, None).await
    }

    pub async fn read_adjacency_at(
        &self,
        vid: Vid,
        version: Option<u64>,
    ) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
        if let Ok(ds) = self.open_at(version).await {
            let mut stream = ds
                .scan()
                .filter(&format!("src_vid = {}", vid.as_u64()))?
                .try_into_stream()
                .await?;

            if let Some(batch) = stream.try_next().await? {
                // Assuming one row per vid (which is true for AdjacencyDataset by design)
                if batch.num_rows() > 0 {
                    let neighbors_list = batch
                        .column_by_name("neighbors")
                        .ok_or(anyhow!("Missing neighbors"))?
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or(anyhow!("Invalid neighbors type"))?;
                    let edge_ids_list = batch
                        .column_by_name("edge_ids")
                        .ok_or(anyhow!("Missing edge_ids"))?
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or(anyhow!("Invalid edge_ids type"))?;

                    let neighbors_array = neighbors_list.value(0); // Row 0
                    let neighbors_uint64 = neighbors_array
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or(anyhow!("Invalid neighbors inner type"))?;

                    let edge_ids_array = edge_ids_list.value(0); // Row 0
                    let edge_ids_uint64 = edge_ids_array
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or(anyhow!("Invalid edge_ids inner type"))?;

                    let mut neighbors = Vec::with_capacity(neighbors_uint64.len());
                    let mut eids = Vec::with_capacity(edge_ids_uint64.len());

                    for i in 0..neighbors_uint64.len() {
                        neighbors.push(Vid::from(neighbors_uint64.value(i)));
                        eids.push(Eid::from(edge_ids_uint64.value(i)));
                    }

                    return Ok(Some((neighbors, eids)));
                }
            }
        }
        Ok(None)
    }
}
