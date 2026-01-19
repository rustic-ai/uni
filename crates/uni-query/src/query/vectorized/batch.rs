// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{Field, Schema as ArrowSchema};
use std::collections::HashMap;
use std::sync::Arc;

/// A batch of data in the vectorized execution engine.
/// Represents a set of rows (bound variables) flowing through the pipeline.
#[derive(Clone, Debug)]
pub struct VectorizedBatch {
    /// Underlying Arrow RecordBatch containing the data columns.
    pub data: RecordBatch,

    /// Optional selection mask.
    /// If Some, only rows where the corresponding boolean is true are active.
    /// This allows for efficient filtering without rewriting the RecordBatch immediately.
    pub selection: Option<BooleanArray>,

    /// Mapping from Cypher variable names (e.g., "n", "a.name") to column indices in `data`.
    pub variables: HashMap<String, usize>,
}

impl VectorizedBatch {
    pub fn new(data: RecordBatch, variables: HashMap<String, usize>) -> Self {
        Self {
            data,
            selection: None,
            variables,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    /// Returns the number of active rows (considering selection).
    pub fn active_rows(&self) -> usize {
        match &self.selection {
            Some(mask) => mask.true_count(),
            None => self.data.num_rows(),
        }
    }

    /// Returns the estimated memory usage of this batch in bytes.
    pub fn get_memory_usage(&self) -> usize {
        let mut total = 0;
        for col in self.data.columns() {
            total += col.get_buffer_memory_size();
        }
        if let Some(selection) = &self.selection {
            total += selection.get_buffer_memory_size();
        }
        // Multiply by 2 to account for some overhead and ensure we don't underestimate
        total * 2
    }

    pub fn compact(&self) -> Result<Self> {
        if self.selection.is_none() {
            return Ok(self.clone());
        }

        let selection = self.selection.as_ref().unwrap();
        let mut indices_builder = arrow_array::builder::UInt64Builder::new();
        for i in 0..selection.len() {
            if selection.value(i) {
                indices_builder.append_value(i as u64);
            }
        }
        let indices = indices_builder.finish();

        let mut columns = Vec::new();
        for col in self.data.columns() {
            columns.push(arrow::compute::take(col.as_ref(), &indices, None)?);
        }

        let data = RecordBatch::try_new(self.data.schema(), columns)?;
        Ok(Self {
            data,
            selection: None,
            variables: self.variables.clone(),
        })
    }

    pub fn column(&self, variable: &str) -> Result<&Arc<dyn Array>> {
        let idx = self
            .variables
            .get(variable)
            .ok_or_else(|| anyhow::anyhow!("Variable '{}' not found in batch", variable))?;
        Ok(self.data.column(*idx))
    }

    pub fn add_column(&mut self, name: String, array: Arc<dyn Array>) -> Result<()> {
        let mut columns = self.data.columns().to_vec();
        columns.push(array);

        let mut fields = self.data.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            &name,
            columns.last().unwrap().data_type().clone(),
            true,
        )));

        let schema = Arc::new(ArrowSchema::new(fields));
        self.data = RecordBatch::try_new(schema, columns)?;
        self.variables.insert(name, self.data.num_columns() - 1);
        Ok(())
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let data = self.data.slice(offset, length);
        let selection = self.selection.as_ref().map(|s| {
            // Slice boolean array
            let sliced = s.slice(offset, length);
            sliced
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .clone()
        });

        Self {
            data,
            selection,
            variables: self.variables.clone(),
        }
    }

    pub fn concat(batches: &[VectorizedBatch]) -> Result<Self> {
        if batches.is_empty() {
            return Err(anyhow::anyhow!("Cannot concat empty batches"));
        }
        let first = &batches[0];
        let schema = first.data.schema();

        let record_batches: Vec<&RecordBatch> = batches.iter().map(|b| &b.data).collect();
        let data = arrow::compute::concat_batches(&schema, record_batches.iter().cloned())?;

        // Concat selections if any
        // If some have selection and some don't, we treat None as "All True"
        let has_selection = batches.iter().any(|b| b.selection.is_some());
        let selection = if has_selection {
            let mut builder = arrow_array::builder::BooleanBuilder::with_capacity(data.num_rows());
            for b in batches {
                if let Some(s) = &b.selection {
                    for i in 0..s.len() {
                        builder.append_value(s.value(i));
                    }
                } else {
                    builder.append_slice(&vec![true; b.num_rows()]);
                }
            }
            Some(builder.finish())
        } else {
            None
        };

        Ok(Self {
            data,
            selection,
            variables: first.variables.clone(),
        })
    }
}
