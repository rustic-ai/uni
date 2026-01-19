// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::executor::core::OperatorStats;
use crate::query::vectorized::batch::VectorizedBatch;
use crate::query::vectorized::operators::{ExecutionContext, VectorizedOperator};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Instant;

/// A wrapper around a VectorizedOperator that collects runtime statistics.
pub struct ProfilingOperator {
    pub inner: Arc<dyn VectorizedOperator>,
    pub name: String,
    pub stats: Arc<Mutex<OperatorStats>>,
}

#[async_trait]
impl VectorizedOperator for ProfilingOperator {
    async fn execute(
        &self,
        input: Option<VectorizedBatch>,
        ctx: &ExecutionContext,
    ) -> Result<VectorizedBatch> {
        let start = Instant::now();
        let result = self.inner.execute(input, ctx).await;
        let duration = start.elapsed();

        let mut stats = self.stats.lock();
        stats.time_ms += duration.as_secs_f64() * 1000.0;

        if let Ok(batch) = &result {
            stats.actual_rows += batch.num_rows();
            stats.memory_bytes = std::cmp::max(stats.memory_bytes, batch.get_memory_usage());
        }

        result
    }

    fn execute_stream(
        self: Arc<Self>,
        input: Option<VectorizedBatch>,
        ctx: ExecutionContext,
    ) -> BoxStream<'static, Result<VectorizedBatch>> {
        let inner = self.inner.clone();
        let stats = self.stats.clone();

        // Wrap the stream from inner
        let stream = inner.execute_stream(input, ctx);

        // We can't easily measure per-poll time without a custom Stream wrapper,
        // but we can measure time-to-first-byte or total time if we consume it here?
        // But we return a stream.
        // For now, we only count rows in streaming mode.
        // TODO: Implement accurate timing for streams.

        stream
            .map(move |res| {
                let mut stats_guard = stats.lock();
                if let Ok(batch) = &res {
                    stats_guard.actual_rows += batch.num_rows();
                    stats_guard.memory_bytes =
                        std::cmp::max(stats_guard.memory_bytes, batch.get_memory_usage());
                }
                res
            })
            .boxed()
    }

    fn is_pipeline_breaker(&self) -> bool {
        self.inner.is_pipeline_breaker()
    }
}
