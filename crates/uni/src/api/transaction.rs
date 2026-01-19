// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use futures::future::BoxFuture;
use metrics;
use std::time::Instant;
use tracing::{error, info, instrument, warn};
use uni_common::{Result, UniError};
use uni_query::{ExecuteResult, QueryResult};
use uuid::Uuid;

use crate::api::Uni;

/// A database transaction.
///
/// Transactions provide ACID guarantees for multiple operations.
/// Changes are isolated until commit.
///
/// # Isolation Level
///
/// Uni uses Snapshot Isolation. Reads see a consistent snapshot of the database
/// at the start of the transaction. Writes are buffered and applied atomically on commit.
///
/// # Concurrency
///
/// Only one write transaction is active at a time (Single Writer).
/// Read-only transactions can run concurrently.
pub struct Transaction<'a> {
    db: &'a Uni,
    completed: bool,
    id: String,
    start_time: Instant,
}

impl<'a> Transaction<'a> {
    pub(crate) async fn new(db: &'a Uni) -> Result<Self> {
        let writer_lock = db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "start_transaction".to_string(),
        })?;
        let mut writer = writer_lock.write().await;
        writer.begin_transaction()?;
        let id = Uuid::new_v4().to_string();
        info!(transaction_id = %id, "Transaction started");
        Ok(Self {
            db,
            completed: false,
            id,
            start_time: Instant::now(),
        })
    }

    /// Execute a Cypher query within the transaction.
    ///
    /// # Arguments
    ///
    /// * `cypher` - The Cypher query string.
    ///
    /// # Returns
    ///
    /// A [`QueryResult`] containing rows and columns.
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.db
            .execute_internal(cypher, std::collections::HashMap::new())
            .await
    }

    /// Execute a Cypher query that doesn't return rows (e.g. CREATE, DELETE).
    ///
    /// # Arguments
    ///
    /// * `cypher` - The Cypher query string.
    ///
    /// # Returns
    ///
    /// An [`ExecuteResult`] with statistics on affected rows.
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        let result = self.query(cypher).await?;
        Ok(ExecuteResult {
            affected_rows: result.len(),
        })
    }

    /// Commit the transaction.
    ///
    /// Persists all changes made during the transaction.
    /// If commit fails, the transaction is rolled back.
    #[instrument(skip(self), fields(transaction_id = %self.id, duration_ms), level = "info")]
    pub async fn commit(mut self) -> Result<()> {
        if self.completed {
            return Err(uni_common::UniError::TransactionAlreadyCompleted);
        }
        let writer_lock = self.db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "commit".to_string(),
        })?;
        let mut writer = writer_lock.write().await;
        writer.commit_transaction().await?;
        self.completed = true;
        let duration = self.start_time.elapsed();
        tracing::Span::current().record("duration_ms", duration.as_millis());
        metrics::histogram!("uni_transaction_duration_seconds").record(duration.as_secs_f64());
        metrics::counter!("uni_transaction_commits_total").increment(1);
        info!("Transaction committed");
        Ok(())
    }

    /// Rollback the transaction.
    ///
    /// Discards all changes made during the transaction.
    #[instrument(skip(self), fields(transaction_id = %self.id, duration_ms), level = "info")]
    pub async fn rollback(mut self) -> Result<()> {
        if self.completed {
            return Err(uni_common::UniError::TransactionAlreadyCompleted);
        }
        let writer_lock = self.db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "rollback".to_string(),
        })?;
        let mut writer = writer_lock.write().await;
        writer.rollback_transaction()?;
        self.completed = true;
        let duration = self.start_time.elapsed();
        tracing::Span::current().record("duration_ms", duration.as_millis());
        metrics::histogram!("uni_transaction_duration_seconds").record(duration.as_secs_f64());
        metrics::counter!("uni_transaction_rollbacks_total").increment(1);
        info!("Transaction rolled back");
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.completed {
            warn!(transaction_id = %self.id, "Transaction dropped without commit or rollback. This may leave the database writer in a locked transaction state.");
        }
    }
}

impl Uni {
    pub async fn begin(&self) -> Result<Transaction<'_>> {
        Transaction::new(self).await
    }

    pub async fn transaction<'a, F, T>(&'a self, f: F) -> Result<T>
    where
        F: for<'b> FnOnce(&'b mut Transaction<'a>) -> BoxFuture<'b, Result<T>>,
    {
        let mut tx = self.begin().await?;

        match f(&mut tx).await {
            Ok(v) => match tx.commit().await {
                Ok(_) => Ok(v),
                Err(uni_common::UniError::TransactionAlreadyCompleted) => Ok(v),
                Err(e) => Err(e),
            },
            Err(e) => {
                // Ignore rollback error if it fails, but log it
                if let Err(rollback_err) = tx.rollback().await {
                    error!(
                        "Transaction rollback failed during error recovery: {}",
                        rollback_err
                    );
                }
                Err(e)
            }
        }
    }
}
