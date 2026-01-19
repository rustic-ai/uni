// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod adjacency;
pub mod adjacency_cache;
pub mod arrow_convert;
pub mod compaction;
pub mod csr;
pub mod delta;
pub mod edge;
pub mod index;
pub mod index_manager;
pub mod index_rebuild;
pub mod inverted_index;
pub mod json_index;
pub mod manager;
pub mod property_builder;
pub mod resilient_store;
pub mod value_codec;
pub mod vertex;

pub use adjacency::AdjacencyDataset;
pub use adjacency_cache::AdjacencyCache;
pub use csr::CompressedSparseRow;
pub use delta::DeltaDataset;
pub use edge::EdgeDataset;
pub use index::UidIndex;
pub use index_manager::{IndexManager, IndexRebuildStatus, IndexRebuildTask};
pub use index_rebuild::IndexRebuildManager;
pub use inverted_index::InvertedIndex;
pub use json_index::JsonPathIndex;
pub use manager::StorageManager;
pub use resilient_store::ResilientObjectStore;
pub use vertex::VertexDataset;
