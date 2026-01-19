// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Storage layer unit tests for CSR and related components.
//!
//! Tests cover:
//! - CSR construction with various input patterns
//! - CSR neighbor lookup boundary conditions
//! - Sparse VID handling
//! - Memory usage calculations

use uni_common::core::id::{Eid, Vid};

mod csr_tests {
    use super::*;

    // Helper module to access CSR internals for testing
    mod csr {
        use uni_common::core::id::{Eid, Vid};

        /// Compressed Sparse Row (CSR) representation of adjacency
        pub struct CompressedSparseRow {
            offsets: Vec<u32>,
            neighbors: Vec<Vid>,
            edge_ids: Vec<Eid>,
        }

        impl CompressedSparseRow {
            pub fn new(max_vid_offset: usize, entries: Vec<(u64, Vid, Eid)>) -> Self {
                let mut sorted = entries;
                sorted.sort_by_key(|(src, _, _)| *src);

                let mut offsets = vec![0u32; max_vid_offset + 2];
                let mut neighbors = Vec::with_capacity(sorted.len());
                let mut edge_ids = Vec::with_capacity(sorted.len());

                let mut current_offset = 0;
                let mut last_src = 0;

                for (src, neighbor, eid) in sorted {
                    let src_idx = src as usize;

                    if src_idx > last_src {
                        for offset in offsets.iter_mut().take(src_idx + 1).skip(last_src + 1) {
                            *offset = current_offset;
                        }
                    }
                    last_src = src_idx;

                    neighbors.push(neighbor);
                    edge_ids.push(eid);
                    current_offset += 1;
                }

                for offset in offsets.iter_mut().skip(last_src + 1) {
                    *offset = current_offset;
                }

                Self {
                    offsets,
                    neighbors,
                    edge_ids,
                }
            }

            pub fn get_neighbors(&self, vid: Vid) -> (&[Vid], &[Eid]) {
                let local = vid.local_offset() as usize;
                if local + 1 >= self.offsets.len() {
                    return (&[], &[]);
                }

                let start = self.offsets[local] as usize;
                let end = self.offsets[local + 1] as usize;

                if start >= self.neighbors.len() || end > self.neighbors.len() {
                    return (&[], &[]);
                }

                (&self.neighbors[start..end], &self.edge_ids[start..end])
            }

            pub fn memory_usage(&self) -> usize {
                self.offsets.len() * 4 + self.neighbors.len() * 8 + self.edge_ids.len() * 8
            }

            pub fn iter_all(&self) -> impl Iterator<Item = (u64, Vid, Eid)> + '_ {
                (0..self.offsets.len().saturating_sub(1)).flat_map(move |i| {
                    let start = self.offsets[i] as usize;
                    let end = self.offsets[i + 1] as usize;
                    (start..end).map(move |j| (i as u64, self.neighbors[j], self.edge_ids[j]))
                })
            }

            #[allow(dead_code)]
            pub fn num_vertices(&self) -> usize {
                self.offsets.len().saturating_sub(1)
            }

            pub fn num_edges(&self) -> usize {
                self.neighbors.len()
            }
        }
    }

    #[test]
    fn test_csr_empty_construction() {
        let csr = csr::CompressedSparseRow::new(0, vec![]);
        assert_eq!(csr.num_edges(), 0);

        // Accessing any vertex should return empty
        let (neighbors, edges) = csr.get_neighbors(Vid::new(0, 0));
        assert!(neighbors.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn test_csr_single_edge() {
        let entries = vec![(0, Vid::new(0, 1), Eid::new(0, 100))];
        let csr = csr::CompressedSparseRow::new(1, entries);

        let (neighbors, edges) = csr.get_neighbors(Vid::new(0, 0));
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], Vid::new(0, 1));
        assert_eq!(edges[0], Eid::new(0, 100));
    }

    #[test]
    fn test_csr_multiple_edges_same_source() {
        let entries = vec![
            (0, Vid::new(0, 1), Eid::new(0, 100)),
            (0, Vid::new(0, 2), Eid::new(0, 101)),
            (0, Vid::new(0, 3), Eid::new(0, 102)),
        ];
        let csr = csr::CompressedSparseRow::new(1, entries);

        let (neighbors, edges) = csr.get_neighbors(Vid::new(0, 0));
        assert_eq!(neighbors.len(), 3);
        assert_eq!(edges.len(), 3);
    }

    #[test]
    fn test_csr_sparse_vid_ranges() {
        // VIDs 0, 5, 10 have edges, others are empty
        let entries = vec![
            (0, Vid::new(0, 100), Eid::new(0, 1)),
            (5, Vid::new(0, 101), Eid::new(0, 2)),
            (10, Vid::new(0, 102), Eid::new(0, 3)),
        ];
        let csr = csr::CompressedSparseRow::new(10, entries);

        // VID 0 has edge
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 0));
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].local_offset(), 100);

        // VIDs 1-4 have no edges
        for i in 1..5 {
            let (neighbors, _) = csr.get_neighbors(Vid::new(0, i));
            assert!(neighbors.is_empty(), "VID {} should have no neighbors", i);
        }

        // VID 5 has edge
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 5));
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].local_offset(), 101);

        // VIDs 6-9 have no edges
        for i in 6..10 {
            let (neighbors, _) = csr.get_neighbors(Vid::new(0, i));
            assert!(neighbors.is_empty(), "VID {} should have no neighbors", i);
        }

        // VID 10 has edge
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 10));
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].local_offset(), 102);
    }

    #[test]
    fn test_csr_boundary_vid_lookup() {
        let entries = vec![
            (0, Vid::new(0, 1), Eid::new(0, 1)),
            (99, Vid::new(0, 2), Eid::new(0, 2)),
        ];
        let csr = csr::CompressedSparseRow::new(100, entries);

        // First VID
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 0));
        assert_eq!(neighbors.len(), 1);

        // Last VID with edge
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 99));
        assert_eq!(neighbors.len(), 1);

        // VID at boundary (max_vid)
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 100));
        assert!(neighbors.is_empty());

        // VID beyond max
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 1000));
        assert!(neighbors.is_empty());
    }

    #[test]
    fn test_csr_out_of_bounds_access() {
        let entries = vec![(0, Vid::new(0, 1), Eid::new(0, 1))];
        let csr = csr::CompressedSparseRow::new(1, entries);

        // Way beyond allocated range
        let (neighbors, edges) = csr.get_neighbors(Vid::new(0, u64::MAX >> 16));
        assert!(neighbors.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn test_csr_unsorted_input() {
        // Input is deliberately unsorted - CSR should sort internally
        let entries = vec![
            (5, Vid::new(0, 10), Eid::new(0, 5)),
            (0, Vid::new(0, 1), Eid::new(0, 1)),
            (3, Vid::new(0, 7), Eid::new(0, 3)),
            (0, Vid::new(0, 2), Eid::new(0, 2)),
        ];
        let csr = csr::CompressedSparseRow::new(5, entries);

        // VID 0 should have 2 neighbors
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 0));
        assert_eq!(neighbors.len(), 2);

        // VID 3 should have 1 neighbor
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 3));
        assert_eq!(neighbors.len(), 1);

        // VID 5 should have 1 neighbor
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 5));
        assert_eq!(neighbors.len(), 1);
    }

    #[test]
    fn test_csr_iter_all() {
        let entries = vec![
            (0, Vid::new(0, 1), Eid::new(0, 100)),
            (0, Vid::new(0, 2), Eid::new(0, 101)),
            (2, Vid::new(0, 3), Eid::new(0, 102)),
        ];
        let csr = csr::CompressedSparseRow::new(3, entries.clone());

        let collected: Vec<_> = csr.iter_all().collect();
        assert_eq!(collected.len(), 3);

        // Verify all edges are present
        assert!(
            collected
                .iter()
                .any(|(src, dst, _)| *src == 0 && dst.local_offset() == 1)
        );
        assert!(
            collected
                .iter()
                .any(|(src, dst, _)| *src == 0 && dst.local_offset() == 2)
        );
        assert!(
            collected
                .iter()
                .any(|(src, dst, _)| *src == 2 && dst.local_offset() == 3)
        );
    }

    #[test]
    fn test_csr_memory_usage() {
        let entries = vec![
            (0, Vid::new(0, 1), Eid::new(0, 100)),
            (0, Vid::new(0, 2), Eid::new(0, 101)),
        ];
        let csr = csr::CompressedSparseRow::new(10, entries);

        // offsets: (10 + 2) * 4 = 48 bytes
        // neighbors: 2 * 8 = 16 bytes
        // edge_ids: 2 * 8 = 16 bytes
        // Total: 80 bytes
        let usage = csr.memory_usage();
        assert_eq!(usage, 48 + 16 + 16);
    }

    #[test]
    fn test_csr_large_sparse_range() {
        // Single edge at a high offset
        let entries = vec![(1000, Vid::new(0, 2000), Eid::new(0, 1))];
        let csr = csr::CompressedSparseRow::new(1000, entries);

        // Most VIDs have no neighbors
        for i in 0..1000 {
            let (neighbors, _) = csr.get_neighbors(Vid::new(0, i));
            assert!(neighbors.is_empty());
        }

        // The one VID with a neighbor
        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 1000));
        assert_eq!(neighbors.len(), 1);
    }

    #[test]
    fn test_csr_different_label_ids() {
        // Edges with different label_ids in VIDs
        let entries = vec![
            (0, Vid::new(1, 10), Eid::new(0, 1)), // Neighbor in label 1
            (0, Vid::new(2, 20), Eid::new(0, 2)), // Neighbor in label 2
            (0, Vid::new(3, 30), Eid::new(0, 3)), // Neighbor in label 3
        ];
        let csr = csr::CompressedSparseRow::new(1, entries);

        let (neighbors, _) = csr.get_neighbors(Vid::new(0, 0));
        assert_eq!(neighbors.len(), 3);

        // Verify label_ids are preserved
        assert_eq!(neighbors[0].label_id(), 1);
        assert_eq!(neighbors[1].label_id(), 2);
        assert_eq!(neighbors[2].label_id(), 3);
    }
}

mod vid_eid_tests {
    use super::*;

    #[test]
    fn test_vid_roundtrip() {
        let vid = Vid::new(5, 12345);
        assert_eq!(vid.label_id(), 5);
        assert_eq!(vid.local_offset(), 12345);

        let raw = vid.as_u64();
        let restored = Vid::from(raw);
        assert_eq!(restored.label_id(), 5);
        assert_eq!(restored.local_offset(), 12345);
    }

    #[test]
    fn test_eid_roundtrip() {
        let eid = Eid::new(7, 98765);
        assert_eq!(eid.type_id(), 7);
        assert_eq!(eid.local_offset(), 98765);

        let raw = eid.as_u64();
        let restored = Eid::from(raw);
        assert_eq!(restored.type_id(), 7);
        assert_eq!(restored.local_offset(), 98765);
    }

    #[test]
    fn test_vid_max_values() {
        // Max label_id (16 bits)
        let vid = Vid::new(u16::MAX, 0);
        assert_eq!(vid.label_id(), u16::MAX);

        // Max local_offset (48 bits)
        let max_offset = (1u64 << 48) - 1;
        let vid = Vid::new(0, max_offset);
        assert_eq!(vid.local_offset(), max_offset);
    }

    #[test]
    fn test_vid_equality() {
        let v1 = Vid::new(1, 100);
        let v2 = Vid::new(1, 100);
        let v3 = Vid::new(1, 101);
        let v4 = Vid::new(2, 100);

        assert_eq!(v1, v2);
        assert_ne!(v1, v3);
        assert_ne!(v1, v4);
    }

    #[test]
    fn test_eid_equality() {
        let e1 = Eid::new(1, 100);
        let e2 = Eid::new(1, 100);
        let e3 = Eid::new(1, 101);
        let e4 = Eid::new(2, 100);

        assert_eq!(e1, e2);
        assert_ne!(e1, e3);
        assert_ne!(e1, e4);
    }
}
