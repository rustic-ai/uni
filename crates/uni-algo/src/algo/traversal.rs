// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Zero-copy graph traversal directly on AdjacencyCache + L0Buffer.
//!
//! For light algorithms that don't need the full projection overhead.

use std::collections::{HashMap, HashSet, VecDeque};
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::l0::L0Buffer;
use uni_store::storage::adjacency_cache::{AdjacencyCache, Direction};

/// Zero-copy traversal on storage primitives.
///
/// Use this for light algorithms like BFS, DFS, or point-to-point shortest path
/// where projection overhead isn't justified.
pub struct DirectTraversal<'a> {
    cache: &'a AdjacencyCache,
    l0: Option<&'a L0Buffer>,
    /// L0 buffers currently being flushed (still visible to reads).
    pending_l0s: Vec<&'a L0Buffer>,
    edge_types: Vec<u16>,
}

impl<'a> DirectTraversal<'a> {
    /// Create a new direct traversal context.
    pub fn new(cache: &'a AdjacencyCache, l0: Option<&'a L0Buffer>, edge_types: Vec<u16>) -> Self {
        Self {
            cache,
            l0,
            pending_l0s: Vec::new(),
            edge_types,
        }
    }

    /// Create a new direct traversal context with pending flush L0s.
    pub fn new_with_pending(
        cache: &'a AdjacencyCache,
        l0: Option<&'a L0Buffer>,
        pending_l0s: Vec<&'a L0Buffer>,
        edge_types: Vec<u16>,
    ) -> Self {
        Self {
            cache,
            l0,
            pending_l0s,
            edge_types,
        }
    }

    /// Iterate neighbors of a vertex in the given direction.
    pub fn neighbors(&self, vid: Vid, direction: Direction) -> Vec<(Vid, Eid)> {
        let mut result = Vec::new();

        for &edge_type in &self.edge_types {
            let neighbors = self.cache.get_neighbors_with_l0s(
                vid,
                edge_type,
                direction,
                self.l0,
                &self.pending_l0s,
            );
            result.extend(neighbors);
        }

        result
    }

    /// BFS from a source vertex.
    pub fn bfs(&self, source: Vid, direction: Direction) -> BfsIterator<'_> {
        BfsIterator::new(self, source, direction)
    }

    /// Find shortest path between source and target using bidirectional BFS.
    ///
    /// Returns the path as a sequence of VIDs and EIDs, or None if no path exists.
    pub fn shortest_path(&self, source: Vid, target: Vid, direction: Direction) -> Option<Path> {
        if source == target {
            return Some(Path {
                vertices: vec![source],
                edges: Vec::new(),
            });
        }

        // Forward search state: vid -> (parent_vid, edge_to_parent)
        let mut forward_visited: HashMap<Vid, (Vid, Eid)> = HashMap::default();
        let mut forward_frontier: VecDeque<Vid> = VecDeque::new();

        // Backward search state: vid -> (child_vid, edge_from_child)
        let mut backward_visited: HashMap<Vid, (Vid, Eid)> = HashMap::default();
        let mut backward_frontier: VecDeque<Vid> = VecDeque::new();

        forward_frontier.push_back(source);
        // Note: we don't put source in visited yet because we need a parent to track the edge.
        // Actually, for BFS start nodes, they have no parent.
        // Let's use a special marker or just handle them separately.

        backward_frontier.push_back(target);

        // Determine directions
        let fwd_dir = direction;
        let bwd_dir = match direction {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
            Direction::Both => Direction::Both,
        };

        // Alternating BFS
        while !forward_frontier.is_empty() || !backward_frontier.is_empty() {
            // Expand forward
            if !forward_frontier.is_empty()
                && (backward_frontier.is_empty()
                    || forward_frontier.len() <= backward_frontier.len())
                && let Some((meeting, edge)) = self.expand_frontier_bidirectional(
                    &mut forward_frontier,
                    &mut forward_visited,
                    &backward_visited,
                    target,
                    fwd_dir,
                )
            {
                return Some(self.reconstruct_path_full(
                    meeting,
                    edge,
                    source,
                    target,
                    &forward_visited,
                    &backward_visited,
                ));
            }

            // Expand backward
            if !backward_frontier.is_empty()
                && let Some((meeting, edge)) = self.expand_frontier_bidirectional(
                    &mut backward_frontier,
                    &mut backward_visited,
                    &forward_visited,
                    source,
                    bwd_dir,
                )
            {
                // Note: direction is flipped for backward search
                return Some(self.reconstruct_path_full(
                    meeting,
                    edge,
                    source,
                    target,
                    &forward_visited,
                    &backward_visited,
                ));
            }
        }

        None
    }

    fn expand_frontier_bidirectional(
        &self,
        frontier: &mut VecDeque<Vid>,
        visited: &mut HashMap<Vid, (Vid, Eid)>,
        other_visited: &HashMap<Vid, (Vid, Eid)>,
        other_root: Vid,
        direction: Direction,
    ) -> Option<(Vid, Eid)> {
        let level_size = frontier.len();

        for _ in 0..level_size {
            let current = frontier.pop_front()?;

            for (neighbor, eid) in self.neighbors(current, direction) {
                if visited.contains_key(&neighbor)
                    || neighbor == source_of(frontier, current, visited)
                {
                    continue;
                }

                visited.insert(neighbor, (current, eid));

                // Check for intersection
                if other_visited.contains_key(&neighbor) || neighbor == other_root {
                    return Some((neighbor, eid));
                }

                frontier.push_back(neighbor);
            }
        }

        None
    }

    fn reconstruct_path_full(
        &self,
        meeting: Vid,
        _meeting_edge: Eid,
        source: Vid,
        target: Vid,
        forward_visited: &HashMap<Vid, (Vid, Eid)>,
        backward_visited: &HashMap<Vid, (Vid, Eid)>,
    ) -> Path {
        let mut vertices = Vec::new();
        let mut edges = Vec::new();

        // 1. Forward part: source -> ... -> meeting_parent
        let _current = meeting;
        // If meeting is not source, it must have been found via an edge in forward search
        if let Some(&(parent, edge)) = forward_visited.get(&meeting) {
            let mut f_v = vec![meeting];
            let mut f_e = vec![edge];
            let mut curr = parent;
            while curr != source {
                let &(p, e) = forward_visited.get(&curr).unwrap();
                f_v.push(curr);
                f_e.push(e);
                curr = p;
            }
            f_v.push(source);
            f_v.reverse();
            f_e.reverse();
            vertices.extend(f_v);
            edges.extend(f_e);
        } else {
            // Meeting is source
            vertices.push(source);
        }

        // 2. Backward part: meeting -> ... -> target
        if meeting != target {
            // If meeting was reached from forward search, it might not be in backward_visited yet
            // but we have meeting_edge from forward search?
            // Wait, bidirectional BFS meeting logic is:
            // Forward search finds neighbor N. If N is in backward_visited, meeting = N.

            let mut b_v = Vec::new();
            let mut b_e = Vec::new();

            let mut curr = meeting;
            while curr != target {
                let &(child, edge) = backward_visited.get(&curr).unwrap();
                b_v.push(child);
                b_e.push(edge);
                curr = child;
            }

            // vertices already contains meeting from forward part
            vertices.extend(b_v);
            edges.extend(b_e);
        }

        Path { vertices, edges }
    }
}

fn source_of(_frontier: &VecDeque<Vid>, current: Vid, visited: &HashMap<Vid, (Vid, Eid)>) -> Vid {
    // Helper to find the start of the search (source or target)
    // This is a bit inefficient, but works for correctly identifying roots in bidirectional BFS
    let mut curr = current;
    while let Some(&(parent, _)) = visited.get(&curr) {
        curr = parent;
    }
    curr
}

/// BFS iterator yielding (vid, distance) pairs.
pub struct BfsIterator<'a> {
    traversal: &'a DirectTraversal<'a>,
    frontier: VecDeque<(Vid, u32)>,
    visited: HashSet<Vid>,
    direction: Direction,
}

impl<'a> BfsIterator<'a> {
    fn new(traversal: &'a DirectTraversal<'a>, source: Vid, direction: Direction) -> Self {
        let mut frontier = VecDeque::new();
        let mut visited = HashSet::default();

        frontier.push_back((source, 0));
        visited.insert(source);

        Self {
            traversal,
            frontier,
            visited,
            direction,
        }
    }
}

impl Iterator for BfsIterator<'_> {
    type Item = (Vid, u32);

    fn next(&mut self) -> Option<Self::Item> {
        let (current, distance) = self.frontier.pop_front()?;

        // Enqueue neighbors
        for (neighbor, _eid) in self.traversal.neighbors(current, self.direction) {
            if self.visited.insert(neighbor) {
                self.frontier.push_back((neighbor, distance + 1));
            }
        }

        Some((current, distance))
    }
}

/// Path representation for shortest path results.
#[derive(Debug, Clone)]
pub struct Path {
    /// Vertices in the path (source to target)
    pub vertices: Vec<Vid>,
    /// Edges in the path
    pub edges: Vec<Eid>,
}

impl Path {
    /// Length of the path (number of edges)
    pub fn len(&self) -> usize {
        self.edges.len()
    }

    /// Whether the path is empty (source == target)
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full tests require mocking AdjacencyCache
    // These are placeholder tests for the data structures

    #[test]
    fn test_path_length() {
        let path = Path {
            vertices: vec![Vid::new(1, 0), Vid::new(1, 1), Vid::new(1, 2)],
            edges: vec![Eid::new(1, 0), Eid::new(1, 1)],
        };

        assert_eq!(path.len(), 2);
        assert!(!path.is_empty());
    }
}
