# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for Vector Search API."""

import tempfile
import pytest
import uni


class TestVectorSearch:
    """Tests for vector search functionality."""

    @pytest.fixture
    def db(self):
        """Create a database with vector-indexed documents."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Document")
            db.add_property("Document", "title", "string", False)
            db.add_property("Document", "embedding", "vector:3", False)
            db.create_vector_index("Document", "embedding", "l2")

            # Insert test documents with embeddings
            db.execute("CREATE (d:Document {title: 'Doc1', embedding: [1.0, 0.0, 0.0]})")
            db.execute("CREATE (d:Document {title: 'Doc2', embedding: [0.0, 1.0, 0.0]})")
            db.execute("CREATE (d:Document {title: 'Doc3', embedding: [0.0, 0.0, 1.0]})")
            db.execute(
                "CREATE (d:Document {title: 'Doc4', embedding: [0.5, 0.5, 0.0]})"
            )
            db.flush()
            yield db

    def test_basic_vector_search(self, db):
        """Test basic vector similarity search."""
        query = [1.0, 0.0, 0.0]  # Should be closest to Doc1
        matches = db.vector_search("Document", "embedding", query, k=2)

        assert len(matches) == 2
        # First match should be the closest
        assert matches[0].distance < matches[1].distance

    def test_vector_search_builder_k(self, db):
        """Test vector search builder with k parameter."""
        query = [0.5, 0.5, 0.0]
        matches = db.vector_search_with("Document", "embedding", query).k(3).search()

        assert len(matches) == 3

    def test_vector_search_builder_threshold(self, db):
        """Test vector search builder with distance threshold."""
        query = [1.0, 0.0, 0.0]
        # Only get matches within a small distance
        matches = (
            db.vector_search_with("Document", "embedding", query)
            .k(10)
            .threshold(0.1)
            .search()
        )

        # Only Doc1 should be within threshold (exact match)
        assert len(matches) <= 1

    def test_vector_search_fetch_nodes(self, db):
        """Test fetching full nodes from vector search."""
        query = [1.0, 0.0, 0.0]
        results = (
            db.vector_search_with("Document", "embedding", query).k(2).fetch_nodes()
        )

        assert len(results) == 2
        for node, distance in results:
            assert "title" in node
            assert distance >= 0


class TestVectorMatch:
    """Tests for VectorMatch data class."""

    def test_vector_match_attributes(self):
        """Test VectorMatch has expected attributes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Doc")
            db.add_property("Doc", "vec", "vector:2", False)
            db.create_vector_index("Doc", "vec", "l2")
            db.execute("CREATE (d:Doc {vec: [1.0, 0.0]})")
            db.flush()

            matches = db.vector_search("Doc", "vec", [1.0, 0.0], k=1)
            assert len(matches) == 1

            match = matches[0]
            assert hasattr(match, "vid")
            assert hasattr(match, "distance")
            assert isinstance(match.vid, int)
            assert isinstance(match.distance, float)
