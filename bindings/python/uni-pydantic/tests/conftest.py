# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Pytest configuration and fixtures for uni-pydantic tests."""

import sys
import tempfile
from pathlib import Path

import pytest


# Add parent directory to path for development testing
parent_path = Path(__file__).parent.parent.parent
if str(parent_path) not in sys.path:
    sys.path.insert(0, str(parent_path))


@pytest.fixture
def temp_db_path():
    """Create a temporary directory for database storage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def db(temp_db_path):
    """Create a temporary database instance."""
    try:
        import uni_db

        return uni_db.DatabaseBuilder.open(temp_db_path).build()
    except ImportError:
        pytest.skip("uni_db not available")


@pytest.fixture
def session(db):
    """Create a UniSession with a temporary database."""
    from uni_pydantic import UniSession

    return UniSession(db)
