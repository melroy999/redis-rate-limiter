"""Shared fixtures for property-based tests."""

import pytest


@pytest.fixture(scope="module")
def property_redis_client(_redis_connection):
    """Provide a module-scoped Redis client for property-based tests."""
    yield _redis_connection
