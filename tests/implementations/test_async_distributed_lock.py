"""Tests for the ``AsyncDistributedLock`` contract compliance.

This module provides the async concrete subclass of ``DistributedLockContractTest``.
Implementation-specific tests (behavioral and observability) are in
``test_distributed_lock.py``, which covers both sync and async variants via the
mixin pattern.

Fixture dependencies:
    - ``async_redis_client``, ``lock_key``: from ``tests/conftest.py``.
"""

import pytest

from redis_rate_limiter.core import AsyncDistributedLock
from tests.contracts.test_distributed_lock import DistributedLockContractTest


@pytest.fixture
def create_lock():
    """Factory fixture for creating ``AsyncDistributedLock`` instances."""
    return AsyncDistributedLock


class TestAsyncDistributedLock(DistributedLockContractTest):
    """Contract compliance for the async Redis-based ``AsyncDistributedLock`` implementation."""

    pass
