"""Tests for the ``AsyncDistributedLock`` implementation.

This module tests the async Redis-based distributed lock implementation.
It inherits the async contract tests and adds implementation-specific tests.
"""

import pytest

from celery_rate_limiter.core import AsyncDistributedLock
from tests.contracts.test_distributed_lock import DistributedLockContractTest


@pytest.fixture
def create_lock():
    """Factory fixture for creating ``AsyncDistributedLock`` instances."""
    return AsyncDistributedLock


class TestAsyncDistributedLock(DistributedLockContractTest):
    """Tests for the async Redis-based ``AsyncDistributedLock`` implementation.

    This class inherits all contract tests from ``DistributedLockContractTest``
    and adds implementation-specific tests for the async Redis-based lock.
    """

    pass
