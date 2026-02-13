"""Tests for the DistributedLock implementation.

This module tests the Redis-based distributed lock implementation used by
the Celery rate limiter. It inherits the contract tests and adds
implementation-specific tests.
"""

import time

import pytest

from celery_rate_limiter import DistributedLock
from tests.contracts.test_distributed_lock import (
    SHORT_TIMEOUT_MS,
    DistributedLockContractTest,
)


@pytest.fixture
def lock_key(default_lock_key):
    """Provide a unique lock key for the test."""
    return default_lock_key


@pytest.fixture
def create_lock():
    """Factory fixture for creating DistributedLock instances."""
    return DistributedLock


class TestDistributedLock(DistributedLockContractTest):
    """Tests for the Redis-based DistributedLock implementation.

    This class inherits all contract tests from DistributedLockContractTest
    and adds implementation-specific tests for the Redis-based lock.
    """

    # ==================== Implementation-Specific Tests ====================

    def test_lock_stores_uuid_token(self, redis_client, lock_key, create_lock):
        """Implementation detail: the lock should use a UUID as the token format."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act
        with lock as acquired:
            assert acquired is True
            token = redis_client.get(lock_key)

            # Assert
            # The token should conform to UUID format (i.e., it contains dashes and has the expected length).
            assert "-" in token, "token should be UUID format (contains dashes)"
            assert len(token) == 36, "UUID should be 36 characters long"

    def test_lock_uses_redis_set_nx(self, redis_client, lock_key, create_lock):
        """Implementation detail: the lock should use the Redis SET command with the NX option."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act
        with lock as acquired:
            # Assert
            # The key should exist and have a TTL assigned.
            assert acquired is True
            assert redis_client.exists(lock_key) == 1
            ttl = redis_client.pttl(lock_key)
            assert 0 < ttl <= 1000, f"TTL should be set and <= 1000ms, got {ttl}ms"

    def test_lock_does_not_delete_expired_lock_with_different_token(
        self, redis_client, lock_key, create_lock
    ):
        """Implementation detail: the Lua release script should only delete locks with a matching token."""
        # Arrange
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=SHORT_TIMEOUT_MS)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=5000)

        # Act
        with lock_1 as acquired_1:
            assert acquired_1 is True
            original_token = lock_1.token

            # Wait for expiration, then allow lock_2 to acquire.
            time.sleep(2 * SHORT_TIMEOUT_MS / 1000)

            with lock_2 as acquired_2:
                assert acquired_2 is True
                new_token = lock_2.token
                assert new_token != original_token

                # Manually trigger lock_1's exit to simulate the scenario
                # where its task finishes after the lock has already expired.
                lock_1.__exit__(None, None, None)

                # Assert that lock_2's token remains intact in Redis.
                assert redis_client.get(lock_key) == new_token, (
                    "lock_1 cleanup should not delete lock_2's token"
                )
                assert redis_client.exists(lock_key) == 1
