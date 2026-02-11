"""Tests for the DistributedLock implementation.

This module tests the Redis-based distributed lock implementation used by
the Celery rate limiter. It inherits contract tests and adds implementation-specific tests.
"""

import time

import pytest

from celery_rate_limiter.core.limiters import DistributedLock
from tests.contracts.test_distributed_lock import (
    SHORT_TIMEOUT_MS,
    DistributedLockContractTest,
)


@pytest.fixture
def lock_key(default_lock_key):
    """Provide a unique lock key for testing."""
    return default_lock_key


@pytest.fixture
def create_lock():
    """Factory fixture for creating DistributedLock instances."""
    return DistributedLock


class TestDistributedLock(DistributedLockContractTest):
    """Test Redis-based DistributedLock implementation.

    Inherits all contract tests from DistributedLockContractTest and adds
    implementation-specific tests for the Redis-based lock.
    """

    # ==================== Implementation-Specific Tests ====================

    def test_lock_stores_uuid_token(self, redis_client, lock_key, create_lock):
        """Implementation detail: lock should use UUID as token format."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act
        with lock as acquired:
            assert acquired is True
            token = redis_client.get(lock_key)

            # Assert
            # Token should look like a UUID (has dashes and length).
            assert "-" in token, "token should be UUID format (contains dashes)"
            assert len(token) == 36, "UUID should be 36 characters long"

    def test_lock_uses_redis_set_nx(self, redis_client, lock_key, create_lock):
        """Implementation detail: lock should use Redis SET with NX option."""
        # Arrange
        lock = create_lock(redis_client, lock_key, timeout_ms=1000)

        # Act
        with lock as acquired:
            # Assert
            # Key should exist and have a TTL.
            assert acquired is True
            assert redis_client.exists(lock_key) == 1
            ttl = redis_client.pttl(lock_key)
            assert 0 < ttl <= 1000, f"TTL should be set and <= 1000ms, got {ttl}ms"

    def test_lock_does_not_delete_expired_lock_with_different_token(
        self, redis_client, lock_key, create_lock
    ):
        """Implementation detail: verify Lua script only deletes matching tokens."""
        # Arrange
        lock_1 = create_lock(redis_client, lock_key, timeout_ms=SHORT_TIMEOUT_MS)
        lock_2 = create_lock(redis_client, lock_key, timeout_ms=5000)

        # Act
        with lock_1 as acquired_1:
            assert acquired_1 is True
            original_token = lock_1.token

            # Wait for expiration and lock_2 acquisition.
            time.sleep(2 * SHORT_TIMEOUT_MS / 1000)

            with lock_2 as acquired_2:
                assert acquired_2 is True
                new_token = lock_2.token
                assert new_token != original_token

                # Manually trigger lock_1's exit, effectively simulating
                # its task finishing after lock expiration.
                lock_1.__exit__(None, None, None)

                # Assert lock_2's token is still in Redis.
                assert redis_client.get(lock_key) == new_token, (
                    "lock_1 cleanup should not delete lock_2's token"
                )
                assert redis_client.exists(lock_key) == 1
