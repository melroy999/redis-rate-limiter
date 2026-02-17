"""Tests for the DistributedLock implementation.

This module tests the Redis-based distributed lock implementation used by
the Celery rate limiter. It inherits the contract tests and adds
implementation-specific tests.
"""

import time

import pytest

from celery_rate_limiter import DistributedLock
from tests.contracts.test_distributed_lock import DistributedLockContractTest


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
            assert acquired is True, "lock should be acquired successfully"
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
            assert acquired is True, "lock should be acquired successfully"
            assert redis_client.exists(lock_key) == 1, (
                "lock key must exist in Redis while held"
            )
            ttl = redis_client.pttl(lock_key)
            assert 0 < ttl <= 1000, f"TTL should be set and <= 1000ms, got {ttl}ms"

    # ==================== Contention-Aware Cooldown Tests ====================

    @staticmethod
    def test_cooldown_not_set_without_contention(redis_client, lock_key, create_lock):
        """Implementation detail: no cooldown key should be created when there is no contention."""
        # Arrange
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        lock = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=200,
            contention_key=contention_key,
        )

        # Act
        # Single worker acquires and releases without any contention.
        with lock as acquired:
            assert acquired is True, "lock should be acquired successfully"

        # Assert
        # No cooldown key should exist (burst behavior preserved).
        assert redis_client.exists(cooldown_key) == 0, (
            "cooldown key must not be created when no contention is detected"
        )

    @staticmethod
    def test_cooldown_set_when_contention_detected(redis_client, lock_key, create_lock):
        """Implementation detail: a cooldown key must be created when contention is detected during the hold period."""
        # Arrange
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        cooldown_ms = 200

        lock_holder = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        # Worker-A holds the lock while worker-B fails to acquire (creating contention).
        with lock_holder as acquired_holder:
            assert acquired_holder is True, "holder should acquire successfully"

            with lock_contender as acquired_contender:
                assert acquired_contender is False, (
                    "contender must fail while holder has the lock"
                )

        # Assert
        # After release, worker-A should have a cooldown key set.
        assert redis_client.exists(cooldown_key) == 1, (
            "cooldown key must be created when contention was detected"
        )
        assert redis_client.get(cooldown_key) == "1", "cooldown key value must be '1'"

    @staticmethod
    def test_cooldown_does_not_affect_other_workers(
        redis_client, lock_key, create_lock
    ):
        """Implementation detail: a cooldown on one worker must not prevent other workers from acquiring."""
        # Arrange
        contention_key = f"{lock_key}:contention"
        cooldown_ms = 500

        lock_a = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-A",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Create contention so worker-A gets a cooldown.
        with lock_a:
            with lock_contender:
                pass

        # Assert
        # Worker-A is in cooldown.
        assert redis_client.exists(f"{lock_key}:cd:worker-A") == 1, (
            "worker-A should be in cooldown after contention"
        )

        # Act
        # Worker-B should be able to acquire the lock despite worker-A's cooldown.
        lock_b = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        with lock_b as acquired_b:
            assert acquired_b is True, (
                "worker-B must be able to acquire while worker-A is in cooldown"
            )

    @staticmethod
    def test_cooldown_key_has_correct_ttl(redis_client, lock_key, create_lock):
        """Implementation detail: the cooldown key TTL must match the configured cooldown_ms."""
        # Arrange
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        cooldown_ms = 500

        lock_holder = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        # Create contention and release.
        with lock_holder:
            with lock_contender:
                pass

        # Assert
        # The cooldown key should have a TTL close to cooldown_ms.
        ttl = redis_client.pttl(cooldown_key)
        assert 0 < ttl <= cooldown_ms, (
            f"cooldown key TTL must be in (0, {cooldown_ms}], got {ttl}ms"
        )

    @staticmethod
    def test_cooldown_expires_allowing_reacquisition(
        redis_client, lock_key, create_lock
    ):
        """Implementation detail: the worker must be able to re-acquire after the cooldown TTL expires."""
        # Arrange
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_ms = 50

        lock_holder = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Create contention and release to trigger cooldown.
        with lock_holder:
            with lock_contender:
                pass

        # Verify the cooldown is active.
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        assert redis_client.exists(cooldown_key) == 1, (
            "cooldown key must exist before expiry wait"
        )

        # Act
        # Wait for the cooldown to expire and attempt re-acquisition.
        time.sleep(cooldown_ms / 1000 + 0.05)
        lock_retry = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        with lock_retry as acquired:
            assert acquired is True, (
                "worker must be able to re-acquire after cooldown expires"
            )

    @staticmethod
    def test_contention_counter_reset_on_release(redis_client, lock_key, create_lock):
        """Implementation detail: the contention counter must be deleted when cooldown is set on release."""
        # Arrange
        contention_key = f"{lock_key}:contention"
        cooldown_ms = 200

        lock_holder = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-A",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        # Create contention and release.
        with lock_holder:
            with lock_contender:
                pass
            # While the lock is still held, contention should be recorded.
            assert int(redis_client.get(contention_key) or 0) > 0, (
                "contention counter must be > 0 while lock is held after failed acquisition"
            )

        # Assert
        # After release, the contention counter should be cleared.
        assert redis_client.exists(contention_key) == 0, (
            "contention counter must be deleted when cooldown is set on release"
        )

    @staticmethod
    def test_contention_counter_auto_expires(redis_client, lock_key, create_lock):
        """Implementation detail: the contention counter must have a TTL to prevent stale state."""
        # Arrange
        contention_key = f"{lock_key}:contention"
        timeout_ms = 100
        cooldown_ms = 200

        lock_holder = create_lock(
            redis_client,
            lock_key,
            timeout_ms=timeout_ms,
            worker_id="worker-A",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            redis_client,
            lock_key,
            timeout_ms=timeout_ms,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        # Create contention (do not release the lock; let it and the counter expire).
        with lock_holder:
            with lock_contender:
                pass

            # Verify the contention counter exists and has a TTL.
            assert redis_client.exists(contention_key) == 1, (
                "contention counter must exist after failed acquisition"
            )
            ttl = redis_client.pttl(contention_key)
            assert 0 < ttl <= timeout_ms, (
                f"contention counter TTL must be in (0, {timeout_ms}], got {ttl}ms"
            )
