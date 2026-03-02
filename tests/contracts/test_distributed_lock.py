"""Contract tests for distributed lock implementations.

These tests define the expected behaviour for any distributed lock
implementation. All lock implementations are required to satisfy these
behavioural contracts.

Sync implementations can use the ``SyncToAsyncLockAdapter`` from
``tests.helpers.adapters`` to satisfy the async test interface.

Fixture dependencies:
    - ``async_redis_client``: from ``tests/conftest.py``.
    - ``lock_key``, ``create_lock``: provided by this module (or subclass conftest).
"""

import pytest

from tests.helpers.utils import wait_for_key_expiry

SHORT_TIMEOUT_MS = 500


class DistributedLockContractTest:
    """Abstract test suite that any distributed lock implementation must pass.

    Subclasses are required to provide the following:
        - async_redis_client: An async fixture that returns an async Redis client.
        - lock_key: A fixture that returns a unique lock key for testing.
        - create_lock: A fixture that returns a factory for lock instances
          (returning either native async locks or ``SyncToAsyncLockAdapter``-wrapped
          sync locks).

    The subclass must also define how lock instances are to be created.
    """

    @staticmethod
    async def test_lock_acquires_and_releases_automatically(
        async_redis_client, lock_key, create_lock
    ):
        """Contract: the lock must be acquired upon context entry and released upon context exit."""
        # Arrange
        lock = create_lock(async_redis_client, lock_key, timeout_ms=1000)

        # Act & Assert
        # The lock should be acquired within the context.
        async with lock as acquired:
            assert acquired is True, "lock should be acquired successfully"
            assert await async_redis_client.exists(lock_key) == 1, (
                "lock key must exist in Redis"
            )
            assert await async_redis_client.get(lock_key) == lock.token, (
                "lock must store correct token"
            )

        # Assert
        # The lock should be released after the context is exited.
        assert await async_redis_client.exists(lock_key) == 0, (
            "lock must be released after context exit"
        )

    @staticmethod
    async def test_lock_prevents_concurrent_acquisition(
        async_redis_client, lock_key, create_lock
    ):
        """Contract: a second lock attempt on the same key must fail while the first lock is held."""
        # Arrange
        lock_1 = create_lock(async_redis_client, lock_key, timeout_ms=1000)
        lock_2 = create_lock(async_redis_client, lock_key, timeout_ms=1000)

        # Act & Assert
        async with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire successfully"

            async with lock_2 as acquired_2:
                assert acquired_2 is False, (
                    "second lock must fail due to mutual exclusion"
                )
                assert await async_redis_client.get(lock_key) == lock_1.token, (
                    "original lock must still hold the lock"
                )

    @staticmethod
    async def test_lock_expires_after_timeout(
        async_redis_client, lock_key, create_lock
    ):
        """Contract: the lock must automatically expire after the timeout period elapses."""
        # Arrange
        lock_1 = create_lock(async_redis_client, lock_key, timeout_ms=SHORT_TIMEOUT_MS)
        lock_2 = create_lock(async_redis_client, lock_key, timeout_ms=5000)

        # Act & Assert
        async with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire"

            # Wait for the lock key to expire in Redis.
            await wait_for_key_expiry(async_redis_client, lock_key)

            # The second lock should now succeed.
            async with lock_2 as acquired_2:
                assert acquired_2 is True, (
                    "second lock should acquire after first expires"
                )
                assert await async_redis_client.get(lock_key) == lock_2.token, (
                    "second lock must store its own token"
                )

    @staticmethod
    async def test_lock_releases_on_exception(
        async_redis_client, lock_key, create_lock
    ):
        """Contract: the lock must be released even when an exception occurs within the context."""
        # Arrange
        lock = create_lock(async_redis_client, lock_key, timeout_ms=5000)

        # Act
        with pytest.raises(ValueError, match="Simulated failure"):
            async with lock:
                assert await async_redis_client.exists(lock_key) == 1, (
                    "lock should be acquired"
                )
                raise ValueError("Simulated failure")

        # Assert
        assert await async_redis_client.exists(lock_key) == 0, (
            "lock must be released even after exception"
        )

    @staticmethod
    async def test_lock_only_releases_own_token(
        async_redis_client, lock_key, create_lock
    ):
        """Contract: the lock must not delete another lock's token after its own expiration."""
        # Arrange
        lock_1 = create_lock(async_redis_client, lock_key, timeout_ms=SHORT_TIMEOUT_MS)
        lock_2 = create_lock(async_redis_client, lock_key, timeout_ms=5000)

        # Act & Assert
        async with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire successfully"

            # Wait for the first lock to expire in Redis.
            await wait_for_key_expiry(async_redis_client, lock_key)

            # The second lock acquires the now-expired lock.
            async with lock_2 as acquired_2:
                assert acquired_2 is True, (
                    "second lock should acquire after first expires"
                )
                assert lock_2.token != lock_1.token, (
                    "the two locks must have different tokens"
                )
                assert await async_redis_client.get(lock_key) == lock_2.token, (
                    "second lock must store its own token"
                )

                # Manually exit the first lock, effectively simulating the
                # scenario in which lock_1 completes after its lock has expired.
                await lock_1.__aexit__(None, None, None)

                # Assert that lock_2 still holds the lock.
                assert await async_redis_client.exists(lock_key) == 1, (
                    "lock_2 must still exist after lock_1 cleanup"
                )
                assert await async_redis_client.get(lock_key) == lock_2.token, (
                    "lock_2 token must remain unchanged"
                )

    @staticmethod
    def test_lock_has_unique_token(async_redis_client, lock_key, create_lock):
        """Contract: each lock instance must possess a unique token."""
        # Arrange & Act
        lock_1 = create_lock(async_redis_client, lock_key, timeout_ms=1000)
        lock_2 = create_lock(async_redis_client, lock_key, timeout_ms=1000)

        # Assert
        assert hasattr(lock_1, "token"), "lock must have a token attribute"
        assert hasattr(lock_2, "token"), "lock must have a token attribute"
        assert lock_1.token != lock_2.token, "each lock must have a unique token"
        assert len(lock_1.token) > 0, "token must not be empty"
        assert len(lock_2.token) > 0, "token must not be empty"

    @staticmethod
    async def test_lock_cooldown_prevents_reacquisition_under_contention(
        async_redis_client, lock_key, create_lock
    ):
        """Contract: after contention is detected and cooldown is set, the same worker cannot re-acquire until expiry."""
        # Arrange
        # A long cooldown ensures the key cannot expire between the release
        # and the re-acquire attempt, even under heavy load (e.g., mutmut).
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_ms = 5000

        lock_1 = create_lock(
            async_redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        # A second lock instance from a different worker to create contention.
        lock_contender = create_lock(
            async_redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        # Worker-A acquires, worker-B fails (creating contention), worker-A releases.
        async with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire successfully"

            async with lock_contender as acquired_contender:
                assert acquired_contender is False, (
                    "contender must fail while first lock is held"
                )

        # Assert
        # Worker-A should now be in cooldown and unable to re-acquire.
        lock_retry = create_lock(
            async_redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        async with lock_retry as acquired_retry:
            assert acquired_retry is False, (
                "worker must not re-acquire while in cooldown"
            )

    @staticmethod
    async def test_lock_cooldown_expires_and_allows_reacquisition(
        async_redis_client, lock_key, create_lock
    ):
        """Contract: after the cooldown period expires, the worker can re-acquire the lock."""
        # Arrange
        # A short cooldown keeps the sleep duration minimal.
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_ms = SHORT_TIMEOUT_MS

        lock_1 = create_lock(
            async_redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            async_redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        # Create contention, then release.
        async with lock_1 as acquired_1:
            assert acquired_1 is True, "first lock should acquire successfully"

            async with lock_contender as acquired_contender:
                assert acquired_contender is False, (
                    "contender must fail while first lock is held"
                )

        # Assert
        # Wait for the cooldown key to expire in Redis.
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        await wait_for_key_expiry(async_redis_client, cooldown_key)

        lock_after = create_lock(
            async_redis_client,
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        async with lock_after as acquired_after:
            assert acquired_after is True, (
                "worker must be able to re-acquire after cooldown expires"
            )
