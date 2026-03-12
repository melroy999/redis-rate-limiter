"""Tests for ``DistributedLock`` and ``AsyncDistributedLock``
behavior and observability.

Tests are written once in async form using the mixin pattern.
The sync variant participates via ``SyncToAsyncLockAdapter``;
the async variant runs natively.
"""

import inspect
import logging

import pytest

from redis_rate_limiter import DistributedLock
from redis_rate_limiter.core import AsyncDistributedLock
from redis_rate_limiter.core.limiters import AbstractDistributedRateLimiter
from tests.contracts.test_distributed_lock import DistributedLockContractTest
from tests.helpers.adapters import SyncToAsyncLockAdapter
from tests.helpers.utils import assert_log_emitted, wait_for_key_expiry


@pytest.mark.contract
class TestDistributedLock(DistributedLockContractTest):
    """Contract compliance for the Redis-based DistributedLock implementation."""

    @pytest.fixture
    def create_lock(self, redis_client):
        """Override the module-level factory to return async-adapted sync locks.

        The unified contract tests pass ``async_redis_client`` as the first
        argument, but the sync ``DistributedLock`` requires a sync client.
        This factory captures ``redis_client`` from the fixture closure and
        ignores the async client passed by the contract.
        """

        def _factory(client, lock_key, **kwargs):
            return SyncToAsyncLockAdapter(
                DistributedLock(redis_client, lock_key, **kwargs)
            )

        return _factory


# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class DistributedLockImplementationTests:
    """Tests for Redis-specific implementation details of the DistributedLock.

    Subclasses must provide:
        - ``create_lock``: a factory ``(lock_key, **kwargs) -> lock`` that
          returns an async-compatible lock (either adapter-wrapped or native).
    """

    @staticmethod
    async def test_lock_stores_uuid_token(async_redis_client, lock_key, create_lock):
        """Verify that the lock uses a UUID as the token format."""
        # Arrange
        lock = create_lock(lock_key, timeout_ms=1000)

        # Act
        async with lock as acquired:
            assert acquired is True, "lock should be acquired successfully"
            token = await async_redis_client.get(lock_key)

            # Assert
            assert "-" in token, "token should be UUID format (contains dashes)"
            assert len(token) == 36, "UUID should be 36 characters long"

    @staticmethod
    async def test_lock_uses_redis_set_nx(async_redis_client, lock_key, create_lock):
        """Verify that the lock uses the Redis SET command with the NX option."""
        # Arrange
        lock = create_lock(lock_key, timeout_ms=1000)

        # Act
        async with lock as acquired:
            # Assert
            assert acquired is True, "lock should be acquired successfully"
            assert await async_redis_client.exists(lock_key) == 1, (
                "lock key must exist in Redis while held"
            )
            ttl = await async_redis_client.pttl(lock_key)
            assert 0 < ttl <= 1000, f"TTL should be set and <= 1000ms, got {ttl}ms"


# ---------------------------------------------------------------------------
# Unified observability tests
# ---------------------------------------------------------------------------


class DistributedLockObservabilityTests:
    """Observability tests for distributed lock log emissions.

    Subclasses must provide:
        - ``create_lock``: a factory ``(lock_key, **kwargs) -> lock`` that
          returns an async-compatible lock (either adapter-wrapped or native).
    """

    @staticmethod
    async def test_lock_acquisition_emits_debug_log(
        async_redis_client, lock_key, create_lock, caplog
    ):
        """Verify that a successful lock acquisition emits a debug log."""
        # Arrange
        lock = create_lock(lock_key, timeout_ms=1000)

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            async with lock:
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"key={lock_key}",
                "token=",
                "timeout_ms=1000",
                "acquired",
            ],
            message=(
                "should emit a debug log for lock "
                "acquisition with key, token, and timeout_ms"
            ),
        )

    @staticmethod
    async def test_lock_release_emits_debug_log(
        async_redis_client, lock_key, create_lock, caplog
    ):
        """Verify that a successful lock release emits a debug log."""
        # Arrange
        lock = create_lock(lock_key, timeout_ms=1000)

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            async with lock:
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[f"key={lock_key}", "token=", "released"],
            message="should emit a debug log for lock release with key and token",
        )

    @staticmethod
    async def test_lock_contention_emits_debug_log(
        async_redis_client, lock_key, create_lock, caplog
    ):
        """Verify that a contention event emits a debug log
        when the lock is already held."""
        # Arrange
        lock_holder = create_lock(lock_key, timeout_ms=5000)
        lock_contender = create_lock(lock_key, timeout_ms=5000)

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            async with lock_holder as acquired_holder:
                assert acquired_holder is True, "holder should acquire successfully"

                async with lock_contender as acquired_contender:
                    assert acquired_contender is False, (
                        "contender must fail while holder has the lock"
                    )

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[f"key={lock_key}", "contended"],
            message="should emit a debug log for lock contention with key",
        )

    @staticmethod
    async def test_lock_expired_before_release_emits_debug_log(
        async_redis_client, lock_key, create_lock, caplog
    ):
        """Verify that an expired-before-release event emits a debug log."""
        # Arrange
        # Use a short timeout so the lock expires before explicit release.
        lock = create_lock(lock_key, timeout_ms=500)

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            async with lock as acquired:
                assert acquired is True, "lock should be acquired successfully"
                # Wait for the lock key to expire in Redis.
                await wait_for_key_expiry(async_redis_client, lock_key)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[f"key={lock_key}", "expired before release"],
            message="should emit a debug log for lock expired before release with key",
        )


class ContentionAwareCooldownTests:
    """Tests for the contention-aware cooldown mechanism of the DistributedLock.

    Subclasses must provide:
        - ``create_lock``: a factory ``(lock_key, **kwargs) -> lock`` that
          returns an async-compatible lock (either adapter-wrapped or native).
    """

    @staticmethod
    async def test_cooldown_not_set_without_contention(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that no cooldown key is created when there is no contention."""
        # Arrange
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        lock = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=200,
            contention_key=contention_key,
        )

        # Act
        async with lock as acquired:
            assert acquired is True, "lock should be acquired successfully"

        # Assert
        assert await async_redis_client.exists(cooldown_key) == 0, (
            "cooldown key must not be created when no contention is detected"
        )

    @staticmethod
    async def test_cooldown_set_when_contention_detected(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that a cooldown key is created when
        contention is detected during the hold period."""
        # Arrange
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        cooldown_ms = 500

        lock_holder = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        async with lock_holder as acquired_holder:
            assert acquired_holder is True, "holder should acquire successfully"

            async with lock_contender as acquired_contender:
                assert acquired_contender is False, (
                    "contender must fail while holder has the lock"
                )

        # Assert
        assert await async_redis_client.exists(cooldown_key) == 1, (
            "cooldown key must be created when contention was detected"
        )
        assert await async_redis_client.get(cooldown_key) == "1", (
            "cooldown key value must be '1'"
        )

    @staticmethod
    async def test_cooldown_does_not_affect_other_workers(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that a cooldown on one worker does not
        prevent other workers from acquiring."""
        # Arrange
        contention_key = f"{lock_key}:contention"
        cooldown_ms = 500

        lock_a = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-A",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        async with lock_a:
            async with lock_contender:
                pass

        # Assert
        assert await async_redis_client.exists(f"{lock_key}:cd:worker-A") == 1, (
            "worker-A should be in cooldown after contention"
        )

        # Act
        lock_b = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        async with lock_b as acquired_b:
            assert acquired_b is True, (
                "worker-B must be able to acquire while worker-A is in cooldown"
            )

    @staticmethod
    async def test_cooldown_key_has_correct_ttl(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that the cooldown key TTL matches the configured cooldown_ms."""
        # Arrange
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        cooldown_ms = 2000

        lock_holder = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        async with lock_holder:
            async with lock_contender:
                pass

        # Assert
        ttl = await async_redis_client.pttl(cooldown_key)
        assert 0 < ttl <= cooldown_ms, (
            f"cooldown key TTL must be in (0, {cooldown_ms}], got {ttl}ms"
        )

    @staticmethod
    async def test_cooldown_expires_allowing_reacquisition(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that the worker can re-acquire after the cooldown TTL expires."""
        # Arrange
        worker_id = "worker-A"
        contention_key = f"{lock_key}:contention"
        cooldown_ms = 500

        lock_holder = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        async with lock_holder:
            async with lock_contender:
                pass

        cooldown_key = f"{lock_key}:cd:{worker_id}"
        assert await async_redis_client.exists(cooldown_key) == 1, (
            "cooldown key must exist before expiry wait"
        )

        # Act
        await wait_for_key_expiry(async_redis_client, cooldown_key)

        lock_retry = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        async with lock_retry as acquired:
            # Assert
            assert acquired is True, (
                "worker must be able to re-acquire after cooldown expires"
            )

    @staticmethod
    async def test_contention_counter_reset_on_release(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that the contention counter is deleted
        when cooldown is set on release."""
        # Arrange
        contention_key = f"{lock_key}:contention"
        cooldown_ms = 500

        lock_holder = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-A",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        async with lock_holder:
            async with lock_contender:
                pass
            assert int(await async_redis_client.get(contention_key) or 0) == 1, (
                "contention counter must be 1 after "
                "exactly one failed acquisition attempt"
            )

        # Assert
        assert await async_redis_client.exists(contention_key) == 0, (
            "contention counter must be deleted when cooldown is set on release"
        )

    @staticmethod
    async def test_contention_counter_auto_expires(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that the contention counter has a TTL to prevent stale state."""
        # Arrange
        contention_key = f"{lock_key}:contention"
        timeout_ms = 2000
        cooldown_ms = 2000

        lock_holder = create_lock(
            lock_key,
            timeout_ms=timeout_ms,
            worker_id="worker-A",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )
        lock_contender = create_lock(
            lock_key,
            timeout_ms=timeout_ms,
            worker_id="worker-B",
            cooldown_ms=cooldown_ms,
            contention_key=contention_key,
        )

        # Act
        # Create contention (do not release the lock; let it and the counter expire).
        async with lock_holder:
            async with lock_contender:
                pass

            assert await async_redis_client.exists(contention_key) == 1, (
                "contention counter must exist after failed acquisition"
            )
            ttl = await async_redis_client.pttl(contention_key)
            assert 0 < ttl <= timeout_ms, (
                f"contention counter TTL must be in (0, {timeout_ms}], got {ttl}ms"
            )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestSyncDistributedLockImplementation(DistributedLockImplementationTests):
    """Sync DistributedLock implementation exercised through the async adapter."""

    @pytest.fixture
    def create_lock(self, redis_client):
        """Factory that creates sync locks wrapped in the async adapter."""

        def _factory(lock_key, **kwargs):
            return SyncToAsyncLockAdapter(
                DistributedLock(redis_client, lock_key, **kwargs)
            )

        return _factory


@pytest.mark.observability
class TestSyncDistributedLockObservability(DistributedLockObservabilityTests):
    """Sync DistributedLock observability exercised through the async adapter."""

    @pytest.fixture
    def create_lock(self, redis_client):
        """Factory that creates sync locks wrapped in the async adapter."""

        def _factory(lock_key, **kwargs):
            return SyncToAsyncLockAdapter(
                DistributedLock(redis_client, lock_key, **kwargs)
            )

        return _factory


@pytest.mark.behavior
class TestSyncContentionAwareCooldown(ContentionAwareCooldownTests):
    """Sync contention-aware cooldown exercised through the async adapter."""

    @pytest.fixture
    def create_lock(self, redis_client):
        """Factory that creates sync locks wrapped in the async adapter."""

        def _factory(lock_key, **kwargs):
            return SyncToAsyncLockAdapter(
                DistributedLock(redis_client, lock_key, **kwargs)
            )

        return _factory


@pytest.mark.behavior
class TestAsyncDistributedLockImplementation(DistributedLockImplementationTests):
    """Async DistributedLock implementation exercised natively."""

    @pytest.fixture
    def create_lock(self, async_redis_client):
        """Factory that creates native async locks."""

        def _factory(lock_key, **kwargs):
            return AsyncDistributedLock(async_redis_client, lock_key, **kwargs)

        return _factory


@pytest.mark.observability
class TestAsyncDistributedLockObservability(DistributedLockObservabilityTests):
    """Async DistributedLock observability exercised natively."""

    @pytest.fixture
    def create_lock(self, async_redis_client):
        """Factory that creates native async locks."""

        def _factory(lock_key, **kwargs):
            return AsyncDistributedLock(async_redis_client, lock_key, **kwargs)

        return _factory


@pytest.mark.behavior
class TestAsyncContentionAwareCooldown(ContentionAwareCooldownTests):
    """Async contention-aware cooldown exercised natively."""

    @pytest.fixture
    def create_lock(self, async_redis_client):
        """Factory that creates native async locks."""

        def _factory(lock_key, **kwargs):
            return AsyncDistributedLock(async_redis_client, lock_key, **kwargs)

        return _factory


# ---------------------------------------------------------------------------
# Boundary tests
# ---------------------------------------------------------------------------


class DistributedLockBoundaryTests:
    """Boundary condition tests for ``DistributedLock`` initialization.

    Verifies that the fairness mechanism remains inactive when any
    required parameter is missing, even under contention. Subclasses
    must provide a ``create_lock`` factory.
    """

    @staticmethod
    async def test_no_cooldown_under_contention_when_worker_id_empty(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that no cooldown key is created under contention
        when ``worker_id`` is empty.

        Mutation target: ``worker_id`` guard in
        ``DistributedLock._fairness_enabled``.
        """
        # Arrange
        contention_key = f"{lock_key}:contention"
        holder = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="",
            cooldown_ms=200,
            contention_key=contention_key,
        )
        contender = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="",
            cooldown_ms=200,
            contention_key=contention_key,
        )

        # Act
        async with holder as acquired_holder:
            assert acquired_holder is True, "holder should acquire successfully"
            async with contender as acquired_contender:
                assert acquired_contender is False, (
                    "contender must fail while holder has the lock"
                )

        # Assert
        assert await async_redis_client.exists(contention_key) == 0, (
            "contention counter must not be incremented when worker_id is empty"
        )

    @staticmethod
    async def test_no_cooldown_under_contention_when_cooldown_ms_zero(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that no cooldown key is created under contention
        when ``cooldown_ms`` is zero."""
        # Arrange
        contention_key = f"{lock_key}:contention"
        worker_id = "worker-X"
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        holder = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=0,
            contention_key=contention_key,
        )
        contender = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-Y",
            cooldown_ms=0,
            contention_key=contention_key,
        )

        # Act
        async with holder as acquired_holder:
            assert acquired_holder is True, "holder should acquire successfully"
            async with contender as acquired_contender:
                assert acquired_contender is False, (
                    "contender must fail while holder has the lock"
                )

        # Assert
        assert await async_redis_client.exists(cooldown_key) == 0, (
            "cooldown key must not be created when cooldown_ms is zero"
        )
        assert await async_redis_client.exists(contention_key) == 0, (
            "contention counter must not be incremented when cooldown_ms is zero"
        )

    @staticmethod
    async def test_no_cooldown_under_contention_when_contention_key_empty(
        async_redis_client, lock_key, create_lock
    ):
        """Verify that no cooldown key is created under contention
        when ``contention_key`` is empty.

        Mutation target: ``contention_key`` guard in
        ``DistributedLock._fairness_enabled``.
        """
        # Arrange
        worker_id = "worker-X"
        cooldown_key = f"{lock_key}:cd:{worker_id}"
        holder = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id=worker_id,
            cooldown_ms=200,
            contention_key="",
        )
        contender = create_lock(
            lock_key,
            timeout_ms=5000,
            worker_id="worker-Y",
            cooldown_ms=200,
            contention_key="",
        )

        # Act
        async with holder as acquired_holder:
            assert acquired_holder is True, "holder should acquire successfully"
            async with contender as acquired_contender:
                assert acquired_contender is False, (
                    "contender must fail while holder has the lock"
                )

        # Assert
        assert await async_redis_client.exists(cooldown_key) == 0, (
            "cooldown key must not be created when contention_key is empty"
        )

    @staticmethod
    async def test_acquired_is_false_before_any_acquisition_attempt(
        lock_key, create_lock
    ):
        """Verify that a newly created lock reports ``acquired``
        as ``False`` before entering the context.

        Mutation target: ``self.acquired = False`` in
        ``DistributedLock.__init__``.
        """
        # Arrange & Act
        lock = create_lock(lock_key, timeout_ms=1000)

        # Assert
        assert lock.acquired is False, (
            "acquired must be False before any acquisition attempt"
        )


@pytest.mark.behavior
class TestSyncDistributedLockBoundary(DistributedLockBoundaryTests):
    """Sync lock boundary conditions via the async adapter."""

    @pytest.fixture
    def create_lock(self, redis_client):
        """Factory that creates sync locks wrapped in the async adapter."""

        def _factory(lock_key, **kwargs):
            return SyncToAsyncLockAdapter(
                DistributedLock(redis_client, lock_key, **kwargs)
            )

        return _factory


@pytest.mark.behavior
class TestAsyncDistributedLockBoundary(DistributedLockBoundaryTests):
    """Async lock boundary conditions exercised natively."""

    @pytest.fixture
    def create_lock(self, async_redis_client):
        """Factory that creates native async locks."""

        def _factory(lock_key, **kwargs):
            return AsyncDistributedLock(async_redis_client, lock_key, **kwargs)

        return _factory


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestDistributedLockSignatures:
    """Signature tests for distributed lock default parameter values."""

    @staticmethod
    @pytest.mark.parametrize(
        "cls",
        [DistributedLock, AsyncDistributedLock],
        ids=["sync", "async"],
    )
    def test_lock_init_default_parameters(cls):
        """Verify that ``worker_id``, ``cooldown_ms``, and
        ``contention_key`` have the expected defaults.

        Mutation target: ``worker_id``, ``cooldown_ms``,
        and ``contention_key`` default values in
        ``DistributedLock.__init__`` and
        ``AsyncDistributedLock.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(cls.__init__)

        # Assert
        assert sig.parameters["worker_id"].default == "", (
            "worker_id default must be an empty string"
        )
        assert sig.parameters["cooldown_ms"].default == 0, (
            "cooldown_ms default must be 0"
        )
        assert sig.parameters["contention_key"].default == "", (
            "contention_key default must be an empty string"
        )

    @staticmethod
    def test_execution_lock_timeout_ms_defaults_to_5000():
        """Verify that the ``timeout_ms`` parameter defaults to ``5000``.

        Mutation target: ``timeout_ms`` default value in
        ``AbstractDistributedRateLimiter.execution_lock``.
        """
        # Arrange & Act
        sig = inspect.signature(AbstractDistributedRateLimiter.execution_lock)

        # Assert
        assert sig.parameters["timeout_ms"].default == 5000, (
            "timeout_ms default must be 5000"
        )
