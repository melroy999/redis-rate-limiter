"""Safety-net tests for the async limiter's persistent loops.

Async mirror of ``test_loop_safety_net.py``. See that file's docstring
and ``TESTING_GUIDELINES.md`` Section 5.4 for the hang-vs-spin distinction
and the conventions that determine which tests live here versus in their
behavioral home file.

Fixture dependencies:
    - ``async_redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from redis_rate_limiter.core.async_limiters import (
    AbstractAsyncDistributedRateLimiter,
    AsyncBackendHealthMonitor,
    AsyncDrainLoop,
    AsyncDrainSignalSubscriber,
    AsyncHeartbeatScheduler,
)
from tests.helpers.utils import (
    async_shutdown_completes_within,
    cap_iterations,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _NoopDispatchAsyncLimiter(AbstractAsyncDistributedRateLimiter):
    """Concrete async limiter with no-op dispatch for drain-integration safety-net tests."""

    async def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        pass


_DRAIN_INTEGRATION_CONFIG: dict = dict(
    limit=5,
    window=1.0,
    max_concurrency=2,
    max_age=3600,
    lease_duration=30,
    drain_enabled=True,
)


# Mocked consume result that drives the rate-limited reschedule branch.
_RATE_LIMITED_RESULT: dict = {
    "success": False,
    "expired": False,
    "task": None,
    "remaining_tokens": 0,
    "active_concurrency": 0,
    "reset_in_ms": 100,
    "remaining_tasks": 1,
    "val_previous": 0,
    "val_current": 100,
}


@asynccontextmanager
async def _bypassed_async_execution_lock():
    """Yield ``True`` without touching Redis, so the lock acquire/release does not amplify mutations."""
    yield True


@pytest.fixture
async def real_drain_async_limiter(async_redis_client, limiter_id):
    """Provide an async limiter with an active drain loop and no-op dispatch."""
    # Setup
    limiter = _NoopDispatchAsyncLimiter(
        redis_client=async_redis_client,
        limiter_id=f"{limiter_id}_loop_safety",
        **_DRAIN_INTEGRATION_CONFIG,
    )
    await limiter.start()

    yield limiter

    # Teardown
    await limiter.shutdown()


# ---------------------------------------------------------------------------
# AsyncDrainLoop
# ---------------------------------------------------------------------------


class TestAsyncDrainLoopSafetyNet:
    """Hang and spin coverage for ``AsyncDrainLoop._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_shutdown_completes_promptly():
        """Verify that ``shutdown()`` completes well within its internal 5.0s timeout."""
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Start the task and let it complete one drain cycle so it is
        # blocked on _condition.wait() when shutdown is called.
        loop.wake(0)
        await asyncio.wait_for(drain_called.wait(), timeout=2.0)

        # Act
        completed = await async_shutdown_completes_within(loop, timeout=1.0)

        # Assert
        assert completed, (
            "shutdown() should complete within 1.0s; "
            "a timeout indicates notify() or _shutdown assignment was mutated"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_run_throttles_iterations_under_watchdog():
        """Verify that ``_run`` waits the watchdog interval between drain calls when no wake is pending."""
        # Arrange
        limiter = MagicMock()
        limiter.drain = AsyncMock(return_value=None)
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        with cap_iterations(limiter, "drain", return_value=None) as count:
            # Act
            loop.wake(0)
            await asyncio.sleep(0.5)
            observed = count()
            await loop.shutdown()

        # Assert
        assert observed < 50, (
            f"async drain loop fired {observed} times in 0.5s with a 60s watchdog; "
            "the watchdog interval clamp has been bypassed"
        )


# ---------------------------------------------------------------------------
# AsyncDrainSignalSubscriber
# ---------------------------------------------------------------------------


class TestAsyncDrainSignalSubscriberSafetyNet:
    """Spin coverage for ``AsyncDrainSignalSubscriber._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_run_throttles_iterations_on_empty_pubsub():
        """Verify that ``_run`` honours its 0.5s poll timeout when no message arrives."""

        # Arrange
        async def _sleep_for_requested_timeout(*args, timeout=0.0, **kwargs):
            await asyncio.sleep(timeout)
            return None

        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        limiter.redis = MagicMock()
        limiter.redis.pubsub = MagicMock(return_value=MagicMock())
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = MagicMock()
        mock_pubsub.get_message = AsyncMock(side_effect=_sleep_for_requested_timeout)
        subscriber._pubsub = mock_pubsub

        with cap_iterations(
            mock_pubsub, "get_message", side_effect=_sleep_for_requested_timeout
        ) as count:
            # Act
            subscriber._shutdown = False
            subscriber._task = asyncio.create_task(subscriber._run())
            await asyncio.sleep(0.5)
            observed = count()
            subscriber._shutdown = True
            await asyncio.wait_for(subscriber._task, timeout=1.0)

        # Assert
        assert observed < 5, (
            f"async subscriber polled get_message {observed} times in 0.5s; "
            "the 0.5s poll timeout literal has been bypassed"
        )


# ---------------------------------------------------------------------------
# AsyncHeartbeatScheduler
# ---------------------------------------------------------------------------


class TestAsyncHeartbeatSchedulerSafetyNet:
    """Hang and spin coverage for ``AsyncHeartbeatScheduler._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_shutdown_completes_promptly(limiter_id):
        """Verify that ``shutdown()`` completes well within its internal timeout."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 60.0
        limiter.extend_lease = AsyncMock(return_value=None)
        scheduler = AsyncHeartbeatScheduler(limiter)
        await scheduler.register("task-1", "warn")

        # Act
        completed = await async_shutdown_completes_within(scheduler, timeout=1.0)

        # Assert
        assert completed, (
            "shutdown() should complete within 1.0s; "
            "a timeout indicates _shutdown assignment was mutated"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_run_does_not_starve_event_loop(limiter_id):
        """Verify that ``_run`` does not starve the event loop with a busy loop."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 10.0
        limiter.extend_lease = AsyncMock(return_value=None)
        scheduler = AsyncHeartbeatScheduler(limiter)
        await scheduler.register("task-1", "warn")

        # Act
        start = time.monotonic()
        await asyncio.sleep(0.05)
        elapsed = time.monotonic() - start
        await async_shutdown_completes_within(scheduler, timeout=1.0)

        # Assert
        assert elapsed < 0.2, (
            f"asyncio.sleep(0.05) took {elapsed:.2f}s; "
            "_run is starving the event loop with a busy loop"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_run_iteration_rate_is_bounded(limiter_id):
        """Verify that ``_run`` does not tight-loop in any branch (empty heap, due-entry, stale entry)."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 60.0
        limiter.extend_lease = AsyncMock(return_value=None)
        scheduler = AsyncHeartbeatScheduler(limiter)
        original_acquire = scheduler._lock.acquire

        with cap_iterations(
            scheduler._lock, "acquire", side_effect=original_acquire
        ) as count:
            # Act
            await scheduler.register("task-1", "warn")
            await asyncio.sleep(0.5)
            observed = count()
            await scheduler.shutdown()

        # Assert
        assert observed < 60, (
            f"async scheduler acquired _lock {observed} times in 0.5s; "
            "_run is iterating without honouring its renewal-interval throttle"
        )


# ---------------------------------------------------------------------------
# AsyncBackendHealthMonitor
# ---------------------------------------------------------------------------


class TestAsyncBackendHealthMonitorSafetyNet:
    """Hang and spin coverage for ``AsyncBackendHealthMonitor._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_shutdown_cancels_task():
        """Verify that ``shutdown()`` cancels the background asyncio task cleanly."""
        # Arrange
        limiter = MagicMock()
        limiter.id = "test-limiter"
        limiter._check_backend_health = AsyncMock(return_value=True)
        monitor = AsyncBackendHealthMonitor(limiter, interval=1.0)
        monitor.start()

        # Act
        shutdown_task = asyncio.create_task(monitor.shutdown())
        await asyncio.sleep(0.05)

        # Assert
        assert shutdown_task.done(), (
            "shutdown() should complete promptly; "
            "still running indicates the _run loop did not exit"
        )
        assert monitor._task is None or monitor._task.done(), (
            "background task should be done after shutdown"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_run_throttles_iterations_when_healthy():
        """Verify that ``_run`` honours its interval between health checks."""
        # Arrange
        limiter = MagicMock()
        limiter.id = "test-limiter"
        limiter._check_backend_health = AsyncMock(return_value=True)
        monitor = AsyncBackendHealthMonitor(limiter, interval=60.0)

        with cap_iterations(
            limiter, "_check_backend_health", return_value=True
        ) as count:
            # Act
            monitor.start()
            await asyncio.sleep(0.5)
            observed = count()
            await monitor.shutdown()

        # Assert
        assert observed < 5, (
            f"health monitor checked {observed} times in 0.5s with a 60s interval; "
            "the interval clamp has been bypassed"
        )


# ---------------------------------------------------------------------------
# Async drain rescheduling integration (real limiter + drain task + mocked consume)
# ---------------------------------------------------------------------------


class TestAsyncDrainReschedulingSafetyNet:
    """Spin coverage for the async drain loop's reschedule branches (rate-limited, no-capacity, lock-held)."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_drain_throttles_iterations_under_rate_limit(
        real_drain_async_limiter,
    ):
        """Verify async drain reschedules with a bounded delay when consume reports rate limited."""
        # Arrange
        with (
            cap_iterations(
                real_drain_async_limiter,
                "consume",
                return_value=_RATE_LIMITED_RESULT,
            ) as count,
            patch.object(
                real_drain_async_limiter,
                "execution_lock",
                _bypassed_async_execution_lock,
            ),
        ):
            # Act
            await real_drain_async_limiter.trigger_consume()
            await asyncio.sleep(0.5)
            observed = count()

        # Assert
        assert observed < 50, (
            f"async drain looped {observed} times in 0.5s under rate limit; "
            "the reschedule throttle (max(0.001, base_delay + jitter)) has been bypassed"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_drain_throttles_iterations_when_local_capacity_exhausted(
        real_drain_async_limiter,
    ):
        """Verify async drain reschedules with a bounded delay when local capacity is exhausted."""
        # Arrange
        real_drain = real_drain_async_limiter.drain
        with (
            patch.object(
                real_drain_async_limiter,
                "_has_local_capacity",
                return_value=False,
            ),
            cap_iterations(
                real_drain_async_limiter, "drain", side_effect=real_drain
            ) as count,
        ):
            # Act
            await real_drain_async_limiter.trigger_consume()
            await asyncio.sleep(0.5)
            observed = count()

        # Assert
        assert observed < 50, (
            f"async drain looped {observed} times in 0.5s with no local capacity; "
            "the _token_interval reschedule has been bypassed"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_drain_throttles_iterations_when_lock_held(
        real_drain_async_limiter,
    ):
        """Verify async drain reschedules with a bounded delay when the dispatch lock is held by another worker."""

        # Arrange
        @asynccontextmanager
        async def _lock_held():
            yield False

        real_drain = real_drain_async_limiter.drain
        with (
            patch.object(real_drain_async_limiter, "execution_lock", _lock_held),
            cap_iterations(
                real_drain_async_limiter, "drain", side_effect=real_drain
            ) as count,
        ):
            # Act
            await real_drain_async_limiter.trigger_consume()
            await asyncio.sleep(0.5)
            observed = count()

        # Assert
        assert observed < 50, (
            f"async drain looped {observed} times in 0.5s with the lock held; "
            "the _schedule_backup_drain throttle has been bypassed"
        )
