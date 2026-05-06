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
from unittest.mock import AsyncMock, MagicMock

import pytest

from redis_rate_limiter.core.async_limiters import (
    AsyncBackendHealthMonitor,
    AsyncDrainLoop,
    AsyncHeartbeatScheduler,
)
from tests.helpers.utils import (
    async_shutdown_completes_within,
    shutdown_timer,
    trip_after_deadline,
)

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
        with shutdown_timer(scheduler, timeout=0.3):
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
        """Verify that ``_run`` does not tight-loop in either the empty-heap or due-entry branch."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 0.05
        limiter.extend_lease = AsyncMock(return_value=None)
        scheduler = AsyncHeartbeatScheduler(limiter)
        original_acquire = scheduler._lock.acquire

        with trip_after_deadline(
            scheduler._lock, "acquire", 0.3, side_effect=original_acquire
        ) as count:
            # Act
            await scheduler.register("task-1", "warn")
            await asyncio.sleep(0.08)
            try:
                await scheduler.deregister("task-1")
            except RuntimeError:
                pass
            await asyncio.sleep(0.08)
            observed = count()

        try:
            await async_shutdown_completes_within(scheduler, timeout=1.0)
        except RuntimeError:
            pass

        # Assert
        assert observed < 60, (
            f"async scheduler acquired _lock {observed} times in ~0.16s; "
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
        loop = asyncio.get_running_loop()

        # Act
        shutdown_task = asyncio.create_task(monitor.shutdown())
        with shutdown_timer(
            timeout=0.3,
            on_fire=lambda: loop.call_soon_threadsafe(monitor._shutdown_event.set),
        ):
            await asyncio.sleep(0.05)

        # Assert
        assert shutdown_task.done(), (
            "shutdown() should complete promptly; "
            "still running indicates the _run loop did not exit"
        )
        assert monitor._task is None or monitor._task.done(), (
            "background task should be done after shutdown"
        )
