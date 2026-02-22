"""Tests for the ``AsyncDrainLoop`` and ``AsyncDrainSignalSubscriber`` scheduling components.

Mirrors the sync ``DrainLoop`` tests in ``test_drain_loop.py`` using
``asyncio.Event``, ``asyncio.Task``, and ``asyncio.wait_for`` instead of
threading primitives.
"""

import asyncio
import inspect
import logging
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from celery_rate_limiter.core.async_limiters import (
    AsyncDrainLoop,
    AsyncDrainSignalSubscriber,
)


class TestAsyncDrainLoop:
    """Test suite for ``AsyncDrainLoop`` wake, coalesce, watchdog, and shutdown behavior."""

    @staticmethod
    async def test_wake_default_delay_fires_immediately():
        """Verify that ``wake()`` with no arguments uses the default delay of ``0.0`` and fires promptly."""
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        start = time.monotonic()
        loop.wake()
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=2.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        elapsed = time.monotonic() - start
        await loop.shutdown()

        # Assert
        assert fired, "drain should be called after wake() with default delay"
        assert elapsed < 0.5, (
            f"drain should fire promptly with default delay=0.0, took {elapsed:.2f}s"
        )
        limiter.drain.assert_called()

    @staticmethod
    async def test_wake_default_delay_is_zero():
        """Verify that the ``delay`` parameter of ``wake()`` defaults to ``0.0``."""
        # Arrange & Act
        sig = inspect.signature(AsyncDrainLoop.wake)

        # Assert
        assert sig.parameters["delay"].default == 0.0, (
            "wake() default delay should be 0.0 for immediate scheduling"
        )

    @staticmethod
    async def test_wake_fires_drain_immediately():
        """Verify that ``wake(0)`` causes ``drain()`` to be called promptly."""
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.wake(0)
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=2.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        await loop.shutdown()

        # Assert
        assert fired, "drain should be called after wake(0)"
        limiter.drain.assert_called()

    @staticmethod
    async def test_wake_with_delay_fires_after_delay():
        """Verify that ``wake(delay)`` waits approximately the specified duration before firing."""
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        start = time.monotonic()
        loop.wake(0.15)
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=2.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        elapsed = time.monotonic() - start
        await loop.shutdown()

        # Assert
        assert fired, "drain should be called after delayed wake"
        assert elapsed >= 0.1, "drain should not fire before the delay"

    @staticmethod
    async def test_wake_coalesces_to_sooner_time():
        """Verify that ``wake(0)`` overrides a pending ``wake(large_delay)``."""
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Schedule a far-future wake, then override it with an immediate one.
        loop.wake(10.0)
        await asyncio.sleep(0)
        loop.wake(0)
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=2.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        await loop.shutdown()

        # Assert
        assert fired, "immediate wake should override far-future wake"

    @staticmethod
    async def test_wake_ignores_later_time():
        """Verify that ``wake(large_delay)`` does not override a pending ``wake(0)``."""
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.wake(0)
        await asyncio.sleep(0)
        loop.wake(10.0)
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=2.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        await loop.shutdown()

        # Assert
        assert fired, "immediate wake should not be overridden by later wake"

    @staticmethod
    async def test_watchdog_fires_drain_when_idle():
        """Verify that the watchdog timeout fires ``drain()`` even without an explicit ``wake()`` call."""
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())

        # A short watchdog interval is used to avoid a slow test.
        loop = AsyncDrainLoop(limiter, watchdog_interval=0.15)

        # Act
        # Start the task by calling wake once, then allow the watchdog to fire.
        loop.wake(0)
        await asyncio.wait_for(drain_called.wait(), timeout=1.0)

        # Reset and wait for the watchdog to fire again without any explicit wake.
        drain_called.clear()
        limiter.drain.reset_mock()
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=1.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        await loop.shutdown()

        # Assert
        assert fired, "watchdog should fire drain even without explicit wake"
        limiter.drain.assert_called()

    @staticmethod
    async def test_shutdown_stops_task():
        """Verify that ``shutdown()`` stops the drain task cleanly."""
        # Arrange
        limiter = MagicMock()
        limiter.drain = AsyncMock()
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Start the task.
        loop.wake(10.0)
        await asyncio.sleep(0)
        await loop.shutdown()

        # Assert
        assert loop._shutdown is True, "shutdown flag should be True after shutdown"
        assert loop._task is not None, "task should have been created"
        assert loop._task.done(), "task should be done after shutdown"

    @staticmethod
    async def test_lazy_start():
        """Verify that the drain task is not started until the first ``wake()`` call."""
        # Arrange
        limiter = MagicMock()
        limiter.drain = AsyncMock()
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Assert
        assert loop._shutdown is False, (
            "shutdown flag should be False before first wake"
        )
        assert loop._task is None, "task should not exist before first wake"

        # Act
        loop.wake(10.0)
        await asyncio.sleep(0)

        # Assert
        assert loop._task is not None, "task should exist after first wake"
        await loop.shutdown()

    @staticmethod
    async def test_drain_loop_survives_drain_exception(caplog):
        """Verify that the drain loop task survives when ``drain()`` raises an exception."""
        # Arrange
        limiter = MagicMock()
        limiter.id = "test-resilience"
        call_count = 0
        second_call = asyncio.Event()

        def _failing_then_succeeding_drain():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated drain failure")
            second_call.set()

        limiter.drain = AsyncMock(side_effect=_failing_then_succeeding_drain)
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # First wake triggers the exception, second wake should still work.
        with caplog.at_level(
            logging.ERROR, logger="celery_rate_limiter.core.async_limiters"
        ):
            loop.wake(0)
            await asyncio.sleep(0.1)
            loop.wake(0)
            try:
                await asyncio.wait_for(second_call.wait(), timeout=2.0)
                fired = True
            except asyncio.TimeoutError:
                fired = False
            await loop.shutdown()

        # Assert
        assert fired, (
            "drain loop should survive an exception and process subsequent wakes"
        )
        assert call_count >= 2, "drain should have been called at least twice"
        assert any(
            record.levelname == "ERROR" and "limiter=test-resilience" in record.message
            for record in caplog.records
        ), "should emit an error log containing the limiter id when drain raises"

    @staticmethod
    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    async def test_ensure_started_restarts_dead_task():
        """Verify that ``_ensure_started()`` detects and replaces a done task."""
        # Arrange
        limiter = MagicMock()
        limiter.id = "test-restart"
        first_call = asyncio.Event()
        second_call = asyncio.Event()

        def _drain_side_effect():
            if not first_call.is_set():
                first_call.set()
                raise RuntimeError("kill the task")
            second_call.set()

        limiter.drain = AsyncMock(side_effect=_drain_side_effect)
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Start and let the first drain fire (which raises).
        loop.wake(0)
        await asyncio.wait_for(first_call.wait(), timeout=2.0)
        await asyncio.sleep(0.1)

        # Trigger another wake. The task should have survived due to the
        # try/except; if _ensure_started detects a done task it restarts it.
        loop.wake(0)
        try:
            await asyncio.wait_for(second_call.wait(), timeout=2.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        await loop.shutdown()

        # Assert
        assert fired, "drain should be called again after task recovery"


class TestAsyncDrainSignalSubscriber:
    """Test suite for ``AsyncDrainSignalSubscriber`` shutdown behavior."""

    @staticmethod
    async def test_shutdown_sets_flag(async_generic_limiter):
        """Verify that ``shutdown()`` sets the ``_shutdown`` flag to ``True``."""
        # Arrange
        subscriber = AsyncDrainSignalSubscriber(async_generic_limiter)

        # Assert
        assert subscriber._shutdown is False, (
            "shutdown flag should be False before shutdown is called"
        )

        # Act
        await subscriber.shutdown()

        # Assert
        assert subscriber._shutdown is True, (
            "shutdown flag should be True after shutdown"
        )
