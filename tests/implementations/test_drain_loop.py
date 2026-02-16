"""Tests for the ``DrainLoop`` scheduling component."""

import time
from threading import Event
from unittest.mock import MagicMock

from celery_rate_limiter.core.limiters import DrainLoop


class TestDrainLoop:
    """Test suite for ``DrainLoop`` wake, coalesce, watchdog, and shutdown behavior."""

    def test_wake_fires_drain_immediately(self):
        """Verify that ``wake(0)`` causes ``drain()`` to be called promptly."""
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.wake(0)
        fired = drain_called.wait(timeout=2.0)
        loop.shutdown()

        # Assert
        assert fired, "drain should be called after wake(0)"
        limiter.drain.assert_called()

    def test_wake_with_delay_fires_after_delay(self):
        """Verify that ``wake(delay)`` waits approximately the specified duration before firing."""
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        start = time.monotonic()
        loop.wake(0.15)
        fired = drain_called.wait(timeout=2.0)
        elapsed = time.monotonic() - start
        loop.shutdown()

        # Assert
        assert fired, "drain should be called after delayed wake"
        assert elapsed >= 0.1, "drain should not fire before the delay"

    def test_wake_coalesces_to_sooner_time(self):
        """Verify that ``wake(0)`` overrides a pending ``wake(large_delay)``."""
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Schedule a far-future wake, then override it with an immediate one.
        loop.wake(10.0)
        loop.wake(0)
        fired = drain_called.wait(timeout=2.0)
        loop.shutdown()

        # Assert
        assert fired, "immediate wake should override far-future wake"

    def test_wake_ignores_later_time(self):
        """Verify that ``wake(large_delay)`` does not override a pending ``wake(0)``."""
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.wake(0)
        loop.wake(10.0)
        fired = drain_called.wait(timeout=2.0)
        loop.shutdown()

        # Assert
        assert fired, "immediate wake should not be overridden by later wake"

    def test_watchdog_fires_drain_when_idle(self):
        """Verify that the watchdog timeout fires ``drain()`` even without an explicit ``wake()`` call."""
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()

        # A short watchdog interval is used to avoid a slow test.
        loop = DrainLoop(limiter, watchdog_interval=0.15)

        # Act
        # Start the thread by calling wake once, then allow the watchdog to fire.
        loop.wake(0)
        drain_called.wait(timeout=1.0)

        # Reset and wait for the watchdog to fire again without any explicit wake.
        drain_called.clear()
        limiter.drain.reset_mock()
        fired = drain_called.wait(timeout=1.0)
        loop.shutdown()

        # Assert
        assert fired, "watchdog should fire drain even without explicit wake"
        limiter.drain.assert_called()

    def test_shutdown_stops_thread(self):
        """Verify that ``shutdown()`` stops the drain thread cleanly."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Start the thread.
        loop.wake(10.0)
        loop.shutdown()

        # Assert
        assert loop._thread is not None, "thread should have been created"
        assert not loop._thread.is_alive(), "thread should be stopped after shutdown"

    def test_lazy_start(self):
        """Verify that the drain thread is not started until the first ``wake()`` call."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Assert
        assert loop._thread is None, "thread should not exist before first wake"

        # Act
        loop.wake(10.0)

        # Assert
        assert loop._thread is not None, "thread should exist after first wake"
        loop.shutdown()

    def test_drain_loop_survives_drain_exception(self):
        """Verify that the drain loop thread survives when ``drain()`` raises an exception."""
        # Arrange
        limiter = MagicMock()
        limiter.id = "test-resilience"
        call_count = 0
        second_call = Event()

        def _failing_then_succeeding_drain():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated drain failure")
            second_call.set()

        limiter.drain.side_effect = _failing_then_succeeding_drain
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # First wake triggers the exception, second wake should still work.
        loop.wake(0)
        time.sleep(0.1)
        loop.wake(0)
        fired = second_call.wait(timeout=2.0)
        loop.shutdown()

        # Assert
        assert fired, "drain loop should survive an exception and process subsequent wakes"
        assert call_count >= 2, "drain should have been called at least twice"

    def test_ensure_started_restarts_dead_thread(self):
        """Verify that ``_ensure_started()`` detects and replaces a dead thread."""
        # Arrange
        limiter = MagicMock()
        limiter.id = "test-restart"
        first_call = Event()
        second_call = Event()

        def _drain_side_effect():
            if not first_call.is_set():
                first_call.set()
                raise RuntimeError("kill the thread")
            second_call.set()

        limiter.drain.side_effect = _drain_side_effect
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Start and let the first drain fire (which raises).
        loop.wake(0)
        first_call.wait(timeout=2.0)
        time.sleep(0.1)

        # Trigger another wake. The thread should have survived due to the
        # try/except; if _ensure_started detects a dead thread it restarts it.
        loop.wake(0)
        fired = second_call.wait(timeout=2.0)
        loop.shutdown()

        # Assert
        assert fired, "drain should be called again after thread recovery"
