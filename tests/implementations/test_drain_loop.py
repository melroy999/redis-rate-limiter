"""Tests for the ``DrainLoop`` and ``DrainSignalSubscriber`` scheduling components."""

import inspect
import logging
import time
from threading import Event, Timer
from unittest.mock import MagicMock

from celery_rate_limiter.core.limiters import DrainLoop, DrainSignalSubscriber


class TestDrainLoop:
    """Test suite for ``DrainLoop`` wake, coalesce, watchdog, and shutdown behavior."""

    @staticmethod
    def test_shutdown_sets_flag():
        """Verify that ``shutdown()`` sets the ``_shutdown`` flag to exactly ``True`` without a running thread."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.shutdown()

        # Assert
        # Identity check catches mutations to None and False.
        assert loop._shutdown is True, (
            "shutdown flag must be exactly True after shutdown"
        )
        assert loop._thread is None, (
            "thread should not have been started without a wake call"
        )

    @staticmethod
    def test_wake_default_delay_fires_immediately():
        """Verify that ``wake()`` with no arguments uses the default delay of 0.0 and fires promptly."""
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        start = time.monotonic()
        loop.wake()
        fired = drain_called.wait(timeout=2.0)
        elapsed = time.monotonic() - start
        loop.shutdown()

        # Assert
        assert fired, "drain should be called after wake() with default delay"
        assert elapsed < 0.5, (
            f"drain should fire promptly with default delay=0.0, took {elapsed:.2f}s"
        )
        limiter.drain.assert_called()

    @staticmethod
    def test_wake_default_delay_is_zero():
        """Verify that the ``delay`` parameter of ``wake()`` defaults to ``0.0``."""
        # Arrange & Act
        sig = inspect.signature(DrainLoop.wake)

        # Assert
        assert sig.parameters["delay"].default == 0.0, (
            "wake() default delay should be 0.0 for immediate scheduling"
        )

    @staticmethod
    def test_wake_fires_drain_immediately():
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

    @staticmethod
    def test_wake_with_delay_fires_after_delay():
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

    @staticmethod
    def test_wake_coalesces_to_sooner_time():
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

    @staticmethod
    def test_wake_ignores_later_time():
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

    @staticmethod
    def test_watchdog_fires_drain_when_idle():
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

    @staticmethod
    def test_shutdown_stops_thread():
        """Verify that ``shutdown()`` stops the drain thread cleanly."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Start the thread.
        loop.wake(10.0)
        loop.shutdown()

        # Assert
        assert loop._shutdown is True, "shutdown flag should be True after shutdown"
        assert loop._thread is not None, "thread should have been created"
        assert not loop._thread.is_alive(), "thread should be stopped after shutdown"

    @staticmethod
    def test_lazy_start():
        """Verify that the drain thread is not started until the first ``wake()`` call."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Assert
        assert loop._shutdown is False, (
            "shutdown flag should be False before first wake"
        )
        assert loop._thread is None, "thread should not exist before first wake"

        # Act
        loop.wake(10.0)

        # Assert
        assert loop._thread is not None, "thread should exist after first wake"
        loop.shutdown()

    @staticmethod
    def test_drain_loop_survives_drain_exception(caplog):
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
        with caplog.at_level(logging.ERROR, logger="celery_rate_limiter.core.limiters"):
            loop.wake(0)
            time.sleep(0.1)
            loop.wake(0)
            fired = second_call.wait(timeout=2.0)
            loop.shutdown()

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
    def test_ensure_started_thread_is_daemon():
        """Verify that the drain thread is started as a daemon thread so it does not block process exit."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.wake(10.0)

        # Assert
        assert loop._thread is not None, "thread should exist after wake"
        assert loop._thread.daemon is True, (
            "drain thread must be a daemon thread to avoid blocking process exit"
        )
        loop.shutdown()

    @staticmethod
    def test_ensure_started_reuses_alive_thread():
        """Verify that ``wake()`` reuses the existing thread when it is still alive."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.wake(10.0)
        first_thread = loop._thread
        loop.wake(10.0)
        second_thread = loop._thread
        loop.shutdown()

        # Assert
        assert first_thread is not None, "first wake should create a thread"
        assert second_thread is first_thread, (
            "wake on an alive thread should reuse the existing thread, not replace it"
        )

    @staticmethod
    def test_ensure_started_restarts_dead_thread():
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


class TestDrainSignalSubscriber:
    """Test suite for ``DrainSignalSubscriber`` shutdown and message-processing behavior."""

    @staticmethod
    def test_shutdown_sets_flag_without_starting_thread():
        """Verify that ``shutdown()`` sets the ``_shutdown`` flag to ``True`` without a running thread."""
        # Arrange
        limiter = MagicMock()
        subscriber = DrainSignalSubscriber(limiter)

        # Assert
        # Verify initial state.
        assert subscriber._shutdown is False, (
            "shutdown flag should be False before shutdown is called"
        )

        # Act
        subscriber.shutdown()

        # Assert
        # Identity check catches mutations to None and False.
        assert subscriber._shutdown is True, (
            "shutdown flag must be exactly True after shutdown"
        )
        assert subscriber._thread is None, (
            "thread should not have been started without a start call"
        )

    @staticmethod
    def test_shutdown_sets_flag(generic_limiter):
        """Verify that ``shutdown()`` sets the ``_shutdown`` flag to ``True``."""
        # Arrange
        subscriber = DrainSignalSubscriber(generic_limiter)

        # Assert
        assert subscriber._shutdown is False, (
            "shutdown flag should be False before shutdown is called"
        )

        # Act
        subscriber.shutdown()

        # Assert
        assert subscriber._shutdown is True, (
            "shutdown flag should be True after shutdown"
        )

    @staticmethod
    def test_subscriber_processes_remote_drain_signal():
        """Verify that the subscriber calls ``_schedule_drain`` upon receiving a remote drain signal."""
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        mock_pubsub = MagicMock()

        drain_scheduled = Event()
        limiter._schedule_drain.side_effect = lambda: drain_scheduled.set()

        call_count = 0

        def get_message_effect(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "type": "message",
                    "data": "remote-worker",
                    "channel": b"test:drain_signal",
                }
            # Subsequent calls simulate blocking on the pubsub socket.
            time.sleep(timeout or 0.1)
            return None

        mock_pubsub.get_message.side_effect = get_message_effect
        limiter.redis.pubsub.return_value = mock_pubsub

        subscriber = DrainSignalSubscriber(limiter)

        # Act
        subscriber.start()
        signaled = drain_scheduled.wait(timeout=2.0)
        subscriber.shutdown()

        # Assert
        assert signaled, (
            "subscriber should call _schedule_drain upon receiving a remote drain signal"
        )

    @staticmethod
    def test_run_processes_message_in_main_thread(caplog):
        """Verify that ``_run`` processes a remote message and calls ``_schedule_drain`` when invoked directly.

        Calling ``_run()`` directly (rather than via ``start()``) ensures
        coverage tracks the execution in the main thread, enabling mutmut
        to select this test for mutations within ``_run``.

        A ``Timer`` sets ``_shutdown`` from another thread after 0.5 seconds
        as a safety net: if a mutation replaces the ``get_message()`` call
        with ``None``, the side_effect that normally exits the loop never
        executes, creating a tight infinite loop. The Timer breaks that
        loop so the assertion ``get_message.assert_called()`` can execute
        and fail, killing the mutation.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = DrainSignalSubscriber(limiter)
        mock_pubsub = MagicMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        def get_message_effect(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "type": "message",
                    "data": "remote-worker",
                    "channel": b"test:drain_signal",
                }
            # Return None without setting _shutdown on the second call so that
            # an and-to-or mutation triggers a logged exception before exiting.
            if call_count >= 3:
                subscriber._shutdown = True
            return None

        mock_pubsub.get_message.side_effect = get_message_effect

        # Act
        safety_timer = Timer(0.5, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        with caplog.at_level(logging.ERROR, logger="celery_rate_limiter.core.limiters"):
            subscriber._run()
        safety_timer.cancel()

        # Assert
        mock_pubsub.get_message.assert_called()
        limiter._schedule_drain.assert_called_once()
        assert not any(
            record.levelname in ("ERROR", "CRITICAL") for record in caplog.records
        ), "no exceptions should be logged during normal message processing"

    @staticmethod
    def test_subscriber_ignores_local_drain_signal():
        """Verify that the subscriber ignores drain signals originating from the local worker."""
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        mock_pubsub = MagicMock()

        call_count = 0

        def get_message_effect(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Signal from the local worker should be ignored.
                return {
                    "type": "message",
                    "data": "local-worker",
                    "channel": b"test:drain_signal",
                }
            time.sleep(timeout or 0.1)
            return None

        mock_pubsub.get_message.side_effect = get_message_effect
        limiter.redis.pubsub.return_value = mock_pubsub

        subscriber = DrainSignalSubscriber(limiter)

        # Act
        subscriber.start()
        time.sleep(0.3)
        subscriber.shutdown()

        # Assert
        limiter._schedule_drain.assert_not_called()
