"""Tests for the ``DrainLoop`` and ``DrainSignalSubscriber`` scheduling components.

These tests use threading primitives (``Event``, ``Timer``, ``time.sleep``)
and therefore cannot be deduplicated with the async variant via the mixin
pattern.

Fixture dependencies:
    - ``stub_limiter``: from ``tests/implementations/conftest.py``.
"""

import inspect
import logging
import time
from threading import Event, Thread
from unittest.mock import MagicMock

import pytest

from redis_rate_limiter.core.limiters import DrainLoop, DrainSignalSubscriber
from tests.helpers.utils import assert_log_emitted, shutdown_timer
from tests.implementations.conftest import StubRateLimiter


@pytest.mark.behavior
class TestDrainLoop:
    """Test suite for ``DrainLoop`` wake, coalesce, watchdog, and shutdown behavior."""

    @staticmethod
    def test_wake_with_delay_fires_after_delay():
        """Verify that ``wake(delay)`` waits approximately the
        specified duration before firing.
        """
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
        """Verify that the watchdog timeout fires ``drain()``
        even without an explicit ``wake()`` call.
        """
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()

        # A short watchdog interval is used to avoid a slow test.
        loop = DrainLoop(limiter, watchdog_interval=0.15)

        # Act
        loop.wake(0)
        drain_called.wait(timeout=1.0)

        drain_called.clear()
        limiter.drain.reset_mock()
        fired = drain_called.wait(timeout=1.0)
        loop.shutdown()

        # Assert
        assert fired, "watchdog should fire drain even without explicit wake"
        limiter.drain.assert_called()

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_shutdown_completes_promptly():
        """Verify that ``shutdown()`` completes well within
        its internal 5.0s join timeout.
        """
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Start the thread and let it complete one drain cycle so it is
        # blocked on _condition.wait() when shutdown is called.
        loop.wake(0)
        drain_called.wait(timeout=2.0)

        # shutdown() has an internal 5.0s join; mutations that break the
        # _shutdown flag (e.g., None/False) cause the full 5s block, which
        # triggers SIGXCPU under mutmut before the assertion can run.
        shutdown_thread = Thread(target=loop.shutdown, daemon=True)
        shutdown_thread.start()
        shutdown_thread.join(timeout=1.0)
        completed = not shutdown_thread.is_alive()

        # Assert
        assert completed, (
            "shutdown() should complete within 1.0s; "
            "a timeout indicates _shutdown assignment was mutated"
        )
        assert not loop._thread.is_alive(), "thread should be stopped after shutdown"

    @staticmethod
    def test_shutdown_is_idempotent():
        """Verify that calling ``shutdown()`` twice does not raise."""
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.wake(0)
        drain_called.wait(timeout=2.0)
        loop.shutdown()

        # Assert
        loop.shutdown()
        assert loop._shutdown is True, (
            "shutdown flag should remain True after second shutdown"
        )

    @staticmethod
    def test_lazy_start():
        """Verify that the drain thread is not started until
        the first ``wake()`` call.
        """
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
    def test_drain_loop_has_lock_after_construction():
        """Verify that ``DrainLoop`` initializes with a threading lock."""
        # Arrange & Act
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Assert
        assert loop._lock is not None, "drain loop must have a lock after construction"

    @staticmethod
    def test_drain_loop_survives_drain_exception():
        """Verify that the drain loop thread survives when
        ``drain()`` raises an exception.
        """
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

    @staticmethod
    def test_ensure_started_thread_is_daemon():
        """Verify that the drain thread is started as a daemon
        thread so it does not block process exit.
        """
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


@pytest.mark.behavior
class TestDrainSignalSubscriber:
    """Test suite for ``DrainSignalSubscriber`` shutdown and
    message-processing behavior.
    """

    @staticmethod
    def test_shutdown_sets_flag_without_starting_thread():
        """Verify that ``shutdown()`` sets the ``_shutdown``
        flag to ``True`` without a running thread.
        """
        # Arrange
        limiter = MagicMock()
        subscriber = DrainSignalSubscriber(limiter)

        # Assert
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
    def test_subscriber_processes_remote_drain_signal():
        """Verify that the subscriber calls ``_schedule_drain``
        upon receiving a remote drain signal.
        """
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
            "subscriber should call _schedule_drain upon receiving "
            "a remote drain signal"
        )

    @pytest.mark.timeout_safety_net
    @staticmethod
    def test_run_processes_message_in_main_thread():
        """Verify that ``_run`` processes a remote message and
        calls ``_schedule_drain`` when invoked directly.

        Calling ``_run()`` directly (rather than via ``start()``)
        ensures coverage tracks the execution in the main thread.
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
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message.side_effect = get_message_effect

        # Act
        with shutdown_timer(subscriber):
            subscriber._run()

        # Assert
        mock_pubsub.get_message.assert_called()
        limiter._schedule_drain.assert_called_once()

    @staticmethod
    def test_subscriber_ignores_local_drain_signal():
        """Verify that the subscriber ignores drain signals
        originating from the local worker.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        mock_pubsub = MagicMock()

        call_count = 0

        def get_message_effect(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
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

    @staticmethod
    def test_start_creates_daemon_thread():
        """Verify that ``start()`` creates a daemon thread."""
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        mock_pubsub = MagicMock()
        mock_pubsub.get_message.return_value = None
        limiter.redis.pubsub.return_value = mock_pubsub

        subscriber = DrainSignalSubscriber(limiter)

        # Act
        subscriber.start()

        # Assert
        assert subscriber._thread is not None, "thread should exist after start"
        assert subscriber._thread.daemon is True, (
            "subscriber thread must be a daemon thread"
        )
        subscriber.shutdown()

    @pytest.mark.timeout_safety_net
    @staticmethod
    def test_run_survives_exception_and_retries():
        """Verify that ``_run`` logs the exception and
        continues the loop when ``get_message`` raises.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        limiter.id = "test-resilience"
        subscriber = DrainSignalSubscriber(limiter)
        mock_pubsub = MagicMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        def get_message_effect(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("simulated pubsub failure")
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message.side_effect = get_message_effect

        # Act
        with shutdown_timer(subscriber, timeout=2.0):
            subscriber._run()

        # Assert
        assert call_count >= 2, "get_message should be called again after exception"

    @staticmethod
    def test_run_exits_silently_on_exception_during_shutdown():
        """Verify that ``_run`` returns immediately when an
        exception occurs while ``_shutdown`` is ``True``.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = DrainSignalSubscriber(limiter)
        mock_pubsub = MagicMock()
        subscriber._pubsub = mock_pubsub

        def get_message_effect(timeout=None):
            subscriber._shutdown = True
            raise ConnectionError("shutdown in progress")

        mock_pubsub.get_message.side_effect = get_message_effect

        # Act
        subscriber._run()

        # Assert
        assert subscriber._shutdown is True, "_run should return without re-raising"

    @pytest.mark.timeout_safety_net
    @staticmethod
    def test_run_processes_message_then_idles_before_exit():
        """Verify that ``_run`` continues polling after
        processing a message and exits only when
        ``_shutdown`` is set.

        The idle iteration (call 2 returns ``None``) exercises
        the ``while not self._shutdown`` + ``message is not
        None`` compound condition.
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
            if call_count >= 3:
                subscriber._shutdown = True
            return None

        mock_pubsub.get_message.side_effect = get_message_effect

        # Act
        with shutdown_timer(subscriber):
            subscriber._run()

        # Assert
        assert call_count >= 3, (
            "get_message should be called at least 3 times: "
            "message, idle, then shutdown"
        )
        limiter._schedule_drain.assert_called_once()

    @staticmethod
    def test_shutdown_swallows_pubsub_exception():
        """Verify that ``shutdown()`` does not propagate exceptions
        raised by the Pub/Sub ``unsubscribe()`` or ``close()`` calls.
        """
        # Arrange
        limiter = MagicMock()
        subscriber = DrainSignalSubscriber(limiter)
        mock_pubsub = MagicMock()
        mock_pubsub.unsubscribe.side_effect = ConnectionError("connection lost")
        subscriber._pubsub = mock_pubsub

        # Act
        subscriber.shutdown()

        # Assert
        assert subscriber._shutdown is True, (
            "shutdown flag should be True even when pubsub cleanup raises"
        )


@pytest.mark.behavior
class TestWatchdogInterval:
    """Tests for the watchdog interval computation in
    ``AbstractDistributedRateLimiter``.
    """

    @staticmethod
    def test_watchdog_interval_floor_is_five_seconds(redis_client, limiter_id):
        """Verify that the watchdog interval floor is 5.0
        seconds when ``window * 2`` is smaller.
        """
        # Arrange
        # window=1.0 produces window*2=2.0 which is below the 5.0 floor.
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_watchdog",
            limit=10,
            window=1.0,
            max_concurrency=2,
        )

        # Assert
        assert limiter._drain_loop._watchdog_interval == pytest.approx(5.0), (
            "watchdog interval should be 5.0 when window * 2 is below the floor"
        )

        # Teardown
        limiter.shutdown()


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestDrainLoopObservability:
    """Observability tests for ``DrainLoop`` log emissions."""

    @staticmethod
    def test_drain_exception_emits_error_log(caplog):
        """Verify that the drain loop emits an ERROR log when
        ``drain()`` raises an exception.
        """
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
        with caplog.at_level(logging.ERROR, logger="redis_rate_limiter.core.limiters"):
            loop.wake(0)
            time.sleep(0.1)
            loop.wake(0)
            second_call.wait(timeout=2.0)
            loop.shutdown()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="ERROR",
            label="[DrainLoop]",
            required_fragments=["limiter=test-resilience"],
            message=(
                "should emit an error log containing the limiter id when drain raises"
            ),
        )


@pytest.mark.observability
class TestDrainSignalSubscriberObservability:
    """Observability tests for ``DrainSignalSubscriber`` log emissions."""

    @pytest.mark.timeout_safety_net
    @staticmethod
    def test_run_normal_processing_does_not_emit_error_log(caplog):
        """Verify that ``_run`` does not emit error or critical
        logs during normal message processing.
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
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message.side_effect = get_message_effect

        # Act
        with shutdown_timer(subscriber):
            with caplog.at_level(
                logging.ERROR,
                logger="redis_rate_limiter.core.limiters",
            ):
                subscriber._run()

        # Assert
        assert not any(
            record.levelname in ("ERROR", "CRITICAL") for record in caplog.records
        ), "no error logs should be emitted during normal processing"

    @pytest.mark.timeout_safety_net
    @staticmethod
    def test_run_exception_emits_error_log(caplog):
        """Verify that ``_run`` emits an ERROR log containing
        the limiter id when ``get_message`` raises.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        limiter.id = "test-subscriber-error"
        subscriber = DrainSignalSubscriber(limiter)
        mock_pubsub = MagicMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        def get_message_effect(timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("simulated pubsub failure")
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message.side_effect = get_message_effect

        # Act
        with shutdown_timer(subscriber, timeout=2.0):
            with caplog.at_level(
                logging.ERROR, logger="redis_rate_limiter.core.limiters"
            ):
                subscriber._run()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="ERROR",
            label="[DrainSignalSubscriber]",
            required_fragments=[
                "limiter=test-subscriber-error",
            ],
            message=(
                "should emit an error log containing"
                " the limiter id when get_message raises"
            ),
        )


# ---------------------------------------------------------------------------
# Boundary tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestDrainLoopBoundary:
    """Boundary condition tests for ``DrainLoop`` internal parameters."""

    @staticmethod
    def test_shutdown_joins_thread_with_five_second_timeout():
        """Verify that ``shutdown()`` joins the drain thread
        with a 5.0 second timeout."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        loop._thread = mock_thread

        # Act
        loop.shutdown()

        # Assert
        mock_thread.join.assert_called_once_with(timeout=5.0)


@pytest.mark.behavior
class TestDrainSignalSubscriberBoundary:
    """Boundary condition tests for ``DrainSignalSubscriber`` internal parameters."""

    @staticmethod
    def test_shutdown_joins_thread_with_five_second_timeout():
        """Verify that ``shutdown()`` joins the subscriber thread
        with a 5.0 second timeout."""
        # Arrange
        limiter = MagicMock()
        subscriber = DrainSignalSubscriber(limiter)
        mock_thread = MagicMock()
        subscriber._thread = mock_thread

        # Act
        subscriber.shutdown()

        # Assert
        mock_thread.join.assert_called_once_with(timeout=5.0)

    @staticmethod
    def test_run_polls_with_half_second_timeout():
        """Verify that ``_run()`` calls ``get_message`` with
        ``timeout=0.5`` for responsive shutdown detection."""
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
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message.side_effect = get_message_effect

        # Act
        subscriber._run()

        # Assert
        mock_pubsub.get_message.assert_called_with(timeout=0.5)


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestDrainLoopSignatures:
    """Signature tests for ``DrainLoop`` default parameter values."""

    @staticmethod
    def test_wake_default_delay_is_zero():
        """Verify that the ``delay`` parameter of ``wake()`` defaults to ``0.0``.

        Mutation target: default value of ``delay`` in ``DrainLoop.wake()``.
        """
        # Arrange & Act
        sig = inspect.signature(DrainLoop.wake)

        # Assert
        assert sig.parameters["delay"].default == 0.0, (
            "wake() default delay should be 0.0 for immediate scheduling"
        )
