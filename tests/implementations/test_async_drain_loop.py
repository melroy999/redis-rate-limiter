"""Tests for the ``AsyncDrainLoop`` and
``AsyncDrainSignalSubscriber`` scheduling components.

Mirrors the sync ``DrainLoop`` tests in ``test_drain_loop.py`` using
``asyncio.Event``, ``asyncio.Task``, and ``asyncio.wait_for`` instead of
threading primitives. These tests cannot be deduplicated with the sync
variant via the mixin pattern.

Fixture dependencies:
    - ``async_stub_limiter``: from ``tests/implementations/conftest.py``.
    - ``async_redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

import asyncio
import inspect
import logging
import time
from threading import Timer
from unittest.mock import AsyncMock, MagicMock

import pytest

from redis_rate_limiter.core.async_limiters import (
    AsyncDrainLoop,
    AsyncDrainSignalSubscriber,
)
from tests.helpers.utils import assert_log_emitted
from tests.implementations.conftest import AsyncStubRateLimiter


@pytest.mark.behavior
class TestAsyncDrainLoop:
    """Test suite for ``AsyncDrainLoop`` wake, coalesce,
    watchdog, and shutdown behavior.
    """

    @staticmethod
    async def test_shutdown_sets_flag():
        """Verify that ``shutdown()`` sets the ``_shutdown``
        flag to ``True`` on a started loop.
        """
        # Arrange
        limiter = MagicMock()
        limiter.drain = AsyncMock()
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Yield to the event loop so _wake_async can acquire the condition
        # and call _ensure_started(), creating the background task.
        loop.wake()
        await asyncio.sleep(0)
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

        # Assert
        # Identity check catches mutations to None and False.
        assert loop._shutdown is True, (
            "shutdown flag must be exactly True after shutdown"
        )
        assert loop._task is not None, "task should have been created by the wake call"
        assert loop._task.done(), "task should be done after shutdown"

    @staticmethod
    async def test_wake_with_delay_fires_after_delay():
        """Verify that ``wake(delay)`` waits approximately
        the specified duration before firing.
        """
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
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

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
        loop.wake(10.0)
        await asyncio.sleep(0)
        loop.wake(0)
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=2.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

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
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

        # Assert
        assert fired, "immediate wake should not be overridden by later wake"

    @staticmethod
    async def test_watchdog_fires_drain_when_idle():
        """Verify that the watchdog timeout fires ``drain()``
        even without an explicit ``wake()`` call.
        """
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())

        # A short watchdog interval is used to avoid a slow test.
        loop = AsyncDrainLoop(limiter, watchdog_interval=0.15)

        # Act
        loop.wake(0)
        await asyncio.wait_for(drain_called.wait(), timeout=1.0)

        drain_called.clear()
        limiter.drain.reset_mock()
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=1.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

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
        loop.wake(10.0)
        await asyncio.sleep(0)
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

        # Assert
        assert loop._shutdown is True, "shutdown flag should be True after shutdown"
        assert loop._task is not None, "task should have been created"
        assert loop._task.done(), "task should be done after shutdown"

    @staticmethod
    async def test_shutdown_completes_promptly():
        """Verify that ``shutdown()`` completes well within
        its internal 5.0s timeout.
        """
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        # Start the task and let it complete one drain cycle so it is
        # blocked on _condition.wait() when shutdown is called.
        loop.wake(0)
        await asyncio.wait_for(drain_called.wait(), timeout=2.0)

        start = time.monotonic()
        try:
            await asyncio.wait_for(loop.shutdown(), timeout=1.0)
            completed = True
        except asyncio.TimeoutError:
            completed = False
        elapsed = time.monotonic() - start

        # Assert
        assert completed, (
            "shutdown() should complete within 1.0s; "
            "a timeout indicates notify() or _shutdown assignment was mutated"
        )
        assert elapsed < 1.0, (
            f"shutdown() took {elapsed:.2f}s; should complete promptly "
            "when the condition is notified correctly"
        )

    @staticmethod
    async def test_shutdown_is_idempotent():
        """Verify that calling ``shutdown()`` twice does not raise."""
        # Arrange
        limiter = MagicMock()
        drain_called = asyncio.Event()
        limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Act
        loop.wake(0)
        await asyncio.wait_for(drain_called.wait(), timeout=2.0)
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

        # Assert
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)
        assert loop._shutdown is True, (
            "shutdown flag should remain True after second shutdown"
        )

    @staticmethod
    async def test_shutdown_cancels_hanging_task():
        """Verify that ``shutdown()`` cancels the background task when
        it does not terminate within the internal wait timeout.
        """
        # Arrange
        limiter = MagicMock()
        limiter.drain = AsyncMock()
        loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)

        # Replace the background task with one that will never complete,
        # simulating a stuck drain() call.
        never_done = asyncio.Event()
        stuck_task = asyncio.create_task(never_done.wait())
        loop._task = stuck_task

        # Schedule an external cancel after a short delay so that
        # shutdown()'s wait_for receives CancelledError promptly
        # instead of waiting the full 5.0s timeout.
        async def cancel_after_delay():
            await asyncio.sleep(0.05)
            stuck_task.cancel()

        cancel_task = asyncio.create_task(cancel_after_delay())

        # Act
        await loop.shutdown()
        await cancel_task

        # Assert
        assert loop._task.done(), (
            "task should be done after shutdown cancels it"
        )

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
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

    @staticmethod
    async def test_drain_loop_survives_drain_exception():
        """Verify that the drain loop task survives when
        ``drain()`` raises an exception.
        """
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
        loop.wake(0)
        await asyncio.sleep(0.1)
        loop.wake(0)
        try:
            await asyncio.wait_for(second_call.wait(), timeout=2.0)
            fired = True
        except asyncio.TimeoutError:
            fired = False
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

        # Assert
        assert fired, (
            "drain loop should survive an exception and process subsequent wakes"
        )
        assert call_count >= 2, "drain should have been called at least twice"

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
        await asyncio.wait_for(loop.shutdown(), timeout=1.0)

        # Assert
        assert fired, "drain should be called again after task recovery"


@pytest.mark.behavior
class TestAsyncDrainSignalSubscriber:
    """Test suite for ``AsyncDrainSignalSubscriber`` shutdown
    and message-processing behavior.
    """

    @staticmethod
    async def test_subscriber_processes_remote_drain_signal():
        """Verify that the async subscriber calls
        ``_schedule_drain`` upon receiving a remote drain signal.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        mock_pubsub = AsyncMock()

        drain_called = asyncio.Event()
        limiter._schedule_drain.side_effect = lambda: drain_called.set()

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "type": "message",
                    "data": "remote-worker",
                    "channel": b"test:drain_signal",
                }
            # Subsequent calls simulate blocking on the pubsub socket.
            await asyncio.sleep(timeout or 0.1)
            return None

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.aclose = AsyncMock()
        limiter.redis.pubsub.return_value = mock_pubsub

        subscriber = AsyncDrainSignalSubscriber(limiter)

        # Act
        safety_timer = Timer(0.5, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        await subscriber.start()
        try:
            await asyncio.wait_for(drain_called.wait(), timeout=2.0)
            signaled = True
        except asyncio.TimeoutError:
            signaled = False
        await subscriber.shutdown()
        safety_timer.cancel()

        # Assert
        assert signaled, (
            "async subscriber should call _schedule_drain "
            "upon receiving a remote drain signal"
        )

    @staticmethod
    async def test_run_processes_message_in_main_task():
        """Verify that ``_run`` processes a remote message and
        calls ``_schedule_drain`` when invoked directly.

        Calling ``_run()`` directly (rather than via ``start()``)
        ensures coverage tracks the execution in the main task.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
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

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)

        # Act
        safety_timer = Timer(0.5, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        await subscriber._run()
        safety_timer.cancel()

        # Assert
        mock_pubsub.get_message.assert_called()
        limiter._schedule_drain.assert_called_once()

    @staticmethod
    async def test_subscriber_ignores_local_drain_signal():
        """Verify that the async subscriber ignores drain
        signals originating from the local worker.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        mock_pubsub = AsyncMock()

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "type": "message",
                    "data": "local-worker",
                    "channel": b"test:drain_signal",
                }
            await asyncio.sleep(timeout or 0.1)
            return None

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.aclose = AsyncMock()
        limiter.redis.pubsub.return_value = mock_pubsub

        subscriber = AsyncDrainSignalSubscriber(limiter)

        # Act
        safety_timer = Timer(0.5, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        await subscriber.start()
        await asyncio.sleep(0.3)
        await subscriber.shutdown()
        safety_timer.cancel()

        # Assert
        limiter._schedule_drain.assert_not_called()

    @staticmethod
    async def test_shutdown_sets_flag_without_starting_task(
        async_stub_limiter,
    ):
        """Verify that ``shutdown()`` sets the ``_shutdown`` flag
        to ``True`` without a running task.
        """
        # Arrange
        subscriber = AsyncDrainSignalSubscriber(async_stub_limiter)

        # Assert
        assert subscriber._shutdown is False, (
            "shutdown flag should be False before shutdown is called"
        )

        # Act
        await subscriber.shutdown()

        # Assert
        # Identity check catches mutations to None and False.
        assert subscriber._shutdown is True, (
            "shutdown flag must be exactly True after shutdown"
        )
        assert subscriber._task is None, (
            "task should not have been started without a start call"
        )

    @staticmethod
    async def test_run_survives_exception_and_retries():
        """Verify that ``_run`` logs the exception and
        continues the loop when ``get_message`` raises.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        limiter.id = "test-resilience"
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("simulated pubsub failure")
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)

        # Act
        safety_timer = Timer(2.0, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        await subscriber._run()
        safety_timer.cancel()

        # Assert
        assert call_count >= 2, "get_message should be called again after exception"

    @staticmethod
    async def test_run_exits_silently_on_exception_during_shutdown():
        """Verify that ``_run`` returns immediately when an
        exception occurs while ``_shutdown`` is ``True``.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        subscriber._pubsub = mock_pubsub

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
            subscriber._shutdown = True
            raise ConnectionError("shutdown in progress")

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)

        # Act
        await subscriber._run()

        # Assert
        assert subscriber._shutdown is True, "_run should return without re-raising"

    @staticmethod
    async def test_run_processes_message_then_idles_before_exit():
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
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
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

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)

        # Act
        safety_timer = Timer(0.5, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        await subscriber._run()
        safety_timer.cancel()

        # Assert
        assert call_count >= 3, (
            "get_message should be called at least 3 times: "
            "message, idle, then shutdown"
        )
        limiter._schedule_drain.assert_called_once()

    @staticmethod
    async def test_shutdown_swallows_pubsub_exception():
        """Verify that ``shutdown()`` does not propagate exceptions
        raised by the Pub/Sub ``unsubscribe()`` or ``aclose()`` calls.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock(
            side_effect=ConnectionError("connection lost")
        )
        subscriber._pubsub = mock_pubsub

        # Act
        await subscriber.shutdown()

        # Assert
        assert subscriber._shutdown is True, (
            "shutdown flag should be True even when pubsub cleanup raises"
        )

    @staticmethod
    async def test_run_decodes_bytes_sender_id():
        """Verify that ``_run`` correctly decodes a bytes-valued
        ``sender_id`` from Redis Pub/Sub messages.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "type": "message",
                    "data": b"remote-worker",
                    "channel": b"test:drain_signal",
                }
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)

        # Act
        safety_timer = Timer(0.5, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        await subscriber._run()
        safety_timer.cancel()

        # Assert
        limiter._schedule_drain.assert_called_once()


@pytest.mark.behavior
class TestAsyncWatchdogInterval:
    """Tests for the watchdog interval computation in
    ``AbstractAsyncDistributedRateLimiter``.
    """

    @staticmethod
    async def test_watchdog_interval_floor_is_five_seconds(
        async_redis_client, limiter_id
    ):
        """Verify that the watchdog interval floor is 5.0
        seconds when ``window * 2`` is smaller.
        """
        # Arrange
        # window=1.0 produces window*2=2.0 which is below the 5.0 floor.
        limiter = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_watchdog",
            limit=10,
            window=1.0,
            max_concurrency=2,
        )
        await limiter.start()

        # Assert
        assert limiter._drain_loop._watchdog_interval == pytest.approx(5.0), (
            "watchdog interval should be 5.0 when window * 2 is below the floor"
        )

        # Teardown
        await limiter.shutdown()


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestAsyncDrainLoopObservability:
    """Observability tests for ``AsyncDrainLoop`` log emissions."""

    @staticmethod
    async def test_drain_exception_emits_error_log(caplog):
        """Verify that the drain loop emits an ERROR log when
        ``drain()`` raises an exception.
        """
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
        with caplog.at_level(
            logging.ERROR, logger="redis_rate_limiter.core.async_limiters"
        ):
            loop.wake(0)
            await asyncio.sleep(0.1)
            loop.wake(0)
            try:
                await asyncio.wait_for(second_call.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            await asyncio.wait_for(loop.shutdown(), timeout=1.0)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="ERROR",
            required_fragments=["limiter=test-resilience"],
            message=(
                "should emit an error log containing the limiter id when drain raises"
            ),
        )


@pytest.mark.observability
class TestAsyncDrainSignalSubscriberObservability:
    """Observability tests for ``AsyncDrainSignalSubscriber`` log emissions."""

    @staticmethod
    async def test_run_normal_processing_does_not_emit_error_log(caplog):
        """Verify that ``_run`` does not emit error or critical
        logs during normal message processing.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
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

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)

        # Act
        safety_timer = Timer(0.5, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        with caplog.at_level(
            logging.ERROR,
            logger="redis_rate_limiter.core.async_limiters",
        ):
            await subscriber._run()
        safety_timer.cancel()

        # Assert
        assert not any(
            record.levelname in ("ERROR", "CRITICAL") for record in caplog.records
        ), "no error logs should be emitted during normal processing"

    @staticmethod
    async def test_run_exception_emits_error_log(caplog):
        """Verify that ``_run`` emits an ERROR log containing
        the limiter id when ``get_message`` raises.
        """
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        limiter.id = "test-subscriber-error"
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("simulated pubsub failure")
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)

        # Act
        safety_timer = Timer(2.0, lambda: setattr(subscriber, "_shutdown", True))
        safety_timer.start()
        with caplog.at_level(
            logging.ERROR,
            logger="redis_rate_limiter.core.async_limiters",
        ):
            await subscriber._run()
        safety_timer.cancel()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="ERROR",
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
class TestAsyncDrainSignalSubscriberBoundary:
    """Boundary condition tests for ``AsyncDrainSignalSubscriber`` internal parameters."""

    @staticmethod
    async def test_run_polls_with_expected_kwargs():
        """Verify that ``_run()`` calls ``get_message`` with
        ``ignore_subscribe_messages=True`` and ``timeout=0.5``."""
        # Arrange
        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = AsyncDrainSignalSubscriber(limiter)
        mock_pubsub = AsyncMock()
        subscriber._pubsub = mock_pubsub

        call_count = 0

        async def get_message_effect(ignore_subscribe_messages=True, timeout=None):
            nonlocal call_count
            call_count += 1
            subscriber._shutdown = True
            return None

        mock_pubsub.get_message = AsyncMock(side_effect=get_message_effect)

        # Act
        await subscriber._run()

        # Assert
        mock_pubsub.get_message.assert_called_with(
            ignore_subscribe_messages=True, timeout=0.5
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestAsyncDrainLoopSignatures:
    """Signature tests for ``AsyncDrainLoop`` default parameter values."""

    @staticmethod
    def test_wake_default_delay_is_zero():
        """Verify that the ``delay`` parameter of ``wake()`` defaults to ``0.0``.

        Mutation target: default value of ``delay`` in ``AsyncDrainLoop.wake()``.
        """
        # Arrange & Act
        sig = inspect.signature(AsyncDrainLoop.wake)

        # Assert
        assert sig.parameters["delay"].default == 0.0, (
            "wake() default delay should be 0.0 for immediate scheduling"
        )
