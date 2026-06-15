"""Safety-net tests for the async limiter's persistent loops.

Async mirror of ``test_loop_safety_net.py``. Each test body runs inside
``completes_within``, which executes the async body via
``asyncio.run()`` in a daemon thread. This catches event-loop starvation
that in-process mechanisms (``asyncio.wait_for``) cannot detect: if a
spinning task monopolises the loop, ``asyncio.run()`` never returns, the
thread stays alive past the join deadline, and the test fails with an
assertion rather than a mutmut timeout.

Within the body, ``cap_iterations`` provides spin detection. Shutdown is
awaited directly; hang detection is handled by the outer
``completes_within`` wall-clock guard. I/O is mocked throughout. See
``TESTING_GUIDELINES.md`` Section 5.4.

Bounded fakes replace original-method forwarding in ``cap_iterations``
side effects: they ignore mutated timeout arguments and always complete
within a fixed small interval (0.02s), preventing hang-class mutations
from surviving via ``shutdown()`` signal wake-ups.

Fixture dependencies:
    - ``limiter_id``: from ``tests/conftest.py``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from redis_rate_limiter.core import async_limiters as _async_limiters_module
from redis_rate_limiter.core.async_limiters import (
    AsyncBackendHealthMonitor,
    AsyncDrainLoop,
    AsyncDrainSignalSubscriber,
    AsyncHeartbeatScheduler,
)
from tests.helpers.utils import (
    cap_iterations,
    completes_within,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_async_bounded_wait(original, timeout=0.02):
    async def bounded_wait():
        try:
            await asyncio.wait_for(original(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
    return bounded_wait


def _consume_task_exception(task: asyncio.Task | None) -> None:
    """Mark a done task's exception as retrieved to suppress the asyncio GC warning."""
    if task is not None and task.done():
        try:
            task.exception()
        except (asyncio.CancelledError, BaseException):
            pass


# ---------------------------------------------------------------------------
# AsyncDrainLoop
# ---------------------------------------------------------------------------


class TestAsyncDrainLoopSafetyNet:
    """Hang and spin coverage for ``AsyncDrainLoop._run`` paths."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_drain_loop_paths_stay_bounded():
        """Verify ``AsyncDrainLoop._run`` paths stay bounded and shutdown completes."""

        async def body():
            # Arrange
            limiter = MagicMock()
            drain_called = asyncio.Event()
            limiter.drain = AsyncMock(side_effect=lambda: drain_called.set())
            loop = AsyncDrainLoop(limiter, watchdog_interval=60.0)
            original_wait = loop._condition.wait

            # Act
            with cap_iterations(
                limiter,
                "drain",
                side_effect=lambda: drain_called.set(),
                cap=10,
            ) as drain_count:
                with cap_iterations(
                    loop._condition,
                    "wait",
                    side_effect=_make_async_bounded_wait(original_wait),
                    cap=10,
                ) as wait_count:
                    loop.wake(0)
                    await asyncio.wait_for(drain_called.wait(), timeout=0.2)
                    loop.wake(0.05)
                    await asyncio.sleep(0.05)
                    drain_observed = drain_count()
                    wait_observed = wait_count()

            _consume_task_exception(loop._task)
            await loop.shutdown()

            # Assert
            assert drain_observed < 10, (
                f"AsyncDrainLoop drain called {drain_observed} times, spin in loop body"
            )
            assert wait_observed < 10, (
                f"AsyncDrainLoop _condition.wait called {wait_observed} times, spin on wait paths"
            )
            assert loop._task is None or loop._task.done(), (
                "AsyncDrainLoop worker task must be done after shutdown"
            )

        assert completes_within(body, timeout=2.0), (
            "AsyncDrainLoop safety-net test did not complete within 2.0s"
        )


# ---------------------------------------------------------------------------
# AsyncDrainSignalSubscriber
# ---------------------------------------------------------------------------


class TestAsyncDrainSignalSubscriberSafetyNet:
    """Hang and spin coverage for ``AsyncDrainSignalSubscriber._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_subscriber_paths_stay_bounded(limiter_id):
        """Verify ``AsyncDrainSignalSubscriber._run`` stays bounded across all message and exception branches and shutdown completes."""

        async def body():
            # Arrange
            limiter = MagicMock()
            limiter.id = limiter_id
            limiter._worker_id = f"{limiter_id}_self"
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock()
            mock_pubsub.unsubscribe = AsyncMock()
            mock_pubsub.aclose = AsyncMock()
            mock_pubsub.get_message = AsyncMock()
            limiter.redis.pubsub.return_value = mock_pubsub

            scripted_messages: list[object] = [
                None,
                {"type": "subscribe", "data": "ack"},
                {"type": "message", "data": f"{limiter_id}_self"},
                {"type": "message", "data": "remote-worker"},
                RuntimeError("simulated pubsub failure"),
            ]
            msg_iter = iter(scripted_messages + [None] * 200)
            original_sleep = asyncio.sleep

            async def fake_get_message(*_args, **_kwargs):
                value = next(msg_iter)
                if isinstance(value, BaseException):
                    raise value
                await original_sleep(0.005)
                return value

            async def selective_sleep(seconds):
                if seconds >= 1.0:
                    return None
                await original_sleep(seconds)

            subscriber = AsyncDrainSignalSubscriber(limiter)

            # Act
            with patch.object(_async_limiters_module.asyncio, "sleep", selective_sleep):
                with cap_iterations(
                    subscriber._pubsub,
                    "get_message",
                    side_effect=fake_get_message,
                    cap=30,
                ) as msg_count:
                    await subscriber.start()
                    await asyncio.sleep(0.05)
                    observed = msg_count()

                _consume_task_exception(subscriber._task)
                await subscriber.shutdown()

            # Assert
            assert observed < 30, (
                f"subscriber.get_message called {observed} times in 50ms, spin in loop body"
            )
            assert limiter._schedule_drain.called, (
                "remote-worker message path must call _schedule_drain"
            )

        assert completes_within(body, timeout=2.0), (
            "AsyncDrainSignalSubscriber safety-net test did not complete within 2.0s"
        )


# ---------------------------------------------------------------------------
# AsyncHeartbeatScheduler
# ---------------------------------------------------------------------------


class TestAsyncHeartbeatSchedulerSafetyNet:
    """Hang and spin coverage for ``AsyncHeartbeatScheduler._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_scheduler_paths_stay_bounded(limiter_id):
        """Verify ``AsyncHeartbeatScheduler._run`` stays bounded across registration, renewal, and stale-entry handling."""

        async def body():
            # Arrange
            limiter = MagicMock()
            limiter.id = limiter_id
            limiter.lease_duration = 0.05
            limiter.extend_lease = AsyncMock(return_value=None)
            scheduler = AsyncHeartbeatScheduler(limiter)
            original_clear = scheduler._wakeup.clear
            original_wait = scheduler._wakeup.wait
            original_renew_one = scheduler._renew_one

            # Act
            with cap_iterations(
                scheduler._wakeup,
                "clear",
                side_effect=original_clear,
                cap=30,
            ) as clear_count:
                with cap_iterations(
                    scheduler._wakeup,
                    "wait",
                    side_effect=_make_async_bounded_wait(original_wait),
                    cap=30,
                ):
                    with cap_iterations(
                        scheduler,
                        "_renew_one",
                        side_effect=original_renew_one,
                        cap=30,
                    ) as renew_count:
                        await scheduler.register("task-1", "warn")
                        await scheduler.register("task-2", "warn")
                        await asyncio.sleep(0.08)
                        await scheduler.deregister("task-1")
                        await scheduler.deregister("task-2")
                        await asyncio.sleep(0.04)
                        clear_observed = clear_count()
                        renew_observed = renew_count()

            _consume_task_exception(scheduler._task)
            await scheduler.shutdown()

            # Assert
            assert clear_observed < 30, (
                f"AsyncHeartbeatScheduler cleared {clear_observed} times in ~120ms, spin on wait paths"
            )
            assert renew_observed < 30, (
                f"AsyncHeartbeatScheduler renewed {renew_observed} times in ~120ms, spin in renewal cycle"
            )
            assert scheduler._shutdown is True, (
                "AsyncHeartbeatScheduler._shutdown must be True after shutdown"
            )

        assert completes_within(body, timeout=2.0), (
            "AsyncHeartbeatScheduler safety-net test did not complete within 2.0s"
        )


# ---------------------------------------------------------------------------
# AsyncBackendHealthMonitor
# ---------------------------------------------------------------------------


class TestAsyncBackendHealthMonitorSafetyNet:
    """Hang and spin coverage for ``AsyncBackendHealthMonitor._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_monitor_paths_stay_bounded(limiter_id):
        """Verify ``AsyncBackendHealthMonitor._run`` stays bounded across the poll-and-check cycle and shutdown completes."""

        async def body():
            # Arrange
            limiter = MagicMock()
            limiter.id = limiter_id
            limiter._check_backend_health = AsyncMock(return_value=True)
            monitor = AsyncBackendHealthMonitor(limiter, interval=0.02)
            original_wait = monitor._shutdown_event.wait

            # Act
            with cap_iterations(
                monitor._shutdown_event,
                "wait",
                side_effect=_make_async_bounded_wait(original_wait),
                cap=30,
            ) as count:
                monitor.start()
                await asyncio.sleep(0.08)
                observed = count()

            _consume_task_exception(monitor._task)
            monitor._shutdown_event.set()
            await monitor.shutdown()

            # Assert
            assert observed < 30, (
                f"AsyncBackendHealthMonitor _shutdown_event.wait called {observed} times in 80ms, spin in loop body"
            )

        assert completes_within(body, timeout=2.0), (
            "AsyncBackendHealthMonitor safety-net test did not complete within 2.0s"
        )
