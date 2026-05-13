"""Safety-net tests for the sync limiter's persistent loops.

One consolidated test per loop. Each test body runs inside
``completes_within``, ensuring the test itself never hangs under any
mutation. Within the body, ``cap_iterations`` provides spin detection
and ``completes_within`` on shutdown verifies prompt termination. I/O
is mocked throughout. See ``TESTING_GUIDELINES.md`` Section 5.4.

Fixture dependencies:
    - ``limiter_id``: from ``tests/conftest.py``.
"""

from __future__ import annotations

from threading import Event
from unittest.mock import MagicMock, patch

import pytest

from redis_rate_limiter.core import limiters as _limiters_module
from redis_rate_limiter.core.limiters import (
    BackendHealthMonitor,
    DrainLoop,
    DrainSignalSubscriber,
    HeartbeatScheduler,
)
from tests.helpers.utils import (
    cap_iterations,
    completes_within,
)

# ---------------------------------------------------------------------------
# DrainLoop
# ---------------------------------------------------------------------------


class TestDrainLoopSafetyNet:
    """Hang and spin coverage for ``DrainLoop._run`` paths."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_drain_loop_paths_stay_bounded():
        """Verify ``DrainLoop._run`` paths stay bounded and shutdown completes."""

        def body():
            # Arrange
            limiter = MagicMock()
            drain_called = Event()
            limiter.drain.side_effect = lambda: drain_called.set()
            loop = DrainLoop(limiter, watchdog_interval=60.0)
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
                    side_effect=original_wait,
                    cap=10,
                ) as wait_count:
                    loop.wake(0)
                    drain_called.wait(timeout=0.2)
                    loop.wake(0.05)
                    Event().wait(timeout=0.05)
                    drain_observed = drain_count()
                    wait_observed = wait_count()

            completed = completes_within(loop.shutdown, timeout=0.3)

            # Assert
            assert drain_observed < 10, (
                f"DrainLoop drain called {drain_observed} times, spin in loop body"
            )
            assert wait_observed < 10, (
                f"DrainLoop _condition.wait called {wait_observed} times, spin on wait paths"
            )
            assert completed, "DrainLoop.shutdown should complete within 0.3s"
            assert loop._thread is None or not loop._thread.is_alive(), (
                "DrainLoop worker thread must be terminated after shutdown"
            )

        assert completes_within(body, timeout=2.0), (
            "DrainLoop safety-net test did not complete within 2.0s"
        )


# ---------------------------------------------------------------------------
# DrainSignalSubscriber
# ---------------------------------------------------------------------------


class TestDrainSignalSubscriberSafetyNet:
    """Hang and spin coverage for ``DrainSignalSubscriber._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_subscriber_paths_stay_bounded(limiter_id):
        """Verify ``DrainSignalSubscriber._run`` stays bounded across all message and exception branches and shutdown completes."""

        def body():
            # Arrange
            limiter = MagicMock()
            limiter.id = limiter_id
            limiter._worker_id = f"{limiter_id}_self"
            scripted_messages: list[object] = [
                None,
                {"type": "subscribe", "data": "ack"},
                {"type": "message", "data": f"{limiter_id}_self"},
                {"type": "message", "data": "remote-worker"},
                RuntimeError("simulated pubsub failure"),
            ]
            msg_iter = iter(scripted_messages + [None] * 200)

            def fake_get_message(*_args, **_kwargs):
                value = next(msg_iter)
                if isinstance(value, BaseException):
                    raise value
                Event().wait(timeout=0.005)
                return value

            subscriber = DrainSignalSubscriber(limiter)

            # Act
            with patch.object(_limiters_module.time, "sleep", lambda _seconds: None):
                with cap_iterations(
                    subscriber._pubsub,
                    "get_message",
                    side_effect=fake_get_message,
                    cap=30,
                ) as msg_count:
                    subscriber.start()
                    Event().wait(timeout=0.05)
                    observed = msg_count()

                completed = completes_within(subscriber.shutdown, timeout=0.3)

            # Assert
            assert observed < 30, (
                f"subscriber.get_message called {observed} times in 50ms, spin in loop body"
            )
            assert completed, (
                "DrainSignalSubscriber.shutdown should complete within 0.3s"
            )
            assert limiter._schedule_drain.called, (
                "remote-worker message path must call _schedule_drain"
            )
            limiter.redis.publish.assert_called_with(subscriber._channel, "")

        assert completes_within(body, timeout=2.0), (
            "DrainSignalSubscriber safety-net test did not complete within 2.0s"
        )


# ---------------------------------------------------------------------------
# HeartbeatScheduler
# ---------------------------------------------------------------------------


class TestHeartbeatSchedulerSafetyNet:
    """Hang and spin coverage for ``HeartbeatScheduler._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_scheduler_paths_stay_bounded(limiter_id):
        """Verify ``HeartbeatScheduler._run`` stays bounded across registration, renewal, and stale-entry handling."""

        def body():
            # Arrange
            limiter = MagicMock()
            limiter.id = limiter_id
            limiter.lease_duration = 0.05
            limiter.extend_lease.return_value = None
            scheduler = HeartbeatScheduler(limiter)
            original_wait = scheduler._condition.wait
            original_renew_one = scheduler._renew_one

            # Act
            with cap_iterations(
                scheduler._condition,
                "wait",
                side_effect=original_wait,
                cap=30,
            ) as wait_count:
                with cap_iterations(
                    scheduler,
                    "_renew_one",
                    side_effect=original_renew_one,
                    cap=30,
                ) as renew_count:
                    scheduler.register("task-1", "warn")
                    Event().wait(timeout=0.08)
                    scheduler.deregister("task-1")
                    Event().wait(timeout=0.04)
                    wait_observed = wait_count()
                    renew_observed = renew_count()

            completed = completes_within(scheduler.shutdown, timeout=0.5)

            # Assert
            assert wait_observed < 30, (
                f"HeartbeatScheduler waited {wait_observed} times in ~120ms, spin on wait paths"
            )
            assert renew_observed < 30, (
                f"HeartbeatScheduler renewed {renew_observed} times in ~120ms, spin in renewal cycle"
            )
            assert completed, "HeartbeatScheduler.shutdown should complete within 0.5s"
            assert scheduler._shutdown is True, (
                "HeartbeatScheduler._shutdown must be True after shutdown"
            )

        assert completes_within(body, timeout=2.0), (
            "HeartbeatScheduler safety-net test did not complete within 2.0s"
        )


# ---------------------------------------------------------------------------
# BackendHealthMonitor
# ---------------------------------------------------------------------------


class TestBackendHealthMonitorSafetyNet:
    """Hang and spin coverage for ``BackendHealthMonitor._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_monitor_paths_stay_bounded(limiter_id):
        """Verify ``BackendHealthMonitor._run`` stays bounded across the poll-and-check cycle and shutdown completes."""

        def body():
            # Arrange
            limiter = MagicMock()
            limiter.id = limiter_id
            limiter._check_backend_health.return_value = True
            monitor = BackendHealthMonitor(limiter, interval=0.02)
            original_wait = monitor._shutdown_event.wait

            # Act
            with cap_iterations(
                monitor._shutdown_event,
                "wait",
                side_effect=original_wait,
                cap=30,
            ) as count:
                monitor.start()
                Event().wait(timeout=0.08)
                observed = count()

            completed = completes_within(monitor.shutdown, timeout=0.5)

            # Assert
            assert observed < 30, (
                f"BackendHealthMonitor _shutdown_event.wait called {observed} times in 80ms, spin in loop body"
            )
            assert completed, (
                "BackendHealthMonitor.shutdown should complete within 0.5s"
            )

        assert completes_within(body, timeout=2.0), (
            "BackendHealthMonitor safety-net test did not complete within 2.0s"
        )
