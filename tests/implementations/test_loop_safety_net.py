"""Safety-net tests for the sync limiter's persistent loops.

This file gathers two intent-driven safety-net flavors that detect the
mutation classes documented in ``TESTING_GUIDELINES.md`` Section 5.4:

- **Hang detection**: wall-clock-bounded ``shutdown_completes_within``
  tests verify that each loop's shutdown path terminates promptly. The
  subject under test is constructed with a ``MagicMock`` limiter (or
  comparably mocked I/O) so that no spin-class mutation in the
  surrounding code can amplify the test through real Redis traffic.
- **Spin detection**: ``cap_iterations`` tests install a counting stub
  on the loop's primary I/O primitive and assert on the iteration count
  after a bounded sleep, catching mutations that collapse the loop's
  internal throttle (sleep argument, floor literal, clamp operator) to
  zero or negative values.

Behavioral tests that incidentally use a bounded primitive (e.g.,
``TaskLifecycle.__exit__``) keep the ``timeout_safety_net`` marker but
remain in their natural homes; only tests whose primary purpose is
hang or spin detection live here.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from threading import Event
from unittest.mock import MagicMock, patch

import pytest

from redis_rate_limiter.core.limiters import (
    AbstractDistributedRateLimiter,
    DrainLoop,
    DrainSignalSubscriber,
    HeartbeatScheduler,
)
from tests.helpers.utils import (
    cap_iterations,
    shutdown_completes_within,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _NoopDispatchLimiter(AbstractDistributedRateLimiter):
    """Concrete sync limiter with no-op dispatch for drain-integration safety-net tests."""

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        pass


class _CountingCondition:
    """Wraps a ``threading.Condition`` and counts each ``__enter__`` call so iteration-rate tests can target loops that do not consistently call a single I/O primitive."""

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.count = 0

    def __enter__(self):
        self.count += 1
        return self._wrapped.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._wrapped.__exit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


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


@contextmanager
def _bypassed_execution_lock():
    """Yield ``True`` without touching Redis, so the lock acquire/release does not amplify mutations."""
    yield True


@pytest.fixture
def real_drain_limiter(redis_client, limiter_id):
    """Provide a sync limiter with an active drain loop and no-op dispatch."""
    # Setup
    limiter = _NoopDispatchLimiter(
        redis_client=redis_client,
        limiter_id=f"{limiter_id}_loop_safety",
        **_DRAIN_INTEGRATION_CONFIG,
    )

    yield limiter

    # Teardown
    limiter.shutdown()


# ---------------------------------------------------------------------------
# DrainLoop
# ---------------------------------------------------------------------------


class TestDrainLoopSafetyNet:
    """Hang and spin coverage for ``DrainLoop._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_shutdown_completes_promptly():
        """Verify that ``shutdown()`` completes well within its internal 5.0s join timeout."""
        # Arrange
        limiter = MagicMock()
        drain_called = Event()
        limiter.drain.side_effect = lambda: drain_called.set()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        # Start the thread and let it complete one drain cycle so it is
        # blocked on _condition.wait() when shutdown is called.
        loop.wake(0)
        drain_called.wait(timeout=2.0)

        # Act
        completed = shutdown_completes_within(loop, timeout=1.0)

        # Assert
        assert completed, (
            "shutdown() should complete within 1.0s; "
            "a timeout indicates _shutdown assignment was mutated"
        )
        assert not loop._thread.is_alive(), "thread should be stopped after shutdown"

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_run_throttles_iterations_under_watchdog():
        """Verify that ``_run`` waits the watchdog interval between drain calls when no wake is pending."""
        # Arrange
        limiter = MagicMock()
        loop = DrainLoop(limiter, watchdog_interval=60.0)

        with cap_iterations(limiter, "drain", return_value=None) as count:
            # Act
            loop.wake(0)
            time.sleep(0.5)
            observed = count()
            loop.shutdown()

        # Assert
        assert observed < 50, (
            f"drain loop fired {observed} times in 0.5s with a 60s watchdog; "
            "the watchdog interval clamp has been bypassed"
        )


# ---------------------------------------------------------------------------
# DrainSignalSubscriber
# ---------------------------------------------------------------------------


class TestDrainSignalSubscriberSafetyNet:
    """Hang and spin coverage for ``DrainSignalSubscriber._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_shutdown_completes_promptly(redis_client, limiter_id):
        """Verify that ``shutdown()`` completes well under the 0.5s poll fallback interval."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.redis = redis_client
        limiter._worker_id = f"{limiter_id}_worker"
        subscriber = DrainSignalSubscriber(limiter)
        subscriber.start()

        # Let the listener thread reach its first get_message poll.
        time.sleep(0.05)

        # Act
        completed = shutdown_completes_within(subscriber, timeout=0.1)

        # Assert
        assert completed, (
            "shutdown() should complete within 0.1s; "
            "a timeout indicates _shutdown or the publish-wake was mutated"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_run_throttles_iterations_on_empty_pubsub():
        """Verify that ``_run`` honours its 0.5s poll timeout when no message arrives."""

        # Arrange
        def _sleep_for_requested_timeout(*args, timeout=0.0, **kwargs):
            time.sleep(timeout)
            return None

        limiter = MagicMock()
        limiter._worker_id = "local-worker"
        subscriber = DrainSignalSubscriber(limiter)
        mock_pubsub = MagicMock()
        subscriber._pubsub = mock_pubsub

        with cap_iterations(
            mock_pubsub, "get_message", side_effect=_sleep_for_requested_timeout
        ) as count:
            # Act
            from threading import Thread

            subscriber._shutdown = False
            subscriber._thread = Thread(target=subscriber._run, daemon=True)
            subscriber._thread.start()
            time.sleep(0.5)
            observed = count()
            subscriber._shutdown = True
            subscriber._thread.join(timeout=1.0)

        # Assert
        assert observed < 5, (
            f"subscriber polled get_message {observed} times in 0.5s; "
            "the 0.5s poll timeout literal has been bypassed"
        )


# ---------------------------------------------------------------------------
# HeartbeatScheduler
# ---------------------------------------------------------------------------


class TestHeartbeatSchedulerSafetyNet:
    """Hang and spin coverage for ``HeartbeatScheduler._run``."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_shutdown_completes_promptly(limiter_id):
        """Verify that ``shutdown()`` completes well within its internal 5.0s join timeout."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 60.0
        limiter.extend_lease.return_value = None
        scheduler = HeartbeatScheduler(limiter)
        scheduler.register("task-1", "warn")

        # Act
        completed = shutdown_completes_within(scheduler, timeout=1.0)

        # Assert
        assert completed, (
            "shutdown() should complete within 1.0s; "
            "a timeout indicates _shutdown assignment was mutated"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_run_iteration_rate_is_bounded(limiter_id):
        """Verify that ``_run`` does not tight-loop in any branch (empty heap, due-entry, stale entry)."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 60.0
        limiter.extend_lease.return_value = None
        scheduler = HeartbeatScheduler(limiter)
        counting_condition = _CountingCondition(scheduler._condition)
        scheduler._condition = counting_condition

        # Act
        scheduler.register("task-1", "warn")
        time.sleep(0.5)
        observed = counting_condition.count
        scheduler.shutdown()

        # Assert
        assert observed < 60, (
            f"scheduler entered _condition {observed} times in 0.5s; "
            "_run is iterating without honouring its renewal-interval throttle"
        )


# ---------------------------------------------------------------------------
# Drain rescheduling integration (real limiter + drain loop + mocked consume)
# ---------------------------------------------------------------------------


class TestDrainReschedulingSafetyNet:
    """Spin coverage for the drain loop's reschedule branches (rate-limited, no-capacity, lock-held)."""

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_drain_throttles_iterations_under_rate_limit(real_drain_limiter):
        """Verify drain reschedules with a bounded delay when consume reports rate limited."""
        # Arrange
        with (
            cap_iterations(
                real_drain_limiter, "consume", return_value=_RATE_LIMITED_RESULT
            ) as count,
            patch.object(
                real_drain_limiter, "execution_lock", _bypassed_execution_lock
            ),
        ):
            # Act
            real_drain_limiter.trigger_consume()
            time.sleep(0.5)
            observed = count()

        # Assert
        assert observed < 50, (
            f"drain looped {observed} times in 0.5s under rate limit; "
            "the reschedule throttle (max(0.001, base_delay + jitter)) has been bypassed"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_drain_throttles_iterations_when_local_capacity_exhausted(
        real_drain_limiter,
    ):
        """Verify drain reschedules with a bounded delay when local capacity is exhausted."""
        # Arrange
        real_drain = real_drain_limiter.drain
        with (
            patch.object(real_drain_limiter, "_has_local_capacity", return_value=False),
            cap_iterations(
                real_drain_limiter, "drain", side_effect=real_drain
            ) as count,
        ):
            # Act
            real_drain_limiter.trigger_consume()
            time.sleep(0.5)
            observed = count()

        # Assert
        assert observed < 50, (
            f"drain looped {observed} times in 0.5s with no local capacity; "
            "the _token_interval reschedule has been bypassed"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_drain_throttles_iterations_when_lock_held(real_drain_limiter):
        """Verify drain reschedules with a bounded delay when the dispatch lock is held by another worker."""

        # Arrange
        @contextmanager
        def _lock_held():
            yield False

        real_drain = real_drain_limiter.drain
        with (
            patch.object(real_drain_limiter, "execution_lock", _lock_held),
            cap_iterations(
                real_drain_limiter, "drain", side_effect=real_drain
            ) as count,
        ):
            # Act
            real_drain_limiter.trigger_consume()
            time.sleep(0.5)
            observed = count()

        # Assert
        assert observed < 50, (
            f"drain looped {observed} times in 0.5s with the lock held; "
            "the _schedule_backup_drain throttle has been bypassed"
        )
