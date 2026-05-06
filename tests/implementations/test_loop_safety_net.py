"""Safety-net tests for the sync limiter's persistent loops.

Wall-clock-bounded ``shutdown_completes_within`` tests verify that each
loop's shutdown path terminates promptly. The subject under test is
constructed with a ``MagicMock`` limiter (or comparably mocked I/O) so
that no spin-class mutation in the surrounding code can amplify the test
through real Redis traffic. See ``TESTING_GUIDELINES.md`` Section 5.4
for the hang-vs-spin distinction.

Behavioral tests that incidentally use a bounded primitive (e.g.,
``TaskLifecycle.__exit__``) keep the ``timeout_safety_net`` marker but
remain in their natural homes; only tests whose primary purpose is
hang detection live here.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

from __future__ import annotations

import time
from threading import Event
from unittest.mock import MagicMock

import pytest

from redis_rate_limiter.core.limiters import (
    DrainLoop,
    DrainSignalSubscriber,
    HeartbeatScheduler,
)
from tests.helpers.utils import (
    shutdown_completes_within,
)

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
