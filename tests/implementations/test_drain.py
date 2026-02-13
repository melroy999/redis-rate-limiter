"""Tests for the ``drain()`` and ``trigger_consume()`` branch behavior."""

import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest


class TestDrain:
    """Test suite for branch coverage in ``drain()``."""

    @staticmethod
    @contextmanager
    def lock_result(acquired: bool):
        """Provide a context manager that yields a deterministic lock outcome."""
        yield acquired

    def test_drain_defers_when_paused(self, tracking_limiter):
        """Verify that ``drain()`` defers execution and schedules a follow-up when the limiter is paused."""
        # Arrange
        tracking_limiter._paused_until = time.time() + 0.2
        consume_mock = MagicMock()

        # Act
        with patch.object(tracking_limiter, "consume", consume_mock):
            tracking_limiter.drain()

        # Assert
        assert consume_mock.call_count == 0, "drain should not consume while paused"
        assert len(tracking_limiter.scheduled_drains) == 1, (
            "drain should schedule one follow-up while paused"
        )
        assert tracking_limiter.scheduled_drains[0] > 0.0, (
            "paused follow-up delay should be positive"
        )

    def test_drain_schedules_backup_when_lock_contended(self, tracking_limiter):
        """Verify that ``drain()`` schedules a backup drain when the dispatch lock is not acquired."""
        # Arrange
        consume_mock = MagicMock()

        # Act
        with (
            patch.object(
                tracking_limiter,
                "execution_lock",
                return_value=self.lock_result(False),
            ),
            patch.object(tracking_limiter, "consume", consume_mock),
        ):
            tracking_limiter.drain()

        # Assert
        assert consume_mock.call_count == 0, (
            "drain should not consume when lock is contended"
        )
        assert tracking_limiter.dispatched_tasks == [], (
            "drain should not dispatch when lock is contended"
        )
        assert len(tracking_limiter.scheduled_drains) == 1, (
            "drain should schedule a backup drain when lock is contended"
        )
        expected_delay = tracking_limiter.window / tracking_limiter.limit
        assert tracking_limiter.scheduled_drains[0] == expected_delay, (
            "backup drain delay should be one token interval"
        )

    def test_drain_dispatches_task_and_schedules_follow_up(self, tracking_limiter):
        """Verify that a successful consume dispatches the task and schedules the next drain."""
        # Arrange
        consume_result = {
            "success": True,
            "expired": False,
            "task": {
                "id": "task-1",
                "func_path": "myapp.tasks.work",
                "payload": {"x": 1},
            },
            "remaining_tokens": 4,
            "active_concurrency": 1,
            "reset_in_ms": 100,
            "remaining_tasks": 2,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
        ):
            tracking_limiter.drain()

        # Assert
        assert len(tracking_limiter.dispatched_tasks) == 1, (
            "drain should dispatch exactly one task on successful consume"
        )
        assert tracking_limiter.dispatched_tasks[0]["task_id"] == "task-1", (
            "dispatched task id should match consumed task id"
        )
        assert tracking_limiter.scheduled_drains == [0.0], (
            "drain should schedule an immediate follow-up when tasks remain"
        )

    def test_drain_handles_consume_exception(self, tracking_limiter):
        """Verify that consume exceptions are caught and a recovery drain is scheduled."""
        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(
                tracking_limiter, "consume", side_effect=RuntimeError("consume failed")
            ),
        ):
            tracking_limiter.drain()

        # Assert
        assert tracking_limiter.dispatched_tasks == [], (
            "drain should not dispatch if consume fails"
        )
        assert len(tracking_limiter.scheduled_drains) == 1, (
            "drain should schedule a recovery drain after consume failure"
        )
        assert tracking_limiter.scheduled_drains[0] > 0, (
            "recovery drain delay should be positive"
        )
        assert tracking_limiter._consecutive_drain_failures == 1, (
            "failure counter should be incremented to 1"
        )

    def test_drain_handles_dispatch_exception(self, tracking_limiter):
        """Verify that dispatch exceptions are caught and a recovery drain is scheduled."""
        # Arrange
        consume_result = {
            "success": True,
            "expired": False,
            "task": {
                "id": "task-dispatch-error",
                "func_path": "myapp.tasks.work",
                "payload": {"x": 1},
            },
            "remaining_tokens": 4,
            "active_concurrency": 1,
            "reset_in_ms": 100,
            "remaining_tasks": 2,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
            patch.object(
                tracking_limiter,
                "_dispatch_task",
                side_effect=RuntimeError("dispatch failed"),
            ),
        ):
            tracking_limiter.drain()

        # Assert
        assert len(tracking_limiter.scheduled_drains) == 1, (
            "drain should schedule a recovery drain after dispatch failure"
        )
        assert tracking_limiter.scheduled_drains[0] > 0, (
            "recovery drain delay should be positive"
        )
        assert tracking_limiter._consecutive_drain_failures == 1, (
            "failure counter should be incremented to 1"
        )

    def test_drain_stops_when_buffer_empty(self, tracking_limiter):
        """Verify that ``drain()`` stops without scheduling a follow-up when no tasks remain."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
        ):
            tracking_limiter.drain()

        # Assert
        assert tracking_limiter.dispatched_tasks == [], (
            "drain should not dispatch when buffer is empty"
        )
        assert tracking_limiter.scheduled_drains == [], (
            "drain should not schedule follow-up when buffer is empty"
        )

    def test_drain_handles_expired_task_without_dispatch(self, tracking_limiter):
        """Verify that an expired consume result is neither dispatched nor rescheduled."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": True,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
        ):
            tracking_limiter.drain()

        # Assert
        assert tracking_limiter.dispatched_tasks == [], (
            "drain should not dispatch expired task results"
        )
        assert tracking_limiter.scheduled_drains == [], (
            "drain should not schedule follow-up when expired result has no remaining tasks"
        )

    def test_drain_stops_when_concurrency_at_capacity(self, tracking_limiter):
        """Verify that ``drain()`` stops without scheduling a follow-up when concurrency is saturated."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": tracking_limiter.max_concurrency,
            "reset_in_ms": 100,
            "remaining_tasks": 3,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
        ):
            tracking_limiter.drain()

        # Assert
        assert tracking_limiter.dispatched_tasks == [], (
            "drain should not dispatch when consume is unsuccessful"
        )
        assert tracking_limiter.scheduled_drains == [], (
            "drain should not schedule retry when concurrency is at capacity"
        )

    def test_drain_schedules_delayed_retry_when_rate_limited(self, tracking_limiter):
        """Verify that ``drain()`` schedules a delayed retry when the remaining tokens are exhausted."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 250,
            "remaining_tasks": 4,
            "val_previous": 0,
            "val_current": 5,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
            patch.object(
                tracking_limiter, "_calculate_smart_jitter", return_value=0.0
            ) as mock_jitter,
        ):
            tracking_limiter.drain()

        # Assert
        assert mock_jitter.call_count == 1, (
            "drain should compute jitter for rate-limited retry"
        )
        assert len(tracking_limiter.scheduled_drains) == 1, (
            "drain should schedule one delayed retry when rate-limited"
        )
        assert tracking_limiter.scheduled_drains[0] > 0.0, (
            "rate-limited retry delay should be positive"
        )

    def test_drain_calls_refresh_config_if_available(self, tracking_limiter):
        """Verify that ``drain()`` calls ``refresh_config()`` when the attribute exists."""
        # Arrange
        tracking_limiter.refresh_config = MagicMock()
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
        ):
            tracking_limiter.drain()

        # Assert
        tracking_limiter.refresh_config.assert_called_once()

    def test_drain_resets_failure_counter_on_success(self, tracking_limiter):
        """Verify that the consecutive failure counter resets to zero after a successful drain."""
        # Arrange
        tracking_limiter._consecutive_drain_failures = 3
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
        ):
            tracking_limiter.drain()

        # Assert
        assert tracking_limiter._consecutive_drain_failures == 0, (
            "failure counter should reset to 0 after successful drain"
        )

    def test_drain_backoff_increases_with_consecutive_failures(self, tracking_limiter):
        """Verify that the recovery delay doubles with each consecutive failure."""
        # Act
        with (
            patch.object(
                tracking_limiter,
                "execution_lock",
                side_effect=lambda: self.lock_result(True),
            ),
            patch.object(
                tracking_limiter, "consume", side_effect=RuntimeError("fail")
            ),
        ):
            # Two consecutive failures to verify escalating backoff.
            tracking_limiter.drain()
            tracking_limiter.drain()

        # Assert
        assert tracking_limiter._consecutive_drain_failures == 2, (
            "failure counter should reflect two consecutive failures"
        )
        assert len(tracking_limiter.scheduled_drains) == 2, (
            "each failure should schedule a recovery drain"
        )
        first_delay = tracking_limiter.scheduled_drains[0]
        second_delay = tracking_limiter.scheduled_drains[1]
        assert first_delay == pytest.approx(0.1), (
            "first recovery delay should be 100ms"
        )
        assert second_delay == pytest.approx(0.2), (
            "second recovery delay should be 200ms"
        )

    def test_drain_handles_double_failure_when_schedule_drain_also_fails(
        self, tracking_limiter
    ):
        """Verify that ``drain()`` does not propagate when both the inner drain and recovery scheduling fail."""
        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(
                tracking_limiter, "consume", side_effect=RuntimeError("consume failed")
            ),
            patch.object(
                tracking_limiter,
                "_schedule_drain",
                side_effect=RuntimeError("schedule also failed"),
            ),
        ):
            # This invocation must not raise.
            tracking_limiter.drain()

        # Assert
        assert tracking_limiter._consecutive_drain_failures == 1, (
            "failure counter should be incremented despite double failure"
        )

    def test_drain_skips_jitter_on_token_recovery_path(self, tracking_limiter):
        """Verify that jitter is skipped when the token recovery uses sliding-window decay."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 500,
            "remaining_tasks": 4,
            # Non-fallback path: val_previous > 0 and val_current < limit.
            "val_previous": 5,
            "val_current": 3,
        }

        # Act
        with (
            patch.object(
                tracking_limiter, "execution_lock", return_value=self.lock_result(True)
            ),
            patch.object(tracking_limiter, "consume", return_value=consume_result),
            patch.object(
                tracking_limiter, "_calculate_smart_jitter", return_value=0.0
            ) as mock_jitter,
        ):
            tracking_limiter.drain()

        # Assert
        assert mock_jitter.call_count == 0, (
            "jitter should not be calculated on the token-recovery path"
        )
        assert len(tracking_limiter.scheduled_drains) == 1, (
            "drain should schedule a retry based on pure token-recovery delay"
        )
        assert tracking_limiter.scheduled_drains[0] > 0.0, (
            "token-recovery retry delay should be positive"
        )

    def test_shutdown_delegates_to_drain_loop(self, tracking_limiter):
        """Verify that ``shutdown()`` completes without error on an idle limiter."""
        # Act & Assert
        # This invocation must not raise. The drain loop was never woken because
        # TrackingRateLimiter overrides _schedule_drain; as such, this exercises
        # the delegation path on the base class.
        tracking_limiter.shutdown()

    def test_trigger_consume_schedules_drain(self, tracking_limiter):
        """Verify that ``trigger_consume()`` schedules a drain."""
        # Act
        tracking_limiter.trigger_consume()

        # Assert
        assert len(tracking_limiter.scheduled_drains) == 1, (
            "trigger_consume should schedule exactly one drain"
        )
