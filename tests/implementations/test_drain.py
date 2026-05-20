"""Tests for ``drain()`` and ``trigger_consume()`` behavior and observability.

Tests are written once in async form using a mixin pattern.
The sync variant participates via ``SyncToAsyncLimiterAdapter``;
the async variant runs natively. Fixture mixins
(``_SyncDrainFixture``, ``_AsyncDrainFixture``) supply the
variant-specific fixtures and customization points.
"""

import inspect
import logging
import time
from contextlib import asynccontextmanager, contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from redis_rate_limiter.core.async_limiters import (
    AbstractAsyncDistributedRateLimiter,
)
from redis_rate_limiter.core.limiters import AbstractDistributedRateLimiter
from tests.helpers.adapters import SyncToAsyncLimiterAdapter
from tests.helpers.utils import assert_log_emitted, assert_log_emitted_with_exc_info
from tests.implementations.conftest import (
    AsyncStubRateLimiter,
    AsyncTrackingRateLimiter,
    TrackingRateLimiter,
)

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class DrainBehaviorTests:
    """Behavioral test mixin for ``drain()`` branch coverage.

    Concrete test classes compose this mixin with a fixture mixin
    (``_SyncDrainFixture`` or ``_AsyncDrainFixture``) that supplies
    ``limiter``, ``mock_target``, ``lock_result``, and ``_mock_cls``.
    """

    _mock_cls = None

    @staticmethod
    def lock_result(acquired):
        """Override in subclass to return a sync or async context manager."""
        raise NotImplementedError

    @staticmethod
    async def test_drain_defers_when_paused(limiter, mock_target):
        """Verify that ``drain()`` defers execution and schedules
        a follow-up when the limiter is paused."""
        # Arrange
        mock_target._drain_paused_until = time.time() + 0.2
        consume_mock = MagicMock()

        # Act
        with patch.object(mock_target, "consume", consume_mock):
            await limiter.drain()

        # Assert
        assert consume_mock.call_count == 0, "drain should not consume while paused"
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule one follow-up while paused"
        )
        assert limiter.scheduled_drains[0] > 0.0, (
            "paused follow-up delay should be positive"
        )

    async def test_drain_schedules_backup_when_lock_contended(
        self, limiter, mock_target
    ):
        """Verify that ``drain()`` schedules a backup drain
        when the dispatch lock is not acquired."""
        # Arrange
        consume_mock = MagicMock()

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(False),
            ),
            patch.object(mock_target, "consume", consume_mock),
        ):
            await limiter.drain()

        # Assert
        assert consume_mock.call_count == 0, (
            "drain should not consume when lock is contended"
        )
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when lock is contended"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a backup drain when lock is contended"
        )
        expected_delay = limiter.window / limiter.limit
        assert limiter.scheduled_drains[0] == pytest.approx(expected_delay), (
            "backup drain delay should be one token interval"
        )

    async def test_drain_dispatches_task_and_schedules_follow_up(
        self, limiter, mock_target
    ):
        """Verify that a successful consume dispatches the task
        and schedules the next drain."""
        # Arrange
        consume_result = {
            "success": True,
            "expired": False,
            "marker_skipped": False,
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
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert len(limiter.dispatched_tasks) == 1, (
            "drain should dispatch exactly one task on successful consume"
        )
        assert limiter.dispatched_tasks[0]["task_id"] == "task-1", (
            "dispatched task id should match consumed task id"
        )
        assert limiter.dispatched_tasks[0]["func_path"] == "myapp.tasks.work", (
            "dispatched func_path should match the scheduled task"
        )
        assert limiter.dispatched_tasks[0]["payload"] == {"x": 1}, (
            "dispatched payload should match the scheduled task"
        )
        assert limiter.scheduled_drains == [0.0], (
            "drain should schedule an immediate follow-up when tasks remain"
        )

    async def test_drain_stops_when_buffer_empty(self, limiter, mock_target):
        """Verify that ``drain()`` stops without scheduling
        a follow-up when no tasks remain."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when buffer is empty"
        )
        assert limiter.scheduled_drains == [], (
            "drain should not schedule follow-up when buffer is empty"
        )

    async def test_drain_handles_expired_task_without_dispatch(
        self, limiter, mock_target
    ):
        """Verify that an expired consume result is neither
        dispatched nor rescheduled."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": True,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch expired task results"
        )
        assert limiter.scheduled_drains == [], (
            "drain should not schedule follow-up when expired "
            "result has no remaining tasks"
        )

    async def test_drain_stops_when_concurrency_at_capacity(self, limiter, mock_target):
        """Verify that ``drain()`` stops without scheduling
        a follow-up when concurrency is saturated."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": limiter.max_concurrency,
            "reset_in_ms": 100,
            "remaining_tasks": 3,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when consume is unsuccessful"
        )
        assert limiter.scheduled_drains == [], (
            "drain should not schedule retry when concurrency is at capacity"
        )

    async def test_drain_schedules_delayed_retry_when_rate_limited(
        self, limiter, mock_target
    ):
        """Verify that ``drain()`` schedules a delayed retry
        when the remaining tokens are exhausted."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
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
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_calculate_smart_jitter",
                return_value=0.0,
            ) as mock_jitter,
        ):
            await limiter.drain()

        # Assert
        assert mock_jitter.call_count == 1, (
            "drain should compute jitter for rate-limited retry"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule one delayed retry when rate-limited"
        )
        assert limiter.scheduled_drains[0] == pytest.approx(0.25), (
            "rate-limited retry delay should be "
            "reset_in_ms/1000 = 0.25 on fallback path"
        )

    async def test_drain_calls_refresh_config_if_available(self, limiter, mock_target):
        """Verify that ``drain()`` calls ``refresh_config()``
        when the attribute exists."""
        # Arrange
        mock_target.refresh_config = self._mock_cls()
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        mock_target.refresh_config.assert_called_once()

    async def test_drain_resets_failure_counter_on_success(self, limiter, mock_target):
        """Verify that the consecutive failure counter resets
        to zero after a successful drain."""
        # Arrange
        mock_target._consecutive_drain_failures = 3
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert limiter._consecutive_drain_failures == 0, (
            "failure counter should reset to 0 after successful drain"
        )

    async def test_drain_backoff_increases_with_consecutive_failures(
        self, limiter, mock_target
    ):
        """Verify that the recovery delay doubles with each consecutive failure."""
        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                side_effect=lambda: self.lock_result(True),
            ),
            patch.object(
                mock_target,
                "consume",
                side_effect=RuntimeError("fail"),
            ),
        ):
            await limiter.drain()
            await limiter.drain()

        # Assert
        assert limiter._consecutive_drain_failures == 2, (
            "failure counter should reflect two consecutive failures"
        )
        assert len(limiter.scheduled_drains) == 2, (
            "each failure should schedule a recovery drain"
        )
        first_delay = limiter.scheduled_drains[0]
        second_delay = limiter.scheduled_drains[1]
        assert first_delay == pytest.approx(0.1), "first recovery delay should be 100ms"
        assert second_delay == pytest.approx(0.2), (
            "second recovery delay should be 200ms"
        )

    async def test_drain_dispatches_last_task_without_follow_up(
        self, limiter, mock_target
    ):
        """Verify that drain does not schedule a follow-up
        after dispatching the last task."""
        # Arrange
        consume_result = {
            "success": True,
            "expired": False,
            "marker_skipped": False,
            "task": {
                "id": "last-task",
                "func_path": "myapp.tasks.work",
                "payload": {"x": 1},
            },
            "remaining_tokens": 4,
            "active_concurrency": 1,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert len(limiter.dispatched_tasks) == 1, (
            "drain should dispatch the task even when it is the last one"
        )
        assert limiter.dispatched_tasks[0]["task_id"] == "last-task", (
            "dispatched task id should match the consumed task"
        )
        assert limiter.scheduled_drains == [], (
            "drain should not schedule a follow-up when no tasks remain after dispatch"
        )

    async def test_drain_stops_after_expired_task_with_remaining_tasks(
        self, limiter, mock_target
    ):
        """Verify that drain stops without follow-up when an
        expired task leaves remaining tasks."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": True,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 3,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when consume reports an expired task"
        )
        assert limiter.scheduled_drains == [], (
            "drain should not schedule a follow-up in the "
            "expired-with-remaining fall-through path"
        )

    async def test_drain_handles_double_failure_when_schedule_drain_also_fails(
        self, limiter, mock_target
    ):
        """Verify that ``drain()`` does not propagate when both
        the inner drain and recovery scheduling fail."""
        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(
                mock_target,
                "consume",
                side_effect=RuntimeError("consume failed"),
            ),
            patch.object(
                mock_target,
                "_schedule_drain",
                side_effect=RuntimeError("schedule also failed"),
            ),
        ):
            await limiter.drain()

        # Assert
        assert limiter._consecutive_drain_failures == 1, (
            "failure counter should be incremented despite double failure"
        )

    async def test_drain_skips_jitter_on_token_recovery_path(
        self, limiter, mock_target
    ):
        """Verify that jitter is skipped when the token
        recovery uses sliding-window decay."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
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
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_calculate_smart_jitter",
                return_value=0.0,
            ) as mock_jitter,
        ):
            await limiter.drain()

        # Assert
        assert mock_jitter.call_count == 0, (
            "jitter should not be calculated on the token-recovery path"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a retry based on pure token-recovery delay"
        )
        assert limiter.scheduled_drains[0] == pytest.approx(0.001), (
            "token-recovery retry delay should be 0.001 "
            "when decay has already freed a token"
        )

    @staticmethod
    async def test_shutdown_delegates_to_drain_loop(limiter):
        """Verify that ``shutdown()`` completes without error on an idle limiter."""
        # Act & Assert
        await limiter.shutdown()

    @staticmethod
    async def test_drain_defers_when_local_capacity_full(limiter, mock_target):
        """Verify that ``drain()`` defers execution when local capacity is exhausted."""
        # Arrange
        consume_mock = MagicMock()

        # Act
        with (
            patch.object(mock_target, "_has_local_capacity", return_value=False),
            patch.object(mock_target, "consume", consume_mock),
        ):
            await limiter.drain()

        # Assert
        assert consume_mock.call_count == 0, (
            "drain should not consume when local capacity is full"
        )
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when local capacity is full"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a retry when local capacity is full"
        )
        expected_delay = limiter.window / limiter.limit
        assert limiter.scheduled_drains[0] == pytest.approx(expected_delay), (
            "local-capacity-full retry delay should equal one token interval"
        )

    @staticmethod
    async def test_drain_loop_watchdog_interval(limiter):
        """Verify that the watchdog interval is ``max(5.0, window * 2)``."""
        expected = max(5.0, limiter.window * 2)
        actual = limiter._drain_loop._watchdog_interval
        assert actual == expected, (
            f"watchdog interval should be "
            f"max(5.0, window * 2) = {expected}, "
            f"got {actual}"
        )

    @staticmethod
    async def test_trigger_consume_schedules_drain(limiter):
        """Verify that ``trigger_consume()`` schedules a drain."""
        # Act
        await limiter.trigger_consume()

        # Assert
        assert len(limiter.scheduled_drains) == 1, (
            "trigger_consume should schedule exactly one drain"
        )


# ---------------------------------------------------------------------------
# Unified observability tests
# ---------------------------------------------------------------------------


class DrainObservabilityTests:
    """Observability test mixin for ``drain()`` log emissions.

    Mirrors the behavioral scenarios in ``DrainBehaviorTests``
    but asserts exclusively on log output. Uses the same fixture
    mixins for variant-specific customization points.

    Subclasses must set ``_log_label`` to the expected log prefix
    (e.g., ``"[DistributedRateLimiter]"``).
    """

    _log_label: str

    async def test_drain_loop_start_emits_debug_log(self, limiter, mock_target, caplog):
        """Verify that ``_drain_inner`` emits a debug log at loop start."""
        # Arrange
        # Buffer-empty path is the simplest way to enter _drain_inner.
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[f"limiter={limiter.id}", "loop start"],
            message="should emit a debug log at drain loop start",
        )

    async def test_drain_paused_emits_debug_log(self, limiter, mock_target, caplog):
        """Verify that ``drain()`` emits a debug log when deferred due to pause."""
        # Arrange
        mock_target._drain_paused_until = time.time() + 0.2

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            with patch.object(mock_target, "consume", MagicMock()):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[f"limiter={limiter.id}", "is paused"],
            message=(
                "should emit a debug log indicating the drain is deferred due to pause"
            ),
        )

    async def test_drain_lock_acquired_emits_debug_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``_drain_inner`` emits a debug log with the lock acquisition result."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "acquired=True",
            ],
            message=("should emit a debug log with the lock acquisition result"),
        )

    async def test_drain_lock_contended_emits_debug_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` emits debug logs when
        the dispatch lock is contended."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(False),
                ),
                patch.object(mock_target, "consume", MagicMock()),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "held",
            ],
            message=(
                "should emit a debug log indicating the drain was "
                "skipped because the lock is held by another drainer"
            ),
        )
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[f"limiter={limiter.id}", "delay_s=12.000"],
            message=(
                "should emit a debug log for the backup drain with limiter id and delay"
            ),
        )

    async def test_drain_dispatch_emits_expected_logs(
        self, limiter, mock_target, caplog
    ):
        """Verify that a successful dispatch emits an info
        log and a follow-up debug log."""
        # Arrange
        consume_result = {
            "success": True,
            "expired": False,
            "marker_skipped": False,
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
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "task_id=task-1",
                "func_path=myapp.tasks.work",
            ],
            message=(
                "should emit an info log for the dispatched "
                "task with limiter id, task id, and func path"
            ),
        )
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[f"limiter={limiter.id}", "follow-up"],
            message="should emit a debug log for follow-up drain scheduling",
        )

    async def test_drain_consume_exception_emits_error_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that a consume exception emits an error
        log with the attempt number."""
        # Act
        with caplog.at_level(logging.ERROR, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(
                    mock_target,
                    "consume",
                    side_effect=RuntimeError("consume failed"),
                ),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted_with_exc_info(
            caplog.records,
            level="ERROR",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "attempt #1",
                "in 0.100s",
            ],
            message=(
                "should emit an error log containing the "
                "limiter id, failure attempt number, and recovery delay"
            ),
        )

    async def test_drain_double_failure_emits_critical_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` emits a CRITICAL log when
        both consume and recovery scheduling fail."""
        # Act
        with caplog.at_level(logging.CRITICAL, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(
                    mock_target,
                    "consume",
                    side_effect=RuntimeError("consume failed"),
                ),
                patch.object(
                    mock_target,
                    "_schedule_drain",
                    side_effect=RuntimeError("schedule also failed"),
                ),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted_with_exc_info(
            caplog.records,
            level="CRITICAL",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
            ],
            message=(
                "should emit a critical log when both "
                "drain and recovery scheduling fail"
            ),
        )

    async def test_drain_expired_task_emits_warning_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that an expired consume result emits a
        warning log mentioning the DLQ."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": True,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with caplog.at_level(logging.WARNING, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            label=self._log_label,
            required_fragments=[f"limiter={limiter.id}", "DLQ"],
            message=(
                "should emit a warning log indicating "
                "the expired task was moved to the DLQ"
            ),
        )

    async def test_drain_concurrency_at_capacity_emits_debug_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` emits a debug log when concurrency is at capacity."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": limiter.max_concurrency,
            "reset_in_ms": 100,
            "remaining_tasks": 3,
        }

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                f"active={limiter.max_concurrency}",
                f"max={limiter.max_concurrency}",
            ],
            message=(
                "should emit a debug log indicating "
                "concurrency is at capacity with "
                "active and max counts"
            ),
        )

    async def test_drain_buffer_empty_emits_debug_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``_drain_inner`` emits a debug log when the buffer is empty."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[f"limiter={limiter.id}", "buffer empty"],
            message=(
                "should emit a debug log indicating the "
                "drain stopped because the buffer is empty"
            ),
        )

    async def test_drain_rate_limited_emits_info_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` emits an info log when
        scheduling a rate-limited retry."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 250,
            "remaining_tasks": 4,
            "val_previous": 0,
            "val_current": 5,
        }

        # Act
        with caplog.at_level(logging.INFO, logger="redis_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
                patch.object(
                    mock_target,
                    "_calculate_smart_jitter",
                    return_value=0.0,
                ),
            ):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "delay_s=0.250",
                "base_delay_s=0.250",
                "jitter_s=0.000",
                "remaining_tasks=4",
                "val_previous=0",
                "val_current=5",
                "fallback=True",
            ],
            message=(
                "should emit an info log for the rate-limited retry with all parameters"
            ),
        )

    async def test_drain_fallback_jitter_when_current_at_limit(
        self, limiter, mock_target
    ):
        """Verify that fallback jitter is applied when ``val_current >= limit``
        even if ``val_previous > 0``.

        Mutation target: ``or`` in ``is_fallback = val_previous <= 0 or val_current >= self.limit``.
        """
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 250,
            "remaining_tasks": 4,
            "val_previous": 3,
            "val_current": 5,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_calculate_smart_jitter",
                return_value=0.05,
            ) as mock_jitter,
        ):
            await limiter.drain()

        # Assert
        assert mock_jitter.call_count == 1, (
            "smart jitter should be called when val_current >= limit"
        )

    async def test_publish_drain_signal_emits_debug_log_on_failure(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``_publish_drain_signal()`` emits a
        debug log when Redis publish fails."""
        # Arrange
        with patch.object(
            mock_target.redis, "publish", side_effect=Exception("publish boom")
        ):
            # Act
            with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
                await limiter.trigger_consume()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "Failed",
                "publish",
            ],
            message="should emit a debug log when redis publish raises an exception",
        )

    async def test_drain_deferred_local_capacity_emits_debug_log(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` emits a debug log when local
        execution capacity is exhausted."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            with patch.object(mock_target, "_has_local_capacity", return_value=False):
                await limiter.drain()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "capacity reached",
            ],
            message=(
                "should emit a debug log when drain is "
                "deferred due to local execution capacity"
            ),
        )

    async def test_trigger_consume_emits_debug_log(self, limiter, caplog):
        """Verify that ``trigger_consume()`` emits a debug log."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            await limiter.trigger_consume()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "Trigger consume",
            ],
            message="should emit a debug log when trigger_consume is called",
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class _SyncDrainFixture:
    """Shared fixture mixin for sync drain tests."""

    _mock_cls = MagicMock

    @staticmethod
    @contextmanager
    def lock_result(acquired):
        """Yield a deterministic lock outcome."""
        yield acquired

    @pytest.fixture
    def limiter(self, tracking_limiter):
        """Wrap the sync tracking limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(tracking_limiter)

    @pytest.fixture
    def mock_target(self, tracking_limiter):
        """Return the inner sync limiter as the patch target."""
        return tracking_limiter


class _AsyncDrainFixture:
    """Shared fixture mixin for async drain tests."""

    _mock_cls = AsyncMock

    @staticmethod
    @asynccontextmanager
    async def lock_result(acquired):
        """Yield a deterministic lock outcome."""
        yield acquired

    @pytest.fixture
    def limiter(self, async_tracking_limiter):
        """Provide the async tracking limiter directly."""
        return async_tracking_limiter

    @pytest.fixture
    def mock_target(self, async_tracking_limiter):
        """Return the async limiter as the patch target."""
        return async_tracking_limiter


@pytest.mark.behavior
class TestSyncDrainBehavior(_SyncDrainFixture, DrainBehaviorTests):
    """Sync drain behavior via the async adapter."""


@pytest.mark.behavior
class TestAsyncDrainBehavior(_AsyncDrainFixture, DrainBehaviorTests):
    """Async drain behavior exercised natively."""


# ---------------------------------------------------------------------------
# Boundary tests
# ---------------------------------------------------------------------------


class DrainBoundaryTests:
    """Boundary condition tests for ``drain()`` decision logic.

    Concrete test classes compose this mixin with a fixture mixin
    (``_SyncDrainFixture`` or ``_AsyncDrainFixture``) that supplies
    ``limiter``, ``mock_target``, ``lock_result``, and ``_mock_cls``.
    """

    async def test_drain_schedules_followup_when_exactly_one_task_remains(
        self, limiter, mock_target
    ):
        """Verify that ``drain()`` schedules a follow-up when
        exactly one task remains after dispatch."""
        # Arrange
        consume_result = {
            "success": True,
            "expired": False,
            "marker_skipped": False,
            "task": {
                "id": "task-boundary",
                "func_path": "myapp.tasks.work",
                "payload": {"x": 1},
            },
            "remaining_tokens": 4,
            "active_concurrency": 1,
            "reset_in_ms": 100,
            "remaining_tasks": 1,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert len(limiter.dispatched_tasks) == 1, "drain should dispatch the task"
        assert limiter.scheduled_drains == [0.0], (
            "drain should schedule an immediate follow-up when exactly one task remains"
        )

    async def test_drain_delay_is_base_plus_jitter_rounded_to_three_decimals(
        self, limiter, mock_target
    ):
        """Verify that the rate-limited retry delay equals
        ``round(max(0.001, base_delay + jitter), 3)``."""
        # Arrange
        # val_previous=0 triggers the fallback path, so
        # base_delay = reset_in_ms / 1000.0 = 0.123.
        # Jitter is pinned because it uses random.random() internally.
        pinned_jitter = 0.05
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 123,
            "remaining_tasks": 4,
            "val_previous": 0,
            "val_current": 5,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_calculate_smart_jitter",
                return_value=pinned_jitter,
            ),
        ):
            await limiter.drain()

        # Assert
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule exactly one retry"
        )
        # base_delay = 0.123, jitter = 0.05, sum = 0.173.
        expected_delay = round(0.123 + pinned_jitter, 3)
        assert limiter.scheduled_drains[0] == expected_delay, (
            f"delay should be round(base + jitter, 3) = {expected_delay},"
            f" got {limiter.scheduled_drains[0]}"
        )

    async def test_drain_passes_consume_result_values_to_jitter_calculator(
        self, limiter, mock_target
    ):
        """Verify that ``_calculate_smart_jitter`` receives the
        actual consume result values, not defaults or None."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 250,
            "remaining_tasks": 7,
            "val_previous": 0,
            "val_current": 5,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_calculate_smart_jitter",
                return_value=0.0,
            ) as mock_jitter,
        ):
            await limiter.drain()

        # Assert
        mock_jitter.assert_called_once_with(
            remaining_tasks=7,
            remaining_tokens=0,
            active_concurrency=1,
        )

    async def test_drain_stops_when_concurrency_equals_max(self, limiter, mock_target):
        """Verify that ``drain()`` does not schedule a follow-up
        when active concurrency equals max concurrency."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 3,
            "active_concurrency": mock_target.max_concurrency,
            "reset_in_ms": 100,
            "remaining_tasks": 5,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert limiter.scheduled_drains == [], (
            "drain should not schedule a follow-up when concurrency is at capacity"
        )

    async def test_drain_skips_jitter_on_token_recovery_path(
        self, limiter, mock_target
    ):
        """Verify that jitter is not applied when the token recovery
        delay is computed from previous-window decay (non-fallback)."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 500,
            "remaining_tasks": 3,
            "val_previous": 5,
            "val_current": 3,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_calculate_smart_jitter",
                return_value=0.1,
            ) as mock_jitter,
        ):
            await limiter.drain()

        # Assert
        mock_jitter.assert_not_called()
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a recovery retry"
        )

    async def test_drain_schedules_recovery_when_remaining_tokens_exactly_zero(
        self, limiter, mock_target
    ):
        """Verify that ``drain()`` enters the rate-limited recovery
        path when ``remaining_tokens`` is exactly zero."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "marker_skipped": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 200,
            "remaining_tasks": 3,
            "val_previous": 0,
            "val_current": 5,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_calculate_smart_jitter",
                return_value=0.0,
            ),
        ):
            await limiter.drain()

        # Assert
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a recovery when remaining_tokens is exactly zero"
        )
        assert limiter.scheduled_drains[0] > 0, "recovery delay should be positive"


@pytest.mark.behavior
class TestSyncDrainBoundary(_SyncDrainFixture, DrainBoundaryTests):
    """Sync drain boundary conditions via the async adapter."""


@pytest.mark.behavior
class TestAsyncDrainBoundary(_AsyncDrainFixture, DrainBoundaryTests):
    """Async drain boundary conditions exercised natively."""


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestSyncDrainObservability(_SyncDrainFixture, DrainObservabilityTests):
    """Sync drain observability via the async adapter."""

    _log_label = "[TrackingRateLimiter]"


@pytest.mark.observability
class TestAsyncDrainObservability(_AsyncDrainFixture, DrainObservabilityTests):
    """Async drain observability exercised natively."""

    _log_label = "[AsyncTrackingRateLimiter]"


# ---------------------------------------------------------------------------
# Drain-disabled tests
# ---------------------------------------------------------------------------


class DrainDisabledTests:
    """Tests for scheduler-only mode (``drain_enabled=False``).

    Subclasses must provide a ``limiter_factory`` fixture that returns
    a factory ``(**kwargs) -> limiter`` for creating limiters.
    """

    @staticmethod
    async def test_schedule_drain_is_noop_when_drain_disabled(limiter_factory):
        """Verify that ``_schedule_drain()`` is a no-op when ``drain_enabled=False``."""
        # Arrange
        limiter = await limiter_factory(drain_enabled=False)

        # Assert
        assert limiter._drain_loop is None, (
            "drain loop should not be created when drain_enabled=False"
        )
        assert limiter.drain_enabled is False, (
            "drain_enabled should be False when explicitly disabled"
        )

        # Act
        await limiter.trigger_consume()

        # Assert
        assert limiter._drain_loop is None, (
            "drain loop must remain None when drain_enabled=False"
        )

    @staticmethod
    async def test_shutdown_is_safe_when_drain_disabled(limiter_factory):
        """Verify that ``shutdown()`` does not raise when ``drain_enabled=False``."""
        # Arrange
        limiter = await limiter_factory(drain_enabled=False)

        # Act & Assert
        await limiter.shutdown()

    @staticmethod
    async def test_shutdown_twice_does_not_raise_when_drain_disabled(limiter_factory):
        """Verify that calling ``shutdown()`` twice does not
        raise when ``drain_enabled=False``."""
        # Arrange
        limiter = await limiter_factory(drain_enabled=False)

        # Act
        await limiter.shutdown()

        # Assert
        await limiter.shutdown()


@pytest.mark.behavior
class TestSyncDrainDisabled(DrainDisabledTests):
    """Sync scheduler-only mode exercised through the async adapter."""

    @pytest.fixture
    def limiter_factory(self, redis_client, limiter_id):
        """Factory that creates sync limiters wrapped in the async adapter."""

        async def _factory(**kwargs):
            defaults = dict(
                redis_client=redis_client,
                limiter_id=f"{limiter_id}_drain_disabled",
                limit=5,
                window=60,
                max_concurrency=2,
            )
            defaults.update(kwargs)
            return SyncToAsyncLimiterAdapter(TrackingRateLimiter(**defaults))

        return _factory


@pytest.mark.behavior
class TestAsyncDrainDisabled(DrainDisabledTests):
    """Async scheduler-only mode exercised natively."""

    @pytest.fixture
    def limiter_factory(self, async_redis_client, limiter_id):
        """Factory that creates async limiters."""

        async def _factory(**kwargs):
            defaults = dict(
                redis_client=async_redis_client,
                limiter_id=f"{limiter_id}_async_drain_disabled",
                limit=5,
                window=60,
                max_concurrency=2,
            )
            defaults.update(kwargs)
            lim = AsyncStubRateLimiter(**defaults)
            await lim.start()
            return lim

        return _factory


@pytest.mark.behavior
class TestAsyncScheduleDrainDelegation:
    """Tests for the base-class ``_schedule_drain`` implementation on async limiters.

    The tracking and minimal async limiters override ``_schedule_drain`` with
    no-ops, so the base-class guard (``if self._drain_loop is not None``) is
    never exercised through normal fixtures. These tests call the base-class
    method directly to verify the delegation logic.
    """

    @staticmethod
    async def test_schedule_drain_delegates_to_drain_loop_wake(
        async_stub_limiter,
    ):
        """Verify that the base-class ``_schedule_drain``
        delegates to ``_drain_loop.wake()``."""
        # Arrange
        mock_loop = MagicMock()

        # Act
        with patch.object(async_stub_limiter, "_drain_loop", mock_loop):
            AbstractAsyncDistributedRateLimiter._schedule_drain(
                async_stub_limiter, delay=1.5
            )

        # Assert
        mock_loop.wake.assert_called_once_with(1.5)

    @staticmethod
    async def test_schedule_drain_is_noop_when_drain_loop_is_none(
        async_stub_limiter,
    ):
        """Verify that the base-class ``_schedule_drain`` is
        a no-op when ``_drain_loop`` is ``None``."""
        # Act & Assert
        with patch.object(async_stub_limiter, "_drain_loop", None):
            AbstractAsyncDistributedRateLimiter._schedule_drain(async_stub_limiter)


@pytest.mark.behavior
class TestCrossProcessDrainSignal:
    """Tests for the Redis Pub/Sub cross-process drain notification mechanism.

    These tests are inherently sync-specific: they use ``redis_client.pubsub()``
    and threading-based subscribers.
    """

    @staticmethod
    def test_trigger_consume_publishes_drain_signal(redis_client, limiter_id):
        """Verify that ``trigger_consume()`` publishes a
        drain signal to the Pub/Sub channel."""
        # Arrange
        limiter_id = f"{limiter_id}_pubsub"
        channel = f"{limiter_id}:drain_signal"
        test_sub = redis_client.pubsub()
        test_sub.subscribe(channel)
        test_sub.get_message(timeout=1.0)  # consume the subscribe confirmation

        limiter = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )

        try:
            # Act
            limiter.trigger_consume()

            # Assert
            message = test_sub.get_message(timeout=2.0)
            assert message is not None, "trigger_consume should publish a drain signal"
            assert message["type"] == "message", (
                "received message must be of type 'message'"
            )
            assert message["data"] == limiter._worker_id, (
                "drain signal payload should be the sender's worker_id"
            )
        finally:
            limiter.shutdown()
            test_sub.unsubscribe()
            test_sub.close()

    @staticmethod
    def test_subscriber_wakes_drain_on_cross_process_signal(redis_client, limiter_id):
        """Verify that a drain signal from one limiter wakes
        another limiter's drain loop."""
        # Arrange
        limiter_id = f"{limiter_id}_cross"
        limiter_a = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )
        limiter_b = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )

        def simulate_subscriber(channel, message):
            if message != limiter_b._worker_id:
                limiter_b._schedule_drain()

        try:
            # Act
            with patch.object(redis_client, "publish", side_effect=simulate_subscriber):
                limiter_a.trigger_consume()

            # Assert
            assert len(limiter_b.scheduled_drains) >= 1, (
                "limiter B should have received a drain signal from limiter A"
            )
        finally:
            limiter_a.shutdown()
            limiter_b.shutdown()

    @staticmethod
    def test_subscriber_ignores_self_notification(redis_client, limiter_id):
        """Verify that the subscriber ignores drain signals
        originating from the local process."""
        # Arrange
        limiter_id = f"{limiter_id}_self"
        limiter = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )

        def simulate_subscriber(channel, message):
            if message != limiter._worker_id:
                limiter._schedule_drain()

        try:
            # Act
            with patch.object(redis_client, "publish", side_effect=simulate_subscriber):
                limiter.trigger_consume()

            # Assert
            assert len(limiter.scheduled_drains) == 1, (
                "subscriber should ignore self-notifications; "
                f"expected 1 scheduled drain, got {len(limiter.scheduled_drains)}"
            )
        finally:
            limiter.shutdown()

    @staticmethod
    def test_drain_disabled_still_publishes(redis_client, limiter_id):
        """Verify that ``trigger_consume()`` publishes even
        when ``drain_enabled=False``."""
        # Arrange
        limiter_id = f"{limiter_id}_disabled_pub"
        channel = f"{limiter_id}:drain_signal"
        test_sub = redis_client.pubsub()
        test_sub.subscribe(channel)
        test_sub.get_message(timeout=1.0)  # consume the subscribe confirmation

        limiter = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )

        try:
            # Act
            limiter.trigger_consume()

            # Assert
            message = test_sub.get_message(timeout=2.0)
            assert message is not None, (
                "trigger_consume should publish even with drain_enabled=False"
            )
            assert message["data"] == limiter._worker_id, (
                "drain signal payload should be the sender's worker_id"
            )
        finally:
            limiter.shutdown()
            test_sub.unsubscribe()
            test_sub.close()


# ---------------------------------------------------------------------------
# Cross-process drain signal tests (async, variant-specific)
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestAsyncCrossProcessDrainSignal:
    """Async tests for the Redis Pub/Sub cross-process drain notification mechanism.

    These tests are inherently async-specific: they use ``redis.asyncio.Redis.pubsub()``
    and task-based subscribers.
    """

    @staticmethod
    async def test_trigger_consume_publishes_drain_signal(
        async_redis_client, limiter_id
    ):
        """Verify that ``trigger_consume()`` publishes a
        drain signal to the Pub/Sub channel."""
        # Arrange
        limiter_id = f"{limiter_id}_async_pubsub"
        channel = f"{limiter_id}:drain_signal"
        test_sub = async_redis_client.pubsub()
        await test_sub.subscribe(channel)
        await test_sub.get_message(timeout=1.0)  # consume the subscribe confirmation

        limiter = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )
        await limiter.start()

        try:
            # Act
            await limiter.trigger_consume()

            # Assert
            message = await test_sub.get_message(timeout=2.0)
            assert message is not None, "trigger_consume should publish a drain signal"
            assert message["type"] == "message", (
                "received message must be of type 'message'"
            )
            payload = message["data"]
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            assert payload == limiter._worker_id, (
                "drain signal payload should be the sender's worker_id"
            )
        finally:
            await limiter.shutdown()
            await test_sub.unsubscribe()
            await test_sub.aclose()

    @staticmethod
    async def test_subscriber_wakes_drain_on_cross_process_signal(
        async_redis_client, limiter_id
    ):
        """Verify that a drain signal from one limiter wakes
        another limiter's drain loop."""
        # Arrange
        limiter_id = f"{limiter_id}_async_cross"
        limiter_a = AsyncTrackingRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )
        limiter_b = AsyncTrackingRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )
        await limiter_a.start()
        await limiter_b.start()

        async def simulate_subscriber(channel, message):
            if message != limiter_b._worker_id:
                limiter_b._schedule_drain()

        try:
            # Act
            with patch.object(
                async_redis_client, "publish", side_effect=simulate_subscriber
            ):
                await limiter_a.trigger_consume()

            # Assert
            assert len(limiter_b.scheduled_drains) >= 1, (
                "limiter B should have received a drain signal from limiter A"
            )
        finally:
            await limiter_a.shutdown()
            await limiter_b.shutdown()

    @staticmethod
    async def test_subscriber_ignores_self_notification(async_redis_client, limiter_id):
        """Verify that the subscriber ignores drain signals
        originating from the local process."""
        # Arrange
        limiter_id = f"{limiter_id}_async_self"
        limiter = AsyncTrackingRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )
        await limiter.start()

        async def simulate_subscriber(channel, message):
            if message != limiter._worker_id:
                limiter._schedule_drain()

        try:
            # Act
            with patch.object(
                async_redis_client, "publish", side_effect=simulate_subscriber
            ):
                await limiter.trigger_consume()

            # Assert
            assert len(limiter.scheduled_drains) == 1, (
                "subscriber should ignore self-notifications; "
                f"expected 1 scheduled drain, got {len(limiter.scheduled_drains)}"
            )
        finally:
            await limiter.shutdown()

    @staticmethod
    async def test_drain_disabled_still_publishes(async_redis_client, limiter_id):
        """Verify that ``trigger_consume()`` publishes even
        when ``drain_enabled=False``."""
        # Arrange
        limiter_id = f"{limiter_id}_async_disabled_pub"
        channel = f"{limiter_id}:drain_signal"
        test_sub = async_redis_client.pubsub()
        await test_sub.subscribe(channel)
        await test_sub.get_message(timeout=1.0)  # consume the subscribe confirmation

        limiter = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )
        await limiter.start()

        try:
            # Act
            await limiter.trigger_consume()

            # Assert
            message = await test_sub.get_message(timeout=2.0)
            assert message is not None, (
                "trigger_consume should publish even with drain_enabled=False"
            )
            payload = message["data"]
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            assert payload == limiter._worker_id, (
                "drain signal payload should be the sender's worker_id"
            )
        finally:
            await limiter.shutdown()
            await test_sub.unsubscribe()
            await test_sub.aclose()


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestScheduleDrainSignatures:
    """Signature tests for ``_schedule_drain()`` default parameter values."""

    @staticmethod
    @pytest.mark.parametrize(
        "cls",
        [AbstractAsyncDistributedRateLimiter, AbstractDistributedRateLimiter],
        ids=["async", "sync"],
    )
    def test_schedule_drain_default_delay_is_zero(cls):
        """Verify that ``_schedule_drain`` default delay is ``0.0``.

        Mutation target: ``delay=0.0`` default parameter on ``_schedule_drain()``.
        """
        # Act
        sig = inspect.signature(cls._schedule_drain)

        # Assert
        assert sig.parameters["delay"].default == 0.0, (
            "_schedule_drain() default delay should be 0.0 for immediate scheduling"
        )
