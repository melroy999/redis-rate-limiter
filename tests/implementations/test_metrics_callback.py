"""Tests for the optional metrics callback on AbstractDistributedRateLimiter.

The metrics callback enables observability by emitting events after consume
and schedule operations, thereby allowing integration with external monitoring
systems. Tests are written once in async form; the sync implementation
participates via the ``SyncToAsyncLimiterAdapter``.

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``, ``limiter_id``, ``func_path``: from ``tests/conftest.py``.
"""

import inspect
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.helpers.adapters import SyncToAsyncLimiterAdapter
from tests.helpers.utils import assert_log_emitted
from tests.implementations.conftest import MinimalAsyncRateLimiter, MinimalRateLimiter

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class MetricsCallbackTests:
    """Unified test suite for the metrics callback behavior.

    Subclasses must provide:
        - ``limiter``: a rate limiter with a metrics callback set.
        - ``limiter_no_callback``: a rate limiter without a metrics callback.
    """

    @pytest.fixture
    def callback(self):
        """Provide a mock callback for capturing metric emissions."""
        return MagicMock()

    @staticmethod
    async def test_metrics_callback_none_by_default(limiter_no_callback):
        """Verify that the metrics_callback defaults to ``None`` and does not cause errors."""
        # Arrange
        limiter = limiter_no_callback

        # Act
        result = await limiter.consume()

        # Assert
        assert result is not None, "consume should succeed without a callback"
        assert result["success"] is False, (
            "consume with empty buffer should not succeed"
        )
        assert limiter.metrics_callback is None, "callback should default to None"

    @staticmethod
    async def test_consume_emits_metric(limiter, callback):
        """Verify that consume emits a metric with the correct event name and data keys."""
        # Act
        await limiter.consume()

        # Assert
        callback.assert_called_once()
        event_name, event_data = callback.call_args[0]
        assert event_name == "consume", "event name should be 'consume'"
        expected_keys = {
            "success",
            "expired",
            "remaining_tokens",
            "active_concurrency",
            "reset_in_ms",
            "remaining_tasks",
        }
        assert set(event_data.keys()) == expected_keys, (
            f"consume event should contain keys {expected_keys}, got {set(event_data.keys())}"
        )

    @staticmethod
    async def test_schedule_emits_metric(limiter, callback, func_path):
        """Verify that ``schedule_task()`` emits a metric with the correct event name and data."""
        # Act
        was_scheduled, task_id = await limiter.schedule_task(
            func_path, {"key": "value"}
        )

        # Assert
        # Backend implementations may trigger a follow-up consume asynchronously.
        # Filter to isolate the schedule event only.
        schedule_calls = [
            call for call in callback.call_args_list if call[0][0] == "schedule"
        ]
        assert len(schedule_calls) == 1, "exactly one schedule event should be emitted"
        event_name, event_data = schedule_calls[0][0]
        assert event_name == "schedule", "event name should be 'schedule'"
        assert event_data["scheduled"] is True, "task should be reported as scheduled"
        assert event_data["task_id"] == task_id, (
            f"emitted task_id ({event_data['task_id']}) should match returned task_id ({task_id})"
        )

    @staticmethod
    async def test_schedule_duplicate_emits_not_scheduled(limiter, callback, func_path):
        """Verify that scheduling a duplicate task emits ``scheduled=False``."""
        # Arrange
        # Schedule the task once so that it becomes in-flight.
        await limiter.schedule_task(func_path, {"key": "value"})
        callback.reset_mock()

        # Act
        # Schedule the same task again.
        was_scheduled, task_id = await limiter.schedule_task(
            func_path, {"key": "value"}
        )

        # Assert
        assert was_scheduled is False, "duplicate task should not be scheduled"
        callback.assert_called_once_with(
            "schedule", {"scheduled": False, "task_id": task_id}
        )

    @staticmethod
    async def test_consume_metric_includes_expired_flag(limiter, callback):
        """Verify that the metric data correctly reflects ``expired=True`` for expired consume results."""
        # Arrange
        task_json = json.dumps(
            {
                "id": "expired-task",
                "func_path": "tests.helpers.tasks.noop_task",
                "payload": {"key": "value"},
                "inflight_key": "test:inflight:expired-task",
            }
        )
        sentinel_result = [
            "-1",  # [0] expired flag
            task_json,  # [1] task data
            "10",  # [2] remaining_tokens
            "0",  # [3] active_concurrency
            "500",  # [4] reset_in_ms
            "3",  # [5] remaining_tasks
            "5",  # [6] val_previous
            "2",  # [7] val_current
        ]

        actual_limiter = getattr(limiter, "_inner", limiter)
        mock_cls = (
            AsyncMock
            if inspect.iscoroutinefunction(actual_limiter._eval_script)
            else MagicMock
        )

        # Act
        with patch.object(
            actual_limiter, "_eval_script", mock_cls(return_value=sentinel_result)
        ):
            await limiter.consume()

        # Assert
        callback.assert_called_once()
        _, event_data = callback.call_args[0]
        assert event_data["expired"] is True, (
            "metric expired should be True when consume returns an expired result"
        )
        assert event_data["success"] is False, (
            "metric success should be False for an expired consume result"
        )

    @staticmethod
    async def test_callback_exception_does_not_break_consume(limiter, callback):
        """Verify that consume continues to function when the callback raises an exception."""
        # Arrange
        callback.side_effect = RuntimeError("callback failure")

        # Act
        result = await limiter.consume()

        # Assert
        assert result is not None, (
            "consume should return a result despite callback failure"
        )
        assert result["success"] is False, (
            "consume with empty buffer should not succeed even when callback raises"
        )

    @staticmethod
    async def test_callback_exception_does_not_break_schedule(
        limiter, callback, func_path
    ):
        """Verify that ``schedule_task()`` continues to function when the callback raises an exception."""
        # Arrange
        callback.side_effect = RuntimeError("callback failure")

        # Act
        was_scheduled, task_id = await limiter.schedule_task(
            func_path, {"key": "value"}
        )

        # Assert
        assert was_scheduled is True, (
            "task should be scheduled despite callback failure"
        )
        assert isinstance(task_id, str), (
            "task_id should be returned despite callback failure"
        )
        assert len(task_id) > 0, "task id should be a non-empty string"

    @staticmethod
    async def test_consume_after_schedule_emits_both_events(
        limiter, callback, func_path
    ):
        """Verify that both the schedule and consume events are emitted in sequence."""
        # Arrange
        await limiter.schedule_task(func_path, {"key": "value"})
        callback.reset_mock()

        # Act
        await limiter.consume()

        # Assert
        callback.assert_called_once()
        event_name, _ = callback.call_args[0]
        assert event_name == "consume", "standalone consume should emit a consume event"


# ---------------------------------------------------------------------------
# Unified observability tests
# ---------------------------------------------------------------------------


class MetricsCallbackObservabilityTests:
    """Observability tests for the metrics callback feature.

    These tests verify logging behavior and are separated from the behavioral
    tests in ``MetricsCallbackTests`` per the separation of concerns guideline
    (Section 4.1). Subclasses must provide the same ``limiter`` and ``callback``
    fixtures as ``MetricsCallbackTests``.
    """

    @pytest.fixture
    def callback(self):
        """Provide a mock callback for capturing metric emissions."""
        return MagicMock()

    @staticmethod
    async def test_consume_emits_attempt_and_result_debug_logs(limiter, caplog):
        """Verify that consume emits debug logs for the attempt start and result."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            await limiter.consume()

        # Assert
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            ["Consume attempt started", f"limiter={limiter.id}"],
            "should emit a debug log for the consume attempt with the limiter id",
        )
        assert_log_emitted(
            caplog.records,
            "DEBUG",
            [f"limiter={limiter.id}", "success=False", "remaining_tokens=10"],
            "should emit a debug log for the consume result with limiter id, success, and remaining tokens",
        )

    @staticmethod
    async def test_callback_exception_during_consume_emits_warning_log(
        limiter, callback, caplog
    ):
        """Verify that a callback exception during consume emits a warning log."""
        # Arrange
        callback.side_effect = RuntimeError("callback failure")

        # Act
        with caplog.at_level(logging.WARNING, logger="redis_rate_limiter"):
            await limiter.consume()

        # Assert
        assert_log_emitted(
            caplog.records,
            "WARNING",
            [
                "Metrics callback raised an exception",
                f"limiter={limiter.id}",
                "event=consume",
            ],
            "should emit a warning log when the metrics callback raises during consume",
        )

    @staticmethod
    async def test_callback_exception_during_schedule_emits_warning_log(
        limiter, callback, func_path, caplog
    ):
        """Verify that a callback exception during schedule emits a warning log."""
        # Arrange
        callback.side_effect = RuntimeError("callback failure")

        # Act
        with caplog.at_level(logging.WARNING, logger="redis_rate_limiter"):
            await limiter.schedule_task(func_path, {"key": "value"})

        # Assert
        assert_log_emitted(
            caplog.records,
            "WARNING",
            [
                "Metrics callback raised an exception",
                f"limiter={limiter.id}",
                "event=schedule",
            ],
            "should emit a warning log when the metrics callback raises during schedule",
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestSyncMetricsCallback(MetricsCallbackTests, MetricsCallbackObservabilityTests):
    """Sync rate limiter metrics callback exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, redis_client, callback, limiter_id):
        """Create a sync limiter with a metrics callback, wrapped in the async adapter."""
        _limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_sync_metrics",
            limit=10,
            window=60,
            max_concurrency=5,
            metrics_callback=callback,
        )
        yield SyncToAsyncLimiterAdapter(_limiter)
        _limiter.shutdown()

    @pytest.fixture
    def limiter_no_callback(self, redis_client, limiter_id):
        """Create a sync limiter without a metrics callback, wrapped in the async adapter."""
        _limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_sync_no_metrics",
            limit=10,
            window=60,
            max_concurrency=5,
        )
        yield SyncToAsyncLimiterAdapter(_limiter)
        _limiter.shutdown()


class TestAsyncMetricsCallback(MetricsCallbackTests, MetricsCallbackObservabilityTests):
    """Async rate limiter metrics callback exercised natively."""

    @pytest.fixture
    async def limiter(self, async_redis_client, callback, limiter_id):
        """Create an async limiter with a metrics callback."""
        lim = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_async_metrics",
            limit=10,
            window=60,
            max_concurrency=5,
            metrics_callback=callback,
        )
        await lim.start()
        yield lim
        await lim.shutdown()

    @pytest.fixture
    async def limiter_no_callback(self, async_redis_client, limiter_id):
        """Create an async limiter without a metrics callback."""
        lim = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_async_no_metrics",
            limit=10,
            window=60,
            max_concurrency=5,
        )
        await lim.start()
        yield lim
        await lim.shutdown()
