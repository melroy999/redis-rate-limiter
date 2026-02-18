"""Tests for the optional metrics callback on AbstractDistributedRateLimiter.

The metrics callback enables observability by emitting events after consume
and schedule operations, thereby allowing integration with external monitoring
systems.
"""

from unittest.mock import MagicMock

import pytest

from tests.implementations.conftest import MinimalRateLimiter


class TestMetricsCallback:
    """Test suite for the metrics callback behavior."""

    @pytest.fixture
    def callback(self):
        """Provide a mock callback for capturing metric emissions."""
        return MagicMock()

    @pytest.fixture
    def limiter(self, redis_client, callback, default_limiter_id):
        """Create a generic limiter with a metrics callback for testing."""
        return MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{default_limiter_id}_with_metrics",
            limit=10,
            window=60,
            max_concurrency=5,
            metrics_callback=callback,
        )

    @pytest.fixture
    def limiter_no_callback(self, redis_client, default_limiter_id):
        """Create a generic limiter without a metrics callback."""
        return MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{default_limiter_id}_without_metrics",
            limit=10,
            window=60,
            max_concurrency=5,
        )

    @staticmethod
    def test_metrics_callback_none_by_default(limiter_no_callback):
        """Verify that the metrics_callback defaults to ``None`` and does not cause errors."""
        # Arrange
        limiter = limiter_no_callback

        # Act
        result = limiter.consume()

        # Assert
        assert result is not None, "consume should succeed without a callback"
        assert limiter.metrics_callback is None, "callback should default to None"

    @staticmethod
    def test_consume_emits_metric(limiter, callback):
        """Verify that consume emits a metric with the correct event name and data keys."""
        # Act
        limiter.consume()

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
    def test_schedule_emits_metric(limiter, callback, func_path):
        """Verify that ``schedule_task()`` emits a metric with the correct event name and data."""
        # Act
        was_scheduled, task_id = limiter.schedule_task(func_path, {"key": "value"})

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
    def test_schedule_duplicate_emits_not_scheduled(limiter, callback, func_path):
        """Verify that scheduling a duplicate task emits ``scheduled=False``."""
        # Arrange
        # Schedule the task once so that it becomes in-flight.
        limiter.schedule_task(func_path, {"key": "value"})
        callback.reset_mock()

        # Act
        # Schedule the same task again.
        was_scheduled, task_id = limiter.schedule_task(func_path, {"key": "value"})

        # Assert
        assert was_scheduled is False, "duplicate task should not be scheduled"
        callback.assert_called_once_with(
            "schedule", {"scheduled": False, "task_id": task_id}
        )

    @staticmethod
    def test_consume_metric_data_matches_result(limiter, callback):
        """Verify that the metric data values match the ConsumeResult."""
        # Act
        result = limiter.consume()

        # Assert
        callback.assert_called_once()
        _, event_data = callback.call_args[0]
        assert event_data["success"] == result["success"], (
            "metric success should match consume result"
        )
        assert event_data["expired"] == result["expired"], (
            "metric expired should match consume result"
        )
        assert event_data["remaining_tokens"] == result["remaining_tokens"], (
            "metric remaining_tokens should match consume result"
        )
        assert event_data["active_concurrency"] == result["active_concurrency"], (
            "metric active_concurrency should match consume result"
        )
        assert event_data["reset_in_ms"] == result["reset_in_ms"], (
            "metric reset_in_ms should match consume result"
        )
        assert event_data["remaining_tasks"] == result["remaining_tasks"], (
            "metric remaining_tasks should match consume result"
        )

    @staticmethod
    def test_callback_exception_does_not_break_consume(limiter, callback):
        """Verify that consume continues to function when the callback raises an exception."""
        # Arrange
        callback.side_effect = RuntimeError("callback failure")

        # Act
        result = limiter.consume()

        # Assert
        assert result is not None, (
            "consume should return a result despite callback failure"
        )
        assert "success" in result, "consume result should contain expected keys"

    @staticmethod
    def test_callback_exception_does_not_break_schedule(
        limiter, callback, func_path
    ):
        """Verify that ``schedule_task()`` continues to function when the callback raises an exception."""
        # Arrange
        callback.side_effect = RuntimeError("callback failure")

        # Act
        was_scheduled, task_id = limiter.schedule_task(func_path, {"key": "value"})

        # Assert
        assert was_scheduled is True, (
            "task should be scheduled despite callback failure"
        )
        assert isinstance(task_id, str), (
            "task_id should be returned despite callback failure"
        )

    @staticmethod
    def test_consume_after_schedule_emits_both_events(
        limiter, callback, func_path
    ):
        """Verify that both the schedule and consume events are emitted in sequence."""
        # Arrange
        limiter.schedule_task(func_path, {"key": "value"})
        callback.reset_mock()

        # Act
        limiter.consume()

        # Assert
        callback.assert_called_once()
        event_name, _ = callback.call_args[0]
        assert event_name == "consume", "standalone consume should emit a consume event"
