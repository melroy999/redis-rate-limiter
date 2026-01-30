"""Integration tests for rate limiting behavior.

End-to-end tests verifying rate limiter correctly limits requests
and handles burst scenarios using Redis and Lua scripts.
"""

import time

import pytest

from celery_rate_limiter.limiters import CeleryRateLimiter


@pytest.fixture
def integration_limiter(redis_client, celery_app):
    """Create a limiter with explicit configuration for integration tests.

    Config:
        - limit: 5 requests
        - window: 60 seconds
        - max_concurrency: 2 simultaneous tasks
        - max_age: 3600 seconds (1 hour)
        - lease_duration: 30 seconds
    """
    limiter = CeleryRateLimiter(
        redis_client=redis_client,
        celery_app=celery_app,
        limiter_id="integration_test_limiter",
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
    )

    yield limiter

    # Cleanup
    keys = redis_client.keys(f"{limiter.id}:*")
    if keys:
        redis_client.delete(*keys)


class TestRateLimitingIntegration:
    """Integration tests for rate limiting with Redis."""

    def test_basic_rate_limit_enforcement(self, integration_limiter, redis_client):
        """Verify rate limiter enforces the configured limit.

        Limiter config: limit=5 per window=60 seconds.
        Schedule 10 tasks, consume up to limit, verify queueing.
        """
        # Arrange
        func_path = "myapp.tasks.process_data"
        for i in range(10):
            success, _ = integration_limiter.schedule_task(func_path, {"index": i})
            assert success is True

        # Act
        consumed_count = 0
        results = []
        for _ in range(10):
            result = integration_limiter.consume()
            results.append(result)
            if result["success"]:
                consumed_count += 1

        # Assert
        assert consumed_count == 5, "should consume exactly 5 tasks"
        last_successful = [r for r in results if r["success"]][-1]
        assert last_successful["remaining_tokens"] == 0
        assert results[-1]["remaining_tasks"] == 5

    def test_burst_at_window_boundary(self, integration_limiter, redis_client):
        """Verify sliding window allows burst up to limit at window start.

        Sliding window permits consuming up to limit immediately when
        previous window is empty, then rate-limits additional requests.
        """
        # Arrange
        func_path = "myapp.tasks.process_data"
        for i in range(8):
            integration_limiter.schedule_task(func_path, {"index": i})

        # Act
        burst_consumed = 0
        for _ in range(5):
            result = integration_limiter.consume()
            if result["success"]:
                burst_consumed += 1

        # Assert
        assert burst_consumed == 5
        next_result = integration_limiter.consume()
        assert next_result["success"] is False
        assert next_result["remaining_tokens"] == 0
        assert next_result["remaining_tasks"] == 3

    def test_rate_limit_recovery_over_time(self, integration_limiter, redis_client):
        """Verify rate limit recovers as sliding window progresses."""
        # Arrange
        func_path = "myapp.tasks.process_data"
        for i in range(8):
            integration_limiter.schedule_task(func_path, {"index": i})

        # Act
        for _ in range(5):
            result = integration_limiter.consume()
            assert result["success"] is True

        result = integration_limiter.consume()
        assert result["success"] is False
        initial_reset_time = result["reset_in_ms"]

        # Wait for window to slide.
        time.sleep(3)
        result = integration_limiter.consume()

        # Assert
        assert result["reset_in_ms"] < initial_reset_time
        # Short wait insufficient for token recovery with limit=5, window=60.
        assert result["success"] is False

    def test_concurrency_limit_enforcement(self, integration_limiter, redis_client):
        """Verify concurrency limits are enforced independently of rate limit.

        Limiter config: max_concurrency=2.
        Verify only 2 tasks consumed simultaneously even if rate limit allows more.
        """
        # Arrange
        func_path = "myapp.tasks.process_data"
        for i in range(5):
            integration_limiter.schedule_task(func_path, {"index": i})

        # Act
        results = []
        for _ in range(3):
            result = integration_limiter.consume()
            results.append(result)

        # Assert
        assert results[0]["success"] is True
        assert results[1]["success"] is True
        assert results[0]["active_concurrency"] == 1
        assert results[1]["active_concurrency"] == 2
        assert results[2]["success"] is False
        assert results[2]["active_concurrency"] == 2

    def test_multiple_burst_windows(self, integration_limiter, redis_client):
        """Verify burst behavior at window boundaries."""
        # Arrange
        func_path = "myapp.tasks.process_data"
        for i in range(10):
            integration_limiter.schedule_task(func_path, {"index": i})

        # Act
        first_burst = []
        for _ in range(6):
            result = integration_limiter.consume()
            first_burst.append(result)

        # Assert
        successful = sum(1 for r in first_burst if r["success"])
        assert successful == 5
        assert first_burst[-1]["success"] is False
        assert first_burst[-1]["remaining_tasks"] == 5

    @pytest.mark.parametrize("num_tasks", [3, 5, 10, 20])
    def test_accurate_telemetry_tracking(self, integration_limiter, redis_client, num_tasks):
        """Verify telemetry accurately tracks remaining tokens and tasks."""
        # Arrange
        func_path = "myapp.tasks.process_data"
        for i in range(num_tasks):
            integration_limiter.schedule_task(func_path, {"index": i})

        # Act
        results = []
        for _ in range(num_tasks):
            result = integration_limiter.consume()
            results.append(result)

        # Assert
        consumed = sum(1 for r in results if r["success"])
        expected_consumed = min(num_tasks, 5)
        assert consumed == expected_consumed

        final_result = results[-1]
        expected_remaining = max(0, num_tasks - consumed)
        assert final_result["remaining_tasks"] == expected_remaining

        successful_results = [r for r in results if r["success"]]
        if successful_results:
            expected_tokens = [4, 3, 2, 1, 0]
            actual_tokens = [r["remaining_tokens"] for r in successful_results]
            assert actual_tokens == expected_tokens[:len(successful_results)]

    def test_empty_buffer_returns_no_task(self, integration_limiter, redis_client):
        """Verify consuming from empty buffer returns unsuccessful result."""
        # Act
        result = integration_limiter.consume()

        # Assert
        assert result["success"] is False
        assert result["task"] is None
        assert result["remaining_tasks"] == 0
        assert result["remaining_tokens"] == 5
