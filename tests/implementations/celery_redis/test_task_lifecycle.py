"""Tests for the TaskLifecycle context manager.

This module tests the TaskLifecycle implementation that manages concurrency
slots and task cleanup. It inherits contract tests and adds implementation-specific tests.
"""

from unittest.mock import MagicMock, patch

import pytest

from celery_rate_limiter.limiters import CeleryRateLimiter, TaskLifecycle
from tests.contracts.test_lifecycle_contract import TaskLifecycleContractTest


@pytest.fixture
def task_id():
    """Provide a consistent task ID for testing."""
    return "task123"


@pytest.fixture
def mock_limiter(redis_client, task_id):
    """Create a mock limiter that uses the real Redis client but mocks internal helpers.

    This fixture provides a limiter with real Redis operations but mocked
    Celery-specific methods to avoid needing a full Celery setup.
    """
    limiter = MagicMock()
    limiter.redis = redis_client
    limiter.concurrency_key = "test:concurrency"
    limiter.lease_duration = 30
    limiter.id = "test_limiter"
    limiter.get_active_key.side_effect = lambda _: f"test:active:{task_id}"
    limiter.extend_lease.return_value = 1  # For background thread tests
    return limiter


@pytest.fixture
def active_key(mock_limiter, task_id):
    """Provide the active key for the test task."""
    return mock_limiter.get_active_key(task_id)


@pytest.fixture
def lifecycle_class():
    """Provide the TaskLifecycle class for contract tests."""
    return TaskLifecycle


class TestTaskLifecycle(TaskLifecycleContractTest):
    """Test TaskLifecycle context manager implementation.

    Inherits all contract tests from TaskLifecycleContractTest and adds
    Celery-specific tests for heartbeat handling and lifecycle behavior.
    """

    # ==================== Implementation-Specific Tests ====================

    def test_lifecycle_with_multiple_concurrent_tasks(
        self, redis_client, mock_limiter, task_id, active_key
    ):
        """Verify lifecycle only removes the specific task from concurrency set."""
        # Arrange
        # Simulate 5 concurrent tasks.
        concurrent_tasks = {
            "other_task_1": 100,
            "other_task_2": 100,
            "other_task_3": 100,
            "other_task_4": 100,
            task_id: 100,
        }
        redis_client.zadd(mock_limiter.concurrency_key, concurrent_tasks)
        redis_client.set(active_key, "1")

        # Act & Assert
        # Prevent heartbeat thread from starting.
        with patch("threading.Thread"):
            with TaskLifecycle(mock_limiter, task_id):
                # During execution, all 5 tasks should be present.
                assert redis_client.zcard(mock_limiter.concurrency_key) == 5

        # After completion, only our task should be removed.
        assert redis_client.zcard(mock_limiter.concurrency_key) == 4
        assert redis_client.zscore(mock_limiter.concurrency_key, task_id) is None
        assert redis_client.zscore(mock_limiter.concurrency_key, "other_task_1") is not None
        assert redis_client.exists(active_key) == 0

    def test_lifecycle_handles_redis_failure_during_cleanup(
        self, redis_client, mock_limiter, task_id, active_key
    ):
        """Verify lifecycle raises exception but still triggers consume on Redis failure."""
        # Arrange
        with patch.object(
            mock_limiter.redis,
            "zrem",
            side_effect=Exception("Redis connection lost"),
        ) as mock_zrem:
            # Act & Assert
            with patch("threading.Thread"):
                with pytest.raises(Exception, match="Redis connection lost"):
                    with TaskLifecycle(mock_limiter, task_id):
                        pass

                # Verify the exception came from zrem.
                mock_zrem.assert_called_once()

        # Assert that trigger_consume is still called.
        # noinspection PyUnboundLocalVariable
        mock_limiter.trigger_consume.assert_called_once()

    @pytest.mark.parametrize(
        "original, override",
        [("warn", "kill"), ("kill", "warn")],
        ids=["warn_to_kill", "kill_to_warn"],
    )
    def test_heartbeat_failure_override_precedence(
        self, redis_client, celery_app, task_id, original, override
    ):
        """Verify that override parameter takes precedence over limiter default."""
        # Arrange
        # Use real limiter to test override mechanism.
        limiter = CeleryRateLimiter(
            redis_client,
            celery_app,
            limiter_id="test_id",
            limit=1,
            window=1,
            max_concurrency=1,
            max_age=1,
            on_heartbeat_failure=original,
        )

        # Act
        # Create lifecycle with override.
        # noinspection PyTypeChecker
        lifecycle_with_override = limiter.task_lifecycle(
            task_id,
            on_heartbeat_failure_override=override,
        )

        # Assert
        # Override should take precedence.
        assert lifecycle_with_override.on_failure_action == override, (
            f"override {override} should take precedence over default {original}"
        )