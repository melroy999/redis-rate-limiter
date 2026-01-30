"""Contract tests that any RateLimiter implementation must satisfy.

These tests define the expected behavior for all implementations of
AbstractDistributedRateLimiter. Any concrete implementation should inherit
from RateLimiterContractTest and provide its own limiter fixture.
"""

import pytest


class RateLimiterContractTest:
    """Abstract test suite that any RateLimiter implementation must pass.

    Subclasses must provide:
        - limiter: A fixture that returns a configured limiter instance
        - redis_client: A fixture that returns a Redis client

    Example:
        class TestCeleryLimiter(RateLimiterContractTest):
            @pytest.fixture
            def limiter(self, redis_client, celery_app):
                return CeleryRateLimiter(redis_client, celery_app, ...)
    """

    # ==================== Contract Tests ====================

    @staticmethod
    def test_schedule_task_returns_success_and_task_id(limiter, redis_client):
        """Contract: schedule_task must return (bool, str) tuple."""
        # Arrange
        func_path = "myapp.tasks.example"
        payload = {"key": "value"}

        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert isinstance(success, bool), "first return value must be a boolean"
        assert isinstance(task_id, str), "second return value must be a string"
        assert len(task_id) > 0, "task ID must not be empty"

    @staticmethod
    def test_schedule_task_marks_task_as_active(limiter, redis_client):
        """Contract: scheduled tasks must be marked as active in Redis."""
        # Arrange
        func_path = "myapp.tasks.example"
        payload = {"user_id": 123}

        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed for first task"
        active_key = limiter.get_active_key(task_id)
        assert redis_client.exists(active_key) == 1, (
            f"task {task_id} must be marked as active in Redis"
        )

    @staticmethod
    def test_schedule_task_adds_to_buffer(limiter, redis_client):
        """Contract: scheduled tasks must be added to the buffer."""
        # Arrange
        func_path = "myapp.tasks.example"
        payload = {"user_id": 123}

        # Act
        success, task_id = limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed"
        buffer_size = redis_client.zcard(limiter.buffer_key)
        assert buffer_size >= 1, "buffer must contain at least the scheduled task"

    @staticmethod
    def test_schedule_duplicate_task_returns_false(limiter, redis_client):
        """Contract: scheduling identical tasks must return False on duplicate."""
        # Arrange
        func_path = "myapp.tasks.example"
        payload = {"user_id": 123}

        # Act
        success_1, task_id_1 = limiter.schedule_task(func_path, payload)
        success_2, task_id_2 = limiter.schedule_task(func_path, payload)

        # Assert
        assert success_1 is True, "first scheduling should succeed"
        assert success_2 is False, "duplicate scheduling should fail"
        assert task_id_1 == task_id_2, "same task should get same ID"

    @staticmethod
    def test_get_active_key_format(limiter):
        """Contract: get_active_key must return a consistent key format."""
        # Arrange
        task_id = "test-task-123"

        # Act
        active_key = limiter.get_active_key(task_id)

        # Assert
        assert isinstance(active_key, str), "active key must be a string"
        assert task_id in active_key, "active key must contain the task ID"
        assert limiter.id in active_key, "active key must contain the limiter ID"

    @staticmethod
    def test_limiter_has_required_attributes(limiter):
        """Contract: limiter must have all required configuration attributes."""
        # Assert required attributes exist and have correct types.
        assert hasattr(limiter, "id"), "limiter must have an 'id' attribute"
        assert hasattr(limiter, "redis"), "limiter must have a 'redis' attribute"
        assert hasattr(limiter, "buffer_key"), "limiter must have a 'buffer_key'"
        assert hasattr(limiter, "concurrency_key"), "limiter must have a 'concurrency_key'"
        assert hasattr(limiter, "limit"), "limiter must have a 'limit' attribute"
        assert hasattr(limiter, "window"), "limiter must have a 'window' attribute"
        assert hasattr(limiter, "max_concurrency"), "limiter must have a 'max_concurrency'"

        # Assert attributes have valid values.
        assert isinstance(limiter.limit, int) and limiter.limit > 0
        assert isinstance(limiter.window, int) and limiter.window > 0
        assert isinstance(limiter.max_concurrency, int) and limiter.max_concurrency > 0

    @staticmethod
    def test_schedule_multiple_different_tasks(limiter, redis_client):
        """Contract: multiple different tasks should all be scheduled successfully."""
        # Arrange
        tasks = [
            ("myapp.tasks.task1", {"user_id": 1}),
            ("myapp.tasks.task2", {"user_id": 2}),
            # Same function path, but with a different payload.
            ("myapp.tasks.task1", {"user_id": 3}),
        ]

        # Act & Assert
        for func_path, payload in tasks:
            success, task_id = limiter.schedule_task(func_path, payload)
            assert success is True, f"task {func_path} with {payload} should succeed"
            assert len(task_id) > 0, "each task should get a valid ID"

        # Assert all tasks are in buffer.
        buffer_size = redis_client.zcard(limiter.buffer_key)
        assert buffer_size == len(tasks), (
            f"buffer should contain {len(tasks)} tasks, found {buffer_size}"
        )
