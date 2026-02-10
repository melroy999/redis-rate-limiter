"""Contract tests that any RateLimiter implementation must satisfy.

These tests define the expected behavior for all implementations of
AbstractDistributedRateLimiter. Any concrete implementation should inherit
from RateLimiterContractTest and provide its own limiter fixture.
"""

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
    def test_schedule_task_returns_success_and_task_id(limiter, func_path, default_payload):
        """Contract: schedule_task must return (bool, str) tuple."""
        # Act
        success, task_id = limiter.schedule_task(func_path, default_payload)

        # Assert
        assert isinstance(success, bool), "first return value must be a boolean"
        assert isinstance(task_id, str), "second return value must be a string"
        assert len(task_id) > 0, "task ID must not be empty"

    @staticmethod
    def test_schedule_task_marks_task_as_active(limiter, redis_client, func_path, default_payload):
        """Contract: scheduled tasks must be marked as active in Redis."""
        # Act
        success, task_id = limiter.schedule_task(func_path, default_payload)

        # Assert
        assert success is True, "scheduling should succeed for first task"
        active_key = limiter.get_active_key(task_id)
        assert redis_client.exists(active_key) == 1, (
            f"task {task_id} must be marked as active in Redis"
        )

    @staticmethod
    def test_schedule_task_adds_to_buffer(limiter, redis_client, func_path, default_payload):
        """Contract: scheduled tasks must be added to the buffer."""
        # Act
        success, task_id = limiter.schedule_task(func_path, default_payload)

        # Assert
        assert success is True, "scheduling should succeed"
        buffer_size = redis_client.zcard(limiter.buffer_key)
        assert buffer_size >= 1, "buffer must contain at least the scheduled task"

    @staticmethod
    def test_schedule_duplicate_task_returns_false(limiter, func_path, default_payload):
        """Contract: scheduling identical tasks must return False on duplicate."""
        # Act
        success_1, task_id_1 = limiter.schedule_task(func_path, default_payload)
        success_2, task_id_2 = limiter.schedule_task(func_path, default_payload)

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

        # Assert attributes have valid types.
        assert isinstance(limiter.limit, int), "limit must be an int"
        assert isinstance(limiter.window, int), "window must be an int"
        assert isinstance(limiter.max_concurrency, int), "max_concurrency must be an int"

        # Assert attributes have valid values.
        assert limiter.limit > 0, "limit must be positive"
        assert limiter.window > 0, "window must be positive"
        assert limiter.max_concurrency > 0, "max_concurrency must be positive"

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

    @staticmethod
    def test_consume_returns_expected_structure(limiter):
        """Contract: consume() returns all required consume result keys."""
        # Act
        result = limiter.consume()

        # Assert
        expected_keys = {
            "success",
            "expired",
            "task",
            "remaining_tokens",
            "active_concurrency",
            "reset_in_ms",
            "remaining_tasks",
        }
        assert isinstance(result, dict), "consume result must be a dictionary"
        assert set(result.keys()) == expected_keys, (
            f"consume result keys must match {expected_keys}"
        )

        # Assert value types match the ConsumeResult TypedDict contract.
        assert isinstance(result["success"], bool), "success must be a bool"
        assert isinstance(result["expired"], bool), "expired must be a bool"
        assert result["task"] is None or isinstance(result["task"], dict), (
            "task must be None or a dict"
        )
        assert isinstance(result["remaining_tokens"], int), "remaining_tokens must be an int"
        assert isinstance(result["active_concurrency"], int), "active_concurrency must be an int"
        assert isinstance(result["reset_in_ms"], int), "reset_in_ms must be an int"
        assert isinstance(result["remaining_tasks"], int), "remaining_tasks must be an int"

        # Assert values have valid bounds.
        assert result["remaining_tokens"] >= 0, "remaining_tokens must be non-negative on empty buffer"
        assert result["active_concurrency"] >= 0, "active_concurrency must be non-negative"
        assert result["reset_in_ms"] >= 0, "reset_in_ms must be non-negative"
        assert result["remaining_tasks"] >= 0, "remaining_tasks must be non-negative"

    @staticmethod
    def test_consume_empty_buffer_returns_unsuccessful(limiter):
        """Contract: consuming an empty buffer returns no task and unsuccessful result."""
        # Act
        result = limiter.consume()

        # Assert
        assert result["success"] is False, "consume should fail on empty buffer"
        assert result["task"] is None, "consume should return no task on empty buffer"

    @staticmethod
    def test_consume_expired_field_is_boolean(limiter, func_path, default_payload):
        """Contract: consume() expired flag must always be a boolean."""
        # Arrange
        limiter.schedule_task(func_path, default_payload)

        # Act
        result = limiter.consume()

        # Assert
        assert isinstance(result["expired"], bool), "expired must be a bool"

    @staticmethod
    def test_execution_lock_context_manager_yields_boolean(limiter):
        """Contract: execution_lock() yields a boolean acquisition result."""
        # Act & Assert
        with limiter.execution_lock(timeout_ms=50) as acquired:
            assert isinstance(acquired, bool), "execution_lock must yield a boolean"

    @staticmethod
    def test_get_buffer_count_returns_nonnegative_integer(limiter):
        """Contract: get_buffer_count() returns a non-negative integer."""
        # Act
        count = limiter.get_buffer_count()

        # Assert
        assert isinstance(count, int), "buffer count must be an integer"
        assert count >= 0, "buffer count must be non-negative"

    @staticmethod
    def test_get_status_returns_dict_with_required_sections(limiter):
        """Contract: get_status() returns required top-level status sections."""
        # Act
        result = limiter.get_status()

        # Assert
        required_sections = {
            "limiter_id",
            "concurrency",
            "buffer",
            "rate_limit",
            "dispatcher",
        }
        assert isinstance(result, dict), "status result must be a dictionary"
        assert required_sections.issubset(result.keys()), (
            f"status result must include sections {required_sections}"
        )
