"""Contract tests that any RateLimiter implementation must satisfy.

These tests define the expected behaviour for all implementations of
``AbstractDistributedRateLimiter`` and ``AbstractAsyncDistributedRateLimiter``.
Any concrete implementation should inherit from ``RateLimiterContractTest`` and
provide its own limiter fixture.

Sync implementations can use the ``SyncToAsyncLimiterAdapter`` from
``tests.helpers.adapters`` to satisfy the async test interface.
"""


class RateLimiterContractTest:
    """Abstract test suite that any RateLimiter implementation must pass.

    Subclasses are required to provide the following:
        - limiter: A fixture that returns a configured limiter instance
          (or a ``SyncToAsyncLimiterAdapter`` wrapping a sync limiter).
        - async_redis_client: An async fixture that returns an async Redis client.

    Example (async backend — conftest provides ``limiter`` directly):
        class TestAsyncIOContracts(RateLimiterContractTest):
            pass

    Example (sync backend via adapter):
        class TestThreadPoolContracts(RateLimiterContractTest):
            @pytest.fixture
            def limiter(self, limiter):
                return SyncToAsyncLimiterAdapter(limiter)
    """

    @staticmethod
    async def test_schedule_task_returns_success_and_task_id(
        limiter, func_path, payload
    ):
        """Contract: ``schedule_task()`` must return a ``(bool, str)`` tuple."""
        # Act
        success, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        assert isinstance(success, bool), "first return value must be a boolean"
        assert isinstance(task_id, str), "second return value must be a string"
        assert len(task_id) > 0, "task ID must not be empty"

    @staticmethod
    async def test_schedule_task_marks_task_as_inflight(
        limiter, async_redis_client, func_path, payload
    ):
        """Contract: scheduled tasks must be marked as in-flight within Redis."""
        # Act
        success, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed for first task"
        inflight_key = limiter.get_inflight_key(task_id)
        assert await async_redis_client.exists(inflight_key) == 1, (
            f"task {task_id} must be marked as in-flight in Redis"
        )

    @staticmethod
    async def test_schedule_task_adds_to_buffer(
        limiter, async_redis_client, func_path, payload
    ):
        """Contract: scheduled tasks must be appended to the buffer."""
        # Act
        success, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed"
        buffer_size = await async_redis_client.zcard(limiter.buffer_key)
        assert buffer_size >= 1, "buffer must contain at least the scheduled task"

    @staticmethod
    async def test_schedule_duplicate_task_returns_false(
        limiter, func_path, payload
    ):
        """Contract: scheduling identical tasks must return ``False`` for the duplicate."""
        # Act
        success_1, task_id_1 = await limiter.schedule_task(func_path, payload)
        success_2, task_id_2 = await limiter.schedule_task(func_path, payload)

        # Assert
        assert success_1 is True, "first scheduling should succeed"
        assert success_2 is False, "duplicate scheduling should fail"
        assert task_id_1 == task_id_2, "same task should get same ID"

    @staticmethod
    def test_get_inflight_key_format(limiter):
        """Contract: ``get_inflight_key`` must return a consistent key format."""
        # Arrange
        task_id = "test-task-123"

        # Act
        inflight_key = limiter.get_inflight_key(task_id)

        # Assert
        assert isinstance(inflight_key, str), "inflight key must be a string"
        assert task_id in inflight_key, "inflight key must contain the task ID"
        assert limiter.id in inflight_key, "inflight key must contain the limiter ID"

    @staticmethod
    def test_limiter_has_required_attributes(limiter):
        """Contract: the limiter must expose all required configuration attributes."""
        # Assert that the required attributes exist and have the correct types.
        assert hasattr(limiter, "id"), "limiter must have an 'id' attribute"
        assert hasattr(limiter, "redis"), "limiter must have a 'redis' attribute"
        assert hasattr(limiter, "buffer_key"), "limiter must have a 'buffer_key'"
        assert hasattr(limiter, "concurrency_key"), (
            "limiter must have a 'concurrency_key'"
        )
        assert hasattr(limiter, "limit"), "limiter must have a 'limit' attribute"
        assert hasattr(limiter, "window"), "limiter must have a 'window' attribute"
        assert hasattr(limiter, "max_concurrency"), (
            "limiter must have a 'max_concurrency'"
        )

        # Assert that the attributes have valid types.
        assert isinstance(limiter.limit, int), "limit must be an int"
        assert isinstance(limiter.window, (int, float)), "window must be numeric"
        assert isinstance(limiter.max_concurrency, int), (
            "max_concurrency must be an int"
        )

        # Assert that the attributes have valid values.
        assert limiter.limit > 0, "limit must be positive"
        assert limiter.window > 0, "window must be positive"
        assert limiter.max_concurrency > 0, "max_concurrency must be positive"

    @staticmethod
    async def test_schedule_multiple_different_tasks(limiter, async_redis_client):
        """Contract: multiple distinct tasks must all be scheduled successfully."""
        # Arrange
        tasks = [
            ("myapp.tasks.task1", {"user_id": 1}),
            ("myapp.tasks.task2", {"user_id": 2}),
            # The same function path, but with a different payload.
            ("myapp.tasks.task1", {"user_id": 3}),
        ]

        # Act & Assert
        for func_path, payload in tasks:
            success, task_id = await limiter.schedule_task(func_path, payload)
            assert success is True, f"task {func_path} with {payload} should succeed"
            assert len(task_id) > 0, "each task should get a valid ID"

    @staticmethod
    async def test_consume_returns_expected_structure(limiter):
        """Contract: ``consume()`` must return all required consume result keys."""
        # Act
        result = await limiter.consume()

        # Assert
        expected_keys = {
            "success",
            "expired",
            "task",
            "remaining_tokens",
            "active_concurrency",
            "reset_in_ms",
            "remaining_tasks",
            "val_current",
            "val_previous",
        }
        assert isinstance(result, dict), "consume result must be a dictionary"
        assert set(result.keys()) == expected_keys, (
            f"consume result keys must match {expected_keys}"
        )

        # Assert that the value types match the ConsumeResult TypedDict contract.
        assert isinstance(result["success"], bool), "success must be a bool"
        assert isinstance(result["expired"], bool), "expired must be a bool"
        assert result["task"] is None or isinstance(result["task"], dict), (
            "task must be None or a dict"
        )
        assert isinstance(result["remaining_tokens"], int), (
            "remaining_tokens must be an int"
        )
        assert isinstance(result["active_concurrency"], int), (
            "active_concurrency must be an int"
        )
        assert isinstance(result["reset_in_ms"], int), "reset_in_ms must be an int"
        assert isinstance(result["remaining_tasks"], int), (
            "remaining_tasks must be an int"
        )
        assert isinstance(result["val_previous"], int), "val_previous must be an int"
        assert isinstance(result["val_current"], int), "val_current must be an int"

        # Assert that the values fall within valid bounds.
        assert result["remaining_tokens"] >= 0, (
            "remaining_tokens must be non-negative on empty buffer"
        )
        assert result["active_concurrency"] >= 0, (
            "active_concurrency must be non-negative"
        )
        assert result["reset_in_ms"] >= 0, "reset_in_ms must be non-negative"
        assert result["remaining_tasks"] >= 0, "remaining_tasks must be non-negative"

    @staticmethod
    async def test_consume_empty_buffer_returns_unsuccessful(limiter):
        """Contract: consuming from an empty buffer must return no task and an unsuccessful result."""
        # Act
        result = await limiter.consume()

        # Assert
        assert result["success"] is False, "consume should fail on empty buffer"
        assert result["task"] is None, "consume should return no task on empty buffer"

    @staticmethod
    async def test_consume_expired_field_is_boolean(
        limiter, func_path, payload
    ):
        """Contract: the ``consume()`` expired flag must always be a boolean."""
        # Arrange
        await limiter.schedule_task(func_path, payload)

        # Act
        result = await limiter.consume()

        # Assert
        assert isinstance(result["expired"], bool), "expired must be a bool"

    @staticmethod
    async def test_consume_result_fields_have_distinct_values(
        limiter, func_path, payload
    ):
        """Contract: ``consume()`` result fields must map to the correct Lua return indices.

        This test creates a state where ``val_previous``, ``val_current``, and
        ``reset_in_ms`` are distinguishable from one another to catch index swap
        mutations in the result parsing logic.
        """
        # Arrange
        await limiter.schedule_task(func_path, payload)

        # Act
        result = await limiter.consume()

        # Assert
        assert result["success"] is True, "consume should succeed with a scheduled task"
        # In the first window, val_previous must be 0 and val_current must be >= 1.
        assert result["val_previous"] == 0, (
            "val_previous should be 0 in the first window"
        )
        assert result["val_current"] >= 1, (
            "val_current should reflect the consumed task count"
        )
        # reset_in_ms is a positive countdown; remaining_tasks is 0 after the only task is consumed.
        assert result["reset_in_ms"] > 0, (
            "reset_in_ms should be a positive countdown within the current window"
        )
        assert result["remaining_tasks"] == 0, (
            "remaining_tasks should be 0 after the only scheduled task is consumed"
        )

    @staticmethod
    async def test_execution_lock_context_manager_yields_boolean(limiter):
        """Contract: ``execution_lock()`` must yield a boolean indicating the acquisition result."""
        # Act & Assert
        async with limiter.execution_lock(timeout_ms=50) as acquired:
            assert isinstance(acquired, bool), "execution_lock must yield a boolean"

    @staticmethod
    async def test_execution_lock_default_timeout(limiter):
        """Contract: ``execution_lock()`` must work with the default timeout_ms parameter."""
        # Act & Assert
        # Calling without arguments exercises the default timeout_ms=5000.
        async with limiter.execution_lock() as acquired:
            assert isinstance(acquired, bool), "execution_lock with default timeout must yield a boolean"
            assert acquired is True, "execution_lock should succeed when uncontested"

    @staticmethod
    async def test_get_buffer_count_returns_nonnegative_integer(limiter):
        """Contract: ``get_buffer_count()`` must return a non-negative integer."""
        # Act
        count = await limiter.get_buffer_count()

        # Assert
        assert isinstance(count, int), "buffer count must be an integer"
        assert count >= 0, "buffer count must be non-negative"

    @staticmethod
    async def test_get_status_returns_dict_with_required_sections(limiter):
        """Contract: ``get_status()`` must return a dictionary containing all required top-level status sections."""
        # Act
        result = await limiter.get_status()

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
