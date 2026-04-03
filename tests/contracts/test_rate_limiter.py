"""Contract tests that any RateLimiter implementation must satisfy.

These tests define the expected behaviour for all implementations of
``AbstractDistributedRateLimiter`` and ``AbstractAsyncDistributedRateLimiter``.
Any concrete implementation should inherit from ``RateLimiterContractTest`` and
provide its own limiter fixture.

Sync implementations can use the ``SyncToAsyncLimiterAdapter`` from
``tests.helpers.adapters`` to satisfy the async test interface.

Fixture dependencies:
    - ``limiter``: must be provided by subclass conftest.
    - ``async_redis_client``, ``func_path``, ``payload``: from ``tests/conftest.py``.
"""

import inspect
from unittest.mock import MagicMock

import pytest

from redis_rate_limiter.core.async_limiters import (
    AbstractAsyncDistributedRateLimiter,
)
from redis_rate_limiter.core.limiters import AbstractDistributedRateLimiter


class _BareSyncLimiter(AbstractDistributedRateLimiter):
    """Subclass that does not override ``_dispatch_task``.

    Used by ``TestAbstractDispatchHook`` to verify that the sync base class
    properly enforces the interface contract via ``NotImplementedError``.
    """


class _BareAsyncLimiter(AbstractAsyncDistributedRateLimiter):
    """Subclass that does not override ``_dispatch_task``.

    Used by ``TestAbstractDispatchHook`` to verify that the async base class
    properly enforces the interface contract via ``NotImplementedError``.
    """


@pytest.mark.contract
class TestAbstractDispatchHook:
    """Verifies that ``_dispatch_task`` enforces the override contract."""

    @staticmethod
    @pytest.mark.parametrize(
        "bare_cls",
        [_BareSyncLimiter, _BareAsyncLimiter],
        ids=["sync", "async"],
    )
    async def test_dispatch_task_raises_not_implemented(bare_cls):
        """Contract: ``_dispatch_task`` must raise ``NotImplementedError``
        when not overridden."""
        # Arrange
        limiter = bare_cls(
            redis_client=MagicMock(),
            limiter_id="test-bare",
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )

        # Act & Assert
        with pytest.raises(NotImplementedError, match="^Subclasses"):
            result = limiter._dispatch_task("myapp.tasks.process", {}, "task-1")
            if inspect.isawaitable(result):
                await result


class RateLimiterContractTest:
    """Abstract test suite that any RateLimiter implementation must pass.

    Subclasses are required to provide the following:
        - limiter: A fixture that returns a configured limiter instance
          (or a ``SyncToAsyncLimiterAdapter`` wrapping a sync limiter).
        - async_redis_client: An async fixture that returns an async Redis client.

    Example (async backend, conftest provides ``limiter`` directly):
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
        # Arrange
        limiter._drain_paused_until = 5_000_000_000.0

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
        # Arrange
        limiter._drain_paused_until = 5_000_000_000.0

        # Act
        success, task_id = await limiter.schedule_task(func_path, payload)

        # Assert
        assert success is True, "scheduling should succeed"
        buffer_size = await async_redis_client.zcard(limiter.buffer_key)
        assert buffer_size == 1, "buffer must contain exactly the one scheduled task"

    @staticmethod
    async def test_schedule_duplicate_task_returns_false(limiter, func_path, payload):
        """Contract: scheduling identical tasks must return
        ``False`` for the duplicate."""
        # Arrange
        limiter._drain_paused_until = 5_000_000_000.0

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
        # Assert
        # Verify that the required attributes exist.
        assert hasattr(limiter, "id"), "limiter must have an 'id' attribute"
        assert hasattr(limiter, "redis"), "limiter must have a 'redis' attribute"
        assert hasattr(limiter, "buffer_key"), "limiter must have a 'buffer_key'"
        assert hasattr(limiter, "concurrency_key"), (
            "limiter must have a 'concurrency_key'"
        )
        assert hasattr(limiter, "dlq_key"), "limiter must have a 'dlq_key'"
        assert hasattr(limiter, "lock_key"), "limiter must have a 'lock_key'"
        assert hasattr(limiter, "limit"), "limiter must have a 'limit' attribute"
        assert hasattr(limiter, "window"), "limiter must have a 'window' attribute"
        assert hasattr(limiter, "max_concurrency"), (
            "limiter must have a 'max_concurrency'"
        )
        assert hasattr(limiter, "max_age"), "limiter must have a 'max_age' attribute"
        assert hasattr(limiter, "lease_duration"), (
            "limiter must have a 'lease_duration'"
        )

        # Verify that the attributes have valid types.
        assert isinstance(limiter.limit, int), "limit must be an int"
        assert isinstance(limiter.window, (int, float)), "window must be numeric"
        assert isinstance(limiter.max_concurrency, int), (
            "max_concurrency must be an int"
        )
        assert isinstance(limiter.max_age, (int, float)), "max_age must be numeric"
        assert isinstance(limiter.lease_duration, (int, float)), (
            "lease_duration must be numeric"
        )

        # Verify that the attributes have valid values.
        assert limiter.limit > 0, "limit must be positive"
        assert limiter.window > 0, "window must be positive"
        assert limiter.max_concurrency > 0, "max_concurrency must be positive"
        assert limiter.max_age > 0, "max_age must be positive"
        assert limiter.lease_duration > 0, "lease_duration must be positive"

    @staticmethod
    async def test_schedule_multiple_different_tasks(limiter):
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
        assert result["remaining_tokens"] == limiter.limit, (
            "remaining_tokens must equal limit on empty buffer with no prior consumes"
        )
        assert result["active_concurrency"] == 0, (
            "active_concurrency must be 0 when no tasks are dispatched"
        )
        assert result["reset_in_ms"] >= 0, "reset_in_ms must be non-negative"
        assert result["remaining_tasks"] == 0, (
            "remaining_tasks must be 0 on empty buffer"
        )

    @staticmethod
    async def test_consume_empty_buffer_returns_unsuccessful(limiter):
        """Contract: consuming from an empty buffer must return no task and an
        unsuccessful result."""
        # Act
        result = await limiter.consume()

        # Assert
        assert result["success"] is False, "consume should fail on empty buffer"
        assert result["task"] is None, "consume should return no task on empty buffer"

    @staticmethod
    async def test_consume_reflects_correct_state_after_scheduling(
        limiter, func_path, payload
    ):
        """Contract: ``consume()`` must reflect the correct limiter state.

        Schedules three tasks and consumes one. The result must show the
        correct values for all numeric fields: window counters, token
        budget, concurrency, buffer depth, and window countdown.
        """
        # Arrange
        limiter._drain_paused_until = 5_000_000_000.0

        # Schedule three distinct tasks so remaining_tasks=2 after one consume.
        await limiter.schedule_task(func_path, payload)
        await limiter.schedule_task(func_path, {**payload, "__k": "a"})
        await limiter.schedule_task(func_path, {**payload, "__k": "b"})

        # Act
        result = await limiter.consume()

        # Assert
        assert result["val_previous"] == 0, (
            "val_previous should be 0 in the first window"
        )
        assert result["val_current"] == 1, (
            "val_current should be exactly 1 after a single consume"
        )
        assert result["remaining_tokens"] == 4, (
            "remaining_tokens should be limit minus one after a single consume"
        )
        assert result["active_concurrency"] == 1, (
            "active_concurrency should be 1 with one dispatched task"
        )
        assert result["remaining_tasks"] == 2, (
            "remaining_tasks should be 2 with two tasks still in the buffer"
        )
        assert result["reset_in_ms"] > 2, (
            "reset_in_ms should be a countdown value much larger than 2"
        )

    @staticmethod
    async def test_get_status_returns_dict_with_required_sections(limiter):
        """Contract: ``get_status()`` must return a dictionary with all
        required sections."""
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
