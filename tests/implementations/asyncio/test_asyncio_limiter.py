"""Tests for the AsyncIO task limiter backend.

Fixture dependencies:
    - ``limiter``, ``_reset_asyncio_limiter_class_state``:
      from ``tests/implementations/asyncio/conftest.py``.
"""

import asyncio
import logging

import pytest

from redis_rate_limiter.backends.asyncio import AsyncIOTaskLimiter
from tests.helpers.utils import assert_log_emitted


@pytest.mark.behavior
class TestAsyncIOTaskLimiterClassApi:
    """AsyncIO-specific tests for class API and backend context behaviour."""

    @staticmethod
    async def test_configure_without_max_tasks_raises_error(async_redis_client):
        """Verify that ``configure`` raises an error when
        the ``max_tasks`` argument is not provided."""
        # Arrange
        AsyncIOTaskLimiter._reset()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match=r"^AsyncIOTaskLimiter\.configure\(redis_client, max_tasks=N\) must be called",
        ):
            AsyncIOTaskLimiter.configure(async_redis_client)

    @staticmethod
    async def test_reset_clears_backend_context_to_none():
        """Verify that ``_reset()`` sets the backend
        attribute to exactly ``None``."""
        # Act
        AsyncIOTaskLimiter._reset()

        # Assert
        # _has_backend_context uses ``is not None``, so a falsy
        # non-None value like 0 would incorrectly signal that
        # max_tasks is configured.
        assert AsyncIOTaskLimiter._max_tasks is None, (
            "_max_tasks must be None after reset, not another falsy value"
        )


@pytest.mark.behavior
class TestAsyncIOTaskLimiter:
    """Tests for the ``AsyncIOTaskLimiter`` backend."""

    @staticmethod
    async def test_schedule_task_returns_success(limiter):
        """Verify that scheduling a task returns a success
        flag and a task identifier."""
        # Act
        scheduled, task_id = await limiter.schedule_task(
            "tests.helpers.tasks.async_noop_task", {"key": "value"}
        )

        # Assert
        assert scheduled is True, "first scheduling should succeed"
        assert isinstance(task_id, str), "task_id should be a string"

    @staticmethod
    async def test_schedule_duplicate_task_returns_false(limiter):
        """Verify that scheduling the same task twice
        returns ``False`` on the second attempt."""
        # Arrange
        await limiter.schedule_task(
            "tests.helpers.tasks.async_noop_task", {"key": "value"}
        )

        # Act
        scheduled, _ = await limiter.schedule_task(
            "tests.helpers.tasks.async_noop_task", {"key": "value"}
        )

        # Assert
        assert scheduled is False, "duplicate scheduling should return False"

    @staticmethod
    async def test_consume_returns_expected_structure(limiter):
        """Verify that ``consume`` returns a result with the expected fields."""
        # Arrange
        await limiter.schedule_task(
            "tests.helpers.tasks.async_noop_task", {"key": "value"}
        )

        # Act
        result = await limiter.consume()

        # Assert
        assert "success" in result, "result should have a 'success' field"
        assert "task" in result, "result should have a 'task' field"
        assert "remaining_tokens" in result, "result should have 'remaining_tokens'"
        assert "active_concurrency" in result, "result should have 'active_concurrency'"

    @staticmethod
    async def test_consume_empty_buffer_returns_unsuccessful(limiter):
        """Verify that consuming from an empty buffer returns an unsuccessful result."""
        # Act
        result = await limiter.consume()

        # Assert
        assert result["success"] is False, "empty buffer consume should be unsuccessful"

    @staticmethod
    async def test_get_buffer_count_reflects_scheduled_tasks(limiter):
        """Verify that ``get_buffer_count`` returns the correct count after scheduling."""
        # Arrange
        limiter._drain_paused_until = 5_000_000_000.0
        await limiter.schedule_task("tests.helpers.tasks.async_noop_task", {"key": "a"})
        await limiter.schedule_task(
            "tests.helpers.tasks.async_noop_task_2", {"key": "b"}
        )

        # Act
        count = await limiter.get_buffer_count()

        # Assert
        assert count == 2, f"buffer count should equal 2 scheduled tasks, got {count}"

    @staticmethod
    async def test_get_status_returns_dict_with_required_sections(
        limiter,
    ):
        """Verify that ``get_status`` returns a dictionary with expected sections."""
        # Act
        status = await limiter.get_status()

        # Assert
        assert "limiter_id" in status, "status should include limiter_id"
        assert "concurrency" in status, "status should include concurrency section"
        assert "buffer" in status, "status should include buffer section"
        assert "rate_limit" in status, "status should include rate_limit section"

    @staticmethod
    async def test_has_local_capacity_respects_max_tasks(limiter):
        """Verify that ``_has_local_capacity`` returns ``False`` at ``max_tasks``."""
        # Arrange
        # Verify precondition: capacity is available when no tasks are dispatched.
        assert limiter._has_local_capacity() is True, (
            "_has_local_capacity should return True when no tasks are dispatched"
        )
        limiter._active_count = limiter.max_tasks

        # Act
        result = limiter._has_local_capacity()

        # Assert
        assert result is False, "_has_local_capacity should return False at max_tasks"

    @staticmethod
    async def test_dispatch_task_creates_asyncio_task(limiter):
        """Verify that ``_dispatch_task`` creates an asyncio task and tracks it."""
        # Arrange
        assert limiter._active_count == 0, "active count should start at zero"

        # Act
        await limiter._dispatch_task(
            "tests.helpers.tasks.async_noop_task", {}, "test-task-id"
        )

        # Assert
        assert limiter._active_count == 1, "active count should be incremented"
        assert len(limiter._active_tasks) == 1, "active tasks set should have one entry"

    @staticmethod
    async def test_dispatch_sync_function_raises_type_error_with_message(
        limiter, caplog
    ):
        """Verify that dispatching a synchronous function
        produces a TypeError with an informative message."""
        # Act
        with caplog.at_level(
            logging.ERROR, logger="redis_rate_limiter.backends.asyncio.limiter"
        ):
            await limiter._dispatch_task(
                "tests.helpers.tasks.noop_task", {}, "sync-type-err"
            )
            # Allow the created task to fully complete (TypeError from sync function).
            await asyncio.gather(*list(limiter._active_tasks), return_exceptions=True)

        # Assert
        error_records = [
            r
            for r in caplog.records
            if r.levelname == "ERROR" and r.exc_info and r.exc_info[1]
        ]
        assert len(error_records) >= 1, (
            "at least one error record with exc_info should be present"
        )
        exception = error_records[0].exc_info[1]
        assert isinstance(exception, TypeError), (
            "the caught exception should be a TypeError"
        )
        assert "requires coroutine functions" in str(exception), (
            "the TypeError message should mention that coroutine functions are required"
        )

    @staticmethod
    async def test_shutdown_cancels_active_tasks(limiter):
        """Verify that ``shutdown`` cancels all active asyncio tasks."""
        # Arrange
        await limiter._dispatch_task(
            "tests.helpers.tasks.slow_task", {}, "slow-task-id"
        )
        assert len(limiter._active_tasks) >= 1, (
            "at least one task should be active before shutdown"
        )

        # Act
        await limiter.shutdown()

        # Assert
        assert len(limiter._active_tasks) == 0, (
            "all tasks should be cleared after shutdown"
        )
        assert limiter._active_count == 0, "active count should be zero after shutdown"


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestAsyncIODispatchObservability:
    """Observability tests for the ``_dispatch_task`` log emissions."""

    @staticmethod
    async def test_dispatch_task_emits_debug_log(limiter, caplog):
        """Verify that ``_dispatch_task`` emits a DEBUG log
        with limiter id, task id, func path, and active
        count."""
        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.backends.asyncio.limiter"
        ):
            await limiter._dispatch_task(
                "tests.helpers.tasks.async_noop_task", {}, "test-task-id"
            )

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[AsyncIOTaskLimiter]",
            required_fragments=[
                f"limiter={limiter.id}",
                "task_id=test-task-id",
                "func_path=tests.helpers.tasks.async_noop_task",
                "active_count=1",
            ],
            message="should emit a debug log containing "
            "the limiter id, task id, func path, "
            "and active count",
        )

    @staticmethod
    async def test_task_exception_emits_error_log(limiter, caplog):
        """Verify that ``_dispatch_task`` emits an ERROR log
        when the dispatched task raises an exception."""
        # Act
        with caplog.at_level(
            logging.ERROR, logger="redis_rate_limiter.backends.asyncio.limiter"
        ):
            await limiter._dispatch_task(
                "tests.helpers.tasks.noop_task", {}, "sync-err-task"
            )
            # Allow the created task to fully complete (TypeError from sync function).
            await asyncio.gather(*list(limiter._active_tasks), return_exceptions=True)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="ERROR",
            label="[AsyncIOTaskLimiter]",
            required_fragments=[
                f"limiter={limiter.id}",
                "task_id=sync-err-task",
                "func_path=tests.helpers.tasks.noop_task",
            ],
            message="should emit an error log containing "
            "the limiter id, task id, and func path "
            "on task exception",
        )

    @staticmethod
    async def test_shutdown_cancellation_emits_info_log(limiter, caplog):
        """Verify that ``shutdown()`` emits an INFO log when cancelling active tasks."""
        # Arrange
        await limiter._dispatch_task(
            "tests.helpers.tasks.slow_task", {}, "cancel-task-id"
        )
        assert len(limiter._active_tasks) >= 1, (
            "at least one task should be active before shutdown"
        )

        # Act
        with caplog.at_level(
            logging.INFO, logger="redis_rate_limiter.backends.asyncio.limiter"
        ):
            await limiter.shutdown()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            label="[AsyncIOTaskLimiter]",
            required_fragments=[
                f"limiter={limiter.id}",
                "Cancelling",
            ],
            message="should emit an info log with the "
            "limiter id when cancelling active tasks",
        )

    @staticmethod
    async def test_dispatch_task_normal_execution_does_not_emit_error_log(
        limiter, caplog
    ):
        """Verify that ``_dispatch_task`` does not emit
        error logs when the target function executes
        successfully."""
        # Act
        with caplog.at_level(
            logging.ERROR, logger="redis_rate_limiter.backends.asyncio.limiter"
        ):
            await limiter._dispatch_task(
                "tests.helpers.tasks.async_noop_task", {}, "exec-task-id"
            )
            await asyncio.gather(*list(limiter._active_tasks), return_exceptions=True)

        # Assert
        assert not any(record.levelname == "ERROR" for record in caplog.records), (
            "target function should execute without error"
        )


# ---------------------------------------------------------------------------
# Boundary tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestAsyncIOTaskLimiterBoundary:
    """Boundary condition tests for ``AsyncIOTaskLimiter`` capacity tracking."""

    @staticmethod
    async def test_active_count_accumulates_across_dispatches(limiter):
        """Verify that dispatching multiple tasks increments
        the active count additively, not by assignment."""
        # Act
        await limiter._dispatch_task(
            "tests.helpers.tasks.slow_task", {}, "accum-task-1"
        )
        await limiter._dispatch_task(
            "tests.helpers.tasks.slow_task", {}, "accum-task-2"
        )

        # Assert
        assert limiter._active_count == 2, (
            "active count should be 2 after two dispatches"
        )

    @staticmethod
    async def test_active_count_decrements_by_one_on_completion(limiter):
        """Verify that completing a task decrements the active
        count by exactly one."""
        # Arrange
        await limiter._dispatch_task(
            "tests.helpers.tasks.async_noop_task", {}, "decr-task-1"
        )
        await asyncio.gather(*list(limiter._active_tasks), return_exceptions=True)
        assert limiter._active_count == 0, (
            "active count should be 0 after completing the only task"
        )

    @staticmethod
    async def test_active_tasks_contains_real_task_objects(limiter):
        """Verify that ``_active_tasks`` contains actual
        ``asyncio.Task`` instances, not ``None``."""
        # Act
        await limiter._dispatch_task(
            "tests.helpers.tasks.slow_task", {}, "real-task-check"
        )

        # Assert
        for entry in limiter._active_tasks:
            assert isinstance(entry, asyncio.Task), (
                "each entry in _active_tasks must be an asyncio.Task"
            )
