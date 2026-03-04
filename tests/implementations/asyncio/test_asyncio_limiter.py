"""Tests for the AsyncIO task limiter backend.

Fixture dependencies:
    - ``limiter``, ``_reset_asyncio_limiter_class_state``: from ``tests/implementations/asyncio/conftest.py``.
"""

import asyncio
import logging

from tests.helpers.utils import assert_log_emitted


class TestAsyncIOTaskLimiter:
    """Tests for the ``AsyncIOTaskLimiter`` backend."""

    @staticmethod
    async def test_schedule_task_returns_success(limiter):
        """Verify that scheduling a task returns a success flag and a task identifier."""
        # Act
        scheduled, task_id = await limiter.schedule_task(
            "tests.helpers.tasks.async_noop_task", {"key": "value"}
        )

        # Assert
        assert scheduled is True, "first scheduling should succeed"
        assert isinstance(task_id, str), "task_id should be a string"

    @staticmethod
    async def test_schedule_duplicate_task_returns_false(limiter):
        """Verify that scheduling the same task twice returns ``False`` on the second attempt."""
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
        """Verify that ``get_buffer_count`` returns the correct count after scheduling.

        Because the async drain loop runs cooperatively in the same event loop,
        tasks may be consumed between schedule calls. The assertion checks that
        the combined count of buffered and dispatched tasks equals the expected total.
        """
        # Arrange
        await limiter.schedule_task("tests.helpers.tasks.async_noop_task", {"key": "a"})
        await limiter.schedule_task(
            "tests.helpers.tasks.async_noop_task_2", {"key": "b"}
        )

        # Act
        count = await limiter.get_buffer_count()
        total = count + limiter._active_count

        # Assert
        assert total == 2, (
            f"buffer ({count}) + active ({limiter._active_count}) "
            f"should equal 2 scheduled tasks"
        )

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
        assert "dispatcher" in status, "status should include dispatcher section"

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


class TestAsyncIODispatchObservability:
    """Observability tests for the ``_dispatch_task`` log emissions."""

    @staticmethod
    async def test_dispatch_task_emits_debug_log(limiter, caplog):
        """Verify that ``_dispatch_task`` emits a DEBUG log with limiter id, task id, func path, and active count."""
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
            required_fragments=[
                f"limiter={limiter.id}",
                "task_id=test-task-id",
                "func_path=tests.helpers.tasks.async_noop_task",
                "active_count=1",
            ],
            message="should emit a debug log containing the limiter id, task id, func path, and active count",
        )

    @staticmethod
    async def test_task_exception_emits_error_log(limiter, caplog):
        """Verify that ``_dispatch_task`` emits an ERROR log when the dispatched task raises an exception."""
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
            required_fragments=[
                f"limiter={limiter.id}",
                "task_id=sync-err-task",
                "func_path=tests.helpers.tasks.noop_task",
            ],
            message="should emit an error log containing the limiter id, task id, and func path on task exception",
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
            required_fragments=[
                f"limiter={limiter.id}",
                "Cancelling",
            ],
            message="should emit an info log with the limiter id when cancelling active tasks",
        )

    @staticmethod
    async def test_dispatch_task_normal_execution_does_not_emit_error_log(
        limiter, caplog
    ):
        """Verify that ``_dispatch_task`` does not emit error logs when the target function executes successfully."""
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
