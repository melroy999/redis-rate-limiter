"""Tests for the AsyncIO task limiter backend."""

import asyncio
import logging


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
        await limiter.schedule_task(
            "tests.helpers.tasks.async_noop_task", {"key": "a"}
        )
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
        # Assert
        assert limiter._has_local_capacity() is True, (
            "_has_local_capacity should return True when no tasks are dispatched"
        )

        # Arrange
        limiter._active_count = limiter.max_tasks

        # Assert
        assert limiter._has_local_capacity() is False, (
            "_has_local_capacity should return False at max_tasks"
        )

    @staticmethod
    async def test_dispatch_task_creates_asyncio_task(limiter, caplog):
        """Verify that ``_dispatch_task`` creates an asyncio task and tracks it."""
        # Arrange
        assert limiter._active_count == 0, "active count should start at zero"

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.backends.asyncio.limiter"):
            await limiter._dispatch_task(
                "tests.helpers.tasks.async_noop_task", {}, "test-task-id"
            )

        # Assert
        assert limiter._active_count == 1, "active count should be incremented"
        assert len(limiter._active_tasks) == 1, (
            "active tasks set should have one entry"
        )
        assert any(
            record.levelname == "DEBUG"
            and limiter.id in record.message
            and "test-task-id" in record.message
            and "tests.helpers.tasks.async_noop_task" in record.message
            for record in caplog.records
        ), "should emit a debug log containing the limiter id, task id, and func path"

    @staticmethod
    async def test_dispatch_sync_function_raises_type_error(limiter):
        """Verify that dispatching a synchronous function raises ``TypeError`` internally."""
        # Arrange
        assert limiter._active_count == 0, "active count should start at zero"

        # Act
        # Dispatch a sync function; the TypeError is raised inside the created
        # asyncio task and caught by the internal exception handler.
        await limiter._dispatch_task(
            "tests.helpers.tasks.noop_task", {}, "sync-task-id"
        )

        # Allow the created task to fully complete.
        # A single sleep(0) is insufficient because the task lifecycle context
        # manager performs multiple async Redis operations during cleanup.
        await asyncio.gather(*list(limiter._active_tasks), return_exceptions=True)

        # Assert
        # The task should have completed (with an error), cleaning up after itself.
        assert limiter._active_count == 0, (
            "active count should return to zero after sync function rejection"
        )
        assert len(limiter._active_tasks) == 0, (
            "active tasks set should be empty after sync function rejection"
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
        assert limiter._active_count == 0, (
            "active count should be zero after shutdown"
        )
