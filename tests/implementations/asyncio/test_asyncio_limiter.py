"""Tests for the AsyncIO task limiter backend."""


class TestAsyncIOTaskLimiter:
    """Tests for the ``AsyncIOTaskLimiter`` backend."""

    @staticmethod
    async def test_schedule_task_returns_success(asyncio_limiter):
        """Verify that scheduling a task returns a success flag and a task identifier."""
        # Act
        scheduled, task_id = await asyncio_limiter.schedule_task(
            "tests.helpers.tasks.noop_task", {"key": "value"}
        )

        # Assert
        assert scheduled is True, "first scheduling should succeed"
        assert isinstance(task_id, str), "task_id should be a string"

    @staticmethod
    async def test_schedule_duplicate_task_returns_false(asyncio_limiter):
        """Verify that scheduling the same task twice returns ``False`` on the second attempt."""
        # Arrange
        await asyncio_limiter.schedule_task(
            "tests.helpers.tasks.noop_task", {"key": "value"}
        )

        # Act
        scheduled, _ = await asyncio_limiter.schedule_task(
            "tests.helpers.tasks.noop_task", {"key": "value"}
        )

        # Assert
        assert scheduled is False, "duplicate scheduling should return False"

    @staticmethod
    async def test_consume_returns_expected_structure(asyncio_limiter):
        """Verify that ``consume`` returns a result with the expected fields."""
        # Arrange
        await asyncio_limiter.schedule_task(
            "tests.helpers.tasks.noop_task", {"key": "value"}
        )

        # Act
        result = await asyncio_limiter.consume()

        # Assert
        assert "success" in result, "result should have a 'success' field"
        assert "task" in result, "result should have a 'task' field"
        assert "remaining_tokens" in result, "result should have 'remaining_tokens'"
        assert "active_concurrency" in result, "result should have 'active_concurrency'"

    @staticmethod
    async def test_consume_empty_buffer_returns_unsuccessful(asyncio_limiter):
        """Verify that consuming from an empty buffer returns an unsuccessful result."""
        # Act
        result = await asyncio_limiter.consume()

        # Assert
        assert result["success"] is False, "empty buffer consume should be unsuccessful"

    @staticmethod
    async def test_get_buffer_count_reflects_scheduled_tasks(asyncio_limiter):
        """Verify that ``get_buffer_count`` returns the correct count after scheduling.

        Because the async drain loop runs cooperatively in the same event loop,
        tasks may be consumed between schedule calls. The assertion checks that
        the combined count of buffered and dispatched tasks equals the expected total.
        """
        # Arrange
        await asyncio_limiter.schedule_task(
            "tests.helpers.tasks.noop_task", {"key": "a"}
        )
        await asyncio_limiter.schedule_task(
            "tests.helpers.tasks.noop_task_2", {"key": "b"}
        )

        # Act
        count = await asyncio_limiter.get_buffer_count()
        total = count + asyncio_limiter._active_count

        # Assert
        assert total == 2, (
            f"buffer ({count}) + active ({asyncio_limiter._active_count}) "
            f"should equal 2 scheduled tasks"
        )

    @staticmethod
    async def test_get_status_returns_dict_with_required_sections(
        asyncio_limiter,
    ):
        """Verify that ``get_status`` returns a dictionary with expected sections."""
        # Act
        status = await asyncio_limiter.get_status()

        # Assert
        assert "limiter_id" in status, "status should include limiter_id"
        assert "concurrency" in status, "status should include concurrency section"
        assert "buffer" in status, "status should include buffer section"
        assert "rate_limit" in status, "status should include rate_limit section"
        assert "dispatcher" in status, "status should include dispatcher section"

    @staticmethod
    async def test_has_local_capacity_respects_max_tasks(asyncio_limiter):
        """Verify that ``_has_local_capacity`` returns ``False`` at ``max_tasks``."""
        # Assert
        assert asyncio_limiter._has_local_capacity() is True, (
            "_has_local_capacity should return True when no tasks are dispatched"
        )

        # Arrange
        asyncio_limiter._active_count = asyncio_limiter.max_tasks

        # Assert
        assert asyncio_limiter._has_local_capacity() is False, (
            "_has_local_capacity should return False at max_tasks"
        )

    @staticmethod
    async def test_dispatch_task_creates_asyncio_task(asyncio_limiter):
        """Verify that ``_dispatch_task`` creates an asyncio task and tracks it."""
        # Arrange
        assert asyncio_limiter._active_count == 0, "active count should start at zero"

        # Act
        await asyncio_limiter._dispatch_task(
            "tests.helpers.tasks.noop_task", {}, "test-task-id"
        )

        # Assert
        assert asyncio_limiter._active_count == 1, "active count should be incremented"
        assert len(asyncio_limiter._active_tasks) == 1, (
            "active tasks set should have one entry"
        )

    @staticmethod
    async def test_shutdown_cancels_active_tasks(asyncio_limiter):
        """Verify that ``shutdown`` cancels all active asyncio tasks."""
        # Arrange
        await asyncio_limiter._dispatch_task(
            "tests.helpers.tasks.slow_task", {}, "slow-task-id"
        )
        assert len(asyncio_limiter._active_tasks) >= 1, (
            "at least one task should be active before shutdown"
        )

        # Act
        await asyncio_limiter.shutdown()

        # Assert
        assert len(asyncio_limiter._active_tasks) == 0, (
            "all tasks should be cleared after shutdown"
        )
        assert asyncio_limiter._active_count == 0, (
            "active count should be zero after shutdown"
        )
