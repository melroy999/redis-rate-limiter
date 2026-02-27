"""Tests for the ``AsyncTaskLifecycle`` context manager.

This module tests the async task lifecycle implementation that manages
concurrency slots, in-flight key cleanup, and heartbeat-based lease renewal.
It inherits the unified contract tests from ``TaskLifecycleContractTest`` and
adds implementation-specific behavioral and observability tests that mirror
the sync lifecycle tests in ``test_task_lifecycle.py``.

Fixture dependencies:
    - ``async_redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``async_generic_limiter``, ``task_id``: from ``tests/implementations/conftest.py``.
"""

import asyncio
import inspect
import logging
import os
import signal
from typing import Literal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from celery_rate_limiter.core import AsyncTaskLifecycle
from tests.contracts.test_task_lifecycle import TaskLifecycleContractTest
from tests.helpers.utils import assert_log_emitted
from tests.implementations.conftest import MinimalAsyncRateLimiter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HeartbeatFailureMode = Literal["warn", "kill"]
HEARTBEAT_OVERRIDE_CASES: list[tuple[HeartbeatFailureMode, HeartbeatFailureMode]] = [
    ("warn", "kill"),
    ("kill", "warn"),
]


@pytest.fixture
def mock_limiter(async_redis_client, task_id):
    """Create a mock async limiter that uses the real async Redis client but mocks internal helpers.

    This fixture provides a limiter with real async Redis operations but mocked
    backend-specific methods, thereby avoiding the need for a full backend setup.

    ``MagicMock`` is used as the base rather than ``AsyncMock`` because the
    lifecycle code calls ``get_inflight_key()`` synchronously (without ``await``).
    Methods that the lifecycle awaits (``trigger_consume``, ``extend_lease``) are
    explicitly set to ``AsyncMock`` instances.
    """
    limiter = MagicMock()

    # Use the real async Redis client for actual Redis operations.
    limiter.redis = async_redis_client
    limiter.concurrency_key = "test:concurrency"
    limiter.id = "test_limiter"
    limiter.get_inflight_key.side_effect = lambda _: f"test:inflight:{task_id}"

    # A short duration is used for fast test execution.
    limiter.lease_duration = 0.2

    # Async methods that the lifecycle awaits.
    limiter.trigger_consume = AsyncMock()
    limiter.extend_lease = AsyncMock(return_value=None)
    return limiter


@pytest.fixture
def inflight_key(mock_limiter, task_id):
    """Provide the in-flight key for the test task."""
    return mock_limiter.get_inflight_key(task_id)


@pytest.fixture
def create_lifecycle():
    """Provide a factory for ``AsyncTaskLifecycle`` instances.

    The unified contract tests use ``async with create_lifecycle(limiter, task_id):``,
    and the async lifecycle is used natively without an adapter.
    """

    def _factory(limiter, task_id, **kwargs):
        return AsyncTaskLifecycle(limiter, task_id, **kwargs)

    return _factory


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestAsyncTaskLifecycle(TaskLifecycleContractTest):
    """Contract compliance for the ``AsyncTaskLifecycle`` context manager implementation."""

    pass


class TestAsyncTaskLifecycleImplementation:
    """Tests for implementation-specific behaviour of the AsyncTaskLifecycle context manager."""

    @staticmethod
    async def test_lifecycle_with_multiple_concurrent_tasks(
        async_redis_client, mock_limiter, task_id, inflight_key
    ):
        """Verify that the lifecycle only removes the specific task from the concurrency set."""
        # Arrange
        # Simulate five concurrent tasks.
        concurrent_tasks = {
            "other_task_1": 100,
            "other_task_2": 100,
            "other_task_3": 100,
            "other_task_4": 100,
            task_id: 100,
        }
        await async_redis_client.zadd(mock_limiter.concurrency_key, concurrent_tasks)
        await async_redis_client.set(inflight_key, "1")

        # Act
        async with AsyncTaskLifecycle(mock_limiter, task_id):
            # During execution, all five tasks should be present.
            assert await async_redis_client.zcard(mock_limiter.concurrency_key) == 5, (
                "all five tasks should be present during execution"
            )

        # Assert
        # After completion, only the target task should have been removed.
        assert await async_redis_client.zcard(mock_limiter.concurrency_key) == 4, (
            "concurrency set should have four tasks after target completion"
        )
        assert (
            await async_redis_client.zscore(mock_limiter.concurrency_key, task_id)
            is None
        ), "target task must be removed from concurrency set"
        assert await async_redis_client.zscore(
            mock_limiter.concurrency_key, "other_task_1"
        ) == pytest.approx(100.0), (
            "other tasks must remain in concurrency set with their original score"
        )
        assert await async_redis_client.exists(inflight_key) == 0, (
            "inflight marker must be removed after completion"
        )

    @staticmethod
    async def test_lifecycle_handles_redis_failure_during_cleanup(
        async_redis_client, mock_limiter, task_id, inflight_key
    ):
        """Verify that the lifecycle raises an exception but still triggers consume on Redis failure."""
        # Arrange
        with patch.object(
            mock_limiter.redis,
            "zrem",
            side_effect=Exception("Redis connection lost"),
        ) as mock_zrem:
            # Act & Assert
            with pytest.raises(Exception, match="Redis connection lost"):
                async with AsyncTaskLifecycle(mock_limiter, task_id):
                    pass

            # Verify that the exception originated from zrem.
            mock_zrem.assert_called_once()

        # Assert that trigger_consume is still invoked.
        mock_limiter.trigger_consume.assert_called_once()

    @staticmethod
    async def test_aexit_cancels_stuck_heartbeat_task(mock_limiter, task_id):
        """Verify that ``__aexit__`` cancels the heartbeat task when it does not finish within the timeout."""
        # Arrange
        lifecycle = AsyncTaskLifecycle(mock_limiter, task_id)

        async def _hang_forever():
            await asyncio.Event().wait()

        stuck_task = asyncio.create_task(_hang_forever())
        lifecycle._task = stuck_task

        # Act
        # The original timeout=1.0 causes __aexit__ to cancel the stuck task
        # after 1 second. Mutating timeout to None would hang indefinitely,
        # causing the 3.0s safety net to trip.
        await asyncio.wait_for(
            lifecycle.__aexit__(None, None, None),
            timeout=3.0,
        )

        # Assert
        assert stuck_task.cancelled(), (
            "stuck heartbeat task should be cancelled by the __aexit__ timeout"
        )

    @staticmethod
    async def test_empty_task_id_skips_inflight_cleanup(async_redis_client):
        """Verify that an empty ``task_id`` skips inflight key deletion."""
        # Arrange
        limiter = MagicMock()
        limiter.redis = async_redis_client
        limiter.concurrency_key = "test:concurrency"
        limiter.id = "test_limiter"
        limiter.lease_duration = 0.2
        limiter.trigger_consume = AsyncMock()
        limiter.extend_lease = AsyncMock(return_value=None)

        # Act
        async with AsyncTaskLifecycle(limiter, task_id=""):
            pass

        # Assert
        # trigger_consume should still be called on exit.
        limiter.trigger_consume.assert_called_once()

    @staticmethod
    @pytest.mark.parametrize(
        "original, override",
        HEARTBEAT_OVERRIDE_CASES,
        ids=["default_warn_override_kill", "default_kill_override_warn"],
    )
    async def test_heartbeat_failure_override_precedence(
        async_redis_client,
        task_id,
        limiter_id,
        original: HeartbeatFailureMode,
        override: HeartbeatFailureMode,
    ):
        """Verify that the override parameter takes precedence over the limiter default."""
        # Arrange
        # A real async limiter is used to test the override mechanism.
        limiter = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_async_lifecycle_heartbeat_override",
            limit=1,
            window=1,
            max_concurrency=1,
            max_age=1,
            on_heartbeat_failure=original,
        )
        await limiter.start()

        try:
            # Act
            # Create the lifecycle with the override.
            lifecycle_with_override = limiter.task_lifecycle(
                task_id,
                on_heartbeat_failure_override=override,
            )

            # Assert
            # The override should take precedence.
            assert lifecycle_with_override.on_failure_action == override, (
                f"override {override} should take precedence over default {original}"
            )
        finally:
            await limiter.shutdown()

    @staticmethod
    async def test_task_lifecycle_passes_all_attributes(
        async_redis_client, task_id, limiter_id
    ):
        """Verify that ``task_lifecycle()`` forwards the task_id, limiter, and default strategy."""
        # Arrange
        limiter = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_async_lifecycle_passthrough",
            limit=1,
            window=1,
            max_concurrency=1,
            max_age=1,
            on_heartbeat_failure="kill",
        )
        await limiter.start()

        try:
            # Act
            # Create the lifecycle without an override.
            lifecycle = limiter.task_lifecycle(task_id)

            # Assert
            assert lifecycle.task_id == task_id, (
                "task_lifecycle should forward task_id to the lifecycle constructor"
            )
            assert lifecycle.limiter is limiter, (
                "task_lifecycle should forward self as the limiter reference"
            )
            assert lifecycle.on_failure_action == "kill", (
                "task_lifecycle should use the limiter default when no override is provided"
            )
        finally:
            await limiter.shutdown()



# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


class TestAsyncTaskLifecycleObservability:
    """Observability tests for the ``AsyncTaskLifecycle`` context manager log emissions."""

    @staticmethod
    async def test_lifecycle_cleanup_emits_debug_log(
        async_redis_client, mock_limiter, task_id, inflight_key, caplog
    ):
        """Verify that the lifecycle cleanup emits a DEBUG log with removal details."""
        # Arrange
        await async_redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        await async_redis_client.set(inflight_key, "1")

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="celery_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(mock_limiter, task_id):
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                "removed_concurrency=True",
                "removed_inflight=True",
            ],
            message="should emit a debug log for concurrency slot release with limiter id, task id, and removal counts",
        )

    @staticmethod
    async def test_empty_task_id_emits_removed_inflight_false(
        async_redis_client, caplog
    ):
        """Verify that an empty ``task_id`` emits ``removed_inflight=False`` in the cleanup log."""
        # Arrange
        limiter = MagicMock()
        limiter.redis = async_redis_client
        limiter.concurrency_key = "test:concurrency"
        limiter.id = "test_limiter"
        limiter.lease_duration = 0.2
        limiter.trigger_consume = AsyncMock()
        limiter.extend_lease = AsyncMock(return_value=None)

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="celery_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(limiter, task_id=""):
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=["removed_inflight=False"],
            message="empty task_id should log removed_inflight=False",
        )


# ---------------------------------------------------------------------------
# Heartbeat loop tests
# ---------------------------------------------------------------------------


class TestAsyncHeartbeatLoop:
    """Tests for the async heartbeat loop that periodically extends the task lease."""

    @staticmethod
    async def test_heartbeat_interval_calculation(mock_limiter, task_id):
        """Verify that the heartbeat interval is correctly calculated as ``lease_duration / 2``.

        Mutation target: ``/ 2`` divisor in ``AsyncTaskLifecycle.__init__`` interval calculation.
        """
        # Arrange & Act
        lifecycle = AsyncTaskLifecycle(mock_limiter, task_id)

        # Assert
        expected_interval = mock_limiter.lease_duration / 2
        assert lifecycle.interval == expected_interval, (
            f"interval must be lease_duration / 2 = {expected_interval} seconds"
        )

    @staticmethod
    async def test_heartbeat_loop_restores_health_on_recovery(
        async_redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop restores the health status after recovering from a failure."""
        # Act
        async with AsyncTaskLifecycle(mock_limiter, task_id) as lifecycle:
            # Simulate an unhealthy state.
            lifecycle.is_healthy = False

            # Wait for the heartbeat to execute multiple times.
            await asyncio.sleep(0.75 * mock_limiter.lease_duration)

            # Assert
            # The lifecycle should have recovered and be marked as healthy.
            assert lifecycle.is_healthy, "lifecycle must restore health after recovery"

    @staticmethod
    async def test_heartbeat_loop_flags_unhealthy_on_failure_warn_mode(
        async_redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop flags the lifecycle as unhealthy on failure in warn mode."""
        # Arrange
        mock_limiter.extend_lease = AsyncMock(
            side_effect=Exception("Simulated Redis failure")
        )

        # Act
        async with AsyncTaskLifecycle(
            mock_limiter, task_id, on_heartbeat_failure="warn"
        ) as lifecycle:
            # Wait for the heartbeat to fail.
            await asyncio.sleep(0.75 * mock_limiter.lease_duration)

            # Assert
            # The lifecycle should be marked as unhealthy.
            assert not lifecycle.is_healthy, (
                "lifecycle must be marked unhealthy after heartbeat failure"
            )

    @staticmethod
    async def test_heartbeat_loop_terminates_worker_on_failure_kill_mode(
        async_redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop terminates the worker on failure in kill mode."""
        # Arrange
        mock_limiter.extend_lease = AsyncMock(
            side_effect=Exception("Simulated Redis failure")
        )

        # Act & Assert
        with patch("os.kill") as mock_kill:
            async with AsyncTaskLifecycle(
                mock_limiter, task_id, on_heartbeat_failure="kill"
            ):
                # Wait for the heartbeat to fail and trigger termination.
                await asyncio.sleep(0.75 * mock_limiter.lease_duration)

                # Verify that termination was attempted.
                assert mock_kill.call_count > 0, (
                    "os.kill must be called in kill mode on heartbeat failure"
                )

                # Verify the correct signal and PID.
                mock_kill.assert_called_with(os.getpid(), signal.SIGTERM)

    @staticmethod
    async def test_heartbeat_loop_stops_on_exit(
        async_redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop stops when exiting the lifecycle context."""
        # Act
        lifecycle = AsyncTaskLifecycle(mock_limiter, task_id)
        await lifecycle.__aenter__()

        # The task should be running.
        assert lifecycle._task is not None, "task must be created on enter"
        assert not lifecycle._task.done(), "task must be running during lifecycle"

        # Exit the context.
        await lifecycle.__aexit__(None, None, None)

        # Wait for the task to stop.
        await asyncio.sleep(0.1)

        # Assert
        # The task should have stopped.
        assert lifecycle._stop_event.is_set(), "stop event must be set on exit"
        assert lifecycle._task.done(), "task must be done after exiting lifecycle"

    @staticmethod
    async def test_aexit_without_aenter_skips_task_join(
        async_redis_client, mock_limiter, task_id
    ):
        """Verify that ``__aexit__`` completes cleanly when ``_task`` is ``None``.

        Calling ``__aexit__`` without a preceding ``__aenter__`` leaves
        ``_task`` as ``None``. The guard ``if self._task is not None and
        not self._task.done()`` must short-circuit on the first operand.
        An ``and``-to-``or`` mutation would evaluate ``None.done()`` and
        raise ``AttributeError``.
        """
        # Arrange
        lifecycle = AsyncTaskLifecycle(mock_limiter, task_id)
        assert lifecycle._task is None, "task must be None before __aenter__"

        # Act
        # __aexit__ must not raise when _task is None.
        await lifecycle.__aexit__(None, None, None)

        # Assert
        assert lifecycle._stop_event.is_set(), (
            "stop event must be set even without a prior __aenter__"
        )
        mock_limiter.trigger_consume.assert_called_once()

    @staticmethod
    async def test_heartbeat_loop_calls_extend_lease_with_correct_parameters(
        async_redis_client, mock_limiter, task_id
    ):
        """Verify that the heartbeat loop calls ``extend_lease`` with the correct ``task_id`` and duration.

        Mutation target: argument order in the ``extend_lease`` call inside the async heartbeat loop.
        """
        # Act
        async with AsyncTaskLifecycle(mock_limiter, task_id):
            await asyncio.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        # Verify that extend_lease was called with the correct parameters.
        assert mock_limiter.extend_lease.call_count >= 1, (
            "extend_lease must be called at least once with correct parameters"
        )
        for call in mock_limiter.extend_lease.call_args_list:
            assert call[0][0] == task_id, "extend_lease must be called with task_id"
            assert call[0][1] == mock_limiter.lease_duration, (
                "extend_lease must be called with lease_duration"
            )


# ---------------------------------------------------------------------------
# Heartbeat observability tests
# ---------------------------------------------------------------------------


class TestAsyncHeartbeatLoopObservability:
    """Observability tests for log emissions from the async heartbeat loop."""

    @staticmethod
    async def test_lifecycle_entry_emits_debug_log(mock_limiter, task_id, caplog):
        """Verify that lifecycle entry emits a DEBUG log with limiter id, task id, and heartbeat interval."""
        # Act
        with caplog.at_level(
            logging.DEBUG, logger="celery_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(mock_limiter, task_id):
                await asyncio.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                "heartbeat_interval_s=0.1",
            ],
            message="should emit a debug log for lifecycle entry with limiter id, task id, and heartbeat interval",
        )

    @staticmethod
    async def test_heartbeat_recovery_emits_info_log(mock_limiter, task_id, caplog):
        """Verify that heartbeat recovery emits an INFO log with task id and limiter id."""
        # Act
        with caplog.at_level(
            logging.INFO, logger="celery_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(mock_limiter, task_id) as lifecycle:
                lifecycle.is_healthy = False
                await asyncio.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[
                f"task {task_id}",
                f"limiter {mock_limiter.id}",
                "restored",
            ],
            message="should emit an info log for heartbeat connection restoration with task id and limiter id",
        )

    @staticmethod
    async def test_heartbeat_failure_warn_mode_emits_critical_log(
        mock_limiter, task_id, caplog
    ):
        """Verify that heartbeat failure in warn mode emits a CRITICAL log."""
        # Arrange
        mock_limiter.extend_lease = AsyncMock(
            side_effect=Exception("Simulated Redis failure")
        )

        # Act
        with caplog.at_level(
            logging.CRITICAL, logger="celery_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(
                mock_limiter, task_id, on_heartbeat_failure="warn"
            ):
                await asyncio.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="CRITICAL",
            required_fragments=[
                f"task {task_id}",
                "flagged as unhealthy",
                "Simulated Redis failure",
            ],
            message="should emit a critical log for heartbeat failure with task id and error message",
        )

    @staticmethod
    async def test_heartbeat_failure_kill_mode_emits_critical_log(
        mock_limiter, task_id, caplog
    ):
        """Verify that heartbeat failure in kill mode emits a CRITICAL log with termination action."""
        # Arrange
        mock_limiter.extend_lease = AsyncMock(
            side_effect=Exception("Simulated Redis failure")
        )

        # Act
        with caplog.at_level(
            logging.CRITICAL, logger="celery_rate_limiter.core.async_limiters"
        ):
            with patch("os.kill"):
                async with AsyncTaskLifecycle(
                    mock_limiter, task_id, on_heartbeat_failure="kill"
                ):
                    await asyncio.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="CRITICAL",
            required_fragments=[
                f"task {task_id}",
                "terminating worker",
                "Simulated Redis failure",
            ],
            message="should emit a critical log for heartbeat kill mode with task id and error message",
        )


# ---------------------------------------------------------------------------
# Async extend lease tests
# ---------------------------------------------------------------------------


class TestAsyncExtendLease:
    """Tests for async ``extend_lease()`` success and error handling."""

    @staticmethod
    async def test_extend_lease_raises_key_error_for_unknown_task(
        async_generic_limiter,
    ):
        """Verify that ``extend_lease()`` raises a KeyError for unknown task identifiers."""
        # Act & Assert
        with pytest.raises(KeyError, match=r'in the concurrency set\."'):
            await async_generic_limiter.extend_lease("nonexistent", 30)

    @staticmethod
    async def test_extend_lease_succeeds_for_existing_task(
        async_generic_limiter, async_redis_client, task_id
    ):
        """Verify that ``extend_lease()`` updates the score for a task present in the concurrency set."""
        # Arrange
        # Seed the concurrency sorted set with a low score so the update is observable.
        initial_score = 1000.0
        await async_redis_client.zadd(
            async_generic_limiter.concurrency_key, {task_id: initial_score}
        )

        # Act
        await async_generic_limiter.extend_lease(task_id, 30)

        # Assert
        new_score = await async_redis_client.zscore(
            async_generic_limiter.concurrency_key, task_id
        )
        assert new_score is not None, (
            "task should still be present in the concurrency set after lease extension"
        )
        assert new_score > initial_score, (
            "lease extension should update the score to a value greater than the initial score"
        )

    @staticmethod
    async def test_extend_lease_passes_correct_arguments_to_lua(
        async_generic_limiter, task_id
    ):
        """Verify that ``extend_lease()`` invokes ``_eval_script`` with the expected arguments.

        Mutation target: argument order and values in the ``_eval_script`` call within ``extend_lease()``.
        """
        # Arrange
        duration = 45

        with patch.object(
            async_generic_limiter, "_eval_script", return_value=1
        ) as mock_eval:
            # Act
            await async_generic_limiter.extend_lease(task_id, duration)

        # Assert
        mock_eval.assert_called_once_with(
            "renew.lua",
            1,
            async_generic_limiter.concurrency_key,
            task_id,
            duration,
        )


# ---------------------------------------------------------------------------
# Async extend lease observability tests
# ---------------------------------------------------------------------------


class TestAsyncExtendLeaseObservability:
    """Observability tests for async ``extend_lease()`` log emissions."""

    @staticmethod
    async def test_extend_lease_unknown_task_emits_debug_log(
        async_generic_limiter, caplog
    ):
        """Verify that ``extend_lease()`` emits a DEBUG log with ``renewed=False`` for unknown tasks."""
        # Act
        with caplog.at_level(
            logging.DEBUG, logger="celery_rate_limiter.core.async_limiters"
        ):
            with pytest.raises(KeyError, match="not found in the concurrency set"):
                await async_generic_limiter.extend_lease("nonexistent", 30)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={async_generic_limiter.id}",
                "task_id=nonexistent",
                "duration_s=30",
                "renewed=False",
            ],
            message="should emit a debug log containing the limiter id, task id, duration, and renewed=False",
        )

    @staticmethod
    async def test_extend_lease_success_emits_debug_log(
        async_generic_limiter, async_redis_client, task_id, caplog
    ):
        """Verify that ``extend_lease()`` emits a DEBUG log with ``renewed=True`` for existing tasks."""
        # Arrange
        await async_redis_client.zadd(
            async_generic_limiter.concurrency_key, {task_id: 1000.0}
        )

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="celery_rate_limiter.core.async_limiters"
        ):
            await async_generic_limiter.extend_lease(task_id, 30)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            required_fragments=[
                f"limiter={async_generic_limiter.id}",
                f"task_id={task_id}",
                "duration_s=30",
                "renewed=True",
            ],
            message="should emit a debug log containing the limiter id, task id, duration, and renewed=True",
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


class TestAsyncTaskLifecycleSignatures:
    """Signature tests for ``AsyncTaskLifecycle`` default parameter values."""

    @staticmethod
    def test_default_on_heartbeat_failure_is_warn():
        """Verify that the default ``on_heartbeat_failure`` parameter is lowercase ``'warn'``.

        Mutation target: ``on_heartbeat_failure`` default value in ``AsyncTaskLifecycle.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(AsyncTaskLifecycle.__init__)

        # Assert
        assert sig.parameters["on_heartbeat_failure"].default == "warn", (
            "default on_heartbeat_failure must be lowercase 'warn'"
        )
