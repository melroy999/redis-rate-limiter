"""Tests for the ``AsyncTaskLifecycle`` context manager.

This module tests the async task lifecycle implementation that manages
concurrency slots, in-flight key cleanup, and heartbeat-based lease renewal.
It inherits the unified contract tests from ``TaskLifecycleContractTest`` and
adds implementation-specific behavioral and observability tests that mirror
the sync lifecycle tests in ``test_task_lifecycle.py``.

Fixture dependencies:
    - ``async_redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``async_stub_limiter``, ``task_id``: from
      ``tests/implementations/conftest.py``.
"""

import asyncio
import inspect
import logging
import os
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis

from redis_rate_limiter.core import AsyncTaskLifecycle
from redis_rate_limiter.core.async_limiters import AsyncHeartbeatScheduler
from redis_rate_limiter.core.scripts import load_lua_script
from tests.contracts.test_task_lifecycle import TaskLifecycleContractTest
from tests.helpers.utils import (
    assert_log_emitted,
    shutdown_timer,
)
from tests.implementations.conftest import (
    HEARTBEAT_OVERRIDE_CASES,
    AsyncNoopHeartbeatScheduler,
    AsyncStubRateLimiter,
    HeartbeatFailureMode,
    make_async_eval_script,
)

RELEASE_SOURCE = load_lua_script("release.lua")


@pytest.fixture
def mock_limiter(async_redis_client, limiter_id, task_id):
    """Create a mock async limiter with real Redis but mocked backend methods.

    Uses ``AsyncNoopHeartbeatScheduler`` to avoid spawning real tasks;
    mutations on the scheduler loop would otherwise create busy loops
    in mutmut's forked children. Tests that need real heartbeat
    behaviour should use ``scheduler_limiter`` instead.
    """
    limiter = MagicMock()
    limiter.redis = async_redis_client
    limiter.concurrency_key = f"{limiter_id}:concurrency"
    limiter.id = limiter_id
    limiter.get_inflight_key.side_effect = lambda tid: f"{limiter_id}:inflight:{tid}"

    # A short duration is used for fast test execution.
    limiter.lease_duration = 0.2
    limiter.trigger_consume = AsyncMock()
    limiter.extend_lease = AsyncMock(return_value=None)
    limiter._eval_script = AsyncMock(
        side_effect=make_async_eval_script(async_redis_client)
    )

    # release.lua needs the drain channel and worker id.
    limiter._drain_signal_channel = f"{limiter_id}:drain_signal"
    limiter._worker_id = f"{limiter_id}_worker"

    limiter._heartbeat_scheduler = AsyncNoopHeartbeatScheduler()
    return limiter


@pytest.fixture
async def scheduler_limiter(mock_limiter):
    """``mock_limiter`` with a real ``AsyncHeartbeatScheduler`` attached."""
    scheduler = AsyncHeartbeatScheduler(mock_limiter)
    mock_limiter._heartbeat_scheduler = scheduler
    yield mock_limiter
    await scheduler.shutdown()


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


@pytest.mark.contract
class TestAsyncTaskLifecycle(TaskLifecycleContractTest):
    """Contract compliance for the ``AsyncTaskLifecycle`` context
    manager implementation.
    """

    pass


@pytest.mark.behavior
class TestAsyncTaskLifecycleImplementation:
    """Tests for implementation-specific behaviour of the
    AsyncTaskLifecycle context manager.
    """

    @staticmethod
    async def test_lifecycle_with_multiple_concurrent_tasks(
        async_redis_client, mock_limiter, task_id, inflight_key
    ):
        """Verify that the lifecycle only removes the specific task
        from the concurrency set.
        """
        # Arrange
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
        """Verify that the lifecycle raises an exception but still
        wakes the local drain loop on Redis failure.
        """
        # Arrange
        # The cleanup path calls release.lua via _eval_script; simulate a
        # Redis failure by making _eval_script raise on the cleanup call.
        mock_limiter._eval_script.side_effect = Exception("Redis connection lost")

        # Act & Assert
        with pytest.raises(Exception, match="Redis connection lost"):
            async with AsyncTaskLifecycle(mock_limiter, task_id):
                pass

        mock_limiter._eval_script.assert_called_once()
        mock_limiter._schedule_drain.assert_called_once()

    @staticmethod
    async def test_empty_task_id_skips_inflight_cleanup(async_redis_client, limiter_id):
        """Verify that an empty ``task_id`` skips inflight key deletion."""
        # Arrange
        limiter = MagicMock()
        limiter.redis = async_redis_client
        limiter.concurrency_key = f"{limiter_id}:concurrency"
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        limiter.extend_lease = AsyncMock(return_value=None)
        limiter._eval_script = AsyncMock(
            side_effect=make_async_eval_script(async_redis_client)
        )
        limiter._drain_signal_channel = f"{limiter_id}:drain_signal"
        limiter._worker_id = f"{limiter_id}_worker"
        limiter._heartbeat_scheduler = AsyncHeartbeatScheduler(limiter)

        try:
            # Act
            async with AsyncTaskLifecycle(limiter, task_id=""):
                pass
        finally:
            await limiter._heartbeat_scheduler.shutdown()

        # Assert
        limiter._schedule_drain.assert_called_once()

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
        """Verify that the override parameter takes precedence over
        the limiter default.
        """
        # Arrange
        limiter = AsyncStubRateLimiter(
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
            lifecycle_with_override = limiter.task_lifecycle(
                task_id,
                on_heartbeat_failure_override=override,
            )

            # Assert
            assert lifecycle_with_override.on_failure_action == override, (
                f"override {override} should take precedence over default {original}"
            )
        finally:
            await limiter.shutdown()

    @staticmethod
    async def test_task_lifecycle_passes_all_attributes(
        async_redis_client, task_id, limiter_id
    ):
        """Verify that ``task_lifecycle()`` forwards the task_id,
        limiter, and default strategy.
        """
        # Arrange
        limiter = AsyncStubRateLimiter(
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
            lifecycle = limiter.task_lifecycle(task_id)

            # Assert
            assert lifecycle.task_id == task_id, (
                "task_lifecycle should forward task_id to the lifecycle constructor"
            )
            assert lifecycle.limiter is limiter, (
                "task_lifecycle should forward self as the limiter reference"
            )
            assert lifecycle.on_failure_action == "kill", (
                "task_lifecycle should use the limiter default"
                " when no override is provided"
            )
        finally:
            await limiter.shutdown()


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestAsyncTaskLifecycleObservability:
    """Observability tests for the ``AsyncTaskLifecycle`` context
    manager log emissions.
    """

    @staticmethod
    async def test_lifecycle_entry_emits_debug_log(mock_limiter, task_id, caplog):
        """Verify that lifecycle entry emits a DEBUG log with limiter
        id, task id, and heartbeat interval.
        """
        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(mock_limiter, task_id):
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[AsyncTaskLifecycle]",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                f"heartbeat_interval_s={mock_limiter.lease_duration / 2:.1f}",
            ],
            message=(
                "should emit a debug log for lifecycle entry"
                " with limiter id, task id, and heartbeat interval"
            ),
        )

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
            logging.DEBUG, logger="redis_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(mock_limiter, task_id):
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[AsyncTaskLifecycle]",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                "removed_concurrency=True",
                "removed_inflight=True",
            ],
            message=(
                "should emit a debug log for concurrency slot release"
                " with limiter id, task id, and removal counts"
            ),
        )

    @staticmethod
    async def test_empty_task_id_emits_removed_inflight_false(
        async_redis_client, limiter_id, caplog
    ):
        """Verify that an empty ``task_id`` emits
        ``removed_inflight=False`` in the cleanup log.
        """
        # Arrange
        limiter = MagicMock()
        limiter.redis = async_redis_client
        limiter.concurrency_key = f"{limiter_id}:concurrency"
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        limiter.extend_lease = AsyncMock(return_value=None)
        limiter._eval_script = AsyncMock(
            side_effect=make_async_eval_script(async_redis_client)
        )
        limiter._drain_signal_channel = f"{limiter_id}:drain_signal"
        limiter._worker_id = f"{limiter_id}_worker"
        limiter._heartbeat_scheduler = AsyncHeartbeatScheduler(limiter)

        try:
            # Act
            with caplog.at_level(
                logging.DEBUG, logger="redis_rate_limiter.core.async_limiters"
            ):
                async with AsyncTaskLifecycle(limiter, task_id=""):
                    pass
        finally:
            await limiter._heartbeat_scheduler.shutdown()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[AsyncTaskLifecycle]",
            required_fragments=["removed_inflight=False"],
            message="empty task_id should log removed_inflight=False",
        )

    @staticmethod
    async def test_aexit_emits_lifecycle_exit_debug_log(mock_limiter, task_id, caplog):
        """Verify that ``__aexit__`` emits a DEBUG log with the task
        id in the finally block.
        """
        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(mock_limiter, task_id):
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[AsyncTaskLifecycle]",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                "follow-up consume",
            ],
            message=(
                "should emit a debug log for lifecycle exit with limiter id and task id"
            ),
        )


# ---------------------------------------------------------------------------
# Heartbeat scheduler tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestAsyncTaskLifecycleInterval:
    """Verify the lifecycle exposes the heartbeat interval to its callers."""

    @staticmethod
    async def test_heartbeat_interval_calculation(mock_limiter, task_id):
        """Verify that ``lifecycle.interval`` is ``lease_duration / 2``."""
        # Arrange & Act
        lifecycle = AsyncTaskLifecycle(mock_limiter, task_id)

        # Assert
        expected_interval = mock_limiter.lease_duration / 2
        assert lifecycle.interval == expected_interval, (
            f"interval must be lease_duration / 2 = {expected_interval} seconds"
        )


@pytest.mark.behavior
class TestAsyncHeartbeatScheduler:
    """Tests for the shared ``AsyncHeartbeatScheduler``."""

    @staticmethod
    async def test_register_returns_entry_with_initial_state(
        scheduler_limiter, task_id
    ):
        """Verify that register returns an entry with the expected fields."""
        # Act
        entry = await scheduler_limiter._heartbeat_scheduler.register(task_id, "warn")

        try:
            # Assert
            assert entry.task_id == task_id, "entry must carry the task id"
            assert entry.on_failure_action == "warn", (
                "entry must carry the on_failure_action"
            )
            assert entry.is_healthy is True, "entry must start in a healthy state"
        finally:
            await scheduler_limiter._heartbeat_scheduler.deregister(task_id)

    @staticmethod
    async def test_register_then_deregister_removes_entry(scheduler_limiter, task_id):
        """Verify that ``get_entry`` returns ``None`` after deregistration."""
        # Arrange
        await scheduler_limiter._heartbeat_scheduler.register(task_id, "warn")

        # Act
        await scheduler_limiter._heartbeat_scheduler.deregister(task_id)

        # Assert
        assert (
            await scheduler_limiter._heartbeat_scheduler.get_entry(task_id) is None
        ), "deregistered task must not be retrievable via get_entry"

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_multiple_tasks_renewed_independently(scheduler_limiter):
        """Verify that several tasks registered concurrently all see renewals."""
        # Arrange
        scheduler = scheduler_limiter._heartbeat_scheduler
        task_ids = ["task_a", "task_b", "task_c"]
        for tid in task_ids:
            await scheduler.register(tid, "warn")

        try:
            # Act
            with shutdown_timer(scheduler, timeout=0.3):
                await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

            # Assert
            renewed_ids = {
                call[0][0] for call in scheduler_limiter.extend_lease.call_args_list
            }
            for tid in task_ids:
                assert tid in renewed_ids, (
                    f"extend_lease must be called for registered task {tid}"
                )
            assert scheduler_limiter.extend_lease.call_count <= 30, (
                f"extend_lease called {scheduler_limiter.extend_lease.call_count} "
                "times; renewal rate exceeds expected interval"
            )
        finally:
            for tid in task_ids:
                await scheduler.deregister(tid)

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_heartbeat_recovery_restores_entry_health(
        async_redis_client, scheduler_limiter, task_id
    ):
        """Verify that an unhealthy entry recovers when ``extend_lease`` succeeds."""
        # Act
        scheduler = scheduler_limiter._heartbeat_scheduler
        async with AsyncTaskLifecycle(scheduler_limiter, task_id) as lifecycle:
            lifecycle.is_healthy = False
            with shutdown_timer(scheduler, timeout=0.3):
                await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

            # Assert
            assert lifecycle.is_healthy, "entry must restore health after recovery"

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_heartbeat_failure_warn_mode_marks_entry_unhealthy(
        async_redis_client, scheduler_limiter, task_id
    ):
        """Verify that warn mode flags the entry as unhealthy on failure."""
        # Arrange
        scheduler_limiter.extend_lease = AsyncMock(
            side_effect=redis.RedisError("Simulated Redis failure")
        )
        scheduler = scheduler_limiter._heartbeat_scheduler

        # Act
        with patch("os.kill") as mock_kill:
            async with AsyncTaskLifecycle(
                scheduler_limiter, task_id, on_heartbeat_failure="warn"
            ) as lifecycle:
                with shutdown_timer(scheduler, timeout=0.3):
                    await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

                # Assert
                assert not lifecycle.is_healthy, (
                    "entry must be marked unhealthy after heartbeat failure"
                )
                assert mock_kill.call_count == 0, (
                    "os.kill must not be called in warn mode"
                )

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_heartbeat_failure_kill_mode_terminates_worker(
        async_redis_client, scheduler_limiter, task_id
    ):
        """Verify that kill mode terminates the worker on failure."""
        # Arrange
        scheduler_limiter.extend_lease = AsyncMock(
            side_effect=redis.RedisError("Simulated Redis failure")
        )
        scheduler = scheduler_limiter._heartbeat_scheduler

        # Act & Assert
        with patch("os.kill") as mock_kill:
            async with AsyncTaskLifecycle(
                scheduler_limiter, task_id, on_heartbeat_failure="kill"
            ):
                with shutdown_timer(scheduler, timeout=0.3):
                    await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

                assert mock_kill.call_count > 0, (
                    "os.kill must be called in kill mode on heartbeat failure"
                )
                mock_kill.assert_called_with(os.getpid(), signal.SIGTERM)

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_extend_lease_called_with_correct_parameters(
        async_redis_client, scheduler_limiter, task_id
    ):
        """Verify that ``extend_lease`` is called with the right task id and duration."""
        # Act
        scheduler = scheduler_limiter._heartbeat_scheduler
        async with AsyncTaskLifecycle(scheduler_limiter, task_id):
            with shutdown_timer(scheduler, timeout=0.3):
                await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

        # Assert
        assert scheduler_limiter.extend_lease.call_count >= 1, (
            "extend_lease must be called at least once with correct parameters"
        )
        for call in scheduler_limiter.extend_lease.call_args_list:
            assert call[0][0] == task_id, "extend_lease must be called with task_id"
            assert call[0][1] == scheduler_limiter.lease_duration, (
                "extend_lease must be called with lease_duration"
            )


@pytest.mark.behavior
class TestAsyncHeartbeatSchedulerBoundary:
    """Boundary tests for ``AsyncHeartbeatScheduler`` task management."""

    @staticmethod
    async def test_lazy_task_start(async_redis_client, limiter_id):
        """Verify that the worker task is not started until first register."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        scheduler = AsyncHeartbeatScheduler(limiter)

        try:
            # Assert
            assert scheduler._task is None, (
                "worker task must not exist before any task is registered"
            )
        finally:
            await scheduler.shutdown()

    @staticmethod
    async def test_shutdown_is_idempotent(async_redis_client, limiter_id):
        """Verify that calling shutdown twice does not raise."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        limiter.extend_lease = AsyncMock(return_value=None)
        scheduler = AsyncHeartbeatScheduler(limiter)
        await scheduler.register("task-1", "warn")

        # Act
        await scheduler.shutdown()
        await scheduler.shutdown()  # second call must be safe

        # Assert: no exception raised; task is no longer running.
        assert scheduler._task is None or scheduler._task.done(), (
            "worker task must be stopped after shutdown"
        )

    @staticmethod
    async def test_task_restart_after_shutdown_and_reregister(
        async_redis_client, limiter_id
    ):
        """Verify that registering after shutdown revives the worker task."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        limiter.extend_lease = AsyncMock(return_value=None)
        scheduler = AsyncHeartbeatScheduler(limiter)
        await scheduler.register("task-1", "warn")
        await scheduler.shutdown()

        # Act
        await scheduler.register("task-2", "warn")

        try:
            # Assert
            assert scheduler._task is not None, (
                "scheduler must spawn a new task after shutdown + register"
            )
            assert not scheduler._task.done(), (
                "scheduler task must be running after restart"
            )
        finally:
            await scheduler.shutdown()

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_stale_heap_entry_is_skipped(scheduler_limiter, task_id):
        """Verify that a deregistered task's leftover heap entry does
        not trigger a renewal.
        """
        # Arrange
        scheduler = scheduler_limiter._heartbeat_scheduler
        await scheduler.register(task_id, "warn")
        await scheduler.deregister(task_id)
        await scheduler.register("task_other", "warn")

        # Act
        with shutdown_timer(scheduler, timeout=0.3):
            await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

        # Assert
        renewed_ids = {
            call[0][0] for call in scheduler_limiter.extend_lease.call_args_list
        }
        assert task_id not in renewed_ids, (
            "deregistered task must not be renewed via a stale heap entry"
        )
        await scheduler.deregister("task_other")

    @staticmethod
    async def test_is_healthy_true_before_aenter(mock_limiter, task_id):
        """Verify that ``is_healthy`` returns ``True`` before
        ``__aenter__`` when no scheduler entry exists.
        """
        # Arrange & Act
        lifecycle = AsyncTaskLifecycle(mock_limiter, task_id)

        # Assert
        assert lifecycle.is_healthy is True, (
            "is_healthy must return True when no scheduler entry exists"
        )

    @staticmethod
    async def test_empty_heap_does_not_crash_scheduler(scheduler_limiter, task_id):
        """Verify that the scheduler task survives when the heap
        drains completely after all tasks are deregistered.
        """
        # Arrange
        scheduler = scheduler_limiter._heartbeat_scheduler
        await scheduler.register(task_id, "warn")

        # Act
        await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)
        await scheduler.deregister(task_id)
        await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

        # Assert
        assert scheduler._task is not None, (
            "scheduler task must still exist after heap drains"
        )
        assert not scheduler._task.done(), (
            "scheduler task must remain alive after heap drains"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    async def test_shutdown_cancels_stuck_scheduler_task(
        async_redis_client, limiter_id
    ):
        """Verify that ``shutdown`` cancels the worker task when it
        does not finish within the timeout.
        """
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        limiter.extend_lease = AsyncMock(return_value=None)
        scheduler = AsyncHeartbeatScheduler(limiter)
        scheduler._shutdown_timeout = 0.1
        await scheduler.register("task-1", "warn")

        # Replace the running task with one that ignores shutdown.
        if scheduler._task:
            scheduler._task.cancel()
            try:
                await scheduler._task
            except asyncio.CancelledError:
                pass

        hung = asyncio.Event()
        scheduler._task = asyncio.create_task(hung.wait())

        # Act
        try:
            await asyncio.wait_for(scheduler.shutdown(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

        # Assert
        assert scheduler._task.done(), (
            "stuck task must be cancelled after shutdown timeout"
        )


# ---------------------------------------------------------------------------
# Heartbeat scheduler observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestAsyncHeartbeatSchedulerObservability:
    """Observability tests for log emissions from the shared async scheduler."""

    @staticmethod
    async def test_heartbeat_recovery_emits_info_log(
        scheduler_limiter, task_id, caplog
    ):
        """Verify that heartbeat recovery emits an INFO log with task
        id and limiter id.
        """
        # Act
        with caplog.at_level(
            logging.INFO, logger="redis_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(scheduler_limiter, task_id) as lifecycle:
                lifecycle.is_healthy = False
                await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            label="[AsyncTaskLifecycle]",
            required_fragments=[
                f"task {task_id}",
                f"limiter {scheduler_limiter.id}",
                "restored",
            ],
            message=(
                "should emit an info log for heartbeat connection"
                " restoration with task id and limiter id"
            ),
        )

    @staticmethod
    async def test_heartbeat_failure_warn_mode_emits_critical_log(
        scheduler_limiter, task_id, caplog
    ):
        """Verify that heartbeat failure in warn mode emits a CRITICAL log."""
        # Arrange
        scheduler_limiter.extend_lease = AsyncMock(
            side_effect=redis.RedisError("Simulated Redis failure")
        )

        # Act
        with caplog.at_level(
            logging.CRITICAL, logger="redis_rate_limiter.core.async_limiters"
        ):
            async with AsyncTaskLifecycle(
                scheduler_limiter, task_id, on_heartbeat_failure="warn"
            ):
                await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="CRITICAL",
            label="[AsyncTaskLifecycle]",
            required_fragments=[
                f"task {task_id}",
                "flagged as unhealthy",
                "Simulated Redis failure",
            ],
            message=(
                "should emit a critical log for heartbeat failure"
                " with task id and error message"
            ),
        )

    @staticmethod
    async def test_heartbeat_failure_kill_mode_emits_critical_log(
        scheduler_limiter, task_id, caplog
    ):
        """Verify that heartbeat failure in kill mode emits a CRITICAL
        log with termination action.
        """
        # Arrange
        scheduler_limiter.extend_lease = AsyncMock(
            side_effect=redis.RedisError("Simulated Redis failure")
        )

        # Act
        with caplog.at_level(
            logging.CRITICAL, logger="redis_rate_limiter.core.async_limiters"
        ):
            with patch("os.kill"):
                async with AsyncTaskLifecycle(
                    scheduler_limiter, task_id, on_heartbeat_failure="kill"
                ):
                    await asyncio.sleep(0.75 * scheduler_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="CRITICAL",
            label="[AsyncTaskLifecycle]",
            required_fragments=[
                f"task {task_id}",
                "terminating worker",
                "Simulated Redis failure",
            ],
            message=(
                "should emit a critical log for heartbeat kill mode"
                " with task id and error message"
            ),
        )


# ---------------------------------------------------------------------------
# Async extend lease tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestAsyncExtendLease:
    """Tests for async ``extend_lease()`` success and error handling."""

    @staticmethod
    async def test_extend_lease_raises_key_error_for_unknown_task(
        async_stub_limiter,
    ):
        """Verify that ``extend_lease()`` raises a KeyError for
        unknown task identifiers.
        """
        # Act & Assert
        with pytest.raises(KeyError, match=r'in the concurrency set\."'):
            await async_stub_limiter.extend_lease("nonexistent", 30)

    @staticmethod
    async def test_extend_lease_succeeds_for_existing_task(
        async_stub_limiter, async_redis_client, task_id
    ):
        """Verify that ``extend_lease()`` updates the score for a task
        present in the concurrency set.
        """
        # Arrange
        # Seed the concurrency sorted set with a low score so the update is observable.
        initial_score = 1000.0
        await async_redis_client.zadd(
            async_stub_limiter.concurrency_key, {task_id: initial_score}
        )

        # Act
        await async_stub_limiter.extend_lease(task_id, 30)

        # Assert
        new_score = await async_redis_client.zscore(
            async_stub_limiter.concurrency_key, task_id
        )
        assert new_score is not None, (
            "task should still be present in the concurrency set after lease extension"
        )
        assert new_score > initial_score, (
            "lease extension should update the score to a value"
            " greater than the initial score"
        )


# ---------------------------------------------------------------------------
# Async extend lease observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestAsyncExtendLeaseObservability:
    """Observability tests for async ``extend_lease()`` log emissions."""

    @staticmethod
    async def test_extend_lease_unknown_task_emits_debug_log(
        async_stub_limiter, caplog
    ):
        """Verify that ``extend_lease()`` emits a DEBUG log with
        ``renewed=False`` for unknown tasks.
        """
        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.core.async_limiters"
        ):
            with pytest.raises(KeyError, match="not found in the concurrency set"):
                await async_stub_limiter.extend_lease("nonexistent", 30)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[AsyncStubRateLimiter]",
            required_fragments=[
                f"limiter={async_stub_limiter.id}",
                "task_id=nonexistent",
                "duration_s=30",
                "renewed=False",
            ],
            message=(
                "should emit a debug log containing the limiter id,"
                " task id, duration, and renewed=False"
            ),
        )

    @staticmethod
    async def test_extend_lease_success_emits_debug_log(
        async_stub_limiter, async_redis_client, task_id, caplog
    ):
        """Verify that ``extend_lease()`` emits a DEBUG log with
        ``renewed=True`` for existing tasks.
        """
        # Arrange
        await async_redis_client.zadd(
            async_stub_limiter.concurrency_key, {task_id: 1000.0}
        )

        # Act
        with caplog.at_level(
            logging.DEBUG, logger="redis_rate_limiter.core.async_limiters"
        ):
            await async_stub_limiter.extend_lease(task_id, 30)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[AsyncStubRateLimiter]",
            required_fragments=[
                f"limiter={async_stub_limiter.id}",
                f"task_id={task_id}",
                "duration_s=30",
                "renewed=True",
            ],
            message=(
                "should emit a debug log containing the limiter id,"
                " task id, duration, and renewed=True"
            ),
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestAsyncTaskLifecycleSignatures:
    """Signature tests for ``AsyncTaskLifecycle`` default parameter values."""

    @staticmethod
    def test_default_on_heartbeat_failure_is_warn():
        """Verify that the default ``on_heartbeat_failure`` parameter
        is lowercase ``'warn'``.

        Mutation target: ``on_heartbeat_failure`` default value in
        ``AsyncTaskLifecycle.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(AsyncTaskLifecycle.__init__)

        # Assert
        assert sig.parameters["on_heartbeat_failure"].default == "warn", (
            "default on_heartbeat_failure must be lowercase 'warn'"
        )
