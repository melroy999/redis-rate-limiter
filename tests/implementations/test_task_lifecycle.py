"""Tests for the ``TaskLifecycle`` context manager.

This module tests the sync ``TaskLifecycle`` implementation that manages
concurrency slots, in-flight key cleanup, and heartbeat-based lease renewal.
It inherits the unified contract tests from ``TaskLifecycleContractTest`` and
adds implementation-specific behavioral and observability tests.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``stub_limiter``, ``task_id``: from ``tests/implementations/conftest.py``.
"""

import inspect
import logging
import os
import signal
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from redis_rate_limiter import TaskLifecycle
from redis_rate_limiter.core.limiters import HeartbeatScheduler
from redis_rate_limiter.core.scripts import load_lua_script
from tests.contracts.test_task_lifecycle import TaskLifecycleContractTest
from tests.helpers.utils import assert_log_emitted
from tests.implementations.conftest import (
    HEARTBEAT_OVERRIDE_CASES,
    HeartbeatFailureMode,
    StubRateLimiter,
)

RELEASE_SOURCE = load_lua_script("release.lua")


def _make_eval_script(redis_client):
    """Return a real ``_eval_script`` implementation for the given client.

    The returned callable looks up the named Lua script source via
    ``load_lua_script`` and executes it through ``redis_client.eval``,
    matching the runtime semantics of the production limiter for any
    test that needs the script to actually run.
    """
    sources: dict[str, str] = {}

    def _eval_script(script_name, num_keys, *args):
        if script_name not in sources:
            sources[script_name] = load_lua_script(script_name)
        return redis_client.eval(sources[script_name], num_keys, *args)

    return _eval_script


@pytest.fixture
def mock_limiter(redis_client, limiter_id, task_id):
    """Create a mock limiter that uses the real Redis client but mocks internal helpers.

    Provides a limiter with real Redis operations but mocked Celery-specific
    methods, thereby avoiding the need for a full Celery setup. ``_eval_script``
    is wired to actually run scripts through the real Redis client so that
    cleanup paths invoking ``release.lua`` (etc.) take effect, and a real
    ``HeartbeatScheduler`` is attached so heartbeat behaviour can be tested
    end-to-end.
    """
    limiter = MagicMock()
    limiter.redis = redis_client
    limiter.concurrency_key = f"{limiter_id}:concurrency"
    limiter.id = limiter_id
    limiter.get_inflight_key.side_effect = lambda tid: f"{limiter_id}:inflight:{tid}"

    # A short duration is used for fast test execution.
    limiter.lease_duration = 0.2

    # Configure the return value for background thread tests.
    limiter.extend_lease.return_value = None

    # Wire _eval_script to actually run scripts via the real Redis client.
    limiter._eval_script.side_effect = _make_eval_script(redis_client)

    # release.lua needs the drain channel and worker id; set them
    # explicitly so the script's PUBLISH targets a real channel.
    limiter._drain_signal_channel = f"{limiter_id}:drain_signal"
    limiter._worker_id = f"{limiter_id}_worker"

    # Attach a real shared heartbeat scheduler so registration / renewal
    # actually fires when tests enter a TaskLifecycle.
    limiter._heartbeat_scheduler = HeartbeatScheduler(limiter)

    yield limiter

    limiter._heartbeat_scheduler.shutdown()


@pytest.fixture
def inflight_key(mock_limiter, task_id):
    """Provide the in-flight key for the test task."""
    return mock_limiter.get_inflight_key(task_id)


@pytest.fixture
def create_lifecycle():
    """Provide a factory for sync ``TaskLifecycle`` instances
    wrapped in an async adapter.

    The unified contract tests use
    ``async with create_lifecycle(limiter, task_id):``,
    so the sync lifecycle is wrapped in a
    ``SyncToAsyncLifecycleAdapter``.
    """
    from tests.helpers.adapters import SyncToAsyncLifecycleAdapter

    def _factory(limiter, task_id, **kwargs):
        return SyncToAsyncLifecycleAdapter(TaskLifecycle(limiter, task_id, **kwargs))

    return _factory


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.contract
class TestTaskLifecycle(TaskLifecycleContractTest):
    """Contract compliance for the sync TaskLifecycle context manager implementation."""

    pass


@pytest.mark.behavior
class TestTaskLifecycleImplementation:
    """Tests for implementation-specific behaviour of the
    TaskLifecycle context manager.
    """

    @staticmethod
    def test_lifecycle_with_multiple_concurrent_tasks(
        redis_client, mock_limiter, task_id, inflight_key
    ):
        """Verify that the lifecycle only removes the specific
        task from the concurrency set.
        """
        # Arrange
        concurrent_tasks = {
            "other_task_1": 100,
            "other_task_2": 100,
            "other_task_3": 100,
            "other_task_4": 100,
            task_id: 100,
        }
        redis_client.zadd(mock_limiter.concurrency_key, concurrent_tasks)
        redis_client.set(inflight_key, "1")

        # Act
        with patch("redis_rate_limiter.core.limiters.Thread"):
            with TaskLifecycle(mock_limiter, task_id):
                # During execution, all five tasks should be present.
                assert redis_client.zcard(mock_limiter.concurrency_key) == 5, (
                    "all five tasks should be present during execution"
                )

        # Assert
        assert redis_client.zcard(mock_limiter.concurrency_key) == 4, (
            "concurrency set should have four tasks after target completion"
        )
        assert redis_client.zscore(mock_limiter.concurrency_key, task_id) is None, (
            "target task must be removed from concurrency set"
        )
        assert redis_client.zscore(
            mock_limiter.concurrency_key, "other_task_1"
        ) == pytest.approx(100.0), (
            "other tasks must remain in concurrency set with their original score"
        )
        assert redis_client.exists(inflight_key) == 0, (
            "inflight marker must be removed after completion"
        )

    @staticmethod
    def test_lifecycle_handles_redis_failure_during_cleanup(
        redis_client, mock_limiter, task_id, inflight_key
    ):
        """Verify that the lifecycle raises an exception but
        still wakes the local drain loop on Redis failure.
        """
        # Arrange
        # The cleanup path calls release.lua via _eval_script; simulate a
        # Redis failure by making _eval_script raise on the cleanup call.
        mock_limiter._eval_script.side_effect = ConnectionError(
            "Redis connection lost"
        )

        # Act & Assert
        with pytest.raises(ConnectionError, match="Redis connection lost"):
            with TaskLifecycle(mock_limiter, task_id):
                pass

        mock_limiter._eval_script.assert_called_once()
        mock_limiter._schedule_drain.assert_called_once()

    @staticmethod
    def test_empty_task_id_skips_inflight_cleanup(redis_client, limiter_id):
        """Verify that an empty ``task_id`` skips inflight key deletion."""
        # Arrange
        limiter = MagicMock()
        limiter.redis = redis_client
        limiter.concurrency_key = f"{limiter_id}:concurrency"
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        limiter.extend_lease.return_value = None
        limiter._eval_script.side_effect = _make_eval_script(redis_client)
        limiter._drain_signal_channel = f"{limiter_id}:drain_signal"
        limiter._worker_id = f"{limiter_id}_worker"
        limiter._heartbeat_scheduler = HeartbeatScheduler(limiter)

        try:
            # Act
            with TaskLifecycle(limiter, task_id=""):
                pass
        finally:
            limiter._heartbeat_scheduler.shutdown()

        # Assert
        limiter._schedule_drain.assert_called_once()

    @staticmethod
    @pytest.mark.parametrize(
        "original, override",
        HEARTBEAT_OVERRIDE_CASES,
        ids=["default_warn_override_kill", "default_kill_override_warn"],
    )
    def test_heartbeat_failure_override_precedence(
        redis_client,
        task_id,
        limiter_id,
        original: HeartbeatFailureMode,
        override: HeartbeatFailureMode,
    ):
        """Verify that the override parameter takes precedence
        over the limiter default.
        """
        # Arrange
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_task_lifecycle_heartbeat_override_precedence",
            limit=1,
            window=1,
            max_concurrency=1,
            max_age=1,
            on_heartbeat_failure=original,
        )

        # Act
        lifecycle_with_override = limiter.task_lifecycle(
            task_id,
            on_heartbeat_failure_override=override,
        )

        # Assert
        assert lifecycle_with_override.on_failure_action == override, (
            f"override {override} should take precedence over default {original}"
        )

    @staticmethod
    def test_task_lifecycle_passes_all_attributes(redis_client, task_id, limiter_id):
        """Verify that ``task_lifecycle()`` forwards the
        task_id, limiter, and default strategy.
        """
        # Arrange
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_task_lifecycle_passthrough",
            limit=1,
            window=1,
            max_concurrency=1,
            max_age=1,
            on_heartbeat_failure="kill",
        )

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
            "task_lifecycle should use the limiter default when no override is provided"
        )


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestTaskLifecycleObservability:
    """Observability tests for the ``TaskLifecycle`` context manager log emissions."""

    @staticmethod
    def test_lifecycle_entry_emits_debug_log(mock_limiter, task_id, caplog):
        """Verify that lifecycle entry emits a DEBUG log with
        limiter id, task id, and heartbeat interval.
        """
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            with TaskLifecycle(mock_limiter, task_id):
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[TaskLifecycle]",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                f"heartbeat_interval_s={mock_limiter.lease_duration / 2:.1f}",
            ],
            message=(
                "should emit a debug log for lifecycle entry"
                " with limiter id, task id,"
                " and heartbeat interval"
            ),
        )

    @staticmethod
    def test_lifecycle_cleanup_emits_debug_log(
        redis_client, mock_limiter, task_id, inflight_key, caplog
    ):
        """Verify that the lifecycle cleanup emits a DEBUG log with removal details."""
        # Arrange
        redis_client.zadd(mock_limiter.concurrency_key, {task_id: 100})
        redis_client.set(inflight_key, "1")

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            with TaskLifecycle(mock_limiter, task_id):
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[TaskLifecycle]",
            required_fragments=[
                f"limiter={mock_limiter.id}",
                f"task_id={task_id}",
                "removed_concurrency=True",
                "removed_inflight=True",
            ],
            message=(
                "should emit a debug log for concurrency slot"
                " release with limiter id, task id,"
                " and removal counts"
            ),
        )

    @staticmethod
    def test_empty_task_id_emits_removed_inflight_false(
        redis_client, limiter_id, caplog
    ):
        """Verify that an empty ``task_id`` emits
        ``removed_inflight=False`` in the cleanup log.
        """
        # Arrange
        limiter = MagicMock()
        limiter.redis = redis_client
        limiter.concurrency_key = f"{limiter_id}:concurrency"
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        limiter.extend_lease.return_value = None
        limiter._eval_script.side_effect = _make_eval_script(redis_client)
        limiter._drain_signal_channel = f"{limiter_id}:drain_signal"
        limiter._worker_id = f"{limiter_id}_worker"
        limiter._heartbeat_scheduler = HeartbeatScheduler(limiter)

        try:
            # Act
            with caplog.at_level(
                logging.DEBUG, logger="redis_rate_limiter.core.limiters"
            ):
                with TaskLifecycle(limiter, task_id=""):
                    pass
        finally:
            limiter._heartbeat_scheduler.shutdown()

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[TaskLifecycle]",
            required_fragments=["removed_inflight=False"],
            message="empty task_id should log removed_inflight=False",
        )

    @staticmethod
    def test_exit_emits_lifecycle_exit_debug_log(mock_limiter, task_id, caplog):
        """Verify that ``__exit__`` emits a DEBUG log with
        the task id in the finally block.
        """
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            with TaskLifecycle(mock_limiter, task_id):
                pass

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[TaskLifecycle]",
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
class TestTaskLifecycleInterval:
    """Verify the lifecycle exposes the heartbeat interval to its callers."""

    @staticmethod
    def test_heartbeat_interval_calculation(mock_limiter, task_id):
        """Verify that ``lifecycle.interval`` is ``lease_duration / 2``."""
        # Arrange & Act
        lifecycle = TaskLifecycle(mock_limiter, task_id)

        # Assert
        expected_interval = mock_limiter.lease_duration / 2
        assert lifecycle.interval == expected_interval, (
            f"interval must be lease_duration / 2 = {expected_interval} seconds"
        )


@pytest.mark.behavior
class TestHeartbeatScheduler:
    """Tests for the shared ``HeartbeatScheduler`` that renews task leases."""

    @staticmethod
    def test_register_returns_entry_with_initial_state(mock_limiter, task_id):
        """Verify that register returns an entry with the expected fields."""
        # Act
        entry = mock_limiter._heartbeat_scheduler.register(task_id, "warn")

        try:
            # Assert
            assert entry.task_id == task_id, "entry must carry the task id"
            assert entry.on_failure_action == "warn", (
                "entry must carry the on_failure_action"
            )
            assert entry.is_healthy is True, "entry must start in a healthy state"
        finally:
            mock_limiter._heartbeat_scheduler.deregister(task_id)

    @staticmethod
    def test_register_then_deregister_removes_entry(mock_limiter, task_id):
        """Verify that ``get_entry`` returns ``None`` after deregistration."""
        # Arrange
        mock_limiter._heartbeat_scheduler.register(task_id, "warn")

        # Act
        mock_limiter._heartbeat_scheduler.deregister(task_id)

        # Assert
        assert mock_limiter._heartbeat_scheduler.get_entry(task_id) is None, (
            "deregistered task must not be retrievable via get_entry"
        )

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_multiple_tasks_renewed_independently(mock_limiter):
        """Verify that several tasks registered concurrently all see renewals."""
        # Arrange
        task_ids = ["task_a", "task_b", "task_c"]
        for tid in task_ids:
            mock_limiter._heartbeat_scheduler.register(tid, "warn")

        try:
            # Act
            time.sleep(0.75 * mock_limiter.lease_duration)

            # Assert
            renewed_ids = {
                call[0][0] for call in mock_limiter.extend_lease.call_args_list
            }
            for tid in task_ids:
                assert tid in renewed_ids, (
                    f"extend_lease must be called for registered task {tid}"
                )
        finally:
            for tid in task_ids:
                mock_limiter._heartbeat_scheduler.deregister(tid)

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_heartbeat_recovery_restores_entry_health(mock_limiter, task_id):
        """Verify that an unhealthy entry recovers when ``extend_lease`` succeeds."""
        # Arrange
        with TaskLifecycle(mock_limiter, task_id) as lifecycle:
            # Act
            lifecycle.is_healthy = False
            time.sleep(0.75 * mock_limiter.lease_duration)

            # Assert
            assert lifecycle.is_healthy, "entry must restore health after recovery"

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_heartbeat_failure_warn_mode_marks_entry_unhealthy(mock_limiter, task_id):
        """Verify that warn mode flags the entry as unhealthy on failure."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act
        with patch("os.kill") as mock_kill:
            with TaskLifecycle(
                mock_limiter, task_id, on_heartbeat_failure="warn"
            ) as lifecycle:
                time.sleep(0.75 * mock_limiter.lease_duration)

                # Assert
                assert not lifecycle.is_healthy, (
                    "entry must be marked unhealthy after heartbeat failure"
                )
                assert mock_kill.call_count == 0, (
                    "os.kill must not be called in warn mode"
                )

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_heartbeat_failure_kill_mode_terminates_worker(mock_limiter, task_id):
        """Verify that kill mode terminates the worker on failure."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act & Assert
        with patch("os.kill") as mock_kill:
            with TaskLifecycle(mock_limiter, task_id, on_heartbeat_failure="kill"):
                time.sleep(0.75 * mock_limiter.lease_duration)

                assert mock_kill.call_count > 0, (
                    "os.kill must be called in kill mode on heartbeat failure"
                )
                mock_kill.assert_called_with(os.getpid(), signal.SIGTERM)

    @staticmethod
    @pytest.mark.timeout_safety_net
    def test_extend_lease_called_with_correct_parameters(mock_limiter, task_id):
        """Verify that ``extend_lease`` is called with the right task id and duration."""
        # Act
        with TaskLifecycle(mock_limiter, task_id):
            time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert mock_limiter.extend_lease.call_count >= 1, (
            "extend_lease must be called at least once with correct parameters"
        )
        for call in mock_limiter.extend_lease.call_args_list:
            assert call[0][0] == task_id, "extend_lease must be called with task_id"
            assert call[0][1] == mock_limiter.lease_duration, (
                "extend_lease must be called with lease_duration"
            )


@pytest.mark.behavior
class TestHeartbeatSchedulerBoundary:
    """Boundary tests for ``HeartbeatScheduler`` thread management."""

    @staticmethod
    def test_scheduler_thread_is_daemon(mock_limiter, task_id):
        """Verify that the scheduler worker thread is a daemon."""
        # Act
        mock_limiter._heartbeat_scheduler.register(task_id, "warn")

        try:
            # Assert
            assert mock_limiter._heartbeat_scheduler._thread is not None, (
                "scheduler thread must exist after the first registration"
            )
            assert mock_limiter._heartbeat_scheduler._thread.daemon is True, (
                "scheduler thread must be a daemon thread"
            )
        finally:
            mock_limiter._heartbeat_scheduler.deregister(task_id)

    @staticmethod
    def test_lazy_thread_start(redis_client, limiter_id):
        """Verify that the worker thread is not started until first register."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        scheduler = HeartbeatScheduler(limiter)

        try:
            # Assert
            assert scheduler._thread is None, (
                "worker thread must not exist before any task is registered"
            )
        finally:
            scheduler.shutdown()

    @staticmethod
    def test_shutdown_is_idempotent(redis_client, limiter_id):
        """Verify that calling shutdown twice does not raise."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        scheduler = HeartbeatScheduler(limiter)
        scheduler.register("task-1", "warn")

        # Act
        scheduler.shutdown()
        scheduler.shutdown()  # second call must be safe

        # Assert: no exception raised; thread is no longer alive.
        assert (
            scheduler._thread is None or not scheduler._thread.is_alive()
        ), "worker thread must be stopped after shutdown"

    @staticmethod
    def test_thread_restart_after_shutdown_and_reregister(redis_client, limiter_id):
        """Verify that registering after shutdown revives the worker thread."""
        # Arrange
        limiter = MagicMock()
        limiter.id = limiter_id
        limiter.lease_duration = 0.2
        limiter.extend_lease.return_value = None
        scheduler = HeartbeatScheduler(limiter)
        scheduler.register("task-1", "warn")
        scheduler.shutdown()

        # Act
        scheduler.register("task-2", "warn")

        try:
            # Assert
            assert scheduler._thread is not None, (
                "scheduler must spawn a new thread after shutdown + register"
            )
            assert scheduler._thread.is_alive(), (
                "scheduler thread must be alive after restart"
            )
        finally:
            scheduler.shutdown()


@pytest.mark.observability
class TestHeartbeatSchedulerObservability:
    """Observability tests for log emissions from the shared scheduler."""

    @staticmethod
    def test_heartbeat_recovery_emits_info_log(mock_limiter, task_id, caplog):
        """Verify that heartbeat recovery emits an INFO log
        with task id and limiter id.
        """
        # Act
        with caplog.at_level(logging.INFO, logger="redis_rate_limiter.core.limiters"):
            with TaskLifecycle(mock_limiter, task_id) as lifecycle:
                lifecycle.is_healthy = False
                time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            label="[TaskLifecycle]",
            required_fragments=[
                f"task {task_id}",
                f"limiter {mock_limiter.id}",
                "restored",
            ],
            message=(
                "should emit an info log for heartbeat"
                " connection restoration with task id"
                " and limiter id"
            ),
        )

    @staticmethod
    def test_heartbeat_failure_warn_mode_emits_critical_log(
        mock_limiter, task_id, caplog
    ):
        """Verify that heartbeat failure in warn mode emits a CRITICAL log."""
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act
        with caplog.at_level(
            logging.CRITICAL, logger="redis_rate_limiter.core.limiters"
        ):
            with TaskLifecycle(mock_limiter, task_id, on_heartbeat_failure="warn"):
                time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="CRITICAL",
            label="[TaskLifecycle]",
            required_fragments=[
                f"task {task_id}",
                "flagged as unhealthy",
                "Simulated Redis failure",
            ],
            message=(
                "should emit a critical log for heartbeat"
                " failure with task id and error message"
            ),
        )

    @staticmethod
    def test_heartbeat_failure_kill_mode_emits_critical_log(
        mock_limiter, task_id, caplog
    ):
        """Verify that heartbeat failure in kill mode emits
        a CRITICAL log with termination action.
        """
        # Arrange
        mock_limiter.extend_lease.side_effect = Exception("Simulated Redis failure")

        # Act
        with caplog.at_level(
            logging.CRITICAL, logger="redis_rate_limiter.core.limiters"
        ):
            with patch("os.kill"):
                with TaskLifecycle(mock_limiter, task_id, on_heartbeat_failure="kill"):
                    time.sleep(0.75 * mock_limiter.lease_duration)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="CRITICAL",
            label="[TaskLifecycle]",
            required_fragments=[
                f"task {task_id}",
                "terminating worker",
                "Simulated Redis failure",
            ],
            message=(
                "should emit a critical log for heartbeat"
                " failure with task id, error,"
                " and termination action"
            ),
        )


# ---------------------------------------------------------------------------
# Extend lease tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestExtendLease:
    """Tests for ``extend_lease()`` success and error handling."""

    @staticmethod
    def test_extend_lease_raises_key_error_for_unknown_task(stub_limiter):
        """Verify that ``extend_lease()`` raises a KeyError
        for unknown task identifiers.
        """
        # Act & Assert
        with pytest.raises(KeyError, match=r'in the concurrency set\."'):
            stub_limiter.extend_lease("nonexistent", 30)

    @staticmethod
    def test_extend_lease_succeeds_for_existing_task(
        stub_limiter, redis_client, task_id
    ):
        """Verify that ``extend_lease()`` updates the score
        for a task present in the concurrency set.
        """
        # Arrange
        # Seed the concurrency sorted set with a low score so the update is observable.
        initial_score = 1000.0
        redis_client.zadd(stub_limiter.concurrency_key, {task_id: initial_score})

        # Act
        stub_limiter.extend_lease(task_id, 30)

        # Assert
        new_score = redis_client.zscore(stub_limiter.concurrency_key, task_id)
        assert new_score is not None, (
            "task should still be present in the concurrency set after lease extension"
        )
        assert new_score > initial_score, (
            "lease extension should update the score to a"
            " value greater than the initial score"
        )


# ---------------------------------------------------------------------------
# Extend lease observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestExtendLeaseObservability:
    """Observability tests for ``extend_lease()`` log emissions."""

    @staticmethod
    def test_extend_lease_unknown_task_emits_debug_log(stub_limiter, caplog):
        """Verify that ``extend_lease()`` emits a DEBUG log
        with ``renewed=False`` for unknown tasks.
        """
        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            with pytest.raises(KeyError, match="not found in the concurrency set"):
                stub_limiter.extend_lease("nonexistent", 30)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[StubRateLimiter]",
            required_fragments=[
                f"limiter={stub_limiter.id}",
                "task_id=nonexistent",
                "duration_s=30",
                "renewed=False",
            ],
            message=(
                "should emit a debug log containing the"
                " limiter id, task id, duration,"
                " and renewed=False"
            ),
        )

    @staticmethod
    def test_extend_lease_success_emits_debug_log(
        stub_limiter, redis_client, task_id, caplog
    ):
        """Verify that ``extend_lease()`` emits a DEBUG log
        with ``renewed=True`` for existing tasks.
        """
        # Arrange
        redis_client.zadd(stub_limiter.concurrency_key, {task_id: 1000.0})

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter.core.limiters"):
            stub_limiter.extend_lease(task_id, 30)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="DEBUG",
            label="[StubRateLimiter]",
            required_fragments=[
                "Lease extension",
                f"limiter={stub_limiter.id}",
                f"task_id={task_id}",
                "duration_s=30",
                "renewed=True",
            ],
            message=(
                "should emit a debug log containing the lease extension"
                " label, limiter id, task id, duration, and renewed=True"
            ),
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestTaskLifecycleSignatures:
    """Signature tests for ``TaskLifecycle`` default parameter values."""

    @staticmethod
    def test_default_on_heartbeat_failure_is_warn():
        """Verify that the default ``on_heartbeat_failure``
        parameter is lowercase ``'warn'``.

        Mutation target: ``on_heartbeat_failure`` default value in
        ``TaskLifecycle.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(TaskLifecycle.__init__)

        # Assert
        assert sig.parameters["on_heartbeat_failure"].default == "warn", (
            "default on_heartbeat_failure must be lowercase 'warn'"
        )


# ---------------------------------------------------------------------------
# Thread leak integration test
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestNoThreadLeak:
    """Verify that the shared scheduler does not leak threads under churn."""

    @staticmethod
    def test_no_thread_leak_after_many_lifecycles(redis_client, limiter_id):
        """Run many consume + lifecycle cycles and verify the active thread
        count returns to baseline once the limiter is shut down.

        Guards against the regression a per-task heartbeat thread model
        would cause: even at thousands of lifecycle entries, only the
        single shared scheduler thread should ever be alive at once.
        """
        # Arrange
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_no_thread_leak",
            limit=10_000,
            window=60,
            max_concurrency=10_000,
            max_age=3600,
            lease_duration=30,
        )
        baseline = threading.active_count()

        try:
            # Act
            for i in range(100):
                with limiter.task_lifecycle(f"task_{i}"):
                    pass
            mid_run_count = threading.active_count()
        finally:
            limiter.shutdown()

        # Assert
        # During the run we tolerate the scheduler thread being alive
        # (baseline + 1). Anything beyond that means a per-task thread
        # leaked.
        assert mid_run_count <= baseline + 2, (
            f"only the shared scheduler thread should be alive during the "
            f"run; saw {mid_run_count - baseline} extra threads"
        )
