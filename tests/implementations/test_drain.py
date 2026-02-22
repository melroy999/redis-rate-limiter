"""Tests for the ``drain()`` and ``trigger_consume()`` branch behavior.

Tests are written once in async form using a mixin pattern. Each test
receives variant-specific fixtures (``limiter``, ``mock_target``) and uses
class-level customization points (``lock_result``, ``_mock_cls``) to
accommodate the structural differences between sync and async drain
implementations.

The sync variant participates via the ``SyncToAsyncLimiterAdapter``;
the async variant runs natively.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager, contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.asyncio

from celery_rate_limiter.core.async_limiters import (
    AbstractAsyncDistributedRateLimiter,
)
from tests.helpers.adapters import SyncToAsyncLimiterAdapter
from tests.implementations.conftest import (
    AsyncTrackingRateLimiter,
    MinimalAsyncRateLimiter,
    TrackingRateLimiter,
)

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class DrainBehaviorTests:
    """Abstract test suite for branch coverage in ``drain()``.

    Subclasses must provide:
        - ``limiter``: a fixture returning the limiter to call (adapter or native).
        - ``mock_target``: a fixture returning the object to patch (inner for sync, direct for async).
        - ``lock_result(acquired)``: a class method returning a sync or async context manager.
        - ``_mock_cls``: a class attribute (``MagicMock`` or ``AsyncMock``) for mocking awaitable methods
          that are set as attributes rather than patched.
    """

    _mock_cls = None

    @staticmethod
    def lock_result(acquired):
        """Override in subclass to return a sync or async context manager."""
        raise NotImplementedError

    async def test_drain_defers_when_paused(self, limiter, mock_target, caplog):
        """Verify that ``drain()`` defers execution and schedules a follow-up when the limiter is paused."""
        # Arrange
        mock_target._paused_until = time.time() + 0.2
        consume_mock = MagicMock()

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
            with patch.object(mock_target, "consume", consume_mock):
                await limiter.drain()

        # Assert
        assert consume_mock.call_count == 0, "drain should not consume while paused"
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule one follow-up while paused"
        )
        assert limiter.scheduled_drains[0] > 0.0, (
            "paused follow-up delay should be positive"
        )
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={limiter.id}" in record.message
            and "paused" in record.message
            for record in caplog.records
        ), "should emit a debug log indicating the drain is deferred due to pause"

    async def test_drain_schedules_backup_when_lock_contended(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` schedules a backup drain when the dispatch lock is not acquired."""
        # Arrange
        consume_mock = MagicMock()

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(False),
                ),
                patch.object(mock_target, "consume", consume_mock),
            ):
                await limiter.drain()

        # Assert
        assert consume_mock.call_count == 0, (
            "drain should not consume when lock is contended"
        )
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when lock is contended"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a backup drain when lock is contended"
        )
        expected_delay = limiter.window / limiter.limit
        assert limiter.scheduled_drains[0] == expected_delay, (
            "backup drain delay should be one token interval"
        )
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={limiter.id}" in record.message
            and "delay_s=" in record.message
            for record in caplog.records
        ), "should emit a debug log for the backup drain with limiter id and delay"

    async def test_drain_dispatches_task_and_schedules_follow_up(
        self, limiter, mock_target, caplog
    ):
        """Verify that a successful consume dispatches the task and schedules the next drain."""
        # Arrange
        consume_result = {
            "success": True,
            "expired": False,
            "task": {
                "id": "task-1",
                "func_path": "myapp.tasks.work",
                "payload": {"x": 1},
            },
            "remaining_tokens": 4,
            "active_concurrency": 1,
            "reset_in_ms": 100,
            "remaining_tasks": 2,
        }

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert len(limiter.dispatched_tasks) == 1, (
            "drain should dispatch exactly one task on successful consume"
        )
        assert limiter.dispatched_tasks[0]["task_id"] == "task-1", (
            "dispatched task id should match consumed task id"
        )
        assert limiter.dispatched_tasks[0]["func_path"] == "myapp.tasks.work", (
            "dispatched func_path should match the scheduled task"
        )
        assert limiter.dispatched_tasks[0]["payload"] == {"x": 1}, (
            "dispatched payload should match the scheduled task"
        )
        assert limiter.scheduled_drains == [0.0], (
            "drain should schedule an immediate follow-up when tasks remain"
        )
        assert any(
            record.levelname == "INFO"
            and f"limiter={limiter.id}" in record.message
            and "task_id=task-1" in record.message
            and "func_path=myapp.tasks.work" in record.message
            for record in caplog.records
        ), (
            "should emit an info log for the dispatched task with limiter id, task id, and func path"
        )
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={limiter.id}" in record.message
            and "follow-up" in record.message
            for record in caplog.records
        ), "should emit a debug log for follow-up drain scheduling"

    async def test_drain_handles_consume_exception(self, limiter, mock_target, caplog):
        """Verify that consume exceptions are caught and a recovery drain is scheduled."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(
                    mock_target,
                    "consume",
                    side_effect=RuntimeError("consume failed"),
                ),
            ):
                await limiter.drain()

        # Assert
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch if consume fails"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a recovery drain after consume failure"
        )
        assert limiter.scheduled_drains[0] == pytest.approx(0.1), (
            "recovery drain delay should be min(60, 0.1 * 2^(1-1)) = 0.1"
        )
        assert limiter._consecutive_drain_failures == 1, (
            "failure counter should be incremented to 1"
        )
        assert any(
            record.levelname == "ERROR"
            and f"limiter={limiter.id}" in record.message
            and "attempt #1" in record.message
            for record in caplog.records
        ), (
            "should emit an error log containing the limiter id and failure attempt number"
        )

    async def test_drain_handles_dispatch_exception(self, limiter, mock_target):
        """Verify that dispatch exceptions are caught and a recovery drain is scheduled."""
        # Arrange
        consume_result = {
            "success": True,
            "expired": False,
            "task": {
                "id": "task-dispatch-error",
                "func_path": "myapp.tasks.work",
                "payload": {"x": 1},
            },
            "remaining_tokens": 4,
            "active_concurrency": 1,
            "reset_in_ms": 100,
            "remaining_tasks": 2,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_dispatch_task",
                side_effect=RuntimeError("dispatch failed"),
            ),
        ):
            await limiter.drain()

        # Assert
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a recovery drain after dispatch failure"
        )
        assert limiter.scheduled_drains[0] == pytest.approx(0.1), (
            "recovery drain delay should be min(60, 0.1 * 2^(1-1)) = 0.1"
        )
        assert limiter._consecutive_drain_failures == 1, (
            "failure counter should be incremented to 1"
        )

    async def test_drain_stops_when_buffer_empty(self, limiter, mock_target):
        """Verify that ``drain()`` stops without scheduling a follow-up when no tasks remain."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when buffer is empty"
        )
        assert limiter.scheduled_drains == [], (
            "drain should not schedule follow-up when buffer is empty"
        )

    async def test_drain_handles_expired_task_without_dispatch(
        self, limiter, mock_target, caplog
    ):
        """Verify that an expired consume result is neither dispatched nor rescheduled."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": True,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with caplog.at_level(logging.WARNING, logger="celery_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch expired task results"
        )
        assert limiter.scheduled_drains == [], (
            "drain should not schedule follow-up when expired result has no remaining tasks"
        )
        assert any(
            record.levelname == "WARNING"
            and f"limiter={limiter.id}" in record.message
            and "DLQ" in record.message
            for record in caplog.records
        ), "should emit a warning log indicating the expired task was moved to the DLQ"

    async def test_drain_stops_when_concurrency_at_capacity(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` stops without scheduling a follow-up when concurrency is saturated."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": limiter.max_concurrency,
            "reset_in_ms": 100,
            "remaining_tasks": 3,
        }

        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
            ):
                await limiter.drain()

        # Assert
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when consume is unsuccessful"
        )
        assert limiter.scheduled_drains == [], (
            "drain should not schedule retry when concurrency is at capacity"
        )
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={limiter.id}" in record.message
            and f"active={limiter.max_concurrency}" in record.message
            and f"max={limiter.max_concurrency}" in record.message
            for record in caplog.records
        ), (
            "should emit a debug log indicating concurrency is at capacity with active and max counts"
        )

    async def test_drain_schedules_delayed_retry_when_rate_limited(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` schedules a delayed retry when the remaining tokens are exhausted."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 250,
            "remaining_tasks": 4,
            "val_previous": 0,
            "val_current": 5,
        }

        # Act
        with caplog.at_level(logging.INFO, logger="celery_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(mock_target, "consume", return_value=consume_result),
                patch.object(
                    mock_target,
                    "_calculate_smart_jitter",
                    return_value=0.0,
                ) as mock_jitter,
            ):
                await limiter.drain()

        # Assert
        assert mock_jitter.call_count == 1, (
            "drain should compute jitter for rate-limited retry"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule one delayed retry when rate-limited"
        )
        assert limiter.scheduled_drains[0] == pytest.approx(0.251), (
            "rate-limited retry delay should be reset_in_ms/1000 + 0.001 = 0.251 on fallback path"
        )
        assert any(
            record.levelname == "INFO"
            and f"limiter={limiter.id}" in record.message
            and "delay_s=" in record.message
            and "remaining_tasks=" in record.message
            for record in caplog.records
        ), (
            "should emit an info log for the rate-limited retry with delay and remaining tasks"
        )

    async def test_drain_calls_refresh_config_if_available(self, limiter, mock_target):
        """Verify that ``drain()`` calls ``refresh_config()`` when the attribute exists."""
        # Arrange
        mock_target.refresh_config = self._mock_cls()
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        mock_target.refresh_config.assert_called_once()

    async def test_drain_resets_failure_counter_on_success(self, limiter, mock_target):
        """Verify that the consecutive failure counter resets to zero after a successful drain."""
        # Arrange
        mock_target._consecutive_drain_failures = 3
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 5,
            "active_concurrency": 0,
            "reset_in_ms": 100,
            "remaining_tasks": 0,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
        ):
            await limiter.drain()

        # Assert
        assert limiter._consecutive_drain_failures == 0, (
            "failure counter should reset to 0 after successful drain"
        )

    async def test_drain_backoff_increases_with_consecutive_failures(
        self, limiter, mock_target
    ):
        """Verify that the recovery delay doubles with each consecutive failure."""
        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                side_effect=lambda: self.lock_result(True),
            ),
            patch.object(
                mock_target,
                "consume",
                side_effect=RuntimeError("fail"),
            ),
        ):
            # Two consecutive failures to verify escalating backoff.
            await limiter.drain()
            await limiter.drain()

        # Assert
        assert limiter._consecutive_drain_failures == 2, (
            "failure counter should reflect two consecutive failures"
        )
        assert len(limiter.scheduled_drains) == 2, (
            "each failure should schedule a recovery drain"
        )
        first_delay = limiter.scheduled_drains[0]
        second_delay = limiter.scheduled_drains[1]
        assert first_delay == pytest.approx(0.1), "first recovery delay should be 100ms"
        assert second_delay == pytest.approx(0.2), (
            "second recovery delay should be 200ms"
        )

    async def test_drain_handles_double_failure_when_schedule_drain_also_fails(
        self, limiter, mock_target, caplog
    ):
        """Verify that ``drain()`` does not propagate when both the inner drain and recovery scheduling fail."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
            with (
                patch.object(
                    mock_target,
                    "execution_lock",
                    return_value=self.lock_result(True),
                ),
                patch.object(
                    mock_target,
                    "consume",
                    side_effect=RuntimeError("consume failed"),
                ),
                patch.object(
                    mock_target,
                    "_schedule_drain",
                    side_effect=RuntimeError("schedule also failed"),
                ),
            ):
                # This invocation must not raise.
                await limiter.drain()

        # Assert
        assert limiter._consecutive_drain_failures == 1, (
            "failure counter should be incremented despite double failure"
        )
        assert any(
            record.levelname == "CRITICAL"
            and f"limiter={limiter.id}" in record.message
            and "Recovery scheduling also failed" in record.message
            for record in caplog.records
        ), "should emit a critical log when both drain and recovery scheduling fail"

    async def test_drain_skips_jitter_on_token_recovery_path(
        self, limiter, mock_target
    ):
        """Verify that jitter is skipped when the token recovery uses sliding-window decay."""
        # Arrange
        consume_result = {
            "success": False,
            "expired": False,
            "task": None,
            "remaining_tokens": 0,
            "active_concurrency": 1,
            "reset_in_ms": 500,
            "remaining_tasks": 4,
            # Non-fallback path: val_previous > 0 and val_current < limit.
            "val_previous": 5,
            "val_current": 3,
        }

        # Act
        with (
            patch.object(
                mock_target,
                "execution_lock",
                return_value=self.lock_result(True),
            ),
            patch.object(mock_target, "consume", return_value=consume_result),
            patch.object(
                mock_target,
                "_calculate_smart_jitter",
                return_value=0.0,
            ) as mock_jitter,
        ):
            await limiter.drain()

        # Assert
        assert mock_jitter.call_count == 0, (
            "jitter should not be calculated on the token-recovery path"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a retry based on pure token-recovery delay"
        )
        assert limiter.scheduled_drains[0] == pytest.approx(0.001), (
            "token-recovery retry delay should be 0.001 when decay has already freed a token"
        )

    @staticmethod
    async def test_shutdown_delegates_to_drain_loop(limiter):
        """Verify that ``shutdown()`` completes without error on an idle limiter."""
        # Act & Assert
        # This invocation must not raise. The drain loop was never woken because
        # TrackingRateLimiter overrides _schedule_drain; as such, this exercises
        # the delegation path on the base class.
        await limiter.shutdown()

    @staticmethod
    async def test_drain_defers_when_local_capacity_full(limiter, mock_target):
        """Verify that ``drain()`` defers execution when local capacity is exhausted."""
        # Arrange
        consume_mock = MagicMock()

        # Act
        with (
            patch.object(mock_target, "_has_local_capacity", return_value=False),
            patch.object(mock_target, "consume", consume_mock),
        ):
            await limiter.drain()

        # Assert
        assert consume_mock.call_count == 0, (
            "drain should not consume when local capacity is full"
        )
        assert limiter.dispatched_tasks == [], (
            "drain should not dispatch when local capacity is full"
        )
        assert len(limiter.scheduled_drains) == 1, (
            "drain should schedule a retry when local capacity is full"
        )
        expected_delay = limiter.window / limiter.limit
        assert limiter.scheduled_drains[0] == expected_delay, (
            "local-capacity-full retry delay should equal one token interval"
        )

    @staticmethod
    async def test_drain_loop_watchdog_interval(limiter):
        """Verify that the watchdog interval is ``max(5.0, window * 2)``."""
        expected = max(5.0, limiter.window * 2)
        actual = limiter._drain_loop._watchdog_interval
        assert actual == expected, (
            f"watchdog interval should be max(5.0, window * 2) = {expected}, got {actual}"
        )

    @staticmethod
    async def test_trigger_consume_schedules_drain(limiter):
        """Verify that ``trigger_consume()`` schedules a drain."""
        # Act
        await limiter.trigger_consume()

        # Assert
        assert len(limiter.scheduled_drains) == 1, (
            "trigger_consume should schedule exactly one drain"
        )

    @staticmethod
    async def test_publish_drain_signal_logs_on_failure(limiter, mock_target, caplog):
        """Verify that ``_publish_drain_signal()`` emits a debug log when Redis publish fails."""
        # Arrange
        with patch.object(
            mock_target.redis, "publish", side_effect=Exception("publish boom")
        ):
            # Act
            with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter"):
                await limiter.trigger_consume()

        # Assert
        assert any(
            record.levelname == "DEBUG"
            and f"limiter={limiter.id}" in record.message
            and "Failed to publish drain signal" in record.message
            for record in caplog.records
        ), "should emit a debug log when redis publish raises an exception"


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestSyncDrain(DrainBehaviorTests):
    """Sync drain behavior exercised through the async adapter."""

    _mock_cls = MagicMock

    @staticmethod
    @contextmanager
    def lock_result(acquired):
        """Provide a sync context manager that yields a deterministic lock outcome."""
        yield acquired

    @pytest.fixture
    def limiter(self, tracking_limiter):
        """Wrap the sync tracking limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(tracking_limiter)

    @pytest.fixture
    def mock_target(self, tracking_limiter):
        """Return the inner sync tracking limiter as the patch target."""
        return tracking_limiter


class TestAsyncDrain(DrainBehaviorTests):
    """Async drain behavior exercised natively."""

    _mock_cls = AsyncMock

    @staticmethod
    @asynccontextmanager
    async def lock_result(acquired):
        """Provide an async context manager that yields a deterministic lock outcome."""
        yield acquired

    @pytest.fixture
    def limiter(self, async_tracking_limiter):
        """Provide the async tracking limiter directly."""
        return async_tracking_limiter

    @pytest.fixture
    def mock_target(self, async_tracking_limiter):
        """Return the async tracking limiter as the patch target."""
        return async_tracking_limiter


# ---------------------------------------------------------------------------
# Drain-disabled tests
# ---------------------------------------------------------------------------


class DrainDisabledTests:
    """Tests for scheduler-only mode (``drain_enabled=False``).

    Subclasses must provide a ``limiter_factory`` fixture that returns
    a factory ``(**kwargs) -> limiter`` for creating limiters.
    """

    @staticmethod
    async def test_schedule_drain_is_noop_when_drain_disabled(limiter_factory):
        """Verify that ``_schedule_drain()`` is a no-op when ``drain_enabled=False``."""
        # Arrange
        limiter = await limiter_factory(drain_enabled=False)

        # Assert
        assert limiter._drain_loop is None, (
            "drain loop should not be created when drain_enabled=False"
        )
        assert limiter.drain_enabled is False, (
            "drain_enabled should be False when explicitly disabled"
        )

        # Act
        # trigger_consume delegates to _schedule_drain, which should be a no-op.
        await limiter.trigger_consume()

        # Assert
        assert limiter._drain_loop is None, (
            "drain loop must remain None when drain_enabled=False"
        )

    @staticmethod
    async def test_shutdown_is_safe_when_drain_disabled(limiter_factory):
        """Verify that ``shutdown()`` does not raise when ``drain_enabled=False``."""
        # Arrange
        limiter = await limiter_factory(drain_enabled=False)

        # Act & Assert
        # Must not raise.
        await limiter.shutdown()


class TestSyncDrainDisabled(DrainDisabledTests):
    """Sync scheduler-only mode exercised through the async adapter."""

    @pytest.fixture
    def limiter_factory(self, redis_client, limiter_id):
        """Factory that creates sync limiters wrapped in the async adapter."""

        async def _factory(**kwargs):
            defaults = dict(
                redis_client=redis_client,
                limiter_id=f"{limiter_id}_drain_disabled",
                limit=5,
                window=60,
                max_concurrency=2,
            )
            defaults.update(kwargs)
            return SyncToAsyncLimiterAdapter(TrackingRateLimiter(**defaults))

        return _factory


class TestAsyncDrainDisabled(DrainDisabledTests):
    """Async scheduler-only mode exercised natively."""

    @pytest.fixture
    def limiter_factory(self, async_redis_client, limiter_id):
        """Factory that creates async limiters."""

        async def _factory(**kwargs):
            defaults = dict(
                redis_client=async_redis_client,
                limiter_id=f"{limiter_id}_async_drain_disabled",
                limit=5,
                window=60,
                max_concurrency=2,
            )
            defaults.update(kwargs)
            lim = MinimalAsyncRateLimiter(**defaults)
            await lim.start()
            return lim

        return _factory


class TestAsyncScheduleDrainDelegation:
    """Tests for the base-class ``_schedule_drain`` implementation on async limiters.

    The tracking and minimal async limiters override ``_schedule_drain`` with
    no-ops, so the base-class guard (``if self._drain_loop is not None``) is
    never exercised through normal fixtures. These tests call the base-class
    method directly to verify the delegation logic.
    """

    @staticmethod
    async def test_schedule_drain_delegates_to_drain_loop_wake(
        async_generic_limiter,
    ):
        """Verify that the base-class ``_schedule_drain`` delegates to ``_drain_loop.wake()``."""
        # Arrange
        mock_loop = MagicMock()

        # Act
        with patch.object(async_generic_limiter, "_drain_loop", mock_loop):
            AbstractAsyncDistributedRateLimiter._schedule_drain(
                async_generic_limiter, delay=1.5
            )

        # Assert
        (
            mock_loop.wake.assert_called_once_with(1.5),
            (
                "_schedule_drain should delegate to _drain_loop.wake with the provided delay"
            ),
        )

    @staticmethod
    async def test_schedule_drain_is_noop_when_drain_loop_is_none(
        async_generic_limiter,
    ):
        """Verify that the base-class ``_schedule_drain`` is a no-op when ``_drain_loop`` is ``None``."""
        # Act & Assert
        # Must not raise AttributeError.
        with patch.object(async_generic_limiter, "_drain_loop", None):
            AbstractAsyncDistributedRateLimiter._schedule_drain(async_generic_limiter)


class TestCrossProcessDrainSignal:
    """Tests for the Redis Pub/Sub cross-process drain notification mechanism.

    These tests are inherently sync-specific: they use ``redis_client.pubsub()``,
    ``time.sleep()``, and threading-based subscribers.
    """

    @staticmethod
    def test_trigger_consume_publishes_drain_signal(redis_client, limiter_id):
        """Verify that ``trigger_consume()`` publishes a drain signal to the Pub/Sub channel."""
        # Arrange
        # Set up a test subscriber to capture the message.
        limiter_id = f"{limiter_id}_pubsub"
        channel = f"{limiter_id}:drain_signal"
        test_sub = redis_client.pubsub()
        test_sub.subscribe(channel)
        test_sub.get_message(timeout=1.0)  # consume the subscribe confirmation

        limiter = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )

        try:
            # Act
            limiter.trigger_consume()

            # Assert
            message = test_sub.get_message(timeout=2.0)
            assert message is not None, "trigger_consume should publish a drain signal"
            assert message["type"] == "message", (
                "received message must be of type 'message'"
            )
            assert message["data"] == limiter._worker_id, (
                "drain signal payload should be the sender's worker_id"
            )
        finally:
            limiter.shutdown()
            test_sub.unsubscribe()
            test_sub.close()

    @staticmethod
    def test_subscriber_wakes_drain_on_cross_process_signal(redis_client, limiter_id):
        """Verify that a drain signal from one limiter wakes another limiter's drain loop."""
        # Arrange
        # Two limiter instances with the same ID (simulating two workers).
        limiter_id = f"{limiter_id}_cross"
        limiter_a = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )
        limiter_b = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )

        try:
            # Act
            # Trigger consume on limiter A (publishes with A's worker_id).
            limiter_a.trigger_consume()

            # Allow the subscriber thread time to process the message.
            time.sleep(1.0)

            # Assert
            # Limiter B's subscriber should have received the signal
            # and called _schedule_drain() on limiter B.
            assert len(limiter_b.scheduled_drains) >= 1, (
                "limiter B should have received a drain signal from limiter A"
            )
        finally:
            limiter_a.shutdown()
            limiter_b.shutdown()

    @staticmethod
    def test_subscriber_ignores_self_notification(redis_client, limiter_id):
        """Verify that the subscriber ignores drain signals originating from the local process."""
        # Arrange
        limiter_id = f"{limiter_id}_self"
        limiter = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )

        try:
            # Act
            limiter.trigger_consume()

            # Allow the subscriber thread time to process (or ignore) the message.
            time.sleep(1.0)

            # Assert
            # scheduled_drains should have exactly 1 entry from the
            # direct _schedule_drain() call in trigger_consume(). The subscriber
            # receives the message but ignores it because the sender's worker_id
            # matches the local worker_id.
            assert len(limiter.scheduled_drains) == 1, (
                "subscriber should ignore self-notifications; "
                f"expected 1 scheduled drain, got {len(limiter.scheduled_drains)}"
            )
        finally:
            limiter.shutdown()

    @staticmethod
    def test_drain_disabled_still_publishes(redis_client, limiter_id):
        """Verify that ``trigger_consume()`` publishes even when ``drain_enabled=False``."""
        # Arrange
        limiter_id = f"{limiter_id}_disabled_pub"
        channel = f"{limiter_id}:drain_signal"
        test_sub = redis_client.pubsub()
        test_sub.subscribe(channel)
        test_sub.get_message(timeout=1.0)  # consume the subscribe confirmation

        limiter = TrackingRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )

        try:
            # Act
            limiter.trigger_consume()

            # Assert
            message = test_sub.get_message(timeout=2.0)
            assert message is not None, (
                "trigger_consume should publish even with drain_enabled=False"
            )
            assert message["data"] == limiter._worker_id, (
                "drain signal payload should be the sender's worker_id"
            )
        finally:
            limiter.shutdown()
            test_sub.unsubscribe()
            test_sub.close()


# ---------------------------------------------------------------------------
# Cross-process drain signal tests (async, variant-specific)
# ---------------------------------------------------------------------------


class TestAsyncCrossProcessDrainSignal:
    """Async tests for the Redis Pub/Sub cross-process drain notification mechanism.

    These tests are inherently async-specific: they use ``redis.asyncio.Redis.pubsub()``,
    ``asyncio.sleep()``, and task-based subscribers.
    """

    @staticmethod
    async def test_trigger_consume_publishes_drain_signal(
        async_redis_client, limiter_id
    ):
        """Verify that ``trigger_consume()`` publishes a drain signal to the Pub/Sub channel."""
        # Arrange
        # Set up a test subscriber to capture the message.
        limiter_id = f"{limiter_id}_async_pubsub"
        channel = f"{limiter_id}:drain_signal"
        test_sub = async_redis_client.pubsub()
        await test_sub.subscribe(channel)
        await test_sub.get_message(timeout=1.0)  # consume the subscribe confirmation

        limiter = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )
        await limiter.start()

        try:
            # Act
            await limiter.trigger_consume()

            # Assert
            message = await test_sub.get_message(timeout=2.0)
            assert message is not None, "trigger_consume should publish a drain signal"
            assert message["type"] == "message", (
                "received message must be of type 'message'"
            )
            payload = message["data"]
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            assert payload == limiter._worker_id, (
                "drain signal payload should be the sender's worker_id"
            )
        finally:
            await limiter.shutdown()
            await test_sub.unsubscribe()
            await test_sub.aclose()

    @staticmethod
    async def test_subscriber_wakes_drain_on_cross_process_signal(
        async_redis_client, limiter_id
    ):
        """Verify that a drain signal from one limiter wakes another limiter's drain loop."""
        # Arrange
        # Two limiter instances with the same ID (simulating two workers).
        # Each needs its own Redis connection for independent Pub/Sub.
        limiter_id = f"{limiter_id}_async_cross"
        conn_a = redis.asyncio.Redis(
            host=async_redis_client.connection_pool.connection_kwargs["host"],
            port=async_redis_client.connection_pool.connection_kwargs["port"],
            db=async_redis_client.connection_pool.connection_kwargs.get("db", 0),
            decode_responses=True,
        )
        conn_b = redis.asyncio.Redis(
            host=async_redis_client.connection_pool.connection_kwargs["host"],
            port=async_redis_client.connection_pool.connection_kwargs["port"],
            db=async_redis_client.connection_pool.connection_kwargs.get("db", 0),
            decode_responses=True,
        )

        limiter_a = AsyncTrackingRateLimiter(
            redis_client=conn_a,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )
        limiter_b = AsyncTrackingRateLimiter(
            redis_client=conn_b,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )
        await limiter_a.start()
        await limiter_b.start()

        try:
            # Act
            # Trigger consume on limiter A (publishes with A's worker_id).
            await limiter_a.trigger_consume()

            # Allow the subscriber task time to process the message.
            await asyncio.sleep(1.0)

            # Assert
            # Limiter B's subscriber should have received the signal
            # and called _schedule_drain() on limiter B.
            assert len(limiter_b.scheduled_drains) >= 1, (
                "limiter B should have received a drain signal from limiter A"
            )
        finally:
            await limiter_a.shutdown()
            await limiter_b.shutdown()
            await conn_a.aclose()
            await conn_b.aclose()

    @staticmethod
    async def test_subscriber_ignores_self_notification(async_redis_client, limiter_id):
        """Verify that the subscriber ignores drain signals originating from the local process."""
        # Arrange
        limiter_id = f"{limiter_id}_async_self"
        limiter = AsyncTrackingRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )
        await limiter.start()

        try:
            # Act
            await limiter.trigger_consume()

            # Allow the subscriber task time to process (or ignore) the message.
            await asyncio.sleep(1.0)

            # Assert
            # scheduled_drains should have exactly 1 entry from the
            # direct _schedule_drain() call in trigger_consume(). The subscriber
            # receives the message but ignores it because the sender's worker_id
            # matches the local worker_id.
            assert len(limiter.scheduled_drains) == 1, (
                "subscriber should ignore self-notifications; "
                f"expected 1 scheduled drain, got {len(limiter.scheduled_drains)}"
            )
        finally:
            await limiter.shutdown()

    @staticmethod
    async def test_drain_disabled_still_publishes(async_redis_client, limiter_id):
        """Verify that ``trigger_consume()`` publishes even when ``drain_enabled=False``."""
        # Arrange
        limiter_id = f"{limiter_id}_async_disabled_pub"
        channel = f"{limiter_id}:drain_signal"
        test_sub = async_redis_client.pubsub()
        await test_sub.subscribe(channel)
        await test_sub.get_message(timeout=1.0)  # consume the subscribe confirmation

        limiter = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )
        await limiter.start()

        try:
            # Act
            await limiter.trigger_consume()

            # Assert
            message = await test_sub.get_message(timeout=2.0)
            assert message is not None, (
                "trigger_consume should publish even with drain_enabled=False"
            )
            payload = message["data"]
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            assert payload == limiter._worker_id, (
                "drain signal payload should be the sender's worker_id"
            )
        finally:
            await limiter.shutdown()
            await test_sub.unsubscribe()
            await test_sub.aclose()
