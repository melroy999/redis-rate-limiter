"""Unit tests for the ``BackendHealthMonitor`` and ``AsyncBackendHealthMonitor``.

Tests are written once in async form using a mixin pattern. Each test
receives variant-specific fixtures (``monitor``, ``mock_limiter``) and uses
a class-level ``run_once(monitor)`` customization point to accommodate the
structural differences between the sync and async health monitor
implementations.

The ``_run_once()`` method is tested directly (without starting the
background thread or asyncio task) to keep tests deterministic, following
the same pattern used for ``DrainLoop`` testing via ``_drain_inner()``.

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``,
      ``limiter_id``: from ``tests/conftest.py``.
    - ``stub_limiter``, ``async_stub_limiter``:
      from ``tests/implementations/conftest.py``.
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from redis_rate_limiter.core.async_limiters import (
    AbstractAsyncDistributedRateLimiter,
    AsyncBackendHealthMonitor,
)
from redis_rate_limiter.core.limiters import (
    BackendHealthMonitor,
    DistributedRateLimiterMixin,
)
from tests.helpers.utils import assert_log_emitted


# ---------------------------------------------------------------------------
# Unified behavioral tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class HealthMonitorBehaviorTests:
    """Abstract test suite for ``_run_once()`` state-transition logic.

    Subclasses must provide:
        - ``run_once(monitor)``: an async static method that invokes
          ``_run_once()`` on the monitor (sync or async).
        - ``monitor``: a fixture returning a ``BackendHealthMonitor`` or
          ``AsyncBackendHealthMonitor``.
        - ``mock_limiter``: a fixture returning a mock with
          ``_check_backend_health`` configured as sync (``MagicMock``) or
          async (``AsyncMock``).
    """

    @staticmethod
    async def run_once(monitor):
        """Override in subclass to invoke ``_run_once()`` on the monitor."""
        raise NotImplementedError

    @staticmethod
    async def test_is_healthy_defaults_to_true(monitor):
        """Verify that a freshly created monitor reports
        healthy before any check runs.
        """
        # Assert
        assert monitor.is_healthy is True, (
            "monitor should default to healthy before first check"
        )

    async def test_is_healthy_reflects_unhealthy_state(self, monitor, mock_limiter):
        """Verify that ``is_healthy`` returns ``False`` after the health check fails."""
        # Arrange
        mock_limiter._check_backend_health.return_value = False

        # Act
        await self.run_once(monitor)

        # Assert
        assert monitor.is_healthy is False, (
            "monitor should reflect unhealthy state after failed check"
        )

    async def test_is_healthy_recovers_after_healthy_check(self, monitor, mock_limiter):
        """Verify that ``is_healthy`` returns ``True`` after
        recovery from unhealthy state.
        """
        # Arrange
        mock_limiter._check_backend_health.return_value = False
        await self.run_once(monitor)
        mock_limiter._check_backend_health.return_value = True

        # Act
        await self.run_once(monitor)

        # Assert
        assert monitor.is_healthy is True, (
            "monitor should reflect healthy state after recovery"
        )

    async def test_health_check_exception_transitions_to_unhealthy(
        self, monitor, mock_limiter
    ):
        """Verify that an exception from the health check is treated as unhealthy."""
        # Arrange
        mock_limiter._check_backend_health.side_effect = ConnectionError("redis down")

        # Act
        await self.run_once(monitor)

        # Assert
        assert monitor.is_healthy is False, "exception should be treated as unhealthy"


# ---------------------------------------------------------------------------
# Unified observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class HealthMonitorObservabilityTests:
    """Observability tests for state-transition log emissions.

    Subclasses must provide the same customization points as
    ``HealthMonitorBehaviorTests``.
    """

    @staticmethod
    async def run_once(monitor):
        """Override in subclass to invoke ``_run_once()`` on the monitor."""
        raise NotImplementedError

    async def test_unhealthy_transition_emits_warning_log(
        self, monitor, mock_limiter, caplog
    ):
        """Verify that transitioning to unhealthy emits a WARNING log."""
        # Arrange
        mock_limiter._check_backend_health.return_value = False

        # Act
        with caplog.at_level(logging.WARNING, logger="redis_rate_limiter"):
            await self.run_once(monitor)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            required_fragments=[f"limiter={monitor._limiter.id}"],
            message="should emit a warning log on transition to unhealthy",
        )

    async def test_recovery_emits_info_log(self, monitor, mock_limiter, caplog):
        """Verify that recovery from unhealthy emits an INFO log."""
        # Arrange
        mock_limiter._check_backend_health.return_value = False
        await self.run_once(monitor)
        mock_limiter._check_backend_health.return_value = True

        # Act
        with caplog.at_level(logging.INFO, logger="redis_rate_limiter"):
            await self.run_once(monitor)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="INFO",
            required_fragments=[f"limiter={monitor._limiter.id}"],
            message="should emit an info log on recovery to healthy",
        )

    async def test_repeated_unhealthy_does_not_repeat_warning(
        self, monitor, mock_limiter, caplog
    ):
        """Verify that consecutive unhealthy checks do not produce repeated warnings."""
        # Arrange
        mock_limiter._check_backend_health.return_value = False

        # Act
        with caplog.at_level(logging.WARNING, logger="redis_rate_limiter"):
            await self.run_once(monitor)
            await self.run_once(monitor)
            await self.run_once(monitor)

        # Assert
        warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warning_records) == 1, (
            "only one warning should be emitted for consecutive unhealthy states"
        )

    async def test_exception_transition_emits_warning_log(
        self, monitor, mock_limiter, caplog
    ):
        """Verify that a health check exception triggers the
        same warning as a ``False`` return.
        """
        # Arrange
        mock_limiter._check_backend_health.side_effect = RuntimeError("check failed")

        # Act
        with caplog.at_level(logging.WARNING, logger="redis_rate_limiter"):
            await self.run_once(monitor)

        # Assert
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            required_fragments=[f"limiter={monitor._limiter.id}"],
            message="should emit a warning log when health check raises an exception",
        )

    async def test_healthy_to_healthy_emits_no_log(self, monitor, mock_limiter, caplog):
        """Verify that no log is emitted when the state remains healthy."""
        # Arrange
        mock_limiter._check_backend_health.return_value = True

        # Act
        with caplog.at_level(logging.DEBUG, logger="redis_rate_limiter"):
            await self.run_once(monitor)

        # Assert
        transition_records = [
            r
            for r in caplog.records
            if r.levelname in ("WARNING", "INFO")
            and "health check" in r.getMessage().lower()
        ]
        assert len(transition_records) == 0, (
            "no transition log should be emitted when state remains healthy"
        )


# ---------------------------------------------------------------------------
# Sync-specific tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestSyncHealthMonitor(
    HealthMonitorBehaviorTests, HealthMonitorObservabilityTests
):
    """Sync health monitor behavior exercised through the unified mixin."""

    @staticmethod
    async def run_once(monitor):
        """Invoke the sync ``_run_once()`` method (no await needed)."""
        monitor._run_once()

    @pytest.fixture
    def mock_limiter(self):
        """Provide a mock limiter with a sync ``_check_backend_health`` method."""
        limiter = MagicMock()
        limiter.id = "test_health_monitor"
        limiter._check_backend_health.return_value = True
        return limiter

    @pytest.fixture
    def monitor(self, mock_limiter):
        """Provide a ``BackendHealthMonitor`` instance backed by a mock limiter."""
        return BackendHealthMonitor(mock_limiter, interval=1.0)


@pytest.mark.behavior
class TestSyncHealthMonitorLifecycle:
    """Sync-only lifecycle and opt-in detection tests
    for ``BackendHealthMonitor``.
    """

    @pytest.fixture
    def mock_limiter(self):
        """Provide a mock limiter with a sync ``_check_backend_health`` method."""
        limiter = MagicMock()
        limiter.id = "test_health_monitor_lifecycle"
        limiter._check_backend_health.return_value = True
        return limiter

    @pytest.fixture
    def monitor(self, mock_limiter):
        """Provide a ``BackendHealthMonitor`` instance backed by a mock limiter."""
        return BackendHealthMonitor(mock_limiter, interval=1.0)

    @staticmethod
    def test_shutdown_stops_thread(monitor):
        """Verify that ``shutdown()`` terminates the background thread cleanly."""
        # Arrange
        monitor.start()

        # Act
        monitor.shutdown()

        # Assert
        assert monitor._thread is None or not monitor._thread.is_alive(), (
            "background thread should not be alive after shutdown"
        )

    @staticmethod
    def test_monitor_not_started_for_default_implementation(stub_limiter):
        """Verify that in-process backends (default
        ``_check_backend_health``) do not start a monitor.
        """
        # Assert
        assert stub_limiter._backend_health_monitor is None, (
            "in-process backends should not have a health monitor"
        )

    @staticmethod
    def test_monitor_not_started_when_drain_disabled(redis_client, limiter_id):
        """Verify that scheduler-only instances do not start a health monitor."""
        # Arrange
        from tests.implementations.conftest import StubRateLimiter

        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_drain_disabled",
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )

        # Assert
        try:
            assert limiter._backend_health_monitor is None, (
                "scheduler-only instances should not have a health monitor"
            )
        finally:
            limiter.shutdown()

    @staticmethod
    def test_monitor_uses_default_healthy_hook(stub_limiter):
        """Verify that the base mixin ``_check_backend_health`` returns ``True``."""
        # Arrange
        # Use a real limiter instance; MagicMock is incompatible with the
        # mutmut trampoline (object.__getattribute__ bypasses mock __getattr__).
        result = DistributedRateLimiterMixin._check_backend_health(stub_limiter)

        # Assert
        assert result is True, "base mixin health check should always return true"


# ---------------------------------------------------------------------------
# Async-specific tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestAsyncHealthMonitor(
    HealthMonitorBehaviorTests, HealthMonitorObservabilityTests
):
    """Async health monitor behavior exercised natively."""

    @staticmethod
    async def run_once(monitor):
        """Invoke the async ``_run_once()`` coroutine."""
        await monitor._run_once()

    @pytest.fixture
    def mock_limiter(self):
        """Provide a mock limiter with an async ``_check_backend_health`` method."""
        limiter = MagicMock()
        limiter.id = "test_async_health_monitor"
        limiter._check_backend_health = AsyncMock(return_value=True)
        return limiter

    @pytest.fixture
    def monitor(self, mock_limiter):
        """Provide an ``AsyncBackendHealthMonitor`` instance
        backed by a mock limiter.
        """
        return AsyncBackendHealthMonitor(mock_limiter, interval=1.0)


@pytest.mark.behavior
class TestAsyncHealthMonitorLifecycle:
    """Async-only lifecycle and opt-in detection tests
    for ``AsyncBackendHealthMonitor``.
    """

    @pytest.fixture
    def mock_limiter(self):
        """Provide a mock limiter with an async ``_check_backend_health`` method."""
        limiter = MagicMock()
        limiter.id = "test_async_health_monitor_lifecycle"
        limiter._check_backend_health = AsyncMock(return_value=True)
        return limiter

    @pytest.fixture
    def monitor(self, mock_limiter):
        """Provide an ``AsyncBackendHealthMonitor`` instance
        backed by a mock limiter.
        """
        return AsyncBackendHealthMonitor(mock_limiter, interval=1.0)

    @staticmethod
    async def test_shutdown_cancels_task(monitor):
        """Verify that ``shutdown()`` cancels the background asyncio task cleanly."""
        # Arrange
        monitor.start()

        # Act
        await monitor.shutdown()

        # Assert
        assert monitor._task is None or monitor._task.done(), (
            "background task should be done after shutdown"
        )

    @staticmethod
    async def test_monitor_not_started_for_default_implementation(
        async_stub_limiter,
    ):
        """Verify that in-process async backends (default
        ``_check_backend_health``) do not start a monitor.
        """
        # Assert
        assert async_stub_limiter._backend_health_monitor is None, (
            "in-process async backends should not have a health monitor"
        )

    @staticmethod
    async def test_monitor_not_started_when_drain_disabled(
        async_redis_client, limiter_id
    ):
        """Verify that scheduler-only async instances do not start a health monitor."""
        # Arrange
        from tests.implementations.conftest import AsyncStubRateLimiter

        limiter = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_async_drain_disabled",
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )
        await limiter.start()

        # Assert
        try:
            assert limiter._backend_health_monitor is None, (
                "scheduler-only async instances should not have a health monitor"
            )
        finally:
            await limiter.shutdown()

    @staticmethod
    async def test_monitor_uses_default_healthy_hook(async_stub_limiter):
        """Verify that the async base class
        ``_check_backend_health`` returns ``True``.
        """
        # Act
        result = await AbstractAsyncDistributedRateLimiter._check_backend_health(
            async_stub_limiter
        )

        # Assert
        assert result is True, "async base class health check should always return true"
