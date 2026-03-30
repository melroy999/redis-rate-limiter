"""Tests for limiter configuration defaults, boundary conditions,
window change logging, and metric callbacks.

This module covers the initial default values of freshly constructed limiters,
configuration boundary conditions (invalid and edge-case parameter values),
the ``_apply_config_overrides`` window-change detection log, and the
``_emit_metric`` warning when a metrics callback raises.

Tests are written once in async form via the mixin pattern; the sync
implementation participates via ``SyncToAsyncLimiterAdapter``.

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``, ``limiter_id``:
      from ``tests/conftest.py``.
    - ``stub_limiter``, ``async_stub_limiter``:
      from ``tests/implementations/conftest.py``.
"""

import inspect
import logging

import pytest

from redis_rate_limiter.core.async_limiters import AbstractAsyncDistributedRateLimiter
from redis_rate_limiter.core.limiters import (
    AbstractDistributedRateLimiter,
    DistributedRateLimiterMixin,
)
from tests.helpers.adapters import SyncToAsyncLimiterAdapter
from tests.helpers.utils import assert_log_emitted
from tests.implementations.conftest import StubRateLimiter

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class InitialDefaultTests:
    """Unified tests for the initial default values of freshly constructed limiters.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async limiter.
    """

    @staticmethod
    async def test_fresh_limiter_has_expected_defaults(limiter):
        """Verify that a freshly constructed limiter exposes
        the correct initial defaults.
        """
        # Assert
        assert limiter._drain_paused_until == pytest.approx(0.0), (
            "fresh limiter should not be paused"
        )
        assert limiter._config_version == 0, (
            "fresh limiter should start at config version 0"
        )
        assert limiter.jitter_enabled is True, (
            "fresh limiter should have jitter enabled by default"
        )
        assert limiter.jitter_min_pct == pytest.approx(0.02), (
            "fresh limiter should default to jitter_min_pct of 0.02"
        )
        assert limiter.jitter_max_pct == pytest.approx(0.08), (
            "fresh limiter should default to jitter_max_pct of 0.08"
        )
        assert limiter.drain_enabled is True, (
            "fresh limiter should have draining enabled by default"
        )


# ---------------------------------------------------------------------------
# Unified observability tests
# ---------------------------------------------------------------------------


class WindowChangeObservabilityTests:
    """Unified tests for the window-change detection log in ``_apply_config_overrides``.

    Subclasses must provide a ``limiter`` fixture that returns either a
    ``SyncToAsyncLimiterAdapter``-wrapped sync limiter or a native async limiter,
    and must set ``_log_label`` to the expected log prefix.
    """

    _log_label: str

    async def test_window_change_emits_info_log(self, limiter, caplog):
        """Verify that changing the window via
        ``_apply_config_overrides`` emits an INFO log.
        """
        # Arrange
        old_window = limiter.window
        new_window = old_window * 2
        pause = max(old_window, new_window)

        # Act
        with caplog.at_level(logging.INFO, logger="redis_rate_limiter.core.base"):
            limiter._apply_config_overrides({"window": new_window})

        # Assert
        assert limiter.window == new_window, "window should be updated to the new value"
        assert_log_emitted(
            caplog.records,
            level="INFO",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                f"new_window={new_window:g}",
                f"paused_for_s={pause:g}",
            ],
            message=(
                "should emit an info log containing the"
                " limiter id, new window, and pause duration"
            ),
        )


class EmitMetricObservabilityTests:
    """Unified tests for the ``_emit_metric`` warning log when the callback raises.

    Subclasses must provide a ``limiter_with_failing_callback`` fixture that
    returns a limiter configured with a callback that raises ``RuntimeError``,
    and must set ``_log_label`` to the expected log prefix.
    """

    _log_label: str

    async def test_emit_metric_logs_warning_on_callback_exception(
        self, limiter_with_failing_callback, caplog
    ):
        """Verify that ``_emit_metric`` emits a WARNING log when the callback raises."""
        # Arrange
        limiter = limiter_with_failing_callback

        # Act
        with caplog.at_level(
            logging.WARNING, logger="redis_rate_limiter.core.limiters"
        ):
            limiter._emit_metric("consume", {"success": True})

        # Assert
        assert_log_emitted(
            caplog.records,
            level="WARNING",
            label=self._log_label,
            required_fragments=[
                f"limiter={limiter.id}",
                "event=consume",
                "callback boom",
            ],
            message=(
                "should emit a warning log containing the"
                " limiter id, event name, and error message"
            ),
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestSyncInitialDefaults(InitialDefaultTests):
    """Sync rate limiter initial defaults exercised through the async adapter."""

    @pytest.fixture
    def limiter(self, stub_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(stub_limiter)


@pytest.mark.behavior
class TestAsyncInitialDefaults(InitialDefaultTests):
    """Async rate limiter initial defaults exercised natively."""

    @pytest.fixture
    def limiter(self, async_stub_limiter):
        """Provide the async generic limiter directly."""
        return async_stub_limiter


# ---------------------------------------------------------------------------
# Boundary tests
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestBaseConfigBoundaryDecisions:
    """Boundary condition tests for ``AbstractRateLimiter.__init__`` parameter validation."""

    @staticmethod
    def test_rejects_empty_limiter_id(redis_client):
        """Verify that an empty ``limiter_id`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^limiter_id must be a non-empty string"):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="",
                limit=5,
                window=60,
                max_concurrency=2,
            )

    @staticmethod
    def test_rejects_negative_limit(redis_client):
        """Verify that a negative ``limit`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^limit must be a non-negative integer"):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=-1,
                window=60,
                max_concurrency=2,
            )

    @staticmethod
    def test_accepts_limit_zero(redis_client):
        """Verify that ``limit=0`` is accepted as a valid deny-all configuration."""
        # Act
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id="test_limit_zero",
            limit=0,
            window=60,
            max_concurrency=2,
        )

        # Assert
        assert limiter.limit == 0, "limit=0 should be accepted as deny-all mode"
        limiter.shutdown()

    @staticmethod
    def test_rejects_zero_window(redis_client):
        """Verify that ``window=0`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^window must be a positive number"):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=5,
                window=0,
                max_concurrency=2,
            )

    @staticmethod
    def test_rejects_negative_window(redis_client):
        """Verify that a negative ``window`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^window must be a positive number"):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=5,
                window=-1,
                max_concurrency=2,
            )


@pytest.mark.behavior
class TestMixinConfigBoundaryDecisions:
    """Boundary condition tests for ``DistributedRateLimiterMixin.__init__`` parameter validation."""

    @staticmethod
    def test_rejects_zero_max_concurrency(redis_client):
        """Verify that ``max_concurrency=0`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(
            ValueError, match=r"^max_concurrency must be a positive integer"
        ):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=5,
                window=60,
                max_concurrency=0,
            )

    @staticmethod
    def test_rejects_negative_max_concurrency(redis_client):
        """Verify that a negative ``max_concurrency`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(
            ValueError, match=r"^max_concurrency must be a positive integer"
        ):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=5,
                window=60,
                max_concurrency=-1,
            )

    @staticmethod
    def test_rejects_zero_max_age(redis_client):
        """Verify that ``max_age=0`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^max_age must be a positive integer"):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=5,
                window=60,
                max_concurrency=2,
                max_age=0,
            )

    @staticmethod
    def test_rejects_negative_max_age(redis_client):
        """Verify that a negative ``max_age`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^max_age must be a positive integer"):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=5,
                window=60,
                max_concurrency=2,
                max_age=-1,
            )

    @staticmethod
    def test_rejects_zero_lease_duration(redis_client):
        """Verify that ``lease_duration=0`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(
            ValueError, match=r"^lease_duration must be a positive integer"
        ):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=5,
                window=60,
                max_concurrency=2,
                lease_duration=0,
            )

    @staticmethod
    def test_rejects_negative_lease_duration(redis_client):
        """Verify that a negative ``lease_duration`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(
            ValueError, match=r"^lease_duration must be a positive integer"
        ):
            StubRateLimiter(
                redis_client=redis_client,
                limiter_id="test",
                limit=5,
                window=60,
                max_concurrency=2,
                lease_duration=-1,
            )


@pytest.mark.behavior
class TestScheduleTaskPriorityBoundaryDecisions:
    """Boundary condition tests for ``schedule_task()`` priority parameter validation."""

    @staticmethod
    def test_rejects_positive_infinite_priority(stub_limiter):
        """Verify that ``priority=inf`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^priority must be a finite number"):
            stub_limiter.schedule_task(
                "myapp.tasks.work", {"x": 1}, priority=float("inf")
            )

    @staticmethod
    def test_rejects_negative_infinite_priority(stub_limiter):
        """Verify that ``priority=-inf`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^priority must be a finite number"):
            stub_limiter.schedule_task(
                "myapp.tasks.work", {"x": 1}, priority=float("-inf")
            )

    @staticmethod
    def test_rejects_nan_priority(stub_limiter):
        """Verify that ``priority=NaN`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^priority must be a finite number"):
            stub_limiter.schedule_task(
                "myapp.tasks.work", {"x": 1}, priority=float("nan")
            )

    @staticmethod
    def test_accepts_zero_priority(stub_limiter):
        """Verify that ``priority=0`` is accepted as a valid ZSET score."""
        # Act
        was_scheduled, task_id = stub_limiter.schedule_task(
            "myapp.tasks.work", {"zero_priority": True}, priority=0
        )

        # Assert
        assert was_scheduled is True, "priority=0 should be accepted"

    @staticmethod
    def test_accepts_negative_priority(stub_limiter):
        """Verify that a negative ``priority`` is accepted as a valid ZSET score."""
        # Act
        was_scheduled, task_id = stub_limiter.schedule_task(
            "myapp.tasks.work", {"negative_priority": True}, priority=-10
        )

        # Assert
        assert was_scheduled is True, "negative priority should be accepted"


@pytest.mark.behavior
class TestAsyncScheduleTaskPriorityBoundaryDecisions:
    """Boundary condition tests for async ``schedule_task()`` priority parameter validation."""

    @staticmethod
    async def test_rejects_infinite_priority(async_stub_limiter):
        """Verify that ``priority=inf`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^priority must be a finite number"):
            await async_stub_limiter.schedule_task(
                "myapp.tasks.work", {"x": 1}, priority=float("inf")
            )

    @staticmethod
    async def test_rejects_nan_priority(async_stub_limiter):
        """Verify that ``priority=NaN`` raises ``ValueError``."""
        # Act & Assert
        with pytest.raises(ValueError, match=r"^priority must be a finite number"):
            await async_stub_limiter.schedule_task(
                "myapp.tasks.work", {"x": 1}, priority=float("nan")
            )

    @staticmethod
    async def test_accepts_zero_priority(async_stub_limiter):
        """Verify that ``priority=0`` is accepted as a valid ZSET score."""
        # Act
        was_scheduled, task_id = await async_stub_limiter.schedule_task(
            "myapp.tasks.work", {"zero_priority_async": True}, priority=0
        )

        # Assert
        assert was_scheduled is True, "priority=0 should be accepted"


@pytest.mark.behavior
class TestLargeValueAcceptance:
    """Acceptance tests for very large configuration values."""

    @staticmethod
    def test_accepts_large_limit(redis_client):
        """Verify that ``limit=10**9`` does not cause construction failure."""
        # Act
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id="test_large_limit",
            limit=10**9,
            window=60,
            max_concurrency=2,
        )

        # Assert
        assert limiter.limit == 10**9, "large limit should be accepted"
        limiter.shutdown()

    @staticmethod
    def test_accepts_large_window(redis_client):
        """Verify that ``window=86400`` (24 hours) does not cause construction failure."""
        # Act
        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id="test_large_window",
            limit=100,
            window=86400,
            max_concurrency=2,
        )

        # Assert
        assert limiter.window == 86400, "large window should be accepted"
        limiter.shutdown()


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestSyncWindowChangeLogging(WindowChangeObservabilityTests):
    """Sync rate limiter window-change logging exercised through the async adapter."""

    _log_label = "[StubRateLimiter]"

    @pytest.fixture
    def limiter(self, stub_limiter):
        """Wrap the sync generic limiter in an async adapter."""
        return SyncToAsyncLimiterAdapter(stub_limiter)


@pytest.mark.observability
class TestAsyncWindowChangeLogging(WindowChangeObservabilityTests):
    """Async rate limiter window-change logging exercised natively."""

    _log_label = "[AsyncStubRateLimiter]"

    @pytest.fixture
    def limiter(self, async_stub_limiter):
        """Provide the async generic limiter directly."""
        return async_stub_limiter


@pytest.mark.observability
class TestSyncEmitMetricLogging(EmitMetricObservabilityTests):
    """Sync rate limiter ``_emit_metric`` logging exercised
    through the async adapter.
    """

    _log_label = "[StubRateLimiter]"

    @pytest.fixture
    def limiter_with_failing_callback(self, redis_client, limiter_id):
        """Create a sync limiter with a failing callback, wrapped in the adapter."""
        from tests.implementations.conftest import StubRateLimiter

        def failing_callback(event, data):
            raise RuntimeError("callback boom")

        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_emit_metric_sync",
            limit=5,
            window=60,
            max_concurrency=2,
            metrics_callback=failing_callback,
        )
        yield SyncToAsyncLimiterAdapter(limiter)
        limiter.shutdown()


@pytest.mark.observability
class TestAsyncEmitMetricLogging(EmitMetricObservabilityTests):
    """Async rate limiter ``_emit_metric`` logging exercised natively."""

    _log_label = "[AsyncStubRateLimiter]"

    @pytest.fixture
    async def limiter_with_failing_callback(self, async_redis_client, limiter_id):
        """Create an async limiter with a failing callback."""
        from tests.implementations.conftest import AsyncStubRateLimiter

        def failing_callback(event, data):
            raise RuntimeError("callback boom")

        limiter = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_emit_metric_async",
            limit=5,
            window=60,
            max_concurrency=2,
            metrics_callback=failing_callback,
        )
        await limiter.start()
        yield limiter
        await limiter.shutdown()



@pytest.mark.observability
class TestSyncInitializationLog:
    """Sync rate limiter initialization log."""

    @staticmethod
    def test_start_emits_initialization_info_log(redis_client, limiter_id, caplog):
        """Verify that sync ``__init__`` emits an INFO log with limiter configuration."""
        from tests.implementations.conftest import StubRateLimiter

        # Act
        limiter_id = f"{limiter_id}_init_log_sync"
        with caplog.at_level(logging.INFO, logger="redis_rate_limiter.core.limiters"):
            limiter = StubRateLimiter(
                redis_client=redis_client,
                limiter_id=limiter_id,
                limit=5,
                window=60,
                max_concurrency=2,
            )

        try:
            # Assert
            assert_log_emitted(
                caplog.records,
                level="INFO",
                label="[StubRateLimiter]",
                required_fragments=[
                    f"id={limiter_id}",
                    "limit=5",
                    "window_s=60",
                    "max_concurrency=2",
                    "heartbeat_failure=warn",
                    "jitter_enabled=True",
                    "metrics_callback=disabled",
                    "drain_enabled=True",
                    "backend_health_monitor=disabled",
                ],
                message="should emit an info log with limiter configuration parameters",
            )
        finally:
            limiter.shutdown()


@pytest.mark.observability
class TestAsyncInitializationLog:
    """Async rate limiter initialization log."""

    @staticmethod
    async def test_start_emits_initialization_info_log(
        async_redis_client, limiter_id, caplog
    ):
        """Verify that async ``start()`` emits an INFO log with limiter configuration."""
        from tests.implementations.conftest import AsyncStubRateLimiter

        # Arrange
        limiter_id = f"{limiter_id}_init_log_async"
        limiter = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
        )

        try:
            # Act
            with caplog.at_level(
                logging.INFO, logger="redis_rate_limiter.core.async_limiters"
            ):
                await limiter.start()

            # Assert
            assert_log_emitted(
                caplog.records,
                level="INFO",
                label="[AsyncStubRateLimiter]",
                required_fragments=[
                    f"id={limiter_id}",
                    "limit=5",
                    "window_s=60",
                    "max_concurrency=2",
                    "heartbeat_failure=warn",
                    "jitter_enabled=True",
                    "metrics_callback=disabled",
                    "drain_enabled=True",
                    "backend_health_monitor=disabled",
                ],
                message="should emit an info log with limiter configuration parameters",
            )
        finally:
            await limiter.shutdown()


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestMixinInitSignatures:
    """Signature tests for ``DistributedRateLimiterMixin.__init__``
    default parameter values.
    """

    @staticmethod
    def test_mixin_init_default_parameters():
        """Verify that ``max_age`` and ``lease_duration`` have the expected defaults.

        Mutation target: ``max_age`` and ``lease_duration`` default values in
        ``DistributedRateLimiterMixin.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(DistributedRateLimiterMixin.__init__)

        # Assert
        assert sig.parameters["max_age"].default == 3600, "max_age default must be 3600"
        assert sig.parameters["lease_duration"].default == 30, (
            "lease_duration default must be 30"
        )

    @staticmethod
    def test_mixin_init_jitter_defaults():
        """Verify that jitter percentage defaults are correct.

        Mutation target: ``jitter_min_pct``, ``jitter_max_pct``,
        and ``jitter_enabled`` default values in
        ``DistributedRateLimiterMixin.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(DistributedRateLimiterMixin.__init__)

        # Assert
        assert sig.parameters["jitter_min_pct"].default == pytest.approx(0.02), (
            "jitter_min_pct default must be 0.02"
        )
        assert sig.parameters["jitter_max_pct"].default == pytest.approx(0.08), (
            "jitter_max_pct default must be 0.08"
        )
        assert sig.parameters["jitter_enabled"].default is True, (
            "jitter_enabled default must be True"
        )

    @staticmethod
    def test_mixin_init_drain_enabled_default():
        """Verify that ``drain_enabled`` defaults to ``True``.

        Mutation target: ``drain_enabled`` default value in
        ``DistributedRateLimiterMixin.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(DistributedRateLimiterMixin.__init__)

        # Assert
        assert sig.parameters["drain_enabled"].default is True, (
            "drain_enabled default must be True"
        )


@pytest.mark.signature
class TestConcreteClassInitSignatures:
    """Signature tests for concrete ``__init__`` default parameter values.

    Mutation target: default parameter values on ``AbstractDistributedRateLimiter.__init__``
    and ``AbstractAsyncDistributedRateLimiter.__init__`` (these duplicate the mixin defaults;
    the mixin tests above do not cover the concrete class overrides).
    """

    @staticmethod
    def _assert_shared_defaults(sig: inspect.Signature) -> None:
        """Assert that the shared default values match the expected values."""
        assert sig.parameters["max_age"].default == 3600, "max_age default must be 3600"
        assert sig.parameters["lease_duration"].default == 30, (
            "lease_duration default must be 30"
        )
        assert sig.parameters["on_heartbeat_failure"].default == "warn", (
            "on_heartbeat_failure default must be 'warn'"
        )
        assert sig.parameters["jitter_enabled"].default is True, (
            "jitter_enabled default must be True"
        )
        assert sig.parameters["jitter_min_pct"].default == pytest.approx(0.02), (
            "jitter_min_pct default must be 0.02"
        )
        assert sig.parameters["jitter_max_pct"].default == pytest.approx(0.08), (
            "jitter_max_pct default must be 0.08"
        )
        assert sig.parameters["drain_enabled"].default is True, (
            "drain_enabled default must be True"
        )

    def test_sync_init_default_parameters(self):
        """Verify that ``AbstractDistributedRateLimiter.__init__`` defaults match."""
        # Arrange & Act
        sig = inspect.signature(AbstractDistributedRateLimiter.__init__)

        # Assert
        self._assert_shared_defaults(sig)

    def test_async_init_default_parameters(self):
        """Verify that ``AbstractAsyncDistributedRateLimiter.__init__`` defaults match."""
        # Arrange & Act
        sig = inspect.signature(AbstractAsyncDistributedRateLimiter.__init__)

        # Assert
        self._assert_shared_defaults(sig)
