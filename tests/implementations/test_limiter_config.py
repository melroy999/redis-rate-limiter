"""Tests for limiter configuration defaults, window change
logging, and metric callbacks.

This module covers the initial default values of freshly constructed limiters,
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
                "paused_for_s=",
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


# ---------------------------------------------------------------------------
# Initialization log observability tests
# ---------------------------------------------------------------------------


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
                    "Rate limiter initialized",
                    f"id={limiter_id}",
                    "limit=5",
                    "window_s=60",
                ],
                message="should emit an info log with limiter id, limit, and window",
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
                    "Rate limiter initialized",
                    f"id={limiter_id}",
                    "limit=5",
                    "window_s=60",
                ],
                message="should emit an info log with limiter id, limit, and window",
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
