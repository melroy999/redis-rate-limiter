"""Tests for ``acquire()`` behavior on sync and async distributed rate limiters.

Tests are written once in async form using a mixin pattern.
The sync variant participates via ``SyncToAsyncLimiterAdapter``;
the async variant runs natively. Fixture mixins
(``_SyncAcquireFixture``, ``_AsyncAcquireFixture``) supply the
variant-specific fixtures and customization points.

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``: from ``tests/conftest.py``.
    - ``limiter_id``: from ``tests/conftest.py``.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from redis_rate_limiter.core.base import AcquireTimeout
from redis_rate_limiter.core.limiters import _ACQUIRE_MARKER_PATH
from tests.helpers.adapters import SyncToAsyncLimiterAdapter
from tests.implementations.conftest import AsyncStubRateLimiter, StubRateLimiter

# Matches the literal in consume.lua; independent of the Python constant.
_MARKER_LITERAL = "__redis_rate_limiter_acquire_marker__"

ACQUIRE_TIMEOUT = 5.0


# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class AcquireBehaviorTests:
    """Behavioral test mixin for ``acquire()`` preconditions, scheduling,
    and BLPOP signaling.

    Concrete test classes compose this mixin with a fixture mixin
    (``_SyncAcquireFixture`` or ``_AsyncAcquireFixture``) that supplies
    ``limiter`` and ``mock_target``.
    """

    _mock_cls = None

    async def test_acquire_raises_value_error_for_non_positive_timeout(
        self, limiter, mock_target
    ):
        """Verify that ``acquire()`` raises ``ValueError`` for negative timeout."""
        # Arrange
        sentinel = self._mock_cls(
            side_effect=AssertionError("blpop should not be reached")
        )
        with patch.object(mock_target.redis, "blpop", new=sentinel):
            # Act & Assert
            with pytest.raises(ValueError, match="timeout must be positive"):
                await limiter.acquire(-1)

    async def test_acquire_raises_value_error_for_zero_timeout(
        self, limiter, mock_target
    ):
        """Verify that ``acquire()`` raises ``ValueError`` for zero timeout."""
        # Arrange
        sentinel = self._mock_cls(
            side_effect=AssertionError("blpop should not be reached")
        )
        with patch.object(mock_target.redis, "blpop", new=sentinel):
            # Act & Assert
            with pytest.raises(ValueError, match="timeout must be positive"):
                await limiter.acquire(0)

    @staticmethod
    async def test_acquire_raises_runtime_error_without_drain_loop(
        limiter, mock_target
    ):
        """Verify that ``acquire()`` raises ``RuntimeError`` when the drain loop is not running."""
        # Arrange
        mock_target._drain_loop = None

        # Act & Assert
        with pytest.raises(RuntimeError, match="drain_enabled=True"):
            await limiter.acquire(ACQUIRE_TIMEOUT)

    async def test_acquire_schedules_marker_with_correct_func_path(
        self, limiter, mock_target
    ):
        """Verify that ``acquire()`` schedules a task with the acquire marker func path."""
        # Arrange
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ) as mock_schedule,
            patch.object(
                mock_target.redis,
                "blpop",
                new=self._mock_cls(return_value=("key", b"value")),
            ),
        ):
            # Act
            await limiter.acquire(ACQUIRE_TIMEOUT)

        # Assert
        _, kwargs = mock_schedule.call_args
        assert kwargs["func_path"] == _MARKER_LITERAL, (
            "schedule_task func_path should be the acquire marker sentinel"
        )

    async def test_acquire_schedules_marker_with_timeout_in_payload(
        self, limiter, mock_target
    ):
        """Verify that the scheduled marker payload contains the timeout in milliseconds."""
        # Arrange
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ) as mock_schedule,
            patch.object(
                mock_target.redis,
                "blpop",
                new=self._mock_cls(return_value=("key", b"value")),
            ),
        ):
            # Act
            await limiter.acquire(ACQUIRE_TIMEOUT)

        # Assert
        _, kwargs = mock_schedule.call_args
        assert kwargs["payload"]["_acquire_timeout_ms"] == int(
            ACQUIRE_TIMEOUT * 1000
        ), "payload should contain timeout converted to milliseconds"

    async def test_acquire_schedules_marker_with_uuid_in_payload(
        self, limiter, mock_target
    ):
        """Verify that the scheduled marker payload contains a non-empty UUID hex string."""
        # Arrange
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ) as mock_schedule,
            patch.object(
                mock_target.redis,
                "blpop",
                new=self._mock_cls(return_value=("key", b"value")),
            ),
        ):
            # Act
            await limiter.acquire(ACQUIRE_TIMEOUT)

        # Assert
        _, kwargs = mock_schedule.call_args
        uuid_value = kwargs["payload"]["_uuid"]
        assert isinstance(uuid_value, str) and len(uuid_value) > 0, (
            "payload should contain a non-empty UUID hex string"
        )

    async def test_acquire_schedules_marker_with_custom_priority(
        self, limiter, mock_target
    ):
        """Verify that ``acquire()`` forwards the priority argument to ``schedule_task()``."""
        # Arrange
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ) as mock_schedule,
            patch.object(
                mock_target.redis,
                "blpop",
                new=self._mock_cls(return_value=("key", b"value")),
            ),
        ):
            # Act
            await limiter.acquire(ACQUIRE_TIMEOUT, priority=42)

        # Assert
        _, kwargs = mock_schedule.call_args
        assert kwargs["priority"] == 42, (
            "schedule_task should receive the caller-specified priority"
        )

    @pytest.mark.parametrize(
        ("timeout", "expected_max_age"),
        [
            (0.3, 1),
            (1.0, 1),
            (1.1, 2),
            (2.9, 3),
            (5.0, 5),
        ],
        ids=["sub_second", "exactly_one", "just_above_one", "fractional", "integer"],
    )
    async def test_acquire_max_age_is_ceiling_of_timeout(
        self, limiter, mock_target, timeout, expected_max_age
    ):
        """Verify that ``max_age`` is ``max(1, math.ceil(timeout))``."""
        # Arrange
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ) as mock_schedule,
            patch.object(
                mock_target.redis,
                "blpop",
                new=self._mock_cls(return_value=("key", b"value")),
            ),
        ):
            # Act
            await limiter.acquire(timeout)

        # Assert
        _, kwargs = mock_schedule.call_args
        assert kwargs["max_age"] == expected_max_age, (
            f"max_age should be max(1, ceil({timeout})) = {expected_max_age}"
        )

    @staticmethod
    async def test_acquire_raises_runtime_error_when_scheduling_fails(
        limiter, mock_target
    ):
        """Verify that ``acquire()`` raises ``RuntimeError`` when the buffer rejects the marker."""
        # Arrange
        with patch.object(
            mock_target,
            "schedule_task",
            return_value=(False, "task-1"),
        ):
            # Act & Assert
            with pytest.raises(RuntimeError, match="rejected by buffer"):
                await limiter.acquire(ACQUIRE_TIMEOUT)

    async def test_acquire_blocks_on_correct_signal_key(self, limiter, mock_target):
        """Verify that ``acquire()`` calls ``blpop`` on the expected signal key."""
        # Arrange
        mock_blpop = self._mock_cls(return_value=("key", b"value"))
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ),
            patch.object(mock_target.redis, "blpop", new=mock_blpop),
        ):
            # Act
            await limiter.acquire(ACQUIRE_TIMEOUT)

        # Assert
        args, kwargs = mock_blpop.call_args
        expected_key = f"{mock_target.id}:acquire:task-1"
        assert args[0] == expected_key, (
            f"blpop should receive signal key '{expected_key}'"
        )
        assert kwargs.get("timeout") == ACQUIRE_TIMEOUT, (
            f"blpop timeout should be {ACQUIRE_TIMEOUT}"
        )

    async def test_acquire_raises_acquire_timeout_on_blpop_none(
        self, limiter, mock_target
    ):
        """Verify that ``acquire()`` raises ``AcquireTimeout`` when ``blpop`` returns ``None``."""
        # Arrange
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ),
            patch.object(
                mock_target.redis,
                "blpop",
                new=self._mock_cls(return_value=None),
            ),
        ):
            # Act & Assert
            with pytest.raises(AcquireTimeout):
                await limiter.acquire(ACQUIRE_TIMEOUT)

    async def test_acquire_timeout_message_contains_limiter_id(
        self, limiter, mock_target
    ):
        """Verify that the ``AcquireTimeout`` message includes the limiter ID."""
        # Arrange
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ),
            patch.object(
                mock_target.redis,
                "blpop",
                new=self._mock_cls(return_value=None),
            ),
        ):
            # Act & Assert
            with pytest.raises(AcquireTimeout, match=mock_target.id):
                await limiter.acquire(ACQUIRE_TIMEOUT)

    async def test_acquire_returns_lifecycle_with_correct_task_id(
        self, limiter, mock_target
    ):
        """Verify that ``acquire()`` returns a lifecycle object with the correct task ID."""
        # Arrange
        with (
            patch.object(
                mock_target,
                "schedule_task",
                return_value=(True, "task-1"),
            ),
            patch.object(
                mock_target.redis,
                "blpop",
                new=self._mock_cls(return_value=("key", b"value")),
            ),
        ):
            # Act
            result = await limiter.acquire(ACQUIRE_TIMEOUT)

        # Assert
        assert result.task_id == "task-1", (
            "returned lifecycle task_id should match the scheduled task"
        )


# ---------------------------------------------------------------------------
# Marker constant pinning test
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestAcquireMarkerConstant:
    """Pinning test for the ``_ACQUIRE_MARKER_PATH`` constant.

    The Lua script ``consume.lua`` hardcodes this literal in three
    locations. A mutation of the Python constant would cause a silent
    Python/Lua mismatch, so the value is pinned here.
    """

    @staticmethod
    def test_acquire_marker_path_matches_lua_literal():
        """Verify that ``_ACQUIRE_MARKER_PATH`` equals the literal checked by ``consume.lua``.

        Mutation target: ``_ACQUIRE_MARKER_PATH`` constant in ``limiters.py``.
        """
        # Assert
        assert _ACQUIRE_MARKER_PATH == _MARKER_LITERAL, (
            "_ACQUIRE_MARKER_PATH must match the literal in consume.lua"
        )


# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class _SyncAcquireFixture:
    """Shared fixture mixin for sync acquire tests."""

    _mock_cls = MagicMock

    @pytest.fixture
    def limiter(self, redis_client, limiter_id):
        """Create a sync stub limiter with drain loop enabled, wrapped in the async adapter."""
        limiter_id = f"{limiter_id}_acquire"
        test_limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            max_age=3600,
            lease_duration=30,
        )

        yield SyncToAsyncLimiterAdapter(test_limiter)

        test_limiter.shutdown()

    @pytest.fixture
    def mock_target(self, limiter):
        """Return the inner sync limiter as the patch target."""
        return limiter._inner


class _AsyncAcquireFixture:
    """Shared fixture mixin for async acquire tests."""

    _mock_cls = AsyncMock

    @pytest.fixture
    async def limiter(self, async_redis_client, limiter_id):
        """Create an async stub limiter with drain loop enabled."""
        limiter_id = f"{limiter_id}_async_acquire"
        test_limiter = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            max_age=3600,
            lease_duration=30,
        )
        await test_limiter.start()

        yield test_limiter

        await test_limiter.shutdown()

    @pytest.fixture
    def mock_target(self, limiter):
        """Return the async limiter as the patch target."""
        return limiter


@pytest.mark.behavior
class TestSyncAcquireBehavior(_SyncAcquireFixture, AcquireBehaviorTests):
    """Sync acquire behavior via the async adapter."""


@pytest.mark.behavior
class TestAsyncAcquireBehavior(_AsyncAcquireFixture, AcquireBehaviorTests):
    """Async acquire behavior exercised natively."""
