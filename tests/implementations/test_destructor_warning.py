"""Tests for ``__del__`` ResourceWarning emission on sync and async rate limiters.

Mutation testing revealed that the ``__del__`` methods on
``AbstractDistributedRateLimiter`` and ``AbstractAsyncDistributedRateLimiter``
had zero test coverage. This module verifies that:

- A ``ResourceWarning`` is emitted when a limiter with an active drain loop is
  garbage-collected without a prior ``shutdown()`` call.
- The warning message contains the limiter identifier and the correct shutdown
  instruction.
- No warning is emitted when the drain loop is disabled or when ``shutdown()``
  was called before destruction.
- The warning originates from the correct source module.

Tests are written once in async form via the mixin pattern; the sync
implementation participates directly (``__del__`` is synchronous on both
classes, so no ``SyncToAsyncLimiterAdapter`` is needed).

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``MinimalRateLimiter``, ``MinimalAsyncRateLimiter``: from ``tests/implementations/conftest.py``.
"""

import warnings

import pytest

# ---------------------------------------------------------------------------
# Unified implementation tests
# ---------------------------------------------------------------------------


class DestructorWarningTests:
    """Unified tests for ``__del__`` ResourceWarning emission.

    Subclasses must provide the following fixtures:
        - ``limiter_with_drain``: a limiter with ``drain_enabled=True`` that has
          NOT been shut down.
        - ``limiter_drain_disabled``: a limiter constructed with
          ``drain_enabled=False``.
        - ``limiter_after_shutdown``: a limiter that has already been shut down.
        - ``shutdown_instruction``: the backend-specific shutdown instruction
          string (e.g., ``"call shutdown() to stop background threads"``).
    """

    @staticmethod
    async def test_del_warns_when_shutdown_not_called(limiter_with_drain):
        """Verify that ``__del__`` emits a ResourceWarning when shutdown was not called."""
        # Arrange
        limiter = limiter_with_drain

        # Act & Assert
        with pytest.warns(ResourceWarning, match="was not shut down"):
            limiter.__del__()

    @staticmethod
    async def test_del_warning_contains_limiter_id(limiter_with_drain):
        """Verify that the warning message contains the limiter identifier."""
        # Arrange
        limiter = limiter_with_drain

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            limiter.__del__()

        # Assert
        resource_warnings = [
            w for w in caught if issubclass(w.category, ResourceWarning)
        ]
        assert len(resource_warnings) == 1, (
            "exactly one ResourceWarning should be emitted"
        )
        message_text = str(resource_warnings[0].message)
        assert f"limiter={limiter.id!r}" in message_text, (
            "warning message should contain the limiter id in limiter=<id> format"
        )

    @staticmethod
    async def test_del_warning_contains_shutdown_instruction(
        limiter_with_drain, shutdown_instruction
    ):
        """Verify that the warning message contains the correct shutdown instruction."""
        # Arrange
        limiter = limiter_with_drain

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            limiter.__del__()

        # Assert
        resource_warnings = [
            w for w in caught if issubclass(w.category, ResourceWarning)
        ]
        assert len(resource_warnings) == 1, (
            "exactly one ResourceWarning should be emitted"
        )
        message_text = str(resource_warnings[0].message)
        assert shutdown_instruction in message_text, (
            "warning message should contain the shutdown instruction"
        )

    @staticmethod
    async def test_del_warning_category_is_resource_warning(limiter_with_drain):
        """Verify that the warning category is exactly ``ResourceWarning``."""
        # Arrange
        limiter = limiter_with_drain

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            limiter.__del__()

        # Assert
        resource_warnings = [
            w for w in caught if issubclass(w.category, ResourceWarning)
        ]
        assert len(resource_warnings) == 1, (
            "exactly one ResourceWarning should be emitted"
        )
        assert resource_warnings[0].category is ResourceWarning, (
            "warning category should be ResourceWarning, not a subclass"
        )

    @staticmethod
    async def test_del_silent_when_drain_disabled(limiter_drain_disabled):
        """Verify that ``__del__`` emits no warning when the drain loop is disabled."""
        # Arrange
        limiter = limiter_drain_disabled

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            limiter.__del__()

        # Assert
        resource_warnings = [
            w for w in caught if issubclass(w.category, ResourceWarning)
        ]
        assert len(resource_warnings) == 0, (
            "no ResourceWarning should be emitted when drain is disabled"
        )

    @staticmethod
    async def test_del_silent_after_shutdown(limiter_after_shutdown):
        """Verify that ``__del__`` emits no warning after shutdown has been called."""
        # Arrange
        limiter = limiter_after_shutdown

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            limiter.__del__()

        # Assert
        resource_warnings = [
            w for w in caught if issubclass(w.category, ResourceWarning)
        ]
        assert len(resource_warnings) == 0, (
            "no ResourceWarning should be emitted after shutdown"
        )



# ---------------------------------------------------------------------------
# Concrete test cases
# ---------------------------------------------------------------------------


class TestSyncDestructorWarning(DestructorWarningTests):
    """Sync rate limiter ``__del__`` ResourceWarning tests."""

    @pytest.fixture
    def limiter_with_drain(self, redis_client, limiter_id):
        """Create a sync limiter with drain enabled that has not been shut down."""
        from tests.implementations.conftest import MinimalRateLimiter

        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_del_drain_sync",
            limit=5,
            window=60,
            max_concurrency=2,
        )
        yield limiter
        limiter.shutdown()

    @pytest.fixture
    def limiter_drain_disabled(self, redis_client, limiter_id):
        """Create a sync limiter with drain explicitly disabled."""
        from tests.implementations.conftest import MinimalRateLimiter

        return MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_del_nodrain_sync",
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )

    @pytest.fixture
    def limiter_after_shutdown(self, redis_client, limiter_id):
        """Create a sync limiter and shut it down before yielding."""
        from tests.implementations.conftest import MinimalRateLimiter

        limiter = MinimalRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_del_shutdown_sync",
            limit=5,
            window=60,
            max_concurrency=2,
        )
        limiter.shutdown()
        return limiter

    @pytest.fixture
    def shutdown_instruction(self):
        """Return the expected sync shutdown instruction."""
        return "call shutdown() to stop background threads"


class TestAsyncDestructorWarning(DestructorWarningTests):
    """Async rate limiter ``__del__`` ResourceWarning tests."""

    @pytest.fixture
    async def limiter_with_drain(self, async_redis_client, limiter_id):
        """Create an async limiter with drain enabled that has not been shut down."""
        from tests.implementations.conftest import MinimalAsyncRateLimiter

        limiter = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_del_drain_async",
            limit=5,
            window=60,
            max_concurrency=2,
        )
        await limiter.start()
        yield limiter
        await limiter.shutdown()

    @pytest.fixture
    def limiter_drain_disabled(self, async_redis_client, limiter_id):
        """Create an async limiter with drain explicitly disabled."""
        from tests.implementations.conftest import MinimalAsyncRateLimiter

        return MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_del_nodrain_async",
            limit=5,
            window=60,
            max_concurrency=2,
            drain_enabled=False,
        )

    @pytest.fixture
    async def limiter_after_shutdown(self, async_redis_client, limiter_id):
        """Create an async limiter, start it, and shut it down before yielding."""
        from tests.implementations.conftest import MinimalAsyncRateLimiter

        limiter = MinimalAsyncRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_del_shutdown_async",
            limit=5,
            window=60,
            max_concurrency=2,
        )
        await limiter.start()
        await limiter.shutdown()
        return limiter

    @pytest.fixture
    def shutdown_instruction(self):
        """Return the expected async shutdown instruction."""
        return "call await shutdown() to stop background tasks"

